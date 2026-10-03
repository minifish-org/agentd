use super::*;
use serde::{Deserialize, Serialize};
use std::future::Future;

const MAX_ACTION_BYTES: usize = 128;
const MAX_RESOURCE_TYPE_BYTES: usize = 128;
const MAX_ACTOR_KIND_BYTES: usize = 32;
const MAX_OUTCOME_BYTES: usize = 64;
const MAX_DETAILS_BYTES: usize = 65_536;

pub(super) const SCHEMA_STATEMENTS: [&str; 8] = [
    "CREATE TABLE IF NOT EXISTS audit_events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ts TEXT NOT NULL,
        tenant TEXT,
        actor_kind TEXT NOT NULL,
        actor_id TEXT NOT NULL,
        request_id TEXT,
        run_id TEXT,
        action TEXT NOT NULL,
        resource_type TEXT NOT NULL,
        resource_id TEXT,
        outcome TEXT NOT NULL,
        details_json TEXT NOT NULL CHECK (json_valid(details_json))
    )",
    "CREATE INDEX IF NOT EXISTS idx_audit_tenant_id ON audit_events(tenant, id DESC)",
    "CREATE INDEX IF NOT EXISTS idx_audit_ts_id ON audit_events(ts, id DESC)",
    "CREATE INDEX IF NOT EXISTS idx_audit_run_id ON audit_events(run_id, id DESC) WHERE run_id IS NOT NULL",
    "CREATE INDEX IF NOT EXISTS idx_audit_request_id ON audit_events(request_id, id DESC) WHERE request_id IS NOT NULL",
    "CREATE INDEX IF NOT EXISTS idx_audit_action_id ON audit_events(action, id DESC)",
    "CREATE TRIGGER IF NOT EXISTS audit_events_no_update BEFORE UPDATE ON audit_events
     BEGIN SELECT RAISE(ABORT, 'audit events are append-only'); END",
    "CREATE TRIGGER IF NOT EXISTS audit_events_no_delete BEFORE DELETE ON audit_events
     BEGIN SELECT RAISE(ABORT, 'audit events are append-only'); END",
];

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AuditContext {
    pub actor_kind: String,
    pub actor_id: String,
    pub request_id: Option<Uuid>,
    pub run_id: Option<Uuid>,
}

impl AuditContext {
    pub fn system(actor_id: &str) -> Self {
        Self {
            actor_kind: "system".into(),
            actor_id: actor_id.into(),
            request_id: None,
            run_id: None,
        }
    }

    pub fn agent(agent_ref: &str, run_id: Uuid) -> Self {
        Self {
            actor_kind: "agent".into(),
            actor_id: agent_ref.into(),
            request_id: None,
            run_id: Some(run_id),
        }
    }

    /// The boolean records whether shared-token authentication succeeded; the
    /// token value itself is never retained in the audit context.
    pub fn api(authenticated: bool, request_id: Uuid) -> Self {
        Self {
            actor_kind: "api".into(),
            actor_id: if authenticated {
                "shared_api_token"
            } else {
                "unauthenticated"
            }
            .into(),
            request_id: Some(request_id),
            run_id: None,
        }
    }
}

tokio::task_local! {
    static AUDIT_CONTEXT: AuditContext;
}

/// Scope identity to this future, restoring the previous context when the
/// future yields, completes, or is dropped. Spawned tasks must opt in explicitly.
pub async fn with_audit_context<F: Future>(context: AuditContext, future: F) -> F::Output {
    AUDIT_CONTEXT.scope(context, future).await
}

pub fn current_audit_context() -> AuditContext {
    AUDIT_CONTEXT
        .try_with(Clone::clone)
        .unwrap_or_else(|_| AuditContext::system("store"))
}

#[derive(Debug)]
pub struct AuditInput<'a> {
    pub tenant: Option<&'a str>,
    pub action: &'a str,
    pub resource_type: &'a str,
    pub resource_id: Option<&'a str>,
    pub outcome: &'a str,
    pub run_id: Option<Uuid>,
    /// Callers supply only explicitly permitted fields; never include raw
    /// request bodies, credentials, memory text, prompts, or tool arguments.
    pub details: serde_json::Value,
}

impl<'a> AuditInput<'a> {
    pub fn new(
        tenant: Option<&'a str>,
        action: &'a str,
        resource_type: &'a str,
        resource_id: Option<&'a str>,
        outcome: &'a str,
        details: serde_json::Value,
    ) -> Self {
        Self {
            tenant,
            action,
            resource_type,
            resource_id,
            outcome,
            run_id: None,
            details,
        }
    }

    pub fn for_run(mut self, run_id: Uuid) -> Self {
        self.run_id = Some(run_id);
        self
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct AuditEvent {
    pub id: i64,
    pub ts: DateTime<Utc>,
    pub tenant: Option<String>,
    pub actor_kind: String,
    pub actor_id: String,
    pub request_id: Option<Uuid>,
    pub run_id: Option<Uuid>,
    pub action: String,
    pub resource_type: String,
    pub resource_id: Option<String>,
    pub outcome: String,
    pub details: serde_json::Value,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct AuditQuery {
    pub tenant: Option<String>,
    pub action: Option<String>,
    pub outcome: Option<String>,
    pub resource_type: Option<String>,
    pub resource_id: Option<String>,
    pub actor_kind: Option<String>,
    pub actor_id: Option<String>,
    pub request_id: Option<Uuid>,
    pub run_id: Option<Uuid>,
    pub before_id: Option<i64>,
    pub since: Option<DateTime<Utc>>,
    pub until: Option<DateTime<Utc>>,
    pub limit: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct AuditPage {
    pub events: Vec<AuditEvent>,
    pub next_before_id: Option<i64>,
}

fn check_field(name: &str, value: &str, max_bytes: Option<usize>, required: bool) -> Result<()> {
    if required && value.trim().is_empty() {
        return Err(anyhow!("audit {name} must not be empty"));
    }
    if let Some(max_bytes) = max_bytes {
        if value.len() > max_bytes {
            return Err(anyhow!("audit {name} exceeds {max_bytes} UTF-8 bytes"));
        }
    }
    Ok(())
}

fn timestamp(value: DateTime<Utc>) -> String {
    value.to_rfc3339_opts(chrono::SecondsFormat::Nanos, true)
}

/// Append within the caller's business transaction. Explicit event run IDs
/// override the task context, while request identity always comes from context.
pub(super) async fn record(tx: &mut db::Transaction, input: AuditInput<'_>) -> Result<i64> {
    let context = current_audit_context();
    for (name, value, max_bytes) in [
        (
            "actor_kind",
            context.actor_kind.as_str(),
            Some(MAX_ACTOR_KIND_BYTES),
        ),
        ("actor_id", context.actor_id.as_str(), None),
        ("action", input.action, Some(MAX_ACTION_BYTES)),
        (
            "resource_type",
            input.resource_type,
            Some(MAX_RESOURCE_TYPE_BYTES),
        ),
        ("outcome", input.outcome, Some(MAX_OUTCOME_BYTES)),
    ] {
        check_field(name, value, max_bytes, true)?;
    }
    // Business identifiers already accepted by agentd must remain auditable
    // verbatim, including legacy names longer than internal audit label limits.
    let details = serde_json::to_string(&input.details)?;
    if details.len() > MAX_DETAILS_BYTES {
        // These top-level strings are business identifiers or schedule fields
        // already validated by callers. Their full values remain in storage;
        // only the remaining summary consumes the audit details budget. Keep
        // this clone/serialization off the usual small-summary path.
        let mut budgeted = input.details.clone();
        if let Some(fields) = budgeted.as_object_mut() {
            for key in [
                "namespace",
                "scope",
                "agent_ref",
                "target_agent",
                "cron",
                "timezone",
            ] {
                if let Some(value) = fields.get_mut(key) {
                    if value.is_string() {
                        *value = serde_json::Value::Null;
                    }
                }
            }
        }
        if serde_json::to_string(&budgeted)?.len() > MAX_DETAILS_BYTES {
            return Err(anyhow!(
                "audit details exceed {MAX_DETAILS_BYTES} serialized JSON bytes excluding business identifiers"
            ));
        }
    }
    db::query_scalar::<i64>(
        "INSERT INTO audit_events (ts, tenant, actor_kind, actor_id, request_id, run_id, action, resource_type, resource_id, outcome, details_json)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) RETURNING id",
    )
    .bind(timestamp(Utc::now()))
    .bind(input.tenant)
    .bind(context.actor_kind)
    .bind(context.actor_id)
    .bind(context.request_id.map(|id| id.to_string()))
    .bind(input.run_id.or(context.run_id).map(|id| id.to_string()))
    .bind(input.action)
    .bind(input.resource_type)
    .bind(input.resource_id)
    .bind(input.outcome)
    .bind(details)
    .fetch_optional(tx)
    .await?
    .ok_or_else(|| anyhow!("audit insert returned no event id"))
}

fn row_to_event(row: db::SqlRow) -> Result<AuditEvent> {
    let optional_uuid = |name| -> Result<Option<Uuid>> {
        row.try_get::<Option<String>, _>(name)?
            .map(|value| {
                Uuid::parse_str(&value).map_err(|error| StoreError::database(error).into())
            })
            .transpose()
    };
    Ok(AuditEvent {
        id: row.try_get("id")?,
        ts: parse_ts_field(&row, "ts")?,
        tenant: row.try_get("tenant")?,
        actor_kind: row.try_get("actor_kind")?,
        actor_id: row.try_get("actor_id")?,
        request_id: optional_uuid("request_id")?,
        run_id: optional_uuid("run_id")?,
        action: row.try_get("action")?,
        resource_type: row.try_get("resource_type")?,
        resource_id: row.try_get("resource_id")?,
        outcome: row.try_get("outcome")?,
        details: decode_json(&row.try_get::<String, _>("details_json")?)?,
    })
}

impl AgentdStore {
    pub async fn append_audit(&self, input: AuditInput<'_>) -> Result<i64> {
        let mut tx = self.pool.begin().await?;
        let id = record(&mut tx, input).await?;
        tx.commit().await?;
        Ok(id)
    }

    /// Audit history remains queryable after tenant and run deletion. No live
    /// resource lookup is used to authorize or narrow these explicit filters.
    pub async fn list_audit_events(&self, filter: &AuditQuery) -> Result<AuditPage> {
        let limit = filter.limit.unwrap_or(50);
        if !(1..=500).contains(&limit) {
            return Err(invalid!("audit limit must be between 1 and 500"));
        }
        if filter.before_id.is_some_and(|id| id < 0) {
            return Err(invalid!("audit before_id must not be negative"));
        }
        if matches!((filter.since, filter.until), (Some(since), Some(until)) if since > until) {
            return Err(invalid!("audit since must not be later than until"));
        }
        let fields = [
            ("tenant", filter.tenant.as_deref(), None),
            ("action", filter.action.as_deref(), Some(MAX_ACTION_BYTES)),
            (
                "outcome",
                filter.outcome.as_deref(),
                Some(MAX_OUTCOME_BYTES),
            ),
            (
                "resource_type",
                filter.resource_type.as_deref(),
                Some(MAX_RESOURCE_TYPE_BYTES),
            ),
            ("resource_id", filter.resource_id.as_deref(), None),
            (
                "actor_kind",
                filter.actor_kind.as_deref(),
                Some(MAX_ACTOR_KIND_BYTES),
            ),
            ("actor_id", filter.actor_id.as_deref(), None),
        ];
        let mut sql = String::from("SELECT * FROM audit_events WHERE 1 = 1");
        for (name, value, max_bytes) in fields {
            if let Some(value) = value {
                check_field(name, value, max_bytes, false)?;
                sql.push_str(&format!(" AND {name} = ?"));
            }
        }
        for (name, present) in [
            ("request_id", filter.request_id.is_some()),
            ("run_id", filter.run_id.is_some()),
        ] {
            if present {
                sql.push_str(&format!(" AND {name} = ?"));
            }
        }
        if filter.before_id.is_some() {
            sql.push_str(" AND id < ?");
        }
        if filter.since.is_some() {
            sql.push_str(" AND ts >= ?");
        }
        if filter.until.is_some() {
            sql.push_str(" AND ts <= ?");
        }
        sql.push_str(" ORDER BY id DESC LIMIT ?");
        let mut query = db::query(&sql);
        for (_, value, _) in fields {
            if let Some(value) = value {
                query = query.bind(value);
            }
        }
        for id in [filter.request_id, filter.run_id].into_iter().flatten() {
            query = query.bind(id.to_string());
        }
        if let Some(id) = filter.before_id {
            query = query.bind(id);
        }
        if let Some(since) = filter.since {
            query = query.bind(timestamp(since));
        }
        if let Some(until) = filter.until {
            query = query.bind(timestamp(until));
        }
        let rows = query.bind((limit + 1) as i64).fetch_all(&self.pool).await?;
        let has_more = rows.len() > limit;
        let events = rows
            .into_iter()
            .take(limit)
            .map(row_to_event)
            .collect::<Result<Vec<_>>>()?;
        let next_before_id = has_more
            .then(|| events.last().map(|event| event.id))
            .flatten();
        Ok(AuditPage {
            events,
            next_before_id,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    async fn fixture() -> (TempDir, AgentdStore) {
        let dir = TempDir::new().unwrap();
        let store = AgentdStore::new(dir.path().join("agentd.db").to_str().unwrap())
            .await
            .unwrap();
        (dir, store)
    }

    fn event(tenant: Option<&str>) -> AuditInput<'_> {
        AuditInput::new(
            tenant,
            "test.audit",
            "test_resource",
            Some("resource-1"),
            "succeeded",
            json!({"changed":true}),
        )
    }

    fn query(tenant: Option<&str>) -> AuditQuery {
        AuditQuery {
            tenant: tenant.map(str::to_owned),
            action: Some("test.audit".into()),
            ..AuditQuery::default()
        }
    }

    #[tokio::test]
    async fn audit_and_business_mutation_roll_back_together() {
        let (_dir, store) = fixture().await;
        let mut tx = store.pool.begin().await.unwrap();
        let now = Utc::now().to_rfc3339();
        db::query("INSERT INTO tenants (name, metadata_json, created_at, updated_at) VALUES ('rolled-back', '{}', ?, ?)")
            .bind(&now).bind(&now).execute(&mut tx).await.unwrap();
        record(&mut tx, event(Some("rolled-back"))).await.unwrap();
        tx.rollback().await.unwrap();
        assert!(store.get_tenant("rolled-back").await.unwrap().is_none());
        assert!(store
            .list_audit_events(&query(Some("rolled-back")))
            .await
            .unwrap()
            .events
            .is_empty());

        // A later business failure also must not leave an already inserted audit.
        let mut tx = store.pool.begin().await.unwrap();
        record(&mut tx, event(Some("failed-change"))).await.unwrap();
        assert!(db::query("INSERT INTO missing_business_table VALUES (1)")
            .execute(&mut tx)
            .await
            .is_err());
        tx.rollback().await.unwrap();
        assert!(store
            .list_audit_events(&query(Some("failed-change")))
            .await
            .unwrap()
            .events
            .is_empty());
    }

    #[tokio::test]
    async fn audit_events_reject_update_and_delete() {
        let (_dir, store) = fixture().await;
        let id = store.append_audit(event(Some("one"))).await.unwrap();
        for statement in [
            "UPDATE audit_events SET outcome = 'altered' WHERE id = ?",
            "DELETE FROM audit_events WHERE id = ?",
        ] {
            let error = db::query(statement)
                .bind(id)
                .execute(&store.pool)
                .await
                .err()
                .unwrap();
            assert!(error.to_string().contains("append-only"));
        }
        let events = store
            .list_audit_events(&query(Some("one")))
            .await
            .unwrap()
            .events;
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].id, id);
        assert_eq!(events[0].outcome, "succeeded");
    }

    #[tokio::test]
    async fn pagination_is_stable_and_tenant_filters_are_exact() {
        let (_dir, store) = fixture().await;
        let mut expected = Vec::new();
        for _ in 0..7 {
            expected.push(store.append_audit(event(Some("one"))).await.unwrap());
            store.append_audit(event(Some("one-other"))).await.unwrap();
        }
        store.append_audit(event(None)).await.unwrap();
        expected.reverse();
        let mut filter = query(Some("one"));
        filter.limit = Some(3);
        let mut actual = Vec::new();
        loop {
            let page = store.list_audit_events(&filter).await.unwrap();
            assert!(page
                .events
                .iter()
                .all(|event| event.tenant.as_deref() == Some("one")));
            actual.extend(page.events.iter().map(|event| event.id));
            let Some(cursor) = page.next_before_id else {
                break;
            };
            assert_eq!(Some(cursor), page.events.last().map(|event| event.id));
            filter.before_id = Some(cursor);
        }
        assert_eq!(actual, expected);
        let all = store.list_audit_events(&query(None)).await.unwrap();
        assert_eq!(all.events.len(), 15);
        assert!(all.events.iter().any(|event| event.tenant.is_none()));
    }

    #[tokio::test]
    async fn deleted_tenant_history_remains_queryable() {
        let (_dir, store) = fixture().await;
        store.create_tenant("deleted", &json!({})).await.unwrap();
        let id = store.append_audit(event(Some("deleted"))).await.unwrap();
        store.delete_tenant("deleted").await.unwrap();
        assert!(store.get_tenant("deleted").await.unwrap().is_none());
        let page = store
            .list_audit_events(&query(Some("deleted")))
            .await
            .unwrap();
        assert_eq!(page.events.len(), 1);
        assert_eq!(page.events[0].id, id);
    }

    #[tokio::test]
    async fn all_filters_and_timestamp_boundaries_are_applied() {
        let (_dir, store) = fixture().await;
        let request_id = Uuid::new_v4();
        let inherited_run = Uuid::new_v4();
        let explicit_run = Uuid::new_v4();
        let context = AuditContext {
            actor_kind: "agent".into(),
            actor_id: "worker".into(),
            request_id: Some(request_id),
            run_id: Some(inherited_run),
        };
        let id = with_audit_context(
            context,
            store.append_audit(event(Some("one")).for_run(explicit_run)),
        )
        .await
        .unwrap();
        let event = store
            .list_audit_events(&query(Some("one")))
            .await
            .unwrap()
            .events
            .pop()
            .unwrap();
        assert_eq!(event.run_id, Some(explicit_run));
        let mut filter = AuditQuery {
            tenant: Some("one".into()),
            action: Some("test.audit".into()),
            outcome: Some("succeeded".into()),
            resource_type: Some("test_resource".into()),
            resource_id: Some("resource-1".into()),
            actor_kind: Some("agent".into()),
            actor_id: Some("worker".into()),
            request_id: Some(request_id),
            run_id: Some(explicit_run),
            since: Some(event.ts),
            until: Some(event.ts),
            ..AuditQuery::default()
        };
        assert_eq!(
            store.list_audit_events(&filter).await.unwrap().events[0].id,
            id
        );
        filter.run_id = Some(inherited_run);
        assert!(store
            .list_audit_events(&filter)
            .await
            .unwrap()
            .events
            .is_empty());
        filter.run_id = Some(explicit_run);
        filter.since = Some(event.ts + ChronoDuration::nanoseconds(1));
        filter.until = None;
        assert!(store
            .list_audit_events(&filter)
            .await
            .unwrap()
            .events
            .is_empty());
    }

    #[tokio::test]
    async fn invalid_queries_and_oversized_audit_fields_are_rejected() {
        let (_dir, store) = fixture().await;
        for filter in [
            AuditQuery {
                limit: Some(0),
                ..AuditQuery::default()
            },
            AuditQuery {
                limit: Some(501),
                ..AuditQuery::default()
            },
            AuditQuery {
                before_id: Some(-1),
                ..AuditQuery::default()
            },
            AuditQuery {
                since: Some(Utc::now()),
                until: Some(Utc::now() - ChronoDuration::days(1)),
                ..AuditQuery::default()
            },
        ] {
            assert!(store.list_audit_events(&filter).await.is_err());
        }
        assert!(serde_json::from_value::<AuditQuery>(json!({"unknown_filter":1})).is_err());
        assert!(serde_json::from_value::<AuditQuery>(json!({"limit":-1})).is_err());
        let too_long = "字".repeat(MAX_ACTION_BYTES);
        let mut input = event(None);
        input.action = &too_long;
        assert!(store.append_audit(input).await.is_err());
        let mut input = event(None);
        input.details = json!({"oversized":"x".repeat(MAX_DETAILS_BYTES)});
        assert!(store.append_audit(input).await.is_err());
        assert!(store
            .list_audit_events(&query(None))
            .await
            .unwrap()
            .events
            .is_empty());
    }

    #[tokio::test]
    async fn long_business_identifiers_are_preserved_and_queryable() {
        let (_dir, store) = fixture().await;
        let tenant = "tenant-".to_owned() + &"字".repeat(300);
        let resource_id = "resource-".to_owned() + &"r".repeat(5000);
        let actor_id = "agent-".to_owned() + &"a".repeat(2000);
        let mut input = event(Some(&tenant));
        input.resource_id = Some(&resource_id);
        let id = with_audit_context(AuditContext::system(&actor_id), store.append_audit(input))
            .await
            .unwrap();
        let page = store
            .list_audit_events(&AuditQuery {
                tenant: Some(tenant.clone()),
                resource_id: Some(resource_id.clone()),
                actor_id: Some(actor_id.clone()),
                ..AuditQuery::default()
            })
            .await
            .unwrap();
        assert_eq!(page.events.len(), 1);
        assert_eq!(page.events[0].id, id);
        assert_eq!(page.events[0].tenant.as_deref(), Some(tenant.as_str()));
        assert_eq!(
            page.events[0].resource_id.as_deref(),
            Some(resource_id.as_str())
        );
        assert_eq!(page.events[0].actor_id, actor_id);
    }

    #[tokio::test]
    async fn task_context_isolated_between_requests_and_restored_after_nested_scope() {
        let (_dir, store) = fixture().await;
        assert_eq!(current_audit_context(), AuditContext::system("store"));
        let barrier = Arc::new(tokio::sync::Barrier::new(2));
        let mut tasks = Vec::new();
        let request_ids = [Uuid::new_v4(), Uuid::new_v4()];
        for (index, request_id) in request_ids.into_iter().enumerate() {
            let store = store.clone();
            let barrier = barrier.clone();
            tasks.push(tokio::spawn(async move {
                let context = AuditContext::api(index == 0, request_id);
                with_audit_context(context.clone(), async {
                    barrier.wait().await;
                    tokio::task::yield_now().await;
                    assert_eq!(current_audit_context(), context);
                    let agent = AuditContext::agent("nested-agent", Uuid::new_v4());
                    with_audit_context(agent.clone(), async {
                        assert_eq!(current_audit_context(), agent);
                    })
                    .await;
                    assert_eq!(current_audit_context(), context);
                    store
                        .append_audit(event(Some(if index == 0 { "one" } else { "two" })))
                        .await
                        .unwrap();
                })
                .await;
                assert_eq!(current_audit_context(), AuditContext::system("store"));
            }));
        }
        for task in tasks {
            task.await.unwrap();
        }
        let one = store
            .list_audit_events(&query(Some("one")))
            .await
            .unwrap()
            .events
            .pop()
            .unwrap();
        let two = store
            .list_audit_events(&query(Some("two")))
            .await
            .unwrap()
            .events
            .pop()
            .unwrap();
        assert_eq!(one.request_id, Some(request_ids[0]));
        assert_eq!(one.actor_id, "shared_api_token");
        assert_eq!(two.request_id, Some(request_ids[1]));
        assert_eq!(two.actor_id, "unauthenticated");
        let default_id = store.append_audit(event(None)).await.unwrap();
        let defaults = store.list_audit_events(&query(None)).await.unwrap().events;
        let event = defaults
            .iter()
            .find(|event| event.id == default_id)
            .unwrap();
        assert_eq!(event.actor_kind, "system");
        assert_eq!(event.actor_id, "store");
        assert!(event.request_id.is_none());
        assert!(event.run_id.is_none());
    }
}
