use super::*;
use agentd_api::{AgentSpec, BehaviorLearningOptions, BehaviorRevision};

pub(super) const SCHEMA_STATEMENTS: [&str; 3] = [
    "CREATE TABLE IF NOT EXISTS behavior_heads (
        tenant TEXT NOT NULL,
        agent_ref TEXT NOT NULL,
        active_revision INTEGER,
        source_updated_at TEXT,
        source_run_id TEXT,
        PRIMARY KEY (tenant, agent_ref)
    )",
    "CREATE TABLE IF NOT EXISTS behavior_revisions (
        tenant TEXT NOT NULL,
        agent_ref TEXT NOT NULL,
        revision INTEGER NOT NULL,
        parent_revision INTEGER,
        spec_json TEXT NOT NULL,
        instructions TEXT NOT NULL,
        outcome TEXT NOT NULL CHECK (outcome IN ('promoted', 'rejected', 'stale')),
        source_run_id TEXT NOT NULL UNIQUE,
        report_json TEXT NOT NULL,
        created_at TEXT NOT NULL,
        PRIMARY KEY (tenant, agent_ref, revision)
    )",
    "CREATE INDEX IF NOT EXISTS idx_runs_behavior_sources ON runs(tenant, agent_ref, updated_at, run_id)",
];

#[derive(Debug, Clone)]
pub struct BehaviorSnapshot {
    pub agent: Agent,
    pub active_revision: Option<BehaviorRevision>,
}

/// Cheap scheduling eligibility, before the runtime validates captured traces.
/// Counts describe terminal source rows, not necessarily usable model requests.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct BehaviorLearningReadiness {
    pub ready: bool,
    pub reason: &'static str,
    pub source_runs: usize,
    pub independent_scopes: usize,
}

pub struct BehaviorLearningResult<'a> {
    pub target_agent: &'a str,
    pub expected_spec: &'a AgentSpec,
    pub expected_revision: Option<u64>,
    pub instructions: &'a str,
    pub report: &'a serde_json::Value,
    pub promote: bool,
    pub source_updated_at: DateTime<Utc>,
    pub source_run_id: Uuid,
}

pub(super) fn is_foreground_agent(agent: &Agent) -> bool {
    !agent.name.starts_with("system/")
        && agent
            .metadata
            .labels
            .get("agentd.system")
            .is_none_or(|value| value != "true")
}

fn row_to_revision(row: db::SqlRow) -> Result<BehaviorRevision> {
    Ok(BehaviorRevision {
        tenant: row.try_get("tenant")?,
        agent_ref: row.try_get("agent_ref")?,
        revision: row.try_get::<i64, _>("revision")? as u64,
        parent_revision: row
            .try_get::<Option<i64>, _>("parent_revision")?
            .map(|n| n as u64),
        instructions: row.try_get("instructions")?,
        outcome: row.try_get("outcome")?,
        source_run_id: parse_uuid_field(&row, "source_run_id")?,
        report: serde_json::from_str(&row.try_get::<String, _>("report_json")?)?,
        created_at: parse_ts_field(&row, "created_at")?,
    })
}

pub(super) async fn active_revision_for_spec(
    tx: &mut db::Transaction,
    tenant: &str,
    agent: &str,
    spec: &AgentSpec,
) -> Result<Option<BehaviorRevision>> {
    let row = db::query(
        "SELECT r.* FROM behavior_revisions r
         JOIN behavior_heads h ON h.tenant = r.tenant AND h.agent_ref = r.agent_ref AND h.active_revision = r.revision
         JOIN agents a ON a.tenant = r.tenant AND a.name = r.agent_ref
         WHERE r.tenant = ? AND r.agent_ref = ? AND r.outcome = 'promoted'
           AND a.spec_json = r.spec_json
           AND r.created_at >= a.created_at
           AND COALESCE(json_extract(a.metadata_json, '$.labels.\"agentd.system\"'), '') != 'true'",
    ).bind(tenant).bind(agent).fetch_optional(tx).await?;
    if agent.starts_with("system/") {
        return Ok(None);
    }
    let Some(row) = row else { return Ok(None) };
    let stored_spec: AgentSpec = serde_json::from_str(&row.try_get::<String, _>("spec_json")?)?;
    if stored_spec != *spec {
        return Ok(None);
    }
    row_to_revision(row).map(Some)
}

impl AgentdStore {
    /// Inspect a concrete target without reading model traces or calling an LLM.
    /// This is a necessary scheduling gate, not a reservation or an assertion
    /// that the selected traces satisfy the runtime's schema and budget checks.
    pub async fn behavior_learning_readiness(
        &self,
        tenant: &str,
        options: &BehaviorLearningOptions,
    ) -> Result<BehaviorLearningReadiness> {
        options.validate().map_err(|error| anyhow!(error))?;
        if options.target_agent == "*" {
            return Err(anyhow!(
                "behavior readiness requires a concrete target_agent"
            ));
        }
        let mut readiness = BehaviorLearningReadiness {
            ready: false,
            reason: "target_not_found",
            source_runs: 0,
            independent_scopes: 0,
        };
        let Some(agent) = self.get_agent(tenant, &options.target_agent).await? else {
            return Ok(readiness);
        };
        if !is_foreground_agent(&agent) {
            readiness.reason = "system_target";
            return Ok(readiness);
        }
        // Every learner submission canonicalizes this scope from target_agent.
        // A pending run in another tenant or for another target is independent.
        let pending = db::query_scalar::<i64>(
            "SELECT EXISTS(SELECT 1 FROM runs WHERE tenant = ? AND agent_ref = ?
             AND scope = ? AND status IN ('queued', 'running'))",
        )
        .bind(tenant)
        .bind(BEHAVIOR_LEARNER_AGENT)
        .bind(format!("behavior-learning/{}", options.target_agent))
        .fetch_optional(&self.pool)
        .await?
        .unwrap_or(0);
        // Match list_behavior_source_runs and the core's bounded source scan,
        // projecting only scope to avoid loading potentially large run inputs.
        let rows = db::query(
            "SELECT r.scope FROM runs r
             JOIN agents a ON a.tenant = r.tenant AND a.name = r.agent_ref
             LEFT JOIN behavior_heads h ON h.tenant = r.tenant AND h.agent_ref = r.agent_ref
             WHERE r.tenant = ? AND r.agent_ref = ? AND r.status IN ('succeeded', 'failed')
               AND r.created_at >= a.created_at
               AND COALESCE(json_extract(a.metadata_json, '$.labels.\"agentd.system\"'), '') != 'true'
               AND (h.source_updated_at IS NULL OR r.updated_at > h.source_updated_at
                    OR (r.updated_at = h.source_updated_at AND r.run_id > h.source_run_id))
             ORDER BY r.updated_at DESC, r.run_id DESC LIMIT ?",
        )
        .bind(tenant)
        .bind(&options.target_agent)
        .bind(options.max_samples.saturating_mul(4).min(256) as i64)
        .fetch_all(&self.pool)
        .await?;
        let scopes = rows
            .iter()
            .map(|row| row.try_get::<String, _>("scope"))
            .collect::<std::result::Result<BTreeSet<_>, _>>()?;
        readiness.source_runs = rows.len();
        readiness.independent_scopes = scopes.len();
        readiness.reason = if pending != 0 {
            "already_pending"
        } else if readiness.source_runs < options.min_samples {
            "insufficient_samples"
        } else if readiness.independent_scopes < 2 {
            "insufficient_independent_scopes"
        } else {
            readiness.ready = true;
            "ready"
        };
        Ok(readiness)
    }

    pub(super) async fn ensure_foreground_behavior_target(
        &self,
        tenant: &str,
        target_agent: &str,
    ) -> Result<()> {
        let target = self
            .get_agent(tenant, target_agent)
            .await?
            .ok_or_else(|| anyhow!("learning target agent not found: {target_agent}"))?;
        if !is_foreground_agent(&target) {
            return Err(anyhow!(
                "behavior learning target must be a foreground agent"
            ));
        }
        Ok(())
    }

    pub async fn get_behavior_snapshot(
        &self,
        tenant: &str,
        agent: &str,
    ) -> Result<Option<BehaviorSnapshot>> {
        let mut tx = self.pool.begin().await?;
        let row = db::query(
            "SELECT metadata_json, spec_json, created_at, updated_at FROM agents WHERE tenant = ? AND name = ?",
        ).bind(tenant).bind(agent).fetch_optional(&mut tx).await?;
        let Some(row) = row else { return Ok(None) };
        let agent = row_to_agent(row)?;
        let active_revision =
            active_revision_for_spec(&mut tx, tenant, &agent.name, &agent.spec).await?;
        tx.commit().await?;
        Ok(Some(BehaviorSnapshot {
            agent,
            active_revision,
        }))
    }

    /// Recent terminal foreground runs strictly after the consumed cursor,
    /// restricted to this incarnation of the agent. This is recent sampling:
    /// advancing the cursor also retires older runs that were not selected.
    pub async fn list_behavior_source_runs(
        &self,
        tenant: &str,
        agent: &str,
        limit: usize,
    ) -> Result<Vec<AgentRun>> {
        if agent.starts_with("system/") || limit == 0 {
            return Ok(Vec::new());
        }
        let rows = db::query(
            "SELECT r.* FROM runs r
             JOIN agents a ON a.tenant = r.tenant AND a.name = r.agent_ref
             LEFT JOIN behavior_heads h ON h.tenant = r.tenant AND h.agent_ref = r.agent_ref
             WHERE r.tenant = ? AND r.agent_ref = ? AND r.status IN ('succeeded', 'failed')
               AND r.created_at >= a.created_at
               AND COALESCE(json_extract(a.metadata_json, '$.labels.\"agentd.system\"'), '') != 'true'
               AND (h.source_updated_at IS NULL OR r.updated_at > h.source_updated_at
                    OR (r.updated_at = h.source_updated_at AND r.run_id > h.source_run_id))
             ORDER BY r.updated_at DESC, r.run_id DESC LIMIT ?",
        ).bind(tenant).bind(agent).bind(limit.min(256) as i64).fetch_all(&self.pool).await?;
        rows.into_iter().map(row_to_run).collect()
    }

    pub async fn list_behavior_revisions(
        &self,
        tenant: &str,
        agent: &str,
        limit: usize,
    ) -> Result<Vec<BehaviorRevision>> {
        let rows = db::query("SELECT * FROM behavior_revisions WHERE tenant = ? AND agent_ref = ? ORDER BY revision DESC LIMIT ?")
            .bind(tenant).bind(agent).bind(limit.min(256) as i64).fetch_all(&self.pool).await?;
        rows.into_iter().map(row_to_revision).collect()
    }

    /// Disable the active supplement while keeping the complete audit history.
    pub async fn clear_behavior_policy(&self, tenant: &str, agent: &str) -> Result<bool> {
        let mut tx = self.pool.begin().await?;
        let previous_revision = db::query_scalar::<Option<i64>>(
            "SELECT active_revision FROM behavior_heads WHERE tenant = ? AND agent_ref = ?",
        )
        .bind(tenant)
        .bind(agent)
        .fetch_optional(&mut tx)
        .await?
        .flatten();
        let cleared = db::query("UPDATE behavior_heads SET active_revision = NULL WHERE tenant = ? AND agent_ref = ? AND active_revision IS NOT NULL")
            .bind(tenant).bind(agent).execute(&mut tx).await?.rows_affected() > 0;
        audit::record(&mut tx, AuditInput::new(
            Some(tenant), "behavior.clear", "behavior_policy", Some(agent),
            if cleared { "succeeded" } else { "noop" },
            json!({"target_agent":agent,"previous_revision":previous_revision,"cleared":cleared}),
        )).await?;
        tx.commit().await?;
        Ok(cleared)
    }

    /// Atomically publish the candidate, consume its source cursor, and finish
    /// the coordinator. Cancellation or a changed parent can never publish.
    pub async fn finish_behavior_learning(
        &self,
        run_id: Uuid,
        result: BehaviorLearningResult<'_>,
    ) -> Result<BehaviorRevision> {
        if result.instructions.len() > 4096 {
            return Err(anyhow!("learned instructions must be at most 4096 bytes"));
        }
        let mut tx = self.pool.begin().await?;
        let run = db::query("SELECT tenant, agent_ref, status, input_json, output_json, delivery_destination, created_at FROM runs WHERE run_id = ?")
            .bind(run_id.to_string()).fetch_optional(&mut tx).await?
            .ok_or_else(|| anyhow!("learning run not found"))?;
        if run.try_get::<String, _>("agent_ref")? != BEHAVIOR_LEARNER_AGENT
            || run.try_get::<String, _>("status")? != "running"
            || run
                .try_get::<Option<String>, _>("delivery_destination")?
                .is_some()
            || run.try_get::<Option<String>, _>("output_json")?.is_some()
        {
            return Err(anyhow!(
                "only a running behavior learner without delivery can publish"
            ));
        }
        let input: serde_json::Value =
            serde_json::from_str(&run.try_get::<String, _>("input_json")?)?;
        let target = input.get("target_agent").or_else(|| {
            input
                .get("input")
                .and_then(|payload| payload.get("target_agent"))
        });
        if target.and_then(serde_json::Value::as_str) != Some(result.target_agent)
            || result.target_agent.starts_with("system/")
        {
            return Err(anyhow!(
                "learning result target does not match the foreground run target"
            ));
        }
        let tenant = run.try_get::<String, _>("tenant")?;
        let source = db::query("SELECT created_at, updated_at FROM runs WHERE run_id = ? AND tenant = ? AND agent_ref = ? AND status IN ('succeeded', 'failed')")
            .bind(result.source_run_id.to_string()).bind(&tenant).bind(result.target_agent)
            .fetch_optional(&mut tx).await?.ok_or_else(|| anyhow!("invalid learning source cursor"))?;
        if parse_ts_field(&source, "updated_at")? != result.source_updated_at {
            return Err(anyhow!(
                "learning source cursor timestamp does not match its run"
            ));
        }
        let target = db::query("SELECT metadata_json, spec_json, created_at, updated_at FROM agents WHERE tenant = ? AND name = ?")
            .bind(&tenant).bind(result.target_agent).fetch_optional(&mut tx).await?.map(row_to_agent).transpose()?;
        let head = db::query(
            "SELECT active_revision, source_run_id FROM behavior_heads WHERE tenant = ? AND agent_ref = ?",
        )
        .bind(&tenant)
        .bind(result.target_agent)
        .fetch_optional(&mut tx)
        .await?;
        let previous_source_run_id = head
            .as_ref()
            .map(|row| row.try_get::<Option<String>, _>("source_run_id"))
            .transpose()?
            .flatten();
        let active = head
            .map(|row| row.try_get::<Option<i64>, _>("active_revision"))
            .transpose()?
            .flatten()
            .map(|n| n as u64);
        let coordinator_created_at = parse_ts_field(&run, "created_at")?;
        let source_created_at = parse_ts_field(&source, "created_at")?;
        let stale = active != result.expected_revision
            || !target.as_ref().is_some_and(|agent| {
                is_foreground_agent(agent)
                    && agent.spec == *result.expected_spec
                    && coordinator_created_at >= agent.created_at
                    && source_created_at >= agent.created_at
            });
        let outcome = if stale {
            "stale"
        } else if result.promote {
            "promoted"
        } else {
            "rejected"
        };
        let revision = db::query_scalar::<i64>("SELECT COALESCE(MAX(revision), 0) + 1 FROM behavior_revisions WHERE tenant = ? AND agent_ref = ?")
            .bind(&tenant).bind(result.target_agent).fetch_optional(&mut tx).await?.unwrap_or(1);
        let now = Utc::now();
        db::query("INSERT INTO behavior_revisions (tenant, agent_ref, revision, parent_revision, spec_json, instructions, outcome, source_run_id, report_json, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
            .bind(&tenant).bind(result.target_agent).bind(revision).bind(result.expected_revision.map(|n| n as i64))
            .bind(serde_json::to_string(result.expected_spec)?).bind(result.instructions).bind(outcome)
            .bind(run_id.to_string()).bind(result.report.to_string()).bind(now.to_rfc3339()).execute(&mut tx).await?;
        let mut source_cursor_changed = false;
        if !stale {
            db::query("INSERT INTO behavior_heads (tenant, agent_ref) VALUES (?, ?) ON CONFLICT(tenant, agent_ref) DO NOTHING")
                .bind(&tenant).bind(result.target_agent).execute(&mut tx).await?;
            if result.promote {
                db::query("UPDATE behavior_heads SET active_revision = ? WHERE tenant = ? AND agent_ref = ?")
                    .bind(revision).bind(&tenant).bind(result.target_agent).execute(&mut tx).await?;
            }
            // A concurrent rejected cycle must not move consumption backwards.
            source_cursor_changed = db::query("UPDATE behavior_heads SET source_updated_at = ?, source_run_id = ? WHERE tenant = ? AND agent_ref = ? AND (source_updated_at IS NULL OR source_updated_at < ? OR (source_updated_at = ? AND source_run_id < ?))")
                .bind(result.source_updated_at.to_rfc3339()).bind(result.source_run_id.to_string())
                .bind(&tenant).bind(result.target_agent).bind(result.source_updated_at.to_rfc3339())
                .bind(result.source_updated_at.to_rfc3339()).bind(result.source_run_id.to_string())
                .execute(&mut tx).await?.rows_affected() > 0;
        }
        let output = json!({"status":outcome,"outcome":outcome,"target_agent":result.target_agent,"revision":revision,"report":result.report});
        let changed = db::query("UPDATE runs SET output_json = ?, error = NULL, status = 'succeeded', updated_at = ? WHERE run_id = ? AND status = 'running'")
            .bind(output.to_string()).bind(now.to_rfc3339()).bind(run_id.to_string()).execute(&mut tx).await?.rows_affected();
        if changed != 1 {
            return Err(anyhow!("learning run was cancelled during finalization"));
        }
        for (kind, payload) in [
            ("output", output),
            ("status", json!({"status":"succeeded"})),
        ] {
            Self::append_run_trace(&mut tx, &tenant, run_id, kind, &payload, &now.to_rfc3339())
                .await?;
        }
        audit::record(&mut tx, AuditInput::new(
            Some(&tenant), "behavior.finish", "behavior_policy", Some(result.target_agent), outcome,
            json!({
                "target_agent":result.target_agent,"revision":revision,
                "parent_revision":result.expected_revision,"previous_revision":active,
                "source_run_id":result.source_run_id,"source_cursor_advanced":source_cursor_changed,
                "source_count":result.report.get("source_runs").and_then(serde_json::Value::as_array).map(Vec::len),
                "evaluation_count":result.report.get("evaluations").and_then(serde_json::Value::as_array).map(Vec::len),
            }),
        ).for_run(run_id)).await?;
        if source_cursor_changed {
            audit::record(&mut tx, AuditInput::new(
                Some(&tenant), "behavior.source_cursor", "behavior_policy", Some(result.target_agent), "advanced",
                json!({"target_agent":result.target_agent,"previous_source_run_id":previous_source_run_id,"source_run_id":result.source_run_id}),
            ).for_run(run_id)).await?;
        }
        audit::record(&mut tx, AuditInput::new(
            Some(&tenant), "run.succeed", "run", Some(&run_id.to_string()), "succeeded",
            json!({"status_before":"running","status_after":"succeeded","learning_outcome":outcome}),
        ).for_run(run_id)).await?;
        tx.commit().await?;
        Ok(BehaviorRevision {
            tenant,
            agent_ref: result.target_agent.into(),
            revision: revision as u64,
            parent_revision: result.expected_revision,
            instructions: result.instructions.into(),
            outcome: outcome.into(),
            source_run_id: run_id,
            report: result.report.clone(),
            created_at: now,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use agentd_api::{AgentLimits, ResourceMeta};
    use tempfile::TempDir;

    async fn fixture() -> (TempDir, AgentdStore, AgentSpec) {
        let dir = TempDir::new().unwrap();
        let store = AgentdStore::new(dir.path().join("agentd.db").to_str().unwrap())
            .await
            .unwrap();
        let spec = AgentSpec {
            allowed_families: Some(vec![]),
            limits: AgentLimits {
                timeout_ms: 10000,
                max_steps: 8,
            },
            system_prompt: Some("Owner persona".into()),
            model: None,
            temperature: None,
            max_tokens: None,
            context_window: Some(0),
        };
        for tenant in ["one", "two"] {
            store.create_tenant(tenant, &json!({})).await.unwrap();
            for name in ["bot", BEHAVIOR_LEARNER_AGENT] {
                store
                    .apply_agent(&AgentResource {
                        metadata: ResourceMeta {
                            name: name.into(),
                            tenant: tenant.into(),
                            labels: BTreeMap::new(),
                        },
                        spec: spec.clone(),
                    })
                    .await
                    .unwrap();
            }
        }
        (dir, store, spec)
    }

    async fn source(store: &AgentdStore, tenant: &str) -> AgentRun {
        source_with_scope(store, tenant, "chat").await
    }

    async fn source_with_scope(store: &AgentdStore, tenant: &str, scope: &str) -> AgentRun {
        source_for_agent(store, tenant, "bot", scope).await
    }

    async fn source_for_agent(
        store: &AgentdStore,
        tenant: &str,
        agent_ref: &str,
        scope: &str,
    ) -> AgentRun {
        let id = store
            .submit_run(NewRun {
                tenant,
                name: "source",
                agent_ref,
                scope,
                source: "test",
                input: &json!({"text":"compute 2+2"}),
                request_id: None,
                schedule_name: None,
                delivery_destination: None,
            })
            .await
            .unwrap();
        assert_eq!(
            store.claim_next_run().await.unwrap().unwrap().run.run_id,
            id
        );
        store
            .finalize_run_success(id, &json!({"reply":"4"}), None)
            .await
            .unwrap();
        store.get_run(id).await.unwrap().unwrap()
    }

    async fn cycle(store: &AgentdStore, tenant: &str) -> Uuid {
        let id = store
            .submit_run(NewRun {
                tenant,
                name: "learn",
                agent_ref: BEHAVIOR_LEARNER_AGENT,
                scope: "behavior-learning/bot",
                source: "test",
                input: &json!({"target_agent":"bot"}),
                request_id: None,
                schedule_name: None,
                delivery_destination: None,
            })
            .await
            .unwrap();
        assert_eq!(
            store.claim_next_run().await.unwrap().unwrap().run.run_id,
            id
        );
        id
    }

    async fn finish(
        store: &AgentdStore,
        run_id: Uuid,
        spec: &AgentSpec,
        parent: Option<u64>,
        source: &AgentRun,
        promote: bool,
    ) -> Result<BehaviorRevision> {
        store
            .finish_behavior_learning(
                run_id,
                BehaviorLearningResult {
                    target_agent: "bot",
                    expected_spec: spec,
                    expected_revision: parent,
                    instructions: "Verify tool arguments before calling.",
                    report: &json!({"gain":0.2}),
                    promote,
                    source_updated_at: source.updated_at,
                    source_run_id: source.run_id,
                },
            )
            .await
    }

    fn readiness_options() -> BehaviorLearningOptions {
        BehaviorLearningOptions {
            target_agent: "bot".into(),
            min_samples: 4,
            max_samples: 4,
            ..BehaviorLearningOptions::default()
        }
    }

    #[tokio::test]
    async fn behavior_readiness_requires_new_samples_and_independent_scopes() {
        let (_dir, store, _spec) = fixture().await;
        let options = readiness_options();
        let empty = store
            .behavior_learning_readiness("one", &options)
            .await
            .unwrap();
        assert!(!empty.ready);
        assert_eq!(empty.reason, "insufficient_samples");
        assert_eq!(empty.source_runs, 0);
        for _ in 0..3 {
            source(&store, "one").await;
        }
        let short = store
            .behavior_learning_readiness("one", &options)
            .await
            .unwrap();
        assert_eq!(short.reason, "insufficient_samples");
        assert_eq!(short.source_runs, 3);
        assert_eq!(short.independent_scopes, 1);
        source(&store, "one").await;
        let single_scope = store
            .behavior_learning_readiness("one", &options)
            .await
            .unwrap();
        assert_eq!(single_scope.reason, "insufficient_independent_scopes");
        assert_eq!(single_scope.source_runs, 4);
        source_with_scope(&store, "one", "independent").await;
        let ready = store
            .behavior_learning_readiness("one", &options)
            .await
            .unwrap();
        assert!(ready.ready);
        assert_eq!(ready.reason, "ready");
        assert_eq!(ready.source_runs, 5);
        assert_eq!(ready.independent_scopes, 2);
        assert_eq!(
            store
                .behavior_learning_readiness("two", &options)
                .await
                .unwrap()
                .source_runs,
            0
        );
    }

    #[tokio::test]
    async fn behavior_readiness_only_blocks_pending_cycles_for_the_same_tenant_and_target() {
        let (_dir, store, spec) = fixture().await;
        let options = readiness_options();
        for index in 0..4 {
            source_with_scope(&store, "one", &format!("chat/{index}")).await;
        }
        // A running cycle for another tenant must not occupy this target lane.
        let other_tenant = cycle(&store, "two").await;
        assert!(
            store
                .behavior_learning_readiness("one", &options)
                .await
                .unwrap()
                .ready
        );
        store
            .apply_agent(&AgentResource {
                metadata: ResourceMeta {
                    tenant: "one".into(),
                    name: "other-bot".into(),
                    labels: BTreeMap::new(),
                },
                spec,
            })
            .await
            .unwrap();
        let other_target = store
            .submit_run(NewRun {
                tenant: "one",
                name: "other-target-cycle",
                agent_ref: BEHAVIOR_LEARNER_AGENT,
                scope: "ignored",
                source: "api",
                input: &json!({"target_agent":"other-bot"}),
                request_id: None,
                schedule_name: None,
                delivery_destination: None,
            })
            .await
            .unwrap();
        assert!(
            store
                .behavior_learning_readiness("one", &options)
                .await
                .unwrap()
                .ready
        );
        let same_target = store
            .submit_run(NewRun {
                tenant: "one",
                name: "same-target-cycle",
                agent_ref: BEHAVIOR_LEARNER_AGENT,
                scope: "ignored",
                source: "api",
                input: &json!({"target_agent":"bot"}),
                request_id: None,
                schedule_name: None,
                delivery_destination: None,
            })
            .await
            .unwrap();
        let queued = store
            .behavior_learning_readiness("one", &options)
            .await
            .unwrap();
        assert!(!queued.ready);
        assert_eq!(queued.reason, "already_pending");
        store
            .cancel_run_request(other_target, "fixture complete")
            .await
            .unwrap();
        assert_eq!(
            store.claim_next_run().await.unwrap().unwrap().run.run_id,
            same_target
        );
        assert_eq!(
            store
                .behavior_learning_readiness("one", &options)
                .await
                .unwrap()
                .reason,
            "already_pending"
        );
        store
            .fail_run(same_target, "fixture complete")
            .await
            .unwrap();
        assert!(
            store
                .behavior_learning_readiness("one", &options)
                .await
                .unwrap()
                .ready
        );
        store
            .fail_run(other_tenant, "fixture complete")
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn behavior_readiness_excludes_consumed_and_previous_agent_lifecycle_sources() {
        let (_dir, store, spec) = fixture().await;
        let options = readiness_options();
        for index in 0..4 {
            source_with_scope(&store, "one", &format!("chat/{index}")).await;
        }
        let latest = store
            .list_behavior_source_runs("one", "bot", 20)
            .await
            .unwrap()
            .remove(0);
        let coordinator = cycle(&store, "one").await;
        finish(&store, coordinator, &spec, None, &latest, true)
            .await
            .unwrap();
        let consumed = store
            .behavior_learning_readiness("one", &options)
            .await
            .unwrap();
        assert_eq!(consumed.reason, "insufficient_samples");
        assert_eq!(consumed.source_runs, 0);
        store.delete_agent("one", "bot").await.unwrap();
        assert_eq!(
            store
                .behavior_learning_readiness("one", &options)
                .await
                .unwrap()
                .reason,
            "target_not_found"
        );
        let mut agent = AgentResource {
            metadata: ResourceMeta {
                tenant: "one".into(),
                name: "bot".into(),
                labels: BTreeMap::new(),
            },
            spec,
        };
        store.apply_agent(&agent).await.unwrap();
        let recreated = store
            .behavior_learning_readiness("one", &options)
            .await
            .unwrap();
        assert_eq!(recreated.reason, "insufficient_samples");
        assert_eq!(recreated.source_runs, 0);
        agent
            .metadata
            .labels
            .insert("agentd.system".into(), "true".into());
        store.apply_agent(&agent).await.unwrap();
        assert_eq!(
            store
                .behavior_learning_readiness("one", &options)
                .await
                .unwrap()
                .reason,
            "system_target"
        );
        assert_eq!(
            store
                .behavior_learning_readiness("missing", &options)
                .await
                .unwrap()
                .reason,
            "target_not_found"
        );
        assert!(store
            .behavior_learning_readiness("one", &BehaviorLearningOptions::default())
            .await
            .is_err());
    }

    #[tokio::test]
    async fn promotion_is_tenant_scoped_pinned_and_separate_from_memory() {
        let (_dir, store, spec) = fixture().await;
        let source_one = source(&store, "one").await;
        let source_two = source(&store, "two").await;
        let cycle_id = cycle(&store, "one").await;
        let revision = finish(&store, cycle_id, &spec, None, &source_one, true)
            .await
            .unwrap();
        assert_eq!(revision.outcome, "promoted");
        assert_eq!(revision.source_run_id, cycle_id);
        assert_eq!(
            store
                .list_behavior_source_runs("one", "bot", 12)
                .await
                .unwrap()
                .len(),
            0
        );
        assert_eq!(
            store
                .list_behavior_source_runs("two", "bot", 12)
                .await
                .unwrap()[0]
                .run_id,
            source_two.run_id
        );
        assert!(store
            .get_behavior_snapshot("two", "bot")
            .await
            .unwrap()
            .unwrap()
            .active_revision
            .is_none());
        assert!(store
            .list_behavior_revisions("two", "bot", 10)
            .await
            .unwrap()
            .is_empty());
        for table in ["memory", "contexts", "artifacts", "deliveries"] {
            assert_eq!(
                db::query_scalar::<i64>(&format!("SELECT COUNT(*) FROM {table}"))
                    .fetch_optional(&store.pool)
                    .await
                    .unwrap(),
                Some(0)
            );
        }
        let id = store
            .submit_run(NewRun {
                tenant: "one",
                name: "foreground",
                agent_ref: "bot",
                scope: "chat",
                source: "test",
                input: &json!({"text":"next"}),
                request_id: None,
                schedule_name: None,
                delivery_destination: None,
            })
            .await
            .unwrap();
        let assignment = store.claim_next_run().await.unwrap().unwrap();
        assert_eq!(assignment.run.run_id, id);
        assert_eq!(
            assignment.agent_system_prompt.as_deref(),
            Some("Owner persona")
        );
        assert_eq!(assignment.agent_behavior_revision, Some(1));
        assert_eq!(
            assignment.agent_learned_instructions.as_deref(),
            Some(revision.instructions.as_str())
        );
        assert!(store.clear_behavior_policy("one", "bot").await.unwrap());
        assert!(store
            .get_behavior_snapshot("one", "bot")
            .await
            .unwrap()
            .unwrap()
            .active_revision
            .is_none());
        assert_eq!(assignment.agent_behavior_revision, Some(1));
        assert_eq!(
            store
                .list_behavior_revisions("one", "bot", 10)
                .await
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            store
                .get_run(cycle_id)
                .await
                .unwrap()
                .unwrap()
                .output
                .unwrap()["status"],
            "promoted"
        );
        let logs = store.list_run_log(cycle_id).await.unwrap();
        assert_eq!(
            logs.iter()
                .map(|entry| entry.kind.as_str())
                .collect::<Vec<_>>(),
            ["output", "status"]
        );
    }

    #[tokio::test]
    async fn cancelled_or_failed_coordinator_cannot_activate_or_consume() {
        let (_dir, store, spec) = fixture().await;
        let source = source(&store, "one").await;
        let id = cycle(&store, "one").await;
        store.cancel_run_request(id, "stop").await.unwrap();
        assert!(finish(&store, id, &spec, None, &source, true)
            .await
            .is_err());
        let id = cycle(&store, "one").await;
        store.fail_run(id, "model unavailable").await.unwrap();
        assert!(finish(&store, id, &spec, None, &source, true)
            .await
            .is_err());
        assert!(store
            .list_behavior_revisions("one", "bot", 10)
            .await
            .unwrap()
            .is_empty());
        assert_eq!(
            store
                .list_behavior_source_runs("one", "bot", 10)
                .await
                .unwrap()
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn changed_spec_and_parent_record_stale_without_consuming() {
        let (_dir, store, spec) = fixture().await;
        let first = source(&store, "one").await;
        let id = cycle(&store, "one").await;
        finish(&store, id, &spec, None, &first, true).await.unwrap();
        let second = source(&store, "one").await;
        let id = cycle(&store, "one").await;
        assert_eq!(
            finish(&store, id, &spec, None, &second, true)
                .await
                .unwrap()
                .outcome,
            "stale"
        );
        assert_eq!(
            store
                .list_behavior_source_runs("one", "bot", 10)
                .await
                .unwrap()
                .len(),
            1
        );
        let id = cycle(&store, "one").await;
        let mut changed = store.get_agent("one", "bot").await.unwrap().unwrap();
        changed.spec.system_prompt = Some("Updated owner persona".into());
        store
            .apply_agent(&AgentResource {
                metadata: changed.metadata,
                spec: changed.spec,
            })
            .await
            .unwrap();
        assert!(store
            .get_behavior_snapshot("one", "bot")
            .await
            .unwrap()
            .unwrap()
            .active_revision
            .is_none());
        assert_eq!(
            finish(&store, id, &spec, Some(1), &second, true)
                .await
                .unwrap()
                .outcome,
            "stale"
        );
        assert_eq!(
            store
                .list_behavior_source_runs("one", "bot", 10)
                .await
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            store
                .list_behavior_revisions("one", "bot", 10)
                .await
                .unwrap()
                .len(),
            3
        );
    }

    #[tokio::test]
    async fn recreated_agent_cannot_inherit_sources_or_inflight_policy() {
        let (_dir, store, spec) = fixture().await;
        let first = source(&store, "one").await;
        let id = cycle(&store, "one").await;
        finish(&store, id, &spec, None, &first, true).await.unwrap();
        let previous = store.get_agent("one", "bot").await.unwrap().unwrap();
        let resource = AgentResource {
            metadata: previous.metadata.clone(),
            spec: previous.spec.clone(),
        };
        // An idempotent apply retains the incarnation and its active policy.
        store.apply_agent(&resource).await.unwrap();
        let reapplied = store
            .get_behavior_snapshot("one", "bot")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(reapplied.agent.created_at, previous.created_at);
        assert_eq!(reapplied.active_revision.unwrap().revision, 1);

        store.clear_behavior_policy("one", "bot").await.unwrap();
        let old_source = source(&store, "one").await;
        let old_cycle = cycle(&store, "one").await;
        assert!(store.delete_agent("one", "bot").await.unwrap());
        store.apply_agent(&resource).await.unwrap();
        let recreated = store
            .get_behavior_snapshot("one", "bot")
            .await
            .unwrap()
            .unwrap();
        assert!(recreated.agent.created_at > previous.created_at);
        assert!(recreated.active_revision.is_none());
        assert!(store
            .list_behavior_source_runs("one", "bot", 10)
            .await
            .unwrap()
            .is_empty());
        assert_eq!(
            db::query_scalar::<i64>(
                "SELECT COUNT(*) FROM behavior_heads WHERE tenant = 'one' AND agent_ref = 'bot'"
            )
            .fetch_optional(&store.pool)
            .await
            .unwrap(),
            Some(0)
        );
        assert_eq!(
            finish(&store, old_cycle, &spec, None, &old_source, true)
                .await
                .unwrap()
                .outcome,
            "stale"
        );
        assert!(store
            .get_behavior_snapshot("one", "bot")
            .await
            .unwrap()
            .unwrap()
            .active_revision
            .is_none());

        let new_source = source(&store, "one").await;
        let candidates = store
            .list_behavior_source_runs("one", "bot", 10)
            .await
            .unwrap();
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].run_id, new_source.run_id);
        let new_cycle = cycle(&store, "one").await;
        assert_eq!(
            finish(&store, new_cycle, &spec, None, &new_source, true)
                .await
                .unwrap()
                .outcome,
            "promoted"
        );
        // Old audit rows remain immutable and do not become the new policy.
        let history = store
            .list_behavior_revisions("one", "bot", 10)
            .await
            .unwrap();
        assert_eq!(
            history
                .iter()
                .map(|r| r.outcome.as_str())
                .collect::<Vec<_>>(),
            ["promoted", "stale", "promoted"]
        );
    }

    #[tokio::test]
    async fn rejection_consumes_sources_and_foreign_source_cannot_publish() {
        let (_dir, store, spec) = fixture().await;
        let one = source(&store, "one").await;
        let two = source(&store, "two").await;
        let id = cycle(&store, "one").await;
        assert!(finish(&store, id, &spec, None, &two, true).await.is_err());
        assert_eq!(
            finish(&store, id, &spec, None, &one, false)
                .await
                .unwrap()
                .outcome,
            "rejected"
        );
        assert!(store
            .get_behavior_snapshot("one", "bot")
            .await
            .unwrap()
            .unwrap()
            .active_revision
            .is_none());
        assert!(store
            .list_behavior_source_runs("one", "bot", 10)
            .await
            .unwrap()
            .is_empty());
    }

    #[tokio::test]
    async fn source_cursor_handles_timestamp_ties_and_excludes_cancelled_runs() {
        let (_dir, store, spec) = fixture().await;
        let mut sources = [source(&store, "one").await, source(&store, "one").await];
        let tied_time = Utc::now();
        for source in &mut sources {
            db::query("UPDATE runs SET updated_at = ? WHERE run_id = ?")
                .bind(tied_time.to_rfc3339())
                .bind(source.run_id.to_string())
                .execute(&store.pool)
                .await
                .unwrap();
            source.updated_at = tied_time;
        }
        sources.sort_by_key(|source| source.run_id);
        // Failed foreground turns are useful evidence too.
        db::query("UPDATE runs SET status = 'failed' WHERE run_id = ?")
            .bind(sources[1].run_id.to_string())
            .execute(&store.pool)
            .await
            .unwrap();
        let cancelled = store
            .submit_run(NewRun {
                tenant: "one",
                name: "cancelled",
                agent_ref: "bot",
                scope: "chat",
                source: "test",
                input: &json!({}),
                request_id: None,
                schedule_name: None,
                delivery_destination: None,
            })
            .await
            .unwrap();
        store.cancel_run_request(cancelled, "cancel").await.unwrap();
        let recent = store
            .list_behavior_source_runs("one", "bot", 1)
            .await
            .unwrap();
        assert_eq!(recent.len(), 1);
        assert_eq!(recent[0].run_id, sources[1].run_id);
        assert_eq!(recent[0].status, AgentRunStatus::Failed);
        let id = cycle(&store, "one").await;
        finish(&store, id, &spec, None, &sources[0], false)
            .await
            .unwrap();
        let remaining = store
            .list_behavior_source_runs("one", "bot", 10)
            .await
            .unwrap();
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].run_id, sources[1].run_id);
    }

    #[tokio::test]
    async fn system_label_disables_policy_and_source_selection() {
        let (_dir, store, spec) = fixture().await;
        let source = source(&store, "one").await;
        let id = cycle(&store, "one").await;
        finish(&store, id, &spec, None, &source, true)
            .await
            .unwrap();
        let mut agent = store.get_agent("one", "bot").await.unwrap().unwrap();
        agent
            .metadata
            .labels
            .insert("agentd.system".into(), "true".into());
        store
            .apply_agent(&AgentResource {
                metadata: agent.metadata,
                spec: agent.spec,
            })
            .await
            .unwrap();
        assert!(store
            .get_behavior_snapshot("one", "bot")
            .await
            .unwrap()
            .unwrap()
            .active_revision
            .is_none());
        assert!(store
            .list_behavior_source_runs("one", "bot", 10)
            .await
            .unwrap()
            .is_empty());
    }

    #[tokio::test]
    async fn learning_submissions_validate_target_and_canonicalize_scope() {
        let (_dir, store, _) = fixture().await;
        for target in ["*", "missing", BEHAVIOR_LEARNER_AGENT] {
            assert!(store
                .submit_run(NewRun {
                    tenant: "one",
                    name: "learn",
                    agent_ref: BEHAVIOR_LEARNER_AGENT,
                    scope: "arbitrary",
                    source: "api",
                    input: &json!({"target_agent":target}),
                    request_id: None,
                    schedule_name: None,
                    delivery_destination: None,
                })
                .await
                .is_err());
        }
        for (scope, input) in [
            ("caller-scope-a", json!({"target_agent":"bot"})),
            (
                "caller-scope-b",
                json!({"activation":"schedule","input":{"target_agent":"bot"}}),
            ),
        ] {
            let id = store
                .submit_run(NewRun {
                    tenant: "one",
                    name: "learn",
                    agent_ref: BEHAVIOR_LEARNER_AGENT,
                    scope,
                    source: "api",
                    input: &input,
                    request_id: None,
                    schedule_name: None,
                    delivery_destination: None,
                })
                .await
                .unwrap();
            assert_eq!(
                store.get_run(id).await.unwrap().unwrap().scope,
                "behavior-learning/bot"
            );
        }
        assert!(store.claim_next_run().await.unwrap().is_some());
        assert!(store.claim_next_run().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn behavior_schedule_fanout_excludes_system_and_serializes_targets() {
        let (_dir, store, spec) = fixture().await;
        for (name, system) in [("other", false), ("hidden", true), ("system/helper", false)] {
            store
                .apply_agent(&AgentResource {
                    metadata: ResourceMeta {
                        name: name.into(),
                        tenant: "one".into(),
                        labels: if system {
                            BTreeMap::from([("agentd.system".into(), "true".into())])
                        } else {
                            BTreeMap::new()
                        },
                    },
                    spec: spec.clone(),
                })
                .await
                .unwrap();
        }
        for agent in ["bot", "other", "hidden", "system/helper"] {
            for index in 0..4 {
                source_for_agent(&store, "one", agent, &format!("chat/{}", index % 2)).await;
            }
        }
        let now = Utc::now();
        store
            .put_schedule(
                "one",
                BEHAVIOR_LEARNING_SCHEDULE,
                &ScheduleSpec {
                    agent_ref: BEHAVIOR_LEARNER_AGENT.into(),
                    scope: "behavior-learning/*".into(),
                    payload: json!({"target_agent":"*","min_samples":4,"max_samples":4}),
                    delivery: None,
                    at: Some(now + ChronoDuration::minutes(1)),
                    cron: None,
                    timezone: None,
                    enabled: true,
                },
            )
            .await
            .unwrap();
        let mut all = Vec::new();
        for attempt in 0..2 {
            db::query("UPDATE schedules SET next_trigger_at = ? WHERE tenant = 'one'")
                .bind((now - ChronoDuration::seconds(1)).to_rfc3339())
                .execute(&store.pool)
                .await
                .unwrap();
            let triggered = store.trigger_due_schedules(now, 10).await.unwrap();
            if attempt == 1 {
                assert!(
                    triggered.is_empty(),
                    "queued target cycles must not accumulate"
                );
            }
            all.extend(triggered);
        }
        assert_eq!(all.len(), 2);
        for id in all {
            let run = store.get_run(id).await.unwrap().unwrap();
            let target = run.input["input"]["target_agent"].as_str().unwrap();
            assert!(["bot", "other"].contains(&target));
            assert_eq!(run.scope, format!("behavior-learning/{target}"));
            assert_eq!(run.tenant, "one");
        }
        let first = store.claim_next_run().await.unwrap().unwrap();
        let second = store.claim_next_run().await.unwrap().unwrap();
        assert_ne!(first.run.scope, second.run.scope);
        assert!(store.claim_next_run().await.unwrap().is_none());
        assert!(store
            .submit_run(NewRun {
                tenant: "one",
                name: "bad",
                agent_ref: BEHAVIOR_LEARNER_AGENT,
                scope: "x",
                source: "test",
                input: &json!({"target_agent":"bot"}),
                request_id: None,
                schedule_name: None,
                delivery_destination: Some("telegram:1"),
            })
            .await
            .is_err());
    }

    #[tokio::test]
    async fn behavior_schedule_validates_options_and_normalizes_wildcard_defaults() {
        let (_dir, store, spec) = fixture().await;
        store
            .apply_agent(&AgentResource {
                metadata: ResourceMeta {
                    name: "hidden".into(),
                    tenant: "one".into(),
                    labels: BTreeMap::from([("agentd.system".into(), "true".into())]),
                },
                spec,
            })
            .await
            .unwrap();
        let now = Utc::now();
        let mut schedule = ScheduleSpec {
            agent_ref: BEHAVIOR_LEARNER_AGENT.into(),
            scope: "caller-scope".into(),
            payload: json!({}),
            delivery: None,
            at: None,
            cron: Some("0 4 * * *".into()),
            timezone: Some("UTC".into()),
            enabled: true,
        };
        for payload in [
            json!({"unexpected":true}),
            json!({"max_model_calls":0}),
            json!({"min_samples":2}),
            json!({"target_agent":"missing"}),
            json!({"target_agent":"system/helper"}),
            json!({"target_agent":"hidden"}),
        ] {
            schedule.payload = payload;
            assert!(store
                .put_schedule("one", BEHAVIOR_LEARNING_SCHEDULE, &schedule)
                .await
                .is_err());
            assert!(store
                .get_schedule("one", BEHAVIOR_LEARNING_SCHEDULE)
                .await
                .unwrap()
                .is_none());
        }
        for payload in [json!({}), json!({"target_agent":"*"})] {
            schedule.payload = payload;
            assert!(store
                .put_schedule("one", "custom-learning", &schedule)
                .await
                .is_err());
        }
        schedule.payload = json!({});
        store
            .put_schedule("one", BEHAVIOR_LEARNING_SCHEDULE, &schedule)
            .await
            .unwrap();
        let stored = store
            .get_schedule("one", BEHAVIOR_LEARNING_SCHEDULE)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored.spec.payload["target_agent"], "*");
        assert_eq!(stored.spec.payload["min_samples"], 8);
        assert_eq!(stored.spec.scope, "behavior-learning/*");
        schedule.payload = json!({"min_samples":4,"max_samples":4});
        store
            .put_schedule("one", BEHAVIOR_LEARNING_SCHEDULE, &schedule)
            .await
            .unwrap();
        db::query("UPDATE schedules SET next_trigger_at = ? WHERE tenant = 'one'")
            .bind((now - ChronoDuration::seconds(1)).to_rfc3339())
            .execute(&store.pool)
            .await
            .unwrap();
        let triggered = store.trigger_due_schedules(now, 10).await.unwrap();
        assert!(
            triggered.is_empty(),
            "an empty tenant must not create a learning run"
        );
        let after_skip = store
            .get_schedule("one", BEHAVIOR_LEARNING_SCHEDULE)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(after_skip.last_triggered_at, Some(now));
        assert!(after_skip.next_trigger_at.unwrap() > now);
        assert!(after_skip.last_run_id.is_none());
        for index in 0..4 {
            source_with_scope(&store, "one", &format!("chat/{}", index % 2)).await;
        }
        let triggered = store
            .trigger_due_schedules(after_skip.next_trigger_at.unwrap(), 10)
            .await
            .unwrap();
        assert_eq!(
            triggered.len(),
            1,
            "new independent source runs make the next tick eligible"
        );
        assert_eq!(
            store.get_run(triggered[0]).await.unwrap().unwrap().input["input"]["target_agent"],
            "bot"
        );

        schedule.payload = json!({"target_agent":"bot"});
        store
            .put_schedule("one", "custom-learning", &schedule)
            .await
            .unwrap();
        let concrete = store
            .get_schedule("one", "custom-learning")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(concrete.spec.scope, "behavior-learning/bot");
        assert_eq!(concrete.spec.payload["max_tokens"], 2048);
    }

    #[tokio::test]
    async fn schema_eight_migrates_without_resetting_memory_or_delivery() {
        let (dir, store, _) = fixture().await;
        let source = source(&store, "one").await;
        let mut embedding = vec![0.; MEMORY_EMBEDDING_DIM];
        embedding[0] = 1.;
        store
            .put_memory("one", "bot", "fact", "preserve", &embedding)
            .await
            .unwrap();
        for table in ["behavior_heads", "behavior_revisions"] {
            db::query(&format!("DROP TABLE {table}"))
                .execute(&store.pool)
                .await
                .unwrap();
        }
        db::query("PRAGMA user_version = 8")
            .execute(&store.pool)
            .await
            .unwrap();
        drop(store);
        let migrated = AgentdStore::new(dir.path().join("agentd.db").to_str().unwrap())
            .await
            .unwrap();
        assert_eq!(
            db::query_scalar::<i64>("PRAGMA user_version")
                .fetch_optional(&migrated.pool)
                .await
                .unwrap(),
            Some(11)
        );
        assert!(migrated
            .get_memory("one", "bot", "fact")
            .await
            .unwrap()
            .is_some());
        assert_eq!(
            migrated
                .get_run(source.run_id)
                .await
                .unwrap()
                .unwrap()
                .output,
            source.output
        );
        assert!(migrated
            .get_behavior_snapshot("one", "bot")
            .await
            .unwrap()
            .unwrap()
            .active_revision
            .is_none());
    }
}
