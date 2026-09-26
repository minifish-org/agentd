use super::*;

pub(super) const SCHEMA_STATEMENTS: [&str; 3] = [
    "CREATE TABLE IF NOT EXISTS memory_maintenance_state (
        tenant TEXT NOT NULL,
        namespace TEXT NOT NULL,
        external_revision INTEGER NOT NULL DEFAULT 0,
        consumed_revision INTEGER NOT NULL DEFAULT 0,
        PRIMARY KEY (tenant, namespace)
    )",
    "CREATE TABLE IF NOT EXISTS memory_maintenance_runs (
        run_id TEXT PRIMARY KEY NOT NULL,
        tenant TEXT NOT NULL,
        namespace TEXT NOT NULL,
        start_revision INTEGER NOT NULL
    )",
    "INSERT OR IGNORE INTO memory_maintenance_state
        (tenant, namespace, external_revision, consumed_revision)
     SELECT tenant, namespace, 1, 0 FROM memory GROUP BY tenant, namespace",
];

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct MemoryMaintenanceReadiness {
    pub ready: bool,
    pub reason: Option<String>,
    pub entries: usize,
    pub external_revision: u64,
}

/// Accept both direct maintenance options and the scheduled-run wrapper.
pub fn memory_maintenance_min_entries(payload: &serde_json::Value) -> Result<usize> {
    let Some(value) = payload
        .get("min_entries")
        .or_else(|| payload.pointer("/input/min_entries"))
    else {
        return Ok(5);
    };
    let value = value
        .as_u64()
        .filter(|value| (2..=10_000).contains(value))
        .ok_or_else(|| anyhow!("memory maintenance min_entries must be between 2 and 10000"))?;
    Ok(value as usize)
}

pub(super) fn maintenance_namespace(input: &serde_json::Value) -> Result<String> {
    let namespace = input
        .get("namespace")
        .or_else(|| input.pointer("/input/namespace"))
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| anyhow!("memory maintainer input requires a namespace"))?;
    let namespace = normalize_memory_component(namespace, "namespace")?;
    if namespace == ALL_MEMORY_NAMESPACES {
        return Err(anyhow!(
            "memory maintenance runs require a concrete namespace"
        ));
    }
    Ok(namespace)
}

async fn readiness(
    tx: &mut db::Transaction,
    tenant: &str,
    namespace: &str,
    min_entries: usize,
) -> Result<MemoryMaintenanceReadiness> {
    if !(2..=10_000).contains(&min_entries) {
        return Err(anyhow!(
            "memory maintenance min_entries must be between 2 and 10000"
        ));
    }
    let entries =
        db::query_scalar::<i64>("SELECT COUNT(*) FROM memory WHERE tenant = ? AND namespace = ?")
            .bind(tenant)
            .bind(namespace)
            .fetch_optional(&mut *tx)
            .await?
            .unwrap_or(0) as usize;
    let row = db::query(
        "SELECT external_revision, consumed_revision FROM memory_maintenance_state WHERE tenant = ? AND namespace = ?",
    )
    .bind(tenant)
    .bind(namespace)
    .fetch_optional(&mut *tx)
    .await?;
    let (external_revision, consumed_revision) = match row {
        Some(row) => (
            row.try_get::<i64, _>("external_revision")?,
            row.try_get::<i64, _>("consumed_revision")?,
        ),
        None => (0, 0),
    };
    let reason = if entries < min_entries {
        Some("too_few_entries".into())
    } else if external_revision <= consumed_revision {
        Some("unchanged".into())
    } else {
        None
    };
    Ok(MemoryMaintenanceReadiness {
        ready: reason.is_none(),
        reason,
        entries,
        external_revision: external_revision as u64,
    })
}

impl AgentdStore {
    /// A cheap snapshot for deciding whether maintenance can add value. Reading
    /// it does not reserve work or consume any external memory changes.
    pub async fn memory_maintenance_readiness(
        &self,
        tenant: &str,
        namespace: &str,
        min_entries: usize,
    ) -> Result<MemoryMaintenanceReadiness> {
        let namespace = normalize_memory_component(namespace, "namespace")?;
        let mut tx = self.pool.begin().await?;
        let state = readiness(&mut tx, tenant, &namespace, min_entries).await?;
        tx.commit().await?;
        Ok(state)
    }

    /// Bind the starting revision to a running maintainer before its first
    /// model/tool call. Success consumes exactly this revision, leaving later
    /// external writes eligible for a subsequent pass.
    pub async fn prepare_memory_maintenance(
        &self,
        run_id: Uuid,
        min_entries: usize,
    ) -> Result<MemoryMaintenanceReadiness> {
        let mut tx = self.pool.begin().await?;
        let run =
            db::query("SELECT tenant, agent_ref, status, input_json FROM runs WHERE run_id = ?")
                .bind(run_id.to_string())
                .fetch_optional(&mut tx)
                .await?
                .ok_or_else(|| anyhow!("memory maintenance run not found"))?;
        if run.try_get::<String, _>("agent_ref")? != MEMORY_MAINTAINER_AGENT
            || run.try_get::<String, _>("status")? != "running"
        {
            return Err(anyhow!(
                "only a running memory maintainer can prepare maintenance"
            ));
        }
        let tenant = run.try_get::<String, _>("tenant")?;
        let input: serde_json::Value =
            serde_json::from_str(&run.try_get::<String, _>("input_json")?)?;
        let namespace = maintenance_namespace(&input)?;
        let state = readiness(&mut tx, &tenant, &namespace, min_entries).await?;
        if state.ready {
            db::query(
                "INSERT OR IGNORE INTO memory_maintenance_runs (run_id, tenant, namespace, start_revision) VALUES (?, ?, ?, ?)",
            )
            .bind(run_id.to_string())
            .bind(&tenant)
            .bind(&namespace)
            .bind(state.external_revision)
            .execute(&mut tx)
            .await?;
        }
        tx.commit().await?;
        Ok(state)
    }
}

/// Call inside every memory mutation's transaction, even for an unchanged put
/// or missing delete, so source validation cannot be bypassed with a no-op.
/// A source run is resolved from durable state; an agent name supplied by a
/// tool argument never grants exemption from change tracking. Only changes to
/// entry IDs or text advance the revision; embeddings and graphs do not.
pub(super) async fn record_memory_change(
    tx: &mut db::Transaction,
    tenant: &str,
    namespace: &str,
    source_run_id: Option<Uuid>,
    content_changed: bool,
) -> Result<()> {
    if let Some(run_id) = source_run_id {
        let run =
            db::query("SELECT tenant, agent_ref, status, input_json FROM runs WHERE run_id = ?")
                .bind(run_id.to_string())
                .fetch_optional(&mut *tx)
                .await?
                .ok_or_else(|| anyhow!("memory write source run not found"))?;
        if run.try_get::<String, _>("tenant")? != tenant
            || run.try_get::<String, _>("status")? != "running"
        {
            return Err(anyhow!(
                "memory write requires a running source in the same tenant"
            ));
        }
        if run.try_get::<String, _>("agent_ref")? == MEMORY_MAINTAINER_AGENT {
            let input: serde_json::Value =
                serde_json::from_str(&run.try_get::<String, _>("input_json")?)?;
            if maintenance_namespace(&input)? != namespace {
                return Err(anyhow!(
                    "memory maintainer cannot write outside its input namespace"
                ));
            }
            let prepared = db::query(
                "SELECT m.start_revision, s.external_revision FROM memory_maintenance_runs m
                 JOIN memory_maintenance_state s ON s.tenant = m.tenant AND s.namespace = m.namespace
                 WHERE m.run_id = ? AND m.tenant = ? AND m.namespace = ?",
            )
            .bind(run_id.to_string())
            .bind(tenant)
            .bind(namespace)
            .fetch_optional(&mut *tx)
            .await?;
            let prepared = prepared.ok_or_else(|| {
                anyhow!("memory maintainer must prepare a ready namespace before writing")
            })?;
            if prepared.try_get::<i64, _>("start_revision")?
                != prepared.try_get::<i64, _>("external_revision")?
            {
                return Err(anyhow!(
                    "memory changed during maintenance; retry with a fresh scan"
                ));
            }
            return Ok(());
        }
    }
    if !content_changed {
        return Ok(());
    }
    db::query(
        "INSERT INTO memory_maintenance_state (tenant, namespace, external_revision, consumed_revision) VALUES (?, ?, 1, 0)
         ON CONFLICT(tenant, namespace) DO UPDATE SET external_revision = external_revision + 1",
    )
    .bind(tenant)
    .bind(namespace)
    .execute(&mut *tx)
    .await?;
    Ok(())
}

/// Part of the successful run finalization transaction. Failed/cancelled runs
/// cannot acknowledge memory changes, and writes after prepare remain pending.
pub(super) async fn checkpoint_success(tx: &mut db::Transaction, run_id: Uuid) -> Result<()> {
    let row = db::query(
        "SELECT m.tenant, m.namespace, m.start_revision, r.agent_ref, r.status
         FROM memory_maintenance_runs m JOIN runs r ON r.run_id = m.run_id AND r.tenant = m.tenant
         WHERE m.run_id = ?",
    )
    .bind(run_id.to_string())
    .fetch_optional(&mut *tx)
    .await?;
    let Some(row) = row else { return Ok(()) };
    if row.try_get::<String, _>("agent_ref")? != MEMORY_MAINTAINER_AGENT
        || !matches!(
            row.try_get::<String, _>("status")?.as_str(),
            "running" | "succeeded"
        )
    {
        return Err(anyhow!(
            "only successful memory maintenance can consume external changes"
        ));
    }
    db::query(
        "UPDATE memory_maintenance_state SET consumed_revision = MAX(consumed_revision, ?)
         WHERE tenant = ? AND namespace = ?",
    )
    .bind(row.try_get::<i64, _>("start_revision")?)
    .bind(row.try_get::<String, _>("tenant")?)
    .bind(row.try_get::<String, _>("namespace")?)
    .execute(&mut *tx)
    .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use agentd_api::{AgentLimits, AgentSpec, ResourceMeta};
    use tempfile::TempDir;

    async fn fixture() -> (TempDir, AgentdStore) {
        let dir = TempDir::new().unwrap();
        let store = AgentdStore::new(dir.path().join("agentd.db").to_str().unwrap())
            .await
            .unwrap();
        for tenant in ["one", "two"] {
            store.create_tenant(tenant, &json!({})).await.unwrap();
            for name in ["bot", MEMORY_MAINTAINER_AGENT] {
                store
                    .apply_agent(&AgentResource {
                        metadata: ResourceMeta {
                            name: name.into(),
                            tenant: tenant.into(),
                            labels: BTreeMap::new(),
                        },
                        spec: AgentSpec {
                            allowed_families: Some(vec![ToolFamily::Memory]),
                            limits: AgentLimits {
                                timeout_ms: 10000,
                                max_steps: 8,
                            },
                            system_prompt: None,
                            model: None,
                            temperature: None,
                            max_tokens: None,
                            context_window: Some(0),
                        },
                    })
                    .await
                    .unwrap();
            }
        }
        (dir, store)
    }

    fn embedding() -> Vec<f32> {
        let mut value = vec![0.; MEMORY_EMBEDDING_DIM];
        value[0] = 1.;
        value
    }

    async fn seed(store: &AgentdStore, namespace: &str, entries: usize) {
        for index in 0..entries {
            store
                .put_memory(
                    "one",
                    namespace,
                    &index.to_string(),
                    &format!("fact {index}"),
                    &embedding(),
                )
                .await
                .unwrap();
        }
    }

    async fn start(store: &AgentdStore, agent_ref: &str, namespace: &str) -> Uuid {
        let id = store
            .submit_run(NewRun {
                tenant: "one",
                name: "test",
                agent_ref,
                scope: &format!("memory-maintenance/{namespace}"),
                source: "test",
                input: &json!({"namespace":namespace}),
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

    async fn mutate(
        store: &AgentdStore,
        tenant: &str,
        namespace: &str,
        actor: Option<Uuid>,
    ) -> Result<()> {
        let mut tx = store.pool.begin().await?;
        record_memory_change(&mut tx, tenant, namespace, actor, true).await?;
        db::query("UPDATE memory SET text = text || ' revised' WHERE tenant = ? AND namespace = ? AND id = '0'")
            .bind(tenant).bind(namespace).execute(&mut tx).await?;
        tx.commit().await?;
        Ok(())
    }

    #[test]
    fn minimum_entries_defaults_and_bounds() {
        assert_eq!(memory_maintenance_min_entries(&json!({})).unwrap(), 5);
        assert_eq!(
            memory_maintenance_min_entries(&json!({"input":{"min_entries":2}})).unwrap(),
            2
        );
        assert_eq!(
            memory_maintenance_min_entries(&json!({"min_entries":10000})).unwrap(),
            10000
        );
        for value in [
            json!(0),
            json!(1),
            json!(-1),
            json!(10001),
            json!(2.5),
            json!("5"),
            json!(null),
        ] {
            assert!(memory_maintenance_min_entries(&json!({"min_entries":value})).is_err());
        }
    }

    #[tokio::test]
    async fn readiness_requires_enough_entries_and_is_tenant_scoped() {
        let (_dir, store) = fixture().await;
        let empty = store
            .memory_maintenance_readiness("one", "profile", 5)
            .await
            .unwrap();
        assert_eq!(empty.reason.as_deref(), Some("too_few_entries"));
        assert_eq!(empty.entries, 0);
        seed(&store, "profile", 1).await;
        assert!(
            !store
                .memory_maintenance_readiness("one", "profile", 2)
                .await
                .unwrap()
                .ready
        );
        seed(&store, "profile", 2).await;
        assert!(
            store
                .memory_maintenance_readiness("one", "profile", 2)
                .await
                .unwrap()
                .ready
        );
        assert!(
            !store
                .memory_maintenance_readiness("one", "profile", 5)
                .await
                .unwrap()
                .ready
        );
        seed(&store, "profile", 5).await;
        let ready = store
            .memory_maintenance_readiness("one", "profile", 5)
            .await
            .unwrap();
        assert!(ready.ready);
        assert_eq!(ready.entries, 5);
        assert!(ready.external_revision >= 5);
        assert_eq!(
            store
                .memory_maintenance_readiness("two", "profile", 5)
                .await
                .unwrap()
                .entries,
            0
        );
        assert!(store
            .memory_maintenance_readiness("one", "profile", 1)
            .await
            .is_err());
        assert!(store
            .memory_maintenance_readiness("one", "profile", 10001)
            .await
            .is_err());
    }

    #[tokio::test]
    async fn successful_maintenance_does_not_retrigger_from_its_own_writes() {
        let (_dir, store) = fixture().await;
        seed(&store, "profile", 5).await;
        let run_id = start(&store, MEMORY_MAINTAINER_AGENT, "profile").await;
        let prepared = store.prepare_memory_maintenance(run_id, 5).await.unwrap();
        assert!(prepared.ready);
        store
            .put_memory_with_graph_for_run(
                run_id,
                "profile",
                "0",
                "fact cleaned by maintenance",
                &embedding(),
                &MemoryGraphInput::default(),
            )
            .await
            .unwrap();
        assert_eq!(
            store
                .memory_maintenance_readiness("one", "profile", 5)
                .await
                .unwrap()
                .external_revision,
            prepared.external_revision
        );
        store
            .finalize_run_success(run_id, &json!({"done":true}), None)
            .await
            .unwrap();
        let settled = store
            .memory_maintenance_readiness("one", "profile", 5)
            .await
            .unwrap();
        assert_eq!(settled.reason.as_deref(), Some("unchanged"));
        let existing = store
            .get_memory("one", "profile", "0")
            .await
            .unwrap()
            .unwrap();
        store
            .put_memory("one", "profile", "0", &existing.text, &embedding())
            .await
            .unwrap();
        assert!(!store
            .delete_memory("one", "profile", "missing")
            .await
            .unwrap());
        let unchanged = store
            .memory_maintenance_readiness("one", "profile", 5)
            .await
            .unwrap();
        assert!(!unchanged.ready);
        assert_eq!(unchanged.external_revision, prepared.external_revision);
        store
            .put_memory("one", "profile", "0", "new external fact", &embedding())
            .await
            .unwrap();
        assert!(
            store
                .memory_maintenance_readiness("one", "profile", 5)
                .await
                .unwrap()
                .ready
        );
    }

    #[tokio::test]
    async fn external_change_during_maintenance_remains_pending_after_success() {
        let (_dir, store) = fixture().await;
        seed(&store, "profile", 5).await;
        let run_id = start(&store, MEMORY_MAINTAINER_AGENT, "profile").await;
        let prepared = store.prepare_memory_maintenance(run_id, 5).await.unwrap();
        mutate(&store, "one", "profile", Some(run_id))
            .await
            .unwrap();
        mutate(&store, "one", "profile", None).await.unwrap();
        store
            .finalize_run_success(run_id, &json!({"done":true}), None)
            .await
            .unwrap();
        let pending = store
            .memory_maintenance_readiness("one", "profile", 5)
            .await
            .unwrap();
        assert!(pending.ready);
        assert_eq!(pending.external_revision, prepared.external_revision + 1);
        let second = start(&store, MEMORY_MAINTAINER_AGENT, "profile").await;
        assert!(
            store
                .prepare_memory_maintenance(second, 5)
                .await
                .unwrap()
                .ready
        );
        store
            .finalize_run_success(second, &json!({"done":true}), None)
            .await
            .unwrap();
        assert!(
            !store
                .memory_maintenance_readiness("one", "profile", 5)
                .await
                .unwrap()
                .ready
        );
    }

    #[tokio::test]
    async fn failed_and_cancelled_maintenance_do_not_consume_external_changes() {
        let (_dir, store) = fixture().await;
        seed(&store, "profile", 5).await;
        let failed = start(&store, MEMORY_MAINTAINER_AGENT, "profile").await;
        store.prepare_memory_maintenance(failed, 5).await.unwrap();
        mutate(&store, "one", "profile", Some(failed))
            .await
            .unwrap();
        store.fail_run(failed, "model error").await.unwrap();
        assert!(store
            .finalize_run_success(failed, &json!({"done":true}), None)
            .await
            .is_err());
        assert!(
            store
                .memory_maintenance_readiness("one", "profile", 5)
                .await
                .unwrap()
                .ready
        );
        let cancelled = start(&store, MEMORY_MAINTAINER_AGENT, "profile").await;
        store
            .prepare_memory_maintenance(cancelled, 5)
            .await
            .unwrap();
        store.cancel_run_request(cancelled, "cancel").await.unwrap();
        assert!(
            store
                .memory_maintenance_readiness("one", "profile", 5)
                .await
                .unwrap()
                .ready
        );
        assert!(store
            .finalize_run_success(cancelled, &json!({"done":true}), None)
            .await
            .is_err());
        let mut tx = store.pool.begin().await.unwrap();
        assert!(checkpoint_success(&mut tx, cancelled).await.is_err());
        tx.rollback().await.unwrap();
    }

    #[tokio::test]
    async fn source_exemption_requires_bound_running_prepared_maintainer() {
        let (_dir, store) = fixture().await;
        seed(&store, "profile", 5).await;
        let before = store
            .memory_maintenance_readiness("one", "profile", 5)
            .await
            .unwrap();
        let ordinary = start(&store, "bot", "profile").await;
        mutate(&store, "one", "profile", Some(ordinary))
            .await
            .unwrap();
        assert_eq!(
            store
                .memory_maintenance_readiness("one", "profile", 5)
                .await
                .unwrap()
                .external_revision,
            before.external_revision + 1
        );
        assert!(store.prepare_memory_maintenance(ordinary, 5).await.is_err());
        assert!(mutate(&store, "two", "profile", Some(ordinary))
            .await
            .is_err());
        assert!(mutate(&store, "one", "profile", Some(Uuid::new_v4()))
            .await
            .is_err());
        let maintainer = start(&store, MEMORY_MAINTAINER_AGENT, "profile").await;
        assert!(mutate(&store, "one", "profile", Some(maintainer))
            .await
            .is_err());
        // Unchanged writes and absent deletes still validate their actor.
        let previous = store
            .get_memory("one", "profile", "0")
            .await
            .unwrap()
            .unwrap();
        assert!(store
            .put_memory_with_graph_for_run(
                maintainer,
                "profile",
                "0",
                &previous.text,
                &embedding(),
                &MemoryGraphInput::default(),
            )
            .await
            .is_err());
        assert!(store
            .delete_memory_for_run(maintainer, "profile", "missing")
            .await
            .is_err());
        store
            .prepare_memory_maintenance(maintainer, 5)
            .await
            .unwrap();
        assert!(mutate(&store, "one", "another", Some(maintainer))
            .await
            .is_err());
        assert!(mutate(&store, "two", "profile", Some(maintainer))
            .await
            .is_err());
        mutate(&store, "one", "profile", Some(maintainer))
            .await
            .unwrap();
        store.cancel_run_request(maintainer, "stop").await.unwrap();
        assert!(mutate(&store, "one", "profile", Some(maintainer))
            .await
            .is_err());
        assert!(store
            .delete_memory_for_run(maintainer, "profile", "0")
            .await
            .is_err());
        assert!(store
            .get_memory("one", "profile", "0")
            .await
            .unwrap()
            .is_some());
    }

    #[tokio::test]
    async fn stale_maintenance_put_and_delete_cannot_overwrite_external_changes() {
        let (_dir, store) = fixture().await;
        seed(&store, "profile", 5).await;
        let run_id = start(&store, MEMORY_MAINTAINER_AGENT, "profile").await;
        let prepared = store.prepare_memory_maintenance(run_id, 5).await.unwrap();
        store
            .put_memory("one", "profile", "0", "fresh foreground fact", &embedding())
            .await
            .unwrap();
        let error = store
            .put_memory_with_graph_for_run(
                run_id,
                "profile",
                "0",
                "stale cleanup",
                &embedding(),
                &MemoryGraphInput::default(),
            )
            .await
            .unwrap_err();
        assert!(error
            .to_string()
            .contains("memory changed during maintenance"));
        let error = store
            .delete_memory_for_run(run_id, "profile", "0")
            .await
            .unwrap_err();
        assert!(error
            .to_string()
            .contains("memory changed during maintenance"));
        assert_eq!(
            store
                .get_memory("one", "profile", "0")
                .await
                .unwrap()
                .unwrap()
                .text,
            "fresh foreground fact"
        );
        store
            .finalize_run_success(run_id, &json!({"conflict":true}), None)
            .await
            .unwrap();
        let pending = store
            .memory_maintenance_readiness("one", "profile", 5)
            .await
            .unwrap();
        assert!(pending.ready);
        assert_eq!(pending.external_revision, prepared.external_revision + 1);
        assert_eq!(db::query_scalar::<i64>("SELECT consumed_revision FROM memory_maintenance_state WHERE tenant = 'one' AND namespace = 'profile'")
            .fetch_optional(&store.pool).await.unwrap(), Some(prepared.external_revision as i64));
    }

    #[tokio::test]
    async fn skipped_maintenance_creates_no_checkpoint_and_cannot_write() {
        let (_dir, store) = fixture().await;
        seed(&store, "profile", 1).await;
        let run_id = start(&store, MEMORY_MAINTAINER_AGENT, "profile").await;
        assert!(
            !store
                .prepare_memory_maintenance(run_id, 5)
                .await
                .unwrap()
                .ready
        );
        assert!(mutate(&store, "one", "profile", Some(run_id))
            .await
            .is_err());
        assert_eq!(
            db::query_scalar::<i64>("SELECT COUNT(*) FROM memory_maintenance_runs")
                .fetch_optional(&store.pool)
                .await
                .unwrap(),
            Some(0)
        );
        store
            .finalize_run_success(run_id, &json!({"status":"skipped"}), None)
            .await
            .unwrap();
        seed(&store, "profile", 5).await;
        assert!(
            store
                .memory_maintenance_readiness("one", "profile", 5)
                .await
                .unwrap()
                .ready
        );
    }

    #[tokio::test]
    async fn schema_nine_migration_marks_existing_namespaces_ready() {
        let (dir, store) = fixture().await;
        seed(&store, "profile", 5).await;
        for table in ["memory_maintenance_runs", "memory_maintenance_state"] {
            db::query(&format!("DROP TABLE {table}"))
                .execute(&store.pool)
                .await
                .unwrap();
        }
        db::query("PRAGMA user_version = 9")
            .execute(&store.pool)
            .await
            .unwrap();
        drop(store);
        let migrated = AgentdStore::new(dir.path().join("agentd.db").to_str().unwrap())
            .await
            .unwrap();
        let ready = migrated
            .memory_maintenance_readiness("one", "profile", 5)
            .await
            .unwrap();
        assert!(ready.ready);
        assert_eq!(ready.entries, 5);
        assert_eq!(ready.external_revision, 1);
        assert_eq!(
            db::query_scalar::<i64>("PRAGMA user_version")
                .fetch_optional(&migrated.pool)
                .await
                .unwrap(),
            Some(10)
        );
    }
}
