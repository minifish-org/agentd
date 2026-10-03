use super::*;

impl AgentdStore {
    pub(super) async fn initialize_schema(&self) -> Result<()> {
        const SCHEMA_VERSION: i64 = 11;
        const GRAPH_MIGRATION_SCHEMA_VERSION: i64 = 6;
        const DELIVERY_PAYLOAD_SCHEMA_VERSION: i64 = 7;
        let version = db::query_scalar::<i64>("PRAGMA user_version")
            .fetch_optional(&self.pool)
            .await?
            .unwrap_or(0);
        if version != 0
            && version != GRAPH_MIGRATION_SCHEMA_VERSION
            && version != DELIVERY_PAYLOAD_SCHEMA_VERSION
            && version != 8
            && version != 9
            && version != 10
            && version != SCHEMA_VERSION
        {
            return Err(anyhow!(
                "agentd schema version {version} is unsupported; restart with --reset-data"
            ));
        }
        if version == 0 {
            let existing = db::query_scalar::<String>(
                "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' LIMIT 1",
            )
            .fetch_optional(&self.pool)
            .await?;
            if let Some(table) = existing {
                return Err(anyhow!(
                    "unversioned agentd data found ({table}); restart with --reset-data"
                ));
            }
        }

        let statements = [
            r#"CREATE TABLE tenants (
                name TEXT PRIMARY KEY NOT NULL,
                metadata_json TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )"#,
            r#"CREATE TABLE agents (
                tenant TEXT NOT NULL,
                name TEXT NOT NULL,
                metadata_json TEXT NOT NULL,
                spec_json TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (tenant, name)
            )"#,
            r#"CREATE TABLE runs (
                run_id TEXT PRIMARY KEY NOT NULL,
                tenant TEXT NOT NULL,
                name TEXT NOT NULL,
                agent_ref TEXT NOT NULL,
                scope TEXT NOT NULL,
                source TEXT NOT NULL,
                input_json TEXT NOT NULL,
                output_json TEXT,
                error TEXT,
                status TEXT NOT NULL,
                request_id TEXT,
                schedule_name TEXT,
                delivery_destination TEXT,
                created_at TEXT NOT NULL,
                started_at TEXT,
                updated_at TEXT NOT NULL
            )"#,
            r#"CREATE TABLE run_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT NOT NULL,
                kind TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                ts TEXT NOT NULL
            )"#,
            r#"CREATE TABLE contexts (
                tenant TEXT NOT NULL,
                agent TEXT NOT NULL,
                scope TEXT NOT NULL,
                revision INTEGER NOT NULL,
                state_json TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (tenant, agent, scope)
            )"#,
            r#"CREATE TABLE artifacts (
                tenant TEXT NOT NULL,
                path TEXT NOT NULL,
                body BLOB NOT NULL,
                content_type TEXT,
                meta_json TEXT,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (tenant, path)
            )"#,
            r#"CREATE TABLE memory (
                tenant TEXT NOT NULL,
                namespace TEXT NOT NULL,
                id TEXT NOT NULL,
                text TEXT NOT NULL,
                embedding BLOB NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                UNIQUE (tenant, namespace, id)
            )"#,
            "CREATE VIRTUAL TABLE memory_fts USING fts5(tenant UNINDEXED, namespace UNINDEXED, id UNINDEXED, text, content='memory', content_rowid='rowid')",
            r#"CREATE TRIGGER memory_ai AFTER INSERT ON memory BEGIN
                INSERT INTO memory_fts(rowid, tenant, namespace, id, text)
                VALUES (new.rowid, new.tenant, new.namespace, new.id, new.text);
            END"#,
            r#"CREATE TRIGGER memory_ad AFTER DELETE ON memory BEGIN
                INSERT INTO memory_fts(memory_fts, rowid, tenant, namespace, id, text)
                VALUES ('delete', old.rowid, old.tenant, old.namespace, old.id, old.text);
            END"#,
            r#"CREATE TRIGGER memory_au AFTER UPDATE ON memory BEGIN
                INSERT INTO memory_fts(memory_fts, rowid, tenant, namespace, id, text)
                VALUES ('delete', old.rowid, old.tenant, old.namespace, old.id, old.text);
                INSERT INTO memory_fts(rowid, tenant, namespace, id, text)
                VALUES (new.rowid, new.tenant, new.namespace, new.id, new.text);
            END"#,
            r#"CREATE TABLE schedules (
                tenant TEXT NOT NULL,
                name TEXT NOT NULL,
                spec_json TEXT NOT NULL,
                last_triggered_at TEXT,
                next_trigger_at TEXT,
                last_run_id TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (tenant, name)
            )"#,
            r#"CREATE TABLE deliveries (
                delivery_id TEXT PRIMARY KEY NOT NULL,
                tenant TEXT NOT NULL,
                run_id TEXT NOT NULL,
                status TEXT NOT NULL,
                destination TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                idempotency_key TEXT NOT NULL UNIQUE,
                attempt INTEGER NOT NULL DEFAULT 0,
                next_attempt_at TEXT,
                last_error TEXT,
                claim_token TEXT,
                claim_expires_at TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )"#,
            r#"CREATE TABLE mcp_servers (
                tenant TEXT NOT NULL,
                name TEXT NOT NULL,
                spec_json TEXT NOT NULL,
                tools_json TEXT NOT NULL,
                last_error TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (tenant, name)
            )"#,
            "CREATE UNIQUE INDEX idx_runs_request_id ON runs(tenant, request_id) WHERE request_id IS NOT NULL",
            "CREATE UNIQUE INDEX idx_runs_active_scope ON runs(tenant, agent_ref, scope) WHERE status = 'running'",
            "CREATE INDEX idx_runs_queue ON runs(status, created_at)",
            "CREATE INDEX idx_runs_tenant_created ON runs(tenant, created_at DESC)",
            "CREATE INDEX idx_run_log_run ON run_log(run_id, id)",
            "CREATE INDEX idx_artifacts_tenant_path ON artifacts(tenant, path)",
            "CREATE INDEX idx_deliveries_claim ON deliveries(tenant, status, next_attempt_at, created_at)",
        ];
        let graph_statements = [
            r#"CREATE TABLE entities (
                tenant TEXT NOT NULL,
                namespace TEXT NOT NULL,
                memory_id TEXT NOT NULL,
                entity_id TEXT NOT NULL,
                label TEXT NOT NULL,
                entity_type TEXT,
                properties_json TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (tenant, namespace, memory_id, entity_id),
                FOREIGN KEY (tenant, namespace, memory_id)
                    REFERENCES memory(tenant, namespace, id) ON DELETE CASCADE
            )"#,
            r#"CREATE TABLE edges (
                tenant TEXT NOT NULL,
                namespace TEXT NOT NULL,
                memory_id TEXT NOT NULL,
                source_entity_id TEXT NOT NULL,
                relation TEXT NOT NULL,
                target_entity_id TEXT NOT NULL,
                properties_json TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (
                    tenant, namespace, memory_id,
                    source_entity_id, relation, target_entity_id
                ),
                FOREIGN KEY (tenant, namespace, memory_id)
                    REFERENCES memory(tenant, namespace, id) ON DELETE CASCADE
            )"#,
            "CREATE INDEX idx_entities_lookup ON entities(tenant, namespace, entity_id)",
            "CREATE INDEX idx_entities_label ON entities(tenant, namespace, label)",
            "CREATE INDEX idx_edges_outgoing ON edges(tenant, namespace, source_entity_id, relation)",
            "CREATE INDEX idx_edges_incoming ON edges(tenant, namespace, target_entity_id, relation)",
        ];
        if version == 0 {
            let mut tx = self.pool.begin().await?;
            for statement in statements
                .into_iter()
                .chain(graph_statements)
                .chain(behavior::SCHEMA_STATEMENTS)
                .chain(maintenance::SCHEMA_STATEMENTS)
                .chain(audit::SCHEMA_STATEMENTS)
            {
                db::query(statement).execute(&mut tx).await?;
            }
            db::query("PRAGMA user_version = 11")
                .execute(&mut tx)
                .await?;
            audit::record(
                &mut tx,
                AuditInput::new(
                    None,
                    "schema.initialize",
                    "database",
                    None,
                    "succeeded",
                    json!({"from_version":0,"to_version":11}),
                ),
            )
            .await?;
            tx.commit().await?;
        } else if version < SCHEMA_VERSION {
            let mut tx = self.pool.begin().await?;
            if version == GRAPH_MIGRATION_SCHEMA_VERSION {
                for statement in graph_statements {
                    db::query(statement).execute(&mut tx).await?;
                }
            }
            if version <= DELIVERY_PAYLOAD_SCHEMA_VERSION {
                db::query(
                    "ALTER TABLE deliveries ADD COLUMN payload_json TEXT NOT NULL DEFAULT 'null'",
                )
                .execute(&mut tx)
                .await?;
                db::query(
                    "UPDATE deliveries SET payload_json = COALESCE(\
                     (SELECT output_json FROM runs WHERE runs.run_id = deliveries.run_id), 'null')",
                )
                .execute(&mut tx)
                .await?;
            }
            for statement in behavior::SCHEMA_STATEMENTS {
                db::query(statement).execute(&mut tx).await?;
            }
            for statement in maintenance::SCHEMA_STATEMENTS
                .into_iter()
                .chain(audit::SCHEMA_STATEMENTS)
            {
                db::query(statement).execute(&mut tx).await?;
            }
            audit::record(
                &mut tx,
                AuditInput::new(
                    None,
                    "schema.migrate",
                    "database",
                    None,
                    "succeeded",
                    json!({"from_version":version,"to_version":11}),
                ),
            )
            .await?;
            db::query("PRAGMA user_version = 11")
                .execute(&mut tx)
                .await?;
            tx.commit().await?;
        }
        Ok(())
    }
}
