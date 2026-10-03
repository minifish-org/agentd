use super::*;

impl AgentdStore {
    pub async fn create_tenant(
        &self,
        name: &str,
        metadata: &serde_json::Value,
    ) -> Result<(TenantRecord, bool)> {
        let name = normalize_tenant_name(name)?;
        let now = Utc::now().to_rfc3339();
        let metadata_json = serde_json::to_string(metadata)?;
        let mut tx = self.pool.begin().await?;
        let result = db::query(
            "INSERT OR IGNORE INTO tenants (name, metadata_json, created_at, updated_at) \
             VALUES (?, ?, ?, ?)",
        )
        .bind(&name)
        .bind(&metadata_json)
        .bind(&now)
        .bind(&now)
        .execute(&mut tx)
        .await?;
        let created = result.rows_affected() > 0;
        audit::record(
            &mut tx,
            AuditInput::new(
                Some(&name),
                "tenant.create",
                "tenant",
                Some(&name),
                audit_mutations::outcome(created),
                json!({"created":created,"metadata_bytes":metadata_json.len()}),
            ),
        )
        .await?;
        tx.commit().await?;
        let record = self
            .get_tenant(&name)
            .await?
            .ok_or_else(|| anyhow!("tenant was not readable after create: {name}"))?;
        Ok((record, created))
    }

    pub async fn get_tenant(&self, name: &str) -> Result<Option<TenantRecord>> {
        let name = normalize_tenant_name(name)?;
        db::query("SELECT name, metadata_json, created_at, updated_at FROM tenants WHERE name = ?")
            .bind(&name)
            .fetch_optional(&self.pool)
            .await?
            .map(row_to_tenant_record)
            .transpose()
    }

    pub(super) async fn ensure_tenant_exists(&self, tenant: &str) -> Result<()> {
        if self.get_tenant(tenant).await?.is_none() {
            return Err(missing!("tenant not found: {tenant}"));
        }
        Ok(())
    }

    pub async fn patch_tenant_metadata(
        &self,
        name: &str,
        metadata: &serde_json::Value,
        if_updated_at: Option<&str>,
    ) -> Result<TenantMetadataPatchResult> {
        let name = normalize_tenant_name(name)?;
        let mut tx = self.pool.begin().await?;
        let current = db::query(
            "SELECT name, metadata_json, created_at, updated_at FROM tenants WHERE name = ?",
        )
        .bind(&name)
        .fetch_optional(&mut tx)
        .await?
        .map(row_to_tenant_record)
        .transpose()?;
        let Some(current) = current else {
            audit::record(
                &mut tx,
                AuditInput::new(
                    Some(&name),
                    "tenant.metadata",
                    "tenant",
                    Some(&name),
                    "not_found",
                    json!({}),
                ),
            )
            .await?;
            tx.commit().await?;
            return Ok(TenantMetadataPatchResult::NotFound);
        };
        if let Some(expected) = if_updated_at {
            if current.updated_at != expected {
                audit::record(
                    &mut tx,
                    AuditInput::new(
                        Some(&name),
                        "tenant.metadata",
                        "tenant",
                        Some(&name),
                        "rejected",
                        json!({"reason":"version_conflict"}),
                    ),
                )
                .await?;
                tx.commit().await?;
                return Ok(TenantMetadataPatchResult::Conflict(current));
            }
        }

        let now = Utc::now().to_rfc3339();
        let metadata_json = serde_json::to_string(metadata)?;
        let changed =
            db::query("UPDATE tenants SET metadata_json = ?, updated_at = ? WHERE name = ? AND updated_at = ?")
                .bind(&metadata_json)
                .bind(&now)
                .bind(&name)
                .bind(&current.updated_at)
                .execute(&mut tx)
                .await?
                .rows_affected();

        if changed == 0 {
            audit::record(
                &mut tx,
                AuditInput::new(
                    Some(&name),
                    "tenant.metadata",
                    "tenant",
                    Some(&name),
                    "not_found",
                    json!({}),
                ),
            )
            .await?;
            tx.commit().await?;
            return Ok(TenantMetadataPatchResult::NotFound);
        }

        audit::record(&mut tx, AuditInput::new(Some(&name), "tenant.metadata", "tenant", Some(&name), "succeeded", json!({"changed":current.metadata != *metadata,"metadata_bytes":metadata_json.len()}))).await?;
        tx.commit().await?;
        let updated = TenantRecord {
            name,
            metadata: metadata.clone(),
            created_at: current.created_at,
            updated_at: now,
        };
        Ok(TenantMetadataPatchResult::Updated(updated))
    }

    pub async fn list_tenants(&self) -> Result<Vec<String>> {
        let tenants = db::query_scalar::<String>("SELECT name FROM tenants ORDER BY name")
            .fetch_all(&self.pool)
            .await?;
        Ok(tenants)
    }

    /// List the existing context scopes for a given (tenant, agent), most
    /// recently updated first. Scopes are not "created" out of band — a row
    /// exists once a turn (or `context create`) has written context for it.
    pub async fn apply_agent(&self, agent: &AgentResource) -> Result<()> {
        agent.validate().map_err(validation)?;
        let mut tx = self.pool.begin().await?;
        Self::ensure_tenant_in_tx(&mut tx, &agent.metadata.tenant).await?;
        Self::write_agent(&mut tx, agent).await?;
        tx.commit().await?;
        Ok(())
    }

    pub(super) async fn ensure_tenant_in_tx(tx: &mut db::Transaction, tenant: &str) -> Result<()> {
        if db::query_scalar::<String>("SELECT name FROM tenants WHERE name = ?")
            .bind(tenant)
            .fetch_optional(&mut *tx)
            .await?
            .is_none()
        {
            return Err(not_found(format!("tenant not found: {tenant}")));
        }
        Ok(())
    }

    /// Reserve the writer before validating ownership so tenant deletion
    /// cannot commit between the existence check and the resource write.
    pub(super) async fn begin_tenant_write(&self, tenant: &str) -> Result<db::Transaction> {
        #[cfg(test)]
        {
            let hook = self.tenant_write_hook.lock().await.take();
            if let Some(hook) = hook {
                hook.before_transaction.notify_one();
                hook.resume_writer.notified().await;
            }
        }
        let mut tx = self.pool.begin_immediate().await?;
        Self::ensure_tenant_in_tx(&mut tx, tenant).await?;
        Ok(tx)
    }

    pub(super) async fn get_agent_in_tx(
        tx: &mut db::Transaction,
        tenant: &str,
        name: &str,
    ) -> Result<Option<Agent>> {
        db::query(
            "SELECT metadata_json, spec_json, created_at, updated_at FROM agents WHERE tenant = ? AND name = ?",
        )
        .bind(tenant)
        .bind(name)
        .fetch_optional(&mut *tx)
        .await?
        .map(row_to_agent)
        .transpose()
    }

    pub(super) async fn write_agent(tx: &mut db::Transaction, agent: &AgentResource) -> Result<()> {
        let now = Utc::now().to_rfc3339();
        let previous_spec = db::query_scalar::<String>(
            "SELECT spec_json FROM agents WHERE tenant = ? AND name = ?",
        )
        .bind(&agent.metadata.tenant)
        .bind(&agent.metadata.name)
        .fetch_optional(&mut *tx)
        .await?;
        let previous_spec = previous_spec
            .as_deref()
            .map(decode_json::<agentd_api::AgentSpec>)
            .transpose()?;
        let spec_changed = previous_spec
            .as_ref()
            .is_some_and(|previous| *previous != agent.spec);
        if spec_changed
            || agent.metadata.name.starts_with("system/")
            || agent
                .metadata
                .labels
                .get("agentd.system")
                .is_some_and(|value| value == "true")
        {
            let invalidated = db::query("UPDATE behavior_heads SET active_revision = NULL WHERE tenant = ? AND agent_ref = ? AND active_revision IS NOT NULL")
                .bind(&agent.metadata.tenant)
                .bind(&agent.metadata.name)
                .execute(&mut *tx).await?.rows_affected();
            if invalidated > 0 {
                audit::record(
                    &mut *tx,
                    AuditInput::new(
                        Some(&agent.metadata.tenant),
                        "behavior.invalidate",
                        "agent",
                        Some(&agent.metadata.name),
                        "succeeded",
                        json!({"reason":"agent_configuration_changed"}),
                    ),
                )
                .await?;
            }
        }
        db::query(
            r#"INSERT INTO agents (tenant, name, metadata_json, spec_json, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?)
               ON CONFLICT(tenant, name) DO UPDATE SET
                 metadata_json=excluded.metadata_json,
                 spec_json=excluded.spec_json,
                 updated_at=excluded.updated_at"#,
        )
        .bind(&agent.metadata.tenant)
        .bind(&agent.metadata.name)
        .bind(serde_json::to_string(&agent.metadata)?)
        .bind(serde_json::to_string(&agent.spec)?)
        .bind(&now)
        .bind(&now)
        .execute(&mut *tx)
        .await?;
        audit::record(&mut *tx, AuditInput::new(Some(&agent.metadata.tenant), "agent.put", "agent", Some(&agent.metadata.name), "succeeded", json!({
            "created":previous_spec.is_none(),"changed_fields":audit_mutations::agent_changed_fields(previous_spec.as_ref(),&agent.spec),
            "before":previous_spec.as_ref().map(audit_mutations::agent_spec),"after":audit_mutations::agent_spec(&agent.spec),"label_count":agent.metadata.labels.len()
        }))).await?;
        Ok(())
    }

    pub async fn get_agent(&self, tenant: &str, name: &str) -> Result<Option<Agent>> {
        let row = db::query(
            "SELECT metadata_json, spec_json, created_at, updated_at FROM agents WHERE tenant = ? AND name = ?",
        )
        .bind(tenant)
        .bind(name)
        .fetch_optional(&self.pool)
        .await?;
        row.map(row_to_agent).transpose()
    }

    pub async fn list_agents(&self, tenant: Option<&str>) -> Result<Vec<Agent>> {
        let rows = if let Some(tenant) = tenant {
            db::query(
                "SELECT metadata_json, spec_json, created_at, updated_at FROM agents WHERE tenant = ? ORDER BY tenant, name",
            )
            .bind(tenant)
            .fetch_all(&self.pool)
            .await?
        } else {
            db::query(
                "SELECT metadata_json, spec_json, created_at, updated_at FROM agents ORDER BY tenant, name",
            )
            .fetch_all(&self.pool)
            .await?
        };
        rows.into_iter().map(row_to_agent).collect()
    }
    pub async fn apply_mcp_server(
        &self,
        tenant: &str,
        name: &str,
        spec: &agentd_api::McpServerSpec,
        tools: &[agentd_api::McpTool],
        last_error: Option<&str>,
    ) -> Result<()> {
        let _guard = self.mcp_apply_lock.lock().await;
        spec.validate().map_err(validation)?;
        if self.get_tenant(tenant).await?.is_none() {
            return Err(missing!("tenant not found: {tenant}"));
        }
        self.ensure_unique_mcp_tool_names(tenant, name, spec.enabled, tools)
            .await?;
        let now = Utc::now().to_rfc3339();
        let mut tx = self.pool.begin().await?;
        db::query(
            r#"INSERT INTO mcp_servers (
                   tenant, name, spec_json, tools_json, last_error, created_at, updated_at
               ) VALUES (?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(tenant, name) DO UPDATE SET
                 spec_json = excluded.spec_json,
                 tools_json = excluded.tools_json,
                 last_error = excluded.last_error,
                 updated_at = excluded.updated_at"#,
        )
        .bind(tenant)
        .bind(name)
        .bind(serde_json::to_string(spec)?)
        .bind(serde_json::to_string(tools)?)
        .bind(last_error)
        .bind(&now)
        .bind(&now)
        .execute(&mut tx)
        .await?;
        audit::record(&mut tx, AuditInput::new(Some(tenant), "mcp.put", "mcp_server", Some(name), "succeeded", json!({"enabled":spec.enabled,"tool_count":tools.len(),"discovery_failed":last_error.is_some()}))).await?;
        tx.commit().await?;
        Ok(())
    }

    pub(super) async fn ensure_unique_mcp_tool_names(
        &self,
        tenant: &str,
        candidate_server: &str,
        candidate_enabled: bool,
        candidate_tools: &[agentd_api::McpTool],
    ) -> Result<()> {
        let mut owners = BTreeMap::new();
        for server in self.list_mcp_servers(Some(tenant)).await? {
            if server.name == candidate_server || !server.spec.enabled {
                continue;
            }
            for tool in server.tools {
                let exposed = mcp_exposed_name(&server.name, &tool.name);
                let owner = format!("{}/{}", server.name, tool.name);
                if let Some(existing) = owners.insert(exposed.clone(), owner.clone()) {
                    return Err(conflicting!(
                        "MCP exposed tool name collision for {exposed}: {existing} and {owner}"
                    ));
                }
            }
        }
        if candidate_enabled {
            for tool in candidate_tools {
                let exposed = mcp_exposed_name(candidate_server, &tool.name);
                let owner = format!("{candidate_server}/{}", tool.name);
                if let Some(existing) = owners.insert(exposed.clone(), owner.clone()) {
                    return Err(conflicting!(
                        "MCP exposed tool name collision for {exposed}: {existing} and {owner}"
                    ));
                }
            }
        }
        Ok(())
    }

    pub async fn delete_mcp_server(&self, tenant: &str, name: &str) -> Result<bool> {
        let mut tx = self.pool.begin().await?;
        let deleted = db::query("DELETE FROM mcp_servers WHERE tenant = ? AND name = ?")
            .bind(tenant)
            .bind(name)
            .execute(&mut tx)
            .await?
            .rows_affected()
            > 0;
        audit::record(
            &mut tx,
            AuditInput::new(
                Some(tenant),
                "mcp.delete",
                "mcp_server",
                Some(name),
                audit_mutations::outcome(deleted),
                json!({"deleted":deleted}),
            ),
        )
        .await?;
        tx.commit().await?;
        Ok(deleted)
    }

    pub async fn get_mcp_server(&self, tenant: &str, name: &str) -> Result<Option<McpServer>> {
        let row = db::query(
            "SELECT tenant, name, spec_json, tools_json, last_error, created_at, updated_at \
             FROM mcp_servers WHERE tenant = ? AND name = ?",
        )
        .bind(tenant)
        .bind(name)
        .fetch_optional(&self.pool)
        .await?;
        row.map(row_to_mcp_server).transpose()
    }

    pub async fn list_mcp_servers(&self, tenant: Option<&str>) -> Result<Vec<McpServer>> {
        let rows = if let Some(tenant) = tenant {
            db::query(
                "SELECT tenant, name, spec_json, tools_json, last_error, created_at, updated_at \
                 FROM mcp_servers WHERE tenant = ? ORDER BY name",
            )
            .bind(tenant)
            .fetch_all(&self.pool)
            .await?
        } else {
            db::query(
                "SELECT tenant, name, spec_json, tools_json, last_error, created_at, updated_at \
                 FROM mcp_servers ORDER BY tenant, name",
            )
            .fetch_all(&self.pool)
            .await?
        };
        rows.into_iter().map(row_to_mcp_server).collect()
    }

    pub async fn get_mcp_tool_invocation_target(
        &self,
        tenant: &str,
        exposed_name: &str,
    ) -> Result<Option<McpToolInvocationTarget>> {
        for server in self.list_mcp_servers(Some(tenant)).await? {
            if !server.spec.enabled {
                continue;
            }
            for tool in &server.tools {
                if mcp_exposed_name(&server.name, &tool.name) == exposed_name {
                    return Ok(Some(McpToolInvocationTarget {
                        server: server.clone(),
                        tool: tool.clone(),
                    }));
                }
            }
        }
        Ok(None)
    }

    pub async fn list_visible_tools(
        &self,
        tenant: &str,
        allowed_families: &[ToolFamily],
    ) -> Result<Vec<ToolSpec>> {
        let mut tools = visible_tools(&builtin_tool_catalog(), allowed_families);
        let mcp_allowed = allowed_families.contains(&ToolFamily::Mcp);
        if mcp_allowed {
            for server in self.list_mcp_servers(Some(tenant)).await? {
                if !server.spec.enabled {
                    continue;
                }
                tools.extend(server.tools.iter().map(|tool| ToolSpec {
                    name: mcp_exposed_name(&server.name, &tool.name),
                    family: ToolFamily::Mcp,
                    description:
                        tool.description.clone().unwrap_or_else(|| {
                            format!("MCP tool {} from {}", tool.name, server.name)
                        }),
                    input_schema: tool.input_schema.clone(),
                    mutating: true,
                }));
            }
        }
        Ok(tools)
    }
    pub async fn delete_agent(&self, tenant: &str, name: &str) -> Result<bool> {
        let mut tx = self.pool.begin().await?;
        let result = db::query("DELETE FROM agents WHERE tenant = ? AND name = ?")
            .bind(tenant)
            .bind(name)
            .execute(&mut tx)
            .await?;
        // A recreated name starts a new learning lifecycle. Immutable revision
        // history remains available, but its policy and consumption cursor do not.
        db::query("DELETE FROM behavior_heads WHERE tenant = ? AND agent_ref = ?")
            .bind(tenant)
            .bind(name)
            .execute(&mut tx)
            .await?;
        audit::record(
            &mut tx,
            AuditInput::new(
                Some(tenant),
                "agent.delete",
                "agent",
                Some(name),
                audit_mutations::outcome(result.rows_affected() > 0),
                json!({"deleted":result.rows_affected() > 0,"behavior_head_cleared":true}),
            ),
        )
        .await?;
        tx.commit().await?;
        Ok(result.rows_affected() > 0)
    }

    pub async fn delete_tenant(&self, tenant: &str) -> Result<serde_json::Value> {
        if tenant == "system" {
            return Err(invalid!("cannot delete the system tenant"));
        }
        let mut tx = self.pool.begin().await?;
        let mut removed = serde_json::Map::new();
        let logs = db::query(
            "DELETE FROM run_log WHERE run_id IN (SELECT run_id FROM runs WHERE tenant = ?)",
        )
        .bind(tenant)
        .execute(&mut tx)
        .await?
        .rows_affected();
        removed.insert("run_log".into(), json!(logs));
        for table in [
            "deliveries",
            "runs",
            "contexts",
            "artifacts",
            "edges",
            "entities",
            "memory",
            "schedules",
            "mcp_servers",
            "behavior_heads",
            "behavior_revisions",
            "memory_maintenance_state",
            "memory_maintenance_runs",
            "agents",
        ] {
            let count = db::query(&format!("DELETE FROM {table} WHERE tenant = ?"))
                .bind(tenant)
                .execute(&mut tx)
                .await?
                .rows_affected();
            removed.insert(table.into(), json!(count));
        }
        let deleted = db::query("DELETE FROM tenants WHERE name = ?")
            .bind(tenant)
            .execute(&mut tx)
            .await?
            .rows_affected();
        audit::record(
            &mut tx,
            AuditInput::new(
                Some(tenant),
                "tenant.delete",
                "tenant",
                Some(tenant),
                audit_mutations::outcome(deleted > 0),
                json!({"deleted":deleted > 0,"removed":removed}),
            ),
        )
        .await?;
        tx.commit().await?;
        Ok(json!({ "tenant": tenant, "deleted": deleted > 0 }))
    }
}

pub(super) fn normalize_tenant_name(name: &str) -> Result<String> {
    let name = name.trim();
    if name.is_empty() {
        return Err(invalid!("tenant name is required"));
    }
    Ok(name.to_string())
}

pub(super) fn row_to_tenant_record(row: db::SqlRow) -> Result<TenantRecord> {
    let metadata_json = row.try_get::<String, _>("metadata_json")?;
    Ok(TenantRecord {
        name: row.try_get::<String, _>("name")?,
        metadata: decode_json(&metadata_json)?,
        created_at: row.try_get::<String, _>("created_at")?,
        updated_at: row.try_get::<String, _>("updated_at")?,
    })
}

pub(super) fn row_to_agent(row: db::SqlRow) -> Result<Agent> {
    let metadata: agentd_api::ResourceMeta =
        decode_json(&row.try_get::<String, _>("metadata_json")?)?;
    let spec: agentd_api::AgentSpec = decode_json(&row.try_get::<String, _>("spec_json")?)?;
    Ok(Agent {
        tenant: metadata.tenant.clone(),
        name: metadata.name.clone(),
        metadata,
        spec,
        created_at: parse_ts_field(&row, "created_at")?,
        updated_at: parse_ts_field(&row, "updated_at")?,
    })
}

pub(super) fn row_to_mcp_server(row: db::SqlRow) -> Result<McpServer> {
    Ok(McpServer {
        tenant: row.try_get("tenant")?,
        name: row.try_get("name")?,
        spec: decode_json(&row.try_get::<String, _>("spec_json")?)?,
        tools: decode_json(&row.try_get::<String, _>("tools_json")?)?,
        last_error: row.try_get("last_error")?,
        created_at: parse_ts_field(&row, "created_at")?,
        updated_at: parse_ts_field(&row, "updated_at")?,
    })
}

pub(super) fn mcp_exposed_name(server_name: &str, tool_name: &str) -> String {
    truncate_tool_name(&format!(
        "mcp_{}_{}",
        sanitize_tool_name_part(server_name),
        sanitize_tool_name_part(tool_name)
    ))
}

pub(super) fn truncate_tool_name(raw: &str) -> String {
    if raw.len() <= 64 {
        return raw.to_string();
    }
    let digest = hex_encode(Sha256::digest(raw.as_bytes()));
    let suffix = format!("_{}", &digest[..8]);
    let head = truncate_ascii(raw, 64 - suffix.len());
    format!("{head}{suffix}")
}

pub(super) fn truncate_ascii(raw: &str, max_len: usize) -> String {
    raw.chars().take(max_len).collect()
}

pub(super) fn sanitize_tool_name_part(raw: &str) -> String {
    let mut out = raw
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '_' || ch == '-' {
                ch
            } else {
                '_'
            }
        })
        .collect::<String>();
    while out.contains("__") {
        out = out.replace("__", "_");
    }
    let mut out = out.trim_matches('_').to_string();
    if out.is_empty() {
        out.push_str("tool");
    }
    if out == raw {
        out
    } else {
        let digest = hex_encode(Sha256::digest(raw.as_bytes()));
        format!("{out}_{}", &digest[..8])
    }
}
