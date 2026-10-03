use super::*;

impl AgentdStore {
    pub async fn submit_run(&self, run: NewRun<'_>) -> Result<Uuid> {
        if run.agent_ref == BEHAVIOR_LEARNER_AGENT && run.delivery_destination.is_some() {
            return Err(invalid!("behavior learning runs do not support delivery"));
        }
        if run
            .delivery_destination
            .is_some_and(|destination| destination.trim().is_empty())
        {
            return Err(invalid!("delivery destination is required"));
        }
        let mut tx = self.begin_tenant_write(run.tenant).await?;
        if let Some(request_id) = run.request_id {
            if let Some(existing) =
                Self::audit_existing_run_request(&mut tx, run.tenant, request_id).await?
            {
                tx.commit().await?;
                return Ok(existing);
            }
        }
        let background_scope = if run.agent_ref == MEMORY_MAINTAINER_AGENT {
            memory_maintenance_min_entries(run.input)?;
            Some(format!(
                "memory-maintenance/{}",
                maintenance::maintenance_namespace(run.input)?
            ))
        } else if run.agent_ref == BEHAVIOR_LEARNER_AGENT {
            let payload = if run
                .input
                .get("activation")
                .and_then(serde_json::Value::as_str)
                == Some("schedule")
            {
                run.input
                    .get("input")
                    .ok_or_else(|| invalid!("behavior learning schedule payload is missing"))?
            } else {
                run.input
            };
            let options: agentd_api::BehaviorLearningOptions =
                serde_json::from_value(payload.clone()).map_err(validation)?;
            options.validate().map_err(validation)?;
            if options.target_agent == "*" {
                return Err(invalid!("behavior learning runs require a concrete target_agent; use the preset schedule for all agents"));
            }
            Self::ensure_foreground_behavior_target(&mut tx, run.tenant, &options.target_agent)
                .await?;
            Some(format!("behavior-learning/{}", options.target_agent))
        } else {
            None
        };
        if Self::get_agent_in_tx(&mut tx, run.tenant, run.agent_ref)
            .await?
            .is_none()
        {
            return Err(missing!(
                "agent not found: {}/{}",
                run.tenant,
                run.agent_ref
            ));
        }
        let run_id = Uuid::new_v4();
        let now = Utc::now().to_rfc3339();
        let input_json = serde_json::to_string(run.input)?;
        db::query(
            r#"INSERT INTO runs (
                   run_id, tenant, name, agent_ref, scope, source, input_json,
                   status, request_id, schedule_name, delivery_destination,
                   created_at, updated_at
               ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"#,
        )
        .bind(run_id.to_string())
        .bind(run.tenant)
        .bind(run.name)
        .bind(run.agent_ref)
        .bind(background_scope.as_deref().unwrap_or(run.scope))
        .bind(run.source)
        .bind(&input_json)
        .bind(status_to_wire(AgentRunStatus::Queued))
        .bind(run.request_id)
        .bind(run.schedule_name)
        .bind(run.delivery_destination.map(str::trim))
        .bind(&now)
        .bind(&now)
        .execute(&mut tx)
        .await?;
        audit::record(&mut tx, AuditInput::new(Some(run.tenant), "run.submit", "run", Some(&run_id.to_string()), "succeeded", json!({
            "status":"queued","agent_ref":run.agent_ref,"input_bytes":input_json.len(),
            "delivery_configured":run.delivery_destination.is_some(),"schedule_configured":run.schedule_name.is_some(),
            "idempotency_configured":run.request_id.is_some(),"reused":false
        })).for_run(run_id)).await?;
        tx.commit().await?;
        Ok(run_id)
    }

    pub(super) async fn audit_existing_run_request(
        tx: &mut db::Transaction,
        tenant: &str,
        request_id: &str,
    ) -> Result<Option<Uuid>> {
        let row = db::query(
            "SELECT run_id, agent_ref, status \
             FROM runs WHERE tenant = ? AND request_id = ? LIMIT 1",
        )
        .bind(tenant)
        .bind(request_id)
        .fetch_optional(&mut *tx)
        .await?;
        let Some(row) = row else {
            return Ok(None);
        };
        let run_id = parse_uuid_field(&row, "run_id")?;
        audit::record(tx, AuditInput::new(Some(tenant), "run.submit", "run", Some(&run_id.to_string()), "noop", json!({
            "reused":true,"status":row.try_get::<String,_>("status")?,"agent_ref":row.try_get::<String,_>("agent_ref")?
        })).for_run(run_id)).await?;
        Ok(Some(run_id))
    }

    pub async fn get_run(&self, run_id: Uuid) -> Result<Option<AgentRun>> {
        let row = db::query(
            "SELECT run_id, tenant, name, agent_ref, scope, source, input_json, \
                    output_json, error, status, request_id, created_at, started_at, updated_at \
             FROM runs WHERE run_id = ?",
        )
        .bind(run_id.to_string())
        .fetch_optional(&self.pool)
        .await?;
        row.map(row_to_run).transpose()
    }

    pub async fn list_runs(&self, query: &RunListQuery) -> Result<Vec<AgentRun>> {
        let mut sql = String::from(
            "SELECT run_id, tenant, name, agent_ref, scope, source, input_json, \
                    output_json, error, status, request_id, created_at, started_at, updated_at \
             FROM runs WHERE 1 = 1",
        );
        if query.tenant.is_some() {
            sql.push_str(" AND tenant = ?");
        }
        if query.agent_ref.is_some() {
            sql.push_str(" AND agent_ref = ?");
        }
        if query.status.is_some() {
            sql.push_str(" AND status = ?");
        }
        sql.push_str(" ORDER BY created_at DESC LIMIT ?");

        let mut q = db::query(&sql);
        if let Some(tenant) = query.tenant.as_deref() {
            q = q.bind(tenant);
        }
        if let Some(agent_ref) = query.agent_ref.as_deref() {
            q = q.bind(agent_ref);
        }
        if let Some(status) = query.status {
            q = q.bind(status_to_wire(status));
        }
        q = q.bind(query.limit.max(1) as i64);

        let rows = q.fetch_all(&self.pool).await?;
        rows.into_iter().map(row_to_run).collect()
    }

    pub async fn get_run_output(&self, run_id: Uuid) -> Result<Option<serde_json::Value>> {
        let row = db::query("SELECT output_json FROM runs WHERE run_id = ?")
            .bind(run_id.to_string())
            .fetch_optional(&self.pool)
            .await?;
        row.map(|row| row.try_get::<Option<String>, _>("output_json"))
            .transpose()?
            .flatten()
            .map(|raw| decode_json(&raw))
            .transpose()
    }

    pub async fn cancel_run_request(&self, run_id: Uuid, reason: &str) -> Result<AgentRunStatus> {
        let mut tx = self.pool.begin().await?;
        let row = db::query("SELECT tenant, status FROM runs WHERE run_id = ?")
            .bind(run_id.to_string())
            .fetch_optional(&mut tx)
            .await?
            .ok_or_else(|| missing!("run not found"))?;
        let tenant = row.try_get::<String, _>("tenant")?;
        let current = status_from_wire(&row.try_get::<String, _>("status")?)?;
        if !matches!(current, AgentRunStatus::Queued | AgentRunStatus::Running) {
            audit::record(
                &mut tx,
                AuditInput::new(
                    Some(&tenant),
                    "run.cancel",
                    "run",
                    Some(&run_id.to_string()),
                    "noop",
                    json!({"status":status_to_wire(current)}),
                )
                .for_run(run_id),
            )
            .await?;
            tx.commit().await?;
            return Ok(current);
        }

        let now = Utc::now().to_rfc3339();
        let changed = db::query(
            "UPDATE runs SET status = 'cancelled', error = ?, updated_at = ? \
             WHERE run_id = ? AND status IN ('queued', 'running')",
        )
        .bind(reason)
        .bind(&now)
        .bind(run_id.to_string())
        .execute(&mut tx)
        .await?
        .rows_affected();
        if changed != 1 {
            return Err(conflicting!("run status changed during cancellation"));
        }
        Self::append_run_trace(
            &mut tx,
            &tenant,
            run_id,
            "status",
            &json!({"status":"cancelled", "reason":reason}),
            &now,
        )
        .await?;
        audit::record(
            &mut tx,
            AuditInput::new(
                Some(&tenant),
                "run.cancel",
                "run",
                Some(&run_id.to_string()),
                "succeeded",
                json!({
                    "status_before":status_to_wire(current),"status_after":"cancelled"
                }),
            )
            .for_run(run_id),
        )
        .await?;
        tx.commit().await?;
        Ok(AgentRunStatus::Cancelled)
    }

    pub async fn claim_next_run(&self) -> Result<Option<AssignedRun>> {
        self.claim_next_run_excluding_tenants(&[]).await
    }

    pub async fn claim_next_run_excluding_tenants(
        &self,
        excluded: &[String],
    ) -> Result<Option<AssignedRun>> {
        let mut sql = String::from(
            r#"SELECT run_id, tenant, name, agent_ref, scope, created_at, updated_at
               FROM runs queued
               WHERE status = 'queued'
                 AND NOT EXISTS (
                     SELECT 1 FROM runs active
                     WHERE active.tenant = queued.tenant
                       AND active.agent_ref = queued.agent_ref
                       AND active.scope = queued.scope
                       AND active.status = 'running'
                 )
               "#,
        );
        if !excluded.is_empty() {
            sql.push_str(" AND queued.tenant NOT IN (");
            sql.push_str(&vec!["?"; excluded.len()].join(","));
            sql.push(')');
        }
        sql.push_str(" ORDER BY created_at ASC LIMIT 64");
        let mut query = db::query(&sql);
        for tenant in excluded {
            query = query.bind(tenant);
        }
        let rows = query.fetch_all(&self.pool).await?;
        if rows.is_empty() {
            return Ok(None);
        }

        for row in rows {
            let run_id = parse_uuid_field(&row, "run_id")?;
            let tenant = row.try_get::<String, _>("tenant")?;
            let agent_ref = row.try_get::<String, _>("agent_ref")?;
            let scope = row.try_get::<String, _>("scope")?;
            let Some(mut run) = self.get_run(run_id).await? else {
                continue;
            };
            if run.status != AgentRunStatus::Queued {
                continue;
            }
            let Some(agent) = self.get_agent(&tenant, &agent_ref).await? else {
                self.fail_run(
                    run_id,
                    &format!("agent {agent_ref} not found for run {run_id}"),
                )
                .await?;
                continue;
            };
            let visible_tools = match self
                .list_visible_tools(&tenant, &agent.spec.effective_allowed_families())
                .await
            {
                Ok(tools) => tools,
                Err(error) => {
                    self.fail_run(run_id, &format!("failed to resolve run tools: {error}"))
                        .await?;
                    continue;
                }
            };

            let started_at = Utc::now();
            let started_at_raw = started_at.to_rfc3339();
            let mut tx = self.pool.begin().await?;
            let claimed = db::query(
                "UPDATE runs SET status = 'running', started_at = ?, updated_at = ? \
                 WHERE run_id = ? AND status = 'queued' \
                 AND NOT EXISTS (SELECT 1 FROM runs active WHERE active.tenant = ? \
                   AND active.agent_ref = ? AND active.scope = ? AND active.status = 'running')",
            )
            .bind(&started_at_raw)
            .bind(&started_at_raw)
            .bind(run_id.to_string())
            .bind(&tenant)
            .bind(&agent_ref)
            .bind(&scope)
            .execute(&mut tx)
            .await?
            .rows_affected()
                > 0;
            if !claimed {
                tx.rollback().await?;
                continue;
            }
            // Read the policy in the claim transaction and only pair it with
            // the exact owner spec used for this assignment.
            let active_revision =
                behavior::active_revision_for_spec(&mut tx, &tenant, &agent_ref, &agent.spec)
                    .await?;
            audit::record(&mut tx, AuditInput::new(Some(&tenant), "run.claim", "run", Some(&run_id.to_string()), "succeeded", json!({
                "status_before":"queued","status_after":"running","agent_ref":agent_ref,
                "behavior_revision":active_revision.as_ref().map(|revision| revision.revision)
            })).for_run(run_id)).await?;
            tx.commit().await?;
            run.status = AgentRunStatus::Running;
            run.started_at = Some(started_at);
            run.updated_at = started_at;
            return Ok(Some(AssignedRun {
                run,
                timeout_ms: agent.spec.limits.timeout_ms,
                max_steps: agent.spec.limits.max_steps,
                agent_system_prompt: agent.spec.system_prompt,
                agent_model: agent.spec.model,
                agent_temperature: agent.spec.temperature,
                agent_max_tokens: agent.spec.max_tokens,
                agent_context_turns: agent.spec.context_window,
                agent_learned_instructions: active_revision
                    .as_ref()
                    .map(|revision| revision.instructions.clone()),
                agent_behavior_revision: active_revision.map(|revision| revision.revision),
                visible_tools,
            }));
        }
        Ok(None)
    }

    pub async fn reset_local_runtime_state(&self) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        let rows = db::query("SELECT run_id, tenant, error FROM runs WHERE status = 'running'")
            .fetch_all(&mut tx)
            .await?;
        let now = Utc::now().to_rfc3339();
        for row in rows {
            let run_id = parse_uuid_field(&row, "run_id")?;
            let tenant = row.try_get::<String, _>("tenant")?;
            let error = row
                .try_get::<Option<String>, _>("error")?
                .unwrap_or_else(|| "agentd restarted".into());
            let changed = db::query("UPDATE runs SET status = 'failed', error = COALESCE(error, 'agentd restarted'), updated_at = ? WHERE run_id = ? AND status = 'running'")
                .bind(&now).bind(run_id.to_string()).execute(&mut tx).await?.rows_affected();
            if changed == 0 {
                continue;
            }
            Self::append_run_trace(
                &mut tx,
                &tenant,
                run_id,
                "error",
                &json!({"error":error}),
                &now,
            )
            .await?;
            Self::append_run_trace(
                &mut tx,
                &tenant,
                run_id,
                "status",
                &json!({"status":"failed"}),
                &now,
            )
            .await?;
            audit::record(&mut tx, AuditInput::new(Some(&tenant), "run.restart", "run", Some(&run_id.to_string()), "succeeded", json!({
                "status_before":"running","status_after":"failed","error_code":"runtime_restarted"
            })).for_run(run_id)).await?;
        }
        tx.commit().await?;
        Ok(())
    }

    pub async fn append_event(
        &self,
        run_id: Uuid,
        event_type: &str,
        payload: serde_json::Value,
        ts: DateTime<Utc>,
    ) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        let tenant = db::query_scalar::<String>("SELECT tenant FROM runs WHERE run_id = ?")
            .bind(run_id.to_string())
            .fetch_optional(&mut tx)
            .await?
            .ok_or_else(|| missing!("run not found for trace append"))?;
        Self::append_run_trace(
            &mut tx,
            &tenant,
            run_id,
            event_type,
            &payload,
            &ts.to_rfc3339(),
        )
        .await?;
        tx.commit().await?;
        Ok(())
    }

    pub(crate) async fn append_run_trace(
        tx: &mut db::Transaction,
        tenant: &str,
        run_id: Uuid,
        kind: &str,
        payload: &serde_json::Value,
        ts: &str,
    ) -> Result<i64> {
        let trace_id = db::query_scalar::<i64>(
            "INSERT INTO run_log (run_id, kind, payload_json, ts) VALUES (?, ?, ?, ?) RETURNING id",
        )
        .bind(run_id.to_string())
        .bind(kind)
        .bind(payload.to_string())
        .bind(ts)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| anyhow!("trace insert returned no id"))?;
        let empty_summary = json!({});
        let summary = if matches!(kind, "output" | "error") {
            &empty_summary
        } else {
            payload
        };
        let outcome = if matches!(kind, "maintenance_check" | "behavior_check")
            && summary.get("ready").and_then(serde_json::Value::as_bool) == Some(false)
        {
            "skipped"
        } else {
            match summary.get("phase").and_then(serde_json::Value::as_str) {
                Some("request" | "call") => "started",
                Some("error") => "failed",
                Some("response" | "result")
                    if kind == "tool"
                        && payload
                            .pointer("/result/ok")
                            .and_then(serde_json::Value::as_bool)
                            == Some(false) =>
                {
                    "failed"
                }
                Some("response" | "result") => "succeeded",
                _ => "recorded",
            }
        };
        audit::record(
            tx,
            AuditInput::new(
                Some(tenant),
                "run.trace",
                "run_trace",
                Some(&trace_id.to_string()),
                outcome,
                audit_mutations::trace_summary(kind, summary, trace_id),
            )
            .for_run(run_id),
        )
        .await?;
        Ok(trace_id)
    }

    /// Commit the durable result of a successful run in one transaction.
    /// Model/tool trace entries are written while execution is in progress;
    /// this transaction owns the final output, rolling context, terminal
    /// status, and optional outbox row so consumers never observe a partial
    /// success.
    pub async fn finalize_run_success(
        &self,
        run_id: Uuid,
        output: &serde_json::Value,
        context_state: Option<&serde_json::Value>,
    ) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        let row = db::query(
            "SELECT tenant, agent_ref, scope, status, output_json, delivery_destination \
             FROM runs WHERE run_id = ?",
        )
        .bind(run_id.to_string())
        .fetch_optional(&mut tx)
        .await?
        .ok_or_else(|| missing!("run not found for finalization"))?;
        if row.try_get::<Option<String>, _>("output_json")?.is_some() {
            return Err(conflicting!("run output was already finalized"));
        }
        if row.try_get::<String, _>("status")? != "running" {
            return Err(conflicting!("only a running run can be finalized"));
        }
        let tenant = row.try_get::<String, _>("tenant")?;
        let agent = row.try_get::<String, _>("agent_ref")?;
        let scope = row.try_get::<String, _>("scope")?;
        let delivery_destination = row.try_get::<Option<String>, _>("delivery_destination")?;
        let now = Utc::now().to_rfc3339();
        let output_json = serde_json::to_string(output)?;

        let changed = db::query(
            "UPDATE runs SET output_json = ?, error = NULL, status = 'succeeded', updated_at = ? \
             WHERE run_id = ? AND status = 'running'",
        )
        .bind(&output_json)
        .bind(&now)
        .bind(run_id.to_string())
        .execute(&mut tx)
        .await?
        .rows_affected();
        if changed != 1 {
            return Err(conflicting!(
                "run was no longer running during finalization"
            ));
        }

        Self::append_run_trace(&mut tx, &tenant, run_id, "output", output, &now).await?;
        Self::append_run_trace(
            &mut tx,
            &tenant,
            run_id,
            "status",
            &json!({"status":"succeeded"}),
            &now,
        )
        .await?;
        if let Some(state) = context_state {
            let state_json = serde_json::to_string(state)?;
            let revision = db::query_scalar::<i64>(
                "INSERT INTO contexts (tenant, agent, scope, revision, state_json, updated_at) \
                 VALUES (?, ?, ?, 1, ?, ?) \
                 ON CONFLICT(tenant, agent, scope) DO UPDATE SET \
                 revision = contexts.revision + 1, state_json = excluded.state_json, \
                 updated_at = excluded.updated_at RETURNING revision",
            )
            .bind(&tenant)
            .bind(&agent)
            .bind(&scope)
            .bind(&state_json)
            .bind(&now)
            .fetch_optional(&mut tx)
            .await?
            .ok_or_else(|| anyhow!("context update returned no revision"))?;
            audit::record(
                &mut tx,
                AuditInput::new(
                    Some(&tenant),
                    "context.put",
                    "context",
                    Some(&agent),
                    "succeeded",
                    json!({
                        "scope":scope,"revision":revision,"state_bytes":state_json.len()
                    }),
                )
                .for_run(run_id),
            )
            .await?;
        } else {
            let deleted =
                db::query("DELETE FROM contexts WHERE tenant = ? AND agent = ? AND scope = ?")
                    .bind(&tenant)
                    .bind(&agent)
                    .bind(&scope)
                    .execute(&mut tx)
                    .await?
                    .rows_affected()
                    > 0;
            audit::record(
                &mut tx,
                AuditInput::new(
                    Some(&tenant),
                    "context.delete",
                    "context",
                    Some(&agent),
                    audit_mutations::outcome(deleted),
                    json!({
                        "scope":scope,"deleted":deleted
                    }),
                )
                .for_run(run_id),
            )
            .await?;
        }

        if let Some(destination) = delivery_destination {
            let delivery_id = Uuid::new_v4();
            let idempotency_key = format!("run:{run_id}:output");
            let inserted = db::query(
                r#"INSERT INTO deliveries (
                       delivery_id, tenant, run_id, status, destination,
                       payload_json, idempotency_key, attempt, created_at, updated_at
                   ) VALUES (?, ?, ?, 'pending', ?, ?, ?, 0, ?, ?)
                   ON CONFLICT(idempotency_key) DO NOTHING"#,
            )
            .bind(delivery_id.to_string())
            .bind(&tenant)
            .bind(run_id.to_string())
            .bind(destination)
            .bind(&output_json)
            .bind(&idempotency_key)
            .bind(&now)
            .bind(&now)
            .execute(&mut tx)
            .await?
            .rows_affected()
                > 0;
            let actual_id = db::query_scalar::<String>(
                "SELECT delivery_id FROM deliveries WHERE idempotency_key = ?",
            )
            .bind(&idempotency_key)
            .fetch_optional(&mut tx)
            .await?
            .ok_or_else(|| anyhow!("delivery not found after enqueue"))?;
            audit::record(
                &mut tx,
                AuditInput::new(
                    Some(&tenant),
                    "delivery.enqueue",
                    "delivery",
                    Some(&actual_id),
                    audit_mutations::outcome(inserted),
                    json!({
                        "status":"pending","payload_bytes":output_json.len(),"payload_kind":"output"
                    }),
                )
                .for_run(run_id),
            )
            .await?;
        }
        maintenance::checkpoint_success(&mut tx, run_id).await?;
        audit::record(&mut tx, AuditInput::new(Some(&tenant), "run.succeed", "run", Some(&run_id.to_string()), "succeeded", json!({
            "status_before":"running","status_after":"succeeded","output_bytes":output_json.len()
        })).for_run(run_id)).await?;
        tx.commit().await?;
        Ok(())
    }

    /// Atomically persist a terminal failure. Partial model/tool trace remains
    /// intact, context is unchanged, and an explicit destination receives one
    /// transport-neutral failure payload.
    pub async fn fail_run(&self, run_id: Uuid, error: &str) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        let row =
            db::query("SELECT tenant, status, delivery_destination FROM runs WHERE run_id = ?")
                .bind(run_id.to_string())
                .fetch_optional(&mut tx)
                .await?;
        let now = Utc::now().to_rfc3339();
        let changed = db::query(
            "UPDATE runs SET status = 'failed', error = ?, updated_at = ? \
             WHERE run_id = ? AND status IN ('queued', 'running')",
        )
        .bind(error)
        .bind(&now)
        .bind(run_id.to_string())
        .execute(&mut tx)
        .await?
        .rows_affected();
        if changed > 0 {
            if let Some(row) = &row {
                let tenant = row.try_get::<String, _>("tenant")?;
                Self::append_run_trace(
                    &mut tx,
                    &tenant,
                    run_id,
                    "error",
                    &json!({"error":error}),
                    &now,
                )
                .await?;
                let destination = row.try_get::<Option<String>, _>("delivery_destination")?;
                if let Some(destination) = destination {
                    let payload = failure_delivery_payload(error).to_string();
                    let delivery_id = Uuid::new_v4();
                    let idempotency_key = format!("run:{run_id}:failure");
                    let inserted = db::query(
                        r#"INSERT INTO deliveries (
                               delivery_id, tenant, run_id, status, destination,
                               payload_json, idempotency_key, attempt, created_at, updated_at
                           ) VALUES (?, ?, ?, 'pending', ?, ?, ?, 0, ?, ?)
                           ON CONFLICT(idempotency_key) DO NOTHING"#,
                    )
                    .bind(delivery_id.to_string())
                    .bind(&tenant)
                    .bind(run_id.to_string())
                    .bind(destination)
                    .bind(&payload)
                    .bind(&idempotency_key)
                    .bind(&now)
                    .bind(&now)
                    .execute(&mut tx)
                    .await?
                    .rows_affected()
                        > 0;
                    let actual_id = db::query_scalar::<String>(
                        "SELECT delivery_id FROM deliveries WHERE idempotency_key = ?",
                    )
                    .bind(&idempotency_key)
                    .fetch_optional(&mut tx)
                    .await?
                    .ok_or_else(|| anyhow!("delivery not found after failure enqueue"))?;
                    audit::record(&mut tx, AuditInput::new(Some(&tenant), "delivery.enqueue", "delivery", Some(&actual_id), audit_mutations::outcome(inserted), json!({
                        "status":"pending","payload_kind":"failure","payload_bytes":payload.len(),"error_code":audit_mutations::run_error_code(error)
                    })).for_run(run_id)).await?;
                }
            }
        }
        let tenant = row
            .as_ref()
            .map(|row| row.try_get::<String, _>("tenant"))
            .transpose()?;
        let before = row
            .as_ref()
            .map(|row| row.try_get::<String, _>("status"))
            .transpose()?;
        audit::record(&mut tx, AuditInput::new(tenant.as_deref(), "run.fail", "run", Some(&run_id.to_string()), audit_mutations::outcome(changed > 0), json!({
            "status_before":before,"status_after":if changed > 0 { Some("failed") } else { before.as_deref() },
            "error_code":audit_mutations::run_error_code(error)
        })).for_run(run_id)).await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn list_run_log(&self, run_id: Uuid) -> Result<Vec<RunLogEntry>> {
        let rows = db::query(
            "SELECT id, run_id, kind, payload_json, ts \
             FROM run_log WHERE run_id = ? ORDER BY id ASC",
        )
        .bind(run_id.to_string())
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(|row| {
                Ok(RunLogEntry {
                    id: row.try_get("id")?,
                    run_id: parse_uuid_field(&row, "run_id")?,
                    kind: row.try_get("kind")?,
                    payload: decode_json(&row.try_get::<String, _>("payload_json")?)?,
                    ts: parse_ts_field(&row, "ts")?,
                })
            })
            .collect()
    }
}

pub(super) fn status_to_wire(status: AgentRunStatus) -> &'static str {
    match status {
        AgentRunStatus::Queued => "queued",
        AgentRunStatus::Running => "running",
        AgentRunStatus::Succeeded => "succeeded",
        AgentRunStatus::Failed => "failed",
        AgentRunStatus::Cancelled => "cancelled",
    }
}

pub(super) fn status_from_wire(raw: &str) -> Result<AgentRunStatus> {
    match raw {
        "queued" => Ok(AgentRunStatus::Queued),
        "running" => Ok(AgentRunStatus::Running),
        "succeeded" => Ok(AgentRunStatus::Succeeded),
        "failed" => Ok(AgentRunStatus::Failed),
        "cancelled" => Ok(AgentRunStatus::Cancelled),
        other => Err(anyhow!("unknown status {other}")),
    }
}

pub(super) fn row_to_run(row: db::SqlRow) -> Result<AgentRun> {
    Ok(AgentRun {
        run_id: parse_uuid_field(&row, "run_id")?,
        tenant: row.try_get("tenant")?,
        name: row.try_get("name")?,
        agent_ref: row.try_get("agent_ref")?,
        scope: row.try_get("scope")?,
        source: row.try_get("source")?,
        input: decode_json(&row.try_get::<String, _>("input_json")?)?,
        output: row
            .try_get::<Option<String>, _>("output_json")?
            .map(|raw| decode_json(&raw))
            .transpose()?,
        error: row.try_get("error")?,
        status: status_from_wire(&row.try_get::<String, _>("status")?)?,
        request_id: row.try_get("request_id")?,
        created_at: parse_ts_field(&row, "created_at")?,
        started_at: optional_ts_field(&row, "started_at")?,
        updated_at: parse_ts_field(&row, "updated_at")?,
    })
}
