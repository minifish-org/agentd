use super::*;

impl AgentdStore {
    pub async fn put_schedule(&self, tenant: &str, name: &str, spec: &ScheduleSpec) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        let spec = Self::normalize_schedule(&mut tx, tenant, name, spec).await?;
        Self::write_schedule(&mut tx, tenant, name, &spec).await?;
        tx.commit().await?;
        Ok(())
    }

    pub(super) async fn normalize_schedule(
        tx: &mut db::Transaction,
        tenant: &str,
        name: &str,
        spec: &ScheduleSpec,
    ) -> Result<ScheduleSpec> {
        spec.validate().map_err(validation)?;
        let mut spec = spec.clone();
        if spec.agent_ref == MEMORY_MAINTAINER_AGENT {
            let min_entries = memory_maintenance_min_entries(&spec.payload)?;
            let namespace = spec
                .payload
                .get("namespace")
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| invalid!("memory maintenance schedule requires namespace"))?;
            let namespace = normalize_memory_component(namespace, "namespace")?;
            if namespace == ALL_MEMORY_NAMESPACES && name != MEMORY_MAINTENANCE_SCHEDULE {
                return Err(invalid!(
                    "wildcard memory maintenance requires the reserved schedule"
                ));
            }
            spec.scope = format!("memory-maintenance/{namespace}");
            spec.payload["namespace"] = json!(namespace);
            spec.payload["min_entries"] = json!(min_entries);
        }
        if spec.agent_ref == BEHAVIOR_LEARNER_AGENT {
            if spec.delivery.is_some() {
                return Err(invalid!("behavior learning runs do not support delivery"));
            }
            let options: agentd_api::BehaviorLearningOptions =
                serde_json::from_value(spec.payload.clone()).map_err(validation)?;
            options.validate().map_err(validation)?;
            if options.target_agent == "*" {
                if name != BEHAVIOR_LEARNING_SCHEDULE {
                    return Err(invalid!("wildcard behavior learning requires the reserved {BEHAVIOR_LEARNING_SCHEDULE} schedule"));
                }
            } else {
                let target = db::query("SELECT * FROM agents WHERE tenant = ? AND name = ?")
                    .bind(tenant)
                    .bind(&options.target_agent)
                    .fetch_optional(&mut *tx)
                    .await?
                    .map(row_to_agent)
                    .transpose()?
                    .ok_or_else(|| not_found("learning target agent not found"))?;
                if !behavior::is_foreground_agent(&target) {
                    return Err(validation("learning target must be a foreground agent"));
                }
            }
            spec.scope = format!("behavior-learning/{}", options.target_agent);
            // Persist defaults explicitly so the scheduler sees '*' even when
            // the caller supplies an empty options object.
            spec.payload = serde_json::to_value(options)?;
        }
        Self::ensure_tenant_in_tx(tx, tenant).await?;
        if db::query_scalar::<String>("SELECT name FROM agents WHERE tenant = ? AND name = ?")
            .bind(tenant)
            .bind(&spec.agent_ref)
            .fetch_optional(&mut *tx)
            .await?
            .is_none()
        {
            return Err(not_found(format!(
                "unknown schedule agent: {}",
                spec.agent_ref
            )));
        }
        Ok(spec)
    }

    pub(super) async fn write_schedule(
        tx: &mut db::Transaction,
        tenant: &str,
        name: &str,
        spec: &ScheduleSpec,
    ) -> Result<()> {
        let previous = db::query_scalar::<String>(
            "SELECT spec_json FROM schedules WHERE tenant = ? AND name = ?",
        )
        .bind(tenant)
        .bind(name)
        .fetch_optional(&mut *tx)
        .await?
        .map(|raw| decode_json::<ScheduleSpec>(&raw))
        .transpose()?;
        let now = Utc::now();
        let next_trigger_at = next_trigger_time(spec, now)?;
        let now_s = now.to_rfc3339();
        db::query(
            r#"INSERT INTO schedules (
                   tenant, name, spec_json, last_triggered_at, next_trigger_at,
                   last_run_id, created_at, updated_at
               ) VALUES (?, ?, ?, NULL, ?, NULL, ?, ?)
               ON CONFLICT(tenant, name) DO UPDATE SET
                 spec_json=excluded.spec_json,
                 next_trigger_at=excluded.next_trigger_at,
                 updated_at=excluded.updated_at"#,
        )
        .bind(tenant)
        .bind(name)
        .bind(serde_json::to_string(&spec)?)
        .bind(next_trigger_at.map(|value| value.to_rfc3339()))
        .bind(&now_s)
        .bind(&now_s)
        .execute(&mut *tx)
        .await?;
        audit::record(&mut *tx, AuditInput::new(Some(tenant), "schedule.put", "schedule", Some(name), "succeeded", json!({
            "created":previous.is_none(),"enabled_before":previous.as_ref().map(|s|s.enabled),"enabled_after":spec.enabled,
            "agent_ref":spec.agent_ref,"cron":spec.cron,"at":spec.at,"timezone":spec.timezone,
            "payload_changed":previous.as_ref().is_none_or(|s|s.payload != spec.payload),"delivery_configured":spec.delivery.is_some(),"next_trigger_at":next_trigger_at
        }))).await?;
        Ok(())
    }

    pub async fn get_schedule(&self, tenant: &str, name: &str) -> Result<Option<Schedule>> {
        let row = db::query(
            "SELECT tenant, name, spec_json, last_triggered_at, next_trigger_at, last_run_id, created_at, updated_at FROM schedules WHERE tenant = ? AND name = ?",
        )
        .bind(tenant)
        .bind(name)
        .fetch_optional(&self.pool)
        .await?;
        row.map(row_to_schedule).transpose()
    }

    pub async fn list_schedules(&self, tenant: Option<&str>) -> Result<Vec<Schedule>> {
        let rows = if let Some(tenant) = tenant {
            db::query(
                "SELECT tenant, name, spec_json, last_triggered_at, next_trigger_at, last_run_id, created_at, updated_at FROM schedules WHERE tenant = ? ORDER BY name",
            )
            .bind(tenant)
            .fetch_all(&self.pool)
            .await?
        } else {
            db::query(
                "SELECT tenant, name, spec_json, last_triggered_at, next_trigger_at, last_run_id, created_at, updated_at FROM schedules ORDER BY tenant, name",
            )
            .fetch_all(&self.pool)
            .await?
        };
        rows.into_iter().map(row_to_schedule).collect()
    }

    pub async fn delete_schedule(&self, tenant: &str, name: &str) -> Result<serde_json::Value> {
        let mut tx = self.pool.begin().await?;
        let deleted = db::query("DELETE FROM schedules WHERE tenant = ? AND name = ?")
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
                "schedule.delete",
                "schedule",
                Some(name),
                audit_mutations::outcome(deleted),
                json!({"deleted":deleted}),
            ),
        )
        .await?;
        tx.commit().await?;
        Ok(json!({"deleted": deleted, "name": name}))
    }

    pub async fn trigger_due_schedules(
        &self,
        now: DateTime<Utc>,
        limit: usize,
    ) -> Result<Vec<Uuid>> {
        fn identity(spec: &ScheduleSpec) -> serde_json::Value {
            json!({
                "agent_ref":spec.agent_ref,"scope":spec.scope,
                "namespace":spec.payload.get("namespace").and_then(serde_json::Value::as_str),
                "target_agent":spec.payload.get("target_agent").and_then(serde_json::Value::as_str),
            })
        }
        let rows = match db::query(
            "SELECT tenant, name, spec_json, created_at, next_trigger_at FROM schedules WHERE next_trigger_at IS NOT NULL AND next_trigger_at <= ? ORDER BY next_trigger_at ASC LIMIT ?",
        )
        .bind(now.to_rfc3339())
        .bind(limit.max(1) as i64)
        .fetch_all(&self.pool)
        .await {
            Ok(rows) => rows,
            Err(error) => {
                self.append_audit(AuditInput::new(None, "schedule.tick", "scheduler", None, "failed",
                    json!({"reason":"due_schedule_query_failed"}))).await?;
                return Err(error.into());
            }
        };

        let mut triggered = Vec::new();
        for row in rows {
            let tenant = row.try_get::<String, _>("tenant")?;
            let name = row.try_get::<String, _>("name")?;
            // The persisted due time and resource incarnation identify this
            // occurrence across later ticks and process restarts.
            let spec_json = row.try_get::<String, _>("spec_json")?;
            let created_at = row.try_get::<String, _>("created_at")?;
            let scheduled_at = row.try_get::<String, _>("next_trigger_at")?;
            let occurrence = format!(
                "schedule:{}:",
                hex_encode(Sha256::digest(serde_json::to_vec(&(
                    &name,
                    &created_at,
                    &scheduled_at,
                    &spec_json
                ))?))
            );
            let committed = db::query("SELECT request_id, run_id FROM runs WHERE tenant = ? AND request_id LIKE ? ORDER BY created_at, run_id")
                .bind(&tenant).bind(format!("{occurrence}%")).fetch_all(&self.pool).await?;
            let committed = committed
                .into_iter()
                .map(|row| {
                    Ok((
                        row.try_get::<String, _>("request_id")?,
                        parse_uuid_field(&row, "run_id")?,
                    ))
                })
                .collect::<Result<Vec<_>>>()?;
            let committed_by_request = committed.iter().cloned().collect::<BTreeMap<_, _>>();
            let spec: ScheduleSpec = match decode_json(&spec_json) {
                Ok(spec) => spec,
                Err(error) => {
                    self.append_audit(AuditInput::new(
                        Some(&tenant),
                        "schedule.trigger",
                        "schedule",
                        Some(&name),
                        "failed",
                        json!({"reason":"invalid_schedule","queued_count":0}),
                    ))
                    .await?;
                    return Err(error);
                }
            };
            let next_trigger = match next_trigger_time(&spec, now + chrono::Duration::seconds(1)) {
                Ok(next) => next,
                Err(error) => {
                    self.append_audit(AuditInput::new(
                        Some(&tenant),
                        "schedule.trigger",
                        "schedule",
                        Some(&name),
                        "failed",
                        json!({"reason":"next_trigger_failed","queued_count":0}),
                    ))
                    .await?;
                    return Err(error);
                }
            };
            let discovered: Result<Vec<ScheduleSpec>> = async {
                if name == MEMORY_MAINTENANCE_SCHEDULE
                    && spec.agent_ref == MEMORY_MAINTAINER_AGENT
                    && spec.payload.get("namespace").and_then(serde_json::Value::as_str) == Some(ALL_MEMORY_NAMESPACES)
                {
                    let namespaces = db::query(
                        "SELECT DISTINCT namespace FROM memory WHERE tenant = ? AND namespace != ? ORDER BY namespace",
                    ).bind(&tenant).bind(MEMORY_MAINTAINER_AGENT).fetch_all(&self.pool).await?;
                    namespaces.into_iter().map(|namespace| {
                        let mut target = spec.clone();
                        target.payload["namespace"] = json!(namespace.try_get::<String, _>("namespace")?);
                        Ok(target)
                    }).collect()
                } else if name == BEHAVIOR_LEARNING_SCHEDULE
                    && spec.agent_ref == BEHAVIOR_LEARNER_AGENT
                    && spec.payload.get("target_agent").and_then(serde_json::Value::as_str) == Some("*")
                {
                    Ok(self.list_agents(Some(&tenant)).await?.into_iter()
                        .filter(behavior::is_foreground_agent).map(|agent| {
                            let mut target = spec.clone();
                            target.payload["target_agent"] = json!(agent.name);
                            target.scope = format!("behavior-learning/{}", agent.name);
                            target
                        }).collect())
                } else {
                    Ok(vec![spec.clone()])
                }
            }.await;
            let (targets, mut failure) = match discovered {
                Ok(targets) => (targets, None),
                Err(error) => (Vec::new(), Some(("target_discovery_failed", error))),
            };
            let targets = targets
                .into_iter()
                .map(|target| {
                    let request_id = format!(
                        "{occurrence}{}",
                        hex_encode(Sha256::digest(serde_json::to_vec(&target)?))
                    );
                    Ok((request_id, target))
                })
                .collect::<Result<Vec<_>>>()?;
            let target_keys = targets
                .iter()
                .map(|(request_id, _)| request_id.clone())
                .collect::<BTreeSet<_>>();
            let target_count = targets.len()
                + committed_by_request
                    .keys()
                    .filter(|request_id| !target_keys.contains(*request_id))
                    .count();
            // Retain already committed targets even when discovery/readiness
            // changes after an interrupted partial fan-out.
            let mut queued = committed
                .iter()
                .map(|(_, run_id)| *run_id)
                .collect::<Vec<_>>();
            let mut skipped = 0usize;
            if failure.is_none() && targets.is_empty() && committed.is_empty() {
                if let Err(error) = self
                    .append_audit(AuditInput::new(
                        Some(&tenant),
                        "schedule.decision",
                        "schedule",
                        Some(&name),
                        "skipped",
                        json!({"reason":"no_targets","agent_ref":spec.agent_ref,"target_count":0}),
                    ))
                    .await
                {
                    failure = Some(("decision_audit_failed", error));
                }
            }
            for (request_id, target) in targets {
                if let Some(run_id) = committed_by_request.get(&request_id) {
                    let mut details = identity(&target);
                    details["reason"] = json!("occurrence_reused");
                    details["reused"] = json!(true);
                    if let Err(error) = self
                        .append_audit(
                            AuditInput::new(
                                Some(&tenant),
                                "schedule.decision",
                                "schedule",
                                Some(&name),
                                "queued",
                                details,
                            )
                            .for_run(*run_id),
                        )
                        .await
                    {
                        failure = Some(("decision_audit_failed", error));
                    }
                    continue;
                }
                if failure.is_some() {
                    skipped += 1;
                    let mut details = identity(&target);
                    details["reason"] = json!("fanout_aborted");
                    if let Err(error) = self
                        .append_audit(AuditInput::new(
                            Some(&tenant),
                            "schedule.decision",
                            "schedule",
                            Some(&name),
                            "skipped",
                            details,
                        ))
                        .await
                    {
                        failure = Some(("decision_audit_failed", error));
                    }
                    continue;
                }
                let mut details = match self.background_schedule_ready(&tenant, &target).await {
                    Ok(details) => details,
                    Err(error) => {
                        let mut details = identity(&target);
                        details["reason"] = json!("readiness_failed");
                        let audit_result = self
                            .append_audit(AuditInput::new(
                                Some(&tenant),
                                "schedule.decision",
                                "schedule",
                                Some(&name),
                                "failed",
                                details,
                            ))
                            .await;
                        failure = Some(("readiness_failed", audit_result.err().unwrap_or(error)));
                        continue;
                    }
                };
                if details["ready"] != true {
                    skipped += 1;
                    if let Err(error) = self
                        .append_audit(AuditInput::new(
                            Some(&tenant),
                            "schedule.decision",
                            "schedule",
                            Some(&name),
                            "skipped",
                            details,
                        ))
                        .await
                    {
                        failure = Some(("decision_audit_failed", error));
                    }
                    continue;
                }
                let run_name = format!("{}-{}", name, Uuid::new_v4().simple());
                let run_id = match self
                    .submit_schedule_run(&tenant, &name, &target, &run_name, &request_id, now)
                    .await
                {
                    Ok(run_id) => run_id,
                    Err(error) => {
                        details["reason"] = json!("enqueue_failed");
                        let audit_result = self
                            .append_audit(AuditInput::new(
                                Some(&tenant),
                                "schedule.decision",
                                "schedule",
                                Some(&name),
                                "failed",
                                details,
                            ))
                            .await;
                        failure = Some(("enqueue_failed", audit_result.err().unwrap_or(error)));
                        continue;
                    }
                };
                // Submission has committed. Keep this identity even if a later
                // trace/audit write fails, so a partial fan-out remains visible.
                queued.push(run_id);
                triggered.push(run_id);
                let queue_audit: Result<()> = async {
                    let mut tx = self.pool.begin().await?;
                    audit::record(
                        &mut tx,
                        AuditInput::new(
                            Some(&tenant),
                            "schedule.decision",
                            "schedule",
                            Some(&name),
                            "queued",
                            details,
                        )
                        .for_run(run_id),
                    )
                    .await?;
                    Self::append_run_trace(
                        &mut tx,
                        &tenant,
                        run_id,
                        "status",
                        &json!({"status":"queued","source":"schedule"}),
                        &now.to_rfc3339(),
                    )
                    .await?;
                    tx.commit().await?;
                    Ok(())
                }
                .await;
                if let Err(error) = queue_audit {
                    failure = Some(("queue_audit_failed", error));
                }
            }
            let outcome = if failure.is_some() {
                if queued.is_empty() {
                    "failed"
                } else {
                    "partial"
                }
            } else {
                "completed"
            };
            let summary: Result<()> = async {
                let mut tx = self.pool.begin().await?;
                let changed = db::query(
                    "UPDATE schedules SET last_triggered_at = ?, next_trigger_at = ?, last_run_id = COALESCE(?, last_run_id), updated_at = ? WHERE tenant = ? AND name = ? AND created_at = ? AND spec_json = ? AND next_trigger_at = ?",
                ).bind(now.to_rfc3339()).bind(next_trigger.map(|value| value.to_rfc3339()))
                    .bind(queued.last().map(Uuid::to_string)).bind(now.to_rfc3339())
                    .bind(&tenant).bind(&name).bind(&created_at).bind(&spec_json).bind(&scheduled_at)
                    .execute(&mut tx).await?.rows_affected();
                if changed != 1 {
                    return Err(StoreError::Conflict("schedule changed during triggering".into()).into());
                }
                audit::record(&mut tx, AuditInput::new(Some(&tenant), "schedule.trigger", "schedule", Some(&name), outcome,
                    json!({"reason":failure.as_ref().map(|(reason,_)|*reason),"target_count":target_count,
                        "queued_count":queued.len(),"skipped_count":skipped,"run_ids":queued.iter().take(256).collect::<Vec<_>>(),
                        "run_ids_truncated":queued.len() > 256,
                        "triggered_at":now,"next_trigger_at":next_trigger}),
                )).await?;
                tx.commit().await?;
                Ok(())
            }.await;
            if let Err(error) = summary {
                self.append_audit(AuditInput::new(Some(&tenant), "schedule.trigger", "schedule", Some(&name),
                    if queued.is_empty() {"failed"} else {"partial"},
                    json!({"reason":"summary_commit_failed","queued_count":queued.len(),
                        "run_ids":queued.iter().take(256).collect::<Vec<_>>(),"run_ids_truncated":queued.len() > 256}),
                )).await?;
                return Err(error);
            }
            if let Some((_, error)) = failure {
                return Err(error);
            }
        }
        Ok(triggered)
    }

    pub(super) async fn background_schedule_ready(
        &self,
        tenant: &str,
        spec: &ScheduleSpec,
    ) -> Result<serde_json::Value> {
        if spec.agent_ref == BEHAVIOR_LEARNER_AGENT {
            let options: agentd_api::BehaviorLearningOptions =
                serde_json::from_value(spec.payload.clone()).map_err(StoreError::database)?;
            let state = self.behavior_learning_readiness(tenant, &options).await?;
            return Ok(
                json!({"ready":state.ready,"reason":state.reason,"agent_ref":spec.agent_ref,
                "target_agent":options.target_agent,"source_runs":state.source_runs,"independent_scopes":state.independent_scopes,
                "min_samples":options.min_samples,"required_scopes":2,"max_samples":options.max_samples,
                "pending":state.reason == "already_pending"}),
            );
        }
        if spec.agent_ref == MEMORY_MAINTAINER_AGENT {
            let namespace = maintenance::maintenance_namespace(&spec.payload)?;
            let scope = format!("memory-maintenance/{namespace}");
            let pending = db::query_scalar::<String>(
                "SELECT run_id FROM runs WHERE tenant = ? AND agent_ref = ? AND scope = ? AND status IN ('queued', 'running') LIMIT 1",
            ).bind(tenant).bind(MEMORY_MAINTAINER_AGENT).bind(scope).fetch_optional(&self.pool).await?;
            let min_entries = memory_maintenance_min_entries(&spec.payload)?;
            let state = self
                .memory_maintenance_readiness(tenant, &namespace, min_entries)
                .await?;
            return Ok(json!({"ready":state.ready && pending.is_none(),
                "reason":if pending.is_some() {"already_pending"} else {state.reason.as_deref().unwrap_or("ready")},
                "agent_ref":spec.agent_ref,"namespace":namespace,"entries":state.entries,"min_entries":min_entries,
                "external_revision":state.external_revision,"pending":pending.is_some()}));
        }
        Ok(
            json!({"ready":true,"reason":"ready","agent_ref":spec.agent_ref,"scope":spec.scope,"pending":false}),
        )
    }

    pub(super) async fn submit_schedule_run(
        &self,
        tenant: &str,
        schedule_name: &str,
        spec: &ScheduleSpec,
        run_name: &str,
        request_id: &str,
        now: DateTime<Utc>,
    ) -> Result<Uuid> {
        let mut input = json!({
            "activation": "schedule",
            "schedule_name": schedule_name,
            "input": spec.payload,
            "triggered_at": now.to_rfc3339(),
        });
        if spec.agent_ref == MEMORY_MAINTAINER_AGENT {
            if let Some(namespace) = spec.payload.get("namespace") {
                input["namespace"] = namespace.clone();
            }
        }
        // Keep post-submission writes in the caller, where the committed run ID
        // is available for partial-failure audit summaries.
        self.submit_run(NewRun {
            tenant,
            name: run_name,
            agent_ref: &spec.agent_ref,
            scope: &spec.scope,
            source: "schedule",
            input: &input,
            request_id: Some(request_id),
            schedule_name: Some(schedule_name),
            delivery_destination: spec
                .delivery
                .as_ref()
                .map(|delivery| delivery.destination.as_str()),
        })
        .await
    }
}

pub(super) fn next_trigger_time(
    spec: &ScheduleSpec,
    after: DateTime<Utc>,
) -> Result<Option<DateTime<Utc>>> {
    if !spec.enabled {
        return Ok(None);
    }
    if let Some(at) = spec.at {
        if at >= after {
            return Ok(Some(at));
        }
        return Ok(None);
    }
    if let Some(cron) = spec.cron.as_deref() {
        let timezone = schedule_timezone(spec)?;
        return next_cron_occurrence(cron, timezone, after);
    }
    Ok(None)
}

pub(super) fn schedule_timezone(spec: &ScheduleSpec) -> Result<agentd_api::ResolvedTimezone> {
    let timezone = spec
        .timezone
        .as_deref()
        .ok_or_else(|| invalid!("schedule timezone is required"))?;
    agentd_api::ResolvedTimezone::parse(timezone).map_err(validation)
}

pub(super) fn next_cron_occurrence(
    expr: &str,
    timezone: agentd_api::ResolvedTimezone,
    after: DateTime<Utc>,
) -> Result<Option<DateTime<Utc>>> {
    validate_cron_expression(expr).map_err(validation)?;
    let cron = Cron::from_str(expr)?;
    let next = match timezone {
        agentd_api::ResolvedTimezone::Named(_, timezone) => cron
            .find_next_occurrence(&after.with_timezone(&timezone), false)?
            .with_timezone(&Utc),
        agentd_api::ResolvedTimezone::Fixed(_, timezone) => cron
            .find_next_occurrence(&after.with_timezone(&timezone), false)?
            .with_timezone(&Utc),
    };
    Ok(Some(next))
}

pub(super) fn row_to_schedule(row: db::SqlRow) -> Result<Schedule> {
    let spec: ScheduleSpec = decode_json(&row.try_get::<String, _>("spec_json")?)?;
    Ok(Schedule {
        tenant: row.try_get("tenant")?,
        name: row.try_get("name")?,
        spec,
        last_triggered_at: optional_ts_field(&row, "last_triggered_at")?,
        next_trigger_at: optional_ts_field(&row, "next_trigger_at")?,
        last_run_id: optional_uuid_field(&row, "last_run_id")?,
        created_at: parse_ts_field(&row, "created_at")?,
        updated_at: parse_ts_field(&row, "updated_at")?,
    })
}
