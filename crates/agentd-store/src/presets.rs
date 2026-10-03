use super::*;

#[derive(Clone, Copy, Debug)]
pub enum BuiltinPresetPolicy {
    MemoryMaintenance,
    BehaviorLearning,
}

pub struct BuiltinPresetRequest<'a> {
    pub agent: &'a AgentResource,
    pub schedule_name: &'a str,
    pub schedule: &'a ScheduleSpec,
    pub legacy_schedules: &'a [ScheduleSpec],
    pub policy: BuiltinPresetPolicy,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BuiltinPresetResult {
    pub agent_created: bool,
    pub agent_updated: bool,
    pub schedule_created: bool,
    pub schedule_updated: bool,
}

impl AgentdStore {
    /// Validate, repair and install the reserved pair from a single database
    /// snapshot. A user disabling/editing the schedule cannot be overwritten by
    /// an installer holding an earlier independently read copy.
    pub async fn ensure_builtin_preset(
        &self,
        request: BuiltinPresetRequest<'_>,
    ) -> Result<BuiltinPresetResult> {
        let tenant = &request.agent.metadata.tenant;
        let (agent_name, schedule_name) = match request.policy {
            BuiltinPresetPolicy::MemoryMaintenance => {
                (MEMORY_MAINTAINER_AGENT, MEMORY_MAINTENANCE_SCHEDULE)
            }
            BuiltinPresetPolicy::BehaviorLearning => {
                (BEHAVIOR_LEARNER_AGENT, BEHAVIOR_LEARNING_SCHEDULE)
            }
        };
        if request.agent.metadata.name != agent_name
            || request.schedule_name != schedule_name
            || request.schedule.agent_ref != agent_name
        {
            return Err(validation(
                "builtin preset must use its reserved agent and schedule",
            ));
        }
        request.agent.validate().map_err(validation)?;
        let mut tx = self.pool.begin_immediate().await?;
        Self::ensure_tenant_in_tx(&mut tx, tenant).await?;
        let existing_agent = db::query("SELECT * FROM agents WHERE tenant = ? AND name = ?")
            .bind(tenant)
            .bind(agent_name)
            .fetch_optional(&mut tx)
            .await?
            .map(row_to_agent)
            .transpose()?;
        let existing_schedule = db::query("SELECT * FROM schedules WHERE tenant = ? AND name = ?")
            .bind(tenant)
            .bind(schedule_name)
            .fetch_optional(&mut tx)
            .await?
            .map(row_to_schedule)
            .transpose()?;
        if existing_agent
            .as_ref()
            .is_some_and(|agent| match request.policy {
                BuiltinPresetPolicy::MemoryMaintenance => {
                    agent.spec.allowed_families.as_deref() != Some(&[ToolFamily::Memory])
                }
                BuiltinPresetPolicy::BehaviorLearning => {
                    !agent.spec.effective_allowed_families().is_empty()
                }
            })
        {
            return Err(conflict(
                "reserved preset agent exists with incompatible capabilities",
            ));
        }
        if existing_schedule
            .as_ref()
            .is_some_and(|schedule| schedule.spec.agent_ref != agent_name)
        {
            return Err(conflict(
                "reserved preset schedule targets an incompatible agent",
            ));
        }
        let agent_created = existing_agent.is_none();
        let mut agent = existing_agent
            .as_ref()
            .map(|agent| AgentResource {
                metadata: agent.metadata.clone(),
                spec: agent.spec.clone(),
            })
            .unwrap_or_else(|| request.agent.clone());
        agent.spec.context_window = Some(0);
        if matches!(request.policy, BuiltinPresetPolicy::MemoryMaintenance) {
            agent.spec.model = Some("standard/chat".into());
        }
        let agent_updated = existing_agent
            .as_ref()
            .is_some_and(|existing| existing.spec != agent.spec);
        if agent_created || agent_updated {
            agent.validate().map_err(validation)?;
            Self::write_agent(&mut tx, &agent).await?;
        }
        // Validate requested options even if an existing customized schedule
        // is retained; the installed pair still keeps its existing settings.
        let mut desired =
            Self::normalize_schedule(&mut tx, tenant, schedule_name, request.schedule).await?;
        let schedule_created = existing_schedule.is_none();
        let schedule_updated = existing_schedule.as_ref().is_some_and(|existing| {
            request.legacy_schedules.iter().any(|legacy| {
                let mut legacy = legacy.clone();
                legacy.enabled = existing.spec.enabled;
                legacy == existing.spec
            })
        });
        if let Some(existing) = &existing_schedule {
            desired.enabled = existing.spec.enabled;
        }
        if schedule_created || schedule_updated {
            Self::write_schedule(&mut tx, tenant, schedule_name, &desired).await?;
        }
        tx.commit().await?;
        Ok(BuiltinPresetResult {
            agent_created,
            agent_updated,
            schedule_created,
            schedule_updated,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn defaults() -> (AgentResource, ScheduleSpec) {
        (
            AgentResource {
                metadata: agentd_api::ResourceMeta {
                    tenant: "demo".into(),
                    name: MEMORY_MAINTAINER_AGENT.into(),
                    labels: BTreeMap::new(),
                },
                spec: agentd_api::AgentSpec {
                    allowed_families: Some(vec![ToolFamily::Memory]),
                    limits: agentd_api::AgentLimits {
                        timeout_ms: 300_000,
                        max_steps: 64,
                    },
                    system_prompt: None,
                    model: Some("standard/chat".into()),
                    temperature: None,
                    max_tokens: None,
                    context_window: Some(0),
                },
            },
            ScheduleSpec {
                agent_ref: MEMORY_MAINTAINER_AGENT.into(),
                scope: "memory-maintenance/default".into(),
                payload: json!({"namespace":"*","min_entries":5}),
                delivery: None,
                at: None,
                cron: Some("0 3 * * 0".into()),
                timezone: Some("Asia/Singapore".into()),
                enabled: true,
            },
        )
    }

    #[tokio::test]
    async fn atomic_preset_upgrade_preserves_disabled_schedule_and_custom_agent_fields() {
        let dir = tempfile::tempdir().unwrap();
        let store = AgentdStore::new(dir.path().join("preset.db").to_str().unwrap())
            .await
            .unwrap();
        store.create_tenant("demo", &json!({})).await.unwrap();
        let (mut agent, schedule) = defaults();
        agent.spec.model = Some("custom/model".into());
        agent.spec.context_window = Some(4);
        agent.spec.limits.max_steps = 12;
        store.apply_agent(&agent).await.unwrap();
        let mut legacy = schedule.clone();
        legacy.payload["namespace"] = json!("default");
        legacy.enabled = false;
        store
            .put_schedule("demo", MEMORY_MAINTENANCE_SCHEDULE, &legacy)
            .await
            .unwrap();
        legacy.enabled = true;
        let result = store
            .ensure_builtin_preset(BuiltinPresetRequest {
                agent: &defaults().0,
                schedule_name: MEMORY_MAINTENANCE_SCHEDULE,
                schedule: &schedule,
                legacy_schedules: &[legacy],
                policy: BuiltinPresetPolicy::MemoryMaintenance,
            })
            .await
            .unwrap();
        assert!(result.agent_updated && result.schedule_updated);
        let repaired = store
            .get_agent("demo", MEMORY_MAINTAINER_AGENT)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(repaired.spec.model.as_deref(), Some("standard/chat"));
        assert_eq!(repaired.spec.context_window, Some(0));
        assert_eq!(repaired.spec.limits.max_steps, 12);
        let repaired = store
            .get_schedule("demo", MEMORY_MAINTENANCE_SCHEDULE)
            .await
            .unwrap()
            .unwrap();
        assert!(!repaired.spec.enabled);
        assert_eq!(repaired.spec.payload["namespace"], "*");
        assert!(repaired.next_trigger_at.is_none());
    }

    #[tokio::test]
    async fn preset_schedule_audit_failure_rolls_back_both_resources_with_database_error() {
        let dir = tempfile::tempdir().unwrap();
        let store = AgentdStore::new(dir.path().join("preset.db").to_str().unwrap())
            .await
            .unwrap();
        store.create_tenant("demo", &json!({})).await.unwrap();
        db::query("CREATE TRIGGER reject_schedule BEFORE INSERT ON audit_events WHEN NEW.action = 'schedule.put' BEGIN SELECT RAISE(ABORT, 'reject preset'); END").execute(&store.pool).await.unwrap();
        let (agent, schedule) = defaults();
        let error = store
            .ensure_builtin_preset(BuiltinPresetRequest {
                agent: &agent,
                schedule_name: MEMORY_MAINTENANCE_SCHEDULE,
                schedule: &schedule,
                legacy_schedules: &[],
                policy: BuiltinPresetPolicy::MemoryMaintenance,
            })
            .await
            .unwrap_err();
        assert!(matches!(
            error.downcast_ref::<StoreError>(),
            Some(StoreError::Database(_))
        ));
        assert!(store
            .get_agent("demo", MEMORY_MAINTAINER_AGENT)
            .await
            .unwrap()
            .is_none());
        assert!(store
            .get_schedule("demo", MEMORY_MAINTENANCE_SCHEDULE)
            .await
            .unwrap()
            .is_none());
    }
}
