use crate::{error_response, AppState};
use agentd_api::{
    AgentLimits, AgentResource, AgentSpec, BehaviorLearningOptions, ResourceMeta, ScheduleSpec,
    ToolFamily, ALL_MEMORY_NAMESPACES, BEHAVIOR_LEARNER_AGENT, BEHAVIOR_LEARNING_SCHEDULE,
    MEMORY_MAINTAINER_AGENT, MEMORY_MAINTENANCE_SCHEDULE,
};
use agentd_store::AgentdStore;
use axum::{
    body::Bytes,
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde::Serialize;
use std::collections::BTreeMap;

const MEMORY_MAINTAINER_PROMPT: &str = r#"You maintain durable memory for the current tenant only.

Read the namespace from the run input and pass it explicitly in every memory_list, memory_put, and memory_delete call; do not rely on a tool default. Start memory_list with that namespace and no cursor, then keep following next_cursor until it is null. Identify semantic duplicates, explicit conflicts, and clearly expired entries. Never invent a fact that is not supported by the existing memory. Leave uncertain entries unchanged. Make every change through memory_put or memory_delete, keep a stable surviving ID when merging, and delete only when the conclusion is clear.

Return a JSON maintenance report with namespace and integer counts for scanned, updated, merged, deleted, and unchanged entries, plus a concise notes array."#;

#[derive(Debug, Serialize)]
pub(crate) struct PresetInstallReport {
    pub(crate) tenant: String,
    pub(crate) agent_ref: &'static str,
    pub(crate) schedule: &'static str,
    pub(crate) agent_created: bool,
    pub(crate) agent_updated: bool,
    pub(crate) schedule_created: bool,
    pub(crate) schedule_updated: bool,
}

impl IntoResponse for PresetInstallReport {
    fn into_response(self) -> Response {
        let status = if self.agent_created || self.schedule_created {
            StatusCode::CREATED
        } else {
            StatusCode::OK
        };
        (status, Json(self)).into_response()
    }
}

#[derive(Debug)]
pub(crate) struct PresetInstallError {
    status: StatusCode,
    message: String,
}

impl PresetInstallError {
    fn new(status: StatusCode, message: impl ToString) -> Self {
        Self {
            status,
            message: message.to_string(),
        }
    }

    fn invalid(message: impl ToString) -> Self {
        Self::new(StatusCode::BAD_REQUEST, message)
    }

    pub(crate) fn is_conflict(&self) -> bool {
        self.status == StatusCode::CONFLICT
    }
}

impl std::fmt::Display for PresetInstallError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.message, f)
    }
}

impl std::error::Error for PresetInstallError {}

impl From<anyhow::Error> for PresetInstallError {
    fn from(error: anyhow::Error) -> Self {
        Self::invalid(error)
    }
}

impl IntoResponse for PresetInstallError {
    fn into_response(self) -> Response {
        (self.status, Json(serde_json::json!({"error":self.message}))).into_response()
    }
}

pub(crate) async fn ensure_background_presets(
    store: &AgentdStore,
    tenant: &str,
) -> Result<[PresetInstallReport; 2], PresetInstallError> {
    let memory = ensure_memory_maintenance(store, tenant).await?;
    let behavior =
        ensure_behavior_learning(store, tenant, &BehaviorLearningOptions::default()).await?;
    Ok([memory, behavior])
}

pub(crate) async fn install_memory_maintenance(
    State(state): State<AppState>,
    Path(tenant): Path<String>,
) -> impl IntoResponse {
    match ensure_memory_maintenance(&state.store, &tenant).await {
        Ok(report) => report.into_response(),
        Err(error) => error.into_response(),
    }
}

pub(crate) async fn ensure_memory_maintenance(
    store: &AgentdStore,
    tenant: &str,
) -> Result<PresetInstallReport, PresetInstallError> {
    if store.get_tenant(tenant).await?.is_none() {
        return Err(PresetInstallError::new(
            StatusCode::NOT_FOUND,
            "tenant not found",
        ));
    }
    // Validate both reserved resources before repairing either one.
    let existing_agent = store.get_agent(tenant, MEMORY_MAINTAINER_AGENT).await?;
    if existing_agent
        .as_ref()
        .is_some_and(|agent| agent.spec.allowed_families.as_deref() != Some(&[ToolFamily::Memory]))
    {
        return Err(PresetInstallError::new(
            StatusCode::CONFLICT,
            "reserved maintainer agent exists with incompatible capabilities",
        ));
    }
    let existing_schedule = store
        .get_schedule(tenant, MEMORY_MAINTENANCE_SCHEDULE)
        .await?;
    if existing_schedule
        .as_ref()
        .is_some_and(|schedule| schedule.spec.agent_ref != MEMORY_MAINTAINER_AGENT)
    {
        return Err(PresetInstallError::new(
            StatusCode::CONFLICT,
            "reserved maintenance schedule targets an incompatible agent",
        ));
    }
    let agent_created = existing_agent.is_none();
    let agent_updated = existing_agent.as_ref().is_some_and(|agent| {
        agent.spec.model.as_deref() != Some("standard/chat") || agent.spec.context_window != Some(0)
    });
    let agent = if let Some(mut existing) = existing_agent {
        existing.spec.model = Some("standard/chat".to_string());
        existing.spec.context_window = Some(0);
        AgentResource {
            metadata: existing.metadata,
            spec: existing.spec,
        }
    } else {
        AgentResource {
            metadata: ResourceMeta {
                name: MEMORY_MAINTAINER_AGENT.to_string(),
                tenant: tenant.to_string(),
                labels: BTreeMap::from([
                    ("agentd.system".to_string(), "true".to_string()),
                    (
                        "agentd.preset".to_string(),
                        "memory-maintenance".to_string(),
                    ),
                ]),
            },
            spec: AgentSpec {
                allowed_families: Some(vec![ToolFamily::Memory]),
                limits: AgentLimits {
                    timeout_ms: 300_000,
                    max_steps: 64,
                },
                system_prompt: Some(MEMORY_MAINTAINER_PROMPT.to_string()),
                model: Some("standard/chat".to_string()),
                temperature: Some(0.1),
                max_tokens: None,
                context_window: Some(0),
            },
        }
    };
    if agent_created || agent_updated {
        agent.validate().map_err(PresetInstallError::invalid)?;
        store.apply_agent(&agent).await?;
    }
    let schedule = ScheduleSpec {
        agent_ref: MEMORY_MAINTAINER_AGENT.to_string(),
        scope: "memory-maintenance/default".to_string(),
        payload: serde_json::json!({
            "namespace": ALL_MEMORY_NAMESPACES,
            "min_entries": 5,
            "policy": "Scan the complete namespace and maintain durable memory; leave uncertain entries unchanged"
        }),
        delivery: None,
        at: None,
        cron: Some("0 3 * * 0".to_string()),
        timezone: Some("Asia/Singapore".to_string()),
        enabled: true,
    };
    let (schedule_created, schedule_updated) = if let Some(existing) = existing_schedule {
        let mut legacy = schedule.clone();
        legacy.enabled = existing.spec.enabled;
        legacy.payload["namespace"] = serde_json::json!("default");
        let mut legacy_without_threshold = legacy.clone();
        legacy_without_threshold
            .payload
            .as_object_mut()
            .unwrap()
            .remove("min_entries");
        if existing.spec == legacy || existing.spec == legacy_without_threshold {
            let mut updated = schedule;
            updated.enabled = existing.spec.enabled;
            store
                .put_schedule(tenant, MEMORY_MAINTENANCE_SCHEDULE, &updated)
                .await?;
            (false, true)
        } else {
            (false, false)
        }
    } else {
        store
            .put_schedule(tenant, MEMORY_MAINTENANCE_SCHEDULE, &schedule)
            .await?;
        (true, false)
    };
    Ok(PresetInstallReport {
        tenant: tenant.to_string(),
        agent_ref: MEMORY_MAINTAINER_AGENT,
        schedule: MEMORY_MAINTENANCE_SCHEDULE,
        agent_created,
        agent_updated,
        schedule_created,
        schedule_updated,
    })
}

pub(crate) async fn install_behavior_learning(
    State(state): State<AppState>,
    Path(tenant): Path<String>,
    body: Bytes,
) -> impl IntoResponse {
    let options: BehaviorLearningOptions = if body.is_empty() {
        BehaviorLearningOptions::default()
    } else {
        match serde_json::from_slice(&body) {
            Ok(options) => options,
            Err(error) => return error_response(error),
        }
    };
    match ensure_behavior_learning(&state.store, &tenant, &options).await {
        Ok(report) => report.into_response(),
        Err(error) => error.into_response(),
    }
}

pub(crate) async fn ensure_behavior_learning(
    store: &AgentdStore,
    tenant: &str,
    options: &BehaviorLearningOptions,
) -> Result<PresetInstallReport, PresetInstallError> {
    if store.get_tenant(tenant).await?.is_none() {
        return Err(PresetInstallError::new(
            StatusCode::NOT_FOUND,
            "tenant not found",
        ));
    }
    options.validate().map_err(PresetInstallError::invalid)?;
    if options.target_agent != "*" {
        match store.get_agent(tenant, &options.target_agent).await? {
            Some(agent)
                if !agent.name.starts_with("system/")
                    && agent
                        .metadata
                        .labels
                        .get("agentd.system")
                        .map(String::as_str)
                        != Some("true") => {}
            Some(_) => {
                return Err(PresetInstallError::invalid(
                    "learning target must be a foreground agent",
                ))
            }
            None => {
                return Err(PresetInstallError::new(
                    StatusCode::NOT_FOUND,
                    "target agent not found",
                ))
            }
        }
    }
    let existing_agent = store.get_agent(tenant, BEHAVIOR_LEARNER_AGENT).await?;
    if existing_agent
        .as_ref()
        .is_some_and(|agent| !agent.spec.effective_allowed_families().is_empty())
    {
        return Err(PresetInstallError::new(
            StatusCode::CONFLICT,
            "reserved behavior learner exists with incompatible capabilities",
        ));
    }
    let existing_schedule = store
        .get_schedule(tenant, BEHAVIOR_LEARNING_SCHEDULE)
        .await?;
    if existing_schedule
        .as_ref()
        .is_some_and(|schedule| schedule.spec.agent_ref != BEHAVIOR_LEARNER_AGENT)
    {
        return Err(PresetInstallError::new(
            StatusCode::CONFLICT,
            "reserved behavior schedule targets an incompatible agent",
        ));
    }
    let agent_created = existing_agent.is_none();
    let agent_updated = existing_agent
        .as_ref()
        .is_some_and(|agent| agent.spec.context_window != Some(0));
    let agent = if let Some(mut existing) = existing_agent {
        existing.spec.context_window = Some(0);
        AgentResource {
            metadata: existing.metadata,
            spec: existing.spec,
        }
    } else {
        AgentResource {
            metadata: ResourceMeta {
                name: BEHAVIOR_LEARNER_AGENT.to_string(),
                tenant: tenant.to_string(),
                labels: BTreeMap::from([
                    ("agentd.system".to_string(), "true".to_string()),
                    ("agentd.preset".to_string(), "behavior-learning".to_string()),
                ]),
            },
            spec: AgentSpec {
                allowed_families: Some(vec![]),
                limits: AgentLimits {
                    timeout_ms: 900_000,
                    max_steps: 64,
                },
                system_prompt: None,
                model: None,
                temperature: None,
                max_tokens: None,
                context_window: Some(0),
            },
        }
    };
    if agent_created || agent_updated {
        agent.validate().map_err(PresetInstallError::invalid)?;
        store.apply_agent(&agent).await?;
    }
    let schedule_created = existing_schedule.is_none();
    if schedule_created {
        let schedule = ScheduleSpec {
            agent_ref: BEHAVIOR_LEARNER_AGENT.to_string(),
            scope: if options.target_agent == "*" {
                "behavior-learning".to_string()
            } else {
                format!("behavior-learning/{}", options.target_agent)
            },
            payload: serde_json::to_value(options).map_err(PresetInstallError::invalid)?,
            delivery: None,
            at: None,
            cron: Some("0 4 * * 0".to_string()),
            timezone: Some("Asia/Singapore".to_string()),
            enabled: true,
        };
        store
            .put_schedule(tenant, BEHAVIOR_LEARNING_SCHEDULE, &schedule)
            .await?;
    }
    Ok(PresetInstallReport {
        tenant: tenant.to_string(),
        agent_ref: BEHAVIOR_LEARNER_AGENT,
        schedule: BEHAVIOR_LEARNING_SCHEDULE,
        agent_created,
        agent_updated,
        schedule_created,
        schedule_updated: false,
    })
}
