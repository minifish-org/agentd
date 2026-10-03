use crate::{error_response, AppState};
use agentd_api::{
    AgentLimits, AgentResource, AgentSpec, BehaviorLearningOptions, ResourceMeta, ScheduleSpec,
    ToolFamily, ALL_MEMORY_NAMESPACES, BEHAVIOR_LEARNER_AGENT, BEHAVIOR_LEARNING_SCHEDULE,
    MEMORY_MAINTAINER_AGENT, MEMORY_MAINTENANCE_SCHEDULE,
};
use agentd_store::{AgentdStore, BuiltinPresetPolicy, BuiltinPresetRequest, BuiltinPresetResult};
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
        Self::new(crate::responses::error_status(&error), error)
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
    let agent = preset_agent(
        tenant,
        MEMORY_MAINTAINER_AGENT,
        AgentSpec {
            allowed_families: Some(vec![ToolFamily::Memory]),
            limits: AgentLimits {
                timeout_ms: 300_000,
                max_steps: 64,
            },
            system_prompt: Some(MEMORY_MAINTAINER_PROMPT.into()),
            model: Some("standard/chat".into()),
            temperature: Some(0.1),
            max_tokens: None,
            context_window: Some(0),
        },
        "memory-maintenance",
    );
    let schedule = preset_schedule(
        MEMORY_MAINTAINER_AGENT,
        "memory-maintenance/default",
        "0 3 * * 0",
        serde_json::json!({
            "namespace": ALL_MEMORY_NAMESPACES,
            "min_entries": 5,
            "policy": "Scan the complete namespace and maintain durable memory; leave uncertain entries unchanged"
        }),
    );
    let mut legacy = schedule.clone();
    legacy.payload["namespace"] = serde_json::json!("default");
    let mut legacy_without_threshold = legacy.clone();
    legacy_without_threshold
        .payload
        .as_object_mut()
        .unwrap()
        .remove("min_entries");
    let result = store
        .ensure_builtin_preset(BuiltinPresetRequest {
            agent: &agent,
            schedule_name: MEMORY_MAINTENANCE_SCHEDULE,
            schedule: &schedule,
            legacy_schedules: &[legacy, legacy_without_threshold],
            policy: BuiltinPresetPolicy::MemoryMaintenance,
        })
        .await?;
    Ok(preset_report(
        tenant,
        MEMORY_MAINTAINER_AGENT,
        MEMORY_MAINTENANCE_SCHEDULE,
        result,
    ))
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
    options.validate().map_err(PresetInstallError::invalid)?;
    let agent = preset_agent(
        tenant,
        BEHAVIOR_LEARNER_AGENT,
        AgentSpec {
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
        "behavior-learning",
    );
    let scope = if options.target_agent == "*" {
        "behavior-learning".into()
    } else {
        format!("behavior-learning/{}", options.target_agent)
    };
    let schedule = preset_schedule(
        BEHAVIOR_LEARNER_AGENT,
        &scope,
        "0 4 * * 0",
        serde_json::to_value(options).map_err(PresetInstallError::invalid)?,
    );
    let result = store
        .ensure_builtin_preset(BuiltinPresetRequest {
            agent: &agent,
            schedule_name: BEHAVIOR_LEARNING_SCHEDULE,
            schedule: &schedule,
            legacy_schedules: &[],
            policy: BuiltinPresetPolicy::BehaviorLearning,
        })
        .await?;
    Ok(preset_report(
        tenant,
        BEHAVIOR_LEARNER_AGENT,
        BEHAVIOR_LEARNING_SCHEDULE,
        result,
    ))
}

fn preset_agent(tenant: &str, name: &str, spec: AgentSpec, preset: &str) -> AgentResource {
    AgentResource {
        metadata: ResourceMeta {
            name: name.into(),
            tenant: tenant.into(),
            labels: BTreeMap::from([
                ("agentd.system".into(), "true".into()),
                ("agentd.preset".into(), preset.into()),
            ]),
        },
        spec,
    }
}

fn preset_schedule(
    agent: &str,
    scope: &str,
    cron: &str,
    payload: serde_json::Value,
) -> ScheduleSpec {
    ScheduleSpec {
        agent_ref: agent.into(),
        scope: scope.into(),
        payload,
        delivery: None,
        at: None,
        cron: Some(cron.into()),
        timezone: Some("Asia/Singapore".into()),
        enabled: true,
    }
}

fn preset_report(
    tenant: &str,
    agent_ref: &'static str,
    schedule: &'static str,
    result: BuiltinPresetResult,
) -> PresetInstallReport {
    PresetInstallReport {
        tenant: tenant.into(),
        agent_ref,
        schedule,
        agent_created: result.agent_created,
        agent_updated: result.agent_updated,
        schedule_created: result.schedule_created,
        schedule_updated: result.schedule_updated,
    }
}
