use crate::{error_response, AppState};
use agentd_api::{
    AgentLimits, AgentResource, AgentSpec, BehaviorLearningOptions, ResourceMeta, ScheduleSpec,
    ToolFamily, ALL_MEMORY_NAMESPACES, BEHAVIOR_LEARNER_AGENT, BEHAVIOR_LEARNING_SCHEDULE,
    MEMORY_MAINTAINER_AGENT, MEMORY_MAINTENANCE_SCHEDULE,
};
use axum::{
    body::Bytes,
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use std::collections::BTreeMap;

const MEMORY_MAINTAINER_PROMPT: &str = r#"You maintain durable memory for the current tenant only.

Read the namespace from the run input and pass it explicitly in every memory_list, memory_put, and memory_delete call; do not rely on a tool default. Start memory_list with that namespace and no cursor, then keep following next_cursor until it is null. Identify semantic duplicates, explicit conflicts, and clearly expired entries. Never invent a fact that is not supported by the existing memory. Leave uncertain entries unchanged. Make every change through memory_put or memory_delete, keep a stable surviving ID when merging, and delete only when the conclusion is clear.

Return a JSON maintenance report with namespace and integer counts for scanned, updated, merged, deleted, and unchanged entries, plus a concise notes array."#;

pub(crate) async fn install_memory_maintenance(
    State(state): State<AppState>,
    Path(tenant): Path<String>,
) -> impl IntoResponse {
    match state.store.get_tenant(&tenant).await {
        Ok(Some(_)) => {}
        Ok(None) => {
            return (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({"error":"tenant not found"})),
            )
                .into_response();
        }
        Err(error) => return error_response(error),
    }

    let mut labels = BTreeMap::new();
    labels.insert("agentd.system".to_string(), "true".to_string());
    labels.insert(
        "agentd.preset".to_string(),
        "memory-maintenance".to_string(),
    );
    let agent = AgentResource {
        metadata: ResourceMeta {
            name: MEMORY_MAINTAINER_AGENT.to_string(),
            tenant: tenant.clone(),
            labels,
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
    };
    if let Err(error) = agent.validate() {
        return error_response(error.to_string());
    }

    let agent_created = match state
        .store
        .get_agent(&tenant, MEMORY_MAINTAINER_AGENT)
        .await
    {
        Ok(Some(mut existing))
            if existing.spec.allowed_families.as_deref() == Some(&[ToolFamily::Memory]) =>
        {
            if existing.spec.model.as_deref() != Some("standard/chat") {
                existing.spec.model = Some("standard/chat".to_string());
                let updated = AgentResource {
                    metadata: existing.metadata,
                    spec: existing.spec,
                };
                if let Err(error) = state.store.apply_agent(&updated).await {
                    return error_response(error);
                }
            }
            false
        }
        Ok(Some(_)) => {
            return (
                StatusCode::CONFLICT,
                Json(serde_json::json!({
                    "error":"reserved maintainer agent exists with incompatible capabilities"
                })),
            )
                .into_response();
        }
        Ok(None) => {
            if let Err(error) = state.store.apply_agent(&agent).await {
                return error_response(error);
            }
            true
        }
        Err(error) => return error_response(error),
    };

    let schedule = ScheduleSpec {
        agent_ref: MEMORY_MAINTAINER_AGENT.to_string(),
        scope: "memory-maintenance/default".to_string(),
        payload: serde_json::json!({
            "namespace": ALL_MEMORY_NAMESPACES,
            "policy": "Scan the complete namespace and maintain durable memory; leave uncertain entries unchanged"
        }),
        delivery: None,
        at: None,
        cron: Some("0 3 * * 0".to_string()),
        timezone: Some("Asia/Singapore".to_string()),
        enabled: false,
    };
    let (schedule_created, schedule_updated) = match state
        .store
        .get_schedule(&tenant, MEMORY_MAINTENANCE_SCHEDULE)
        .await
    {
        Ok(Some(existing)) if existing.spec.agent_ref == MEMORY_MAINTAINER_AGENT => {
            let mut legacy = schedule.clone();
            legacy.enabled = existing.spec.enabled;
            legacy.payload["namespace"] = serde_json::json!("default");
            if existing.spec == legacy {
                let mut updated = schedule.clone();
                updated.enabled = existing.spec.enabled;
                if let Err(error) = state
                    .store
                    .put_schedule(&tenant, MEMORY_MAINTENANCE_SCHEDULE, &updated)
                    .await
                {
                    return error_response(error);
                }
                (false, true)
            } else {
                (false, false)
            }
        }
        Ok(Some(_)) => {
            return (
                StatusCode::CONFLICT,
                Json(serde_json::json!({
                    "error":"reserved maintenance schedule targets an incompatible agent"
                })),
            )
                .into_response();
        }
        Ok(None) => {
            if let Err(error) = state
                .store
                .put_schedule(&tenant, MEMORY_MAINTENANCE_SCHEDULE, &schedule)
                .await
            {
                return error_response(error);
            }
            (true, false)
        }
        Err(error) => return error_response(error),
    };

    (
        if agent_created || schedule_created {
            StatusCode::CREATED
        } else {
            StatusCode::OK
        },
        Json(serde_json::json!({
            "tenant": tenant,
            "agent_ref": MEMORY_MAINTAINER_AGENT,
            "schedule": MEMORY_MAINTENANCE_SCHEDULE,
            "agent_created": agent_created,
            "schedule_created": schedule_created,
            "schedule_updated": schedule_updated
        })),
    )
        .into_response()
}

pub(crate) async fn install_behavior_learning(
    State(state): State<AppState>,
    Path(tenant): Path<String>,
    body: Bytes,
) -> impl IntoResponse {
    match state.store.get_tenant(&tenant).await {
        Ok(Some(_)) => {}
        Ok(None) => {
            return (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({"error":"tenant not found"})),
            )
                .into_response();
        }
        Err(error) => return error_response(error),
    }
    let options: BehaviorLearningOptions = if body.is_empty() {
        BehaviorLearningOptions::default()
    } else {
        match serde_json::from_slice(&body) {
            Ok(options) => options,
            Err(error) => return error_response(error),
        }
    };
    if let Err(error) = options.validate() {
        return error_response(error);
    }
    if options.target_agent != "*" {
        match state.store.get_agent(&tenant, &options.target_agent).await {
            Ok(Some(agent))
                if !agent.name.starts_with("system/")
                    && agent
                        .metadata
                        .labels
                        .get("agentd.system")
                        .map(String::as_str)
                        != Some("true") => {}
            Ok(Some(_)) => return error_response("learning target must be a foreground agent"),
            Ok(None) => {
                return (
                    StatusCode::NOT_FOUND,
                    Json(serde_json::json!({"error":"target agent not found"})),
                )
                    .into_response();
            }
            Err(error) => return error_response(error),
        }
    }

    let existing_agent = match state.store.get_agent(&tenant, BEHAVIOR_LEARNER_AGENT).await {
        Ok(agent) => agent,
        Err(error) => return error_response(error),
    };
    if existing_agent
        .as_ref()
        .is_some_and(|agent| !agent.spec.effective_allowed_families().is_empty())
    {
        return (
            StatusCode::CONFLICT,
            Json(serde_json::json!({
                "error":"reserved behavior learner exists with incompatible capabilities"
            })),
        )
            .into_response();
    }
    let existing_schedule = match state
        .store
        .get_schedule(&tenant, BEHAVIOR_LEARNING_SCHEDULE)
        .await
    {
        Ok(schedule) => schedule,
        Err(error) => return error_response(error),
    };
    if existing_schedule
        .as_ref()
        .is_some_and(|schedule| schedule.spec.agent_ref != BEHAVIOR_LEARNER_AGENT)
    {
        return (
            StatusCode::CONFLICT,
            Json(serde_json::json!({
                "error":"reserved behavior schedule targets an incompatible agent"
            })),
        )
            .into_response();
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
                tenant: tenant.clone(),
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
        if let Err(error) = agent.validate() {
            return error_response(error);
        }
        if let Err(error) = state.store.apply_agent(&agent).await {
            return error_response(error);
        }
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
            payload: match serde_json::to_value(&options) {
                Ok(payload) => payload,
                Err(error) => return error_response(error),
            },
            delivery: None,
            at: None,
            cron: Some("0 4 * * 0".to_string()),
            timezone: Some("Asia/Singapore".to_string()),
            enabled: false,
        };
        if let Err(error) = state
            .store
            .put_schedule(&tenant, BEHAVIOR_LEARNING_SCHEDULE, &schedule)
            .await
        {
            return error_response(error);
        }
    }
    (
        if agent_created || schedule_created {
            StatusCode::CREATED
        } else {
            StatusCode::OK
        },
        Json(serde_json::json!({
            "tenant": tenant,
            "agent_ref": BEHAVIOR_LEARNER_AGENT,
            "schedule": BEHAVIOR_LEARNING_SCHEDULE,
            "agent_created": agent_created,
            "agent_updated": agent_updated,
            "schedule_created": schedule_created,
        })),
    )
        .into_response()
}
