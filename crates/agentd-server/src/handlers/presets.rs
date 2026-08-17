use crate::{error_response, AppState};
use agentd_api::{AgentLimits, AgentResource, AgentSpec, ResourceMeta, ScheduleSpec, ToolFamily};
use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use std::collections::BTreeMap;

pub(crate) const MEMORY_MAINTAINER_AGENT: &str = "system/memory-maintainer";
pub(crate) const MEMORY_MAINTENANCE_SCHEDULE: &str = "system/memory-maintenance";

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
            model: None,
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
        Ok(Some(existing))
            if existing.spec.allowed_families.as_deref() == Some(&[ToolFamily::Memory]) =>
        {
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
            "namespace": "default",
            "policy": "Scan the complete namespace and maintain durable memory; leave uncertain entries unchanged"
        }),
        delivery: None,
        at: None,
        cron: Some("0 3 * * 0".to_string()),
        timezone: Some("Asia/Singapore".to_string()),
        enabled: false,
    };
    let schedule_created = match state
        .store
        .get_schedule(&tenant, MEMORY_MAINTENANCE_SCHEDULE)
        .await
    {
        Ok(Some(existing)) if existing.spec.agent_ref == MEMORY_MAINTAINER_AGENT => false,
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
            true
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
            "schedule_created": schedule_created
        })),
    )
        .into_response()
}
