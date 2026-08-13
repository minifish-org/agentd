use crate::{error_response, json_result, AppState};
use agentd_api::{Agent, AgentLimits, AgentResource, AgentSpec, ResourceMeta, ToolFamily};
use agentd_store::TenantMetadataPatchResult;
use axum::{
    body::Bytes,
    extract::{Path, State},
    http::{header::CONTENT_TYPE, HeaderMap, StatusCode},
    response::IntoResponse,
    Json,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct TenantListResponse {
    pub(crate) tenants: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct TenantCreateRequest {
    pub(crate) name: String,
    #[serde(default = "empty_metadata")]
    pub(crate) metadata: serde_json::Value,
}

#[derive(Debug, Deserialize)]
pub(crate) struct TenantPatchRequest {
    pub(crate) metadata: serde_json::Value,
    #[serde(default)]
    pub(crate) if_updated_at: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct AgentConfig {
    #[serde(default)]
    pub(crate) persona: Option<String>,
    #[serde(default)]
    pub(crate) model: Option<String>,
    #[serde(default)]
    pub(crate) allowed_families: Option<Vec<ToolFamily>>,
    #[serde(default = "default_timeout_ms")]
    pub(crate) timeout_ms: u64,
    #[serde(default = "default_max_steps")]
    pub(crate) max_steps: u32,
    #[serde(default)]
    pub(crate) temperature: Option<f32>,
    #[serde(default)]
    pub(crate) max_tokens: Option<u32>,
    #[serde(default)]
    pub(crate) context_window: Option<usize>,
}

fn empty_metadata() -> serde_json::Value {
    serde_json::json!({})
}
fn default_timeout_ms() -> u64 {
    60_000
}
fn default_max_steps() -> u32 {
    12
}

pub(crate) async fn list_tenants(State(state): State<AppState>) -> impl IntoResponse {
    json_result(
        state
            .store
            .list_tenants()
            .await
            .map(|tenants| TenantListResponse { tenants }),
    )
}

pub(crate) async fn create_tenant(
    State(state): State<AppState>,
    Json(req): Json<TenantCreateRequest>,
) -> impl IntoResponse {
    match state.store.create_tenant(&req.name, &req.metadata).await {
        Ok((tenant, created)) => (
            if created {
                StatusCode::CREATED
            } else {
                StatusCode::OK
            },
            Json(serde_json::json!({"tenant":tenant,"created":created})),
        )
            .into_response(),
        Err(error) => error_response(error),
    }
}

pub(crate) async fn get_tenant(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> impl IntoResponse {
    match state.store.get_tenant(&name).await {
        Ok(Some(tenant)) => (StatusCode::OK, Json(tenant)).into_response(),
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error":"tenant not found"})),
        )
            .into_response(),
        Err(error) => error_response(error),
    }
}

pub(crate) async fn patch_tenant(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Json(req): Json<TenantPatchRequest>,
) -> impl IntoResponse {
    match state
        .store
        .patch_tenant_metadata(&name, &req.metadata, req.if_updated_at.as_deref())
        .await
    {
        Ok(TenantMetadataPatchResult::Updated(tenant)) => {
            (StatusCode::OK, Json(tenant)).into_response()
        }
        Ok(TenantMetadataPatchResult::NotFound) => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error":"tenant not found"})),
        )
            .into_response(),
        Ok(TenantMetadataPatchResult::Conflict(current)) => (
            StatusCode::CONFLICT,
            Json(serde_json::json!({"error":"tenant update conflict","current":current})),
        )
            .into_response(),
        Err(error) => error_response(error),
    }
}

pub(crate) async fn delete_tenant(
    State(state): State<AppState>,
    Path(tenant): Path<String>,
) -> impl IntoResponse {
    json_result(state.store.delete_tenant(&tenant).await)
}

pub(crate) async fn list_agents(
    State(state): State<AppState>,
    Path(tenant): Path<String>,
) -> impl IntoResponse {
    json_result(
        state
            .store
            .list_agents(Some(&tenant))
            .await
            .map(|agents| agents.into_iter().map(agent_view).collect::<Vec<_>>()),
    )
}

pub(crate) async fn get_agent(
    State(state): State<AppState>,
    Path((tenant, name)): Path<(String, String)>,
) -> impl IntoResponse {
    match state.store.get_agent(&tenant, &name).await {
        Ok(Some(agent)) => (StatusCode::OK, Json(agent_view(agent))).into_response(),
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error":"agent not found"})),
        )
            .into_response(),
        Err(error) => error_response(error),
    }
}

pub(crate) async fn put_agent(
    State(state): State<AppState>,
    Path((tenant, name)): Path<(String, String)>,
    headers: HeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    let config = match parse_agent_config(&headers, &body) {
        Ok(config) => config,
        Err(error) => return error_response(error),
    };
    let resource = AgentResource {
        metadata: ResourceMeta {
            name: name.clone(),
            tenant: tenant.clone(),
            labels: Default::default(),
        },
        spec: AgentSpec {
            allowed_families: config.allowed_families,
            limits: AgentLimits {
                timeout_ms: config.timeout_ms,
                max_steps: config.max_steps,
            },
            system_prompt: config.persona,
            model: config.model,
            temperature: config.temperature,
            max_tokens: config.max_tokens,
            context_window: config.context_window,
        },
    };
    if let Err(error) = resource.validate() {
        return error_response(error.to_string());
    }
    match state.store.apply_agent(&resource).await {
        Ok(()) => (
            StatusCode::OK,
            Json(serde_json::json!({"tenant":tenant,"name":name})),
        )
            .into_response(),
        Err(error) => error_response(error),
    }
}

pub(crate) async fn delete_agent(
    State(state): State<AppState>,
    Path((tenant, name)): Path<(String, String)>,
) -> impl IntoResponse {
    json_result(
        state
            .store
            .delete_agent(&tenant, &name)
            .await
            .map(|deleted| serde_json::json!({"deleted":deleted})),
    )
}

fn parse_agent_config(headers: &HeaderMap, body: &[u8]) -> anyhow::Result<AgentConfig> {
    let raw = std::str::from_utf8(body)?;
    let content_type = headers
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("");
    if content_type.contains("toml") {
        Ok(toml::from_str(raw)?)
    } else {
        Ok(serde_json::from_str(raw)?)
    }
}

fn agent_view(agent: Agent) -> serde_json::Value {
    serde_json::json!({
        "tenant": agent.tenant,
        "name": agent.name,
        "persona": agent.spec.system_prompt,
        "model": agent.spec.model,
        "allowed_families": agent.spec.allowed_families,
        "timeout_ms": agent.spec.limits.timeout_ms,
        "max_steps": agent.spec.limits.max_steps,
        "temperature": agent.spec.temperature,
        "max_tokens": agent.spec.max_tokens,
        "context_window": agent.spec.context_window,
        "created_at": agent.created_at,
        "updated_at": agent.updated_at,
    })
}
