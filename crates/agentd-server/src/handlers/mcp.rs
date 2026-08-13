use crate::{error_response, json_result, AppState};
use agentd_api::{McpServer, McpServerSpec, McpTool};
use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use chrono::Utc;
use std::collections::BTreeSet;

pub(crate) async fn list_mcp_servers(
    State(state): State<AppState>,
    Path(tenant): Path<String>,
) -> impl IntoResponse {
    json_result(state.store.list_mcp_servers(Some(&tenant)).await)
}

pub(crate) async fn get_mcp_server(
    State(state): State<AppState>,
    Path((tenant, name)): Path<(String, String)>,
) -> impl IntoResponse {
    match state.store.get_mcp_server(&tenant, &name).await {
        Ok(Some(server)) => (StatusCode::OK, Json(server)).into_response(),
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error":"mcp server not found"})),
        )
            .into_response(),
        Err(error) => error_response(error),
    }
}

pub(crate) async fn put_mcp_server(
    State(state): State<AppState>,
    Path((tenant, name)): Path<(String, String)>,
    Json(spec): Json<McpServerSpec>,
) -> impl IntoResponse {
    if let Err(error) = spec.validate() {
        return error_response(error);
    }
    let now = Utc::now();
    let candidate = McpServer {
        tenant: tenant.clone(),
        name: name.clone(),
        spec: spec.clone(),
        tools: Vec::new(),
        last_error: None,
        created_at: now,
        updated_at: now,
    };
    let tools = match tools_for_put(&candidate).await {
        Ok(tools) => tools,
        Err(error) => return error_response(error),
    };
    match state
        .store
        .apply_mcp_server(&tenant, &name, &spec, &tools, None)
        .await
    {
        Ok(()) => {
            state
                .capabilities
                .invalidate_mcp_session(&tenant, &name)
                .await;
            (
                StatusCode::OK,
                Json(serde_json::json!({"tenant":tenant,"name":name,"tools":tools})),
            )
                .into_response()
        }
        Err(error) => error_response(error),
    }
}

pub(crate) async fn delete_mcp_server(
    State(state): State<AppState>,
    Path((tenant, name)): Path<(String, String)>,
) -> impl IntoResponse {
    match state.store.delete_mcp_server(&tenant, &name).await {
        Ok(deleted) => {
            state
                .capabilities
                .invalidate_mcp_session(&tenant, &name)
                .await;
            (StatusCode::OK, Json(serde_json::json!({"deleted":deleted}))).into_response()
        }
        Err(error) => error_response(error),
    }
}

pub(crate) async fn rediscover_enabled_servers(state: &AppState) {
    let servers = match state.store.list_mcp_servers(None).await {
        Ok(servers) => servers,
        Err(error) => {
            tracing::warn!(%error, "failed to list MCP servers at startup");
            return;
        }
    };
    for server in servers.into_iter().filter(|server| server.spec.enabled) {
        match discover_allowed_tools(&server).await {
            Ok(tools) => {
                if let Err(error) = state
                    .store
                    .apply_mcp_server(&server.tenant, &server.name, &server.spec, &tools, None)
                    .await
                {
                    tracing::warn!(tenant=%server.tenant, server=%server.name, %error, "failed to save MCP discovery");
                }
            }
            Err(error) => {
                tracing::warn!(tenant=%server.tenant, server=%server.name, %error, "MCP discovery failed");
                let _ = state
                    .store
                    .apply_mcp_server(
                        &server.tenant,
                        &server.name,
                        &server.spec,
                        &server.tools,
                        Some(&error.to_string()),
                    )
                    .await;
            }
        }
    }
}

async fn discover_allowed_tools(server: &McpServer) -> anyhow::Result<Vec<McpTool>> {
    let discovered = agentd_core::discover_mcp_tools(server).await?;
    let Some(allowed) = server.spec.allowed_tools.as_ref() else {
        return Ok(discovered);
    };
    let discovered_names = discovered
        .iter()
        .map(|tool| tool.name.as_str())
        .collect::<BTreeSet<_>>();
    let unknown = allowed
        .iter()
        .filter(|name| !discovered_names.contains(name.as_str()))
        .cloned()
        .collect::<Vec<_>>();
    if !unknown.is_empty() {
        anyhow::bail!("unknown MCP tools in allowed_tools: {}", unknown.join(", "));
    }
    let allowed = allowed.iter().map(String::as_str).collect::<BTreeSet<_>>();
    Ok(discovered
        .into_iter()
        .filter(|tool| allowed.contains(tool.name.as_str()))
        .collect())
}

async fn tools_for_put(server: &McpServer) -> anyhow::Result<Vec<McpTool>> {
    if !server.spec.enabled {
        return Ok(Vec::new());
    }
    discover_allowed_tools(server).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use agentd_api::McpTransport;
    use axum::{routing::post, Json, Router};
    use serde_json::{json, Value};

    async fn mcp(Json(request): Json<Value>) -> Json<Value> {
        let id = request.get("id").cloned().unwrap_or(Value::Null);
        let result = match request.get("method").and_then(Value::as_str) {
            Some("initialize") => json!({"protocolVersion":"2025-06-18"}),
            Some("tools/list") => json!({
                "tools": [
                    {"name":"one","description":"first","inputSchema":{"type":"object"}},
                    {"name":"two","description":"second","inputSchema":{"type":"object"}}
                ]
            }),
            _ => json!({}),
        };
        Json(json!({"jsonrpc":"2.0","id":id,"result":result}))
    }

    async fn server(allowed_tools: Option<Vec<String>>) -> McpServer {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, Router::new().route("/", post(mcp)))
                .await
                .unwrap();
        });
        let now = Utc::now();
        McpServer {
            tenant: "demo".into(),
            name: "test".into(),
            spec: McpServerSpec {
                enabled: true,
                transport: McpTransport::Http {
                    url: format!("http://{address}/"),
                    headers_from: std::collections::BTreeMap::new(),
                },
                allowed_tools,
            },
            tools: Vec::new(),
            last_error: None,
            created_at: now,
            updated_at: now,
        }
    }

    #[tokio::test]
    async fn discovery_exposes_all_or_an_allowlist() {
        let all = discover_allowed_tools(&server(None).await).await.unwrap();
        assert_eq!(
            all.iter()
                .map(|tool| tool.name.as_str())
                .collect::<Vec<_>>(),
            ["one", "two"]
        );

        let allowed = discover_allowed_tools(&server(Some(vec!["two".into()])).await)
            .await
            .unwrap();
        assert_eq!(allowed.len(), 1);
        assert_eq!(allowed[0].name, "two");
    }

    #[tokio::test]
    async fn discovery_rejects_unknown_allowed_tools() {
        let error = discover_allowed_tools(&server(Some(vec!["missing".into()])).await)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("unknown MCP tools"));
    }

    #[tokio::test]
    async fn disabled_server_can_be_saved_without_discovery() {
        let now = Utc::now();
        let server = McpServer {
            tenant: "demo".into(),
            name: "offline".into(),
            spec: McpServerSpec {
                enabled: false,
                transport: McpTransport::Http {
                    url: "http://127.0.0.1:1/mcp".into(),
                    headers_from: std::collections::BTreeMap::new(),
                },
                allowed_tools: None,
            },
            tools: Vec::new(),
            last_error: None,
            created_at: now,
            updated_at: now,
        };
        assert!(tools_for_put(&server).await.unwrap().is_empty());
    }
}
