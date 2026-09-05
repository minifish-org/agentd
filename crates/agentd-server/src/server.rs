use crate::auth::require_api_token;
use crate::config::Config;
use crate::dispatch::run_local_dispatch_loop;
use crate::handlers::agents::{
    create_tenant, delete_agent, delete_tenant, get_agent, get_tenant, list_agents, list_tenants,
    patch_tenant, put_agent,
};
use crate::handlers::artifact::{delete_artifact, list_artifacts, read_artifact, write_artifact};
use crate::handlers::context::{delete_context, get_context, list_context_scopes};
use crate::handlers::delivery_outbox::{ack_delivery, claim_deliveries, list_deliveries};
use crate::handlers::mcp::{
    delete_mcp_server, get_mcp_server, list_mcp_servers, put_mcp_server, rediscover_enabled_servers,
};
use crate::handlers::memory::{get_memory_item, search_memory};
use crate::handlers::presets::install_memory_maintenance;
use crate::handlers::runs::{cancel_run, get_run, get_run_trace, list_runs, wait_run};
use crate::handlers::schedules::{delete_schedule, get_schedule, list_schedules, put_schedule};
use crate::handlers::tools::list_tools;
use crate::handlers::turns::submit_turn_endpoint;
use crate::scheduler::Scheduler;
use crate::state::AppState;
use agentd_core::{CapabilityEngine, CapabilityEngineConfig, RuntimeEngine};
use agentd_store::AgentdStore;
use anyhow::{Context, Result};
use axum::{
    middleware,
    response::Html,
    routing::{get, post},
    Router,
};
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::sync::{Mutex, Semaphore};
use tracing::info;

pub(crate) async fn run_server(config_path: &str, reset_data: bool) -> Result<()> {
    tracing::info!(config_path, "loading agentd config");
    let mut cfg: Config = toml::from_str(&tokio::fs::read_to_string(config_path).await?)?;
    cfg.resolve_runtime_paths()?;
    let rest_listener = cfg.rest_listener()?;

    let database_path = std::path::Path::new(&cfg.database_path);
    if let Some(parent) = database_path.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .with_context(|| format!("failed to create database directory {}", parent.display()))?;
    }
    if reset_data {
        for path in [
            cfg.database_path.clone(),
            format!("{}-wal", cfg.database_path),
            format!("{}-shm", cfg.database_path),
        ] {
            if let Err(error) = tokio::fs::remove_file(&path).await {
                if error.kind() != std::io::ErrorKind::NotFound {
                    return Err(error)
                        .with_context(|| format!("failed to reset agentd data file {path}"));
                }
            }
        }
        tracing::warn!("agentd runtime data reset requested");
    }

    let store = AgentdStore::new(&cfg.database_path).await?;
    store.reset_local_runtime_state().await?;
    let sandbox_manager = match cfg.sandbox_runtime_config()? {
        Some(config) => Some(
            agentd_core::SandboxSessionManager::new_microsandbox(config)
                .await
                .context("failed to initialize sandbox runtime")?,
        ),
        None => None,
    };
    let mut caps = CapabilityEngine::new_with_config(
        store.clone(),
        CapabilityEngineConfig {
            http_timeout_secs: cfg.http_timeout_secs,
            llm_api_base: cfg.llm_api_base.clone(),
            llm_api_key: cfg.llm_api_key.clone(),
            llm_model: cfg.llm_model.clone(),
            default_chat_system_prompt: cfg.default_chat_system_prompt.clone(),
        },
    );
    if let Some(manager) = sandbox_manager {
        caps = caps.with_sandbox_manager(manager);
        match caps.reap_sandbox_orphans().await {
            Ok(reaped) if reaped > 0 => {
                tracing::info!(count = reaped, "removed orphaned sandbox sessions");
            }
            Ok(_) => {}
            Err(error) => {
                tracing::warn!(error = %error, "failed to remove every orphaned sandbox session");
            }
        }
    }
    caps.warm_up_retrieval_models().await?;

    let runtime = RuntimeEngine::new(caps.clone(), store.clone());
    let scheduler = Scheduler::new(
        store.clone(),
        Duration::from_millis(cfg.scheduler_tick_ms.max(100)),
    );
    tokio::spawn(scheduler.run_forever());

    let app_state = AppState {
        store: store.clone(),
        capabilities: caps,
        runtime,
        run_permits: Arc::new(Semaphore::new(cfg.run_concurrency.unwrap_or(4).max(1))),
        running_tasks: Arc::new(Mutex::new(HashMap::new())),
        dispatch_poll_interval_ms: cfg.dispatch_poll_interval_ms.unwrap_or(250).max(25),
        shutting_down: Arc::new(AtomicBool::new(false)),
    };

    rediscover_enabled_servers(&app_state).await;
    tokio::spawn(run_local_dispatch_loop(app_state.clone()));

    let router = build_router(app_state.clone(), cfg.api_token.clone());

    let shutdown_state = app_state.clone();
    let rest = axum::serve(tokio::net::TcpListener::bind(rest_listener).await?, router)
        .with_graceful_shutdown(async move {
            shutdown_signal().await;
            shutdown_local_runtime(&shutdown_state).await;
        });
    info!(rest = %cfg.rest_addr, "agentd single-host runtime listening");
    let result = rest.await;
    app_state.capabilities.cleanup_all_sandboxes().await;
    result?;
    Ok(())
}

async fn shutdown_local_runtime(state: &AppState) {
    state.shutting_down.store(true, Ordering::SeqCst);
    let handles: Vec<_> = state
        .running_tasks
        .lock()
        .await
        .drain()
        .map(|(_, task)| task)
        .collect();
    for handle in handles {
        handle.abort();
        let _ = handle.await;
    }
    state.capabilities.cleanup_all_sandboxes().await;
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .expect("failed to install SIGTERM handler");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {},
            _ = terminate.recv() => {},
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

pub(crate) fn build_router(app_state: AppState, api_token: Option<String>) -> Router {
    Router::new()
        .route("/", get(console))
        .route("/console", get(console))
        .route("/v1/tenants", get(list_tenants).post(create_tenant))
        .route(
            "/v1/tenants/:name",
            get(get_tenant).patch(patch_tenant).delete(delete_tenant),
        )
        .route(
            "/v1/tenants/:tenant/presets/memory-maintenance",
            post(install_memory_maintenance),
        )
        .route("/v1/tenants/:tenant/agents", get(list_agents))
        .route(
            "/v1/tenants/:tenant/agents/:name",
            get(get_agent).put(put_agent).delete(delete_agent),
        )
        .route("/v1/tenants/:tenant/turns", post(submit_turn_endpoint))
        .route("/v1/tenants/:tenant/runs", get(list_runs))
        .route("/v1/tenants/:tenant/runs/:id", get(get_run))
        .route("/v1/tenants/:tenant/runs/:id/trace", get(get_run_trace))
        .route("/v1/tenants/:tenant/runs/:id/wait", get(wait_run))
        .route("/v1/tenants/:tenant/runs/:id/cancel", post(cancel_run))
        .route(
            "/v1/tenants/:tenant/contexts/:agent",
            get(list_context_scopes),
        )
        .route(
            "/v1/tenants/:tenant/contexts/:agent/*scope",
            get(get_context).delete(delete_context),
        )
        .route("/v1/tenants/:tenant/artifacts", get(list_artifacts))
        .route(
            "/v1/tenants/:tenant/artifacts/*path",
            get(read_artifact)
                .put(write_artifact)
                .delete(delete_artifact),
        )
        .route("/v1/tenants/:tenant/memory/search", get(search_memory))
        .route("/v1/tenants/:tenant/memory/:id", get(get_memory_item))
        .route("/v1/tenants/:tenant/schedules", get(list_schedules))
        .route(
            "/v1/tenants/:tenant/schedules/:name",
            get(get_schedule).put(put_schedule).delete(delete_schedule),
        )
        .route("/v1/tenants/:tenant/tools", get(list_tools))
        .route("/v1/tenants/:tenant/mcp", get(list_mcp_servers))
        .route(
            "/v1/tenants/:tenant/mcp/:name",
            get(get_mcp_server)
                .put(put_mcp_server)
                .delete(delete_mcp_server),
        )
        .route("/v1/tenants/:tenant/deliveries", get(list_deliveries))
        .route(
            "/v1/tenants/:tenant/deliveries/claim",
            post(claim_deliveries),
        )
        .route("/v1/tenants/:tenant/deliveries/:id/ack", post(ack_delivery))
        .with_state(app_state)
        // Artifact bodies may be large media payloads.
        .layer(axum::extract::DefaultBodyLimit::max(64 * 1024 * 1024))
        .layer(middleware::from_fn_with_state(api_token, require_api_token))
        .layer(tower_http::cors::CorsLayer::permissive())
}

/// Read-only browser for runs, raw traces, and deliveries.
async fn console() -> Html<&'static str> {
    Html(include_str!("console.html"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        body::{to_bytes, Body},
        http::{Method, Request, StatusCode},
    };
    use serde_json::{json, Value};
    use tempfile::TempDir;
    use tower::ServiceExt;

    async fn app() -> (TempDir, Router) {
        let dir = TempDir::new().unwrap();
        let store = AgentdStore::new(dir.path().join("agentd.db").to_str().unwrap())
            .await
            .unwrap();
        let caps = CapabilityEngine::new(store.clone());
        let state = AppState {
            store: store.clone(),
            capabilities: caps.clone(),
            runtime: RuntimeEngine::new(caps, store.clone()),
            run_permits: Arc::new(Semaphore::new(2)),
            running_tasks: Arc::new(Mutex::new(HashMap::new())),
            dispatch_poll_interval_ms: 25,
            shutting_down: Arc::new(AtomicBool::new(false)),
        };
        (dir, build_router(state, None))
    }

    async fn request(
        app: &Router,
        method: Method,
        uri: &str,
        body: Option<Value>,
    ) -> (StatusCode, Value) {
        let mut builder = Request::builder().method(method).uri(uri);
        let body = match body {
            Some(value) => {
                builder = builder.header("content-type", "application/json");
                Body::from(serde_json::to_vec(&value).unwrap())
            }
            None => Body::empty(),
        };
        let response = app
            .clone()
            .oneshot(builder.body(body).unwrap())
            .await
            .unwrap();
        let status = response.status();
        let bytes = to_bytes(response.into_body(), 1_048_576).await.unwrap();
        let value = if bytes.is_empty() {
            Value::Null
        } else {
            serde_json::from_slice(&bytes)
                .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&bytes).into_owned()))
        };
        (status, value)
    }

    #[tokio::test]
    async fn tenant_rest_turn_trace_cancel_artifact_and_removed_routes() {
        let (_dir, app) = app().await;
        for tenant in ["demo", "other"] {
            assert_eq!(
                request(
                    &app,
                    Method::POST,
                    "/v1/tenants",
                    Some(json!({"name":tenant}))
                )
                .await
                .0,
                StatusCode::CREATED
            );
        }
        assert_eq!(
            request(
                &app,
                Method::PUT,
                "/v1/tenants/demo/agents/bot",
                Some(json!({"persona":"test","max_steps":4,"max_tokens":128}))
            )
            .await
            .0,
            StatusCode::OK
        );
        let (_, tools) = request(&app, Method::GET, "/v1/tenants/demo/tools?agent=bot", None).await;
        assert!(!tools
            .as_array()
            .unwrap()
            .iter()
            .any(|tool| { tool["name"] == "sandbox_session" || tool["family"] == "sandbox" }));

        let turn = json!({
            "agent":"bot",
            "scope":"chat:1",
            "payload":{"text":"hello"},
            "request_id":"same-request"
        });
        let first = request(
            &app,
            Method::POST,
            "/v1/tenants/demo/turns",
            Some(turn.clone()),
        )
        .await;
        let duplicate = request(&app, Method::POST, "/v1/tenants/demo/turns", Some(turn)).await;
        assert_eq!(first.0, StatusCode::ACCEPTED);
        assert_eq!(duplicate.0, StatusCode::ACCEPTED);
        assert_eq!(first.1["run_id"], duplicate.1["run_id"]);
        assert_eq!(first.1["status"], "queued");
        let run_id = first.1["run_id"].as_str().unwrap();

        let (_, stored) = request(
            &app,
            Method::GET,
            &format!("/v1/tenants/demo/runs/{run_id}"),
            None,
        )
        .await;
        assert_eq!(stored["input"], json!({"text":"hello"}));
        assert_eq!(stored["scope"], "chat:1");

        assert_eq!(
            request(
                &app,
                Method::POST,
                "/v1/tenants/demo/turns",
                Some(json!({
                    "agent":"bot",
                    "scope":"legacy",
                    "payload":{},
                    "wait":false
                }))
            )
            .await
            .0,
            StatusCode::UNPROCESSABLE_ENTITY
        );

        assert_eq!(
            request(
                &app,
                Method::GET,
                &format!("/v1/tenants/other/runs/{run_id}"),
                None
            )
            .await
            .0,
            StatusCode::NOT_FOUND
        );
        assert_eq!(
            request(
                &app,
                Method::POST,
                &format!("/v1/tenants/demo/runs/{run_id}/cancel"),
                Some(json!({"reason":"test"}))
            )
            .await
            .0,
            StatusCode::OK
        );
        let (_, waited) = request(
            &app,
            Method::GET,
            &format!("/v1/tenants/demo/runs/{run_id}/wait?timeout_ms=1"),
            None,
        )
        .await;
        assert_eq!(waited["status"], "cancelled");
        assert_eq!(waited["timed_out"], false);
        assert_eq!(waited["output"], Value::Null);
        assert_eq!(waited["error"], "test");
        let (_, trace) = request(
            &app,
            Method::GET,
            &format!("/v1/tenants/demo/runs/{run_id}/trace"),
            None,
        )
        .await;
        assert!(trace.as_array().is_some_and(|items| !items.is_empty()));

        let artifact_url = "/v1/tenants/demo/artifacts/health/check.txt";
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::PUT)
                    .uri(artifact_url)
                    .header("content-type", "text/plain")
                    .body(Body::from("healthy"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            request(&app, Method::GET, artifact_url, None).await.1,
            Value::String("healthy".into())
        );
        assert_eq!(
            request(&app, Method::DELETE, artifact_url, None).await.0,
            StatusCode::OK
        );

        for removed in ["/v1/turns", "/v1/inspect/runs", "/v1/audit/runs"] {
            assert_eq!(
                request(&app, Method::GET, removed, None).await.0,
                StatusCode::NOT_FOUND
            );
        }
    }

    #[tokio::test]
    async fn console_is_a_read_only_history_browser() {
        let html = console().await.0;
        for read_path in ["/v1/tenants", "/agents", "/runs?", "/trace", "/deliveries?"] {
            assert!(html.contains(read_path), "missing {read_path}");
        }
        for write_surface in [
            "/turns",
            "/cancel",
            "/tools/execute",
            "method: 'POST'",
            "method: 'PUT'",
            "method: 'DELETE'",
        ] {
            assert!(!html.contains(write_surface), "found {write_surface}");
        }
    }

    #[tokio::test]
    async fn memory_maintenance_preset_is_tenant_scoped_restricted_and_disabled() {
        let (_dir, app) = app().await;
        assert_eq!(
            request(
                &app,
                Method::POST,
                "/v1/tenants/missing/presets/memory-maintenance",
                None,
            )
            .await
            .0,
            StatusCode::NOT_FOUND
        );
        assert_eq!(
            request(
                &app,
                Method::POST,
                "/v1/tenants",
                Some(json!({"name":"demo"})),
            )
            .await
            .0,
            StatusCode::CREATED
        );

        let installed = request(
            &app,
            Method::POST,
            "/v1/tenants/demo/presets/memory-maintenance",
            None,
        )
        .await;
        assert_eq!(installed.0, StatusCode::CREATED);
        assert_eq!(installed.1["agent_ref"], "system/memory-maintainer");
        assert_eq!(installed.1["schedule"], "system/memory-maintenance");
        assert_eq!(installed.1["agent_created"], true);
        assert_eq!(installed.1["schedule_created"], true);

        let (_, agents) = request(&app, Method::GET, "/v1/tenants/demo/agents", None).await;
        assert_eq!(agents.as_array().unwrap().len(), 1);
        assert_eq!(agents[0]["name"], "system/memory-maintainer");
        assert_eq!(agents[0]["allowed_families"], json!(["memory"]));
        assert_eq!(agents[0]["model"], "standard/chat");
        assert_eq!(agents[0]["max_steps"], 64);
        assert_eq!(agents[0]["context_window"], 0);
        assert_eq!(
            request(
                &app,
                Method::GET,
                "/v1/tenants/demo/agents/system%2Fmemory-maintainer",
                None,
            )
            .await
            .0,
            StatusCode::OK
        );

        let (_, schedules) = request(&app, Method::GET, "/v1/tenants/demo/schedules", None).await;
        assert_eq!(schedules.as_array().unwrap().len(), 1);
        assert_eq!(schedules[0]["name"], "system/memory-maintenance");
        assert_eq!(schedules[0]["spec"]["enabled"], false);
        assert_eq!(schedules[0]["spec"]["delivery"], Value::Null);
        assert_eq!(schedules[0]["next_trigger_at"], Value::Null);

        let mut enabled_spec = schedules[0]["spec"].clone();
        enabled_spec["enabled"] = json!(true);
        assert_eq!(
            request(
                &app,
                Method::PUT,
                "/v1/tenants/demo/schedules/system%2Fmemory-maintenance",
                Some(enabled_spec),
            )
            .await
            .0,
            StatusCode::OK
        );

        let mut legacy_agent = agents[0].clone();
        legacy_agent["model"] = Value::Null;
        assert_eq!(
            request(
                &app,
                Method::PUT,
                "/v1/tenants/demo/agents/system%2Fmemory-maintainer",
                Some(legacy_agent),
            )
            .await
            .0,
            StatusCode::OK
        );

        let repeated = request(
            &app,
            Method::POST,
            "/v1/tenants/demo/presets/memory-maintenance",
            None,
        )
        .await;
        assert_eq!(repeated.0, StatusCode::OK);
        assert_eq!(repeated.1["agent_created"], false);
        assert_eq!(repeated.1["schedule_created"], false);
        let (_, agents) = request(&app, Method::GET, "/v1/tenants/demo/agents", None).await;
        assert_eq!(agents[0]["model"], "standard/chat");
        let (_, schedules) = request(&app, Method::GET, "/v1/tenants/demo/schedules", None).await;
        assert_eq!(schedules[0]["spec"]["enabled"], true);
        assert!(!schedules[0]["next_trigger_at"].is_null());
    }
}
