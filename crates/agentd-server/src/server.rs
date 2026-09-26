use crate::audit::{audit_request, HttpAuditState};
use crate::auth::require_api_token;
use crate::config::Config;
use crate::dispatch::run_local_dispatch_loop;
use crate::handlers::agents::{
    create_tenant, delete_agent, delete_tenant, get_agent, get_tenant, list_agents, list_tenants,
    patch_tenant, put_agent,
};
use crate::handlers::artifact::{delete_artifact, list_artifacts, read_artifact, write_artifact};
use crate::handlers::audit::{list_audit, list_tenant_audit};
use crate::handlers::behavior::{clear_behavior_learning, get_behavior_learning};
use crate::handlers::context::{delete_context, get_context, list_context_scopes};
use crate::handlers::delivery_outbox::{ack_delivery, claim_deliveries, list_deliveries};
use crate::handlers::mcp::{
    delete_mcp_server, get_mcp_server, list_mcp_servers, put_mcp_server, rediscover_enabled_servers,
};
use crate::handlers::memory::{get_memory_item, search_memory};
use crate::handlers::presets::{
    ensure_behavior_learning, ensure_memory_maintenance, install_behavior_learning,
    install_memory_maintenance,
};
use crate::handlers::runs::{cancel_run, get_run, get_run_trace, list_runs, wait_run};
use crate::handlers::schedules::{delete_schedule, get_schedule, list_schedules, put_schedule};
use crate::handlers::tools::list_tools;
use crate::handlers::turns::submit_turn_endpoint;
use crate::scheduler::Scheduler;
use crate::state::AppState;
use agentd_core::{CapabilityEngine, CapabilityEngineConfig, RuntimeEngine};
use agentd_store::{with_audit_context, AgentdStore, AuditContext, AuditInput};
use anyhow::{Context, Result};
use axum::{
    middleware,
    response::Html,
    routing::{get, post},
    Router,
};
use std::{
    collections::HashMap,
    net::SocketAddr,
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
    with_audit_context(AuditContext::system("startup"), async {
        store
            .append_audit(AuditInput::new(
                None,
                "server.startup",
                "server",
                None,
                "started",
                serde_json::json!({"reset_data":reset_data}),
            ))
            .await?;
        store.reset_local_runtime_state().await
    })
    .await?;
    bootstrap_background_presets(&store).await?;
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
        match with_audit_context(AuditContext::system("startup"), caps.reap_sandbox_orphans()).await
        {
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
    let scheduler_task = tokio::spawn(scheduler.run_forever());

    let app_state = AppState {
        store: store.clone(),
        capabilities: caps,
        runtime,
        run_permits: Arc::new(Semaphore::new(cfg.run_concurrency.unwrap_or(4).max(1))),
        running_tasks: Arc::new(Mutex::new(HashMap::new())),
        dispatch_poll_interval_ms: cfg.dispatch_poll_interval_ms.unwrap_or(250).max(25),
        shutting_down: Arc::new(AtomicBool::new(false)),
    };

    with_audit_context(
        AuditContext::system("mcp_discovery"),
        rediscover_enabled_servers(&app_state),
    )
    .await;
    tokio::spawn(run_local_dispatch_loop(app_state.clone()));

    let router = build_router(app_state.clone(), cfg.api_token.clone());

    let listener = tokio::net::TcpListener::bind(rest_listener).await?;
    with_audit_context(
        AuditContext::system("startup"),
        store.append_audit(AuditInput::new(
            None,
            "server.startup",
            "server",
            None,
            "succeeded",
            serde_json::json!({}),
        )),
    )
    .await?;
    let shutdown_state = app_state.clone();
    let rest = axum::serve(
        listener,
        router.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .with_graceful_shutdown(async move {
        shutdown_signal().await;
        scheduler_task.abort();
        let _ = scheduler_task.await;
        shutdown_local_runtime(&shutdown_state).await;
    });
    info!(rest = %cfg.rest_addr, "agentd single-host runtime listening");
    let result = rest.await;
    with_audit_context(
        AuditContext::system("shutdown"),
        app_state.capabilities.cleanup_all_sandboxes(),
    )
    .await;
    result?;
    Ok(())
}

async fn bootstrap_background_presets(store: &AgentdStore) -> Result<()> {
    with_audit_context(AuditContext::system("bootstrap"), async {
    for tenant in store.list_tenants().await? {
        let results = [
            ("memory-maintenance", ensure_memory_maintenance(store, &tenant).await),
            ("behavior-learning", ensure_behavior_learning(
                store,
                &tenant,
                &agentd_api::BehaviorLearningOptions::default(),
            )
            .await),
        ];
        for (preset, result) in results {
            match result {
                Ok(_) => {}
                Err(error) if error.is_conflict() => {
                    store.append_audit(AuditInput::new(
                        Some(&tenant), "preset.install", "preset", Some(preset), "rejected",
                        serde_json::json!({"reason":"reserved_resource_conflict"}),
                    )).await?;
                    tracing::warn!(tenant, error = %error, "background preset conflicts with an existing reserved resource; preserving it");
                }
                Err(error) => {
                    return Err(error).with_context(|| {
                        format!("failed to initialize background presets for tenant {tenant}")
                    })
                }
            }
        }
    }
    Ok(())
    }).await
}

async fn shutdown_local_runtime(state: &AppState) {
    with_audit_context(AuditContext::system("shutdown"), async {
        state.shutting_down.store(true, Ordering::SeqCst);
        let handles: Vec<_> = state
            .running_tasks
            .lock()
            .await
            .drain()
            .map(|(_, task)| task)
            .collect();
        if state
            .store
            .append_audit(AuditInput::new(
                None,
                "server.shutdown",
                "server",
                None,
                "started",
                serde_json::json!({"active_runs":handles.len()}),
            ))
            .await
            .is_err()
        {
            tracing::error!("server shutdown start audit could not be persisted");
        }
        for handle in handles {
            handle.abort();
            let _ = handle.await;
        }
        state.capabilities.cleanup_all_sandboxes().await;
        if state
            .store
            .append_audit(AuditInput::new(
                None,
                "server.shutdown",
                "server",
                None,
                "succeeded",
                serde_json::json!({}),
            ))
            .await
            .is_err()
        {
            tracing::error!("server shutdown completion audit could not be persisted");
        }
    })
    .await;
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
    let audit_state = HttpAuditState::new(app_state.store.clone(), api_token.clone());
    Router::new()
        .route("/", get(console))
        .route("/console", get(console))
        .route("/v1/audit", get(list_audit))
        .route("/v1/tenants/:tenant/audit", get(list_tenant_audit))
        .route("/v1/tenants", get(list_tenants).post(create_tenant))
        .route(
            "/v1/tenants/:name",
            get(get_tenant).patch(patch_tenant).delete(delete_tenant),
        )
        .route(
            "/v1/tenants/:tenant/presets/memory-maintenance",
            post(install_memory_maintenance),
        )
        .route(
            "/v1/tenants/:tenant/presets/behavior-learning",
            post(install_behavior_learning),
        )
        .route(
            "/v1/tenants/:tenant/learning/:agent",
            get(get_behavior_learning).delete(clear_behavior_learning),
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
        .layer(
            tower_http::cors::CorsLayer::permissive()
                .expose_headers([axum::http::HeaderName::from_static("x-request-id")]),
        )
        // Outermost so authentication errors, extractor rejections, CORS
        // preflight, method mismatches and fallback responses are all audited.
        .layer(middleware::from_fn_with_state(audit_state, audit_request))
}

/// Read-only browser for runs, raw traces, deliveries, and audit history.
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

    fn named_item<'a>(items: &'a Value, name: &str) -> &'a Value {
        items
            .as_array()
            .unwrap()
            .iter()
            .find(|item| item["name"] == name)
            .unwrap()
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
    async fn behavior_learning_preset_is_automatic_tenant_scoped_and_enabled() {
        let (_dir, app) = app().await;
        assert_eq!(
            request(
                &app,
                Method::POST,
                "/v1/tenants/missing/presets/behavior-learning",
                None,
            )
            .await
            .0,
            StatusCode::NOT_FOUND
        );
        request(
            &app,
            Method::POST,
            "/v1/tenants",
            Some(json!({"name":"demo"})),
        )
        .await;
        let installed = request(
            &app,
            Method::POST,
            "/v1/tenants/demo/presets/behavior-learning",
            None,
        )
        .await;
        assert_eq!(installed.0, StatusCode::OK);
        assert_eq!(installed.1["agent_created"], false);
        assert_eq!(installed.1["schedule_created"], false);
        assert_eq!(installed.1["agent_ref"], "system/behavior-learner");
        assert_eq!(installed.1["schedule"], "system/behavior-learning");

        let (_, agent) = request(
            &app,
            Method::GET,
            "/v1/tenants/demo/agents/system%2Fbehavior-learner",
            None,
        )
        .await;
        assert_eq!(agent["allowed_families"], json!([]));
        assert_eq!(agent["context_window"], 0);
        assert_eq!(agent["model"], Value::Null);
        assert_eq!(agent["timeout_ms"], 900_000);
        assert_eq!(agent["max_steps"], 64);
        let (_, schedules) = request(&app, Method::GET, "/v1/tenants/demo/schedules", None).await;
        assert_eq!(schedules.as_array().unwrap().len(), 2);
        assert_eq!(schedules[0]["spec"]["enabled"], true);
        assert_eq!(schedules[0]["spec"]["delivery"], Value::Null);
        assert_eq!(schedules[0]["spec"]["payload"]["target_agent"], "*");
        assert!(!schedules[0]["next_trigger_at"].is_null());
        assert_eq!(
            request(&app, Method::GET, "/v1/tenants/demo/runs", None)
                .await
                .1,
            json!([])
        );
    }

    #[tokio::test]
    async fn behavior_learning_preset_validates_options_and_preserves_custom_settings() {
        let (_dir, app) = app().await;
        request(
            &app,
            Method::POST,
            "/v1/tenants",
            Some(json!({"name":"demo"})),
        )
        .await;
        request(
            &app,
            Method::PUT,
            "/v1/tenants/demo/agents/bot",
            Some(json!({"persona":"Help the user."})),
        )
        .await;
        for (options, expected) in [
            (json!({"target_agent":"missing"}), StatusCode::NOT_FOUND),
            (
                json!({"target_agent":"system/example"}),
                StatusCode::BAD_REQUEST,
            ),
            (json!({"judge_model":" "}), StatusCode::BAD_REQUEST),
            (json!({"unexpected":true}), StatusCode::BAD_REQUEST),
        ] {
            assert_eq!(
                request(
                    &app,
                    Method::POST,
                    "/v1/tenants/demo/presets/behavior-learning",
                    Some(options),
                )
                .await
                .0,
                expected
            );
        }
        // Creation already installed defaults. Remove these resources to test
        // explicit installation with operator-provided options.
        for uri in [
            "/v1/tenants/demo/schedules/system%2Fbehavior-learning",
            "/v1/tenants/demo/agents/system%2Fbehavior-learner",
        ] {
            assert_eq!(
                request(&app, Method::DELETE, uri, None).await.0,
                StatusCode::OK
            );
        }
        let options = json!({
            "target_agent":"bot", "proposer_model":"deepseek-chat", "judge_model":"judge/chat",
            "min_samples":8, "max_samples":10, "max_model_calls":64, "max_tokens":1024,
            "min_improvement":0.2, "rubric":"Prefer valid tool arguments and grounded answers."
        });
        assert_eq!(
            request(
                &app,
                Method::POST,
                "/v1/tenants/demo/presets/behavior-learning",
                Some(options.clone()),
            )
            .await
            .0,
            StatusCode::CREATED
        );
        let (_, schedules) = request(&app, Method::GET, "/v1/tenants/demo/schedules", None).await;
        let mut schedule = schedules[0]["spec"].clone();
        assert_eq!(schedule["payload"], options);
        schedule["enabled"] = json!(false);
        schedule["cron"] = json!("0 5 * * 1");
        request(
            &app,
            Method::PUT,
            "/v1/tenants/demo/schedules/system%2Fbehavior-learning",
            Some(schedule.clone()),
        )
        .await;
        request(
            &app,
            Method::PUT,
            "/v1/tenants/demo/agents/system%2Fbehavior-learner",
            Some(json!({"allowed_families":[],"context_window":3,"model":"custom/chat","timeout_ms":800_000})),
        )
        .await;
        let reapplied = request(
            &app,
            Method::POST,
            "/v1/tenants/demo/presets/behavior-learning",
            None,
        )
        .await;
        assert_eq!(reapplied.0, StatusCode::OK);
        assert_eq!(reapplied.1["agent_created"], false);
        assert_eq!(reapplied.1["agent_updated"], true);
        assert_eq!(reapplied.1["schedule_created"], false);
        let (_, schedules) = request(&app, Method::GET, "/v1/tenants/demo/schedules", None).await;
        assert_eq!(schedules[0]["spec"], schedule);
        assert!(schedules[0]["next_trigger_at"].is_null());
        let (_, agent) = request(
            &app,
            Method::GET,
            "/v1/tenants/demo/agents/system%2Fbehavior-learner",
            None,
        )
        .await;
        assert_eq!(agent["context_window"], 0);
        assert_eq!(agent["model"], "custom/chat");
        assert_eq!(agent["timeout_ms"], 800_000);
    }

    #[tokio::test]
    async fn behavior_learning_preset_rejects_incompatible_reserved_resources() {
        let (_dir, app) = app().await;
        request(
            &app,
            Method::POST,
            "/v1/tenants",
            Some(json!({"name":"demo"})),
        )
        .await;
        let (_, original_schedules) =
            request(&app, Method::GET, "/v1/tenants/demo/schedules", None).await;
        request(
            &app,
            Method::PUT,
            "/v1/tenants/demo/agents/system%2Fbehavior-learner",
            Some(json!({"allowed_families":["memory"]})),
        )
        .await;
        assert_eq!(
            request(
                &app,
                Method::POST,
                "/v1/tenants/demo/presets/behavior-learning",
                None
            )
            .await
            .0,
            StatusCode::CONFLICT
        );
        assert_eq!(
            request(&app, Method::GET, "/v1/tenants/demo/schedules", None)
                .await
                .1,
            original_schedules
        );
        request(
            &app,
            Method::DELETE,
            "/v1/tenants/demo/agents/system%2Fbehavior-learner",
            None,
        )
        .await;
        request(
            &app,
            Method::PUT,
            "/v1/tenants/demo/agents/bot",
            Some(json!({})),
        )
        .await;
        request(
            &app,
            Method::PUT,
            "/v1/tenants/demo/schedules/system%2Fbehavior-learning",
            Some(json!({"agent_ref":"bot","scope":"other","payload":{},"enabled":false,"cron":"0 4 * * 0","timezone":"UTC"})),
        )
        .await;
        assert_eq!(
            request(
                &app,
                Method::POST,
                "/v1/tenants/demo/presets/behavior-learning",
                None
            )
            .await
            .0,
            StatusCode::CONFLICT
        );
        assert_eq!(
            request(
                &app,
                Method::GET,
                "/v1/tenants/demo/agents/system%2Fbehavior-learner",
                None
            )
            .await
            .0,
            StatusCode::NOT_FOUND
        );
    }

    #[tokio::test]
    async fn behavior_learning_state_is_tenant_scoped_and_clear_is_idempotent() {
        let (dir, app) = app().await;
        request(
            &app,
            Method::POST,
            "/v1/tenants",
            Some(json!({"name":"demo"})),
        )
        .await;
        request(
            &app,
            Method::POST,
            "/v1/tenants",
            Some(json!({"name":"other"})),
        )
        .await;
        request(
            &app,
            Method::PUT,
            "/v1/tenants/demo/agents/bot",
            Some(json!({})),
        )
        .await;
        let state = request(&app, Method::GET, "/v1/tenants/demo/learning/bot", None).await;
        assert_eq!(state.0, StatusCode::OK);
        assert_eq!(
            state.1,
            json!({"tenant":"demo","agent":"bot","active_revision":null,"history":[]})
        );
        for uri in [
            "/v1/tenants/other/learning/bot",
            "/v1/tenants/missing/learning/bot",
            "/v1/tenants/demo/learning/missing",
        ] {
            for method in [Method::GET, Method::DELETE] {
                assert_eq!(
                    request(&app, method, uri, None).await.0,
                    StatusCode::NOT_FOUND
                );
            }
        }
        for _ in 0..2 {
            let cleared =
                request(&app, Method::DELETE, "/v1/tenants/demo/learning/bot", None).await;
            assert_eq!(cleared.0, StatusCode::OK);
            assert_eq!(cleared.1["cleared"], false);
        }
        assert_eq!(
            request(&app, Method::GET, "/v1/tenants/demo/agents/bot", None)
                .await
                .0,
            StatusCode::OK
        );

        request(
            &app,
            Method::POST,
            "/v1/tenants/demo/presets/behavior-learning",
            None,
        )
        .await;
        let store = AgentdStore::new(dir.path().join("agentd.db").to_str().unwrap())
            .await
            .unwrap();
        request(
            &app,
            Method::POST,
            "/v1/tenants/demo/turns",
            Some(json!({"agent":"bot","scope":"chat","payload":{"text":"hello"}})),
        )
        .await;
        let source = store.claim_next_run().await.unwrap().unwrap();
        store
            .finalize_run_success(source.run.run_id, &json!({"reply":"hello"}), None)
            .await
            .unwrap();
        let source = store.get_run(source.run.run_id).await.unwrap().unwrap();
        request(
            &app,
            Method::POST,
            "/v1/tenants/demo/turns",
            Some(json!({"agent":"system/behavior-learner","scope":"behavior-learning/bot","payload":{"target_agent":"bot"}})),
        )
        .await;
        let cycle = store.claim_next_run().await.unwrap().unwrap();
        let target = store.get_agent("demo", "bot").await.unwrap().unwrap();
        store
            .finish_behavior_learning(
                cycle.run.run_id,
                agentd_store::BehaviorLearningResult {
                    target_agent: "bot",
                    expected_spec: &target.spec,
                    expected_revision: None,
                    instructions: "Check tool arguments before calling.",
                    report: &json!({"mean_gain":0.2}),
                    promote: true,
                    source_updated_at: source.updated_at,
                    source_run_id: source.run_id,
                },
            )
            .await
            .unwrap();
        let (_, learned) = request(&app, Method::GET, "/v1/tenants/demo/learning/bot", None).await;
        assert_eq!(learned["active_revision"]["revision"], 1);
        assert_eq!(learned["history"].as_array().unwrap().len(), 1);
        assert_eq!(
            request(&app, Method::DELETE, "/v1/tenants/demo/learning/bot", None)
                .await
                .1["cleared"],
            true
        );
        let (_, cleared) = request(&app, Method::GET, "/v1/tenants/demo/learning/bot", None).await;
        assert_eq!(cleared["active_revision"], Value::Null);
        assert_eq!(cleared["history"], learned["history"]);
        assert_eq!(
            request(&app, Method::DELETE, "/v1/tenants/demo/learning/bot", None)
                .await
                .1["cleared"],
            false
        );
    }

    #[tokio::test]
    async fn startup_repairs_missing_presets_and_preserves_existing_disabled_schedules() {
        let (dir, app) = app().await;
        let store = AgentdStore::new(dir.path().join("agentd.db").to_str().unwrap())
            .await
            .unwrap();
        // These tenants predate server-managed preset installation.
        for tenant in ["legacy", "disabled"] {
            store.create_tenant(tenant, &json!({})).await.unwrap();
        }
        bootstrap_background_presets(&store).await.unwrap();
        for tenant in ["legacy", "disabled"] {
            assert_eq!(store.list_agents(Some(tenant)).await.unwrap().len(), 2);
            let schedules = store.list_schedules(Some(tenant)).await.unwrap();
            assert_eq!(schedules.len(), 2);
            assert!(schedules
                .iter()
                .all(|schedule| schedule.spec.enabled && schedule.next_trigger_at.is_some()));
        }
        let mut disabled_specs = Vec::new();
        for schedule in store.list_schedules(Some("disabled")).await.unwrap() {
            let mut spec = schedule.spec;
            spec.enabled = false;
            spec.cron = Some("15 2 * * 2".into());
            store
                .put_schedule("disabled", &schedule.name, &spec)
                .await
                .unwrap();
            disabled_specs.push((schedule.name, spec));
        }
        for agent in store.list_agents(Some("disabled")).await.unwrap() {
            let mut spec = agent.spec;
            spec.context_window = Some(3);
            store
                .apply_agent(&agentd_api::AgentResource {
                    metadata: agent.metadata,
                    spec,
                })
                .await
                .unwrap();
        }
        store
            .delete_agent("legacy", agentd_api::MEMORY_MAINTAINER_AGENT)
            .await
            .unwrap();
        store
            .delete_schedule("legacy", agentd_api::MEMORY_MAINTENANCE_SCHEDULE)
            .await
            .unwrap();
        for _ in 0..2 {
            bootstrap_background_presets(&store).await.unwrap();
            for (name, expected) in &disabled_specs {
                let schedule = store.get_schedule("disabled", name).await.unwrap().unwrap();
                assert_eq!(&schedule.spec, expected);
                assert!(schedule.next_trigger_at.is_none());
            }
        }
        assert!(
            store
                .get_schedule("legacy", agentd_api::MEMORY_MAINTENANCE_SCHEDULE)
                .await
                .unwrap()
                .unwrap()
                .spec
                .enabled
        );
        assert!(store
            .list_agents(Some("disabled"))
            .await
            .unwrap()
            .iter()
            .all(|agent| agent.spec.context_window == Some(0)));
        // Retrying tenant creation is also idempotent and keeps explicit opt-outs.
        let repeated = request(
            &app,
            Method::POST,
            "/v1/tenants",
            Some(json!({"name":"disabled"})),
        )
        .await;
        assert_eq!(repeated.0, StatusCode::OK);
        assert_eq!(repeated.1["created"], false);
        for (name, expected) in disabled_specs {
            assert_eq!(
                store
                    .get_schedule("disabled", &name)
                    .await
                    .unwrap()
                    .unwrap()
                    .spec,
                expected
            );
        }
        assert_eq!(
            request(&app, Method::GET, "/v1/tenants/legacy/runs", None)
                .await
                .1,
            json!([])
        );
        assert_eq!(
            request(&app, Method::GET, "/v1/tenants/disabled/runs", None)
                .await
                .1,
            json!([])
        );
    }

    #[tokio::test]
    async fn startup_preserves_reserved_conflicts_and_installs_other_presets() {
        let (dir, _app) = app().await;
        let store = AgentdStore::new(dir.path().join("agentd.db").to_str().unwrap())
            .await
            .unwrap();
        store.create_tenant("conflict", &json!({})).await.unwrap();
        ensure_memory_maintenance(&store, "conflict").await.unwrap();
        let original = store
            .get_agent("conflict", agentd_api::MEMORY_MAINTAINER_AGENT)
            .await
            .unwrap()
            .unwrap();
        let mut spec = original.spec;
        spec.allowed_families = Some(vec![]);
        spec.context_window = Some(3);
        store
            .apply_agent(&agentd_api::AgentResource {
                metadata: original.metadata,
                spec: spec.clone(),
            })
            .await
            .unwrap();
        bootstrap_background_presets(&store).await.unwrap();
        let preserved = store
            .get_agent("conflict", agentd_api::MEMORY_MAINTAINER_AGENT)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(preserved.spec, spec);
        assert!(store
            .get_agent("conflict", agentd_api::BEHAVIOR_LEARNER_AGENT)
            .await
            .unwrap()
            .is_some());
        assert!(
            store
                .get_schedule("conflict", agentd_api::BEHAVIOR_LEARNING_SCHEDULE)
                .await
                .unwrap()
                .unwrap()
                .spec
                .enabled
        );
    }

    #[tokio::test]
    async fn memory_maintenance_is_automatic_and_preserves_explicit_disable() {
        let (_dir, app) = app().await;
        assert_eq!(
            request(
                &app,
                Method::POST,
                "/v1/tenants/missing/presets/memory-maintenance",
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
                "/v1/tenants",
                Some(json!({"name":"demo"}))
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
        assert_eq!(installed.0, StatusCode::OK);
        assert_eq!(installed.1["agent_ref"], "system/memory-maintainer");
        assert_eq!(installed.1["schedule"], "system/memory-maintenance");
        assert_eq!(installed.1["agent_created"], false);
        assert_eq!(installed.1["schedule_created"], false);

        let (_, agents) = request(&app, Method::GET, "/v1/tenants/demo/agents", None).await;
        assert_eq!(agents.as_array().unwrap().len(), 2);
        let agent = named_item(&agents, "system/memory-maintainer");
        assert_eq!(agent["allowed_families"], json!(["memory"]));
        assert_eq!(agent["model"], "standard/chat");
        assert_eq!(agent["max_steps"], 64);
        assert_eq!(agent["context_window"], 0);
        let (_, schedules) = request(&app, Method::GET, "/v1/tenants/demo/schedules", None).await;
        assert_eq!(schedules.as_array().unwrap().len(), 2);
        let schedule = named_item(&schedules, "system/memory-maintenance");
        assert_eq!(schedule["spec"]["enabled"], true);
        assert_eq!(schedule["spec"]["payload"]["namespace"], "*");
        assert_eq!(schedule["spec"]["payload"]["min_entries"], 5);
        assert_eq!(schedule["spec"]["delivery"], Value::Null);
        assert!(!schedule["next_trigger_at"].is_null());

        let mut disabled_spec = schedule["spec"].clone();
        disabled_spec["enabled"] = json!(false);
        assert_eq!(
            request(
                &app,
                Method::PUT,
                "/v1/tenants/demo/schedules/system%2Fmemory-maintenance",
                Some(disabled_spec.clone())
            )
            .await
            .0,
            StatusCode::OK
        );
        let mut legacy_agent = agent.clone();
        legacy_agent["model"] = Value::Null;
        legacy_agent["context_window"] = json!(4);
        legacy_agent["temperature"] = json!(0.3);
        assert_eq!(
            request(
                &app,
                Method::PUT,
                "/v1/tenants/demo/agents/system%2Fmemory-maintainer",
                Some(legacy_agent)
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
        assert_eq!(repeated.1["agent_updated"], true);
        let (_, repaired) = request(
            &app,
            Method::GET,
            "/v1/tenants/demo/agents/system%2Fmemory-maintainer",
            None,
        )
        .await;
        assert_eq!(repaired["model"], "standard/chat");
        assert_eq!(repaired["context_window"], 0);
        assert!((repaired["temperature"].as_f64().unwrap() - 0.3).abs() < 0.001);
        let (_, schedules) = request(&app, Method::GET, "/v1/tenants/demo/schedules", None).await;
        let schedule = named_item(&schedules, "system/memory-maintenance");
        assert_eq!(schedule["spec"], disabled_spec);
        assert!(schedule["next_trigger_at"].is_null());

        // The historical default-only preset had no min_entries property.
        let mut legacy_spec = disabled_spec;
        legacy_spec["payload"]["namespace"] = json!("default");
        legacy_spec["payload"]
            .as_object_mut()
            .unwrap()
            .remove("min_entries");
        assert_eq!(
            request(
                &app,
                Method::PUT,
                "/v1/tenants/demo/schedules/system%2Fmemory-maintenance",
                Some(legacy_spec)
            )
            .await
            .0,
            StatusCode::OK
        );
        let upgraded = request(
            &app,
            Method::POST,
            "/v1/tenants/demo/presets/memory-maintenance",
            None,
        )
        .await;
        assert_eq!(upgraded.1["schedule_updated"], true);
        let (_, schedules) = request(&app, Method::GET, "/v1/tenants/demo/schedules", None).await;
        let schedule = named_item(&schedules, "system/memory-maintenance");
        assert_eq!(schedule["spec"]["enabled"], false);
        assert_eq!(schedule["spec"]["payload"]["namespace"], "*");
        assert_eq!(schedule["spec"]["payload"]["min_entries"], 5);
        assert!(schedule["next_trigger_at"].is_null());
        assert_eq!(
            request(&app, Method::GET, "/v1/tenants/demo/runs", None)
                .await
                .1,
            json!([])
        );
    }
}
