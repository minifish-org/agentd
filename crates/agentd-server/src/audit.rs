use crate::auth::authenticated_api_token;
use agentd_store::{with_audit_context, AgentdStore, AuditContext, AuditInput};
use axum::{
    extract::{ConnectInfo, FromRequestParts, MatchedPath, Query, RawPathParams, Request, State},
    http::{HeaderValue, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
    Json,
};
use serde_json::{json, Value};
use std::{collections::BTreeMap, net::SocketAddr, time::Instant};
use uuid::Uuid;

#[derive(Clone)]
pub(crate) struct HttpAuditState {
    store: AgentdStore,
    api_token: Option<String>,
    #[cfg(test)]
    fail_at: Option<&'static str>,
}

impl HttpAuditState {
    pub(crate) fn new(store: AgentdStore, api_token: Option<String>) -> Self {
        Self {
            store,
            api_token,
            #[cfg(test)]
            fail_at: None,
        }
    }

    async fn append(&self, input: AuditInput<'_>, _stage: &str) -> anyhow::Result<i64> {
        #[cfg(test)]
        if self.fail_at == Some(_stage) {
            anyhow::bail!("injected audit persistence failure");
        }
        self.store.append_audit(input).await
    }
}

struct RequestTarget {
    tenant: Option<String>,
    resource_type: &'static str,
    resource_id: Option<String>,
}

fn request_target(route: Option<&str>, params: &BTreeMap<String, String>) -> RequestTarget {
    let route = route.unwrap_or("");
    let tenant = if route == "/v1/tenants/:name" {
        params.get("name").cloned()
    } else {
        params.get("tenant").cloned()
    };
    let (resource_type, key) = if route == "/v1/tenants/:name" {
        ("tenant", Some("name"))
    } else if route.ends_with("/audit") || route == "/v1/audit" {
        ("audit", None)
    } else if route.contains("/presets/") {
        ("preset", None)
    } else if route.contains("/learning/") {
        ("behavior_policy", Some("agent"))
    } else if route.contains("/agents") {
        ("agent", Some("name"))
    } else if route.contains("/runs") {
        ("run", Some("id"))
    } else if route.contains("/turns") {
        ("run", None)
    } else if route.contains("/contexts") {
        ("context", Some("agent"))
    } else if route.contains("/artifacts") {
        ("artifact", Some("path"))
    } else if route.contains("/memory") {
        ("memory", Some("id"))
    } else if route.contains("/schedules") {
        ("schedule", Some("name"))
    } else if route.contains("/mcp") {
        ("mcp_server", Some("name"))
    } else if route.contains("/deliveries") {
        ("delivery", Some("id"))
    } else if route.contains("/tools") {
        ("tool", None)
    } else if route == "/v1/tenants" {
        ("tenant", None)
    } else {
        ("http", None)
    };
    RequestTarget {
        tenant,
        resource_type,
        resource_id: key.and_then(|key| params.get(key).cloned()),
    }
}

fn with_request_id(mut response: Response, request_id: Uuid) -> Response {
    response.headers_mut().insert(
        "x-request-id",
        HeaderValue::from_str(&request_id.to_string()).expect("UUID is a valid header value"),
    );
    response
}

fn unavailable(request_id: Uuid, stage: &'static str) -> Response {
    // A completion failure may follow committed business changes. The response
    // deliberately makes no rollback claim; request_id links any store events.
    let mut response = with_request_id(
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error":"audit persistence unavailable","audit_stage":stage})),
        )
            .into_response(),
        request_id,
    );
    // The audit layer is outside CORS so preflight requests are audited too.
    // Its replacement response bypasses the inner CORS layer. Use that same
    // fixed permissive policy so browsers can read the generated request ID.
    response
        .headers_mut()
        .insert("access-control-allow-origin", HeaderValue::from_static("*"));
    response.headers_mut().insert(
        "access-control-expose-headers",
        HeaderValue::from_static("x-request-id"),
    );
    response
}

pub(crate) async fn audit_request(
    State(state): State<HttpAuditState>,
    request: Request,
    next: Next,
) -> Response {
    let path = request.uri().path();
    if path != "/v1" && !path.starts_with("/v1/") {
        return next.run(request).await;
    }
    let started = Instant::now();
    let request_id = Uuid::new_v4();
    let authenticated = authenticated_api_token(state.api_token.as_deref(), request.headers());
    let (mut parts, body) = request.into_parts();
    let route = parts
        .extensions
        .get::<MatchedPath>()
        .map(|path| path.as_str().to_owned());
    let peer = parts
        .extensions
        .get::<ConnectInfo<SocketAddr>>()
        .map(|peer| peer.0.to_string());
    let params = RawPathParams::from_request_parts(&mut parts, &())
        .await
        .ok()
        .map(|params| {
            params
                .iter()
                .map(|(key, value)| (key.to_owned(), value.to_owned()))
                .collect()
        })
        .unwrap_or_default();
    let target = request_target(route.as_deref(), &params);
    // Only the router's template and socket peer are retained. Never inspect
    // either body, raw query strings, forwarding headers or caller actor IDs.
    let mut details =
        json!({"method":parts.method.as_str(),"route":route.as_deref().unwrap_or("unmatched")});
    if target.resource_type == "context" {
        if let Some(scope) = params.get("scope") {
            details["scope"] = json!(scope.trim_start_matches('/'));
        }
    }
    if target.resource_type == "memory" {
        #[derive(serde::Deserialize)]
        struct NamespaceQuery {
            namespace: Option<String>,
        }
        // Only the namespace identifier is eligible; search text, cursors and
        // all other query values stay out of the audit index.
        if let Ok(Query(query)) = Query::<NamespaceQuery>::try_from_uri(&parts.uri) {
            details["namespace"] = json!(query.namespace.as_deref().unwrap_or("default"));
        }
    }
    if let Some(peer) = peer {
        details["peer_addr"] = json!(peer);
    }
    let request = Request::from_parts(parts, body);
    with_audit_context(AuditContext::api(authenticated, request_id), async move {
        let input = |outcome: &'static str, details: Value| {
            let mut event = AuditInput::new(
                target.tenant.as_deref(),
                "http.request",
                target.resource_type,
                target.resource_id.as_deref(),
                outcome,
                details,
            );
            if target.resource_type == "run" {
                event.run_id = target
                    .resource_id
                    .as_deref()
                    .and_then(|id| Uuid::parse_str(id).ok());
            }
            event
        };
        if state
            .append(input("started", details.clone()), "started")
            .await
            .is_err()
        {
            tracing::error!(%request_id, "HTTP request start audit could not be persisted");
            return unavailable(request_id, "started");
        }
        let response = next.run(request).await;
        let status = response.status();
        details["status_code"] = json!(status.as_u16());
        // This measures handler/response-header completion, not body streaming.
        details["duration_ms"] = json!(started.elapsed().as_millis().min(u64::MAX as u128) as u64);
        let outcome = if status.is_server_error() {
            "failed"
        } else if status.is_client_error() {
            "rejected"
        } else {
            "succeeded"
        };
        if state
            .append(input(outcome, details), "completed")
            .await
            .is_err()
        {
            tracing::error!(%request_id, "HTTP request completion audit could not be persisted");
            return unavailable(request_id, "completed");
        }
        with_request_id(response, request_id)
    })
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{server::build_router, AppState};
    use agentd_core::{CapabilityEngine, RuntimeEngine};
    use agentd_store::AuditQuery;
    use axum::{
        body::{to_bytes, Body},
        http::{Method, Request as HttpRequest},
        middleware,
        routing::post,
        Router,
    };
    use std::sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    };
    use tempfile::TempDir;
    use tokio::sync::{Mutex, Semaphore};
    use tower::ServiceExt;

    async fn fixture(token: Option<&str>) -> (TempDir, AgentdStore, Router) {
        let dir = TempDir::new().unwrap();
        let store = AgentdStore::new(dir.path().join("agentd.db").to_str().unwrap())
            .await
            .unwrap();
        let capabilities = CapabilityEngine::new(store.clone());
        let state = AppState {
            store: store.clone(),
            capabilities: capabilities.clone(),
            runtime: RuntimeEngine::new(capabilities, store.clone()),
            run_permits: Arc::new(Semaphore::new(1)),
            running_tasks: Arc::new(Mutex::new(Default::default())),
            dispatch_poll_interval_ms: 25,
            shutting_down: Arc::new(AtomicBool::new(false)),
        };
        let app = build_router(state, token.map(str::to_owned));
        (dir, store, app)
    }

    async fn send(
        app: &Router,
        method: Method,
        uri: &str,
        token: Option<&str>,
        body: &str,
    ) -> (StatusCode, Uuid, Value) {
        let mut builder = HttpRequest::builder()
            .method(method)
            .uri(uri)
            .header("content-type", "application/json")
            .header("x-actor", "forged-actor-private")
            .header("x-request-id", "00000000-0000-0000-0000-000000000001")
            .header("x-forwarded-for", "203.0.113.77");
        if let Some(token) = token {
            builder = builder.header("authorization", format!("Bearer {token}"));
        }
        let response = app
            .clone()
            .oneshot(builder.body(Body::from(body.to_owned())).unwrap())
            .await
            .unwrap();
        let status = response.status();
        let request_id = response.headers()["x-request-id"]
            .to_str()
            .unwrap()
            .parse::<Uuid>()
            .unwrap();
        let bytes = to_bytes(response.into_body(), 1_048_576).await.unwrap();
        let value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
        (status, request_id, value)
    }

    async fn events_for(store: &AgentdStore, request_id: Uuid) -> Value {
        serde_json::to_value(
            store
                .list_audit_events(&AuditQuery {
                    request_id: Some(request_id),
                    limit: Some(100),
                    ..Default::default()
                })
                .await
                .unwrap(),
        )
        .unwrap()["events"]
            .clone()
    }

    #[tokio::test]
    async fn rejected_auth_is_audited_without_credential_or_actor_spoofing() {
        let (_dir, store, app) = fixture(Some("real-private-api-token")).await;
        let (status, request_id, _) = send(
            &app,
            Method::GET,
            "/v1/tenants?private_query=never-record-query",
            Some("wrong-private-api-token"),
            "never-record-body",
        )
        .await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert_ne!(
            request_id.to_string(),
            "00000000-0000-0000-0000-000000000001"
        );
        let events = events_for(&store, request_id).await;
        let events = events.as_array().unwrap();
        assert_eq!(events.len(), 2);
        assert!(events.iter().all(|event| {
            event["actor_kind"] == "api"
                && event["actor_id"] == "unauthenticated"
                && event["details"]["route"] == "/v1/tenants"
                && event["details"].get("peer_addr").is_none()
        }));
        assert!(events.iter().any(|event| {
            event["outcome"] == "rejected" && event["details"]["status_code"] == 401
        }));
        let serialized = serde_json::to_string(events).unwrap();
        for secret in [
            "real-private-api-token",
            "wrong-private-api-token",
            "never-record-query",
            "never-record-body",
            "forged-actor-private",
            "203.0.113.77",
        ] {
            assert!(!serialized.contains(secret));
        }
    }

    #[tokio::test]
    async fn scoped_reads_record_identifiers_and_run_link_without_search_content() {
        let (_dir, store, app) = fixture(None).await;
        let (_, request_id, _) = send(
            &app,
            Method::GET,
            "/v1/tenants/demo/memory/item?namespace=profile&query=PRIVATE_SEARCH",
            None,
            "",
        )
        .await;
        let events = events_for(&store, request_id).await;
        assert!(events
            .as_array()
            .unwrap()
            .iter()
            .all(|event| event["details"]["namespace"] == "profile"));
        assert!(!events.to_string().contains("PRIVATE_SEARCH"));
        let (_, request_id, _) = send(
            &app,
            Method::GET,
            "/v1/tenants/demo/contexts/bot/chat/42",
            None,
            "",
        )
        .await;
        assert!(events_for(&store, request_id)
            .await
            .as_array()
            .unwrap()
            .iter()
            .all(|event| event["details"]["scope"] == "chat/42"));
        let run_id = Uuid::new_v4();
        let (_, request_id, _) = send(
            &app,
            Method::GET,
            &format!("/v1/tenants/demo/runs/{run_id}/trace"),
            None,
            "",
        )
        .await;
        assert!(events_for(&store, request_id)
            .await
            .as_array()
            .unwrap()
            .iter()
            .all(|event| event["run_id"] == run_id.to_string()));
    }

    #[tokio::test]
    async fn extractor_rejection_fallback_and_method_errors_are_audited() {
        let (_dir, store, app) = fixture(Some("token")).await;
        for (method, uri, body, expected, route) in [
            (
                Method::POST,
                "/v1/tenants",
                "{invalid-private-json",
                StatusCode::BAD_REQUEST,
                "/v1/tenants",
            ),
            (
                Method::GET,
                "/v1/no-such-private-route?secret=private",
                "",
                StatusCode::NOT_FOUND,
                "unmatched",
            ),
            (
                Method::DELETE,
                "/v1/tenants",
                "",
                StatusCode::METHOD_NOT_ALLOWED,
                "/v1/tenants",
            ),
        ] {
            let (status, request_id, _) = send(&app, method, uri, Some("token"), body).await;
            assert_eq!(status, expected);
            let events = events_for(&store, request_id).await;
            assert_eq!(events.as_array().unwrap().len(), 2);
            assert!(events.as_array().unwrap().iter().all(|event| {
                event["actor_id"] == "shared_api_token" && event["details"]["route"] == route
            }));
            let serialized = events.to_string();
            assert!(!serialized.contains("invalid-private-json"));
            assert!(!serialized.contains("no-such-private-route"));
        }
    }

    #[tokio::test]
    async fn real_request_id_links_store_mutation_without_recording_bodies() {
        let (_dir, store, app) = fixture(Some("token")).await;
        store.create_tenant("audit-link", &json!({})).await.unwrap();
        let (status, request_id, _) = send(
            &app,
            Method::PUT,
            "/v1/tenants/audit-link/agents/bot",
            Some("token"),
            r#"{"persona":"secret-persona-do-not-log","allowed_families":[]}"#,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let events = events_for(&store, request_id).await;
        let events = events.as_array().unwrap();
        assert!(events.iter().any(|event| {
            event["action"] != "http.request" && event["resource_type"] == "agent"
        }));
        assert!(events.iter().all(|event| {
            event["actor_id"] == "shared_api_token" && event["tenant"] == "audit-link"
        }));
        assert!(!serde_json::to_string(events)
            .unwrap()
            .contains("secret-persona-do-not-log"));
        let (status, read_id, body) = send(
            &app,
            Method::GET,
            "/v1/tenants/audit-link/agents/bot",
            Some("token"),
            "",
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["persona"], "secret-persona-do-not-log");
        assert!(!events_for(&store, read_id)
            .await
            .to_string()
            .contains("secret-persona-do-not-log"));
    }

    #[tokio::test]
    async fn tenant_audit_is_filtered_paginated_and_survives_tenant_deletion() {
        let (_dir, store, app) = fixture(None).await;
        for tenant in ["audit-one", "audit-two"] {
            store.create_tenant(tenant, &json!({})).await.unwrap();
            for _ in 0..3 {
                store
                    .append_audit(AuditInput::new(
                        Some(tenant),
                        "test.canary",
                        "canary",
                        Some("same-resource"),
                        "succeeded",
                        json!({"count":1}),
                    ))
                    .await
                    .unwrap();
            }
        }
        store.delete_tenant("audit-one").await.unwrap();
        let (status, _, first) = send(
            &app,
            Method::GET,
            "/v1/tenants/audit-one/audit?action=test.canary&limit=2",
            None,
            "",
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let events = first["events"].as_array().unwrap();
        assert_eq!(events.len(), 2);
        assert!(events.iter().all(|event| event["tenant"] == "audit-one"));
        let before = first["next_before_id"].as_i64().unwrap();
        let (_, _, second) = send(
            &app,
            Method::GET,
            &format!("/v1/tenants/audit-one/audit?action=test.canary&limit=2&before_id={before}"),
            None,
            "",
        )
        .await;
        assert_eq!(second["events"].as_array().unwrap().len(), 1);
        assert!(second["events"][0]["id"].as_i64().unwrap() < before);
        assert!(second["next_before_id"].is_null());
        let (status, _, _) = send(
            &app,
            Method::GET,
            "/v1/tenants/audit-one/audit?tenant=audit-two",
            None,
            "",
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        let (status, _, global) = send(
            &app,
            Method::GET,
            "/v1/audit?tenant=audit-one&action=test.canary",
            None,
            "",
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(global["events"].as_array().unwrap().len(), 3);
        assert!(global["events"]
            .as_array()
            .unwrap()
            .iter()
            .all(|event| event["tenant"] == "audit-one"));
    }

    #[tokio::test]
    async fn audit_failure_prevents_handler_or_reports_completion_failure() {
        let (_dir, store, _) = fixture(None).await;
        for (stage, expected_calls) in [("started", 0), ("completed", 1)] {
            let calls = Arc::new(AtomicUsize::new(0));
            let handler_calls = calls.clone();
            let mut audit_state = HttpAuditState::new(store.clone(), None);
            audit_state.fail_at = Some(stage);
            let app = Router::new()
                .route(
                    "/v1/canary",
                    post(move || {
                        let calls = handler_calls.clone();
                        async move {
                            calls.fetch_add(1, Ordering::SeqCst);
                            StatusCode::CREATED
                        }
                    }),
                )
                .layer(middleware::from_fn_with_state(audit_state, audit_request));
            let response = app
                .oneshot(
                    HttpRequest::builder()
                        .method(Method::POST)
                        .uri("/v1/canary")
                        .header("origin", "https://console.example")
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
            assert_eq!(response.headers()["access-control-allow-origin"], "*");
            assert_eq!(
                response.headers()["access-control-expose-headers"],
                "x-request-id"
            );
            let request_id = response.headers()["x-request-id"]
                .to_str()
                .unwrap()
                .parse()
                .unwrap();
            let bytes = to_bytes(response.into_body(), 1024).await.unwrap();
            let body: Value = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(calls.load(Ordering::SeqCst), expected_calls);
            assert_eq!(body["audit_stage"], stage);
            assert!(!body.to_string().contains("rollback"));
            let events = events_for(&store, request_id).await;
            assert_eq!(events.as_array().unwrap().len(), expected_calls);
        }
    }

    #[tokio::test]
    async fn static_console_is_not_audited_and_socket_peer_is_not_forwarded_header() {
        let (_dir, store, app) = fixture(None).await;
        let query = AuditQuery {
            action: Some("http.request".into()),
            ..Default::default()
        };
        let before = store.list_audit_events(&query).await.unwrap().events.len();
        let response = app
            .clone()
            .oneshot(
                HttpRequest::builder()
                    .uri("/console")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert!(response.headers().get("x-request-id").is_none());
        assert_eq!(
            store.list_audit_events(&query).await.unwrap().events.len(),
            before
        );
        let peer: SocketAddr = "127.0.0.1:34567".parse().unwrap();
        let response = app
            .oneshot(
                HttpRequest::builder()
                    .uri("/v1/tenants")
                    .header("x-forwarded-for", "203.0.113.77")
                    .extension(ConnectInfo(peer))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let request_id = response.headers()["x-request-id"]
            .to_str()
            .unwrap()
            .parse()
            .unwrap();
        let events = events_for(&store, request_id).await;
        assert!(events
            .as_array()
            .unwrap()
            .iter()
            .all(|event| event["details"]["peer_addr"] == "127.0.0.1:34567"));
        assert!(!events.to_string().contains("203.0.113.77"));
    }
}
