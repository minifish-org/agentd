use crate::{json_result, AppState};
use agentd_store::AuditQuery;
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};

pub(crate) async fn list_audit(
    State(state): State<AppState>,
    Query(query): Query<AuditQuery>,
) -> impl IntoResponse {
    json_result(state.store.list_audit_events(&query).await)
}

pub(crate) async fn list_tenant_audit(
    State(state): State<AppState>,
    Path(tenant): Path<String>,
    Query(mut query): Query<AuditQuery>,
) -> Response {
    if query.tenant.as_ref().is_some_and(|value| value != &tenant) {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error":"audit query tenant must match the route tenant"})),
        )
            .into_response();
    }
    // Retained audit history must remain readable after its tenant is deleted.
    query.tenant = Some(tenant);
    json_result(state.store.list_audit_events(&query).await)
}
