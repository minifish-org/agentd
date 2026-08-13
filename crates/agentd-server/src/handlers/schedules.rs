use crate::{error_response, json_result, AppState};
use agentd_api::ScheduleSpec;
use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};

pub(crate) async fn list_schedules(
    State(state): State<AppState>,
    Path(tenant): Path<String>,
) -> impl IntoResponse {
    json_result(state.store.list_schedules(Some(&tenant)).await)
}

pub(crate) async fn get_schedule(
    State(state): State<AppState>,
    Path((tenant, name)): Path<(String, String)>,
) -> impl IntoResponse {
    match state.store.get_schedule(&tenant, &name).await {
        Ok(Some(schedule)) => (StatusCode::OK, Json(schedule)).into_response(),
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error":"schedule not found"})),
        )
            .into_response(),
        Err(error) => error_response(error),
    }
}

pub(crate) async fn put_schedule(
    State(state): State<AppState>,
    Path((tenant, name)): Path<(String, String)>,
    Json(spec): Json<ScheduleSpec>,
) -> impl IntoResponse {
    match state.store.put_schedule(&tenant, &name, &spec).await {
        Ok(()) => (
            StatusCode::OK,
            Json(serde_json::json!({"tenant":tenant,"name":name})),
        )
            .into_response(),
        Err(error) => error_response(error),
    }
}

pub(crate) async fn delete_schedule(
    State(state): State<AppState>,
    Path((tenant, name)): Path<(String, String)>,
) -> impl IntoResponse {
    json_result(state.store.delete_schedule(&tenant, &name).await)
}
