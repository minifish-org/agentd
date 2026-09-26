use crate::{error_response, AppState};
use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};

pub(crate) async fn get_behavior_learning(
    State(state): State<AppState>,
    Path((tenant, agent)): Path<(String, String)>,
) -> impl IntoResponse {
    let snapshot = match state.store.get_behavior_snapshot(&tenant, &agent).await {
        Ok(Some(snapshot)) => snapshot,
        Ok(None) => {
            return (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({"error":"tenant or agent not found"})),
            )
                .into_response();
        }
        Err(error) => return error_response(error),
    };
    match state
        .store
        .list_behavior_revisions(&tenant, &agent, 20)
        .await
    {
        Ok(history) => (
            StatusCode::OK,
            Json(serde_json::json!({
                "tenant": tenant,
                "agent": agent,
                "active_revision": snapshot.active_revision,
                "history": history,
            })),
        )
            .into_response(),
        Err(error) => error_response(error),
    }
}

pub(crate) async fn clear_behavior_learning(
    State(state): State<AppState>,
    Path((tenant, agent)): Path<(String, String)>,
) -> impl IntoResponse {
    match state.store.get_behavior_snapshot(&tenant, &agent).await {
        Ok(Some(_)) => {}
        Ok(None) => {
            return (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({"error":"tenant or agent not found"})),
            )
                .into_response();
        }
        Err(error) => return error_response(error),
    }
    match state.store.clear_behavior_policy(&tenant, &agent).await {
        Ok(cleared) => (
            StatusCode::OK,
            Json(serde_json::json!({"tenant":tenant,"agent":agent,"cleared":cleared})),
        )
            .into_response(),
        Err(error) => error_response(error),
    }
}
