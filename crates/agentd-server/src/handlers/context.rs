use crate::{json_result, AppState};
use axum::{
    extract::{Path, State},
    response::IntoResponse,
};

pub(crate) async fn list_context_scopes(
    State(state): State<AppState>,
    Path((tenant, agent)): Path<(String, String)>,
) -> impl IntoResponse {
    json_result(
        state
            .store
            .list_context_scopes(&tenant, &agent)
            .await
            .map(|scopes| serde_json::json!({"scopes":scopes})),
    )
}

pub(crate) async fn get_context(
    State(state): State<AppState>,
    Path((tenant, agent, scope)): Path<(String, String, String)>,
) -> impl IntoResponse {
    let scope = scope.trim_start_matches('/');
    json_result(state.store.get_context_state(&tenant, &agent, scope).await)
}

pub(crate) async fn delete_context(
    State(state): State<AppState>,
    Path((tenant, agent, scope)): Path<(String, String, String)>,
) -> impl IntoResponse {
    let scope = scope.trim_start_matches('/');
    json_result(
        state
            .store
            .delete_context_state(&tenant, &agent, scope)
            .await
            .map(|deleted| serde_json::json!({"deleted":deleted})),
    )
}
