use crate::{json_result, AppState};
use axum::{
    extract::{Path, Query, State},
    response::IntoResponse,
};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub(crate) struct MemoryQuery {
    pub(crate) namespace: Option<String>,
    pub(crate) query: Option<String>,
    pub(crate) limit: Option<usize>,
}

pub(crate) async fn get_memory_item(
    State(state): State<AppState>,
    Path((tenant, id)): Path<(String, String)>,
    Query(query): Query<MemoryQuery>,
) -> impl IntoResponse {
    json_result(
        state
            .store
            .get_memory(
                &tenant,
                query.namespace.as_deref().unwrap_or("default"),
                &id,
            )
            .await,
    )
}

pub(crate) async fn search_memory(
    State(state): State<AppState>,
    Path(tenant): Path<String>,
    Query(query): Query<MemoryQuery>,
) -> impl IntoResponse {
    let result = async {
        let text = query
            .query
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("query is required"))?;
        state
            .capabilities
            .search_memory(
                &tenant,
                query.namespace.as_deref().unwrap_or("default"),
                text,
                query.limit.unwrap_or(5),
            )
            .await
    }
    .await;
    json_result(result)
}
