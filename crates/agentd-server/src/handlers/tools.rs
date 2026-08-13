use crate::{json_result, AppState};
use axum::{
    extract::{Path, Query, State},
    response::IntoResponse,
};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub(crate) struct ToolListQuery {
    pub(crate) agent: Option<String>,
}

pub(crate) async fn list_tools(
    State(state): State<AppState>,
    Path(tenant): Path<String>,
    Query(query): Query<ToolListQuery>,
) -> impl IntoResponse {
    let result = async {
        if let Some(agent_name) = query.agent {
            let agent = state
                .store
                .get_agent(&tenant, &agent_name)
                .await?
                .ok_or_else(|| anyhow::anyhow!("agent not found"))?;
            state
                .store
                .list_visible_tools(&tenant, &agent.spec.effective_allowed_families())
                .await
        } else {
            let mut tools = state.store.list_tools();
            tools.extend(
                state
                    .store
                    .list_visible_tools(&tenant, &[agentd_api::ToolFamily::Mcp])
                    .await?,
            );
            Ok(tools)
        }
    }
    .await;
    json_result(result)
}
