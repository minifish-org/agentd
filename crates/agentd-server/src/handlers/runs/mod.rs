use crate::{error_response, json_result, parse_uuid, AppState};
use agentd_api::AgentRunStatus;
use agentd_store::RunListQuery;
use anyhow::Result;
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use serde::Deserialize;
use std::time::Duration;

mod lifecycle;

pub(crate) use lifecycle::{cancel_run, get_run, list_runs, wait_run};

#[derive(Debug, Deserialize)]
pub(crate) struct RunQuery {
    pub(crate) agent_ref: Option<String>,
    pub(crate) status: Option<String>,
    pub(crate) limit: Option<usize>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct CancelReq {
    pub(crate) reason: Option<String>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct RunWaitQuery {
    pub(crate) timeout_ms: Option<u64>,
}

pub(crate) async fn get_run_trace(
    State(state): State<AppState>,
    Path((tenant, id)): Path<(String, String)>,
) -> impl IntoResponse {
    let run_id = match parse_uuid(&id) {
        Ok(value) => value,
        Err(error) => return error_response(error),
    };
    match state.store.get_run(run_id).await {
        Ok(Some(run)) if run.tenant == tenant => {}
        Ok(Some(_)) | Ok(None) => return error_response("run not found"),
        Err(error) => return error_response(error),
    }
    match state.store.list_run_log(run_id).await {
        Ok(log) => json_result(Ok(log)),
        Err(error) => error_response(error),
    }
}
