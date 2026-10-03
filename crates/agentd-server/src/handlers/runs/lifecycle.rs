use super::*;

pub(crate) async fn list_runs(
    State(state): State<AppState>,
    Path(tenant): Path<String>,
    Query(query): Query<RunQuery>,
) -> impl IntoResponse {
    let status = match query.status.as_deref() {
        Some(raw) => match parse_status(raw) {
            Ok(status) => Some(status),
            Err(error) => return error_response(error),
        },
        None => None,
    };
    let query = RunListQuery {
        tenant: Some(tenant),
        agent_ref: query.agent_ref,
        status,
        limit: query.limit.unwrap_or(50),
    };
    json_result(state.store.list_runs(&query).await)
}

pub(crate) async fn get_run(
    State(state): State<AppState>,
    Path((tenant, id)): Path<(String, String)>,
) -> impl IntoResponse {
    match parse_uuid(&id) {
        Ok(result) => match state.store.get_run(result).await {
            Ok(Some(run)) if run.tenant == tenant => (StatusCode::OK, Json(run)).into_response(),
            Ok(Some(_)) | Ok(None) => (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({"error": "run not found"})),
            )
                .into_response(),
            Err(error) => error_response(error),
        },
        Err(error) => error_response(error),
    }
}

pub(crate) async fn wait_run(
    State(state): State<AppState>,
    Path((tenant, id)): Path<(String, String)>,
    Query(query): Query<RunWaitQuery>,
) -> impl IntoResponse {
    let timeout = std::time::Duration::from_millis(query.timeout_ms.unwrap_or(30_000).max(1));
    let Ok(run_id) = parse_uuid(&id) else {
        return error_response(agentd_store::StoreError::Validation(
            "invalid run id".into(),
        ));
    };
    let Some(deadline) = tokio::time::Instant::now().checked_add(timeout) else {
        return error_response(agentd_store::StoreError::Validation(
            "wait timeout is too large".into(),
        ));
    };
    loop {
        match state.store.get_run(run_id).await {
            Ok(Some(run)) => {
                if run.tenant != tenant {
                    return error_response(agentd_store::StoreError::NotFound(
                        "run not found".into(),
                    ));
                }
                let status = run.status;
                if matches!(
                    run.status,
                    AgentRunStatus::Succeeded | AgentRunStatus::Failed | AgentRunStatus::Cancelled
                ) {
                    let error = run.error.clone();
                    return json_result(state.store.get_run_output(run_id).await.map(|output| {
                        serde_json::json!({
                            "run_id": run_id,
                            "status": status,
                            "timed_out": false,
                            "output": output,
                            "error": error,
                        })
                    }));
                }
                if tokio::time::Instant::now() >= deadline || state.supervisor.is_shutting_down() {
                    return (
                        StatusCode::OK,
                        Json(serde_json::json!({
                            "run_id": run_id,
                            "status": status,
                            "timed_out": true,
                        })),
                    )
                        .into_response();
                }
            }
            Ok(None) => {
                return (
                    StatusCode::NOT_FOUND,
                    Json(serde_json::json!({"error": "run not found"})),
                )
                    .into_response();
            }
            Err(error) => return error_response(error),
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

pub(crate) async fn cancel_run(
    State(state): State<AppState>,
    Path((tenant, id)): Path<(String, String)>,
    Json(req): Json<CancelReq>,
) -> impl IntoResponse {
    let run_id = match parse_uuid(&id) {
        Ok(run_id) => run_id,
        Err(error) => return error_response(error),
    };
    match state
        .supervisor
        .cancel(
            &state,
            &tenant,
            run_id,
            req.reason.as_deref().unwrap_or("cancelled"),
        )
        .await
    {
        Ok(status) => (StatusCode::OK, Json(serde_json::json!({"status": status}))).into_response(),
        Err(error) => error_response(error),
    }
}

pub(crate) fn parse_status(raw: &str) -> Result<AgentRunStatus> {
    match raw {
        "queued" => Ok(AgentRunStatus::Queued),
        "running" => Ok(AgentRunStatus::Running),
        "succeeded" => Ok(AgentRunStatus::Succeeded),
        "failed" => Ok(AgentRunStatus::Failed),
        "cancelled" => Ok(AgentRunStatus::Cancelled),
        other => {
            Err(agentd_store::StoreError::Validation(format!("invalid status: {other}")).into())
        }
    }
}
