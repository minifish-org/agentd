use crate::turns::{submit_turn, SubmitTurn};
use crate::{error_response, AppState};
use agentd_api::DeliveryRequest;
use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use serde::Deserialize;
use uuid::Uuid;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct TurnRequest {
    pub agent: String,
    pub scope: String,
    #[serde(default)]
    pub payload: serde_json::Value,
    #[serde(default)]
    pub request_id: Option<String>,
    #[serde(default)]
    pub delivery: Option<DeliveryRequest>,
}

/// Tenant-scoped, transport-neutral asynchronous turn submission. Results are
/// always stored on the run and may be pulled through the run wait endpoint.
/// A delivery is created only when the caller supplies an explicit destination.
pub(crate) async fn submit_turn_endpoint(
    State(state): State<AppState>,
    Path(tenant): Path<String>,
    Json(req): Json<TurnRequest>,
) -> impl IntoResponse {
    if let Some(delivery) = &req.delivery {
        if let Err(error) = delivery.validate() {
            return error_response(error);
        }
    }
    let id = Uuid::new_v4();
    let request = req.payload.clone();

    let run_id = match submit_turn(
        &state,
        SubmitTurn {
            tenant: &tenant,
            agent_ref: &req.agent,
            scope: &req.scope,
            transport: "api",
            request,
            run_name: format!("api-turn-{id}"),
            request_id: req.request_id,
            delivery_destination: req.delivery.map(|delivery| delivery.destination),
        },
    )
    .await
    {
        Ok(run_id) => run_id,
        Err(error) => return error_response(error),
    };

    (
        StatusCode::ACCEPTED,
        Json(serde_json::json!({ "run_id": run_id, "status": "queued" })),
    )
        .into_response()
}
