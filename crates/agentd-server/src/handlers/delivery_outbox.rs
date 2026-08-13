use crate::{error_response, json_result, parse_uuid, AppState};
use axum::{
    extract::{Path, Query, State},
    response::IntoResponse,
    Json,
};
use chrono::Utc;
use serde::Deserialize;
use std::time::Duration;

const CLAIM_TTL: Duration = Duration::from_secs(60);

#[derive(Debug, Deserialize)]
pub(crate) struct DeliveryListQuery {
    pub(crate) status: Option<String>,
    pub(crate) run_id: Option<String>,
    pub(crate) limit: Option<usize>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct DeliveryClaimRequest {
    pub(crate) limit: Option<usize>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct DeliveryAckRequest {
    pub(crate) claim_token: String,
    pub(crate) outcome: String,
    #[serde(default)]
    pub(crate) error: Option<String>,
    #[serde(default)]
    pub(crate) retry_after_ms: Option<u64>,
}

pub(crate) async fn list_deliveries(
    State(state): State<AppState>,
    Path(tenant): Path<String>,
    Query(query): Query<DeliveryListQuery>,
) -> impl IntoResponse {
    let run_id = match query.run_id.as_deref().map(parse_uuid).transpose() {
        Ok(run_id) => run_id,
        Err(error) => return error_response(error),
    };
    json_result(
        state
            .store
            .list_delivery_outbox(
                Some(&tenant),
                query.status.as_deref(),
                run_id,
                query.limit.unwrap_or(50),
            )
            .await
            .map(|deliveries| serde_json::json!({"deliveries":deliveries})),
    )
}

pub(crate) async fn claim_deliveries(
    State(state): State<AppState>,
    Path(tenant): Path<String>,
    Json(req): Json<DeliveryClaimRequest>,
) -> impl IntoResponse {
    json_result(
        state
            .store
            .claim_delivery_outbox(&tenant, req.limit.unwrap_or(10), Utc::now(), CLAIM_TTL)
            .await
            .map(|deliveries| serde_json::json!({"deliveries":deliveries})),
    )
}

pub(crate) async fn ack_delivery(
    State(state): State<AppState>,
    Path((tenant, delivery_id)): Path<(String, String)>,
    Json(req): Json<DeliveryAckRequest>,
) -> impl IntoResponse {
    let delivery_id = match parse_uuid(&delivery_id) {
        Ok(delivery_id) => delivery_id,
        Err(error) => return error_response(error),
    };
    json_result(
        state
            .store
            .ack_delivery(
                &tenant,
                agentd_store::DeliveryAck {
                    delivery_id,
                    claim_token: &req.claim_token,
                    outcome: &req.outcome,
                    error: req.error.as_deref(),
                    retry_after: req.retry_after_ms.map(Duration::from_millis),
                    now: Utc::now(),
                },
            )
            .await,
    )
}
