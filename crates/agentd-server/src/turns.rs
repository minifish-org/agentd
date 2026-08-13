use crate::AppState;
use agentd_store::NewRun;
use anyhow::Result;
use serde_json::Value;
use uuid::Uuid;

pub(crate) struct SubmitTurn<'a> {
    pub tenant: &'a str,
    pub agent_ref: &'a str,
    pub scope: &'a str,
    pub transport: &'a str,
    pub request: Value,
    pub run_name: String,
    pub request_id: Option<String>,
    pub delivery_destination: Option<String>,
}

pub(crate) async fn submit_turn(state: &AppState, turn: SubmitTurn<'_>) -> Result<Uuid> {
    state
        .store
        .submit_run(NewRun {
            tenant: turn.tenant,
            name: &turn.run_name,
            agent_ref: turn.agent_ref,
            scope: turn.scope,
            source: turn.transport,
            input: &turn.request,
            request_id: turn.request_id.as_deref(),
            schedule_name: None,
            delivery_destination: turn.delivery_destination.as_deref(),
        })
        .await
}
