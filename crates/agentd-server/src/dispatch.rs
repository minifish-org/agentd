use crate::AppState;
use agentd_api::AgentRunStatus;
use anyhow::Result;
use chrono::Utc;
use std::time::Duration;

pub(crate) async fn run_local_dispatch_loop(state: AppState) {
    agentd_store::with_audit_context(
        agentd_store::AuditContext::system("dispatcher"),
        dispatch_loop(state),
    )
    .await
}

async fn dispatch_loop(state: AppState) {
    let mut interval =
        tokio::time::interval(Duration::from_millis(state.dispatch_poll_interval_ms));
    while !state.supervisor.is_shutting_down() {
        interval.tick().await;
        loop {
            match state.supervisor.dispatch_next(&state).await {
                Ok(true) => {}
                Ok(false) => break,
                Err(error) => {
                    tracing::error!(%error, "failed to dispatch next run");
                    break;
                }
            }
        }
    }
}

pub(crate) async fn execute_local_run_if_running(
    state: AppState,
    assigned: agentd_store::AssignedRun,
) -> Result<()> {
    let Some(run) = state.store.get_run(assigned.run.run_id).await? else {
        return Ok(());
    };
    if run.status != AgentRunStatus::Running {
        return Ok(());
    }
    execute_local_run(state, assigned).await
}

pub(crate) async fn execute_local_run(
    state: AppState,
    mut assigned: agentd_store::AssignedRun,
) -> Result<()> {
    state
        .capabilities
        .retain_available_tools(&mut assigned.visible_tools);
    state
        .store
        .append_event(
            assigned.run.run_id,
            "status",
            serde_json::json!({"status":"running"}),
            Utc::now(),
        )
        .await?;
    let report = tokio::time::timeout(
        Duration::from_millis(assigned.timeout_ms.max(1)),
        state.runtime.execute_assigned_run(&assigned),
    )
    .await
    .map_err(|_| anyhow::anyhow!("run timeout exceeded"))??;
    if let Some(error) = report.error {
        anyhow::bail!(error);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use agentd_api::{AgentLimits, AgentResource, AgentSpec, ResourceMeta};
    use agentd_core::CapabilityEngine;
    use agentd_store::NewRun;
    use serde_json::json;
    use std::collections::BTreeMap;
    use tempfile::TempDir;

    #[tokio::test]
    async fn cancelled_assignment_is_not_executed_after_dispatch_registration() {
        let dir = TempDir::new().unwrap();
        let store = agentd_store::AgentdStore::new(dir.path().join("agentd.db").to_str().unwrap())
            .await
            .unwrap();
        store.create_tenant("demo", &json!({})).await.unwrap();
        store
            .apply_agent(&AgentResource {
                metadata: ResourceMeta {
                    name: "bot".into(),
                    tenant: "demo".into(),
                    labels: BTreeMap::new(),
                },
                spec: AgentSpec {
                    allowed_families: None,
                    limits: AgentLimits {
                        timeout_ms: 1_000,
                        max_steps: 1,
                    },
                    system_prompt: None,
                    model: None,
                    temperature: None,
                    max_tokens: None,
                    context_window: None,
                },
            })
            .await
            .unwrap();
        let run_id = store
            .submit_run(NewRun {
                tenant: "demo",
                name: "turn",
                agent_ref: "bot",
                scope: "chat:1",
                source: "test",
                input: &json!({"text":"do not execute"}),
                request_id: None,
                schedule_name: None,
                delivery_destination: None,
            })
            .await
            .unwrap();
        let assigned = store.claim_next_run().await.unwrap().unwrap();
        store
            .cancel_run_request(run_id, "test cancel")
            .await
            .unwrap();
        let capabilities = CapabilityEngine::new(store.clone());
        let state = AppState::new(store.clone(), capabilities, 1, 1);

        execute_local_run_if_running(state, assigned).await.unwrap();

        assert_eq!(
            store.get_run(run_id).await.unwrap().unwrap().status,
            AgentRunStatus::Cancelled
        );
        let trace = store.list_run_log(run_id).await.unwrap();
        assert!(trace
            .iter()
            .all(|event| event.payload["status"] != "running"));
    }
}
