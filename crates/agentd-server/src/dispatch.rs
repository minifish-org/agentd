use crate::AppState;
use agentd_api::AgentRunStatus;
use anyhow::Result;
use chrono::Utc;
use std::time::Duration;

pub(crate) async fn run_local_dispatch_loop(state: AppState) {
    let mut interval =
        tokio::time::interval(Duration::from_millis(state.dispatch_poll_interval_ms));
    loop {
        interval.tick().await;
        loop {
            let Ok(permit) = state.run_permits.clone().try_acquire_owned() else {
                break;
            };
            let assigned = match state.store.claim_next_run().await {
                Ok(Some(assigned)) => assigned,
                Ok(None) => break,
                Err(error) => {
                    tracing::error!(error = %error, "failed to claim next run");
                    break;
                }
            };
            let run_id = assigned.run.run_id;
            let task_state = state.clone();
            let (start_tx, start_rx) = tokio::sync::oneshot::channel();
            let handle = tokio::spawn(async move {
                let _permit = permit;
                if start_rx.await.is_err() {
                    return;
                }
                let execution = execute_local_run_if_running(task_state.clone(), assigned).await;
                if let Err(error) = execution {
                    tracing::error!(run_id = %run_id, error = %error, "local run execution failed");
                    let _ = task_state.store.fail_run(run_id, &error.to_string()).await;
                }
                task_state.running_tasks.lock().await.remove(&run_id);
            });
            state.running_tasks.lock().await.insert(run_id, handle);
            let _ = start_tx.send(());
        }
    }
}

async fn execute_local_run_if_running(
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
    assigned: agentd_store::AssignedRun,
) -> Result<()> {
    state
        .store
        .append_event(
            assigned.run.run_id,
            "status",
            serde_json::json!({"status":"running"}),
            Utc::now(),
        )
        .await?;
    let report = match tokio::time::timeout(
        Duration::from_millis(assigned.timeout_ms.max(1)),
        state.runtime.execute_assigned_run(&assigned),
    )
    .await
    {
        Ok(report) => report?,
        Err(_) => {
            state
                .store
                .fail_run(assigned.run.run_id, "run timeout exceeded")
                .await?;
            return Ok(());
        }
    };
    if let Some(error) = report.error {
        state.store.fail_run(assigned.run.run_id, &error).await?;
        return Ok(());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use agentd_api::{AgentLimits, AgentResource, AgentSpec, ResourceMeta};
    use agentd_core::{CapabilityEngine, RuntimeEngine};
    use agentd_store::NewRun;
    use serde_json::json;
    use std::{collections::BTreeMap, sync::Arc};
    use tempfile::TempDir;
    use tokio::sync::{Mutex, Semaphore};

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
        let state = AppState {
            store: store.clone(),
            capabilities: capabilities.clone(),
            runtime: RuntimeEngine::new(capabilities, store.clone()),
            run_permits: Arc::new(Semaphore::new(1)),
            running_tasks: Arc::new(Mutex::new(Default::default())),
            dispatch_poll_interval_ms: 1,
        };

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
