use crate::{dispatch::execute_local_run_if_running, AppState};
use agentd_api::AgentRunStatus;
use agentd_store::{AuditContext, NewRun, StoreError};
use anyhow::Result;
use std::{
    collections::HashMap,
    future::Future,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};
use tokio::{
    sync::{watch, Mutex, OwnedSemaphorePermit, Semaphore},
    task::AbortHandle,
};
use uuid::Uuid;

#[derive(Clone)]
pub(crate) struct RunSupervisor {
    permits: Arc<Semaphore>,
    registry: Arc<Mutex<Registry>>,
    shutting_down: Arc<AtomicBool>,
}

#[derive(Default)]
struct Registry {
    tasks: HashMap<Uuid, RunTask>,
    operations: HashMap<Uuid, Operation>,
    pending_failures: HashMap<Uuid, PendingFailure>,
}

struct RunTask {
    tenant: String,
    abort: AbortHandle,
    done: Completion,
}

type Completion = watch::Receiver<Option<std::result::Result<(), String>>>;

#[derive(Clone)]
struct PendingFailure {
    tenant: String,
    reason: String,
}

struct Operation {
    deleting_tenant: Option<String>,
    done: Completion,
}

impl Registry {
    fn tenant_is_deleting(&self, tenant: &str) -> bool {
        self.operations
            .values()
            .any(|operation| operation.deleting_tenant.as_deref() == Some(tenant))
    }
}

impl RunSupervisor {
    pub(crate) fn new(concurrency: usize) -> Self {
        Self {
            permits: Arc::new(Semaphore::new(concurrency.max(1))),
            registry: Arc::new(Mutex::new(Registry::default())),
            shutting_down: Arc::new(AtomicBool::new(false)),
        }
    }

    pub(crate) fn is_shutting_down(&self) -> bool {
        self.shutting_down.load(Ordering::SeqCst)
    }

    pub(crate) async fn submit_run(&self, state: &AppState, run: NewRun<'_>) -> Result<Uuid> {
        let registry = self.registry.lock().await;
        self.ensure_accepting(&registry, run.tenant)?;
        state.store.submit_run(run).await
    }

    fn ensure_accepting(&self, registry: &Registry, tenant: &str) -> Result<()> {
        if self.is_shutting_down() {
            return Err(StoreError::Conflict("server shutting down".into()).into());
        }
        if registry.tenant_is_deleting(tenant) {
            return Err(StoreError::Conflict("tenant deletion in progress".into()).into());
        }
        Ok(())
    }

    /// Claim and registration share the same lock as cancellation and tenant
    /// teardown, so no assigned task can escape either operation.
    pub(crate) async fn dispatch_next(&self, state: &AppState) -> Result<bool> {
        let mut registry = self.registry.lock().await;
        if self.is_shutting_down() {
            return Ok(false);
        }
        let Ok(permit) = self.permits.clone().try_acquire_owned() else {
            return Ok(false);
        };
        let excluded = registry
            .operations
            .values()
            .filter_map(|operation| operation.deleting_tenant.clone())
            .collect::<Vec<_>>();
        let Some(assigned) = state
            .store
            .claim_next_run_excluding_tenants(&excluded)
            .await?
        else {
            return Ok(false);
        };
        if self.is_shutting_down() {
            let result = state
                .store
                .fail_run(assigned.run.run_id, "server shutting down")
                .await;
            if result.is_err() {
                registry.pending_failures.insert(
                    assigned.run.run_id,
                    PendingFailure {
                        tenant: assigned.run.tenant,
                        reason: "server shutting down".into(),
                    },
                );
            }
            result?;
            return Ok(false);
        }
        let run_id = assigned.run.run_id;
        let tenant = assigned.run.tenant.clone();
        let task_state = state.clone();
        self.supervise(
            &mut registry,
            state.clone(),
            run_id,
            tenant,
            permit,
            async move { execute_local_run_if_running(task_state, assigned).await },
        );
        Ok(true)
    }

    fn supervise<F>(
        &self,
        registry: &mut Registry,
        state: AppState,
        run_id: Uuid,
        tenant: String,
        permit: OwnedSemaphorePermit,
        execution: F,
    ) where
        F: Future<Output = Result<()>> + Send + 'static,
    {
        let (start_tx, start_rx) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            if start_rx.await.is_err() {
                return Ok(());
            }
            agentd_store::with_audit_context(AuditContext::system("dispatcher"), execution).await
        });
        let (done_tx, done) = watch::channel(None);
        registry.tasks.insert(
            run_id,
            RunTask {
                tenant: tenant.clone(),
                abort: task.abort_handle(),
                done,
            },
        );
        let registry = self.registry.clone();
        // This owner always joins execution, including panic and abort. The
        // concurrency permit remains held until cleanup has finished.
        tokio::spawn(async move {
            let failure = match task.await {
                Ok(Ok(())) => None,
                Ok(Err(error)) => Some(error.to_string()),
                Err(error) if error.is_panic() => {
                    tracing::error!(%run_id, %error, "run task panicked");
                    Some("run task panicked".into())
                }
                Err(_) => Some(if state.supervisor.is_shutting_down() {
                    "server shutting down".into()
                } else {
                    "run execution aborted".into()
                }),
            };
            let (result, pending_failure) = agentd_store::with_audit_context(AuditContext::system("dispatcher"), async {
                let mut failure_to_persist = None;
                let mut pending_failure = None;
                if let Some(error) = failure {
                    if let Err(persist_error) = state.store.fail_run(run_id, &error).await {
                        tracing::error!(%run_id, error=%persist_error, "failed to persist run failure");
                        pending_failure = Some(error);
                        failure_to_persist = Some(persist_error);
                    }
                }
                let cleanup = state.capabilities.cleanup_sandbox_run(run_id).await;
                let result = match failure_to_persist { Some(error) => Err(error), None => cleanup };
                (result, pending_failure)
            }).await;
            let mut registry = registry.lock().await;
            registry.tasks.remove(&run_id);
            if let Some(reason) = pending_failure {
                registry
                    .pending_failures
                    .insert(run_id, PendingFailure { tenant, reason });
            }
            drop(registry);
            drop(permit);
            let _ = done_tx.send(Some(result.map_err(|error| error.to_string())));
        });
        let _ = start_tx.send(());
    }

    async fn retry_pending_failures(&self, state: &AppState, tenant: Option<&str>) -> Result<()> {
        let failures = self
            .registry
            .lock()
            .await
            .pending_failures
            .iter()
            .filter(|(_, failure)| tenant.is_none_or(|tenant| failure.tenant == tenant))
            .map(|(run_id, failure)| (*run_id, failure.clone()))
            .collect::<Vec<_>>();
        let mut first_error = None;
        for (run_id, failure) in failures {
            let result = async {
                state.store.fail_run(run_id, &failure.reason).await?;
                if !matches!(state.store.get_run(run_id).await?, Some(run) if matches!(run.status,
                    AgentRunStatus::Succeeded | AgentRunStatus::Failed | AgentRunStatus::Cancelled))
                {
                    anyhow::bail!("run failure could not be confirmed as terminal");
                }
                self.registry.lock().await.pending_failures.remove(&run_id);
                Ok::<_, anyhow::Error>(())
            }
            .await;
            if let Err(error) = result {
                first_error.get_or_insert(error);
            }
        }
        if let Some(error) = first_error {
            return Err(error);
        }
        Ok(())
    }

    /// Operations survive a disconnected client and remain owned by the
    /// supervisor until shutdown has joined their worker and cleanup.
    fn start_operation<T, F>(
        &self,
        registry: &mut Registry,
        deleting_tenant: Option<String>,
        operation: F,
    ) -> tokio::sync::oneshot::Receiver<Result<T>>
    where
        T: Send + 'static,
        F: Future<Output = Result<T>> + Send + 'static,
    {
        let operation_id = Uuid::new_v4();
        let (done_tx, done) = watch::channel(None);
        registry.operations.insert(
            operation_id,
            Operation {
                deleting_tenant,
                done,
            },
        );
        let worker = tokio::spawn(operation);
        let registry = self.registry.clone();
        let (result_tx, result_rx) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let joined = worker.await;
            let completion = joined
                .as_ref()
                .map(|_| ())
                .map_err(|error| error.to_string());
            let result = joined
                .map_err(anyhow::Error::from)
                .and_then(|result| result);
            registry.lock().await.operations.remove(&operation_id);
            let _ = done_tx.send(Some(completion));
            let _ = result_tx.send(result);
        });
        result_rx
    }

    pub(crate) async fn cancel(
        &self,
        state: &AppState,
        tenant: &str,
        run_id: Uuid,
        reason: &str,
    ) -> Result<AgentRunStatus> {
        let mut registry = self.registry.lock().await;
        self.ensure_accepting(&registry, tenant)?;
        let supervisor = self.clone();
        let state = state.clone();
        let tenant = tenant.to_owned();
        let reason = reason.to_owned();
        let audit_context = agentd_store::audit::current_audit_context();
        let result = self.start_operation(
            &mut registry,
            None,
            agentd_store::with_audit_context(audit_context, async move {
                let registry = supervisor.registry.lock().await;
                if !matches!(state.store.get_run(run_id).await?, Some(run) if run.tenant == tenant)
                {
                    return Err(StoreError::NotFound("run not found".into()).into());
                }
                let status = state.store.cancel_run_request(run_id, &reason).await?;
                let done = registry.tasks.get(&run_id).map(|task| {
                    task.abort.abort();
                    task.done.clone()
                });
                drop(registry);
                if let Some(done) = done {
                    wait_for_cleanup(done).await?;
                } else {
                    state.capabilities.cleanup_sandbox_run(run_id).await?;
                }
                Ok(status)
            }),
        );
        drop(registry);
        result.await?
    }

    pub(crate) async fn delete_tenant(
        &self,
        state: &AppState,
        tenant: &str,
    ) -> Result<serde_json::Value> {
        let mut registry = self.registry.lock().await;
        self.ensure_accepting(&registry, tenant)?;
        if tenant == "system" {
            return Err(StoreError::Validation("cannot delete the system tenant".into()).into());
        }
        let completions = registry
            .tasks
            .values()
            .filter(|task| task.tenant == tenant)
            .map(|task| {
                task.abort.abort();
                task.done.clone()
            })
            .collect::<Vec<_>>();
        let task_state = state.clone();
        let task_supervisor = self.clone();
        let task_tenant = tenant.to_owned();
        let audit_context = agentd_store::audit::current_audit_context();
        let result = self.start_operation(
            &mut registry,
            Some(tenant.into()),
            agentd_store::with_audit_context(audit_context, async move {
                let mut failure = None;
                for done in completions {
                    if let Err(error) = wait_for_cleanup(done).await {
                        failure.get_or_insert(error);
                    }
                }
                if let Err(error) = task_supervisor
                    .retry_pending_failures(&task_state, Some(&task_tenant))
                    .await
                {
                    failure.get_or_insert(error);
                }
                // Retry retained failed sandboxes even when their completed run
                // is no longer in the task registry. Never delete on failure.
                if let Err(error) = task_state
                    .capabilities
                    .cleanup_sandbox_tenant(&task_tenant)
                    .await
                {
                    failure.get_or_insert(error);
                }
                task_state
                    .capabilities
                    .cleanup_mcp_tenant(&task_tenant)
                    .await;
                if let Some(error) = failure {
                    return Err(error);
                }
                task_state.store.delete_tenant(&task_tenant).await
            }),
        );
        drop(registry);
        result.await?
    }

    pub(crate) async fn shutdown(&self, state: &AppState) -> Result<()> {
        self.shutting_down.store(true, Ordering::SeqCst);
        let registry = self.registry.lock().await;
        let mut failure = None;
        let operations = registry
            .operations
            .values()
            .map(|operation| operation.done.clone())
            .collect::<Vec<_>>();
        let mut completions = Vec::new();
        for (run_id, task) in &registry.tasks {
            if let Err(error) = state.store.fail_run(*run_id, "server shutting down").await {
                tracing::error!(%run_id, %error, "failed to persist shutdown failure");
                failure.get_or_insert(error);
            }
            task.abort.abort();
            completions.push(task.done.clone());
        }
        drop(registry);
        for done in completions.into_iter().chain(operations) {
            if let Err(error) = wait_for_cleanup(done).await {
                failure.get_or_insert(error);
            }
        }
        if let Err(error) = self.retry_pending_failures(state, None).await {
            failure.get_or_insert(error);
        }
        if let Err(error) = state.capabilities.cleanup_all_sandboxes().await {
            failure.get_or_insert(error);
        }
        if let Some(error) = failure {
            return Err(error);
        }
        Ok(())
    }
}

async fn wait_for_cleanup(mut done: Completion) -> Result<()> {
    loop {
        if let Some(result) = done.borrow_and_update().clone() {
            return result.map_err(anyhow::Error::msg);
        }
        done.changed()
            .await
            .map_err(|_| anyhow::anyhow!("cleanup owner stopped without a result"))?;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use agentd_api::{AgentLimits, AgentResource, AgentSpec, ResourceMeta};
    use agentd_core::CapabilityEngine;
    use serde_json::json;
    use std::time::Duration;
    use tempfile::TempDir;

    async fn fixture() -> (TempDir, AppState, Uuid) {
        let dir = TempDir::new().unwrap();
        let store = agentd_store::AgentdStore::new(dir.path().join("agentd.db").to_str().unwrap())
            .await
            .unwrap();
        store.create_tenant("demo", &json!({})).await.unwrap();
        store
            .apply_agent(&AgentResource {
                metadata: ResourceMeta {
                    tenant: "demo".into(),
                    name: "bot".into(),
                    labels: Default::default(),
                },
                spec: AgentSpec {
                    allowed_families: Some(vec![]),
                    limits: AgentLimits {
                        timeout_ms: 1_000,
                        max_steps: 1,
                    },
                    system_prompt: None,
                    model: None,
                    temperature: None,
                    max_tokens: None,
                    context_window: Some(0),
                },
            })
            .await
            .unwrap();
        let state = AppState::new(store.clone(), CapabilityEngine::new(store.clone()), 1, 1);
        let run_id = queue(&state, "first").await;
        assert_eq!(
            store.claim_next_run().await.unwrap().unwrap().run.run_id,
            run_id
        );
        (dir, state, run_id)
    }

    async fn queue(state: &AppState, name: &str) -> Uuid {
        state
            .supervisor
            .submit_run(
                state,
                NewRun {
                    tenant: "demo",
                    name,
                    agent_ref: "bot",
                    scope: "chat",
                    source: "test",
                    input: &json!({"text":"hello"}),
                    request_id: None,
                    schedule_name: None,
                    delivery_destination: None,
                },
            )
            .await
            .unwrap()
    }

    async fn start<F>(state: &AppState, run_id: Uuid, future: F) -> Completion
    where
        F: Future<Output = Result<()>> + Send + 'static,
    {
        let mut registry = state.supervisor.registry.lock().await;
        let permit = state
            .supervisor
            .permits
            .clone()
            .acquire_owned()
            .await
            .unwrap();
        state.supervisor.supervise(
            &mut registry,
            state.clone(),
            run_id,
            "demo".into(),
            permit,
            future,
        );
        registry.tasks[&run_id].done.clone()
    }

    async fn completed(done: Completion) {
        tokio::time::timeout(Duration::from_secs(2), wait_for_cleanup(done))
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn panicked_run_becomes_failed_and_unblocks_its_scope() {
        let (_dir, state, run_id) = fixture().await;
        let done = start(&state, run_id, async { panic!("injected runtime panic") }).await;
        completed(done).await;
        let run = state.store.get_run(run_id).await.unwrap().unwrap();
        assert_eq!(run.status, AgentRunStatus::Failed);
        assert_eq!(run.error.as_deref(), Some("run task panicked"));
        assert!(state.supervisor.registry.lock().await.tasks.is_empty());
        assert_eq!(state.supervisor.permits.available_permits(), 1);
        let next = queue(&state, "next").await;
        assert_eq!(
            state
                .store
                .claim_next_run()
                .await
                .unwrap()
                .unwrap()
                .run
                .run_id,
            next
        );
    }

    #[tokio::test]
    async fn timeout_failure_is_persisted_and_releases_capacity() {
        let (_dir, state, run_id) = fixture().await;
        let done = start(&state, run_id, async {
            tokio::time::timeout(Duration::from_millis(1), std::future::pending::<()>())
                .await
                .map_err(|_| anyhow::anyhow!("run timeout exceeded"))
        })
        .await;
        completed(done).await;
        let run = state.store.get_run(run_id).await.unwrap().unwrap();
        assert_eq!(run.status, AgentRunStatus::Failed);
        assert_eq!(run.error.as_deref(), Some("run timeout exceeded"));
        assert_eq!(state.supervisor.permits.available_permits(), 1);
    }

    #[tokio::test]
    async fn cancel_and_shutdown_wait_for_execution_and_persist_terminal_states() {
        for cancel in [true, false] {
            let (_dir, state, run_id) = fixture().await;
            let done = start(&state, run_id, std::future::pending()).await;
            if cancel {
                assert_eq!(
                    state
                        .supervisor
                        .cancel(&state, "demo", run_id, "requested")
                        .await
                        .unwrap(),
                    AgentRunStatus::Cancelled
                );
            } else {
                state.supervisor.shutdown(&state).await.unwrap();
            }
            completed(done).await;
            let run = state.store.get_run(run_id).await.unwrap().unwrap();
            assert_eq!(
                run.status,
                if cancel {
                    AgentRunStatus::Cancelled
                } else {
                    AgentRunStatus::Failed
                }
            );
            assert_eq!(
                run.error.as_deref(),
                Some(if cancel {
                    "requested"
                } else {
                    "server shutting down"
                })
            );
            assert!(state.supervisor.registry.lock().await.tasks.is_empty());
            assert_eq!(state.supervisor.permits.available_permits(), 1);
        }
    }

    #[tokio::test]
    async fn disconnected_delete_request_finishes_teardown_and_releases_tenant_gate() {
        let (_dir, state, run_id) = fixture().await;
        let done = start(&state, run_id, std::future::pending()).await;
        // Queue the deletion behind a held registry lock, then take it again
        // after deletion marks the tenant. Cleanup waits behind this test.
        let registry = state.supervisor.registry.lock().await;
        let request_state = state.clone();
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let request = tokio::spawn(async move {
            let _ = started_tx.send(());
            request_state
                .supervisor
                .delete_tenant(&request_state, "demo")
                .await
        });
        started_rx.await.unwrap();
        drop(registry);
        let registry = state.supervisor.registry.lock().await;
        assert!(registry.tenant_is_deleting("demo"));
        request.abort();
        let _ = request.await;
        drop(registry);
        completed(done).await;
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let registry = state.supervisor.registry.lock().await;
                if !registry.tenant_is_deleting("demo") {
                    break;
                }
                drop(registry);
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(state.store.get_tenant("demo").await.unwrap().is_none());
        assert!(state.store.get_run(run_id).await.unwrap().is_none());
        state.store.create_tenant("demo", &json!({})).await.unwrap();
        assert!(!state
            .supervisor
            .registry
            .lock()
            .await
            .tenant_is_deleting("demo"));
    }

    #[tokio::test]
    async fn detached_cancel_and_delete_keep_request_audit_identity() {
        let (_dir, state, run_id) = fixture().await;
        for action in ["run.cancel", "tenant.delete"] {
            let request_id = Uuid::new_v4();
            agentd_store::with_audit_context(AuditContext::api(true, request_id), async {
                if action == "run.cancel" {
                    state
                        .supervisor
                        .cancel(&state, "demo", run_id, "requested")
                        .await
                        .unwrap();
                } else {
                    state
                        .supervisor
                        .delete_tenant(&state, "demo")
                        .await
                        .unwrap();
                }
            })
            .await;
            let events = state
                .store
                .list_audit_events(&agentd_store::AuditQuery {
                    request_id: Some(request_id),
                    action: Some(action.into()),
                    ..Default::default()
                })
                .await
                .unwrap()
                .events;
            assert_eq!(events.len(), 1);
            assert_eq!(events[0].actor_kind, "api");
            assert_eq!(events[0].actor_id, "shared_api_token");
        }
    }

    #[tokio::test]
    async fn shutdown_joins_an_operation_after_its_client_disconnects() {
        let (_dir, state, run_id) = fixture().await;
        let _run_done = start(&state, run_id, std::future::pending()).await;
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        let mut registry = state.supervisor.registry.lock().await;
        let client = state
            .supervisor
            .start_operation(&mut registry, None, async move {
                release_rx.await?;
                Ok(())
            });
        drop(registry);
        drop(client);
        let shutdown_state = state.clone();
        let mut shutdown =
            tokio::spawn(async move { shutdown_state.supervisor.shutdown(&shutdown_state).await });
        tokio::select! {
            result = &mut shutdown => panic!("shutdown abandoned an unfinished operation: {result:?}"),
            _ = tokio::time::sleep(Duration::from_millis(5)) => {},
        }
        release_tx.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(2), shutdown)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(state.supervisor.registry.lock().await.operations.is_empty());
    }

    #[tokio::test]
    async fn failed_run_cleanup_preserves_tenant_and_allows_a_later_delete() {
        let (_dir, state, run_id) = fixture().await;
        let task = tokio::spawn(std::future::pending::<()>());
        let (_done_tx, done) = watch::channel(Some(Err("injected sandbox cleanup failure".into())));
        state.supervisor.registry.lock().await.tasks.insert(
            run_id,
            RunTask {
                tenant: "demo".into(),
                abort: task.abort_handle(),
                done,
            },
        );
        let error = state
            .supervisor
            .delete_tenant(&state, "demo")
            .await
            .unwrap_err();
        assert!(error.to_string().contains("sandbox cleanup failure"));
        let _ = task.await;
        assert!(state.store.get_tenant("demo").await.unwrap().is_some());
        let mut registry = state.supervisor.registry.lock().await;
        assert!(!registry.tenant_is_deleting("demo"));
        // The simulated monitor has finished; a subsequent teardown retries
        // retained sandbox cleanup through the capability engine.
        registry.tasks.remove(&run_id);
        drop(registry);
        state
            .supervisor
            .delete_tenant(&state, "demo")
            .await
            .unwrap();
        assert!(state.store.get_tenant("demo").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn pending_failure_is_cleared_only_after_the_terminal_write_succeeds() {
        let (_dir, state, run_id) = fixture().await;
        state
            .supervisor
            .registry
            .lock()
            .await
            .pending_failures
            .insert(
                run_id,
                PendingFailure {
                    tenant: "demo".into(),
                    reason: "original runtime failure".into(),
                },
            );
        state
            .supervisor
            .retry_pending_failures(&state, Some("demo"))
            .await
            .unwrap();
        assert!(state
            .supervisor
            .registry
            .lock()
            .await
            .pending_failures
            .is_empty());
        let run = state.store.get_run(run_id).await.unwrap().unwrap();
        assert_eq!(run.status, AgentRunStatus::Failed);
        assert_eq!(run.error.as_deref(), Some("original runtime failure"));
    }
}
