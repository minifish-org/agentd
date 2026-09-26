use agentd_store::AgentdStore;
use anyhow::Result;
use std::time::Duration;
use tokio::time;
use tracing::info;

#[derive(Clone)]
pub struct Scheduler {
    store: AgentdStore,
    tick: Duration,
}

impl Scheduler {
    pub fn new(store: AgentdStore, tick: Duration) -> Self {
        Self { store, tick }
    }

    pub async fn run_forever(self) {
        let mut interval = time::interval(self.tick);
        loop {
            interval.tick().await;
            if let Err(error) = self.run_once().await {
                tracing::error!(error = %error, "scheduler tick failed");
            }
        }
    }

    pub async fn run_once(&self) -> Result<()> {
        let triggered = agentd_store::with_audit_context(
            agentd_store::AuditContext::system("scheduler"),
            self.store.trigger_due_schedules(chrono::Utc::now(), 32),
        )
        .await?;
        if !triggered.is_empty() {
            info!(count = triggered.len(), "due schedules triggered");
        }
        Ok(())
    }
}
