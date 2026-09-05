use agentd_core::{CapabilityEngine, RuntimeEngine};
use agentd_store::AgentdStore;
use std::{
    collections::HashMap,
    sync::{atomic::AtomicBool, Arc},
};
use tokio::sync::{Mutex, Semaphore};
use uuid::Uuid;

#[derive(Clone)]
pub(crate) struct AppState {
    pub(crate) store: AgentdStore,
    pub(crate) capabilities: CapabilityEngine,
    pub(crate) runtime: RuntimeEngine,
    pub(crate) run_permits: Arc<Semaphore>,
    pub(crate) running_tasks: Arc<Mutex<HashMap<Uuid, tokio::task::JoinHandle<()>>>>,
    pub(crate) dispatch_poll_interval_ms: u64,
    pub(crate) shutting_down: Arc<AtomicBool>,
}
