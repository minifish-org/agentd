use crate::supervisor::RunSupervisor;
use agentd_core::{CapabilityEngine, RuntimeEngine};
use agentd_store::AgentdStore;

#[derive(Clone)]
pub(crate) struct AppState {
    pub(crate) store: AgentdStore,
    pub(crate) capabilities: CapabilityEngine,
    pub(crate) runtime: RuntimeEngine,
    pub(crate) supervisor: RunSupervisor,
    pub(crate) dispatch_poll_interval_ms: u64,
}

impl AppState {
    pub(crate) fn new(
        store: AgentdStore,
        capabilities: CapabilityEngine,
        concurrency: usize,
        dispatch_poll_interval_ms: u64,
    ) -> Self {
        Self {
            runtime: RuntimeEngine::new(capabilities.clone(), store.clone()),
            store,
            capabilities,
            supervisor: RunSupervisor::new(concurrency),
            dispatch_poll_interval_ms,
        }
    }
}
