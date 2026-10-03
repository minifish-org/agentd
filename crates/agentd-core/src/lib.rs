use agentd_api::{McpToolInvocationTarget, ToolFamily, ToolSpec};
use agentd_store::AgentdStore;
use anyhow::{anyhow, Result};
use serde::Serialize;
use serde_json::json;
use std::time::Duration;

mod behavior;
mod embedding_provider;
mod handlers;
mod llm_provider;
mod mcp_client;
mod model_exchange;
mod reranking_provider;
mod sandbox;
mod storage;
mod time_utils;
mod tool_input;

mod multimodal;
mod runtime;

pub use embedding_provider::{
    BUILTIN_EMBEDDING_ARTIFACT_ID, BUILTIN_EMBEDDING_DIMENSION, BUILTIN_EMBEDDING_MODEL_ID,
};
pub use mcp_client::discover_tools as discover_mcp_tools;
pub use reranking_provider::{BUILTIN_RERANKER_ARTIFACT_ID, BUILTIN_RERANKER_MODEL_ID};
pub use runtime::{ExecutionReport, RuntimeEngine};
use sandbox::RunExecutionContext;
pub use sandbox::{SandboxManagerConfig, SandboxSessionManager};

const DEFAULT_CHAT_SYSTEM_PROMPT: &str = "You are a transport-neutral agent. \
The caller's current input is provided as JSON under `input`; earlier messages \
in the same scope are regular conversation messages. Use only the tools offered \
to you. Read or change long-term memory explicitly with memory tools; do not \
claim to remember facts you did not read or write. Store generated files with \
artifact_write and return their artifact_ref in an `attachments` array. Return \
one JSON object matching any schema requested by the caller. Otherwise return \
at least a non-empty `reply` string. Output JSON only.";

#[derive(Clone, Debug)]
pub struct CapabilityEngineConfig {
    pub http_timeout_secs: Option<u64>,
    pub llm_api_base: Option<String>,
    pub llm_api_key: Option<String>,
    pub llm_model: Option<String>,
    pub default_chat_system_prompt: Option<String>,
}

impl Default for CapabilityEngineConfig {
    fn default() -> Self {
        Self {
            http_timeout_secs: Some(90),
            llm_api_base: Some("http://127.0.0.1:8000/v1".into()),
            llm_api_key: None,
            llm_model: Some("local/chat".into()),
            default_chat_system_prompt: None,
        }
    }
}

#[derive(Clone)]
pub struct CapabilityEngine {
    store: AgentdStore,
    http: reqwest::Client,
    mcp: mcp_client::McpClient,
    llm_api_base: Option<String>,
    llm_api_key: Option<String>,
    llm_model: Option<String>,
    embedding: embedding_provider::BuiltInEmbedding,
    reranker: reranking_provider::BuiltInReranker,
    default_chat_system_prompt: Option<String>,
    sandbox: Option<SandboxSessionManager>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct ToolResult {
    pub(crate) ok: bool,
    pub(crate) result: serde_json::Value,
    pub(crate) error: Option<String>,
}

impl ToolResult {
    fn success(result: serde_json::Value) -> Self {
        Self {
            ok: true,
            result,
            error: None,
        }
    }

    fn failure(error: impl ToString) -> Self {
        Self {
            ok: false,
            result: serde_json::json!({}),
            error: Some(error.to_string()),
        }
    }
}

impl CapabilityEngine {
    pub fn new(store: AgentdStore) -> Self {
        Self::new_with_config(store, CapabilityEngineConfig::default())
    }

    pub fn new_with_config(store: AgentdStore, cfg: CapabilityEngineConfig) -> Self {
        let request_timeout = Duration::from_secs(cfg.http_timeout_secs.unwrap_or(90).max(1));
        let http = reqwest::Client::builder()
            .timeout(request_timeout)
            .build()
            .unwrap_or_else(|_| reqwest::Client::new());
        Self {
            store,
            http,
            mcp: mcp_client::McpClient::new(request_timeout),
            llm_api_base: cfg.llm_api_base,
            llm_api_key: cfg.llm_api_key,
            llm_model: cfg.llm_model,
            embedding: embedding_provider::BuiltInEmbedding::default(),
            reranker: reranking_provider::BuiltInReranker::default(),
            default_chat_system_prompt: cfg.default_chat_system_prompt,
            sandbox: None,
        }
    }

    pub fn with_sandbox_manager(mut self, manager: SandboxSessionManager) -> Self {
        self.sandbox = Some(manager);
        self
    }

    pub fn sandbox_enabled(&self) -> bool {
        self.sandbox.is_some()
    }

    pub fn retain_available_tools(&self, tools: &mut Vec<ToolSpec>) {
        if self.sandbox.is_none() {
            tools.retain(|tool| tool.family != ToolFamily::Sandbox);
        }
    }

    pub async fn cleanup_sandbox_run(&self, run_id: uuid::Uuid) -> Result<()> {
        if let Some(manager) = &self.sandbox {
            let tenant = self
                .store
                .get_run(run_id)
                .await
                .ok()
                .flatten()
                .map(|run| run.tenant);
            self.audit_sandbox_cleanup(
                tenant.as_deref(),
                Some(run_id),
                "sandbox.cleanup",
                "started",
            )
            .await;
            let result = manager.destroy_run(run_id).await;
            self.audit_sandbox_cleanup(
                tenant.as_deref(),
                Some(run_id),
                "sandbox.cleanup",
                if result.is_ok() {
                    "succeeded"
                } else {
                    "failed"
                },
            )
            .await;
            return result;
        }
        Ok(())
    }

    pub async fn cleanup_all_sandboxes(&self) -> Result<()> {
        if let Some(manager) = &self.sandbox {
            self.audit_sandbox_cleanup(None, None, "sandbox.cleanup_all", "started")
                .await;
            let result = manager.destroy_all().await;
            self.audit_sandbox_cleanup(
                None,
                None,
                "sandbox.cleanup_all",
                if result.is_ok() {
                    "succeeded"
                } else {
                    "failed"
                },
            )
            .await;
            return result;
        }
        Ok(())
    }

    pub async fn cleanup_sandbox_tenant(&self, tenant: &str) -> Result<()> {
        let Some(manager) = &self.sandbox else {
            return Ok(());
        };
        self.audit_sandbox_cleanup(Some(tenant), None, "sandbox.cleanup_tenant", "started")
            .await;
        let result = manager.destroy_tenant(tenant).await;
        self.audit_sandbox_cleanup(
            Some(tenant),
            None,
            "sandbox.cleanup_tenant",
            if result.is_ok() {
                "succeeded"
            } else {
                "failed"
            },
        )
        .await;
        result
    }

    pub async fn reap_sandbox_orphans(&self) -> Result<usize> {
        match &self.sandbox {
            Some(manager) => {
                self.store
                    .append_audit(agentd_store::AuditInput::new(
                        None,
                        "sandbox.reap",
                        "sandbox",
                        None,
                        "started",
                        json!({}),
                    ))
                    .await?;
                let result = manager.reap_orphans().await;
                let (outcome, details) = match &result {
                    Ok(count) => ("succeeded", json!({"reaped":count})),
                    Err(_) => ("failed", json!({"reason":"reap_failed"})),
                };
                self.store
                    .append_audit(agentd_store::AuditInput::new(
                        None,
                        "sandbox.reap",
                        "sandbox",
                        None,
                        outcome,
                        details,
                    ))
                    .await?;
                result
            }
            None => Ok(0),
        }
    }

    // Cancellation and shutdown must still release sandbox resources when the
    // database is unavailable. Report audit failures to the service log.
    async fn audit_sandbox_cleanup(
        &self,
        tenant: Option<&str>,
        run_id: Option<uuid::Uuid>,
        action: &str,
        outcome: &str,
    ) {
        let id = run_id.map(|id| id.to_string());
        let mut event = agentd_store::AuditInput::new(
            tenant,
            action,
            "sandbox",
            id.as_deref(),
            outcome,
            json!({}),
        );
        event.run_id = run_id;
        if self.store.append_audit(event).await.is_err() {
            tracing::error!(
                ?run_id,
                action,
                outcome,
                "sandbox cleanup audit could not be persisted"
            );
        }
    }

    pub fn default_chat_system_prompt(&self) -> &str {
        self.default_chat_system_prompt
            .as_deref()
            .unwrap_or(DEFAULT_CHAT_SYSTEM_PROMPT)
    }

    pub(crate) async fn execute_tool(
        &self,
        context: &RunExecutionContext,
        tenant: &str,
        tool: &ToolSpec,
        params: &serde_json::Value,
    ) -> ToolResult {
        if let Err(error) = tool_input::validate_tool_input(&tool.name, &tool.input_schema, params)
        {
            return ToolResult::failure(format!(
                "input does not match {}'s schema: {error}",
                tool.name
            ));
        }

        let result = if tool.family == ToolFamily::Mcp {
            match self.resolve_mcp_invocation_target(tenant, tool).await {
                Ok(target) => self.mcp.call_tool(&target, params).await,
                Err(error) => Err(error),
            }
        } else if tool.family == ToolFamily::Sandbox {
            match &self.sandbox {
                Some(manager) => manager.execute(context, params).await,
                None => Err(anyhow!("sandbox capability is disabled")),
            }
        } else if tool.name == "memory_put" {
            self.execute_memory_put_from_run(tenant, params, Some(context.run_id))
                .await
        } else if tool.name == "memory_delete" {
            self.execute_memory_delete_from_run(tenant, params, Some(context.run_id))
                .await
        } else {
            self.execute_builtin_tool(tenant, &tool.name, params).await
        };
        match result {
            Ok(result) => ToolResult::success(result),
            Err(error) => ToolResult::failure(error),
        }
    }

    pub async fn invalidate_mcp_session(&self, tenant: &str, name: &str) {
        self.mcp.invalidate(tenant, name).await;
    }

    pub async fn cleanup_mcp_tenant(&self, tenant: &str) {
        self.mcp.invalidate_tenant(tenant).await;
    }

    async fn resolve_mcp_invocation_target(
        &self,
        tenant: &str,
        tool: &ToolSpec,
    ) -> Result<McpToolInvocationTarget> {
        self.store
            .get_mcp_tool_invocation_target(tenant, &tool.name)
            .await?
            .ok_or_else(|| anyhow!("MCP tool target not found: {tenant}/{}", tool.name))
    }

    async fn execute_builtin_tool(
        &self,
        tenant: &str,
        handler: &str,
        params: &serde_json::Value,
    ) -> Result<serde_json::Value> {
        match handler {
            "artifact_read" => self.execute_artifact_read(tenant, params).await,
            "artifact_write" => self.execute_artifact_write(tenant, params).await,
            "artifact_list" => self.execute_artifact_list(tenant, params).await,
            "memory_get" => self.execute_memory_get(tenant, params).await,
            "memory_search" => self.execute_memory_search(tenant, params).await,
            "memory_list" => self.execute_memory_list(tenant, params).await,
            "memory_put" => self.execute_memory_put(tenant, params).await,
            "memory_delete" => self.execute_memory_delete(tenant, params).await,
            "graph_query" => self.execute_graph_query(tenant, params).await,
            "schedule_get" => self.execute_schedule_get(tenant, params).await,
            "schedule_list" => self.execute_schedule_list(tenant, params).await,
            "schedule_put" => self.execute_schedule_put(tenant, params).await,
            "schedule_delete" => self.execute_schedule_delete(tenant, params).await,
            "clock_now" => self.execute_clock_now(params).await,
            "web_search" => self.execute_search(params).await,
            "web_fetch" => self.execute_fetch(params).await,
            "calc_eval" => self.execute_calc_eval(params).await,
            other => Err(anyhow!("unsupported builtin tool: {other}")),
        }
    }
}
