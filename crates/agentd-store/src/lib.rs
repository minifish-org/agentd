use agentd_api::{
    builtin_tool_catalog, validate_cron_expression, visible_tools, Agent, AgentResource, AgentRun,
    AgentRunStatus, ArtifactPath, ArtifactRef, DeliveryOutboxRecord, McpServer,
    McpToolInvocationTarget, Schedule, ScheduleSpec, ToolFamily, ToolSpec, ALL_MEMORY_NAMESPACES,
    BEHAVIOR_LEARNER_AGENT, BEHAVIOR_LEARNING_SCHEDULE, MEMORY_MAINTAINER_AGENT,
    MEMORY_MAINTENANCE_SCHEDULE,
};
use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use croner::Cron;
use hex::encode as hex_encode;
use libsql::{Builder, Connection, Database, Value};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet, BinaryHeap};
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

use db::{LibsqlPool, Row};

macro_rules! invalid {
    ($($argument:tt)*) => { validation(format!($($argument)*)) };
}
macro_rules! missing {
    ($($argument:tt)*) => { not_found(format!($($argument)*)) };
}
macro_rules! conflicting {
    ($($argument:tt)*) => { conflict(format!($($argument)*)) };
}

mod error;
pub use error::StoreError;
use error::{conflict, not_found, validation};

mod behavior;
pub use behavior::{BehaviorLearningReadiness, BehaviorLearningResult, BehaviorSnapshot};
pub mod audit;
pub use audit::{with_audit_context, AuditContext, AuditEvent, AuditInput, AuditPage, AuditQuery};
mod audit_mutations;
#[cfg(test)]
mod audit_resource_tests;
#[cfg(test)]
mod audit_run_tests;
#[cfg(test)]
mod audit_scheduler_tests;
mod maintenance;
pub use maintenance::{memory_maintenance_min_entries, MemoryMaintenanceReadiness};
mod presets;
pub use presets::{BuiltinPresetPolicy, BuiltinPresetRequest, BuiltinPresetResult};

pub const MAX_MEMORY_TEXT_BYTES: usize = 4096;
pub const MEMORY_EMBEDDING_DIM: usize = 384;
pub const MAX_GRAPH_ENTITIES_PER_MEMORY: usize = 32;
pub const MAX_GRAPH_EDGES_PER_MEMORY: usize = 64;

const MAX_GRAPH_ID_BYTES: usize = 200;
const MAX_GRAPH_LABEL_BYTES: usize = 500;
const MAX_GRAPH_RELATION_BYTES: usize = 200;
const MAX_GRAPH_PROPERTIES_BYTES: usize = 4096;
const MAX_GRAPH_WALK_ROWS: i64 = 10_000;

#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct TenantRecord {
    pub name: String,
    pub metadata: serde_json::Value,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq)]
pub enum TenantMetadataPatchResult {
    Updated(TenantRecord),
    NotFound,
    Conflict(TenantRecord),
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq)]
pub struct ArtifactListPage {
    pub items: Vec<agentd_api::ArtifactStat>,
    #[serde(default)]
    pub next_cursor: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq)]
pub struct RunLogEntry {
    pub id: i64,
    pub run_id: Uuid,
    pub kind: String,
    pub payload: serde_json::Value,
    pub ts: DateTime<Utc>,
}

pub struct DeliveryAck<'a> {
    pub delivery_id: Uuid,
    pub claim_token: &'a str,
    pub outcome: &'a str,
    pub error: Option<&'a str>,
    pub retry_after: Option<Duration>,
    pub now: DateTime<Utc>,
}

mod db;
mod resources;
use resources::*;
mod contexts;
mod memory;
pub use memory::validate_memory_graph_input;
use memory::*;
mod migrations;
mod schedules;
use schedules::*;
mod runs;
use runs::*;
mod delivery;
use delivery::*;
mod artifacts;

#[derive(Debug, Clone)]
pub struct RunListQuery {
    pub tenant: Option<String>,
    pub agent_ref: Option<String>,
    pub status: Option<AgentRunStatus>,
    pub limit: usize,
}

pub struct NewRun<'a> {
    pub tenant: &'a str,
    pub name: &'a str,
    pub agent_ref: &'a str,
    pub scope: &'a str,
    pub source: &'a str,
    pub input: &'a serde_json::Value,
    pub request_id: Option<&'a str>,
    pub schedule_name: Option<&'a str>,
    pub delivery_destination: Option<&'a str>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct StoredContext {
    pub revision: u64,
    pub updated_at: String,
    pub state: serde_json::Value,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq)]
pub struct MemoryItem {
    pub tenant: String,
    pub namespace: String,
    pub id: String,
    pub text: String,
    pub created_at: String,
    pub updated_at: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub score: Option<f64>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq)]
pub struct MemoryListItem {
    pub id: String,
    pub text: String,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq)]
pub struct MemoryPage {
    pub items: Vec<MemoryListItem>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_after_id: Option<String>,
}

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct MemoryGraphInput {
    #[serde(default)]
    pub entities: Vec<GraphEntityInput>,
    #[serde(default)]
    pub edges: Vec<GraphEdgeInput>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct GraphEntityInput {
    pub id: String,
    pub label: String,
    #[serde(default, rename = "type")]
    pub kind: Option<String>,
    #[serde(default = "empty_json_object")]
    pub properties: serde_json::Value,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct GraphEdgeInput {
    pub from: String,
    pub relation: String,
    pub to: String,
    #[serde(default = "empty_json_object")]
    pub properties: serde_json::Value,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq)]
pub struct GraphEntity {
    pub id: String,
    pub label: String,
    #[serde(skip_serializing_if = "Option::is_none", rename = "type")]
    pub kind: Option<String>,
    pub properties: serde_json::Value,
    pub memory_ids: Vec<String>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq)]
pub struct GraphPathEdge {
    pub from: String,
    pub relation: String,
    pub to: String,
    pub memory_id: String,
    pub properties: serde_json::Value,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq)]
pub struct GraphPath {
    pub hops: usize,
    pub nodes: Vec<String>,
    pub edges: Vec<GraphPathEdge>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq)]
pub struct GraphQueryResult {
    pub entities: Vec<GraphEntity>,
    pub paths: Vec<GraphPath>,
}

pub struct GraphQuery<'a> {
    pub entity: &'a str,
    pub relation: Option<&'a str>,
    pub direction: &'a str,
    pub max_hops: usize,
    pub limit: usize,
}

fn empty_json_object() -> serde_json::Value {
    json!({})
}

#[derive(Debug)]
struct SemanticMemoryHit {
    similarity: f64,
    item: MemoryItem,
}

impl PartialEq for SemanticMemoryHit {
    fn eq(&self, other: &Self) -> bool {
        self.similarity.total_cmp(&other.similarity).is_eq() && self.item.id == other.item.id
    }
}

impl Eq for SemanticMemoryHit {}

impl PartialOrd for SemanticMemoryHit {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for SemanticMemoryHit {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.similarity
            .total_cmp(&other.similarity)
            // For equal similarity, the lexically smaller id ranks first.
            .then_with(|| other.item.id.cmp(&self.item.id))
    }
}

impl Default for RunListQuery {
    fn default() -> Self {
        Self {
            tenant: None,
            agent_ref: None,
            status: None,
            limit: 50,
        }
    }
}

#[derive(Debug, Clone)]
pub struct AssignedRun {
    pub run: AgentRun,
    pub timeout_ms: u64,
    pub max_steps: u32,
    pub agent_system_prompt: Option<String>,
    pub agent_model: Option<String>,
    pub agent_temperature: Option<f32>,
    pub agent_max_tokens: Option<u32>,
    pub agent_context_turns: Option<usize>,
    pub agent_learned_instructions: Option<String>,
    pub agent_behavior_revision: Option<u64>,
    pub visible_tools: Vec<ToolSpec>,
}

#[derive(Clone)]
pub struct AgentdStore {
    pool: LibsqlPool,
    mcp_apply_lock: Arc<tokio::sync::Mutex<()>>,
    #[cfg(test)]
    memory_search_hook: Arc<tokio::sync::Mutex<Option<memory::MemorySearchHook>>>,
    #[cfg(test)]
    tenant_write_hook: Arc<tokio::sync::Mutex<Option<TenantWriteHook>>>,
}

#[cfg(test)]
#[derive(Clone, Default)]
struct TenantWriteHook {
    before_transaction: Arc<tokio::sync::Notify>,
    resume_writer: Arc<tokio::sync::Notify>,
}

impl AgentdStore {
    pub async fn new(database_path: &str) -> Result<Self> {
        let pool = connect_libsql_database(database_path).await?;
        let store = Self {
            pool,
            mcp_apply_lock: Arc::new(tokio::sync::Mutex::new(())),
            #[cfg(test)]
            memory_search_hook: Arc::new(tokio::sync::Mutex::new(None)),
            #[cfg(test)]
            tenant_write_hook: Arc::new(tokio::sync::Mutex::new(None)),
        };
        with_audit_context(AuditContext::system("schema"), store.initialize_schema()).await?;
        Ok(store)
    }

    pub fn list_tools(&self) -> Vec<ToolSpec> {
        builtin_tool_catalog()
    }
}

fn decode_json<T: serde::de::DeserializeOwned>(raw: &str) -> Result<T> {
    serde_json::from_str(raw).map_err(|error| StoreError::database(error).into())
}

fn parse_ts_field(row: &db::SqlRow, name: &str) -> Result<DateTime<Utc>> {
    Ok(
        DateTime::parse_from_rfc3339(&row.try_get::<String, _>(name)?)
            .map_err(StoreError::database)?
            .with_timezone(&Utc),
    )
}

fn optional_ts_field(row: &db::SqlRow, name: &str) -> Result<Option<DateTime<Utc>>> {
    row.try_get::<Option<String>, _>(name)?
        .map(|raw| {
            Ok(DateTime::parse_from_rfc3339(&raw)
                .map_err(StoreError::database)?
                .with_timezone(&Utc))
        })
        .transpose()
}

fn parse_uuid_field(row: &db::SqlRow, name: &str) -> Result<Uuid> {
    Ok(Uuid::parse_str(&row.try_get::<String, _>(name)?).map_err(StoreError::database)?)
}

fn optional_uuid_field(row: &db::SqlRow, name: &str) -> Result<Option<Uuid>> {
    row.try_get::<Option<String>, _>(name)?
        .map(|raw| Ok(Uuid::parse_str(&raw).map_err(StoreError::database)?))
        .transpose()
}

async fn connect_libsql_database(database_path: &str) -> Result<LibsqlPool> {
    LibsqlPool::open(database_path)
        .await
        .with_context(|| format!("failed to open libSQL database at {database_path}"))
}

#[cfg(test)]
mod tests;
