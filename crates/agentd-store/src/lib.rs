use agentd_api::{
    builtin_tool_catalog, parse_timezone_offset, validate_cron_expression, validate_timezone_name,
    visible_tools, Agent, AgentResource, AgentRun, AgentRunStatus, DeliveryOutboxRecord, McpServer,
    McpToolInvocationTarget, Schedule, ScheduleSpec, ToolFamily, ToolSpec,
};
use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Duration as ChronoDuration, FixedOffset, Utc};
use chrono_tz::Tz;
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

mod db {
    use super::*;

    #[derive(Debug)]
    pub enum Error {
        Database(DatabaseError),
        ColumnNotFound(String),
        Decode(String),
    }

    impl std::fmt::Display for Error {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self {
                Error::Database(error) => write!(f, "database error: {}", error.message),
                Error::ColumnNotFound(name) => write!(f, "column not found: {name}"),
                Error::Decode(message) => write!(f, "decode error: {message}"),
            }
        }
    }

    impl std::error::Error for Error {}

    impl From<libsql::Error> for Error {
        fn from(error: libsql::Error) -> Self {
            Error::Database(DatabaseError {
                message: error.to_string(),
            })
        }
    }

    #[derive(Debug, Clone)]
    pub struct DatabaseError {
        message: String,
    }

    #[derive(Clone)]
    pub struct LibsqlPool {
        db: Arc<Database>,
        conn: Arc<tokio::sync::Mutex<Connection>>,
    }

    impl LibsqlPool {
        pub async fn open(path: &str) -> std::result::Result<Self, Error> {
            if path != ":memory:" {
                if let Some(parent) = Path::new(path)
                    .parent()
                    .filter(|p| !p.as_os_str().is_empty())
                {
                    tokio::fs::create_dir_all(parent)
                        .await
                        .map_err(|err| Error::Decode(err.to_string()))?;
                }
            }
            let db = Builder::new_local(path).build().await?;
            let conn = db.connect()?;
            conn.busy_timeout(Duration::from_secs(5))?;
            conn.execute_batch(
                r#"
                PRAGMA journal_mode=WAL;
                PRAGMA busy_timeout=5000;
                PRAGMA foreign_keys=ON;
                "#,
            )
            .await?;
            let pool = Self {
                db: Arc::new(db),
                conn: Arc::new(tokio::sync::Mutex::new(conn)),
            };
            Ok(pool)
        }

        pub fn conn(&self) -> std::result::Result<Connection, Error> {
            let conn = self.db.connect()?;
            conn.busy_timeout(Duration::from_secs(5))?;
            Ok(conn)
        }

        pub async fn begin(&self) -> std::result::Result<Transaction, Error> {
            Ok(Transaction {
                inner: self.conn()?.transaction().await?,
            })
        }
    }

    pub struct Transaction {
        inner: libsql::Transaction,
    }

    impl Transaction {
        pub async fn commit(self) -> std::result::Result<(), Error> {
            Ok(self.inner.commit().await?)
        }

        pub async fn rollback(self) -> std::result::Result<(), Error> {
            Ok(self.inner.rollback().await?)
        }
    }

    pub fn query(sql: &str) -> Query {
        Query {
            sql: sql.to_string(),
            params: Vec::new(),
        }
    }

    pub fn query_scalar<T>(sql: &str) -> QueryScalar<T> {
        QueryScalar {
            query: query(sql),
            _marker: std::marker::PhantomData,
        }
    }

    pub struct Query {
        sql: String,
        params: Vec<Value>,
    }

    impl Query {
        pub fn bind<T: IntoSqlValue>(mut self, value: T) -> Self {
            self.params.push(value.into_sql_value());
            self
        }

        pub async fn execute<E: Executor>(
            self,
            executor: E,
        ) -> std::result::Result<ExecuteResult, Error> {
            executor.execute(&self.sql, self.params).await
        }

        pub async fn fetch_all<E: Executor>(
            self,
            executor: E,
        ) -> std::result::Result<Vec<SqlRow>, Error> {
            executor.fetch_all(&self.sql, self.params).await
        }

        pub async fn fetch_optional<E: Executor>(
            self,
            executor: E,
        ) -> std::result::Result<Option<SqlRow>, Error> {
            let rows = self.fetch_all(executor).await?;
            Ok(rows.into_iter().next())
        }
    }

    pub struct ExecuteResult {
        rows_affected: u64,
    }

    impl ExecuteResult {
        pub fn rows_affected(&self) -> u64 {
            self.rows_affected
        }
    }

    pub struct QueryScalar<T> {
        query: Query,
        _marker: std::marker::PhantomData<T>,
    }

    impl<T> QueryScalar<T> {
        pub fn bind<U: IntoSqlValue>(mut self, value: U) -> Self {
            self.query = self.query.bind(value);
            self
        }
    }

    impl<T: FromSqlValue> QueryScalar<T> {
        pub async fn fetch_all<E: Executor>(
            self,
            executor: E,
        ) -> std::result::Result<Vec<T>, Error> {
            let rows = self.query.fetch_all(executor).await?;
            rows.into_iter().map(|row| row.try_get(0)).collect()
        }

        pub async fn fetch_optional<E: Executor>(
            self,
            executor: E,
        ) -> std::result::Result<Option<T>, Error> {
            let row = self.query.fetch_optional(executor).await?;
            row.map(|row| row.try_get(0)).transpose()
        }
    }

    pub trait Executor {
        fn conn(&self) -> std::result::Result<CowConnection<'_>, Error>;

        async fn execute(
            &self,
            sql: &str,
            params: Vec<Value>,
        ) -> std::result::Result<ExecuteResult, Error> {
            let rows_affected = match self.conn()? {
                CowConnection::Connection(conn) => {
                    conn.execute(sql, libsql::params_from_iter(params)).await?
                }
                CowConnection::Transaction(tx) => {
                    tx.inner
                        .execute(sql, libsql::params_from_iter(params))
                        .await?
                }
            };
            Ok(ExecuteResult { rows_affected })
        }

        async fn fetch_all(
            &self,
            sql: &str,
            params: Vec<Value>,
        ) -> std::result::Result<Vec<SqlRow>, Error> {
            let mut rows = match self.conn()? {
                CowConnection::Connection(conn) => {
                    conn.query(sql, libsql::params_from_iter(params)).await?
                }
                CowConnection::Transaction(tx) => {
                    tx.inner
                        .query(sql, libsql::params_from_iter(params))
                        .await?
                }
            };
            let mut out = Vec::new();
            while let Some(row) = rows.next().await? {
                out.push(SqlRow::from_libsql_row(row)?);
            }
            Ok(out)
        }
    }

    pub enum CowConnection<'a> {
        Connection(Connection),
        Transaction(&'a Transaction),
    }

    impl Executor for &LibsqlPool {
        fn conn(&self) -> std::result::Result<CowConnection<'_>, Error> {
            Ok(CowConnection::Connection(LibsqlPool::conn(self)?))
        }

        async fn execute(
            &self,
            sql: &str,
            params: Vec<Value>,
        ) -> std::result::Result<ExecuteResult, Error> {
            let conn = self.conn.lock().await;
            let rows_affected = conn.execute(sql, libsql::params_from_iter(params)).await?;
            Ok(ExecuteResult { rows_affected })
        }

        async fn fetch_all(
            &self,
            sql: &str,
            params: Vec<Value>,
        ) -> std::result::Result<Vec<SqlRow>, Error> {
            let conn = self.conn.lock().await;
            let mut rows = conn.query(sql, libsql::params_from_iter(params)).await?;
            let mut out = Vec::new();
            while let Some(row) = rows.next().await? {
                out.push(SqlRow::from_libsql_row(row)?);
            }
            Ok(out)
        }
    }

    impl Executor for &mut Transaction {
        fn conn(&self) -> std::result::Result<CowConnection<'_>, Error> {
            Ok(CowConnection::Transaction(self))
        }
    }

    pub struct SqlRow {
        values: Vec<Value>,
        names: Vec<String>,
    }

    impl SqlRow {
        fn from_libsql_row(row: libsql::Row) -> std::result::Result<Self, Error> {
            let mut values = Vec::new();
            let mut names = Vec::new();
            for idx in 0..row.column_count() {
                values.push(row.get_value(idx)?);
                names.push(row.column_name(idx).unwrap_or("").to_string());
            }
            Ok(Self { values, names })
        }
    }

    pub trait Row {
        fn try_get<T, I>(&self, index: I) -> std::result::Result<T, Error>
        where
            T: FromSqlValue,
            I: RowIndex;
    }

    impl Row for SqlRow {
        fn try_get<T, I>(&self, index: I) -> std::result::Result<T, Error>
        where
            T: FromSqlValue,
            I: RowIndex,
        {
            let idx = index.index(self)?;
            let value = self
                .values
                .get(idx as usize)
                .cloned()
                .ok_or_else(|| Error::ColumnNotFound(idx.to_string()))?;
            T::from_sql_value(value)
        }
    }

    pub trait RowIndex {
        fn index(self, row: &SqlRow) -> std::result::Result<i32, Error>;
    }

    impl RowIndex for i32 {
        fn index(self, _row: &SqlRow) -> std::result::Result<i32, Error> {
            Ok(self)
        }
    }

    impl RowIndex for usize {
        fn index(self, _row: &SqlRow) -> std::result::Result<i32, Error> {
            Ok(self as i32)
        }
    }

    impl RowIndex for &str {
        fn index(self, row: &SqlRow) -> std::result::Result<i32, Error> {
            for (idx, name) in row.names.iter().enumerate() {
                if name == self {
                    return Ok(idx as i32);
                }
            }
            Err(Error::ColumnNotFound(self.to_string()))
        }
    }

    pub trait IntoSqlValue {
        fn into_sql_value(self) -> Value;
    }

    impl IntoSqlValue for Value {
        fn into_sql_value(self) -> Value {
            self
        }
    }

    impl IntoSqlValue for &str {
        fn into_sql_value(self) -> Value {
            Value::Text(self.to_string())
        }
    }

    impl IntoSqlValue for String {
        fn into_sql_value(self) -> Value {
            Value::Text(self)
        }
    }

    impl IntoSqlValue for &String {
        fn into_sql_value(self) -> Value {
            Value::Text(self.clone())
        }
    }

    impl IntoSqlValue for Option<&str> {
        fn into_sql_value(self) -> Value {
            self.map_or(Value::Null, |value| Value::Text(value.to_string()))
        }
    }

    impl IntoSqlValue for Option<String> {
        fn into_sql_value(self) -> Value {
            self.map_or(Value::Null, Value::Text)
        }
    }

    impl IntoSqlValue for &Option<String> {
        fn into_sql_value(self) -> Value {
            self.clone().into_sql_value()
        }
    }

    impl IntoSqlValue for i64 {
        fn into_sql_value(self) -> Value {
            Value::Integer(self)
        }
    }

    impl IntoSqlValue for i32 {
        fn into_sql_value(self) -> Value {
            Value::Integer(self as i64)
        }
    }

    impl IntoSqlValue for u64 {
        fn into_sql_value(self) -> Value {
            Value::Integer(self as i64)
        }
    }

    impl IntoSqlValue for bool {
        fn into_sql_value(self) -> Value {
            Value::Integer(if self { 1 } else { 0 })
        }
    }

    impl IntoSqlValue for f64 {
        fn into_sql_value(self) -> Value {
            Value::Real(self)
        }
    }

    impl IntoSqlValue for Option<f64> {
        fn into_sql_value(self) -> Value {
            self.map_or(Value::Null, Value::Real)
        }
    }

    impl IntoSqlValue for f32 {
        fn into_sql_value(self) -> Value {
            Value::Real(self as f64)
        }
    }

    impl IntoSqlValue for &[u8] {
        fn into_sql_value(self) -> Value {
            Value::Blob(self.to_vec())
        }
    }

    impl IntoSqlValue for Vec<u8> {
        fn into_sql_value(self) -> Value {
            Value::Blob(self)
        }
    }

    impl IntoSqlValue for &Vec<u8> {
        fn into_sql_value(self) -> Value {
            Value::Blob(self.clone())
        }
    }

    pub trait FromSqlValue: Sized {
        fn from_sql_value(value: Value) -> std::result::Result<Self, Error>;
    }

    impl FromSqlValue for String {
        fn from_sql_value(value: Value) -> std::result::Result<Self, Error> {
            match value {
                Value::Text(value) => Ok(value),
                Value::Integer(value) => Ok(value.to_string()),
                Value::Real(value) => Ok(value.to_string()),
                Value::Blob(value) => {
                    String::from_utf8(value).map_err(|error| Error::Decode(error.to_string()))
                }
                Value::Null => Err(Error::Decode("expected TEXT, found NULL".into())),
            }
        }
    }

    impl FromSqlValue for Option<String> {
        fn from_sql_value(value: Value) -> std::result::Result<Self, Error> {
            match value {
                Value::Null => Ok(None),
                other => Ok(Some(String::from_sql_value(other)?)),
            }
        }
    }

    impl FromSqlValue for i64 {
        fn from_sql_value(value: Value) -> std::result::Result<Self, Error> {
            match value {
                Value::Integer(value) => Ok(value),
                other => Err(Error::Decode(format!("expected INTEGER, found {other:?}"))),
            }
        }
    }

    impl FromSqlValue for Option<i64> {
        fn from_sql_value(value: Value) -> std::result::Result<Self, Error> {
            match value {
                Value::Null => Ok(None),
                Value::Integer(value) => Ok(Some(value)),
                other => Err(Error::Decode(format!(
                    "expected optional integer, got {other:?}"
                ))),
            }
        }
    }

    impl FromSqlValue for i32 {
        fn from_sql_value(value: Value) -> std::result::Result<Self, Error> {
            Ok(i64::from_sql_value(value)? as i32)
        }
    }

    impl FromSqlValue for u64 {
        fn from_sql_value(value: Value) -> std::result::Result<Self, Error> {
            Ok(i64::from_sql_value(value)? as u64)
        }
    }

    impl FromSqlValue for f64 {
        fn from_sql_value(value: Value) -> std::result::Result<Self, Error> {
            match value {
                Value::Real(value) => Ok(value),
                Value::Integer(value) => Ok(value as f64),
                other => Err(Error::Decode(format!("expected REAL, found {other:?}"))),
            }
        }
    }

    impl FromSqlValue for Option<f64> {
        fn from_sql_value(value: Value) -> std::result::Result<Self, Error> {
            match value {
                Value::Null => Ok(None),
                other => Ok(Some(f64::from_sql_value(other)?)),
            }
        }
    }

    impl FromSqlValue for bool {
        fn from_sql_value(value: Value) -> std::result::Result<Self, Error> {
            Ok(i64::from_sql_value(value)? != 0)
        }
    }

    impl FromSqlValue for Vec<u8> {
        fn from_sql_value(value: Value) -> std::result::Result<Self, Error> {
            match value {
                Value::Blob(value) => Ok(value),
                other => Err(Error::Decode(format!("expected BLOB, found {other:?}"))),
            }
        }
    }

    impl FromSqlValue for Option<Vec<u8>> {
        fn from_sql_value(value: Value) -> std::result::Result<Self, Error> {
            match value {
                Value::Null => Ok(None),
                other => Ok(Some(Vec::<u8>::from_sql_value(other)?)),
            }
        }
    }
}

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
    pub visible_tools: Vec<ToolSpec>,
}

#[derive(Clone)]
pub struct AgentdStore {
    pool: LibsqlPool,
    mcp_apply_lock: Arc<tokio::sync::Mutex<()>>,
}

impl AgentdStore {
    pub async fn new(database_path: &str) -> Result<Self> {
        let pool = connect_libsql_database(database_path).await?;
        let store = Self {
            pool,
            mcp_apply_lock: Arc::new(tokio::sync::Mutex::new(())),
        };
        store.initialize_schema().await?;
        Ok(store)
    }

    pub fn list_tools(&self) -> Vec<ToolSpec> {
        builtin_tool_catalog()
    }

    pub async fn create_tenant(
        &self,
        name: &str,
        metadata: &serde_json::Value,
    ) -> Result<(TenantRecord, bool)> {
        let name = normalize_tenant_name(name)?;
        let now = Utc::now().to_rfc3339();
        let metadata_json = serde_json::to_string(metadata)?;
        let result = db::query(
            "INSERT OR IGNORE INTO tenants (name, metadata_json, created_at, updated_at) \
             VALUES (?, ?, ?, ?)",
        )
        .bind(&name)
        .bind(&metadata_json)
        .bind(&now)
        .bind(&now)
        .execute(&self.pool)
        .await?;
        let created = result.rows_affected() > 0;
        let record = self
            .get_tenant(&name)
            .await?
            .ok_or_else(|| anyhow!("tenant was not readable after create: {name}"))?;
        Ok((record, created))
    }

    pub async fn get_tenant(&self, name: &str) -> Result<Option<TenantRecord>> {
        let name = normalize_tenant_name(name)?;
        db::query("SELECT name, metadata_json, created_at, updated_at FROM tenants WHERE name = ?")
            .bind(&name)
            .fetch_optional(&self.pool)
            .await?
            .map(row_to_tenant_record)
            .transpose()
    }

    async fn ensure_tenant_exists(&self, tenant: &str) -> Result<()> {
        if self.get_tenant(tenant).await?.is_none() {
            return Err(anyhow!("tenant not found: {tenant}"));
        }
        Ok(())
    }

    pub async fn patch_tenant_metadata(
        &self,
        name: &str,
        metadata: &serde_json::Value,
        if_updated_at: Option<&str>,
    ) -> Result<TenantMetadataPatchResult> {
        let name = normalize_tenant_name(name)?;
        let current = self.get_tenant(&name).await?;
        let Some(current) = current else {
            return Ok(TenantMetadataPatchResult::NotFound);
        };
        if let Some(expected) = if_updated_at {
            if current.updated_at != expected {
                return Ok(TenantMetadataPatchResult::Conflict(current));
            }
        }

        let now = Utc::now().to_rfc3339();
        let metadata_json = serde_json::to_string(metadata)?;
        let changed =
            db::query("UPDATE tenants SET metadata_json = ?, updated_at = ? WHERE name = ?")
                .bind(&metadata_json)
                .bind(&now)
                .bind(&name)
                .execute(&self.pool)
                .await?
                .rows_affected();

        if changed == 0 {
            return Ok(TenantMetadataPatchResult::NotFound);
        }

        let updated = self
            .get_tenant(&name)
            .await?
            .ok_or_else(|| anyhow!("tenant was not readable after metadata patch: {name}"))?;
        Ok(TenantMetadataPatchResult::Updated(updated))
    }

    pub async fn list_tenants(&self) -> Result<Vec<String>> {
        let tenants = db::query_scalar::<String>("SELECT name FROM tenants ORDER BY name")
            .fetch_all(&self.pool)
            .await?;
        Ok(tenants)
    }

    /// List the existing context scopes for a given (tenant, agent), most
    /// recently updated first. Scopes are not "created" out of band — a row
    /// exists once a turn (or `context create`) has written context for it.
    pub async fn list_context_scopes(&self, tenant: &str, agent: &str) -> Result<Vec<String>> {
        let scopes = db::query_scalar::<String>(
            "SELECT scope FROM contexts WHERE tenant = ? AND agent = ? ORDER BY updated_at DESC",
        )
        .bind(tenant)
        .bind(agent)
        .fetch_all(&self.pool)
        .await?;
        Ok(scopes)
    }

    pub async fn get_context_state(
        &self,
        tenant: &str,
        agent: &str,
        scope: &str,
    ) -> Result<Option<StoredContext>> {
        let row = db::query(
            "SELECT revision, updated_at, state_json FROM contexts \
             WHERE tenant = ? AND agent = ? AND scope = ? LIMIT 1",
        )
        .bind(tenant)
        .bind(agent)
        .bind(scope)
        .fetch_optional(&self.pool)
        .await?;
        row.map(|row| {
            let revision = row.try_get::<i64, _>("revision")? as u64;
            let updated_at = row.try_get::<String, _>("updated_at")?;
            let state = serde_json::from_str(&row.try_get::<String, _>("state_json")?)
                .unwrap_or_else(|_| serde_json::json!({}));
            Ok(StoredContext {
                revision,
                updated_at,
                state,
            })
        })
        .transpose()
    }

    pub async fn delete_context_state(
        &self,
        tenant: &str,
        agent: &str,
        scope: &str,
    ) -> Result<bool> {
        Ok(
            db::query("DELETE FROM contexts WHERE tenant = ? AND agent = ? AND scope = ?")
                .bind(tenant)
                .bind(agent)
                .bind(scope)
                .execute(&self.pool)
                .await?
                .rows_affected()
                > 0,
        )
    }

    pub async fn get_memory(
        &self,
        tenant: &str,
        namespace: &str,
        id: &str,
    ) -> Result<Option<MemoryItem>> {
        let row = db::query(
            "SELECT tenant, namespace, id, text, created_at, updated_at \
             FROM memory WHERE tenant = ? AND namespace = ? AND id = ?",
        )
        .bind(tenant)
        .bind(normalize_memory_component(namespace, "namespace")?)
        .bind(normalize_memory_component(id, "id")?)
        .fetch_optional(&self.pool)
        .await?;
        row.map(|row| memory_item_from_row(row, None)).transpose()
    }

    pub async fn list_memory_page(
        &self,
        tenant: &str,
        namespace: &str,
        after_id: Option<&str>,
        limit: usize,
    ) -> Result<MemoryPage> {
        let namespace = normalize_memory_component(namespace, "namespace")?;
        let after_id = after_id
            .map(|id| normalize_memory_component(id, "cursor id"))
            .transpose()?;
        let limit = limit.clamp(1, 100);
        let rows = if let Some(after_id) = after_id.as_deref() {
            db::query(
                "SELECT id, text, created_at, updated_at FROM memory \
                 WHERE tenant = ? AND namespace = ? AND id > ? \
                 ORDER BY id ASC LIMIT ?",
            )
            .bind(tenant)
            .bind(&namespace)
            .bind(after_id)
            .bind((limit + 1) as i64)
            .fetch_all(&self.pool)
            .await?
        } else {
            db::query(
                "SELECT id, text, created_at, updated_at FROM memory \
                 WHERE tenant = ? AND namespace = ? \
                 ORDER BY id ASC LIMIT ?",
            )
            .bind(tenant)
            .bind(&namespace)
            .bind((limit + 1) as i64)
            .fetch_all(&self.pool)
            .await?
        };
        let has_more = rows.len() > limit;
        let items = rows
            .into_iter()
            .take(limit)
            .map(|row| {
                Ok(MemoryListItem {
                    id: row.try_get("id")?,
                    text: row.try_get("text")?,
                    created_at: row.try_get("created_at")?,
                    updated_at: row.try_get("updated_at")?,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let next_after_id = has_more
            .then(|| items.last().map(|item| item.id.clone()))
            .flatten();
        Ok(MemoryPage {
            items,
            next_after_id,
        })
    }

    pub async fn search_memory(
        &self,
        tenant: &str,
        namespace: &str,
        query: &str,
        query_embedding: &[f32],
        limit: usize,
    ) -> Result<Vec<MemoryItem>> {
        let namespace = normalize_memory_component(namespace, "namespace")?;
        let query = query.trim();
        if query.is_empty() {
            return Err(anyhow!("memory query is required"));
        }
        validate_memory_embedding(query_embedding)?;
        let limit = limit.clamp(1, 20);
        let candidate_limit = limit.saturating_mul(4);

        let lexical = if let Some(fts_query) = memory_fts_query(query) {
            let rows = db::query(
                "SELECT m.tenant, m.namespace, m.id, m.text, m.created_at, m.updated_at, \
                        bm25(memory_fts) AS lexical_rank \
                 FROM memory_fts JOIN memory m ON m.rowid = memory_fts.rowid \
                 WHERE memory_fts MATCH ? AND m.tenant = ? AND m.namespace = ? \
                 ORDER BY lexical_rank, m.id LIMIT ?",
            )
            .bind(fts_query)
            .bind(tenant)
            .bind(&namespace)
            .bind(candidate_limit as i64)
            .fetch_all(&self.pool)
            .await?;
            rows.into_iter()
                .map(|row| memory_item_from_row(row, None))
                .collect::<Result<Vec<_>>>()?
        } else {
            Vec::new()
        };

        let rows = db::query(
            "SELECT tenant, namespace, id, text, embedding, created_at, updated_at \
             FROM memory WHERE tenant = ? AND namespace = ? ORDER BY id",
        )
        .bind(tenant)
        .bind(&namespace)
        .fetch_all(&self.pool)
        .await?;
        let mut semantic = BinaryHeap::with_capacity(candidate_limit.saturating_add(1));
        for row in rows {
            let stored_embedding =
                decode_memory_embedding(&row.try_get::<Vec<u8>, _>("embedding")?)?;
            let hit = SemanticMemoryHit {
                similarity: cosine_similarity(query_embedding, &stored_embedding),
                item: memory_item_from_row(row, None)?,
            };
            semantic.push(Reverse(hit));
            if semantic.len() > candidate_limit {
                semantic.pop();
            }
        }
        let mut semantic = semantic
            .into_iter()
            .map(|Reverse(hit)| hit)
            .collect::<Vec<_>>();
        semantic.sort_by(|left, right| {
            right
                .similarity
                .total_cmp(&left.similarity)
                .then_with(|| left.item.id.cmp(&right.item.id))
        });

        Ok(fuse_memory_candidates(
            lexical,
            semantic.into_iter().map(|hit| hit.item).collect(),
            limit,
        ))
    }

    pub async fn put_memory(
        &self,
        tenant: &str,
        namespace: &str,
        id: &str,
        text: &str,
        embedding: &[f32],
    ) -> Result<MemoryItem> {
        self.put_memory_with_graph(
            tenant,
            namespace,
            id,
            text,
            embedding,
            &MemoryGraphInput::default(),
        )
        .await
    }

    pub async fn put_memory_with_graph(
        &self,
        tenant: &str,
        namespace: &str,
        id: &str,
        text: &str,
        embedding: &[f32],
        graph: &MemoryGraphInput,
    ) -> Result<MemoryItem> {
        self.ensure_tenant_exists(tenant).await?;
        let namespace = normalize_memory_component(namespace, "namespace")?;
        let id = normalize_memory_component(id, "id")?;
        let text = text.trim();
        if text.is_empty() {
            return Err(anyhow!("memory text is required"));
        }
        if text.len() > MAX_MEMORY_TEXT_BYTES {
            return Err(anyhow!(
                "memory text exceeds {MAX_MEMORY_TEXT_BYTES} UTF-8 bytes; store long content as an artifact"
            ));
        }
        validate_memory_embedding(embedding)?;
        let graph = normalize_memory_graph(graph)?;
        let embedding = encode_memory_embedding(embedding);
        let now = Utc::now().to_rfc3339();
        let mut tx = self.pool.begin().await?;
        db::query(
            "INSERT INTO memory (tenant, namespace, id, text, embedding, created_at, updated_at) \
             VALUES (?, ?, ?, ?, ?, ?, ?) \
             ON CONFLICT(tenant, namespace, id) DO UPDATE SET \
             text = excluded.text, embedding = excluded.embedding, updated_at = excluded.updated_at",
        )
        .bind(tenant)
        .bind(&namespace)
        .bind(&id)
        .bind(text)
        .bind(embedding)
        .bind(&now)
        .bind(&now)
        .execute(&mut tx)
        .await?;

        db::query("DELETE FROM edges WHERE tenant = ? AND namespace = ? AND memory_id = ?")
            .bind(tenant)
            .bind(&namespace)
            .bind(&id)
            .execute(&mut tx)
            .await?;
        db::query("DELETE FROM entities WHERE tenant = ? AND namespace = ? AND memory_id = ?")
            .bind(tenant)
            .bind(&namespace)
            .bind(&id)
            .execute(&mut tx)
            .await?;

        for entity in &graph.entities {
            db::query(
                "INSERT INTO entities (tenant, namespace, memory_id, entity_id, label, entity_type, properties_json, created_at, updated_at) \
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            )
            .bind(tenant)
            .bind(&namespace)
            .bind(&id)
            .bind(&entity.id)
            .bind(&entity.label)
            .bind(&entity.kind)
            .bind(serde_json::to_string(&entity.properties)?)
            .bind(&now)
            .bind(&now)
            .execute(&mut tx)
            .await?;
        }
        for edge in &graph.edges {
            db::query(
                "INSERT INTO edges (tenant, namespace, memory_id, source_entity_id, relation, target_entity_id, properties_json, created_at, updated_at) \
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            )
            .bind(tenant)
            .bind(&namespace)
            .bind(&id)
            .bind(&edge.from)
            .bind(&edge.relation)
            .bind(&edge.to)
            .bind(serde_json::to_string(&edge.properties)?)
            .bind(&now)
            .bind(&now)
            .execute(&mut tx)
            .await?;
        }
        tx.commit().await?;
        self.get_memory(tenant, &namespace, &id)
            .await?
            .ok_or_else(|| anyhow!("memory was not readable after put"))
    }

    pub async fn delete_memory(&self, tenant: &str, namespace: &str, id: &str) -> Result<bool> {
        let deleted = db::query("DELETE FROM memory WHERE tenant = ? AND namespace = ? AND id = ?")
            .bind(tenant)
            .bind(normalize_memory_component(namespace, "namespace")?)
            .bind(normalize_memory_component(id, "id")?)
            .execute(&self.pool)
            .await?
            .rows_affected();
        Ok(deleted > 0)
    }

    pub async fn query_graph(
        &self,
        tenant: &str,
        namespace: &str,
        query: GraphQuery<'_>,
    ) -> Result<GraphQueryResult> {
        let GraphQuery {
            entity,
            relation,
            direction,
            max_hops,
            limit,
        } = query;
        self.ensure_tenant_exists(tenant).await?;
        let namespace = normalize_memory_component(namespace, "namespace")?;
        let entity = normalize_graph_component(entity, "entity", MAX_GRAPH_LABEL_BYTES)?;
        let relation = relation
            .map(|value| normalize_graph_component(value, "relation", MAX_GRAPH_RELATION_BYTES))
            .transpose()?;
        if !matches!(direction, "outgoing" | "incoming" | "both") {
            return Err(anyhow!(
                "graph direction must be outgoing, incoming, or both"
            ));
        }
        let max_hops = max_hops.clamp(1, 3) as i64;
        let limit = limit.clamp(1, 100) as i64;

        let start_ids: BTreeSet<String> = db::query_scalar(
            "SELECT DISTINCT entity_id FROM entities \
             WHERE tenant = ? AND namespace = ? \
               AND (entity_id = ? COLLATE NOCASE OR label = ? COLLATE NOCASE) \
             ORDER BY entity_id ASC LIMIT 16",
        )
        .bind(tenant)
        .bind(&namespace)
        .bind(&entity)
        .bind(&entity)
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .collect();
        if start_ids.is_empty() {
            return Ok(GraphQueryResult {
                entities: Vec::new(),
                paths: Vec::new(),
            });
        }

        let rows = db::query(
            r#"WITH RECURSIVE
               start_nodes(entity_id) AS (
                   SELECT value FROM json_each(?)
               ),
               adjacency(from_id, to_id, edge_from, edge_to, relation, memory_id, properties_json) AS (
                   SELECT source_entity_id, target_entity_id, source_entity_id, target_entity_id,
                          relation, memory_id, properties_json
                   FROM edges
                   WHERE tenant = ? AND namespace = ? AND ? IN ('outgoing', 'both')
                     AND (? IS NULL OR relation = ?)
                   UNION ALL
                   SELECT target_entity_id, source_entity_id, source_entity_id, target_entity_id,
                          relation, memory_id, properties_json
                   FROM edges
                   WHERE tenant = ? AND namespace = ? AND ? IN ('incoming', 'both')
                     AND (? IS NULL OR relation = ?)
               ),
               walk(depth, current_id, visited, node_path, edge_path) AS (
                   SELECT 0, entity_id, char(31) || entity_id || char(31),
                          json_array(entity_id), json_array()
                   FROM start_nodes
                   UNION ALL
                   SELECT walk.depth + 1,
                          adjacency.to_id,
                          walk.visited || adjacency.to_id || char(31),
                          json_insert(walk.node_path, '$[#]', adjacency.to_id),
                          json_insert(
                              walk.edge_path,
                              '$[#]',
                              json_object(
                                  'from', adjacency.edge_from,
                                  'relation', adjacency.relation,
                                  'to', adjacency.edge_to,
                                  'memory_id', adjacency.memory_id,
                                  'properties', json(adjacency.properties_json)
                              )
                          )
                   FROM walk
                   JOIN adjacency ON adjacency.from_id = walk.current_id
                   WHERE walk.depth < ?
                     AND instr(
                         walk.visited,
                         char(31) || adjacency.to_id || char(31)
                     ) = 0
                   LIMIT ?
               )
               SELECT depth, node_path, edge_path
               FROM walk
               WHERE depth > 0
               ORDER BY depth ASC, current_id ASC, node_path ASC
               LIMIT ?"#,
        )
        .bind(serde_json::to_string(&start_ids)?)
        .bind(tenant)
        .bind(&namespace)
        .bind(direction)
        .bind(&relation)
        .bind(&relation)
        .bind(tenant)
        .bind(&namespace)
        .bind(direction)
        .bind(&relation)
        .bind(&relation)
        .bind(max_hops)
        .bind(MAX_GRAPH_WALK_ROWS)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;

        let mut paths = Vec::with_capacity(rows.len());
        let mut used_entity_ids = start_ids;
        for row in rows {
            let nodes: Vec<String> = serde_json::from_str(&row.try_get::<String, _>("node_path")?)?;
            let edges: Vec<GraphPathEdge> =
                serde_json::from_str(&row.try_get::<String, _>("edge_path")?)?;
            used_entity_ids.extend(nodes.iter().cloned());
            paths.push(GraphPath {
                hops: row.try_get::<i64, _>("depth")? as usize,
                nodes,
                edges,
            });
        }
        let entity_rows = db::query(
            "SELECT memory_id, entity_id, label, entity_type, properties_json, updated_at \
             FROM entities WHERE tenant = ? AND namespace = ? \
               AND entity_id IN (SELECT value FROM json_each(?)) \
             ORDER BY updated_at DESC, memory_id ASC",
        )
        .bind(tenant)
        .bind(&namespace)
        .bind(serde_json::to_string(&used_entity_ids)?)
        .fetch_all(&self.pool)
        .await?;
        let mut entity_index = BTreeMap::<String, GraphEntity>::new();
        for row in entity_rows {
            let entity_id = row.try_get::<String, _>("entity_id")?;
            let memory_id = row.try_get::<String, _>("memory_id")?;
            if let Some(existing) = entity_index.get_mut(&entity_id) {
                if !existing.memory_ids.contains(&memory_id) {
                    existing.memory_ids.push(memory_id);
                }
                continue;
            }
            entity_index.insert(
                entity_id.clone(),
                GraphEntity {
                    id: entity_id,
                    label: row.try_get("label")?,
                    kind: row.try_get("entity_type")?,
                    properties: serde_json::from_str(
                        &row.try_get::<String, _>("properties_json")?,
                    )?,
                    memory_ids: vec![memory_id],
                },
            );
        }
        let entities = entity_index.into_values().collect();
        Ok(GraphQueryResult { entities, paths })
    }

    async fn initialize_schema(&self) -> Result<()> {
        const SCHEMA_VERSION: i64 = 8;
        const GRAPH_MIGRATION_SCHEMA_VERSION: i64 = 6;
        const DELIVERY_PAYLOAD_SCHEMA_VERSION: i64 = 7;
        let version = db::query_scalar::<i64>("PRAGMA user_version")
            .fetch_optional(&self.pool)
            .await?
            .unwrap_or(0);
        if version != 0
            && version != GRAPH_MIGRATION_SCHEMA_VERSION
            && version != DELIVERY_PAYLOAD_SCHEMA_VERSION
            && version != SCHEMA_VERSION
        {
            return Err(anyhow!(
                "agentd schema version {version} is unsupported; restart with --reset-data"
            ));
        }
        if version == 0 {
            let existing = db::query_scalar::<String>(
                "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' LIMIT 1",
            )
            .fetch_optional(&self.pool)
            .await?;
            if let Some(table) = existing {
                return Err(anyhow!(
                    "unversioned agentd data found ({table}); restart with --reset-data"
                ));
            }
        }

        let statements = [
            r#"CREATE TABLE tenants (
                name TEXT PRIMARY KEY NOT NULL,
                metadata_json TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )"#,
            r#"CREATE TABLE agents (
                tenant TEXT NOT NULL,
                name TEXT NOT NULL,
                metadata_json TEXT NOT NULL,
                spec_json TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (tenant, name)
            )"#,
            r#"CREATE TABLE runs (
                run_id TEXT PRIMARY KEY NOT NULL,
                tenant TEXT NOT NULL,
                name TEXT NOT NULL,
                agent_ref TEXT NOT NULL,
                scope TEXT NOT NULL,
                source TEXT NOT NULL,
                input_json TEXT NOT NULL,
                output_json TEXT,
                error TEXT,
                status TEXT NOT NULL,
                request_id TEXT,
                schedule_name TEXT,
                delivery_destination TEXT,
                created_at TEXT NOT NULL,
                started_at TEXT,
                updated_at TEXT NOT NULL
            )"#,
            r#"CREATE TABLE run_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT NOT NULL,
                kind TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                ts TEXT NOT NULL
            )"#,
            r#"CREATE TABLE contexts (
                tenant TEXT NOT NULL,
                agent TEXT NOT NULL,
                scope TEXT NOT NULL,
                revision INTEGER NOT NULL,
                state_json TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (tenant, agent, scope)
            )"#,
            r#"CREATE TABLE artifacts (
                tenant TEXT NOT NULL,
                path TEXT NOT NULL,
                body BLOB NOT NULL,
                content_type TEXT,
                meta_json TEXT,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (tenant, path)
            )"#,
            r#"CREATE TABLE memory (
                tenant TEXT NOT NULL,
                namespace TEXT NOT NULL,
                id TEXT NOT NULL,
                text TEXT NOT NULL,
                embedding BLOB NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                UNIQUE (tenant, namespace, id)
            )"#,
            "CREATE VIRTUAL TABLE memory_fts USING fts5(tenant UNINDEXED, namespace UNINDEXED, id UNINDEXED, text, content='memory', content_rowid='rowid')",
            r#"CREATE TRIGGER memory_ai AFTER INSERT ON memory BEGIN
                INSERT INTO memory_fts(rowid, tenant, namespace, id, text)
                VALUES (new.rowid, new.tenant, new.namespace, new.id, new.text);
            END"#,
            r#"CREATE TRIGGER memory_ad AFTER DELETE ON memory BEGIN
                INSERT INTO memory_fts(memory_fts, rowid, tenant, namespace, id, text)
                VALUES ('delete', old.rowid, old.tenant, old.namespace, old.id, old.text);
            END"#,
            r#"CREATE TRIGGER memory_au AFTER UPDATE ON memory BEGIN
                INSERT INTO memory_fts(memory_fts, rowid, tenant, namespace, id, text)
                VALUES ('delete', old.rowid, old.tenant, old.namespace, old.id, old.text);
                INSERT INTO memory_fts(rowid, tenant, namespace, id, text)
                VALUES (new.rowid, new.tenant, new.namespace, new.id, new.text);
            END"#,
            r#"CREATE TABLE schedules (
                tenant TEXT NOT NULL,
                name TEXT NOT NULL,
                spec_json TEXT NOT NULL,
                last_triggered_at TEXT,
                next_trigger_at TEXT,
                last_run_id TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (tenant, name)
            )"#,
            r#"CREATE TABLE deliveries (
                delivery_id TEXT PRIMARY KEY NOT NULL,
                tenant TEXT NOT NULL,
                run_id TEXT NOT NULL,
                status TEXT NOT NULL,
                destination TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                idempotency_key TEXT NOT NULL UNIQUE,
                attempt INTEGER NOT NULL DEFAULT 0,
                next_attempt_at TEXT,
                last_error TEXT,
                claim_token TEXT,
                claim_expires_at TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )"#,
            r#"CREATE TABLE mcp_servers (
                tenant TEXT NOT NULL,
                name TEXT NOT NULL,
                spec_json TEXT NOT NULL,
                tools_json TEXT NOT NULL,
                last_error TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (tenant, name)
            )"#,
            "CREATE UNIQUE INDEX idx_runs_request_id ON runs(tenant, request_id) WHERE request_id IS NOT NULL",
            "CREATE UNIQUE INDEX idx_runs_active_scope ON runs(tenant, agent_ref, scope) WHERE status = 'running'",
            "CREATE INDEX idx_runs_queue ON runs(status, created_at)",
            "CREATE INDEX idx_runs_tenant_created ON runs(tenant, created_at DESC)",
            "CREATE INDEX idx_run_log_run ON run_log(run_id, id)",
            "CREATE INDEX idx_artifacts_tenant_path ON artifacts(tenant, path)",
            "CREATE INDEX idx_deliveries_claim ON deliveries(tenant, status, next_attempt_at, created_at)",
        ];
        let graph_statements = [
            r#"CREATE TABLE entities (
                tenant TEXT NOT NULL,
                namespace TEXT NOT NULL,
                memory_id TEXT NOT NULL,
                entity_id TEXT NOT NULL,
                label TEXT NOT NULL,
                entity_type TEXT,
                properties_json TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (tenant, namespace, memory_id, entity_id),
                FOREIGN KEY (tenant, namespace, memory_id)
                    REFERENCES memory(tenant, namespace, id) ON DELETE CASCADE
            )"#,
            r#"CREATE TABLE edges (
                tenant TEXT NOT NULL,
                namespace TEXT NOT NULL,
                memory_id TEXT NOT NULL,
                source_entity_id TEXT NOT NULL,
                relation TEXT NOT NULL,
                target_entity_id TEXT NOT NULL,
                properties_json TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (
                    tenant, namespace, memory_id,
                    source_entity_id, relation, target_entity_id
                ),
                FOREIGN KEY (tenant, namespace, memory_id)
                    REFERENCES memory(tenant, namespace, id) ON DELETE CASCADE
            )"#,
            "CREATE INDEX idx_entities_lookup ON entities(tenant, namespace, entity_id)",
            "CREATE INDEX idx_entities_label ON entities(tenant, namespace, label)",
            "CREATE INDEX idx_edges_outgoing ON edges(tenant, namespace, source_entity_id, relation)",
            "CREATE INDEX idx_edges_incoming ON edges(tenant, namespace, target_entity_id, relation)",
        ];
        if version == 0 {
            for statement in statements.into_iter().chain(graph_statements) {
                db::query(statement).execute(&self.pool).await?;
            }
            db::query("PRAGMA user_version = 8")
                .execute(&self.pool)
                .await?;
        } else if matches!(
            version,
            GRAPH_MIGRATION_SCHEMA_VERSION | DELIVERY_PAYLOAD_SCHEMA_VERSION
        ) {
            let mut tx = self.pool.begin().await?;
            if version == GRAPH_MIGRATION_SCHEMA_VERSION {
                for statement in graph_statements {
                    db::query(statement).execute(&mut tx).await?;
                }
            }
            db::query(
                "ALTER TABLE deliveries ADD COLUMN payload_json TEXT NOT NULL DEFAULT 'null'",
            )
            .execute(&mut tx)
            .await?;
            db::query(
                "UPDATE deliveries SET payload_json = COALESCE(\
                 (SELECT output_json FROM runs WHERE runs.run_id = deliveries.run_id), 'null')",
            )
            .execute(&mut tx)
            .await?;
            db::query("PRAGMA user_version = 8")
                .execute(&mut tx)
                .await?;
            tx.commit().await?;
        }
        Ok(())
    }

    pub async fn apply_agent(&self, agent: &AgentResource) -> Result<()> {
        if self.get_tenant(&agent.metadata.tenant).await?.is_none() {
            return Err(anyhow!("tenant not found: {}", agent.metadata.tenant));
        }
        let now = Utc::now().to_rfc3339();
        db::query(
            r#"INSERT INTO agents (tenant, name, metadata_json, spec_json, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?)
               ON CONFLICT(tenant, name) DO UPDATE SET
                 metadata_json=excluded.metadata_json,
                 spec_json=excluded.spec_json,
                 updated_at=excluded.updated_at"#,
        )
        .bind(&agent.metadata.tenant)
        .bind(&agent.metadata.name)
        .bind(serde_json::to_string(&agent.metadata)?)
        .bind(serde_json::to_string(&agent.spec)?)
        .bind(&now)
        .bind(&now)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn get_agent(&self, tenant: &str, name: &str) -> Result<Option<Agent>> {
        let row = db::query(
            "SELECT metadata_json, spec_json, created_at, updated_at FROM agents WHERE tenant = ? AND name = ?",
        )
        .bind(tenant)
        .bind(name)
        .fetch_optional(&self.pool)
        .await?;
        row.map(row_to_agent).transpose()
    }

    pub async fn list_agents(&self, tenant: Option<&str>) -> Result<Vec<Agent>> {
        let rows = if let Some(tenant) = tenant {
            db::query(
                "SELECT metadata_json, spec_json, created_at, updated_at FROM agents WHERE tenant = ? ORDER BY tenant, name",
            )
            .bind(tenant)
            .fetch_all(&self.pool)
            .await?
        } else {
            db::query(
                "SELECT metadata_json, spec_json, created_at, updated_at FROM agents ORDER BY tenant, name",
            )
            .fetch_all(&self.pool)
            .await?
        };
        rows.into_iter().map(row_to_agent).collect()
    }
    pub async fn apply_mcp_server(
        &self,
        tenant: &str,
        name: &str,
        spec: &agentd_api::McpServerSpec,
        tools: &[agentd_api::McpTool],
        last_error: Option<&str>,
    ) -> Result<()> {
        let _guard = self.mcp_apply_lock.lock().await;
        spec.validate().map_err(|error| anyhow!(error))?;
        if self.get_tenant(tenant).await?.is_none() {
            return Err(anyhow!("tenant not found: {tenant}"));
        }
        self.ensure_unique_mcp_tool_names(tenant, name, spec.enabled, tools)
            .await?;
        let now = Utc::now().to_rfc3339();
        db::query(
            r#"INSERT INTO mcp_servers (
                   tenant, name, spec_json, tools_json, last_error, created_at, updated_at
               ) VALUES (?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(tenant, name) DO UPDATE SET
                 spec_json = excluded.spec_json,
                 tools_json = excluded.tools_json,
                 last_error = excluded.last_error,
                 updated_at = excluded.updated_at"#,
        )
        .bind(tenant)
        .bind(name)
        .bind(serde_json::to_string(spec)?)
        .bind(serde_json::to_string(tools)?)
        .bind(last_error)
        .bind(&now)
        .bind(&now)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn ensure_unique_mcp_tool_names(
        &self,
        tenant: &str,
        candidate_server: &str,
        candidate_enabled: bool,
        candidate_tools: &[agentd_api::McpTool],
    ) -> Result<()> {
        let mut owners = BTreeMap::new();
        for server in self.list_mcp_servers(Some(tenant)).await? {
            if server.name == candidate_server || !server.spec.enabled {
                continue;
            }
            for tool in server.tools {
                let exposed = mcp_exposed_name(&server.name, &tool.name);
                let owner = format!("{}/{}", server.name, tool.name);
                if let Some(existing) = owners.insert(exposed.clone(), owner.clone()) {
                    return Err(anyhow!(
                        "MCP exposed tool name collision for {exposed}: {existing} and {owner}"
                    ));
                }
            }
        }
        if candidate_enabled {
            for tool in candidate_tools {
                let exposed = mcp_exposed_name(candidate_server, &tool.name);
                let owner = format!("{candidate_server}/{}", tool.name);
                if let Some(existing) = owners.insert(exposed.clone(), owner.clone()) {
                    return Err(anyhow!(
                        "MCP exposed tool name collision for {exposed}: {existing} and {owner}"
                    ));
                }
            }
        }
        Ok(())
    }

    pub async fn delete_mcp_server(&self, tenant: &str, name: &str) -> Result<bool> {
        let deleted = db::query("DELETE FROM mcp_servers WHERE tenant = ? AND name = ?")
            .bind(tenant)
            .bind(name)
            .execute(&self.pool)
            .await?
            .rows_affected()
            > 0;
        Ok(deleted)
    }

    pub async fn get_mcp_server(&self, tenant: &str, name: &str) -> Result<Option<McpServer>> {
        let row = db::query(
            "SELECT tenant, name, spec_json, tools_json, last_error, created_at, updated_at \
             FROM mcp_servers WHERE tenant = ? AND name = ?",
        )
        .bind(tenant)
        .bind(name)
        .fetch_optional(&self.pool)
        .await?;
        row.map(row_to_mcp_server).transpose()
    }

    pub async fn list_mcp_servers(&self, tenant: Option<&str>) -> Result<Vec<McpServer>> {
        let rows = if let Some(tenant) = tenant {
            db::query(
                "SELECT tenant, name, spec_json, tools_json, last_error, created_at, updated_at \
                 FROM mcp_servers WHERE tenant = ? ORDER BY name",
            )
            .bind(tenant)
            .fetch_all(&self.pool)
            .await?
        } else {
            db::query(
                "SELECT tenant, name, spec_json, tools_json, last_error, created_at, updated_at \
                 FROM mcp_servers ORDER BY tenant, name",
            )
            .fetch_all(&self.pool)
            .await?
        };
        rows.into_iter().map(row_to_mcp_server).collect()
    }

    pub async fn get_mcp_tool_invocation_target(
        &self,
        tenant: &str,
        exposed_name: &str,
    ) -> Result<Option<McpToolInvocationTarget>> {
        for server in self.list_mcp_servers(Some(tenant)).await? {
            if !server.spec.enabled {
                continue;
            }
            for tool in &server.tools {
                if mcp_exposed_name(&server.name, &tool.name) == exposed_name {
                    return Ok(Some(McpToolInvocationTarget {
                        server: server.clone(),
                        tool: tool.clone(),
                    }));
                }
            }
        }
        Ok(None)
    }

    pub async fn list_visible_tools(
        &self,
        tenant: &str,
        allowed_families: &[ToolFamily],
    ) -> Result<Vec<ToolSpec>> {
        let mut tools = visible_tools(&builtin_tool_catalog(), allowed_families);
        let mcp_allowed = allowed_families.contains(&ToolFamily::Mcp);
        if mcp_allowed {
            for server in self.list_mcp_servers(Some(tenant)).await? {
                if !server.spec.enabled {
                    continue;
                }
                tools.extend(server.tools.iter().map(|tool| ToolSpec {
                    name: mcp_exposed_name(&server.name, &tool.name),
                    family: ToolFamily::Mcp,
                    description:
                        tool.description.clone().unwrap_or_else(|| {
                            format!("MCP tool {} from {}", tool.name, server.name)
                        }),
                    input_schema: tool.input_schema.clone(),
                    mutating: true,
                }));
            }
        }
        Ok(tools)
    }
    pub async fn put_schedule(&self, tenant: &str, name: &str, spec: &ScheduleSpec) -> Result<()> {
        spec.validate().map_err(|error| anyhow!(error))?;
        if self.get_tenant(tenant).await?.is_none() {
            return Err(anyhow!("tenant not found: {tenant}"));
        }
        if self.get_agent(tenant, &spec.agent_ref).await?.is_none() {
            return Err(anyhow!("unknown schedule agent: {}", spec.agent_ref));
        }
        let now = Utc::now();
        let next_trigger_at = next_trigger_time(spec, now)?;
        let now_s = now.to_rfc3339();
        db::query(
            r#"INSERT INTO schedules (
                   tenant, name, spec_json, last_triggered_at, next_trigger_at,
                   last_run_id, created_at, updated_at
               ) VALUES (?, ?, ?, NULL, ?, NULL, ?, ?)
               ON CONFLICT(tenant, name) DO UPDATE SET
                 spec_json=excluded.spec_json,
                 next_trigger_at=excluded.next_trigger_at,
                 updated_at=excluded.updated_at"#,
        )
        .bind(tenant)
        .bind(name)
        .bind(serde_json::to_string(spec)?)
        .bind(next_trigger_at.map(|value| value.to_rfc3339()))
        .bind(&now_s)
        .bind(&now_s)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn get_schedule(&self, tenant: &str, name: &str) -> Result<Option<Schedule>> {
        let row = db::query(
            "SELECT tenant, name, spec_json, last_triggered_at, next_trigger_at, last_run_id, created_at, updated_at FROM schedules WHERE tenant = ? AND name = ?",
        )
        .bind(tenant)
        .bind(name)
        .fetch_optional(&self.pool)
        .await?;
        row.map(row_to_schedule).transpose()
    }

    pub async fn list_schedules(&self, tenant: Option<&str>) -> Result<Vec<Schedule>> {
        let rows = if let Some(tenant) = tenant {
            db::query(
                "SELECT tenant, name, spec_json, last_triggered_at, next_trigger_at, last_run_id, created_at, updated_at FROM schedules WHERE tenant = ? ORDER BY name",
            )
            .bind(tenant)
            .fetch_all(&self.pool)
            .await?
        } else {
            db::query(
                "SELECT tenant, name, spec_json, last_triggered_at, next_trigger_at, last_run_id, created_at, updated_at FROM schedules ORDER BY tenant, name",
            )
            .fetch_all(&self.pool)
            .await?
        };
        rows.into_iter().map(row_to_schedule).collect()
    }

    pub async fn delete_schedule(&self, tenant: &str, name: &str) -> Result<serde_json::Value> {
        let deleted = db::query("DELETE FROM schedules WHERE tenant = ? AND name = ?")
            .bind(tenant)
            .bind(name)
            .execute(&self.pool)
            .await?
            .rows_affected()
            > 0;
        Ok(json!({"deleted": deleted, "name": name}))
    }

    pub async fn submit_run(&self, run: NewRun<'_>) -> Result<Uuid> {
        if run
            .delivery_destination
            .is_some_and(|destination| destination.trim().is_empty())
        {
            return Err(anyhow!("delivery destination is required"));
        }
        if let Some(request_id) = run.request_id {
            if let Some(existing) = self.find_run_by_request_id(run.tenant, request_id).await? {
                return Ok(existing.run_id);
            }
        }
        if self.get_agent(run.tenant, run.agent_ref).await?.is_none() {
            return Err(anyhow!("agent not found: {}/{}", run.tenant, run.agent_ref));
        }
        let run_id = Uuid::new_v4();
        let now = Utc::now().to_rfc3339();
        let result = db::query(
            r#"INSERT INTO runs (
                   run_id, tenant, name, agent_ref, scope, source, input_json,
                   status, request_id, schedule_name, delivery_destination,
                   created_at, updated_at
               ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"#,
        )
        .bind(run_id.to_string())
        .bind(run.tenant)
        .bind(run.name)
        .bind(run.agent_ref)
        .bind(run.scope)
        .bind(run.source)
        .bind(serde_json::to_string(run.input)?)
        .bind(status_to_wire(AgentRunStatus::Queued))
        .bind(run.request_id)
        .bind(run.schedule_name)
        .bind(run.delivery_destination.map(str::trim))
        .bind(&now)
        .bind(&now)
        .execute(&self.pool)
        .await;
        match result {
            Ok(_) => Ok(run_id),
            Err(error) if run.request_id.is_some() => self
                .find_run_by_request_id(run.tenant, run.request_id.unwrap())
                .await?
                .map(|run| run.run_id)
                .ok_or_else(|| error.into()),
            Err(error) => Err(error.into()),
        }
    }

    pub async fn list_delivery_outbox(
        &self,
        tenant: Option<&str>,
        status: Option<&str>,
        run_id: Option<Uuid>,
        limit: usize,
    ) -> Result<Vec<DeliveryOutboxRecord>> {
        let mut sql = String::from(
            "SELECT d.delivery_id, d.tenant, d.run_id, d.status, d.destination, \
             d.payload_json, d.attempt, d.next_attempt_at, d.last_error, \
             d.claim_token, d.claim_expires_at, d.created_at, d.updated_at \
             FROM deliveries d WHERE 1 = 1",
        );
        if tenant.is_some() {
            sql.push_str(" AND d.tenant = ?");
        }
        if status.is_some() {
            sql.push_str(" AND d.status = ?");
        }
        if run_id.is_some() {
            sql.push_str(" AND d.run_id = ?");
        }
        sql.push_str(" ORDER BY d.created_at DESC LIMIT ?");
        let mut query = db::query(&sql);
        if let Some(tenant) = tenant {
            query = query.bind(tenant);
        }
        if let Some(status) = status {
            query = query.bind(status);
        }
        if let Some(run_id) = run_id {
            query = query.bind(run_id.to_string());
        }
        let rows = query
            .bind(limit.max(1) as i64)
            .fetch_all(&self.pool)
            .await?;
        rows.into_iter().map(row_to_delivery_outbox).collect()
    }

    pub async fn get_delivery_outbox(
        &self,
        delivery_id: Uuid,
    ) -> Result<Option<DeliveryOutboxRecord>> {
        let row = db::query(
            "SELECT d.delivery_id, d.tenant, d.run_id, d.status, d.destination, \
             d.payload_json, d.attempt, d.next_attempt_at, d.last_error, \
             d.claim_token, d.claim_expires_at, d.created_at, d.updated_at \
             FROM deliveries d WHERE d.delivery_id = ?",
        )
        .bind(delivery_id.to_string())
        .fetch_optional(&self.pool)
        .await?;
        row.map(row_to_delivery_outbox).transpose()
    }

    pub async fn claim_delivery_outbox(
        &self,
        tenant: &str,
        limit: usize,
        now: DateTime<Utc>,
        claim_ttl: Duration,
    ) -> Result<Vec<DeliveryOutboxRecord>> {
        let rows = db::query(
            "SELECT d.delivery_id, d.tenant, d.run_id, d.status, d.destination, \
             d.payload_json, d.attempt, d.next_attempt_at, d.last_error, \
             d.claim_token, d.claim_expires_at, d.created_at, d.updated_at \
             FROM deliveries d WHERE d.tenant = ? AND ( \
               (d.status = 'pending' AND (d.next_attempt_at IS NULL OR d.next_attempt_at <= ?)) OR \
               (d.status = 'claimed' AND (d.claim_expires_at IS NULL OR d.claim_expires_at <= ?)) \
             ) ORDER BY d.created_at LIMIT ?",
        )
        .bind(tenant)
        .bind(now.to_rfc3339())
        .bind(now.to_rfc3339())
        .bind(limit.max(1).saturating_mul(4) as i64)
        .fetch_all(&self.pool)
        .await?;
        let expires_at = now + ChronoDuration::from_std(claim_ttl)?;
        let mut claimed = Vec::new();
        for row in rows {
            if claimed.len() >= limit.max(1) {
                break;
            }
            let delivery = row_to_delivery_outbox(row)?;
            let token = Uuid::new_v4().to_string();
            let changed = db::query(
                "UPDATE deliveries SET status = 'claimed', claim_token = ?, claim_expires_at = ?, \
                 updated_at = ? WHERE delivery_id = ? AND (status = 'pending' OR \
                 (status = 'claimed' AND claim_expires_at <= ? AND claim_token IS ?))",
            )
            .bind(&token)
            .bind(expires_at.to_rfc3339())
            .bind(now.to_rfc3339())
            .bind(delivery.delivery_id.to_string())
            .bind(now.to_rfc3339())
            .bind(delivery.claim_token.as_deref())
            .execute(&self.pool)
            .await?
            .rows_affected();
            if changed > 0 {
                if let Some(record) = self.get_delivery_outbox(delivery.delivery_id).await? {
                    claimed.push(record);
                }
            }
        }
        Ok(claimed)
    }

    pub async fn ack_delivery(
        &self,
        tenant: &str,
        ack: DeliveryAck<'_>,
    ) -> Result<DeliveryOutboxRecord> {
        let existing = self
            .get_delivery_outbox(ack.delivery_id)
            .await?
            .ok_or_else(|| anyhow!("delivery not found"))?;
        if existing.tenant != tenant
            || existing.status != "claimed"
            || existing.claim_token.as_deref() != Some(ack.claim_token)
        {
            return Err(anyhow!("delivery claim token does not match"));
        }
        if existing
            .claim_expires_at
            .is_none_or(|expires| expires <= ack.now)
        {
            return Err(anyhow!("delivery claim has expired"));
        }
        let (status, next_attempt_at) = match ack.outcome {
            "delivered" => ("delivered", None),
            "retry" => (
                "pending",
                Some(
                    ack.now
                        + ChronoDuration::from_std(
                            ack.retry_after.unwrap_or(Duration::from_secs(1)),
                        )?,
                ),
            ),
            "failed" => ("failed", None),
            _ => {
                return Err(anyhow!(
                    "delivery outcome must be delivered, retry, or failed"
                ))
            }
        };
        db::query(
            "UPDATE deliveries SET status = ?, attempt = attempt + 1, next_attempt_at = ?, \
             last_error = ?, claim_token = NULL, claim_expires_at = NULL, updated_at = ? \
             WHERE delivery_id = ? AND tenant = ? AND claim_token = ?",
        )
        .bind(status)
        .bind(next_attempt_at.map(|value| value.to_rfc3339()))
        .bind(ack.error)
        .bind(ack.now.to_rfc3339())
        .bind(ack.delivery_id.to_string())
        .bind(tenant)
        .bind(ack.claim_token)
        .execute(&self.pool)
        .await?;
        self.get_delivery_outbox(ack.delivery_id)
            .await?
            .ok_or_else(|| anyhow!("delivery disappeared after ack"))
    }

    async fn find_run_by_request_id(
        &self,
        tenant: &str,
        request_id: &str,
    ) -> Result<Option<AgentRun>> {
        let row = db::query(
            "SELECT run_id, tenant, name, agent_ref, scope, source, input_json, \
                    output_json, error, status, request_id, created_at, started_at, updated_at \
             FROM runs WHERE tenant = ? AND request_id = ? LIMIT 1",
        )
        .bind(tenant)
        .bind(request_id)
        .fetch_optional(&self.pool)
        .await?;
        row.map(row_to_run).transpose()
    }

    pub async fn get_run(&self, run_id: Uuid) -> Result<Option<AgentRun>> {
        let row = db::query(
            "SELECT run_id, tenant, name, agent_ref, scope, source, input_json, \
                    output_json, error, status, request_id, created_at, started_at, updated_at \
             FROM runs WHERE run_id = ?",
        )
        .bind(run_id.to_string())
        .fetch_optional(&self.pool)
        .await?;
        row.map(row_to_run).transpose()
    }

    pub async fn list_runs(&self, query: &RunListQuery) -> Result<Vec<AgentRun>> {
        let mut sql = String::from(
            "SELECT run_id, tenant, name, agent_ref, scope, source, input_json, \
                    output_json, error, status, request_id, created_at, started_at, updated_at \
             FROM runs WHERE 1 = 1",
        );
        if query.tenant.is_some() {
            sql.push_str(" AND tenant = ?");
        }
        if query.agent_ref.is_some() {
            sql.push_str(" AND agent_ref = ?");
        }
        if query.status.is_some() {
            sql.push_str(" AND status = ?");
        }
        sql.push_str(" ORDER BY created_at DESC LIMIT ?");

        let mut q = db::query(&sql);
        if let Some(tenant) = query.tenant.as_deref() {
            q = q.bind(tenant);
        }
        if let Some(agent_ref) = query.agent_ref.as_deref() {
            q = q.bind(agent_ref);
        }
        if let Some(status) = query.status {
            q = q.bind(status_to_wire(status));
        }
        q = q.bind(query.limit.max(1) as i64);

        let rows = q.fetch_all(&self.pool).await?;
        rows.into_iter().map(row_to_run).collect()
    }

    pub async fn get_run_output(&self, run_id: Uuid) -> Result<Option<serde_json::Value>> {
        let row = db::query("SELECT output_json FROM runs WHERE run_id = ?")
            .bind(run_id.to_string())
            .fetch_optional(&self.pool)
            .await?;
        Ok(row
            .map(|row| row.try_get::<Option<String>, _>("output_json"))
            .transpose()?
            .flatten()
            .map(|raw| serde_json::from_str(&raw))
            .transpose()?)
    }

    pub async fn cancel_run_request(&self, run_id: Uuid, reason: &str) -> Result<AgentRunStatus> {
        let mut tx = self.pool.begin().await?;
        let current = db::query_scalar::<String>("SELECT status FROM runs WHERE run_id = ?")
            .bind(run_id.to_string())
            .fetch_optional(&mut tx)
            .await?
            .ok_or_else(|| anyhow!("run not found"))?;
        let current = status_from_wire(&current)?;
        if !matches!(current, AgentRunStatus::Queued | AgentRunStatus::Running) {
            return Ok(current);
        }

        let now = Utc::now().to_rfc3339();
        let changed = db::query(
            "UPDATE runs SET status = 'cancelled', error = ?, updated_at = ? \
             WHERE run_id = ? AND status IN ('queued', 'running')",
        )
        .bind(reason)
        .bind(&now)
        .bind(run_id.to_string())
        .execute(&mut tx)
        .await?
        .rows_affected();
        if changed != 1 {
            return Err(anyhow!("run status changed during cancellation"));
        }
        db::query(
            "INSERT INTO run_log (run_id, kind, payload_json, ts) VALUES (?, 'status', ?, ?)",
        )
        .bind(run_id.to_string())
        .bind(json!({"status":"cancelled", "reason":reason}).to_string())
        .bind(&now)
        .execute(&mut tx)
        .await?;
        tx.commit().await?;
        Ok(AgentRunStatus::Cancelled)
    }

    pub async fn claim_next_run(&self) -> Result<Option<AssignedRun>> {
        let rows = db::query(
            r#"SELECT run_id, tenant, name, agent_ref, scope, created_at, updated_at
               FROM runs
               WHERE status = 'queued'
               ORDER BY created_at ASC
               LIMIT 64"#,
        )
        .fetch_all(&self.pool)
        .await?;
        if rows.is_empty() {
            return Ok(None);
        }

        for row in rows {
            let run_id = parse_uuid_field(&row, "run_id")?;
            let tenant = row.try_get::<String, _>("tenant")?;
            let agent_ref = row.try_get::<String, _>("agent_ref")?;
            let scope = row.try_get::<String, _>("scope")?;
            let Some(mut run) = self.get_run(run_id).await? else {
                continue;
            };
            if run.status != AgentRunStatus::Queued {
                continue;
            }
            let Some(agent) = self.get_agent(&tenant, &agent_ref).await? else {
                self.fail_run(
                    run_id,
                    &format!("agent {agent_ref} not found for run {run_id}"),
                )
                .await?;
                continue;
            };
            let visible_tools = match self
                .list_visible_tools(&tenant, &agent.spec.effective_allowed_families())
                .await
            {
                Ok(tools) => tools,
                Err(error) => {
                    self.fail_run(run_id, &format!("failed to resolve run tools: {error}"))
                        .await?;
                    continue;
                }
            };

            let started_at = Utc::now();
            let started_at_raw = started_at.to_rfc3339();
            let mut tx = self.pool.begin().await?;
            let claimed = db::query(
                "UPDATE runs SET status = 'running', started_at = ?, updated_at = ? \
                 WHERE run_id = ? AND status = 'queued' \
                 AND NOT EXISTS (SELECT 1 FROM runs active WHERE active.tenant = ? \
                   AND active.agent_ref = ? AND active.scope = ? AND active.status = 'running')",
            )
            .bind(&started_at_raw)
            .bind(&started_at_raw)
            .bind(run_id.to_string())
            .bind(&tenant)
            .bind(&agent_ref)
            .bind(&scope)
            .execute(&mut tx)
            .await?
            .rows_affected()
                > 0;
            if !claimed {
                tx.rollback().await?;
                continue;
            }
            tx.commit().await?;
            run.status = AgentRunStatus::Running;
            run.started_at = Some(started_at);
            run.updated_at = started_at;
            return Ok(Some(AssignedRun {
                run,
                timeout_ms: agent.spec.limits.timeout_ms,
                max_steps: agent.spec.limits.max_steps,
                agent_system_prompt: agent.spec.system_prompt,
                agent_model: agent.spec.model,
                agent_temperature: agent.spec.temperature,
                agent_max_tokens: agent.spec.max_tokens,
                agent_context_turns: agent.spec.context_window,
                visible_tools,
            }));
        }
        Ok(None)
    }

    pub async fn reset_local_runtime_state(&self) -> Result<()> {
        db::query(
            "UPDATE runs SET status = 'failed', error = COALESCE(error, 'agentd restarted'), updated_at = ? WHERE status = 'running'",
        )
        .bind(Utc::now().to_rfc3339())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn append_event(
        &self,
        run_id: Uuid,
        event_type: &str,
        payload: serde_json::Value,
        ts: DateTime<Utc>,
    ) -> Result<()> {
        db::query("INSERT INTO run_log (run_id, kind, payload_json, ts) VALUES (?, ?, ?, ?)")
            .bind(run_id.to_string())
            .bind(event_type)
            .bind(payload.to_string())
            .bind(ts.to_rfc3339())
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// Commit the durable result of a successful run in one transaction.
    /// Model/tool trace entries are written while execution is in progress;
    /// this transaction owns the final output, rolling context, terminal
    /// status, and optional outbox row so consumers never observe a partial
    /// success.
    pub async fn finalize_run_success(
        &self,
        run_id: Uuid,
        output: &serde_json::Value,
        context_state: Option<&serde_json::Value>,
    ) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        let row = db::query(
            "SELECT tenant, agent_ref, scope, status, output_json, delivery_destination \
             FROM runs WHERE run_id = ?",
        )
        .bind(run_id.to_string())
        .fetch_optional(&mut tx)
        .await?
        .ok_or_else(|| anyhow!("run not found for finalization"))?;
        if row.try_get::<Option<String>, _>("output_json")?.is_some() {
            return Err(anyhow!("run output was already finalized"));
        }
        if row.try_get::<String, _>("status")? != "running" {
            return Err(anyhow!("only a running run can be finalized"));
        }
        let tenant = row.try_get::<String, _>("tenant")?;
        let agent = row.try_get::<String, _>("agent_ref")?;
        let scope = row.try_get::<String, _>("scope")?;
        let delivery_destination = row.try_get::<Option<String>, _>("delivery_destination")?;
        let now = Utc::now().to_rfc3339();
        let output_json = serde_json::to_string(output)?;

        let changed = db::query(
            "UPDATE runs SET output_json = ?, error = NULL, status = 'succeeded', updated_at = ? \
             WHERE run_id = ? AND status = 'running'",
        )
        .bind(&output_json)
        .bind(&now)
        .bind(run_id.to_string())
        .execute(&mut tx)
        .await?
        .rows_affected();
        if changed != 1 {
            return Err(anyhow!("run was no longer running during finalization"));
        }

        db::query(
            "INSERT INTO run_log (run_id, kind, payload_json, ts) VALUES (?, 'output', ?, ?)",
        )
        .bind(run_id.to_string())
        .bind(&output_json)
        .bind(&now)
        .execute(&mut tx)
        .await?;
        db::query(
            "INSERT INTO run_log (run_id, kind, payload_json, ts) VALUES (?, 'status', ?, ?)",
        )
        .bind(run_id.to_string())
        .bind(serde_json::json!({"status":"succeeded"}).to_string())
        .bind(&now)
        .execute(&mut tx)
        .await?;
        if let Some(state) = context_state {
            db::query(
                "INSERT INTO contexts (tenant, agent, scope, revision, state_json, updated_at) \
                 VALUES (?, ?, ?, 1, ?, ?) \
                 ON CONFLICT(tenant, agent, scope) DO UPDATE SET \
                 revision = contexts.revision + 1, state_json = excluded.state_json, \
                 updated_at = excluded.updated_at",
            )
            .bind(&tenant)
            .bind(&agent)
            .bind(&scope)
            .bind(serde_json::to_string(state)?)
            .bind(&now)
            .execute(&mut tx)
            .await?;
        } else {
            db::query("DELETE FROM contexts WHERE tenant = ? AND agent = ? AND scope = ?")
                .bind(&tenant)
                .bind(&agent)
                .bind(&scope)
                .execute(&mut tx)
                .await?;
        }

        if let Some(destination) = delivery_destination {
            db::query(
                r#"INSERT INTO deliveries (
                       delivery_id, tenant, run_id, status, destination,
                       payload_json, idempotency_key, attempt, created_at, updated_at
                   ) VALUES (?, ?, ?, 'pending', ?, ?, ?, 0, ?, ?)
                   ON CONFLICT(idempotency_key) DO NOTHING"#,
            )
            .bind(Uuid::new_v4().to_string())
            .bind(&tenant)
            .bind(run_id.to_string())
            .bind(destination)
            .bind(&output_json)
            .bind(format!("run:{run_id}:output"))
            .bind(&now)
            .bind(&now)
            .execute(&mut tx)
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    /// Atomically persist a terminal failure. Partial model/tool trace remains
    /// intact, context is unchanged, and an explicit destination receives one
    /// transport-neutral failure payload.
    pub async fn fail_run(&self, run_id: Uuid, error: &str) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        let row = db::query("SELECT tenant, delivery_destination FROM runs WHERE run_id = ?")
            .bind(run_id.to_string())
            .fetch_optional(&mut tx)
            .await?;
        let now = Utc::now().to_rfc3339();
        let changed = db::query(
            "UPDATE runs SET status = 'failed', error = ?, updated_at = ? \
             WHERE run_id = ? AND status IN ('queued', 'running')",
        )
        .bind(error)
        .bind(&now)
        .bind(run_id.to_string())
        .execute(&mut tx)
        .await?
        .rows_affected();
        if changed > 0 {
            db::query(
                "INSERT INTO run_log (run_id, kind, payload_json, ts) VALUES (?, 'error', ?, ?)",
            )
            .bind(run_id.to_string())
            .bind(serde_json::json!({"error":error}).to_string())
            .bind(&now)
            .execute(&mut tx)
            .await?;
            if let Some(row) = row {
                let tenant = row.try_get::<String, _>("tenant")?;
                let destination = row.try_get::<Option<String>, _>("delivery_destination")?;
                if let Some(destination) = destination {
                    let payload = failure_delivery_payload(error).to_string();
                    db::query(
                        r#"INSERT INTO deliveries (
                               delivery_id, tenant, run_id, status, destination,
                               payload_json, idempotency_key, attempt, created_at, updated_at
                           ) VALUES (?, ?, ?, 'pending', ?, ?, ?, 0, ?, ?)
                           ON CONFLICT(idempotency_key) DO NOTHING"#,
                    )
                    .bind(Uuid::new_v4().to_string())
                    .bind(tenant)
                    .bind(run_id.to_string())
                    .bind(destination)
                    .bind(payload)
                    .bind(format!("run:{run_id}:failure"))
                    .bind(&now)
                    .bind(&now)
                    .execute(&mut tx)
                    .await?;
                }
            }
        }
        tx.commit().await?;
        Ok(())
    }

    pub async fn list_run_log(&self, run_id: Uuid) -> Result<Vec<RunLogEntry>> {
        let rows = db::query(
            "SELECT id, run_id, kind, payload_json, ts \
             FROM run_log WHERE run_id = ? ORDER BY id ASC",
        )
        .bind(run_id.to_string())
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(|row| {
                Ok(RunLogEntry {
                    id: row.try_get("id")?,
                    run_id: parse_uuid_field(&row, "run_id")?,
                    kind: row.try_get("kind")?,
                    payload: serde_json::from_str(&row.try_get::<String, _>("payload_json")?)?,
                    ts: parse_ts_field(&row, "ts")?,
                })
            })
            .collect()
    }

    pub async fn put_artifact(
        &self,
        tenant: &str,
        path: &str,
        body: &[u8],
        content_type: &str,
        meta_json: Option<&str>,
    ) -> Result<()> {
        self.ensure_tenant_exists(tenant).await?;
        let now = Utc::now().to_rfc3339();
        db::query(
            r#"INSERT INTO artifacts (tenant, path, body, content_type, meta_json, updated_at)
               VALUES (?, ?, ?, ?, ?, ?)
               ON CONFLICT(tenant, path) DO UPDATE SET
                 body=excluded.body,
                 content_type=excluded.content_type,
                 meta_json=excluded.meta_json,
                 updated_at=excluded.updated_at"#,
        )
        .bind(tenant)
        .bind(path)
        .bind(body)
        .bind(content_type)
        .bind(meta_json)
        .bind(&now)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn delete_artifact(&self, tenant: &str, path: &str) -> Result<()> {
        db::query("DELETE FROM artifacts WHERE tenant = ? AND path = ?")
            .bind(tenant)
            .bind(path)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn get_artifact(
        &self,
        tenant: &str,
        path: &str,
    ) -> Result<Option<(Vec<u8>, Option<String>, Option<String>)>> {
        let row = db::query(
            "SELECT body, content_type, meta_json FROM artifacts WHERE tenant = ? AND path = ?",
        )
        .bind(tenant)
        .bind(path)
        .fetch_optional(&self.pool)
        .await?;
        match row {
            Some(row) => {
                let body = row.try_get::<Vec<u8>, _>("body")?;
                let content_type = row.try_get::<Option<String>, _>("content_type")?;
                let meta_json = row.try_get::<Option<String>, _>("meta_json")?;
                Ok(Some((body, content_type, meta_json)))
            }
            None => Ok(None),
        }
    }

    pub async fn list_artifacts(
        &self,
        tenant: &str,
        prefix: Option<&str>,
    ) -> Result<Vec<agentd_api::ArtifactStat>> {
        Ok(self
            .list_artifacts_page(tenant, prefix, None, usize::MAX)
            .await?
            .items)
    }

    pub async fn list_artifacts_page(
        &self,
        tenant: &str,
        prefix: Option<&str>,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<ArtifactListPage> {
        let limit = limit.clamp(1, 500);
        let fetch_limit = limit + 1;
        let cursor = cursor.map(str::trim).filter(|value| !value.is_empty());
        let rows = match (prefix, cursor) {
            (Some(prefix), Some(cursor)) if !prefix.is_empty() => {
                db::query(
                    "SELECT path, content_type, meta_json, updated_at FROM artifacts \
                     WHERE tenant = ? AND path LIKE ? AND path > ? ORDER BY path ASC LIMIT ?",
                )
                .bind(tenant)
                .bind(format!("{}%", prefix.trim_start_matches('/')))
                .bind(cursor.trim_start_matches('/'))
                .bind(fetch_limit as i64)
                .fetch_all(&self.pool)
                .await?
            }
            (Some(prefix), _) if !prefix.is_empty() => {
                db::query(
                    "SELECT path, content_type, meta_json, updated_at FROM artifacts \
                     WHERE tenant = ? AND path LIKE ? ORDER BY path ASC LIMIT ?",
                )
                .bind(tenant)
                .bind(format!("{}%", prefix.trim_start_matches('/')))
                .bind(fetch_limit as i64)
                .fetch_all(&self.pool)
                .await?
            }
            (_, Some(cursor)) => {
                db::query(
                    "SELECT path, content_type, meta_json, updated_at FROM artifacts \
                     WHERE tenant = ? AND path > ? ORDER BY path ASC LIMIT ?",
                )
                .bind(tenant)
                .bind(cursor.trim_start_matches('/'))
                .bind(fetch_limit as i64)
                .fetch_all(&self.pool)
                .await?
            }
            _ => {
                db::query(
                    "SELECT path, content_type, meta_json, updated_at FROM artifacts \
                     WHERE tenant = ? ORDER BY path ASC LIMIT ?",
                )
                .bind(tenant)
                .bind(fetch_limit as i64)
                .fetch_all(&self.pool)
                .await?
            }
        };
        let mut items = rows
            .into_iter()
            .map(|row| row_to_artifact_stat(tenant, row))
            .collect::<Result<Vec<_>>>()?;
        let next_cursor = if items.len() > limit {
            items.pop().map(|item| item.path)
        } else {
            None
        };
        Ok(ArtifactListPage { items, next_cursor })
    }

    pub async fn get_artifact_stat(
        &self,
        tenant: &str,
        path: &str,
    ) -> Result<Option<agentd_api::ArtifactStat>> {
        let row = db::query(
            "SELECT path, content_type, meta_json, updated_at FROM artifacts \
             WHERE tenant = ? AND path = ?",
        )
        .bind(tenant)
        .bind(path.trim_start_matches('/'))
        .fetch_optional(&self.pool)
        .await?;
        row.map(|row| row_to_artifact_stat(tenant, row)).transpose()
    }

    pub async fn trigger_due_schedules(
        &self,
        now: DateTime<Utc>,
        limit: usize,
    ) -> Result<Vec<Uuid>> {
        let rows = db::query(
            "SELECT tenant, name, spec_json FROM schedules WHERE next_trigger_at IS NOT NULL AND next_trigger_at <= ? ORDER BY next_trigger_at ASC LIMIT ?",
        )
        .bind(now.to_rfc3339())
        .bind(limit.max(1) as i64)
        .fetch_all(&self.pool)
        .await?;

        let mut triggered = Vec::new();
        for row in rows {
            let tenant = row.try_get::<String, _>("tenant")?;
            let name = row.try_get::<String, _>("name")?;
            let spec: ScheduleSpec = serde_json::from_str(&row.try_get::<String, _>("spec_json")?)?;
            let run_name = format!("{}-{}", name, Uuid::new_v4().simple());
            let run_id = self
                .submit_schedule_run(&tenant, &name, &spec, &run_name, now)
                .await?;
            let next_trigger = next_trigger_time(&spec, now + chrono::Duration::seconds(1))?;
            db::query(
                "UPDATE schedules SET last_triggered_at = ?, next_trigger_at = ?, last_run_id = ?, updated_at = ? WHERE tenant = ? AND name = ?",
            )
            .bind(now.to_rfc3339())
            .bind(next_trigger.map(|value| value.to_rfc3339()))
            .bind(run_id.to_string())
            .bind(now.to_rfc3339())
            .bind(&tenant)
            .bind(&name)
            .execute(&self.pool)
            .await?;
            triggered.push(run_id);
        }
        Ok(triggered)
    }

    async fn submit_schedule_run(
        &self,
        tenant: &str,
        schedule_name: &str,
        spec: &ScheduleSpec,
        run_name: &str,
        now: DateTime<Utc>,
    ) -> Result<Uuid> {
        let input = json!({
            "activation": "schedule",
            "schedule_name": schedule_name,
            "input": spec.payload,
            "triggered_at": now.to_rfc3339(),
        });
        let run_id = self
            .submit_run(NewRun {
                tenant,
                name: run_name,
                agent_ref: &spec.agent_ref,
                scope: &spec.scope,
                source: "schedule",
                input: &input,
                request_id: None,
                schedule_name: Some(schedule_name),
                delivery_destination: spec
                    .delivery
                    .as_ref()
                    .map(|delivery| delivery.destination.as_str()),
            })
            .await?;
        self.append_event(
            run_id,
            "status",
            json!({ "status": "queued", "source": "schedule" }),
            now,
        )
        .await?;
        Ok(run_id)
    }

    pub async fn delete_agent(&self, tenant: &str, name: &str) -> Result<bool> {
        let result = db::query("DELETE FROM agents WHERE tenant = ? AND name = ?")
            .bind(tenant)
            .bind(name)
            .execute(&self.pool)
            .await?;
        Ok(result.rows_affected() > 0)
    }

    pub async fn delete_tenant(&self, tenant: &str) -> Result<serde_json::Value> {
        if tenant == "system" {
            return Err(anyhow!("cannot delete the system tenant"));
        }
        let mut tx = self.pool.begin().await?;
        db::query("DELETE FROM run_log WHERE run_id IN (SELECT run_id FROM runs WHERE tenant = ?)")
            .bind(tenant)
            .execute(&mut tx)
            .await?;
        for table in [
            "deliveries",
            "runs",
            "contexts",
            "artifacts",
            "edges",
            "entities",
            "memory",
            "schedules",
            "mcp_servers",
            "agents",
        ] {
            db::query(&format!("DELETE FROM {table} WHERE tenant = ?"))
                .bind(tenant)
                .execute(&mut tx)
                .await?;
        }
        let deleted = db::query("DELETE FROM tenants WHERE name = ?")
            .bind(tenant)
            .execute(&mut tx)
            .await?
            .rows_affected();
        tx.commit().await?;
        Ok(json!({ "tenant": tenant, "deleted": deleted > 0 }))
    }
}

fn next_trigger_time(spec: &ScheduleSpec, after: DateTime<Utc>) -> Result<Option<DateTime<Utc>>> {
    if !spec.enabled {
        return Ok(None);
    }
    if let Some(at) = spec.at {
        if at >= after {
            return Ok(Some(at));
        }
        return Ok(None);
    }
    if let Some(cron) = spec.cron.as_deref() {
        let timezone = schedule_timezone(spec)?;
        return next_cron_occurrence(cron, timezone, after);
    }
    Ok(None)
}

enum ScheduleTimezone {
    Named(Tz),
    Fixed(FixedOffset),
}

impl ScheduleTimezone {
    fn local_after(&self, after: DateTime<Utc>) -> ScheduleLocalAfter {
        match self {
            Self::Named(tz) => ScheduleLocalAfter::Named(after.with_timezone(tz)),
            Self::Fixed(offset) => ScheduleLocalAfter::Fixed(after.with_timezone(offset)),
        }
    }
}

enum ScheduleLocalAfter {
    Named(DateTime<Tz>),
    Fixed(DateTime<FixedOffset>),
}

fn schedule_timezone(spec: &ScheduleSpec) -> Result<ScheduleTimezone> {
    let timezone = spec
        .timezone
        .as_deref()
        .ok_or_else(|| anyhow!("schedule timezone is required"))?;
    validate_timezone_name(timezone)?;
    if let Ok(tz) = timezone.parse::<Tz>() {
        return Ok(ScheduleTimezone::Named(tz));
    }
    let offset = parse_timezone_offset(timezone)
        .and_then(FixedOffset::east_opt)
        .ok_or_else(|| anyhow!("invalid fixed offset timezone: {timezone}"))?;
    Ok(ScheduleTimezone::Fixed(offset))
}

fn next_cron_occurrence(
    expr: &str,
    timezone: ScheduleTimezone,
    after: DateTime<Utc>,
) -> Result<Option<DateTime<Utc>>> {
    validate_cron_expression(expr)?;
    let cron = Cron::from_str(expr)?;
    let next = match timezone.local_after(after) {
        ScheduleLocalAfter::Named(local_after) => cron
            .find_next_occurrence(&local_after, false)?
            .with_timezone(&Utc),
        ScheduleLocalAfter::Fixed(local_after) => cron
            .find_next_occurrence(&local_after, false)?
            .with_timezone(&Utc),
    };
    Ok(Some(next))
}

fn status_to_wire(status: AgentRunStatus) -> &'static str {
    match status {
        AgentRunStatus::Queued => "queued",
        AgentRunStatus::Running => "running",
        AgentRunStatus::Succeeded => "succeeded",
        AgentRunStatus::Failed => "failed",
        AgentRunStatus::Cancelled => "cancelled",
    }
}

fn status_from_wire(raw: &str) -> Result<AgentRunStatus> {
    match raw {
        "queued" => Ok(AgentRunStatus::Queued),
        "running" => Ok(AgentRunStatus::Running),
        "succeeded" => Ok(AgentRunStatus::Succeeded),
        "failed" => Ok(AgentRunStatus::Failed),
        "cancelled" => Ok(AgentRunStatus::Cancelled),
        other => Err(anyhow!("unknown status {other}")),
    }
}

fn normalize_tenant_name(name: &str) -> Result<String> {
    let name = name.trim();
    if name.is_empty() {
        return Err(anyhow!("tenant name is required"));
    }
    Ok(name.to_string())
}

fn normalize_memory_component(value: &str, field: &str) -> Result<String> {
    let value = value.trim();
    if value.is_empty() {
        return Err(anyhow!("memory {field} is required"));
    }
    Ok(value.to_string())
}

fn normalize_graph_component(value: &str, field: &str, max_bytes: usize) -> Result<String> {
    let value = value.trim();
    if value.is_empty() {
        return Err(anyhow!("graph {field} is required"));
    }
    if value.len() > max_bytes {
        return Err(anyhow!("graph {field} exceeds {max_bytes} UTF-8 bytes"));
    }
    if value.chars().any(char::is_control) {
        return Err(anyhow!("graph {field} contains a control character"));
    }
    Ok(value.to_string())
}

fn normalize_graph_properties(
    properties: &serde_json::Value,
    field: &str,
) -> Result<serde_json::Value> {
    if !properties.is_object() {
        return Err(anyhow!("graph {field} properties must be an object"));
    }
    let encoded = serde_json::to_vec(properties)?;
    if encoded.len() > MAX_GRAPH_PROPERTIES_BYTES {
        return Err(anyhow!(
            "graph {field} properties exceed {MAX_GRAPH_PROPERTIES_BYTES} JSON bytes"
        ));
    }
    Ok(properties.clone())
}

fn normalize_memory_graph(graph: &MemoryGraphInput) -> Result<MemoryGraphInput> {
    if graph.entities.len() > MAX_GRAPH_ENTITIES_PER_MEMORY {
        return Err(anyhow!(
            "memory graph exceeds {MAX_GRAPH_ENTITIES_PER_MEMORY} entities"
        ));
    }
    if graph.edges.len() > MAX_GRAPH_EDGES_PER_MEMORY {
        return Err(anyhow!(
            "memory graph exceeds {MAX_GRAPH_EDGES_PER_MEMORY} edges"
        ));
    }

    let mut entity_ids = BTreeSet::new();
    let mut entities = Vec::with_capacity(graph.entities.len());
    for entity in &graph.entities {
        let id = normalize_graph_component(&entity.id, "entity id", MAX_GRAPH_ID_BYTES)?;
        if !entity_ids.insert(id.clone()) {
            return Err(anyhow!("memory graph contains duplicate entity id {id}"));
        }
        let label =
            normalize_graph_component(&entity.label, "entity label", MAX_GRAPH_LABEL_BYTES)?;
        let kind = entity
            .kind
            .as_deref()
            .map(|value| normalize_graph_component(value, "entity type", MAX_GRAPH_ID_BYTES))
            .transpose()?;
        entities.push(GraphEntityInput {
            id,
            label,
            kind,
            properties: normalize_graph_properties(&entity.properties, "entity")?,
        });
    }

    let mut edge_keys = BTreeSet::new();
    let mut edges = Vec::with_capacity(graph.edges.len());
    for edge in &graph.edges {
        let from = normalize_graph_component(&edge.from, "edge from", MAX_GRAPH_ID_BYTES)?;
        let relation =
            normalize_graph_component(&edge.relation, "edge relation", MAX_GRAPH_RELATION_BYTES)?;
        let to = normalize_graph_component(&edge.to, "edge to", MAX_GRAPH_ID_BYTES)?;
        if !entity_ids.contains(&from) || !entity_ids.contains(&to) {
            return Err(anyhow!(
                "memory graph edge {from} -[{relation}]-> {to} must reference entities in the same memory graph"
            ));
        }
        if !edge_keys.insert((from.clone(), relation.clone(), to.clone())) {
            return Err(anyhow!(
                "memory graph contains duplicate edge {from} -[{relation}]-> {to}"
            ));
        }
        edges.push(GraphEdgeInput {
            from,
            relation,
            to,
            properties: normalize_graph_properties(&edge.properties, "edge")?,
        });
    }
    Ok(MemoryGraphInput { entities, edges })
}

fn memory_fts_query(query: &str) -> Option<String> {
    let terms = query
        .split(|character: char| !character.is_alphanumeric())
        .filter(|term| !term.is_empty())
        .map(|term| format!("\"{term}\""))
        .collect::<Vec<_>>();
    (!terms.is_empty()).then(|| terms.join(" OR "))
}

fn memory_item_from_row(row: db::SqlRow, score: Option<f64>) -> Result<MemoryItem> {
    Ok(MemoryItem {
        tenant: row.try_get("tenant")?,
        namespace: row.try_get("namespace")?,
        id: row.try_get("id")?,
        text: row.try_get("text")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
        score,
    })
}

fn validate_memory_embedding(embedding: &[f32]) -> Result<()> {
    if embedding.len() != MEMORY_EMBEDDING_DIM {
        return Err(anyhow!(
            "memory embedding must contain exactly {MEMORY_EMBEDDING_DIM} dimensions, got {}",
            embedding.len()
        ));
    }
    if embedding.iter().any(|value| !value.is_finite()) {
        return Err(anyhow!("memory embedding contains a non-finite value"));
    }
    Ok(())
}

fn encode_memory_embedding(embedding: &[f32]) -> Vec<u8> {
    embedding
        .iter()
        .flat_map(|value| value.to_le_bytes())
        .collect()
}

fn decode_memory_embedding(bytes: &[u8]) -> Result<Vec<f32>> {
    if bytes.is_empty() || !bytes.len().is_multiple_of(std::mem::size_of::<f32>()) {
        return Err(memory_embedding_reset_error(format!(
            "stored vector has invalid byte length {}",
            bytes.len()
        )));
    }
    let embedding = bytes
        .chunks_exact(4)
        .map(|chunk| f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]))
        .collect::<Vec<_>>();
    validate_memory_embedding(&embedding)
        .map_err(|error| memory_embedding_reset_error(error.to_string()))?;
    Ok(embedding)
}

fn cosine_similarity(left: &[f32], right: &[f32]) -> f64 {
    let mut dot = 0.0_f64;
    let mut left_norm = 0.0_f64;
    let mut right_norm = 0.0_f64;
    for (left, right) in left.iter().zip(right) {
        let left = f64::from(*left);
        let right = f64::from(*right);
        dot += left * right;
        left_norm += left * left;
        right_norm += right * right;
    }
    if left_norm == 0.0 || right_norm == 0.0 {
        0.0
    } else {
        dot / (left_norm.sqrt() * right_norm.sqrt())
    }
}

fn fuse_memory_candidates(
    lexical: Vec<MemoryItem>,
    semantic: Vec<MemoryItem>,
    limit: usize,
) -> Vec<MemoryItem> {
    const RRF_K: f64 = 60.0;
    let mut fused: BTreeMap<String, (MemoryItem, f64)> = BTreeMap::new();
    for candidates in [lexical, semantic] {
        for (offset, item) in candidates.into_iter().enumerate() {
            let contribution = 1.0 / (RRF_K + (offset + 1) as f64);
            let entry = fused.entry(item.id.clone()).or_insert((item, 0.0));
            entry.1 += contribution;
        }
    }
    let mut fused = fused
        .into_values()
        .map(|(mut item, score)| {
            item.score = Some(score);
            item
        })
        .collect::<Vec<_>>();
    fused.sort_by(|left, right| {
        right
            .score
            .unwrap_or_default()
            .total_cmp(&left.score.unwrap_or_default())
            .then_with(|| left.id.cmp(&right.id))
    });
    fused.truncate(limit);
    fused
}

fn memory_embedding_reset_error(detail: impl std::fmt::Display) -> anyhow::Error {
    anyhow!("memory embedding is incompatible ({detail}); restart agentd with --reset-data")
}

fn row_to_tenant_record(row: db::SqlRow) -> Result<TenantRecord> {
    let metadata_json = row.try_get::<String, _>("metadata_json")?;
    Ok(TenantRecord {
        name: row.try_get::<String, _>("name")?,
        metadata: serde_json::from_str(&metadata_json)?,
        created_at: row.try_get::<String, _>("created_at")?,
        updated_at: row.try_get::<String, _>("updated_at")?,
    })
}

fn row_to_agent(row: db::SqlRow) -> Result<Agent> {
    let metadata: agentd_api::ResourceMeta =
        serde_json::from_str(&row.try_get::<String, _>("metadata_json")?)?;
    let spec: agentd_api::AgentSpec =
        serde_json::from_str(&row.try_get::<String, _>("spec_json")?)?;
    Ok(Agent {
        tenant: metadata.tenant.clone(),
        name: metadata.name.clone(),
        metadata,
        spec,
        created_at: parse_ts_field(&row, "created_at")?,
        updated_at: parse_ts_field(&row, "updated_at")?,
    })
}

fn row_to_mcp_server(row: db::SqlRow) -> Result<McpServer> {
    Ok(McpServer {
        tenant: row.try_get("tenant")?,
        name: row.try_get("name")?,
        spec: serde_json::from_str(&row.try_get::<String, _>("spec_json")?)?,
        tools: serde_json::from_str(&row.try_get::<String, _>("tools_json")?)?,
        last_error: row.try_get("last_error")?,
        created_at: parse_ts_field(&row, "created_at")?,
        updated_at: parse_ts_field(&row, "updated_at")?,
    })
}

fn mcp_exposed_name(server_name: &str, tool_name: &str) -> String {
    truncate_tool_name(&format!(
        "mcp_{}_{}",
        sanitize_tool_name_part(server_name),
        sanitize_tool_name_part(tool_name)
    ))
}

fn truncate_tool_name(raw: &str) -> String {
    if raw.len() <= 64 {
        return raw.to_string();
    }
    let digest = hex_encode(Sha256::digest(raw.as_bytes()));
    let suffix = format!("_{}", &digest[..8]);
    let head = truncate_ascii(raw, 64 - suffix.len());
    format!("{head}{suffix}")
}

fn truncate_ascii(raw: &str, max_len: usize) -> String {
    raw.chars().take(max_len).collect()
}

fn sanitize_tool_name_part(raw: &str) -> String {
    let mut out = raw
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '_' || ch == '-' {
                ch
            } else {
                '_'
            }
        })
        .collect::<String>();
    while out.contains("__") {
        out = out.replace("__", "_");
    }
    let mut out = out.trim_matches('_').to_string();
    if out.is_empty() {
        out.push_str("tool");
    }
    if out == raw {
        out
    } else {
        let digest = hex_encode(Sha256::digest(raw.as_bytes()));
        format!("{out}_{}", &digest[..8])
    }
}

fn row_to_schedule(row: db::SqlRow) -> Result<Schedule> {
    let spec: ScheduleSpec = serde_json::from_str(&row.try_get::<String, _>("spec_json")?)?;
    Ok(Schedule {
        tenant: row.try_get("tenant")?,
        name: row.try_get("name")?,
        spec,
        last_triggered_at: optional_ts_field(&row, "last_triggered_at")?,
        next_trigger_at: optional_ts_field(&row, "next_trigger_at")?,
        last_run_id: optional_uuid_field(&row, "last_run_id")?,
        created_at: parse_ts_field(&row, "created_at")?,
        updated_at: parse_ts_field(&row, "updated_at")?,
    })
}

fn row_to_artifact_stat(tenant: &str, row: db::SqlRow) -> Result<agentd_api::ArtifactStat> {
    let path = row.try_get::<String, _>("path")?;
    let content_type = row.try_get::<Option<String>, _>("content_type")?;
    let updated_at = row.try_get::<String, _>("updated_at")?;
    let meta: Option<serde_json::Value> = row
        .try_get::<Option<String>, _>("meta_json")?
        .map(|raw| serde_json::from_str(&raw))
        .transpose()?;
    let size_bytes = meta
        .as_ref()
        .and_then(|meta| meta.get("size_bytes"))
        .and_then(serde_json::Value::as_u64);
    Ok(agentd_api::ArtifactStat {
        artifact_ref: format!("artifact://{tenant}/{}", path.trim_start_matches('/')),
        path,
        content_type,
        sha256: meta
            .as_ref()
            .and_then(|meta| meta.get("sha256"))
            .and_then(serde_json::Value::as_str)
            .map(ToString::to_string),
        size_bytes,
        metadata: meta,
        updated_at: Some(updated_at),
    })
}

fn row_to_run(row: db::SqlRow) -> Result<AgentRun> {
    Ok(AgentRun {
        run_id: parse_uuid_field(&row, "run_id")?,
        tenant: row.try_get("tenant")?,
        name: row.try_get("name")?,
        agent_ref: row.try_get("agent_ref")?,
        scope: row.try_get("scope")?,
        source: row.try_get("source")?,
        input: serde_json::from_str(&row.try_get::<String, _>("input_json")?)?,
        output: row
            .try_get::<Option<String>, _>("output_json")?
            .map(|raw| serde_json::from_str(&raw))
            .transpose()?,
        error: row.try_get("error")?,
        status: status_from_wire(&row.try_get::<String, _>("status")?)?,
        request_id: row.try_get("request_id")?,
        created_at: parse_ts_field(&row, "created_at")?,
        started_at: optional_ts_field(&row, "started_at")?,
        updated_at: parse_ts_field(&row, "updated_at")?,
    })
}

fn row_to_delivery_outbox(row: db::SqlRow) -> Result<DeliveryOutboxRecord> {
    Ok(DeliveryOutboxRecord {
        delivery_id: parse_uuid_field(&row, "delivery_id")?,
        tenant: row.try_get("tenant")?,
        run_id: parse_uuid_field(&row, "run_id")?,
        status: row.try_get("status")?,
        destination: row.try_get("destination")?,
        payload: serde_json::from_str(&row.try_get::<String, _>("payload_json")?)?,
        attempt: row.try_get::<i64, _>("attempt")? as u32,
        next_attempt_at: optional_ts_field(&row, "next_attempt_at")?,
        last_error: row.try_get("last_error")?,
        claim_token: row.try_get("claim_token")?,
        claim_expires_at: optional_ts_field(&row, "claim_expires_at")?,
        created_at: parse_ts_field(&row, "created_at")?,
        updated_at: parse_ts_field(&row, "updated_at")?,
    })
}

fn failure_delivery_payload(error: &str) -> serde_json::Value {
    if error == "run timeout exceeded" {
        serde_json::json!({
            "reply":"Sorry, this request took too long to complete. Please try again.",
            "error":{"code":"run_timeout"}
        })
    } else {
        serde_json::json!({
            "reply":"Sorry, this request could not be completed. Please try again.",
            "error":{"code":"run_failed"}
        })
    }
}

fn parse_ts_field(row: &db::SqlRow, name: &str) -> Result<DateTime<Utc>> {
    Ok(DateTime::parse_from_rfc3339(&row.try_get::<String, _>(name)?)?.with_timezone(&Utc))
}

fn optional_ts_field(row: &db::SqlRow, name: &str) -> Result<Option<DateTime<Utc>>> {
    row.try_get::<Option<String>, _>(name)?
        .map(|raw| Ok(DateTime::parse_from_rfc3339(&raw)?.with_timezone(&Utc)))
        .transpose()
}

fn parse_uuid_field(row: &db::SqlRow, name: &str) -> Result<Uuid> {
    Ok(Uuid::parse_str(&row.try_get::<String, _>(name)?)?)
}

fn optional_uuid_field(row: &db::SqlRow, name: &str) -> Result<Option<Uuid>> {
    row.try_get::<Option<String>, _>(name)?
        .map(|raw| Ok(Uuid::parse_str(&raw)?))
        .transpose()
}

async fn connect_libsql_database(database_path: &str) -> Result<LibsqlPool> {
    LibsqlPool::open(database_path)
        .await
        .with_context(|| format!("failed to open libSQL database at {database_path}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use agentd_api::{AgentLimits, AgentSpec, McpServerSpec, McpTool, McpTransport, ResourceMeta};
    use tempfile::TempDir;

    async fn store() -> (TempDir, AgentdStore) {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("agentd.db");
        let store = AgentdStore::new(path.to_str().unwrap()).await.unwrap();
        (dir, store)
    }

    fn test_embedding(axis: usize) -> Vec<f32> {
        let mut embedding = vec![0.0; MEMORY_EMBEDDING_DIM];
        embedding[axis] = 1.0;
        embedding
    }

    fn graph(value: serde_json::Value) -> MemoryGraphInput {
        serde_json::from_value(value).unwrap()
    }

    async fn tenant_with_agent(store: &AgentdStore, tenant: &str) {
        store.create_tenant(tenant, &json!({})).await.unwrap();
        store
            .apply_agent(&AgentResource {
                metadata: ResourceMeta {
                    name: "bot".into(),
                    tenant: tenant.into(),
                    labels: BTreeMap::new(),
                },
                spec: AgentSpec {
                    allowed_families: None,
                    limits: AgentLimits {
                        timeout_ms: 10_000,
                        max_steps: 8,
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
    }

    async fn submit(
        store: &AgentdStore,
        tenant: &str,
        scope: &str,
        request_id: Option<&str>,
    ) -> Uuid {
        submit_with_delivery(store, tenant, scope, request_id, None).await
    }

    async fn submit_with_delivery(
        store: &AgentdStore,
        tenant: &str,
        scope: &str,
        request_id: Option<&str>,
        delivery_destination: Option<&str>,
    ) -> Uuid {
        store
            .submit_run(NewRun {
                tenant,
                name: "turn",
                agent_ref: "bot",
                scope,
                source: "test",
                input: &json!({"text":"test"}),
                request_id,
                schedule_name: None,
                delivery_destination,
            })
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn tenant_scoped_request_ids_are_idempotent() {
        let (_dir, store) = store().await;
        tenant_with_agent(&store, "one").await;
        tenant_with_agent(&store, "two").await;

        let first = submit(&store, "one", "scope", Some("request-1")).await;
        let duplicate = submit(&store, "one", "other", Some("request-1")).await;
        let other_tenant = submit(&store, "two", "scope", Some("request-1")).await;

        assert_eq!(first, duplicate);
        assert_ne!(first, other_tenant);
        assert!(store
            .get_run(first)
            .await
            .unwrap()
            .is_some_and(|run| run.tenant == "one"));
    }

    #[tokio::test]
    async fn same_scope_serializes_while_other_scope_can_run() {
        let (_dir, store) = store().await;
        tenant_with_agent(&store, "demo").await;
        let first = submit(&store, "demo", "same", None).await;
        let second = submit(&store, "demo", "same", None).await;
        let parallel = submit(&store, "demo", "other", None).await;

        assert_eq!(
            store.claim_next_run().await.unwrap().unwrap().run.run_id,
            first
        );
        assert_eq!(
            store.claim_next_run().await.unwrap().unwrap().run.run_id,
            parallel
        );
        assert!(store.claim_next_run().await.unwrap().is_none());

        store
            .finalize_run_success(first, &json!({"done":true}), None)
            .await
            .unwrap();
        assert_eq!(
            store.claim_next_run().await.unwrap().unwrap().run.run_id,
            second
        );
    }

    #[tokio::test]
    async fn missing_agent_fails_queued_run_without_stranding_its_lane() {
        let (_dir, store) = store().await;
        tenant_with_agent(&store, "demo").await;
        let broken = submit(&store, "demo", "same", None).await;
        assert!(store.delete_agent("demo", "bot").await.unwrap());

        assert!(store.claim_next_run().await.unwrap().is_none());
        let run = store.get_run(broken).await.unwrap().unwrap();
        assert_eq!(run.status, AgentRunStatus::Failed);
        assert!(run
            .error
            .as_deref()
            .is_some_and(|error| error.contains("agent bot not found")));

        tenant_with_agent(&store, "demo").await;
        let healthy = submit(&store, "demo", "same", None).await;
        assert_eq!(
            store.claim_next_run().await.unwrap().unwrap().run.run_id,
            healthy
        );
    }

    #[tokio::test]
    async fn final_output_trace_and_delivery_commit_together() {
        let (_dir, store) = store().await;
        tenant_with_agent(&store, "demo").await;
        let run_id = submit_with_delivery(&store, "demo", "chat:42", None, Some("tg:42")).await;
        let output = json!({"reply":"hello"});
        store.claim_next_run().await.unwrap().unwrap();

        store
            .finalize_run_success(run_id, &output, None)
            .await
            .unwrap();
        assert!(store
            .finalize_run_success(run_id, &json!({"reply":"duplicate"}), None)
            .await
            .is_err());

        assert_eq!(store.get_run_output(run_id).await.unwrap(), Some(output));
        let trace = store.list_run_log(run_id).await.unwrap();
        assert_eq!(trace.len(), 2);
        assert_eq!(trace[0].kind, "output");
        let deliveries = store
            .list_delivery_outbox(Some("demo"), None, Some(run_id), 10)
            .await
            .unwrap();
        assert_eq!(deliveries.len(), 1);
        assert_eq!(deliveries[0].status, "pending");
        assert_eq!(deliveries[0].destination, "tg:42");
        assert_eq!(deliveries[0].payload, json!({"reply":"hello"}));
    }

    #[tokio::test]
    async fn successful_run_without_delivery_stays_pull_only() {
        let (_dir, store) = store().await;
        tenant_with_agent(&store, "demo").await;
        let run_id = submit(&store, "demo", "browser:42", None).await;
        store.claim_next_run().await.unwrap().unwrap();
        store
            .finalize_run_success(run_id, &json!({"reply":"hello"}), None)
            .await
            .unwrap();

        assert_eq!(
            store.get_run_output(run_id).await.unwrap(),
            Some(json!({"reply":"hello"}))
        );
        assert!(store
            .list_delivery_outbox(Some("demo"), None, Some(run_id), 10)
            .await
            .unwrap()
            .is_empty());
    }

    #[tokio::test]
    async fn failed_run_enqueues_one_immutable_failure_delivery() {
        let (_dir, store) = store().await;
        tenant_with_agent(&store, "demo").await;
        let run_id = submit_with_delivery(&store, "demo", "chat:42", None, Some("tg:42")).await;
        store.claim_next_run().await.unwrap().unwrap();

        store
            .fail_run(run_id, "run timeout exceeded")
            .await
            .unwrap();
        store.fail_run(run_id, "duplicate failure").await.unwrap();

        let run = store.get_run(run_id).await.unwrap().unwrap();
        assert_eq!(run.status, AgentRunStatus::Failed);
        assert_eq!(run.output, None);
        let deliveries = store
            .list_delivery_outbox(Some("demo"), None, Some(run_id), 10)
            .await
            .unwrap();
        assert_eq!(deliveries.len(), 1);
        assert_eq!(deliveries[0].status, "pending");
        assert_eq!(deliveries[0].destination, "tg:42");
        assert_eq!(deliveries[0].payload["error"]["code"], "run_timeout");
        assert!(deliveries[0].payload["reply"]
            .as_str()
            .is_some_and(|reply| reply.contains("too long")));
    }

    #[tokio::test]
    async fn cancelled_run_cannot_commit_output_context_or_delivery() {
        let (_dir, store) = store().await;
        tenant_with_agent(&store, "demo").await;
        let run_id = submit_with_delivery(&store, "demo", "chat:42", None, Some("tg:42")).await;
        store.claim_next_run().await.unwrap().unwrap();
        store.cancel_run_request(run_id, "cancelled").await.unwrap();

        assert!(store
            .finalize_run_success(
                run_id,
                &json!({"reply":"too late"}),
                Some(&json!({"messages":[]})),
            )
            .await
            .is_err());
        assert_eq!(store.get_run_output(run_id).await.unwrap(), None);
        assert!(store
            .get_context_state("demo", "bot", "chat:42")
            .await
            .unwrap()
            .is_none());
        assert!(store
            .list_delivery_outbox(Some("demo"), None, Some(run_id), 10)
            .await
            .unwrap()
            .is_empty());
    }

    #[tokio::test]
    async fn expired_claim_is_reissued_and_retry_updates_one_row() {
        let (_dir, store) = store().await;
        tenant_with_agent(&store, "demo").await;
        let run_id = submit_with_delivery(&store, "demo", "chat:42", None, Some("tg:42")).await;
        let now = Utc::now();
        store.claim_next_run().await.unwrap().unwrap();
        store
            .finalize_run_success(run_id, &json!({"reply":"hello"}), None)
            .await
            .unwrap();

        let first = store
            .claim_delivery_outbox("demo", 1, now, Duration::from_secs(1))
            .await
            .unwrap()
            .remove(0);
        let later = now + ChronoDuration::seconds(2);
        let second = store
            .claim_delivery_outbox("demo", 1, later, Duration::from_secs(10))
            .await
            .unwrap()
            .remove(0);
        assert_ne!(first.claim_token, second.claim_token);
        assert!(store
            .ack_delivery(
                "demo",
                DeliveryAck {
                    delivery_id: second.delivery_id,
                    claim_token: first.claim_token.as_deref().unwrap(),
                    outcome: "delivered",
                    error: None,
                    retry_after: None,
                    now: later,
                },
            )
            .await
            .is_err());

        let retried = store
            .ack_delivery(
                "demo",
                DeliveryAck {
                    delivery_id: second.delivery_id,
                    claim_token: second.claim_token.as_deref().unwrap(),
                    outcome: "retry",
                    error: Some("temporary"),
                    retry_after: Some(Duration::from_secs(2)),
                    now: later,
                },
            )
            .await
            .unwrap();
        assert_eq!(retried.status, "pending");
        assert_eq!(retried.attempt, 1);
        assert_eq!(
            store
                .list_delivery_outbox(Some("demo"), None, Some(run_id), 10)
                .await
                .unwrap()
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn mcp_catalog_is_tenant_scoped() {
        let (_dir, store) = store().await;
        tenant_with_agent(&store, "one").await;
        tenant_with_agent(&store, "two").await;
        let spec = McpServerSpec {
            enabled: true,
            transport: McpTransport::Http {
                url: "http://127.0.0.1/mcp".into(),
                headers_from: BTreeMap::new(),
            },
            allowed_tools: None,
        };
        store
            .apply_mcp_server(
                "one",
                "alpha",
                &spec,
                &[McpTool {
                    name: "ping".into(),
                    description: None,
                    input_schema: json!({"type":"object"}),
                }],
                None,
            )
            .await
            .unwrap();
        store
            .apply_mcp_server(
                "two",
                "beta",
                &spec,
                &[McpTool {
                    name: "ping".into(),
                    description: None,
                    input_schema: json!({"type":"object"}),
                }],
                None,
            )
            .await
            .unwrap();

        let tools = store
            .list_visible_tools("one", &[ToolFamily::Mcp])
            .await
            .unwrap();
        assert_eq!(tools.len(), 1);
        assert!(tools[0].name.starts_with("mcp_alpha_ping"));
        assert_ne!(
            mcp_exposed_name("a.b", "ping"),
            mcp_exposed_name("a/b", "ping")
        );
    }

    #[tokio::test]
    async fn mcp_exposed_tool_names_must_be_unique_within_a_tenant() {
        let (_dir, store) = store().await;
        tenant_with_agent(&store, "demo").await;
        let spec = McpServerSpec {
            enabled: true,
            transport: McpTransport::Http {
                url: "http://127.0.0.1/mcp".into(),
                headers_from: BTreeMap::new(),
            },
            allowed_tools: None,
        };
        store
            .apply_mcp_server(
                "demo",
                "a_b",
                &spec,
                &[McpTool {
                    name: "c".into(),
                    description: None,
                    input_schema: json!({"type":"object"}),
                }],
                None,
            )
            .await
            .unwrap();

        let error = store
            .apply_mcp_server(
                "demo",
                "a",
                &spec,
                &[McpTool {
                    name: "b_c".into(),
                    description: None,
                    input_schema: json!({"type":"object"}),
                }],
                None,
            )
            .await
            .unwrap_err();
        assert!(error.to_string().contains("mcp_a_b_c"));
        assert!(store.get_mcp_server("demo", "a").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn memory_hybrid_search_is_tenant_and_namespace_scoped() {
        let (_dir, store) = store().await;
        tenant_with_agent(&store, "one").await;
        tenant_with_agent(&store, "two").await;
        let embedding = test_embedding(0);

        store
            .put_memory("one", "profile", "favorite", "likes mangosteen", &embedding)
            .await
            .unwrap();
        store
            .put_memory("one", "other", "favorite", "likes mangosteen", &embedding)
            .await
            .unwrap();
        store
            .put_memory("two", "profile", "favorite", "likes mangosteen", &embedding)
            .await
            .unwrap();

        let matches = store
            .search_memory("one", "profile", "mangosteen", &embedding, 10)
            .await
            .unwrap();
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].tenant, "one");
        assert_eq!(matches[0].namespace, "profile");
        assert!(matches[0].score.is_some_and(|score| score > 0.0));
    }

    #[tokio::test]
    async fn graph_query_walks_one_to_three_hops_and_tracks_memory_lifecycle() {
        let (_dir, store) = store().await;
        tenant_with_agent(&store, "one").await;
        tenant_with_agent(&store, "two").await;
        let embedding = test_embedding(0);
        store
            .put_memory_with_graph(
                "one",
                "profile",
                "ownership",
                "Alice owns agentd",
                &embedding,
                &graph(json!({
                    "entities":[
                        {"id":"alice","label":"Alice","type":"person"},
                        {"id":"agentd","label":"agentd","type":"project"}
                    ],
                    "edges":[
                        {"from":"alice","relation":"owns","to":"agentd"},
                        {"from":"agentd","relation":"owned_by","to":"alice"}
                    ]
                })),
            )
            .await
            .unwrap();
        store
            .put_memory_with_graph(
                "one",
                "profile",
                "storage",
                "agentd uses libSQL",
                &embedding,
                &graph(json!({
                    "entities":[
                        {"id":"agentd","label":"agentd","type":"project"},
                        {"id":"libsql","label":"libSQL","type":"database"}
                    ],
                    "edges":[{"from":"agentd","relation":"uses","to":"libsql"}]
                })),
            )
            .await
            .unwrap();
        store
            .put_memory_with_graph(
                "two",
                "profile",
                "decoy",
                "Alice owns a secret",
                &embedding,
                &graph(json!({
                    "entities":[
                        {"id":"alice","label":"Alice"},
                        {"id":"secret","label":"Secret"}
                    ],
                    "edges":[{"from":"alice","relation":"owns","to":"secret"}]
                })),
            )
            .await
            .unwrap();

        let outgoing = store
            .query_graph(
                "one",
                "profile",
                GraphQuery {
                    entity: "Alice",
                    relation: None,
                    direction: "outgoing",
                    max_hops: 3,
                    limit: 100,
                },
            )
            .await
            .unwrap();
        assert_eq!(outgoing.paths.len(), 2);
        assert_eq!(outgoing.paths[0].nodes, ["alice", "agentd"]);
        assert_eq!(outgoing.paths[1].nodes, ["alice", "agentd", "libsql"]);
        assert!(!outgoing.entities.iter().any(|entity| entity.id == "secret"));
        assert_eq!(
            store
                .query_graph(
                    "one",
                    "profile",
                    GraphQuery {
                        entity: "alice",
                        relation: None,
                        direction: "outgoing",
                        max_hops: 3,
                        limit: 1,
                    },
                )
                .await
                .unwrap()
                .paths
                .len(),
            1
        );

        let one_hop = store
            .query_graph(
                "one",
                "profile",
                GraphQuery {
                    entity: "alice",
                    relation: None,
                    direction: "outgoing",
                    max_hops: 1,
                    limit: 100,
                },
            )
            .await
            .unwrap();
        assert_eq!(one_hop.paths.len(), 1);
        let relation = store
            .query_graph(
                "one",
                "profile",
                GraphQuery {
                    entity: "alice",
                    relation: Some("owns"),
                    direction: "outgoing",
                    max_hops: 3,
                    limit: 100,
                },
            )
            .await
            .unwrap();
        assert_eq!(relation.paths.len(), 1);
        let incoming = store
            .query_graph(
                "one",
                "profile",
                GraphQuery {
                    entity: "libSQL",
                    relation: None,
                    direction: "incoming",
                    max_hops: 3,
                    limit: 100,
                },
            )
            .await
            .unwrap();
        assert_eq!(incoming.paths.len(), 2);
        assert_eq!(incoming.paths[1].nodes, ["libsql", "agentd", "alice"]);

        store
            .put_memory(
                "one",
                "profile",
                "storage",
                "agentd changed storage",
                &embedding,
            )
            .await
            .unwrap();
        let after_update = store
            .query_graph(
                "one",
                "profile",
                GraphQuery {
                    entity: "alice",
                    relation: None,
                    direction: "outgoing",
                    max_hops: 3,
                    limit: 100,
                },
            )
            .await
            .unwrap();
        assert_eq!(after_update.paths.len(), 1);
        assert!(!after_update
            .entities
            .iter()
            .any(|entity| entity.id == "libsql"));

        assert!(store
            .delete_memory("one", "profile", "ownership")
            .await
            .unwrap());
        let after_delete = store
            .query_graph(
                "one",
                "profile",
                GraphQuery {
                    entity: "alice",
                    relation: None,
                    direction: "outgoing",
                    max_hops: 3,
                    limit: 100,
                },
            )
            .await
            .unwrap();
        assert!(after_delete.entities.is_empty());
        assert!(after_delete.paths.is_empty());
    }

    #[tokio::test]
    async fn invalid_graph_does_not_create_memory() {
        let (_dir, store) = store().await;
        tenant_with_agent(&store, "one").await;
        let error = store
            .put_memory_with_graph(
                "one",
                "profile",
                "invalid",
                "Alice owns an undeclared entity",
                &test_embedding(0),
                &graph(json!({
                    "entities":[{"id":"alice","label":"Alice"}],
                    "edges":[{"from":"alice","relation":"owns","to":"missing"}]
                })),
            )
            .await
            .unwrap_err();
        assert!(error.to_string().contains("must reference entities"));
        assert!(store
            .get_memory("one", "profile", "invalid")
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn memory_list_pages_are_stable_bounded_and_tenant_scoped() {
        let (_dir, store) = store().await;
        tenant_with_agent(&store, "one").await;
        tenant_with_agent(&store, "two").await;
        let embedding = test_embedding(0);
        for index in 0..105 {
            store
                .put_memory(
                    "one",
                    "profile",
                    &format!("fact-{index:03}"),
                    &format!("fact {index}"),
                    &embedding,
                )
                .await
                .unwrap();
        }
        store
            .put_memory("one", "other", "secret", "other namespace", &embedding)
            .await
            .unwrap();
        store
            .put_memory("two", "profile", "secret", "other tenant", &embedding)
            .await
            .unwrap();

        let clamped = store
            .list_memory_page("one", "profile", None, usize::MAX)
            .await
            .unwrap();
        assert_eq!(clamped.items.len(), 100);
        assert_eq!(clamped.next_after_id.as_deref(), Some("fact-099"));
        assert_eq!(
            store
                .list_memory_page("one", "profile", None, 0)
                .await
                .unwrap()
                .items
                .len(),
            1
        );

        let mut after_id = None;
        let mut ids = Vec::new();
        loop {
            let page = store
                .list_memory_page("one", "profile", after_id.as_deref(), 17)
                .await
                .unwrap();
            ids.extend(page.items.into_iter().map(|item| item.id));
            after_id = page.next_after_id;
            if after_id.is_none() {
                break;
            }
        }
        assert_eq!(ids.len(), 105);
        assert_eq!(ids.first().map(String::as_str), Some("fact-000"));
        assert_eq!(ids.last().map(String::as_str), Some("fact-104"));
        assert!(ids.windows(2).all(|pair| pair[0] < pair[1]));
        assert!(!ids.iter().any(|id| id == "secret"));
    }

    #[tokio::test]
    async fn memory_semantic_search_updates_vectors_and_preserves_created_at() {
        let (_dir, store) = store().await;
        tenant_with_agent(&store, "one").await;
        let first_embedding = test_embedding(1);
        let updated_embedding = test_embedding(0);

        let created = store
            .put_memory(
                "one",
                "profile",
                "favorite",
                "likes mangosteen",
                &first_embedding,
            )
            .await
            .unwrap();
        let updated = store
            .put_memory(
                "one",
                "profile",
                "favorite",
                "likes durian",
                &updated_embedding,
            )
            .await
            .unwrap();
        assert_eq!(updated.created_at, created.created_at);
        let matches = store
            .search_memory(
                "one",
                "profile",
                "a spiky tropical preference",
                &updated_embedding,
                1,
            )
            .await
            .unwrap();
        assert_eq!(matches[0].id, "favorite");
        assert_eq!(matches[0].text, "likes durian");
    }

    #[tokio::test]
    async fn memory_rejects_wrong_dimensions_and_oversized_text() {
        let (_dir, store) = store().await;
        tenant_with_agent(&store, "one").await;
        let embedding = test_embedding(0);
        store
            .put_memory("one", "profile", "fact", "short", &embedding)
            .await
            .unwrap();
        let error = store
            .search_memory("one", "profile", "short", &[1.0], 5)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("exactly 384 dimensions"));
        assert!(store
            .put_memory("one", "profile", "long", &"界".repeat(1366), &embedding,)
            .await
            .is_err());
    }

    #[test]
    fn memory_fts_query_is_unicode_safe_and_uses_or() {
        assert_eq!(
            memory_fts_query("red fruit"),
            Some("\"red\" OR \"fruit\"".into())
        );
        assert_eq!(memory_fts_query("喜欢榴莲"), Some("\"喜欢榴莲\"".into()));
        assert_eq!(memory_fts_query("?!"), None);
    }

    #[test]
    fn rrf_rewards_candidates_found_by_both_searches_and_breaks_ties_by_id() {
        fn item(id: &str) -> MemoryItem {
            MemoryItem {
                tenant: "one".into(),
                namespace: "profile".into(),
                id: id.into(),
                text: id.into(),
                created_at: "now".into(),
                updated_at: "now".into(),
                score: None,
            }
        }
        let fused = fuse_memory_candidates(
            vec![item("lexical"), item("both")],
            vec![item("both"), item("semantic")],
            3,
        );
        assert_eq!(fused[0].id, "both");
        assert_eq!(fused[1].id, "lexical");
        assert_eq!(fused[2].id, "semantic");
    }

    #[tokio::test]
    async fn memory_schema_stays_single_table_plus_fts() {
        let (_dir, store) = store().await;
        let tables = db::query_scalar::<String>(
            "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'memory%' ORDER BY name",
        )
        .fetch_all(&store.pool)
        .await
        .unwrap();
        assert_eq!(
            tables,
            vec![
                "memory",
                "memory_fts",
                "memory_fts_config",
                "memory_fts_data",
                "memory_fts_docsize",
                "memory_fts_idx"
            ]
        );
    }

    #[tokio::test]
    async fn schema_version_six_migrates_graph_tables_without_resetting_memory() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("agentd.db");
        let store = AgentdStore::new(path.to_str().unwrap()).await.unwrap();
        tenant_with_agent(&store, "one").await;
        store
            .put_memory(
                "one",
                "profile",
                "favorite",
                "likes mangosteen",
                &test_embedding(0),
            )
            .await
            .unwrap();
        db::query("DROP TABLE edges")
            .execute(&store.pool)
            .await
            .unwrap();
        db::query("DROP TABLE entities")
            .execute(&store.pool)
            .await
            .unwrap();
        db::query("ALTER TABLE deliveries DROP COLUMN payload_json")
            .execute(&store.pool)
            .await
            .unwrap();
        db::query("PRAGMA user_version = 6")
            .execute(&store.pool)
            .await
            .unwrap();
        drop(store);

        let migrated = AgentdStore::new(path.to_str().unwrap()).await.unwrap();
        assert_eq!(
            db::query_scalar::<i64>("PRAGMA user_version")
                .fetch_optional(&migrated.pool)
                .await
                .unwrap(),
            Some(8)
        );
        assert!(migrated
            .get_memory("one", "profile", "favorite")
            .await
            .unwrap()
            .is_some());
        assert_eq!(
            db::query_scalar::<String>(
                "SELECT name FROM sqlite_master WHERE type = 'table' AND name IN ('entities', 'edges') ORDER BY name",
            )
            .fetch_all(&migrated.pool)
            .await
            .unwrap(),
            vec!["edges", "entities"]
        );
    }

    #[tokio::test]
    async fn schema_version_seven_backfills_immutable_delivery_payloads() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("agentd.db");
        let store = AgentdStore::new(path.to_str().unwrap()).await.unwrap();
        tenant_with_agent(&store, "demo").await;
        let run_id = submit_with_delivery(&store, "demo", "chat:42", None, Some("tg:42")).await;
        store.claim_next_run().await.unwrap().unwrap();
        store
            .finalize_run_success(run_id, &json!({"reply":"preserved"}), None)
            .await
            .unwrap();
        db::query("ALTER TABLE deliveries DROP COLUMN payload_json")
            .execute(&store.pool)
            .await
            .unwrap();
        db::query("PRAGMA user_version = 7")
            .execute(&store.pool)
            .await
            .unwrap();
        drop(store);

        let migrated = AgentdStore::new(path.to_str().unwrap()).await.unwrap();
        let deliveries = migrated
            .list_delivery_outbox(Some("demo"), None, Some(run_id), 10)
            .await
            .unwrap();
        assert_eq!(deliveries.len(), 1);
        assert_eq!(deliveries[0].payload, json!({"reply":"preserved"}));
        assert_eq!(
            db::query_scalar::<i64>("PRAGMA user_version")
                .fetch_optional(&migrated.pool)
                .await
                .unwrap(),
            Some(8)
        );
    }

    #[tokio::test]
    async fn schema_version_mismatch_requires_data_reset() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("agentd.db");
        let store = AgentdStore::new(path.to_str().unwrap()).await.unwrap();
        db::query("PRAGMA user_version = 99")
            .execute(&store.pool)
            .await
            .unwrap();
        drop(store);

        let error = AgentdStore::new(path.to_str().unwrap())
            .await
            .err()
            .expect("schema mismatch must fail");
        assert!(error.to_string().contains("--reset-data"));
    }
}
