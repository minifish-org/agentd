use chrono::{DateTime, Utc};
use chrono_tz::Tz;
use croner::Cron;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::str::FromStr;
use thiserror::Error;
use uuid::Uuid;

mod builtin_catalog;
mod delivery;
pub use builtin_catalog::{builtin_tool_catalog, visible_tools};
pub use delivery::*;

#[derive(Debug, Error)]
pub enum ApiError {
    #[error("validation error: {0}")]
    Validation(String),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(rename_all = "snake_case")]
pub enum ToolFamily {
    Schedule,
    Artifact,
    Memory,
    Web,
    Clock,
    Calc,
    Mcp,
}

impl ToolFamily {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Schedule => "schedule",
            Self::Artifact => "artifact",
            Self::Memory => "memory",
            Self::Web => "web",
            Self::Clock => "clock",
            Self::Calc => "calc",
            Self::Mcp => "mcp",
        }
    }

    pub fn all() -> Vec<Self> {
        vec![
            Self::Schedule,
            Self::Artifact,
            Self::Memory,
            Self::Web,
            Self::Clock,
            Self::Calc,
            Self::Mcp,
        ]
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ResourceMeta {
    pub name: String,
    pub tenant: String,
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
}

impl ResourceMeta {
    fn validate(&self) -> Result<(), ApiError> {
        if self.name.trim().is_empty() {
            return Err(ApiError::Validation("metadata.name is required".into()));
        }
        if self.tenant.trim().is_empty() {
            return Err(ApiError::Validation("metadata.tenant is required".into()));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AgentLimits {
    pub timeout_ms: u64,
    pub max_steps: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AgentSpec {
    #[serde(default)]
    pub allowed_families: Option<Vec<ToolFamily>>,
    pub limits: AgentLimits,
    /// Standing system prompt / persona for this agent, set at registration
    /// time. The host passes it into every turn, so callers send only payload.
    #[serde(default)]
    pub system_prompt: Option<String>,
    /// Optional LLM model id for this agent's turns (e.g. "premium/chat").
    #[serde(default)]
    pub model: Option<String>,
    /// Optional sampling temperature (0.0–2.0).
    #[serde(default)]
    pub temperature: Option<f32>,
    /// Optional cap on the LLM response token count. Unset uses 4096.
    #[serde(default)]
    pub max_tokens: Option<u32>,
    /// Number of complete user/assistant turns retained for this agent.
    /// `None` uses the host default; zero disables rolling context.
    #[serde(default)]
    pub context_window: Option<usize>,
}

impl AgentSpec {
    pub fn effective_allowed_families(&self) -> Vec<ToolFamily> {
        self.allowed_families.clone().unwrap_or_else(|| {
            vec![
                ToolFamily::Artifact,
                ToolFamily::Memory,
                ToolFamily::Schedule,
                ToolFamily::Clock,
                ToolFamily::Web,
                ToolFamily::Calc,
                ToolFamily::Mcp,
            ]
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AgentResource {
    pub metadata: ResourceMeta,
    pub spec: AgentSpec,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum McpTransport {
    Stdio {
        command: String,
        #[serde(default)]
        args: Vec<String>,
        #[serde(default)]
        env_from: BTreeMap<String, String>,
    },
    Http {
        url: String,
        #[serde(default)]
        headers_from: BTreeMap<String, String>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct McpTool {
    pub name: String,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default = "default_json_schema")]
    pub input_schema: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct McpServerSpec {
    #[serde(default = "default_true")]
    pub enabled: bool,
    pub transport: McpTransport,
    #[serde(default)]
    pub allowed_tools: Option<Vec<String>>,
}

impl McpServerSpec {
    pub fn validate(&self) -> Result<(), ApiError> {
        match &self.transport {
            McpTransport::Stdio {
                command, env_from, ..
            } => {
                if command.trim().is_empty() {
                    return Err(ApiError::Validation(
                        "command is required for stdio MCP servers".into(),
                    ));
                }
                for (child_name, source_name) in env_from {
                    if !is_env_name(child_name) || !is_env_name(source_name) {
                        return Err(ApiError::Validation(format!(
                            "invalid stdio env_from mapping: {child_name} -> {source_name}"
                        )));
                    }
                }
            }
            McpTransport::Http { url, headers_from } => {
                let url = url.trim();
                if !(url.starts_with("http://") || url.starts_with("https://")) {
                    return Err(ApiError::Validation(
                        "http MCP url must use http:// or https://".into(),
                    ));
                }
                for (header_name, source_name) in headers_from {
                    if !is_http_header_name(header_name) || !is_env_name(source_name) {
                        return Err(ApiError::Validation(format!(
                            "invalid http headers_from mapping: {header_name} -> {source_name}"
                        )));
                    }
                    if is_reserved_mcp_header(header_name) {
                        return Err(ApiError::Validation(format!(
                            "http header {header_name} is managed by agentd"
                        )));
                    }
                }
            }
        }

        if let Some(allowed_tools) = &self.allowed_tools {
            let mut unique = BTreeSet::new();
            for name in allowed_tools {
                if name.trim().is_empty() {
                    return Err(ApiError::Validation(
                        "allowed_tools cannot contain an empty name".into(),
                    ));
                }
                if !unique.insert(name) {
                    return Err(ApiError::Validation(format!(
                        "allowed_tools contains duplicate tool: {name}"
                    )));
                }
            }
        }
        Ok(())
    }
}

fn is_env_name(value: &str) -> bool {
    let mut chars = value.chars();
    matches!(chars.next(), Some('_' | 'A'..='Z' | 'a'..='z'))
        && chars.all(|ch| matches!(ch, '_' | 'A'..='Z' | 'a'..='z' | '0'..='9'))
}

fn is_http_header_name(value: &str) -> bool {
    !value.is_empty()
        && value.bytes().all(|byte| {
            byte.is_ascii_alphanumeric()
                || matches!(
                    byte,
                    b'!' | b'#'
                        | b'$'
                        | b'%'
                        | b'&'
                        | b'\''
                        | b'*'
                        | b'+'
                        | b'-'
                        | b'.'
                        | b'^'
                        | b'_'
                        | b'`'
                        | b'|'
                        | b'~'
                )
        })
}

fn is_reserved_mcp_header(value: &str) -> bool {
    matches!(
        value.to_ascii_lowercase().as_str(),
        "accept"
            | "content-length"
            | "content-type"
            | "host"
            | "mcp-protocol-version"
            | "mcp-session-id"
    )
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct McpServer {
    pub tenant: String,
    pub name: String,
    pub spec: McpServerSpec,
    #[serde(default)]
    pub tools: Vec<McpTool>,
    #[serde(default)]
    pub last_error: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct McpToolInvocationTarget {
    pub server: McpServer,
    pub tool: McpTool,
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ScheduleSpec {
    pub agent_ref: String,
    pub scope: String,
    #[serde(default)]
    pub payload: serde_json::Value,
    #[serde(default)]
    pub delivery: Option<DeliveryRequest>,
    #[serde(default)]
    pub at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub cron: Option<String>,
    #[serde(default)]
    pub timezone: Option<String>,
    #[serde(default = "default_true")]
    pub enabled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Schedule {
    pub tenant: String,
    pub name: String,
    pub spec: ScheduleSpec,
    #[serde(default)]
    pub last_triggered_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub next_trigger_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub last_run_id: Option<Uuid>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AgentRunStatus {
    Queued,
    Running,
    Succeeded,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AgentRun {
    pub run_id: Uuid,
    pub tenant: String,
    pub name: String,
    pub agent_ref: String,
    pub scope: String,
    pub source: String,
    pub input: serde_json::Value,
    #[serde(default)]
    pub output: Option<serde_json::Value>,
    #[serde(default)]
    pub error: Option<String>,
    pub status: AgentRunStatus,
    #[serde(default)]
    pub request_id: Option<String>,
    pub created_at: DateTime<Utc>,
    #[serde(default)]
    pub started_at: Option<DateTime<Utc>>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Agent {
    pub tenant: String,
    pub name: String,
    pub metadata: ResourceMeta,
    pub spec: AgentSpec,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ToolSpec {
    pub name: String,
    pub family: ToolFamily,
    pub description: String,
    #[serde(default = "default_json_schema")]
    pub input_schema: serde_json::Value,
    #[serde(default)]
    pub mutating: bool,
}

fn default_json_schema() -> serde_json::Value {
    serde_json::json!({ "type": "object" })
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ArtifactStat {
    pub artifact_ref: String,
    pub path: String,
    #[serde(default)]
    pub content_type: Option<String>,
    #[serde(default)]
    pub sha256: Option<String>,
    #[serde(default)]
    pub size_bytes: Option<u64>,
    #[serde(default)]
    pub metadata: Option<serde_json::Value>,
    #[serde(default)]
    pub updated_at: Option<String>,
}

pub fn validate_cron_expression(raw: &str) -> Result<(), ApiError> {
    let expr = raw.trim();
    if expr.is_empty() {
        return Err(ApiError::Validation("cron must not be empty".into()));
    }
    Cron::from_str(expr)
        .map_err(|error| ApiError::Validation(format!("invalid cron expression: {error}")))?;
    Ok(())
}

pub fn validate_timezone_name(raw: &str) -> Result<(), ApiError> {
    let timezone = raw.trim();
    if timezone.is_empty() {
        return Err(ApiError::Validation("timezone must not be empty".into()));
    }
    if timezone.parse::<Tz>().is_ok() || parse_timezone_offset(timezone).is_some() {
        return Ok(());
    }
    Err(ApiError::Validation(format!(
        "invalid timezone: {timezone}"
    )))
}

pub fn parse_timezone_offset(raw: &str) -> Option<i32> {
    let trimmed = raw.trim();
    let normalized = trimmed
        .strip_prefix("UTC")
        .or_else(|| trimmed.strip_prefix("GMT"))
        .unwrap_or(trimmed);
    if normalized == "Z" {
        return Some(0);
    }
    let sign = if let Some(rest) = normalized.strip_prefix('+') {
        (1, rest)
    } else if let Some(rest) = normalized.strip_prefix('-') {
        (-1, rest)
    } else {
        return None;
    };
    let (hours, minutes) = if let Some((hours, minutes)) = sign.1.split_once(':') {
        (hours, minutes)
    } else if sign.1.len() > 2 {
        sign.1.split_at(sign.1.len() - 2)
    } else {
        (sign.1, "0")
    };
    let hours: i32 = hours.parse().ok()?;
    let minutes: i32 = minutes.parse().ok()?;
    Some(sign.0 * (hours * 3600 + minutes * 60))
}

impl AgentResource {
    pub fn validate(&self) -> Result<(), ApiError> {
        self.metadata.validate()?;
        let effective_allowed = self.spec.effective_allowed_families();
        let unique: BTreeSet<_> = effective_allowed.iter().cloned().collect();
        if unique.len() != effective_allowed.len() {
            return Err(ApiError::Validation(
                "spec.allowed_families must not contain duplicates".into(),
            ));
        }
        if self.spec.limits.timeout_ms == 0 {
            return Err(ApiError::Validation(
                "spec.limits.timeout_ms must be > 0".into(),
            ));
        }
        if self.spec.limits.max_steps == 0 {
            return Err(ApiError::Validation(
                "spec.limits.max_steps must be > 0".into(),
            ));
        }
        Ok(())
    }
}

impl ScheduleSpec {
    pub fn validate(&self) -> Result<(), ApiError> {
        if self.agent_ref.trim().is_empty() {
            return Err(ApiError::Validation("agent_ref is required".into()));
        }
        if self.scope.trim().is_empty() {
            return Err(ApiError::Validation("scope is required".into()));
        }
        if let Some(delivery) = &self.delivery {
            delivery.validate()?;
        }
        let trigger_count = self.at.iter().count() + self.cron.iter().count();
        if trigger_count != 1 {
            return Err(ApiError::Validation(
                "exactly one of at or cron is required".into(),
            ));
        }
        if let Some(timezone) = self.timezone.as_deref() {
            validate_timezone_name(timezone)?;
        }
        if let Some(cron) = self.cron.as_deref() {
            validate_cron_expression(cron)?;
            if self.timezone.is_none() {
                return Err(ApiError::Validation(
                    "timezone is required when cron is set".into(),
                ));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn schedule() -> ScheduleSpec {
        ScheduleSpec {
            agent_ref: "bot".into(),
            scope: "chat/1".into(),
            payload: serde_json::json!({}),
            delivery: None,
            at: None,
            cron: None,
            timezone: None,
            enabled: true,
        }
    }

    #[test]
    fn schedule_accepts_only_at_or_cron_with_timezone() {
        let mut spec = schedule();
        spec.at = Some(Utc::now());
        assert!(spec.validate().is_ok());

        spec.cron = Some("0 9 * * *".into());
        assert!(spec.validate().is_err());

        spec.at = None;
        assert!(spec.validate().is_err());
        spec.timezone = Some("Asia/Singapore".into());
        assert!(spec.validate().is_ok());
    }

    #[test]
    fn mcp_transport_is_strict_and_secret_indirect() {
        let spec: McpServerSpec = serde_json::from_value(serde_json::json!({
            "enabled": true,
            "transport": {
                "type": "stdio",
                "command": "/opt/canopy-mcp",
                "args": ["--quiet"],
                "env_from": {"CANOPY_TOKEN": "AGENTD_CANOPY_TOKEN"}
            }
        }))
        .unwrap();
        assert!(spec.validate().is_ok());

        let old_flat_shape = serde_json::from_value::<McpServerSpec>(serde_json::json!({
            "enabled": true,
            "transport": "stdio",
            "command": "/opt/canopy-mcp"
        }));
        assert!(old_flat_shape.is_err());
    }

    #[test]
    fn mcp_rejects_reserved_headers_and_duplicate_tools() {
        let reserved = McpServerSpec {
            enabled: true,
            transport: McpTransport::Http {
                url: "https://example.test/mcp".into(),
                headers_from: BTreeMap::from([("Mcp-Session-Id".into(), "MCP_SESSION".into())]),
            },
            allowed_tools: None,
        };
        assert!(reserved.validate().is_err());

        let duplicate = McpServerSpec {
            enabled: true,
            transport: McpTransport::Http {
                url: "https://example.test/mcp".into(),
                headers_from: BTreeMap::new(),
            },
            allowed_tools: Some(vec!["route".into(), "route".into()]),
        };
        assert!(duplicate.validate().is_err());
    }
}
