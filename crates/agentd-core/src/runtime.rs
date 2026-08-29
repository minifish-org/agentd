use crate::llm_provider::extract_openai_message_content;
use crate::{CapabilityEngine, ToolResult};
use agentd_api::{ToolFamily, ToolSpec};
use agentd_store::{AgentdStore, AssignedRun};
use anyhow::{anyhow, Result};
use chrono::Utc;
use serde_json::{json, Value};
use std::collections::HashMap;

const DEFAULT_CONTEXT_TURNS: usize = 20;
const MAX_WEB_TOOL_CALLS_PER_RUN: usize = 6;
const MEMORY_MAINTAINER_AGENT: &str = "system/memory-maintainer";

const NATIVE_LOOP_PROMPT: &str = r#"Native tool rules:
- Use real tool calls when a capability is needed.
- Mutating tools execute immediately when their family is allowed.
- Treat tool results as observations and recover from tool errors when possible.
- When durable facts, preferences, or constraints may be missing from context,
  call memory_search. Write important concise facts with stable memory ids; use
  artifacts for long documents.
- Return one JSON object as the final answer. If no structured contract is
  required, return {"reply":"..."}."#;

#[derive(Clone)]
pub struct RuntimeEngine {
    caps: CapabilityEngine,
    store: AgentdStore,
}

#[derive(Debug, Clone)]
pub struct ExecutionReport {
    pub error: Option<String>,
}

#[derive(Debug, Default)]
struct ToolBudget {
    web_calls: usize,
}

impl ToolBudget {
    fn native_tools(&self, callable: &[ToolSpec]) -> Vec<Value> {
        callable
            .iter()
            .filter(|tool| {
                tool.family != ToolFamily::Web || self.web_calls < MAX_WEB_TOOL_CALLS_PER_RUN
            })
            .map(native_function_tool)
            .collect()
    }

    fn admit(&mut self, tool: &ToolSpec) -> bool {
        if tool.family != ToolFamily::Web {
            return true;
        }
        if self.web_calls >= MAX_WEB_TOOL_CALLS_PER_RUN {
            return false;
        }
        self.web_calls += 1;
        true
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum MemoryMaintenanceProgress {
    NotStarted,
    AwaitingCursor(String),
    Complete,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum MemoryMaintenanceScan {
    NotRequired,
    Required {
        namespace: String,
        progress: MemoryMaintenanceProgress,
    },
}

impl MemoryMaintenanceScan {
    fn for_run(agent_ref: &str, input: &Value) -> Result<Self> {
        if agent_ref != MEMORY_MAINTAINER_AGENT {
            return Ok(Self::NotRequired);
        }
        let namespace = input
            .get("namespace")
            .and_then(Value::as_str)
            .filter(|namespace| !namespace.is_empty())
            .ok_or_else(|| anyhow!("memory maintainer input requires a non-empty namespace"))?;
        Ok(Self::Required {
            namespace: namespace.to_string(),
            progress: MemoryMaintenanceProgress::NotStarted,
        })
    }

    fn validate_tool_call(&self, tool: &str, arguments: &Value) -> Result<()> {
        let Self::Required {
            namespace,
            progress,
        } = self
        else {
            return Ok(());
        };
        if !tool.starts_with("memory_") {
            return Ok(());
        }
        let actual_namespace = arguments.get("namespace").and_then(Value::as_str);
        if actual_namespace != Some(namespace.as_str()) {
            return Err(anyhow!(
                "memory maintainer tools must use input namespace {namespace:?}"
            ));
        }
        if matches!(tool, "memory_put" | "memory_delete")
            && !matches!(progress, MemoryMaintenanceProgress::Complete)
        {
            return Err(anyhow!(
                "memory maintainer cannot modify memory before completing memory_list"
            ));
        }
        if tool != "memory_list" {
            return Ok(());
        }
        let cursor = arguments.get("cursor").and_then(Value::as_str);
        match progress {
            MemoryMaintenanceProgress::NotStarted | MemoryMaintenanceProgress::Complete
                if cursor.is_none() =>
            {
                Ok(())
            }
            MemoryMaintenanceProgress::AwaitingCursor(expected)
                if cursor == Some(expected.as_str()) =>
            {
                Ok(())
            }
            MemoryMaintenanceProgress::NotStarted | MemoryMaintenanceProgress::Complete => Err(
                anyhow!("memory maintainer must start memory_list without a cursor"),
            ),
            MemoryMaintenanceProgress::AwaitingCursor(_) => Err(anyhow!(
                "memory maintainer must continue memory_list with the returned next_cursor"
            )),
        }
    }

    fn observe_list_result(&mut self, tool: &str, result: &ToolResult) -> Result<()> {
        let Self::Required { progress, .. } = self else {
            return Ok(());
        };
        if tool != "memory_list" || !result.ok {
            return Ok(());
        }
        *progress = match result.result.get("next_cursor") {
            Some(Value::Null) => MemoryMaintenanceProgress::Complete,
            Some(Value::String(cursor)) if !cursor.is_empty() => {
                MemoryMaintenanceProgress::AwaitingCursor(cursor.clone())
            }
            _ => {
                return Err(anyhow!(
                    "memory_list result must contain a null or non-empty next_cursor"
                ));
            }
        };
        Ok(())
    }

    fn ensure_complete(&self) -> Result<()> {
        match self {
            Self::NotRequired
            | Self::Required {
                progress: MemoryMaintenanceProgress::Complete,
                ..
            } => Ok(()),
            Self::Required {
                progress: MemoryMaintenanceProgress::NotStarted,
                ..
            } => Err(anyhow!(
                "memory maintainer cannot finish before calling memory_list"
            )),
            Self::Required {
                progress: MemoryMaintenanceProgress::AwaitingCursor(_),
                ..
            } => Err(anyhow!(
                "memory maintainer cannot finish before following next_cursor to completion"
            )),
        }
    }
}

impl RuntimeEngine {
    pub fn new(caps: CapabilityEngine, store: AgentdStore) -> Self {
        Self { caps, store }
    }

    pub async fn execute_assigned_run(&self, assigned: &AssignedRun) -> Result<ExecutionReport> {
        match self.run_agent(assigned).await {
            Ok(()) => Ok(ExecutionReport { error: None }),
            Err(error) => Ok(ExecutionReport {
                error: Some(error.to_string()),
            }),
        }
    }

    async fn run_agent(&self, assigned: &AssignedRun) -> Result<()> {
        let run = &assigned.run;
        let mut maintenance_scan = MemoryMaintenanceScan::for_run(&run.agent_ref, &run.input)?;
        let prior_state = self
            .store
            .get_context_state(&run.tenant, &run.agent_ref, &run.scope)
            .await?
            .map(|context| context.state)
            .unwrap_or_else(|| json!({}));
        let prior_messages = context_messages(&prior_state);
        let user_content = json!({
            "input": run.input,
            "source": run.source,
        })
        .to_string();

        let system_prompt = assigned
            .agent_system_prompt
            .as_deref()
            .unwrap_or_else(|| self.caps.default_chat_system_prompt());
        let mut messages = vec![json!({
            "role": "system",
            "content": format!("{system_prompt}\n\n{NATIVE_LOOP_PROMPT}"),
        })];
        messages.extend(prior_messages.iter().filter_map(model_message));
        messages.push(json!({"role":"user", "content":user_content}));

        let callable = assigned.visible_tools.clone();
        let by_name: HashMap<String, ToolSpec> = callable
            .iter()
            .cloned()
            .map(|tool| (tool.name.clone(), tool))
            .collect();
        let mut tool_budget = ToolBudget::default();
        for step in 1..=assigned.max_steps.max(1) {
            let tools = tool_budget.native_tools(&callable);
            let mut request = json!({
                "messages": messages,
                "temperature": assigned.agent_temperature.unwrap_or(0.2),
                "max_tokens": assigned.agent_max_tokens.unwrap_or(4096),
                "parallel_tool_calls": false,
            });
            if !tools.is_empty() {
                request["tools"] = Value::Array(tools.clone());
                request["tool_choice"] = json!("auto");
            }
            if let Some(model) = assigned.agent_model.as_deref() {
                request["model"] = json!(model);
            }

            self.store
                .append_event(
                    run.run_id,
                    "model",
                    json!({"phase":"request", "step":step, "request":request}),
                    Utc::now(),
                )
                .await?;
            let response = self.caps.chat_completion(&request).await?;
            self.store
                .append_event(
                    run.run_id,
                    "model",
                    json!({"phase":"response", "step":step, "response":response}),
                    Utc::now(),
                )
                .await?;

            let choice = response
                .pointer("/choices/0/message")
                .cloned()
                .ok_or_else(|| anyhow!("model response missing choices[0].message"))?;
            let tool_calls = choice
                .get("tool_calls")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default();
            if tool_calls.is_empty() {
                maintenance_scan.ensure_complete()?;
                let content = extract_openai_message_content(&response)
                    .ok_or_else(|| anyhow!("model returned neither tool calls nor content"))?;
                if content.trim().is_empty() {
                    return Err(anyhow!("model returned empty final content"));
                }
                let output =
                    parse_json_object(&content).unwrap_or_else(|| json!({"reply":content}));
                let context = next_context_state(
                    assigned.agent_context_turns,
                    prior_messages,
                    &user_content,
                    &output,
                );
                self.store
                    .finalize_run_success(run.run_id, &output, context.as_ref())
                    .await?;
                return Ok(());
            }

            messages.push(choice);
            for call in tool_calls {
                let call_id = call
                    .get("id")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string();
                let name = call
                    .pointer("/function/name")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string();
                let raw_arguments = call
                    .pointer("/function/arguments")
                    .and_then(Value::as_str)
                    .unwrap_or("{}");
                let arguments: Value = serde_json::from_str(raw_arguments)
                    .map_err(|error| anyhow!("invalid arguments for {name}: {error}"))?;
                let arguments =
                    inject_runtime_context(&name, arguments, &run.agent_ref, &run.scope);
                maintenance_scan.validate_tool_call(&name, &arguments)?;

                self.store
                    .append_event(
                        run.run_id,
                        "tool",
                        json!({
                            "phase":"call",
                            "step":step,
                            "call_id":call_id,
                            "name":name,
                            "arguments":arguments,
                        }),
                        Utc::now(),
                    )
                    .await?;

                let envelope = match by_name.get(&name) {
                    Some(tool) if !tool_budget.admit(tool) => tool_error(
                        "web tool call budget exceeded; answer using results already collected",
                    ),
                    Some(tool) => self.caps.execute_tool(&run.tenant, tool, &arguments).await,
                    None => tool_error("tool is not visible to this agent"),
                };
                self.store
                    .append_event(
                        run.run_id,
                        "tool",
                        json!({
                            "phase":"result",
                            "step":step,
                            "call_id":call_id,
                            "result":envelope,
                        }),
                        Utc::now(),
                    )
                    .await?;
                maintenance_scan.observe_list_result(&name, &envelope)?;
                let content = if envelope.ok {
                    envelope.result.to_string()
                } else {
                    json!({"error":envelope.error}).to_string()
                };
                messages.push(json!({
                    "role":"tool",
                    "tool_call_id":call_id,
                    "name":name,
                    "content":content,
                }));
            }
        }
        Err(anyhow!("max_steps exceeded before a final response"))
    }
}

fn parse_json_object(raw: &str) -> Option<Value> {
    decode_object(raw)
        .or_else(|| decode_object(strip_code_fence(raw.trim()).trim()))
        .or_else(|| first_balanced_object(strip_code_fence(raw.trim())).and_then(decode_object))
}

fn decode_object(raw: &str) -> Option<Value> {
    serde_json::from_str(raw).ok().filter(Value::is_object)
}

fn strip_code_fence(value: &str) -> &str {
    let Some(rest) = value.strip_prefix("```") else {
        return value;
    };
    let content = rest
        .find('\n')
        .map(|index| &rest[index + 1..])
        .unwrap_or(rest);
    content
        .rfind("```")
        .map(|end| content[..end].trim())
        .unwrap_or(content)
}

fn first_balanced_object(value: &str) -> Option<&str> {
    let start = value.find('{')?;
    let mut depth = 0;
    let mut quoted = false;
    let mut escaped = false;
    for (offset, byte) in value.as_bytes()[start..].iter().enumerate() {
        if quoted {
            if escaped {
                escaped = false;
            } else if *byte == b'\\' {
                escaped = true;
            } else if *byte == b'"' {
                quoted = false;
            }
            continue;
        }
        match byte {
            b'"' => quoted = true,
            b'{' => depth += 1,
            b'}' => {
                depth -= 1;
                if depth == 0 {
                    return Some(&value[start..=start + offset]);
                }
            }
            _ => {}
        }
    }
    None
}

fn native_function_tool(tool: &ToolSpec) -> Value {
    json!({
        "type":"function",
        "function":{
            "name":tool.name,
            "description":format!("family={}. {}", tool.family.as_str(), tool.description),
            "parameters":tool.input_schema,
        }
    })
}

fn context_messages(state: &Value) -> Vec<Value> {
    state
        .get("messages")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default()
}

fn model_message(message: &Value) -> Option<Value> {
    let role = message.get("role")?.as_str()?;
    if !matches!(role, "user" | "assistant") {
        return None;
    }
    let content = message
        .get("content")
        .or_else(|| message.get("text"))?
        .as_str()?;
    Some(json!({"role":role, "content":content}))
}

fn next_context_state(
    configured_turns: Option<usize>,
    mut messages: Vec<Value>,
    user_content: &str,
    output: &Value,
) -> Option<Value> {
    let turns = configured_turns.unwrap_or(DEFAULT_CONTEXT_TURNS);
    if turns == 0 {
        return None;
    }
    let now = Utc::now().to_rfc3339();
    messages.push(json!({"role":"user", "content":user_content, "ts":now}));
    messages.push(json!({
        "role":"assistant",
        "content":output.to_string(),
        "ts":now,
    }));
    let keep = turns.saturating_mul(2);
    if messages.len() > keep {
        messages.drain(0..messages.len() - keep);
    }
    Some(json!({"messages":messages}))
}

fn inject_runtime_context(tool: &str, mut arguments: Value, agent: &str, scope: &str) -> Value {
    let Some(object) = arguments.as_object_mut() else {
        return arguments;
    };
    if (tool.starts_with("memory_") || tool == "graph_query") && !object.contains_key("namespace") {
        object.insert("namespace".into(), json!(agent));
    }
    if tool.starts_with("schedule_") {
        object.insert("agent_ref".into(), json!(agent));
        object.insert("scope".into(), json!(scope));
    }
    arguments
}

fn tool_error(error: &str) -> ToolResult {
    ToolResult {
        ok: false,
        result: json!({}),
        error: Some(error.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        inject_runtime_context, next_context_state, parse_json_object, MemoryMaintenanceScan,
        RuntimeEngine, ToolBudget, MAX_WEB_TOOL_CALLS_PER_RUN,
    };
    use crate::{CapabilityEngine, CapabilityEngineConfig, ToolResult};
    use agentd_api::{AgentLimits, AgentResource, AgentSpec, ResourceMeta, ToolFamily, ToolSpec};
    use agentd_store::{AgentdStore, NewRun};
    use axum::{routing::post, Json, Router};
    use serde_json::json;
    use std::collections::BTreeMap;

    fn test_tool(name: &str, family: ToolFamily) -> ToolSpec {
        ToolSpec {
            name: name.into(),
            family,
            description: String::new(),
            input_schema: json!({"type":"object"}),
            mutating: false,
        }
    }

    #[test]
    fn web_tool_budget_hides_web_tools_after_the_limit() {
        let web = test_tool("web_search", ToolFamily::Web);
        let calc = test_tool("calc_eval", ToolFamily::Calc);
        let callable = vec![web.clone(), calc];
        let mut budget = ToolBudget::default();

        for _ in 0..MAX_WEB_TOOL_CALLS_PER_RUN {
            assert!(budget.admit(&web));
        }
        assert!(!budget.admit(&web));

        let tools = budget.native_tools(&callable);
        assert_eq!(tools.len(), 1);
        assert_eq!(tools[0]["function"]["name"], "calc_eval");
    }

    #[test]
    fn context_window_counts_complete_turns_and_zero_disables_it() {
        assert!(next_context_state(Some(0), vec![], "input", &json!({"reply":"x"})).is_none());
        let state = next_context_state(
            Some(1),
            vec![
                json!({"role":"user", "content":"old"}),
                json!({"role":"assistant", "content":"old reply"}),
            ],
            "new",
            &json!({"reply":"new reply"}),
        )
        .unwrap();
        let messages = state["messages"].as_array().unwrap();
        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0]["content"], "new");
    }

    #[test]
    fn final_output_parser_accepts_objects_but_not_other_json_values() {
        assert_eq!(
            parse_json_object("```json\n{\"reply\":\"ok\"}\n```").unwrap(),
            json!({"reply":"ok"})
        );
        assert_eq!(
            parse_json_object("answer: {\"reply\":\"ok\"}").unwrap(),
            json!({"reply":"ok"})
        );
        assert!(parse_json_object("\"plain string\"").is_none());
        assert!(parse_json_object("[1,2,3]").is_none());
    }

    #[test]
    fn graph_query_inherits_the_agent_memory_namespace() {
        assert_eq!(
            inject_runtime_context(
                "graph_query",
                json!({"entity":"agentd"}),
                "assistant",
                "chat"
            ),
            json!({"entity":"agentd","namespace":"assistant"})
        );
    }

    #[test]
    fn maintainer_scan_requires_bound_complete_pagination_before_mutation() {
        let mut scan = MemoryMaintenanceScan::for_run(
            "system/memory-maintainer",
            &json!({"namespace":"profile"}),
        )
        .unwrap();
        assert!(scan.ensure_complete().is_err());
        assert!(scan
            .validate_tool_call("memory_delete", &json!({"namespace":"profile","id":"old"}),)
            .is_err());
        assert!(scan
            .validate_tool_call("memory_list", &json!({"namespace":"other"}))
            .is_err());

        scan.validate_tool_call("memory_list", &json!({"namespace":"profile"}))
            .unwrap();
        scan.observe_list_result(
            "memory_list",
            &ToolResult {
                ok: true,
                result: json!({"items":[],"next_cursor":"cursor-1"}),
                error: None,
            },
        )
        .unwrap();
        assert!(scan.ensure_complete().is_err());
        assert!(scan
            .validate_tool_call(
                "memory_list",
                &json!({"namespace":"profile","cursor":"wrong"}),
            )
            .is_err());

        scan.validate_tool_call(
            "memory_list",
            &json!({"namespace":"profile","cursor":"cursor-1"}),
        )
        .unwrap();
        scan.observe_list_result(
            "memory_list",
            &ToolResult {
                ok: true,
                result: json!({"items":[],"next_cursor":null}),
                error: None,
            },
        )
        .unwrap();
        scan.ensure_complete().unwrap();
        scan.validate_tool_call("memory_delete", &json!({"namespace":"profile","id":"old"}))
            .unwrap();
    }

    #[tokio::test]
    async fn native_loop_commits_output_context_trace_and_delivery() {
        async fn completion() -> Json<serde_json::Value> {
            Json(json!({
                "choices":[{"message":{"role":"assistant","content":"{\"reply\":\"ok\"}"}}]
            }))
        }
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            axum::serve(
                listener,
                Router::new().route("/v1/chat/completions", post(completion)),
            )
            .await
            .unwrap();
        });

        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("agentd.db");
        let store = AgentdStore::new(database.to_str().unwrap()).await.unwrap();
        store.create_tenant("demo", &json!({})).await.unwrap();
        store
            .apply_agent(&AgentResource {
                metadata: ResourceMeta {
                    name: "bot".into(),
                    tenant: "demo".into(),
                    labels: BTreeMap::new(),
                },
                spec: AgentSpec {
                    allowed_families: Some(vec![]),
                    limits: AgentLimits {
                        timeout_ms: 5_000,
                        max_steps: 2,
                    },
                    system_prompt: None,
                    model: Some("test".into()),
                    temperature: None,
                    max_tokens: None,
                    context_window: Some(1),
                },
            })
            .await
            .unwrap();
        let caps = CapabilityEngine::new_with_config(
            store.clone(),
            CapabilityEngineConfig {
                llm_api_base: Some(format!("http://{address}/v1")),
                llm_api_key: None,
                llm_model: Some("test".into()),
                ..CapabilityEngineConfig::default()
            },
        );
        let input = json!({"text":"hello"});
        let run_id = store
            .submit_run(NewRun {
                tenant: "demo",
                name: "turn",
                agent_ref: "bot",
                scope: "chat/1",
                source: "test",
                input: &input,
                request_id: None,
                schedule_name: None,
                delivery_destination: Some("test:1"),
            })
            .await
            .unwrap();
        let assigned = store.claim_next_run().await.unwrap().unwrap();
        let report = RuntimeEngine::new(caps, store.clone())
            .execute_assigned_run(&assigned)
            .await
            .unwrap();

        assert!(report.error.is_none());
        assert_eq!(
            store.get_run_output(run_id).await.unwrap(),
            Some(json!({"reply":"ok"}))
        );
        assert_eq!(
            store
                .get_context_state("demo", "bot", "chat/1")
                .await
                .unwrap()
                .unwrap()
                .state["messages"]
                .as_array()
                .unwrap()
                .len(),
            2
        );
        let trace = store.list_run_log(run_id).await.unwrap();
        assert_eq!(
            trace
                .iter()
                .map(|item| item.kind.as_str())
                .collect::<Vec<_>>(),
            ["model", "model", "output", "status"]
        );
        assert_eq!(
            store
                .list_delivery_outbox(Some("demo"), None, Some(run_id), 10)
                .await
                .unwrap()
                .len(),
            1
        );
        assert!(store
            .get_memory("demo", "bot", "anything")
            .await
            .unwrap()
            .is_none());
        server.abort();
    }

    #[tokio::test]
    async fn maintainer_cannot_finish_without_calling_memory_list() {
        async fn completion() -> Json<serde_json::Value> {
            Json(json!({
                "choices":[{"message":{"role":"assistant","content":"{\"namespace\":\"profile\",\"scanned\":0}"}}]
            }))
        }
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            axum::serve(
                listener,
                Router::new().route("/v1/chat/completions", post(completion)),
            )
            .await
            .unwrap();
        });

        let directory = tempfile::tempdir().unwrap();
        let store = AgentdStore::new(directory.path().join("agentd.db").to_str().unwrap())
            .await
            .unwrap();
        store.create_tenant("demo", &json!({})).await.unwrap();
        store
            .apply_agent(&AgentResource {
                metadata: ResourceMeta {
                    name: "system/memory-maintainer".into(),
                    tenant: "demo".into(),
                    labels: BTreeMap::new(),
                },
                spec: AgentSpec {
                    allowed_families: Some(vec![ToolFamily::Memory]),
                    limits: AgentLimits {
                        timeout_ms: 5_000,
                        max_steps: 2,
                    },
                    system_prompt: None,
                    model: Some("standard/chat".into()),
                    temperature: None,
                    max_tokens: None,
                    context_window: Some(0),
                },
            })
            .await
            .unwrap();
        let caps = CapabilityEngine::new_with_config(
            store.clone(),
            CapabilityEngineConfig {
                llm_api_base: Some(format!("http://{address}/v1")),
                llm_api_key: None,
                llm_model: Some("test".into()),
                ..CapabilityEngineConfig::default()
            },
        );
        let input = json!({"namespace":"profile"});
        let run_id = store
            .submit_run(NewRun {
                tenant: "demo",
                name: "maintenance",
                agent_ref: "system/memory-maintainer",
                scope: "memory-maintenance/profile",
                source: "test",
                input: &input,
                request_id: None,
                schedule_name: None,
                delivery_destination: None,
            })
            .await
            .unwrap();
        let assigned = store.claim_next_run().await.unwrap().unwrap();
        let report = RuntimeEngine::new(caps, store.clone())
            .execute_assigned_run(&assigned)
            .await
            .unwrap();

        assert!(report
            .error
            .as_deref()
            .unwrap()
            .contains("cannot finish before calling memory_list"));
        assert!(store.get_run_output(run_id).await.unwrap().is_none());
        assert!(store
            .list_run_log(run_id)
            .await
            .unwrap()
            .iter()
            .all(|event| event.kind == "model"));
        server.abort();
    }

    #[tokio::test]
    async fn memory_changes_only_through_traced_model_tool_calls() {
        async fn completion(Json(body): Json<serde_json::Value>) -> Json<serde_json::Value> {
            let observed_tool = body["messages"]
                .as_array()
                .unwrap()
                .iter()
                .any(|message| message["role"] == "tool");
            if observed_tool {
                Json(json!({
                    "choices":[{"message":{"role":"assistant","content":"{\"reply\":\"stored\"}"}}]
                }))
            } else {
                Json(json!({
                    "choices":[{"message":{
                        "role":"assistant",
                        "content":null,
                        "tool_calls":[
                            {
                                "id":"memory-list-call",
                                "type":"function",
                                "function":{
                                    "name":"memory_list",
                                    "arguments":"{\"limit\":50}"
                                }
                            },
                            {
                                "id":"memory-put-call",
                                "type":"function",
                                "function":{
                                    "name":"memory_put",
                                    "arguments":"{\"id\":\"favorite-fruit\",\"text\":\"likes durian\"}"
                                }
                            }
                        ]
                    }}]
                }))
            }
        }
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            axum::serve(
                listener,
                Router::new().route("/v1/chat/completions", post(completion)),
            )
            .await
            .unwrap();
        });

        let directory = tempfile::tempdir().unwrap();
        let store = AgentdStore::new(directory.path().join("agentd.db").to_str().unwrap())
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
                    allowed_families: Some(vec![ToolFamily::Memory]),
                    limits: AgentLimits {
                        timeout_ms: 5_000,
                        max_steps: 3,
                    },
                    system_prompt: None,
                    model: Some("test".into()),
                    temperature: None,
                    max_tokens: None,
                    context_window: Some(1),
                },
            })
            .await
            .unwrap();
        let caps = CapabilityEngine::new_with_config(
            store.clone(),
            CapabilityEngineConfig {
                llm_api_base: Some(format!("http://{address}/v1")),
                llm_api_key: None,
                llm_model: Some("test".into()),
                ..CapabilityEngineConfig::default()
            },
        )
        .with_test_embedding(|_| {
            let mut embedding = vec![0.0; agentd_store::MEMORY_EMBEDDING_DIM];
            embedding[0] = 1.0;
            Ok(embedding)
        });
        let input = json!({"text":"remember that I like durian"});
        let run_id = store
            .submit_run(NewRun {
                tenant: "demo",
                name: "turn",
                agent_ref: "bot",
                scope: "chat/1",
                source: "test",
                input: &input,
                request_id: None,
                schedule_name: None,
                delivery_destination: None,
            })
            .await
            .unwrap();
        let assigned = store.claim_next_run().await.unwrap().unwrap();
        let report = RuntimeEngine::new(caps, store.clone())
            .execute_assigned_run(&assigned)
            .await
            .unwrap();
        assert!(report.error.is_none(), "{:?}", report.error);
        assert_eq!(
            store
                .get_memory("demo", "bot", "favorite-fruit")
                .await
                .unwrap()
                .unwrap()
                .text,
            "likes durian"
        );
        let trace = store.list_run_log(run_id).await.unwrap();
        let tool_events = trace
            .iter()
            .filter(|event| event.kind == "tool")
            .collect::<Vec<_>>();
        assert_eq!(tool_events.len(), 4);
        assert_eq!(tool_events[0].payload["name"], "memory_list");
        assert_eq!(tool_events[0].payload["arguments"]["namespace"], "bot");
        assert_eq!(tool_events[2].payload["name"], "memory_put");
        assert_eq!(tool_events[2].payload["arguments"]["namespace"], "bot");
        server.abort();
    }
}
