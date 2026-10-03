//! One pure validation boundary for runtime calls and offline decisions.
//! Compiled JSON Schema validation supports local references without external I/O.
use serde_json::Value;
use std::{
    collections::HashMap,
    sync::{Arc, LazyLock, Mutex, OnceLock},
};

pub(crate) fn validate_tool_input(
    name: &str,
    schema: &Value,
    params: &Value,
) -> Result<(), String> {
    validate_schema(schema, params)?;
    // Apply builtin parsing rules only to the matching host catalog contract.
    // MCP tools retain standard JSON Schema semantics (including 1.0 integers).
    static BUILTINS: LazyLock<HashMap<String, Value>> = LazyLock::new(|| {
        agentd_api::builtin_tool_catalog()
            .into_iter()
            .map(|tool| (tool.name, tool.input_schema))
            .collect()
    });
    if BUILTINS.get(name) != Some(schema) {
        return Ok(());
    }
    if let Some(properties) = schema.get("properties").and_then(Value::as_object) {
        for (field, field_schema) in properties {
            if field_schema.get("type").and_then(Value::as_str) == Some("integer")
                && params
                    .get(field)
                    .is_some_and(|value| value.as_u64().is_none())
            {
                return Err(format!(
                    "{field}: expected an unsigned integer representation"
                ));
            }
        }
    }
    if let Some(required) = schema.get("required").and_then(Value::as_array) {
        for field in required.iter().filter_map(Value::as_str) {
            if params
                .get(field)
                .and_then(Value::as_str)
                .is_some_and(|value| value.trim().is_empty())
            {
                return Err(format!("{field} must not be empty"));
            }
        }
    }
    match name {
        "sandbox_session" => {
            crate::sandbox::validate_input(params).map_err(|error| error.to_string())
        }
        "schedule_put" => {
            crate::handlers::schedule::validate_input(params).map_err(|error| error.to_string())
        }
        "clock_now" => {
            crate::time_utils::resolve_timezone(params.get("timezone").and_then(Value::as_str))
                .map(|_| ())
                .map_err(|error| error.to_string())
        }
        "artifact_write" => {
            crate::handlers::artifact::validate_input(params).map_err(|error| error.to_string())
        }
        "calc_eval" => {
            crate::handlers::calc::validate_input(params).map_err(|error| error.to_string())
        }
        "memory_put" => {
            crate::handlers::memory::validate_input(params).map_err(|error| error.to_string())
        }
        "artifact_read" => params
            .get("artifact_ref")
            .and_then(Value::as_str)
            .map(agentd_api::ArtifactRef::parse)
            .transpose()
            .map(|_| ())
            .map_err(|error| error.to_string()),
        "memory_list" => {
            crate::handlers::memory::validate_list_input(params).map_err(|error| error.to_string())
        }
        "web_fetch" => {
            crate::handlers::web::validate_fetch_input(params).map_err(|error| error.to_string())
        }
        _ => Ok(()),
    }
}

type CachedValidator = Result<Arc<jsonschema::Validator>, String>;
type ValidatorSlot = Arc<OnceLock<CachedValidator>>;
static VALIDATORS: LazyLock<Mutex<HashMap<String, ValidatorSlot>>> =
    LazyLock::new(Default::default);
const MAX_CACHED_SCHEMAS: usize = 128;

fn validate_schema(schema: &Value, value: &Value) -> Result<(), String> {
    let key = schema.to_string();
    let slot = {
        let mut cache = VALIDATORS
            .lock()
            .map_err(|_| "schema validator cache is poisoned")?;
        if cache.len() >= MAX_CACHED_SCHEMAS && !cache.contains_key(&key) {
            if let Some(evicted) = cache.keys().next().cloned() {
                cache.remove(&evicted);
            }
        }
        cache.entry(key).or_default().clone()
    };
    // OnceLock compiles each cached schema once, including concurrent first calls.
    // Cargo features disable HTTP/file resolution; local references are supported.
    let validator = slot
        .get_or_init(|| {
            jsonschema::validator_for(schema)
                .map(Arc::new)
                .map_err(|error| format!("invalid tool schema: {error}"))
        })
        .as_ref()
        .map_err(Clone::clone)?;
    validator
        .validate(value)
        .map_err(|error| format!("{}: {error}", error.instance_path()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn builtin_contract_rejects_invalid_decisions_before_execution() {
        let catalog = agentd_api::builtin_tool_catalog();
        for (name, input) in [
            ("sandbox_session", json!({"action":"shell"})),
            (
                "sandbox_session",
                json!({"action":"shell","script":" ","cwd":"/tmp"}),
            ),
            (
                "sandbox_session",
                json!({"action":"exec","command":"ls","args":[7]}),
            ),
            ("memory_search", json!({"query":"x","limit":"five"})),
            ("memory_search", json!({"query":" "})),
            ("memory_put", json!({"id":" ","text":"x"})),
            ("memory_search", json!({"query":"x","limit":1.0})),
            ("artifact_write", json!({"path":"report.txt"})),
            ("artifact_read", json!({"artifact_ref":"bogus"})),
            ("memory_list", json!({"cursor":"bogus"})),
            ("web_fetch", json!({"url":"bogus"})),
            ("web_fetch", json!({"url":"file:///tmp/test"})),
            (
                "graph_query",
                json!({"entity":"x","direction":"sideways","max_hops":999}),
            ),
            (
                "memory_put",
                json!({"id":"x","text":"x","graph":{"entities":[{"id":"x"}]}}),
            ),
            (
                "memory_put",
                json!({"id":"x","text":"x","graph":{"edges":[{"from":"missing","to":"other","relation":"depends_on"}]}}),
            ),
            ("schedule_put", json!({"name":"x","at":"tomorrow"})),
        ] {
            let tool = catalog.iter().find(|tool| tool.name == name).unwrap();
            assert!(
                validate_tool_input(name, &tool.input_schema, &input).is_err(),
                "{name}: {input}"
            );
        }
        let tool = catalog
            .iter()
            .find(|tool| tool.name == "schedule_put")
            .unwrap();
        assert!(validate_tool_input(&tool.name, &tool.input_schema, &json!({"name":"x","cron":"0 * * * *","timezone":"Asia/Singapore","agent_ref":"bot","scope":"chat"})).is_ok());
    }

    #[test]
    fn validates_nested_values_and_local_refs_without_external_resolution() {
        let schema = json!({"type":"object","required":["value"],"additionalProperties":false,"properties":{"value":{"anyOf":[{"type":"string","minLength":2},{"type":"integer","minimum":1,"maximum":3}]}}});
        for input in [json!({"value":"报告"}), json!({"value":2})] {
            assert!(validate_schema(&schema, &input).is_ok());
        }
        for input in [
            json!({"value":"a"}),
            json!({"value":4}),
            json!({"value":2,"extra":true}),
            json!({}),
        ] {
            assert!(validate_schema(&schema, &input).is_err());
        }
        let references = json!({"$defs":{"quantity":{"type":"integer","minimum":1}},"properties":{"count":{"$ref":"#/$defs/quantity"}}});
        assert!(validate_schema(&references, &json!({"count":2})).is_ok());
        assert!(validate_schema(&references, &json!({"count":0})).is_err());
        for reference in [
            "https://example.com/schema.json",
            "file:///tmp/agentd-schema.json",
        ] {
            assert!(validate_schema(&json!({"$ref":reference}), &json!({})).is_err());
        }
        assert!(validate_schema(&json!({"oneOf":[{},{}]}), &json!({})).is_err());
    }

    #[test]
    fn clock_optional_blank_timezone_keeps_the_handler_default() {
        let tool = agentd_api::builtin_tool_catalog()
            .into_iter()
            .find(|tool| tool.name == "clock_now")
            .unwrap();
        for input in [json!({}), json!({"timezone":""}), json!({"timezone":"   "})] {
            assert!(validate_tool_input(&tool.name, &tool.input_schema, &input).is_ok());
        }
        assert!(validate_tool_input(
            &tool.name,
            &tool.input_schema,
            &json!({"timezone":"+08:99"})
        )
        .is_err());
    }
}
