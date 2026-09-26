//! Whitelisted summaries for the durable audit stream. Full content belongs
//! in the protected resource/run trace, never in this cross-resource index.
use super::*;

pub(super) fn outcome(changed: bool) -> &'static str {
    if changed {
        "succeeded"
    } else {
        "noop"
    }
}

pub(super) fn agent_spec(spec: &agentd_api::AgentSpec) -> serde_json::Value {
    let allowed_families = spec
        .allowed_families
        .as_ref()
        .map(|families| families.iter().collect::<BTreeSet<_>>());
    json!({
        "allowed_families": allowed_families,
        "timeout_ms": spec.limits.timeout_ms,
        "max_steps": spec.limits.max_steps,
        "temperature": spec.temperature,
        "max_tokens": spec.max_tokens,
        "context_window": spec.context_window,
        "model_configured": spec.model.is_some(),
        "system_prompt_bytes": spec.system_prompt.as_ref().map_or(0, String::len),
    })
}

pub(super) fn agent_changed_fields(
    before: Option<&agentd_api::AgentSpec>,
    after: &agentd_api::AgentSpec,
) -> Vec<&'static str> {
    let Some(before) = before else {
        return vec!["created"];
    };
    let mut fields = Vec::new();
    macro_rules! changed {
        ($field:ident) => {
            if before.$field != after.$field {
                fields.push(stringify!($field));
            }
        };
    }
    changed!(allowed_families);
    changed!(limits);
    changed!(system_prompt);
    changed!(model);
    changed!(temperature);
    changed!(max_tokens);
    changed!(context_window);
    fields
}

pub(super) fn run_error_code(error: &str) -> &'static str {
    if error == "agentd restarted" {
        "runtime_restarted"
    } else if error == "run timeout exceeded" {
        "timeout"
    } else {
        "execution_failed"
    }
}

pub(super) fn trace_summary(
    kind: &str,
    payload: &serde_json::Value,
    trace_id: i64,
) -> serde_json::Value {
    let mut details = json!({"trace_id":trace_id,"kind":kind});
    // Tool names and call IDs can be arbitrary model output. Keep them in the
    // protected trace and index only numeric counters and known runtime enums.
    for key in ["step", "revision"] {
        if let Some(value) = payload.get(key).and_then(serde_json::Value::as_u64) {
            details[key] = json!(value);
        }
    }
    for (key, allowed) in [
        (
            "phase",
            &["request", "response", "call", "result", "error"][..],
        ),
        ("stage", &["propose", "baseline", "candidate", "judge"][..]),
    ] {
        if let Some(value) = payload.get(key).and_then(serde_json::Value::as_str) {
            if allowed.contains(&value) {
                details[key] = json!(value);
            }
        }
    }
    if matches!(kind, "maintenance_check" | "behavior_check") {
        if let Some(ready) = payload.get("ready").and_then(serde_json::Value::as_bool) {
            details["ready"] = json!(ready);
        }
        if let Some(reason) = payload.get("reason").and_then(serde_json::Value::as_str) {
            if [
                "too_few_entries",
                "unchanged",
                "insufficient_samples",
                "insufficient_independent_scopes",
                "insufficient_call_budget",
            ]
            .contains(&reason)
            {
                details["reason"] = json!(reason);
            }
        }
        let counts = if kind == "behavior_check" {
            &payload["details"]
        } else {
            payload
        };
        for key in [
            "entries",
            "min_entries",
            "external_revision",
            "consumed_revision",
            "usable_runs",
            "required_runs",
            "usable_scopes",
            "required_scopes",
            "required_calls",
            "call_limit",
        ] {
            if let Some(value) = counts.get(key).and_then(serde_json::Value::as_u64) {
                details[key] = json!(value);
            }
        }
    }
    details
}
