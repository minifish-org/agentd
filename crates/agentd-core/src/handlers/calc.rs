//! `calc_eval` — pure arithmetic expression evaluator.
//!
//! LLMs are notoriously bad at exact math (token-level decoding can't
//! carry carries). When the user asks for "1247 * 0.83" or compound
//! interest over 5 years, the LLM should call this tool instead of
//! guessing. Backed by `meval`: no variable assignment, no scripting,
//! no IO — just expressions over real numbers with the standard math
//! functions and constants.

use crate::CapabilityEngine;
use anyhow::{anyhow, Result};
use serde_json::Value;

impl CapabilityEngine {
    pub(crate) async fn execute_calc_eval(&self, params: &Value) -> Result<Value> {
        let (trimmed, result) = evaluate(params)?;
        Ok(serde_json::json!({
            "expr": trimmed,
            "result": result,
        }))
    }
}

fn evaluate(params: &Value) -> Result<(&str, f64)> {
    let expr = params
        .get("expression")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow!("calc.eval requires params.expression"))?;
    let trimmed = expr.trim();
    if trimmed.is_empty() {
        return Err(anyhow!("calc.eval: expression must not be empty"));
    }
    // meval handles arithmetic + math functions + pi/e. Errors are
    // surfaced verbatim so the LLM sees a useful "unknown function
    // 'fooBar' at position N" rather than a generic failure.
    let result =
        meval::eval_str(trimmed).map_err(|e| anyhow!("calc.eval: invalid expression: {}", e))?;
    if !result.is_finite() {
        return Err(anyhow!(
            "calc.eval: expression evaluated to non-finite value ({result})"
        ));
    }
    Ok((trimmed, result))
}

pub(crate) fn validate_input(params: &Value) -> Result<()> {
    evaluate(params).map(|_| ())
}

#[cfg(test)]
mod tests {
    use crate::{CapabilityEngine, CapabilityEngineConfig, RunExecutionContext};
    use agentd_api::builtin_tool_catalog;
    use agentd_store::AgentdStore;
    use serde_json::json;
    use std::time::Duration;
    use tokio::time::Instant;

    #[tokio::test]
    async fn calc_eval_executes_the_advertised_expression_contract() {
        let directory = tempfile::tempdir().unwrap();
        let store = AgentdStore::new(directory.path().join("agentd.db").to_str().unwrap())
            .await
            .unwrap();
        let engine = CapabilityEngine::new_with_config(store, CapabilityEngineConfig::default());
        let tool = builtin_tool_catalog()
            .into_iter()
            .find(|tool| tool.name == "calc_eval")
            .unwrap();
        let context = RunExecutionContext {
            run_id: uuid::Uuid::new_v4(),
            tenant: "calc-test".into(),
            agent_ref: "calculator".into(),
            scope: "test".into(),
            deadline: Instant::now() + Duration::from_secs(10),
        };

        // Exercise schema validation and dispatch together, using only the
        // argument the model is told to provide. Keep the existing result shape.
        let result = engine
            .execute_tool(
                &context,
                "calc-test",
                &tool,
                &json!({"expression":" (137*29)-48 "}),
            )
            .await;
        assert!(result.ok, "{:?}", result.error);
        assert_eq!(
            result.result,
            json!({"expr":"(137*29)-48", "result":3925.0})
        );

        for (arguments, expected_error) in [
            (json!({"expr":"1+1"}), "required property"),
            (json!({"expression":" "}), "expression must not be empty"),
            (json!({"expression":"1+"}), "invalid expression"),
            (json!({"expression":"1/0"}), "non-finite"),
        ] {
            let result = engine
                .execute_tool(&context, "calc-test", &tool, &arguments)
                .await;
            assert!(!result.ok, "accepted {arguments}");
            assert!(result.error.unwrap().contains(expected_error));
        }
    }
}
