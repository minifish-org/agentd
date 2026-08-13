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
        let expr = params
            .get("expr")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow!("calc.eval requires params.expr"))?;
        let trimmed = expr.trim();
        if trimmed.is_empty() {
            return Err(anyhow!("calc.eval: expr must not be empty"));
        }
        // meval handles arithmetic + math functions + pi/e. Errors are
        // surfaced verbatim so the LLM sees a useful "unknown function
        // 'fooBar' at position N" rather than a generic failure.
        let result = meval::eval_str(trimmed)
            .map_err(|e| anyhow!("calc.eval: invalid expression: {}", e))?;
        if !result.is_finite() {
            return Err(anyhow!(
                "calc.eval: expression evaluated to non-finite value ({result})"
            ));
        }
        Ok(serde_json::json!({
            "expr": trimmed,
            "result": result,
        }))
    }
}
