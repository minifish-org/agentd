use crate::CapabilityEngine;
use anyhow::{anyhow, Result};
use serde_json::{json, Value};

fn required<'a>(params: &'a Value, field: &str) -> Result<&'a str> {
    params
        .get(field)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| anyhow!("graph_query requires {field}"))
}

fn namespace(params: &Value) -> &str {
    params
        .get("namespace")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("default")
}

impl CapabilityEngine {
    pub(crate) async fn execute_graph_query(&self, tenant: &str, params: &Value) -> Result<Value> {
        let relation = params
            .get("relation")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty());
        let direction = params
            .get("direction")
            .and_then(Value::as_str)
            .unwrap_or("both");
        let result = self
            .store
            .query_graph(
                tenant,
                namespace(params),
                agentd_store::GraphQuery {
                    entity: required(params, "entity")?,
                    relation,
                    direction,
                    max_hops: params.get("max_hops").and_then(Value::as_u64).unwrap_or(2) as usize,
                    limit: params.get("limit").and_then(Value::as_u64).unwrap_or(50) as usize,
                },
            )
            .await?;
        Ok(json!({"graph":result}))
    }
}
