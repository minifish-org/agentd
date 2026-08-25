use crate::{ToolFamily, ToolSpec};
use serde_json::{json, Value};

/// The complete host-builtin catalog. Runtime mechanics are deliberately not
/// tools: every entry below is a real capability the model may exercise.
pub fn builtin_tool_catalog() -> Vec<ToolSpec> {
    let mut tools = Vec::with_capacity(16);

    add(
        &mut tools,
        ToolFamily::Artifact,
        "read",
        "Read a tenant artifact.",
        false,
        json!({"type":"object","required":["artifact_ref"],"properties":{
            "artifact_ref":{"type":"string","minLength":1},
            "encoding":{"type":"string","enum":["auto","json","text","base64"]}
        }}),
    );
    add(
        &mut tools,
        ToolFamily::Artifact,
        "write",
        "Write inline content to a tenant artifact.",
        true,
        json!({"type":"object","required":["path"],"properties":{
            "path":{"type":"string","minLength":1},
            "body_text":{"type":"string"},"body_json":{},"body_base64":{"type":"string"},
            "content_type":{"type":"string"}
        }}),
    );
    add(
        &mut tools,
        ToolFamily::Artifact,
        "list",
        "List tenant artifacts beneath a prefix.",
        false,
        json!({"type":"object","properties":{
            "prefix":{"type":"string"},"limit":{"type":"integer","minimum":1}
        }}),
    );

    add(
        &mut tools,
        ToolFamily::Memory,
        "get",
        "Read one durable memory item.",
        false,
        json!({"type":"object","required":["id"],"properties":{
            "id":{"type":"string","minLength":1},"namespace":{"type":"string"}
        }}),
    );
    add(
        &mut tools,
        ToolFamily::Memory,
        "search",
        "Search durable memory by keywords and semantic meaning. Use this when stable facts, preferences, or constraints may be missing from current context.",
        false,
        json!({"type":"object","required":["query"],"properties":{
            "query":{"type":"string","minLength":1},"namespace":{"type":"string"},
            "limit":{"type":"integer","minimum":1,"maximum":5}
        }}),
    );
    add(
        &mut tools,
        ToolFamily::Memory,
        "list",
        "List one namespace of durable memory in stable, bounded pages. Use this for audits and maintenance; use memory_search for relevance retrieval.",
        false,
        json!({"type":"object","properties":{
            "namespace":{"type":"string"},
            "limit":{"type":"integer","minimum":1,"maximum":100},
            "cursor":{"type":"string","minLength":1}
        }}),
    );
    add(
        &mut tools,
        ToolFamily::Memory,
        "put",
        "Create or replace one concise durable fact under a stable id. Store long content as an artifact.",
        true,
        json!({"type":"object","required":["id","text"],"properties":{
            "id":{"type":"string","minLength":1},"text":{"type":"string","minLength":1},
            "namespace":{"type":"string"}
        }}),
    );
    add(
        &mut tools,
        ToolFamily::Memory,
        "delete",
        "Delete one durable memory item.",
        true,
        json!({"type":"object","required":["id"],"properties":{
            "id":{"type":"string","minLength":1},"namespace":{"type":"string"}
        }}),
    );

    add(
        &mut tools,
        ToolFamily::Schedule,
        "get",
        "Read one schedule owned by this agent and scope.",
        false,
        named_schema(),
    );
    add(
        &mut tools,
        ToolFamily::Schedule,
        "list",
        "List schedules owned by this agent and scope.",
        false,
        json!({"type":"object","properties":{"limit":{"type":"integer","minimum":1}}}),
    );
    add(
        &mut tools,
        ToolFamily::Schedule,
        "put",
        "Create or replace an at/cron schedule for this agent and scope.",
        true,
        json!({"type":"object","required":["name"],"properties":{
            "name":{"type":"string","minLength":1},"payload":{},"enabled":{"type":"boolean"},
            "at":{"type":"string"},"cron":{"type":"string"},"timezone":{"type":"string"},
            "delivery":{"type":"object","required":["destination"],"properties":{
                "destination":{"type":"string","minLength":1}
            }}
        }}),
    );
    add(
        &mut tools,
        ToolFamily::Schedule,
        "delete",
        "Delete one schedule owned by this agent and scope.",
        true,
        named_schema(),
    );

    add(
        &mut tools,
        ToolFamily::Clock,
        "now",
        "Return the current time in an optional IANA timezone.",
        false,
        json!({"type":"object","properties":{"timezone":{"type":"string"}}}),
    );
    add(
        &mut tools,
        ToolFamily::Web,
        "search",
        "Search the public web.",
        false,
        json!({"type":"object","required":["query"],"properties":{
            "query":{"type":"string","minLength":1},"limit":{"type":"integer","minimum":1}
        }}),
    );
    add(
        &mut tools,
        ToolFamily::Web,
        "fetch",
        "Fetch bounded text from a public HTTP(S) URL.",
        false,
        json!({"type":"object","required":["url"],"properties":{
            "url":{"type":"string","minLength":1}
        }}),
    );
    add(
        &mut tools,
        ToolFamily::Calc,
        "eval",
        "Evaluate a pure arithmetic expression.",
        false,
        json!({"type":"object","required":["expression"],"properties":{
            "expression":{"type":"string","minLength":1}
        }}),
    );

    tools
}

pub fn visible_tools(catalog: &[ToolSpec], allowed_families: &[ToolFamily]) -> Vec<ToolSpec> {
    let effective: std::collections::BTreeSet<ToolFamily> =
        allowed_families.iter().cloned().collect();
    catalog
        .iter()
        .filter(|tool| effective.contains(&tool.family))
        .cloned()
        .collect()
}

fn named_schema() -> Value {
    json!({"type":"object","required":["name"],"properties":{
        "name":{"type":"string","minLength":1}
    }})
}

fn add(
    tools: &mut Vec<ToolSpec>,
    family: ToolFamily,
    action: &str,
    description: &str,
    mutating: bool,
    input_schema: Value,
) {
    tools.push(ToolSpec {
        name: format!("{}_{}", family.as_str(), action),
        family,
        description: description.to_string(),
        input_schema,
        mutating,
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn catalog_contains_only_the_sixteen_real_capabilities() {
        let tools = builtin_tool_catalog();
        assert_eq!(tools.len(), 16);
        assert!(tools
            .iter()
            .all(|tool| ToolFamily::all().contains(&tool.family)));
        assert!(tools
            .iter()
            .any(|tool| tool.name == "memory_put" && tool.mutating));
        assert!(visible_tools(&tools, &[]).is_empty());
        assert_eq!(visible_tools(&tools, &[ToolFamily::Memory]).len(), 5);
    }
}
