use anyhow::{anyhow, Result};
use chrono::{Local, Offset};

pub(crate) fn resolve_timezone(value: Option<&str>) -> Result<agentd_api::ResolvedTimezone> {
    if let Some(value) = value.map(str::trim).filter(|value| !value.is_empty()) {
        return agentd_api::ResolvedTimezone::parse(value).map_err(Into::into);
    }
    let offset = Local::now().offset().fix();
    Ok(agentd_api::ResolvedTimezone::Fixed(
        format!("UTC{offset}"),
        offset,
    ))
}

pub(crate) fn schedule_name_from_params(
    params: &serde_json::Value,
    operation: &str,
) -> Result<String> {
    params
        .get("name")
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
        .ok_or_else(|| anyhow!("{operation} requires name"))
}
