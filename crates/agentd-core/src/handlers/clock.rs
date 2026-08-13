use crate::time_utils::resolve_timezone;
use crate::CapabilityEngine;
use anyhow::Result;
use chrono::Utc;

impl CapabilityEngine {
    pub(crate) async fn execute_clock_now(
        &self,
        params: &serde_json::Value,
    ) -> Result<serde_json::Value> {
        let timezone = resolve_timezone(params.get("timezone").and_then(|value| value.as_str()))?;
        let utc = Utc::now();
        Ok(serde_json::json!({
            "utc": utc.to_rfc3339(),
            "local": timezone.local_time(utc).to_rfc3339(),
            "timezone": timezone.name(),
            "timestamp_ms": utc.timestamp_millis(),
        }))
    }
}
