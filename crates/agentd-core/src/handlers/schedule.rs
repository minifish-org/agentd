use crate::time_utils::schedule_name_from_params;
use crate::CapabilityEngine;
use agentd_api::{DeliveryRequest, ScheduleSpec};
use anyhow::{anyhow, Result};
use chrono::{DateTime, Utc};

impl CapabilityEngine {
    pub(crate) async fn execute_schedule_get(
        &self,
        tenant: &str,
        params: &serde_json::Value,
    ) -> Result<serde_json::Value> {
        let name = schedule_name_from_params(params, "schedule_get")?;
        let schedule = self
            .store
            .get_schedule(tenant, &name)
            .await?
            .ok_or_else(|| anyhow!("schedule not found"))?;
        ensure_owner(&schedule.spec, params)?;
        Ok(serde_json::to_value(schedule)?)
    }

    pub(crate) async fn execute_schedule_list(
        &self,
        tenant: &str,
        params: &serde_json::Value,
    ) -> Result<serde_json::Value> {
        let agent = required_string(params, "agent_ref")?;
        let scope = required_string(params, "scope")?;
        let schedules = self
            .store
            .list_schedules(Some(tenant))
            .await?
            .into_iter()
            .filter(|schedule| schedule.spec.agent_ref == agent && schedule.spec.scope == scope)
            .collect::<Vec<_>>();
        Ok(serde_json::to_value(schedules)?)
    }

    pub(crate) async fn execute_schedule_put(
        &self,
        tenant: &str,
        params: &serde_json::Value,
    ) -> Result<serde_json::Value> {
        let name = schedule_name_from_params(params, "schedule_put")?;
        let spec = ScheduleSpec {
            agent_ref: required_string(params, "agent_ref")?,
            scope: required_string(params, "scope")?,
            payload: params.get("payload").cloned().unwrap_or_default(),
            delivery: params
                .get("delivery")
                .cloned()
                .map(serde_json::from_value::<DeliveryRequest>)
                .transpose()?,
            at: params
                .get("at")
                .and_then(serde_json::Value::as_str)
                .map(|value| {
                    DateTime::parse_from_rfc3339(value).map(|value| value.with_timezone(&Utc))
                })
                .transpose()?,
            cron: optional_string(params, "cron"),
            timezone: optional_string(params, "timezone"),
            enabled: params
                .get("enabled")
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(true),
        };
        spec.validate().map_err(|error| anyhow!(error))?;
        let store = &self.store;
        store.put_schedule(tenant, &name, &spec).await?;
        Ok(serde_json::to_value(
            store
                .get_schedule(tenant, &name)
                .await?
                .expect("schedule was just stored"),
        )?)
    }

    pub(crate) async fn execute_schedule_delete(
        &self,
        tenant: &str,
        params: &serde_json::Value,
    ) -> Result<serde_json::Value> {
        let name = schedule_name_from_params(params, "schedule_delete")?;
        let schedule = self
            .store
            .get_schedule(tenant, &name)
            .await?
            .ok_or_else(|| anyhow!("schedule not found"))?;
        ensure_owner(&schedule.spec, params)?;
        self.store.delete_schedule(tenant, &name).await
    }
}

fn required_string(params: &serde_json::Value, key: &str) -> Result<String> {
    optional_string(params, key).ok_or_else(|| anyhow!("schedule_put requires {key}"))
}

fn optional_string(params: &serde_json::Value, key: &str) -> Option<String> {
    params
        .get(key)
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
}

fn ensure_owner(spec: &ScheduleSpec, params: &serde_json::Value) -> Result<()> {
    if spec.agent_ref != required_string(params, "agent_ref")?
        || spec.scope != required_string(params, "scope")?
    {
        return Err(anyhow!("schedule not found"));
    }
    Ok(())
}
