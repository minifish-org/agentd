use anyhow::{anyhow, Result};
use chrono::{DateTime, FixedOffset, Local, Offset, Utc};
use chrono_tz::Tz;

pub(crate) enum ResolvedTimezone {
    Named(String, Tz),
    Fixed(String, FixedOffset),
}

impl ResolvedTimezone {
    pub(crate) fn name(&self) -> &str {
        match self {
            Self::Named(name, _) | Self::Fixed(name, _) => name,
        }
    }

    pub(crate) fn local_time(&self, utc: DateTime<Utc>) -> DateTime<FixedOffset> {
        match self {
            Self::Named(_, timezone) => utc.with_timezone(timezone).fixed_offset(),
            Self::Fixed(_, offset) => utc.with_timezone(offset),
        }
    }
}

pub(crate) fn resolve_timezone(value: Option<&str>) -> Result<ResolvedTimezone> {
    if let Some(value) = value.map(str::trim).filter(|value| !value.is_empty()) {
        if let Ok(timezone) = value.parse::<Tz>() {
            return Ok(ResolvedTimezone::Named(value.to_string(), timezone));
        }
        if let Some(offset) = parse_fixed_offset(value) {
            return Ok(ResolvedTimezone::Fixed(value.to_string(), offset));
        }
        return Err(anyhow!("invalid timezone: {value}"));
    }
    let offset = Local::now().offset().fix();
    Ok(ResolvedTimezone::Fixed(format!("UTC{offset}"), offset))
}

fn parse_fixed_offset(value: &str) -> Option<FixedOffset> {
    let raw = value.strip_prefix("UTC").unwrap_or(value);
    let sign = match raw.as_bytes().first().copied()? {
        b'+' => 1,
        b'-' => -1,
        _ => return None,
    };
    let (hours, minutes) = raw[1..].split_once(':').unwrap_or((&raw[1..], "0"));
    let seconds = sign * (hours.parse::<i32>().ok()? * 3600 + minutes.parse::<i32>().ok()? * 60);
    FixedOffset::east_opt(seconds)
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
