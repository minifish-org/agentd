use crate::ApiError;
use chrono::{DateTime, FixedOffset, Utc};
use chrono_tz::Tz;

#[derive(Clone, Debug)]
pub enum ResolvedTimezone {
    Named(String, Tz),
    Fixed(String, FixedOffset),
}

impl ResolvedTimezone {
    pub fn parse(raw: &str) -> Result<Self, ApiError> {
        let value = raw.trim();
        if let Ok(timezone) = value.parse::<Tz>() {
            return Ok(Self::Named(value.into(), timezone));
        }
        if let Some(offset) = parse_timezone_offset(value).and_then(FixedOffset::east_opt) {
            return Ok(Self::Fixed(value.into(), offset));
        }
        Err(ApiError::Validation(format!("invalid timezone: {value}")))
    }

    pub fn name(&self) -> &str {
        match self {
            Self::Named(name, _) | Self::Fixed(name, _) => name,
        }
    }

    pub fn local_time(&self, utc: DateTime<Utc>) -> DateTime<FixedOffset> {
        match self {
            Self::Named(_, timezone) => utc.with_timezone(timezone).fixed_offset(),
            Self::Fixed(_, offset) => utc.with_timezone(offset),
        }
    }
}

pub fn validate_timezone_name(raw: &str) -> Result<(), ApiError> {
    ResolvedTimezone::parse(raw).map(|_| ())
}

/// UTC/GMT offsets use HH, HHMM or HH:MM; offsets must be less than 24 hours.
pub fn parse_timezone_offset(raw: &str) -> Option<i32> {
    let raw = raw.trim();
    let normalized = raw
        .strip_prefix("UTC")
        .or_else(|| raw.strip_prefix("GMT"))
        .unwrap_or(raw);
    if normalized == "Z" {
        return Some(0);
    }
    let (sign, digits) = if let Some(rest) = normalized.strip_prefix('+') {
        (1, rest)
    } else if let Some(rest) = normalized.strip_prefix('-') {
        (-1, rest)
    } else {
        return None;
    };
    if !digits.is_ascii() {
        return None;
    }
    let (hours, minutes) = if let Some(parts) = digits.split_once(':') {
        parts
    } else if digits.len() == 4 {
        digits.split_at(2)
    } else if digits.len() <= 2 {
        (digits, "0")
    } else {
        return None;
    };
    if hours.is_empty()
        || hours.len() > 2
        || minutes.is_empty()
        || minutes.len() > 2
        || !hours.bytes().all(|byte| byte.is_ascii_digit())
        || !minutes.bytes().all(|byte| byte.is_ascii_digit())
    {
        return None;
    }
    let hours: i32 = hours.parse().ok()?;
    let minutes: i32 = minutes.parse().ok()?;
    if hours > 23 || minutes > 59 {
        return None;
    }
    Some(sign * (hours * 3600 + minutes * 60))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn validates_and_resolves_the_same_timezone_contract() {
        for raw in [
            "Asia/Singapore",
            "GMT+08:00",
            "+0800",
            "UTC-5",
            "Z",
            "+23:59",
        ] {
            assert!(validate_timezone_name(raw).is_ok());
            assert!(ResolvedTimezone::parse(raw).is_ok());
        }
        for raw in [
            "",
            "+😀",
            "+2147483647:00",
            "+08:99",
            "+99:00",
            "+24:00",
            "+-1:00",
            "+00:-1",
            "+08:00:00",
        ] {
            assert!(validate_timezone_name(raw).is_err(), "{raw}");
            assert!(parse_timezone_offset(raw).is_none(), "{raw}");
        }
        assert_eq!(parse_timezone_offset("GMT+08:00"), Some(28800));
        assert_eq!(parse_timezone_offset("-0530"), Some(-19800));
    }
}
