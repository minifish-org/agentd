use crate::ApiError;
use std::fmt;

/// A database artifact key. Normalization preserves existing stored keys.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ArtifactPath(String);

impl ArtifactPath {
    pub fn parse(raw: &str) -> Result<Self, ApiError> {
        let path = raw.trim().trim_start_matches('/');
        if path.is_empty() || path.split('/').any(|part| part == "..") {
            return Err(ApiError::Validation("invalid artifact path".into()));
        }
        Ok(Self(path.into()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// An opaque artifact URI; parsing never normalizes dot segments in database keys.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ArtifactRef {
    tenant: String,
    path: ArtifactPath,
}

impl ArtifactRef {
    pub fn new(tenant: &str, path: &ArtifactPath) -> Self {
        Self {
            tenant: tenant.into(),
            path: path.clone(),
        }
    }

    pub fn parse(raw: &str) -> Result<Self, ApiError> {
        let (tenant, path) = raw
            .strip_prefix("artifact://")
            .and_then(|value| value.split_once('/'))
            .ok_or_else(|| {
                ApiError::Validation("artifact_ref must use artifact://tenant/path".into())
            })?;
        if tenant.is_empty() || raw.contains(['?', '#']) {
            return Err(ApiError::Validation("invalid artifact_ref".into()));
        }
        Ok(Self {
            tenant: decode(tenant)?,
            path: ArtifactPath::parse(&decode(path)?)?,
        })
    }

    pub fn path_for_tenant(&self, tenant: &str) -> Result<&ArtifactPath, ApiError> {
        if self.tenant != tenant {
            return Err(ApiError::Validation("artifact_ref tenant mismatch".into()));
        }
        Ok(&self.path)
    }
}

impl fmt::Display for ArtifactRef {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "artifact://{}/{}",
            encode(&self.tenant, false),
            encode(self.path.as_str(), true)
        )
    }
}

fn encode(raw: &str, keep_slashes: bool) -> String {
    use fmt::Write;
    let mut encoded = String::new();
    for byte in raw.bytes() {
        if byte.is_ascii_alphanumeric() || b"-._~".contains(&byte) || (keep_slashes && byte == b'/')
        {
            encoded.push(byte as char);
        } else {
            write!(encoded, "%{byte:02X}").expect("writing to a String cannot fail");
        }
    }
    encoded
}

fn decode(raw: &str) -> Result<String, ApiError> {
    let mut bytes = Vec::with_capacity(raw.len());
    let mut input = raw.bytes();
    while let Some(byte) = input.next() {
        if byte == b'%' {
            let mut digit = || input.next().and_then(|byte| (byte as char).to_digit(16));
            let high = digit()
                .ok_or_else(|| ApiError::Validation("invalid artifact_ref escape".into()))?;
            let low = digit()
                .ok_or_else(|| ApiError::Validation("invalid artifact_ref escape".into()))?;
            bytes.push((high * 16 + low) as u8);
        } else {
            bytes.push(byte);
        }
    }
    String::from_utf8(bytes)
        .map_err(|_| ApiError::Validation("artifact_ref path must be UTF-8".into()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn references_round_trip_database_keys_without_url_normalization() {
        for raw in [
            "报告.txt",
            "my report.txt",
            "a#b?c%41.txt",
            "a/./b.txt",
            "a//b",
            "a%2Fb",
            "ordinary.txt",
        ] {
            let path = ArtifactPath::parse(raw).unwrap();
            let reference = ArtifactRef::new("demo", &path).to_string();
            let parsed = ArtifactRef::parse(&reference).unwrap();
            assert_eq!(parsed.path_for_tenant("demo").unwrap(), &path);
            assert!(parsed.path_for_tenant("other").is_err());
        }
    }

    #[test]
    fn rejects_invalid_paths_and_references() {
        for raw in ["", "/", "a/../b"] {
            assert!(ArtifactPath::parse(raw).is_err());
        }
        for raw in [
            "file://demo/a",
            "artifact:///a",
            "artifact://demo/a%",
            "artifact://demo/%FF",
            "artifact://demo/a#b",
            "artifact://demo/%2E%2E/a",
        ] {
            assert!(ArtifactRef::parse(raw).is_err(), "{raw}");
        }
    }
}
