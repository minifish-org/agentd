use crate::CapabilityEngine;
use anyhow::{anyhow, Result};
use hex::encode as hex_encode;
use sha2::{Digest, Sha256};
use url::Url;

impl CapabilityEngine {
    pub(crate) async fn put_artifact_ref(
        &self,
        tenant: &str,
        path: &str,
        body: &[u8],
        content_type: &str,
        meta_json: Option<&str>,
    ) -> Result<()> {
        self.store
            .put_artifact(tenant, path, body, content_type, meta_json)
            .await
    }

    pub(crate) async fn read_artifact_body_for_ref(
        &self,
        tenant: &str,
        artifact_ref: &str,
    ) -> Result<Vec<u8>> {
        let path = artifact_path_from_ref(tenant, artifact_ref)?;
        self.store
            .get_artifact(tenant, &path)
            .await?
            .map(|(body, _, _)| body)
            .ok_or_else(|| anyhow!("artifact not found: {artifact_ref}"))
    }
}

pub(crate) fn sha256_hex(body: &[u8]) -> String {
    hex_encode(Sha256::digest(body))
}

pub(crate) fn artifact_ref_for_path(tenant: &str, path: &str) -> String {
    format!("artifact://{tenant}/{}", path.trim_start_matches('/'))
}

pub(crate) fn artifact_rel_path(raw: &str) -> Result<String> {
    let path = raw.trim().trim_start_matches('/');
    if path.is_empty() || path.split('/').any(|part| part == "..") {
        return Err(anyhow!("invalid artifact path"));
    }
    Ok(path.to_string())
}

fn artifact_path_from_ref(tenant: &str, artifact_ref: &str) -> Result<String> {
    let parsed = Url::parse(artifact_ref)?;
    if parsed.scheme() != "artifact" {
        return Err(anyhow!("artifact_ref must use artifact://"));
    }
    let ref_tenant = parsed
        .host_str()
        .ok_or_else(|| anyhow!("artifact_ref must include tenant"))?;
    if ref_tenant != tenant {
        return Err(anyhow!("artifact_ref tenant mismatch"));
    }
    artifact_rel_path(parsed.path())
}
