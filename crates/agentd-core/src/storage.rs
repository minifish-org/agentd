use crate::CapabilityEngine;
use agentd_api::{ArtifactPath, ArtifactRef};
use anyhow::{anyhow, Result};
use hex::encode as hex_encode;
use sha2::{Digest, Sha256};

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
            .get_artifact(tenant, path.as_str())
            .await?
            .map(|(body, _, _)| body)
            .ok_or_else(|| anyhow!("artifact not found: {artifact_ref}"))
    }
}

pub(crate) fn sha256_hex(body: &[u8]) -> String {
    hex_encode(Sha256::digest(body))
}

pub(crate) fn artifact_ref_for_path(tenant: &str, path: &ArtifactPath) -> String {
    ArtifactRef::new(tenant, path).to_string()
}

fn artifact_path_from_ref(tenant: &str, artifact_ref: &str) -> Result<ArtifactPath> {
    Ok(ArtifactRef::parse(artifact_ref)?
        .path_for_tenant(tenant)?
        .clone())
}
