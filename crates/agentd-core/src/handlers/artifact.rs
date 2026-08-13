use crate::storage::{artifact_ref_for_path, artifact_rel_path, sha256_hex};
use crate::CapabilityEngine;
use anyhow::{anyhow, Context, Result};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use chrono::Utc;
use uuid::Uuid;

impl CapabilityEngine {
    pub(crate) async fn execute_artifact_read(
        &self,
        tenant: &str,
        params: &serde_json::Value,
    ) -> Result<serde_json::Value> {
        let artifact_ref = params
            .get("artifact_ref")
            .and_then(|value| value.as_str())
            .ok_or_else(|| anyhow!("artifact_read requires artifact_ref"))?;
        let body = self
            .read_artifact_body_for_ref(tenant, artifact_ref)
            .await?;
        let encoding = params
            .get("encoding")
            .and_then(|value| value.as_str())
            .unwrap_or("auto");
        match encoding {
            "json" => Ok(serde_json::json!({
                "artifact_ref": artifact_ref,
                "json": serde_json::from_slice::<serde_json::Value>(&body)?,
            })),
            "text" => Ok(serde_json::json!({
                "artifact_ref": artifact_ref,
                "text": String::from_utf8(body).context("artifact is not valid utf-8")?,
            })),
            "base64" => Ok(serde_json::json!({
                "artifact_ref": artifact_ref,
                "body_base64": BASE64.encode(body),
            })),
            "auto" => match String::from_utf8(body.clone()) {
                Ok(text) => match serde_json::from_str::<serde_json::Value>(&text) {
                    Ok(json) => Ok(serde_json::json!({"artifact_ref": artifact_ref, "json": json})),
                    Err(_) => Ok(serde_json::json!({"artifact_ref": artifact_ref, "text": text})),
                },
                Err(_) => Ok(serde_json::json!({
                    "artifact_ref": artifact_ref,
                    "body_base64": BASE64.encode(body),
                })),
            },
            other => Err(anyhow!("unsupported artifact encoding: {other}")),
        }
    }

    pub(crate) async fn execute_artifact_write(
        &self,
        tenant: &str,
        params: &serde_json::Value,
    ) -> Result<serde_json::Value> {
        let default_path = format!("generated/{}", Uuid::new_v4());
        let path = artifact_rel_path(
            params
                .get("path")
                .and_then(|value| value.as_str())
                .unwrap_or(&default_path),
        )?;
        let (body, content_type) = if let Some(body_json) = params.get("body_json") {
            (
                serde_json::to_vec(body_json)?,
                "application/json".to_string(),
            )
        } else if let Some(body_text) = params.get("body_text").and_then(|value| value.as_str()) {
            (
                body_text.as_bytes().to_vec(),
                params
                    .get("content_type")
                    .and_then(|value| value.as_str())
                    .unwrap_or("text/plain; charset=utf-8")
                    .to_string(),
            )
        } else if let Some(body_base64) = params.get("body_base64").and_then(|value| value.as_str())
        {
            (
                BASE64.decode(body_base64).context("invalid body_base64")?,
                params
                    .get("content_type")
                    .and_then(|value| value.as_str())
                    .unwrap_or("application/octet-stream")
                    .to_string(),
            )
        } else {
            return Err(anyhow!(
                "artifact_write requires body_json, body_text, or body_base64"
            ));
        };

        let artifact_ref = artifact_ref_for_path(tenant, &path);
        let updated_at = Utc::now();
        let sha256 = sha256_hex(&body);
        let metadata = serde_json::json!({
            "artifact_ref": artifact_ref,
            "path": path,
            "content_type": content_type,
            "size_bytes": body.len(),
            "sha256": sha256,
            "updated_at": updated_at,
        });
        self.put_artifact_ref(
            tenant,
            &path,
            &body,
            &content_type,
            Some(&metadata.to_string()),
        )
        .await?;
        Ok(metadata)
    }

    pub(crate) async fn execute_artifact_list(
        &self,
        tenant: &str,
        params: &serde_json::Value,
    ) -> Result<serde_json::Value> {
        let prefix = params
            .get("prefix")
            .and_then(|value| value.as_str())
            .unwrap_or("")
            .trim_start_matches('/');
        let limit = params
            .get("limit")
            .and_then(|value| value.as_u64())
            .unwrap_or(100)
            .min(1000) as usize;
        let mut items = self
            .store
            .list_artifacts(tenant, (!prefix.is_empty()).then_some(prefix))
            .await?;
        items.truncate(limit);
        Ok(serde_json::json!({"items": items}))
    }
}
