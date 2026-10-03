use super::*;

impl AgentdStore {
    pub async fn put_artifact(
        &self,
        tenant: &str,
        path: &str,
        body: &[u8],
        content_type: &str,
        meta_json: Option<&str>,
    ) -> Result<()> {
        let path = ArtifactPath::parse(path).map_err(validation)?;
        let path = path.as_str();
        if let Some(metadata) = meta_json {
            serde_json::from_str::<serde_json::Value>(metadata).map_err(validation)?;
        }
        let now = Utc::now().to_rfc3339();
        let mut tx = self.begin_tenant_write(tenant).await?;
        db::query(
            r#"INSERT INTO artifacts (tenant, path, body, content_type, meta_json, updated_at)
               VALUES (?, ?, ?, ?, ?, ?)
               ON CONFLICT(tenant, path) DO UPDATE SET
                 body=excluded.body,
                 content_type=excluded.content_type,
                 meta_json=excluded.meta_json,
                 updated_at=excluded.updated_at"#,
        )
        .bind(tenant)
        .bind(path)
        .bind(body)
        .bind(content_type)
        .bind(meta_json)
        .bind(&now)
        .execute(&mut tx)
        .await?;
        audit::record(
            &mut tx,
            AuditInput::new(
                Some(tenant),
                "artifact.put",
                "artifact",
                Some(path),
                "succeeded",
                json!({"bytes":body.len(),"metadata_present":meta_json.is_some()}),
            ),
        )
        .await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn delete_artifact(&self, tenant: &str, path: &str) -> Result<()> {
        let path = ArtifactPath::parse(path).map_err(validation)?;
        let path = path.as_str();
        let mut tx = self.pool.begin().await?;
        let deleted = db::query("DELETE FROM artifacts WHERE tenant = ? AND path = ?")
            .bind(tenant)
            .bind(path)
            .execute(&mut tx)
            .await?
            .rows_affected();
        audit::record(
            &mut tx,
            AuditInput::new(
                Some(tenant),
                "artifact.delete",
                "artifact",
                Some(path),
                audit_mutations::outcome(deleted > 0),
                json!({"deleted":deleted > 0}),
            ),
        )
        .await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn get_artifact(
        &self,
        tenant: &str,
        path: &str,
    ) -> Result<Option<(Vec<u8>, Option<String>, Option<String>)>> {
        let path = ArtifactPath::parse(path).map_err(validation)?;
        let path = path.as_str();
        let row = db::query(
            "SELECT body, content_type, meta_json FROM artifacts WHERE tenant = ? AND path = ?",
        )
        .bind(tenant)
        .bind(path)
        .fetch_optional(&self.pool)
        .await?;
        match row {
            Some(row) => {
                let body = row.try_get::<Vec<u8>, _>("body")?;
                let content_type = row.try_get::<Option<String>, _>("content_type")?;
                let meta_json = row.try_get::<Option<String>, _>("meta_json")?;
                Ok(Some((body, content_type, meta_json)))
            }
            None => Ok(None),
        }
    }

    pub async fn list_artifacts(
        &self,
        tenant: &str,
        prefix: Option<&str>,
    ) -> Result<Vec<agentd_api::ArtifactStat>> {
        Ok(self
            .list_artifacts_page(tenant, prefix, None, usize::MAX)
            .await?
            .items)
    }

    pub async fn list_artifacts_page(
        &self,
        tenant: &str,
        prefix: Option<&str>,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<ArtifactListPage> {
        let limit = limit.clamp(1, 500);
        let fetch_limit = limit + 1;
        let prefix = prefix
            .map(|value| value.trim().trim_start_matches('/'))
            .filter(|value| !value.is_empty())
            .map(ArtifactPath::parse)
            .transpose()
            .map_err(validation)?;
        let cursor = cursor
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ArtifactPath::parse)
            .transpose()
            .map_err(validation)?;
        let mut sql = String::from(
            "SELECT path, content_type, meta_json, updated_at FROM artifacts WHERE tenant = ?",
        );
        if prefix.is_some() {
            sql.push_str(" AND path LIKE ? ESCAPE '\\'");
        }
        if cursor.is_some() {
            sql.push_str(" AND path > ?");
        }
        sql.push_str(" ORDER BY path ASC LIMIT ?");
        let mut query = db::query(&sql).bind(tenant);
        if let Some(prefix) = prefix {
            let escaped = prefix
                .as_str()
                .replace('\\', "\\\\")
                .replace('%', "\\%")
                .replace('_', "\\_");
            query = query.bind(format!("{escaped}%"));
        }
        if let Some(cursor) = cursor {
            query = query.bind(cursor.as_str());
        }
        let rows = query.bind(fetch_limit as i64).fetch_all(&self.pool).await?;
        let mut items = rows
            .into_iter()
            .map(|row| row_to_artifact_stat(tenant, row))
            .collect::<Result<Vec<_>>>()?;
        let has_more = items.len() > limit;
        items.truncate(limit);
        let next_cursor = has_more
            .then(|| items.last().map(|item| item.path.clone()))
            .flatten();
        Ok(ArtifactListPage { items, next_cursor })
    }

    pub async fn get_artifact_stat(
        &self,
        tenant: &str,
        path: &str,
    ) -> Result<Option<agentd_api::ArtifactStat>> {
        let path = ArtifactPath::parse(path).map_err(validation)?;
        let path = path.as_str();
        let row = db::query(
            "SELECT path, content_type, meta_json, updated_at FROM artifacts \
             WHERE tenant = ? AND path = ?",
        )
        .bind(tenant)
        .bind(path)
        .fetch_optional(&self.pool)
        .await?;
        row.map(|row| row_to_artifact_stat(tenant, row)).transpose()
    }
}

pub(super) fn row_to_artifact_stat(
    tenant: &str,
    row: db::SqlRow,
) -> Result<agentd_api::ArtifactStat> {
    let path = row.try_get::<String, _>("path")?;
    let content_type = row.try_get::<Option<String>, _>("content_type")?;
    let updated_at = row.try_get::<String, _>("updated_at")?;
    let meta: Option<serde_json::Value> = row
        .try_get::<Option<String>, _>("meta_json")?
        .map(|raw| decode_json(&raw))
        .transpose()?;
    let size_bytes = meta
        .as_ref()
        .and_then(|meta| meta.get("size_bytes"))
        .and_then(serde_json::Value::as_u64);
    Ok(agentd_api::ArtifactStat {
        artifact_ref: ArtifactRef::new(
            tenant,
            &ArtifactPath::parse(&path).map_err(StoreError::database)?,
        )
        .to_string(),
        path,
        content_type,
        sha256: meta
            .as_ref()
            .and_then(|meta| meta.get("sha256"))
            .and_then(serde_json::Value::as_str)
            .map(ToString::to_string),
        size_bytes,
        metadata: meta,
        updated_at: Some(updated_at),
    })
}
