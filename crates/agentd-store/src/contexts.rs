use super::*;

impl AgentdStore {
    pub async fn list_context_scopes(&self, tenant: &str, agent: &str) -> Result<Vec<String>> {
        let scopes = db::query_scalar::<String>(
            "SELECT scope FROM contexts WHERE tenant = ? AND agent = ? ORDER BY updated_at DESC",
        )
        .bind(tenant)
        .bind(agent)
        .fetch_all(&self.pool)
        .await?;
        Ok(scopes)
    }

    pub async fn get_context_state(
        &self,
        tenant: &str,
        agent: &str,
        scope: &str,
    ) -> Result<Option<StoredContext>> {
        let row = db::query(
            "SELECT revision, updated_at, state_json FROM contexts \
             WHERE tenant = ? AND agent = ? AND scope = ? LIMIT 1",
        )
        .bind(tenant)
        .bind(agent)
        .bind(scope)
        .fetch_optional(&self.pool)
        .await?;
        row.map(|row| {
            let revision = row.try_get::<i64, _>("revision")? as u64;
            let updated_at = row.try_get::<String, _>("updated_at")?;
            let state = decode_json(&row.try_get::<String, _>("state_json")?)
                .unwrap_or_else(|_| serde_json::json!({}));
            Ok(StoredContext {
                revision,
                updated_at,
                state,
            })
        })
        .transpose()
    }

    pub async fn delete_context_state(
        &self,
        tenant: &str,
        agent: &str,
        scope: &str,
    ) -> Result<bool> {
        let mut tx = self.pool.begin().await?;
        let changed =
            db::query("DELETE FROM contexts WHERE tenant = ? AND agent = ? AND scope = ?")
                .bind(tenant)
                .bind(agent)
                .bind(scope)
                .execute(&mut tx)
                .await?
                .rows_affected()
                > 0;
        audit::record(
            &mut tx,
            AuditInput::new(
                Some(tenant),
                "context.delete",
                "context",
                Some(agent),
                audit_mutations::outcome(changed),
                json!({"scope":scope,"deleted":changed}),
            ),
        )
        .await?;
        tx.commit().await?;
        Ok(changed)
    }
}
