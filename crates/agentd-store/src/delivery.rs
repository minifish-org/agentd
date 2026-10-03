use super::*;

impl AgentdStore {
    pub async fn list_delivery_outbox(
        &self,
        tenant: Option<&str>,
        status: Option<&str>,
        run_id: Option<Uuid>,
        limit: usize,
    ) -> Result<Vec<DeliveryOutboxRecord>> {
        let mut sql = String::from(
            "SELECT d.delivery_id, d.tenant, d.run_id, d.status, d.destination, \
             d.payload_json, d.attempt, d.next_attempt_at, d.last_error, \
             d.claim_token, d.claim_expires_at, d.created_at, d.updated_at \
             FROM deliveries d WHERE 1 = 1",
        );
        if tenant.is_some() {
            sql.push_str(" AND d.tenant = ?");
        }
        if status.is_some() {
            sql.push_str(" AND d.status = ?");
        }
        if run_id.is_some() {
            sql.push_str(" AND d.run_id = ?");
        }
        sql.push_str(" ORDER BY d.created_at DESC LIMIT ?");
        let mut query = db::query(&sql);
        if let Some(tenant) = tenant {
            query = query.bind(tenant);
        }
        if let Some(status) = status {
            query = query.bind(status);
        }
        if let Some(run_id) = run_id {
            query = query.bind(run_id.to_string());
        }
        let rows = query
            .bind(limit.max(1) as i64)
            .fetch_all(&self.pool)
            .await?;
        rows.into_iter().map(row_to_delivery_outbox).collect()
    }

    pub async fn get_delivery_outbox(
        &self,
        delivery_id: Uuid,
    ) -> Result<Option<DeliveryOutboxRecord>> {
        let row = db::query(
            "SELECT d.delivery_id, d.tenant, d.run_id, d.status, d.destination, \
             d.payload_json, d.attempt, d.next_attempt_at, d.last_error, \
             d.claim_token, d.claim_expires_at, d.created_at, d.updated_at \
             FROM deliveries d WHERE d.delivery_id = ?",
        )
        .bind(delivery_id.to_string())
        .fetch_optional(&self.pool)
        .await?;
        row.map(row_to_delivery_outbox).transpose()
    }

    pub async fn claim_delivery_outbox(
        &self,
        tenant: &str,
        limit: usize,
        now: DateTime<Utc>,
        claim_ttl: Duration,
    ) -> Result<Vec<DeliveryOutboxRecord>> {
        let rows = db::query(
            "SELECT d.delivery_id, d.tenant, d.run_id, d.status, d.destination, \
             d.payload_json, d.attempt, d.next_attempt_at, d.last_error, \
             d.claim_token, d.claim_expires_at, d.created_at, d.updated_at \
             FROM deliveries d WHERE d.tenant = ? AND ( \
               (d.status = 'pending' AND (d.next_attempt_at IS NULL OR d.next_attempt_at <= ?)) OR \
               (d.status = 'claimed' AND (d.claim_expires_at IS NULL OR d.claim_expires_at <= ?)) \
             ) ORDER BY d.created_at LIMIT ?",
        )
        .bind(tenant)
        .bind(now.to_rfc3339())
        .bind(now.to_rfc3339())
        .bind(limit.max(1).saturating_mul(4) as i64)
        .fetch_all(&self.pool)
        .await?;
        let expires_at = now + ChronoDuration::from_std(claim_ttl)?;
        let mut claimed = Vec::new();
        for row in rows {
            if claimed.len() >= limit.max(1) {
                break;
            }
            let delivery_id = parse_uuid_field(&row, "delivery_id")?;
            let mut tx = self.pool.begin().await?;
            let current =
                db::query("SELECT * FROM deliveries WHERE delivery_id = ? AND tenant = ?")
                    .bind(delivery_id.to_string())
                    .bind(tenant)
                    .fetch_optional(&mut tx)
                    .await?;
            let Some(current) = current else {
                tx.rollback().await?;
                continue;
            };
            let delivery = row_to_delivery_outbox(current)?;
            let token = Uuid::new_v4().to_string();
            let changed = db::query(
                "UPDATE deliveries SET status = 'claimed', claim_token = ?, claim_expires_at = ?, \
                 updated_at = ? WHERE delivery_id = ? AND tenant = ? AND ( \
                 (status = 'pending' AND (next_attempt_at IS NULL OR next_attempt_at <= ?)) OR \
                 (status = 'claimed' AND (claim_expires_at IS NULL OR claim_expires_at <= ?) AND claim_token IS ?))",
            )
            .bind(&token)
            .bind(expires_at.to_rfc3339())
            .bind(now.to_rfc3339())
            .bind(delivery.delivery_id.to_string())
            .bind(tenant)
            .bind(now.to_rfc3339())
            .bind(now.to_rfc3339())
            .bind(delivery.claim_token.as_deref())
            .execute(&mut tx)
            .await?
            .rows_affected();
            if changed > 0 {
                let row = db::query("SELECT * FROM deliveries WHERE delivery_id = ?")
                    .bind(delivery_id.to_string())
                    .fetch_optional(&mut tx)
                    .await?
                    .ok_or_else(|| anyhow!("delivery disappeared during claim"))?;
                let record = row_to_delivery_outbox(row)?;
                audit::record(&mut tx, AuditInput::new(Some(tenant), "delivery.claim", "delivery", Some(&delivery_id.to_string()), "succeeded", json!({
                    "status_before":delivery.status,"status_after":"claimed","attempt":record.attempt,"lease_seconds":claim_ttl.as_secs()
                })).for_run(delivery.run_id)).await?;
                tx.commit().await?;
                claimed.push(record);
            } else {
                tx.rollback().await?;
            }
        }
        Ok(claimed)
    }

    pub async fn ack_delivery(
        &self,
        tenant: &str,
        ack: DeliveryAck<'_>,
    ) -> Result<DeliveryOutboxRecord> {
        let mut tx = self.pool.begin().await?;
        let existing = db::query("SELECT * FROM deliveries WHERE delivery_id = ?")
            .bind(ack.delivery_id.to_string())
            .fetch_optional(&mut tx)
            .await?
            .ok_or_else(|| missing!("delivery not found"))?;
        let existing = row_to_delivery_outbox(existing)?;
        if existing.tenant != tenant
            || existing.status != "claimed"
            || existing.claim_token.as_deref() != Some(ack.claim_token)
        {
            return Err(conflicting!("delivery claim token does not match"));
        }
        if existing
            .claim_expires_at
            .is_none_or(|expires| expires <= ack.now)
        {
            return Err(conflicting!("delivery claim has expired"));
        }
        let (status, next_attempt_at) = match ack.outcome {
            "delivered" => ("delivered", None),
            "retry" => (
                "pending",
                Some(
                    ack.now
                        + ChronoDuration::from_std(
                            ack.retry_after.unwrap_or(Duration::from_secs(1)),
                        )?,
                ),
            ),
            "failed" => ("failed", None),
            _ => {
                return Err(invalid!(
                    "delivery outcome must be delivered, retry, or failed"
                ))
            }
        };
        let changed = db::query(
            "UPDATE deliveries SET status = ?, attempt = attempt + 1, next_attempt_at = ?, \
             last_error = ?, claim_token = NULL, claim_expires_at = NULL, updated_at = ? \
             WHERE delivery_id = ? AND tenant = ? AND status = 'claimed' AND claim_token = ? \
             AND claim_expires_at = ? AND claim_expires_at > ?",
        )
        .bind(status)
        .bind(next_attempt_at.map(|value| value.to_rfc3339()))
        .bind(ack.error)
        .bind(ack.now.to_rfc3339())
        .bind(ack.delivery_id.to_string())
        .bind(tenant)
        .bind(ack.claim_token)
        .bind(existing.claim_expires_at.map(|value| value.to_rfc3339()))
        .bind(ack.now.to_rfc3339())
        .execute(&mut tx)
        .await?
        .rows_affected();
        if changed != 1 {
            return Err(conflicting!(
                "delivery claim changed before acknowledgement"
            ));
        }
        let row = db::query("SELECT * FROM deliveries WHERE delivery_id = ?")
            .bind(ack.delivery_id.to_string())
            .fetch_optional(&mut tx)
            .await?
            .ok_or_else(|| anyhow!("delivery disappeared after ack"))?;
        let record = row_to_delivery_outbox(row)?;
        audit::record(&mut tx, AuditInput::new(Some(tenant), "delivery.ack", "delivery", Some(&ack.delivery_id.to_string()), "succeeded", json!({
            "ack_outcome":ack.outcome,"status_before":"claimed","status_after":status,"attempt":record.attempt,
            "retry_scheduled":next_attempt_at.is_some(),"error_code":ack.error.is_some().then_some("delivery_error")
        })).for_run(existing.run_id)).await?;
        tx.commit().await?;
        Ok(record)
    }
}

pub(super) fn row_to_delivery_outbox(row: db::SqlRow) -> Result<DeliveryOutboxRecord> {
    Ok(DeliveryOutboxRecord {
        delivery_id: parse_uuid_field(&row, "delivery_id")?,
        tenant: row.try_get("tenant")?,
        run_id: parse_uuid_field(&row, "run_id")?,
        status: row.try_get("status")?,
        destination: row.try_get("destination")?,
        payload: decode_json(&row.try_get::<String, _>("payload_json")?)?,
        attempt: row.try_get::<i64, _>("attempt")? as u32,
        next_attempt_at: optional_ts_field(&row, "next_attempt_at")?,
        last_error: row.try_get("last_error")?,
        claim_token: row.try_get("claim_token")?,
        claim_expires_at: optional_ts_field(&row, "claim_expires_at")?,
        created_at: parse_ts_field(&row, "created_at")?,
        updated_at: parse_ts_field(&row, "updated_at")?,
    })
}

pub(super) fn failure_delivery_payload(error: &str) -> serde_json::Value {
    if error == "run timeout exceeded" {
        serde_json::json!({
            "reply":"Sorry, this request took too long to complete. Please try again.",
            "error":{"code":"run_timeout"}
        })
    } else {
        serde_json::json!({
            "reply":"Sorry, this request could not be completed. Please try again.",
            "error":{"code":"run_failed"}
        })
    }
}
