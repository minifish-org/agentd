use super::*;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DeliveryRequest {
    pub destination: String,
}

impl DeliveryRequest {
    pub fn validate(&self) -> Result<(), ApiError> {
        if self.destination.trim().is_empty() {
            return Err(ApiError::Validation(
                "delivery.destination is required".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DeliveryOutboxRecord {
    pub delivery_id: Uuid,
    pub tenant: String,
    pub run_id: Uuid,
    pub status: String,
    pub destination: String,
    /// Immutable payload captured when the terminal run state creates the
    /// delivery. Successful runs carry their canonical output; failed runs
    /// carry a transport-neutral user-facing failure message.
    #[serde(default)]
    pub payload: serde_json::Value,
    pub attempt: u32,
    #[serde(default)]
    pub next_attempt_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub last_error: Option<String>,
    #[serde(default)]
    pub claim_token: Option<String>,
    #[serde(default)]
    pub claim_expires_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}
