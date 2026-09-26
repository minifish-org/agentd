use crate::ApiError;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

pub const BEHAVIOR_LEARNER_AGENT: &str = "system/behavior-learner";
pub const BEHAVIOR_LEARNING_SCHEDULE: &str = "system/behavior-learning";

/// Budget and quality controls for a fully automated behavior learning cycle.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(default, deny_unknown_fields)]
pub struct BehaviorLearningOptions {
    pub target_agent: String,
    pub proposer_model: Option<String>,
    pub judge_model: Option<String>,
    pub min_samples: usize,
    pub max_samples: usize,
    pub max_model_calls: u32,
    pub max_tokens: u32,
    pub min_improvement: f64,
    pub rubric: Option<String>,
}

impl Default for BehaviorLearningOptions {
    fn default() -> Self {
        Self {
            target_agent: "*".into(),
            proposer_model: None,
            judge_model: None,
            min_samples: 8,
            max_samples: 12,
            max_model_calls: 64,
            max_tokens: 2048,
            min_improvement: 0.1,
            rubric: None,
        }
    }
}

impl BehaviorLearningOptions {
    pub fn validate(&self) -> Result<(), ApiError> {
        let invalid = if self.target_agent.trim().is_empty() {
            Some("target_agent is required")
        } else if self.target_agent.starts_with("system/") {
            Some("target_agent must be a foreground agent")
        } else if !(4..=16).contains(&self.min_samples) {
            Some("min_samples must be between 4 and 16")
        } else if !(self.min_samples..=32).contains(&self.max_samples) {
            Some("max_samples must be between min_samples and 32")
        } else if !(8..=128).contains(&self.max_model_calls) {
            Some("max_model_calls must be between 8 and 128")
        } else if !(128..=4096).contains(&self.max_tokens) {
            Some("max_tokens must be between 128 and 4096")
        } else if !self.min_improvement.is_finite() || !(0.01..=1.0).contains(&self.min_improvement)
        {
            Some("min_improvement must be between 0.01 and 1")
        } else if self.rubric.as_ref().is_some_and(|text| text.len() > 4000) {
            Some("rubric must be at most 4000 bytes")
        } else if [&self.proposer_model, &self.judge_model]
            .iter()
            .any(|model| model.as_ref().is_some_and(|id| id.trim().is_empty()))
        {
            Some("model names must not be empty")
        } else {
            None
        };
        invalid.map_or(Ok(()), |message| Err(ApiError::Validation(message.into())))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BehaviorRevision {
    pub tenant: String,
    pub agent_ref: String,
    pub revision: u64,
    pub parent_revision: Option<u64>,
    pub instructions: String,
    pub outcome: String,
    pub source_run_id: Uuid,
    pub report: Value,
    pub created_at: DateTime<Utc>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn learning_options_reject_unknown_fields_and_invalid_budgets() {
        let defaults: BehaviorLearningOptions = serde_json::from_str("{}").unwrap();
        defaults.validate().unwrap();
        assert_eq!(defaults.target_agent, "*");
        assert!(serde_json::from_str::<BehaviorLearningOptions>(r#"{"typo":1}"#).is_err());
        for bad in [
            serde_json::json!({"min_samples":3}),
            serde_json::json!({"max_samples":7}),
            serde_json::json!({"max_model_calls":129}),
            serde_json::json!({"max_tokens":0}),
            serde_json::json!({"min_improvement":0.0}),
            serde_json::json!({"target_agent":"system/worker"}),
        ] {
            assert!(serde_json::from_value::<BehaviorLearningOptions>(bad)
                .unwrap()
                .validate()
                .is_err());
        }
    }
}
