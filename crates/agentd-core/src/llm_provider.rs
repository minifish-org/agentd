use crate::CapabilityEngine;
use anyhow::{anyhow, Result};

/// A prepared OpenAI-compatible chat-completions request.
pub(crate) struct LlmRequest {
    pub(crate) api_base: String,
    pub(crate) api_key: String,
    pub(crate) body: serde_json::Value,
}

impl CapabilityEngine {
    fn llm_api_base(&self) -> Result<String> {
        self.llm_api_base
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .map(|value| value.trim_end_matches('/').to_string())
            .ok_or_else(|| anyhow!("llm_api_base is not configured"))
    }

    fn llm_api_key(&self) -> &str {
        self.llm_api_key
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or("no-key")
    }

    fn llm_model(&self) -> Result<&str> {
        self.llm_model
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| anyhow!("llm_model is not configured"))
    }

    /// Build an OpenAI-compatible chat-completions request. agentd speaks only
    /// this transport; `params.model` overrides the configured default, and
    /// `temperature` / `max_tokens` / `response_format` / `thinking` are
    /// forwarded verbatim when present.
    pub(crate) fn prepare_llm_request(
        &self,
        params: &serde_json::Value,
        stream: bool,
    ) -> Result<LlmRequest> {
        let model = params
            .get("model")
            .and_then(|value| value.as_str())
            .map(ToString::to_string)
            .unwrap_or_else(|| self.llm_model().unwrap_or("").to_string());
        if model.trim().is_empty() {
            return Err(anyhow!("no model: set llm_model or pass params.model"));
        }
        let messages = params
            .get("messages")
            .and_then(|value| value.as_array())
            .cloned()
            .ok_or_else(|| anyhow!("llm.generate requires params.messages"))?;
        let mut body = serde_json::json!({
            "model": model,
            "messages": messages,
            "stream": stream,
        });
        // Forward all OpenAI-compatible request fields we care about. `tools`
        // and `tool_choice` are CRUCIAL for native function calling — without
        // them the model can't actually invoke tools and (when the system
        // prompt mentions tool_calls) hallucinates a tool_calls JSON inside
        // `content` with invented tool names. `parallel_tool_calls` defaults
        // to true on OpenAI/DeepSeek; pass through explicitly when set.
        for field in [
            "temperature",
            "max_tokens",
            "tools",
            "tool_choice",
            "parallel_tool_calls",
        ] {
            if let Some(value) = params.get(field) {
                body[field] = value.clone();
            }
        }
        Ok(LlmRequest {
            api_base: self.llm_api_base()?,
            api_key: self.llm_api_key().to_string(),
            body,
        })
    }

    /// Execute one non-streaming OpenAI-compatible chat completion and return
    /// the provider payload unchanged. The native agent loop needs the raw
    /// assistant message because tool-call responses may have no text content.
    pub(crate) async fn chat_completion(
        &self,
        params: &serde_json::Value,
    ) -> Result<serde_json::Value> {
        let llm = self.prepare_llm_request(params, false)?;
        let response = self
            .http
            .post(format!("{}/chat/completions", llm.api_base))
            .bearer_auth(&llm.api_key)
            .json(&llm.body)
            .send()
            .await?;
        let status = response.status();
        let payload: serde_json::Value = response
            .json()
            .await
            .unwrap_or_else(|_| serde_json::json!({}));
        if !status.is_success() {
            return Err(anyhow!(
                "chat completion failed with status {}: {}",
                status,
                payload
            ));
        }
        Ok(payload)
    }
}

pub(crate) fn extract_openai_message_content(payload: &serde_json::Value) -> Option<String> {
    let content = payload.pointer("/choices/0/message/content")?;
    match content {
        serde_json::Value::String(text) => Some(text.clone()),
        serde_json::Value::Array(parts) => {
            let text = parts
                .iter()
                .filter_map(|item| item.get("text").and_then(|value| value.as_str()))
                .collect::<String>();
            (!text.is_empty()).then_some(text)
        }
        _ => None,
    }
}
