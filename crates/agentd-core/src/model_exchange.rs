use crate::RuntimeEngine;
use anyhow::{anyhow, Result};
use chrono::Utc;
use serde_json::{json, Value};
use tokio::time::Instant;
use uuid::Uuid;

impl RuntimeEngine {
    /// Exchange and trace one model request. Callers retain their own budgets,
    /// response rules, and authority to execute tools or persist state.
    pub(crate) async fn traced_completion(
        &self,
        run_id: Uuid,
        step: u32,
        stage: Option<&str>,
        request: &Value,
        deadline: Instant,
    ) -> Result<Value> {
        let event = |phase: &str, field: &str, value: Value| {
            let mut payload = json!({"phase":phase,"step":step});
            if let Some(stage) = stage {
                payload["stage"] = json!(stage);
            }
            payload[field] = value;
            payload
        };
        self.store
            .append_event(
                run_id,
                "model",
                event("request", "request", request.clone()),
                Utc::now(),
            )
            .await?;
        let result = tokio::time::timeout_at(deadline, self.caps.chat_completion(request))
            .await
            .map_err(|_| anyhow!("model request deadline exceeded"))
            .and_then(|result| result);
        match result {
            Ok(response) => {
                self.store
                    .append_event(
                        run_id,
                        "model",
                        event("response", "response", response.clone()),
                        Utc::now(),
                    )
                    .await?;
                Ok(response)
            }
            Err(error) => {
                self.store
                    .append_event(
                        run_id,
                        "model",
                        event("error", "reason", json!("model_request_failed")),
                        Utc::now(),
                    )
                    .await?;
                Err(error)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{CapabilityEngine, CapabilityEngineConfig};
    use agentd_store::{AgentdStore, NewRun};
    use axum::{http::StatusCode, response::IntoResponse, routing::post, Json, Router};
    use std::time::Duration;

    #[tokio::test]
    async fn exchanges_trace_success_provider_failure_and_deadline_for_both_callers() {
        async fn completion(Json(body): Json<Value>) -> axum::response::Response {
            if body["model"] == "failure" {
                return (StatusCode::BAD_GATEWAY, Json(json!({"error":"test"}))).into_response();
            }
            if body["model"] == "deadline" {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Json(json!({"choices":[{"message":{"role":"assistant","content":"ok"}}]}))
                .into_response()
        }
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            axum::serve(
                listener,
                Router::new().route("/v1/chat/completions", post(completion)),
            )
            .await
            .unwrap();
        });
        let directory = tempfile::tempdir().unwrap();
        let store = AgentdStore::new(directory.path().join("agentd.db").to_str().unwrap())
            .await
            .unwrap();
        store.create_tenant("demo", &json!({})).await.unwrap();
        store.apply_agent(&serde_json::from_value(json!({"metadata":{"tenant":"demo","name":"bot"},"spec":{"allowed_families":[],"limits":{"timeout_ms":1000,"max_steps":1}}})).unwrap()).await.unwrap();
        let caps = CapabilityEngine::new_with_config(
            store.clone(),
            CapabilityEngineConfig {
                llm_api_base: Some(format!("http://{address}/v1")),
                ..CapabilityEngineConfig::default()
            },
        );
        let runtime = RuntimeEngine::new(caps, store.clone());
        for (index, model) in ["success", "failure", "deadline"].into_iter().enumerate() {
            let input = json!({});
            let run_id = store
                .submit_run(NewRun {
                    tenant: "demo",
                    name: "test",
                    agent_ref: "bot",
                    scope: model,
                    source: "test",
                    input: &input,
                    request_id: None,
                    schedule_name: None,
                    delivery_destination: None,
                })
                .await
                .unwrap();
            let stage = if index == 1 { Some("judge") } else { None };
            let duration = if model == "deadline" {
                Duration::from_millis(20)
            } else {
                Duration::from_secs(2)
            };
            let result = runtime
                .traced_completion(
                    run_id,
                    1,
                    stage,
                    &json!({"model":model,"messages":[]}),
                    Instant::now() + duration,
                )
                .await;
            assert_eq!(result.is_ok(), model == "success");
            let trace = store.list_run_log(run_id).await.unwrap();
            let model_events = trace
                .iter()
                .filter(|event| event.kind == "model")
                .collect::<Vec<_>>();
            assert_eq!(model_events.len(), 2);
            assert_eq!(model_events[0].payload["phase"], "request");
            assert_eq!(
                model_events[1].payload["phase"],
                if model == "success" {
                    "response"
                } else {
                    "error"
                }
            );
            assert_eq!(
                model_events[1].payload.get("stage").and_then(Value::as_str),
                stage
            );
        }
        server.abort();
    }
}
