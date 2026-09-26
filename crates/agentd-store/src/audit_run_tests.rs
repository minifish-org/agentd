use super::*;
use agentd_api::{AgentLimits, AgentSpec, ResourceMeta};
use tempfile::TempDir;

async fn fixture() -> (TempDir, AgentdStore) {
    let dir = TempDir::new().unwrap();
    let store = AgentdStore::new(dir.path().join("agentd.db").to_str().unwrap())
        .await
        .unwrap();
    store.create_tenant("one", &json!({})).await.unwrap();
    store
        .apply_agent(&AgentResource {
            metadata: ResourceMeta {
                name: "bot".into(),
                tenant: "one".into(),
                labels: BTreeMap::new(),
            },
            spec: AgentSpec {
                allowed_families: Some(vec![]),
                limits: AgentLimits {
                    timeout_ms: 5000,
                    max_steps: 8,
                },
                system_prompt: None,
                model: None,
                temperature: None,
                max_tokens: None,
                context_window: None,
            },
        })
        .await
        .unwrap();
    (dir, store)
}

async fn submit(
    store: &AgentdStore,
    scope: &str,
    request_id: Option<&str>,
    delivery: Option<&str>,
) -> Result<Uuid> {
    store
        .submit_run(NewRun {
            tenant: "one",
            name: "test",
            agent_ref: "bot",
            scope,
            source: "test",
            input: &json!({"text":"INPUT_SECRET"}),
            request_id,
            schedule_name: None,
            delivery_destination: delivery,
        })
        .await
}

async fn events(store: &AgentdStore, run_id: Uuid) -> Vec<AuditEvent> {
    store
        .list_audit_events(&AuditQuery {
            run_id: Some(run_id),
            limit: Some(500),
            ..AuditQuery::default()
        })
        .await
        .unwrap()
        .events
}

async fn reject_audit(store: &AgentdStore, action: &str) {
    assert!(action
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '.'));
    db::query(&format!("CREATE TRIGGER reject_test_audit BEFORE INSERT ON audit_events WHEN NEW.action = '{action}' BEGIN SELECT RAISE(ABORT, 'test audit failure'); END"))
        .execute(&store.pool).await.unwrap();
}

async fn allow_audit(store: &AgentdStore) {
    db::query("DROP TRIGGER reject_test_audit")
        .execute(&store.pool)
        .await
        .unwrap();
}

#[tokio::test]
async fn audit_run_submission_reuse_claim_and_cancel_are_linked_and_redacted() {
    let (_dir, store) = fixture().await;
    let request_id = Uuid::new_v4();
    let run_id = with_audit_context(AuditContext::api(true, request_id), async {
        let first = submit(&store, "chat", Some("IDEMP_SECRET"), None)
            .await
            .unwrap();
        let reused = submit(&store, "another-chat", Some("IDEMP_SECRET"), None)
            .await
            .unwrap();
        assert_eq!(first, reused);
        first
    })
    .await;
    let submitted = events(&store, run_id).await;
    assert_eq!(submitted.len(), 2);
    assert!(submitted
        .iter()
        .all(|event| event.action == "run.submit" && event.request_id == Some(request_id)));
    assert_eq!(submitted[0].outcome, "noop");
    assert_eq!(submitted[0].details["reused"], true);
    assert_eq!(
        store.claim_next_run().await.unwrap().unwrap().run.run_id,
        run_id
    );
    let before_empty_poll = events(&store, run_id).await.len();
    assert!(store.claim_next_run().await.unwrap().is_none());
    assert_eq!(events(&store, run_id).await.len(), before_empty_poll);
    assert_eq!(
        store
            .cancel_run_request(run_id, "CANCEL_SECRET")
            .await
            .unwrap(),
        AgentRunStatus::Cancelled
    );
    assert_eq!(
        store
            .cancel_run_request(run_id, "ANOTHER_CANCEL_SECRET")
            .await
            .unwrap(),
        AgentRunStatus::Cancelled
    );
    let audit = events(&store, run_id).await;
    assert!(audit.iter().any(|event| event.action == "run.claim"));
    assert!(audit
        .iter()
        .any(|event| event.action == "run.cancel" && event.outcome == "succeeded"));
    assert!(audit
        .iter()
        .any(|event| event.action == "run.cancel" && event.outcome == "noop"));
    let serialized = serde_json::to_string(&audit).unwrap();
    for secret in [
        "INPUT_SECRET",
        "IDEMP_SECRET",
        "CANCEL_SECRET",
        "ANOTHER_CANCEL_SECRET",
    ] {
        assert!(!serialized.contains(secret));
    }
    assert!(store
        .list_run_log(run_id)
        .await
        .unwrap()
        .iter()
        .any(|event| event.payload["reason"] == "CANCEL_SECRET"));
}

#[tokio::test]
async fn audit_run_trace_outcomes_and_success_side_effects_are_separate() {
    let (_dir, store) = fixture().await;
    let run_id = submit(&store, "chat", None, Some("DEST_SECRET"))
        .await
        .unwrap();
    store.claim_next_run().await.unwrap().unwrap();
    for (kind, payload) in [
        (
            "model",
            json!({"phase":"request","step":1,"request":{"text":"MODEL_REQUEST_SECRET"}}),
        ),
        (
            "model",
            json!({"phase":"response","step":1,"response":{"text":"MODEL_RESPONSE_SECRET"}}),
        ),
        (
            "tool",
            json!({"phase":"call","step":1,"name":"memory_put","arguments":{"text":"TOOL_INPUT_SECRET"}}),
        ),
        (
            "tool",
            json!({"phase":"result","step":1,"result":{"ok":false,"error":"TOOL_ERROR_SECRET"}}),
        ),
    ] {
        store
            .append_event(run_id, kind, payload, Utc::now())
            .await
            .unwrap();
    }
    store.finalize_run_success(run_id,
        &json!({"reply":"OUTPUT_SECRET","name":"OUTPUT_NAME_SECRET","phase":"call","revision":987654}),
        Some(&json!({"messages":[{"text":"CONTEXT_SECRET"}]})),
    ).await.unwrap();
    let audit = events(&store, run_id).await;
    for action in ["run.succeed", "context.put", "delivery.enqueue"] {
        assert_eq!(
            audit.iter().filter(|event| event.action == action).count(),
            1
        );
    }
    let trace_audit = audit
        .iter()
        .filter(|event| event.action == "run.trace")
        .collect::<Vec<_>>();
    assert_eq!(trace_audit.len(), 6);
    assert_eq!(
        trace_audit
            .iter()
            .filter(|event| event.outcome == "started")
            .count(),
        2
    );
    assert_eq!(
        trace_audit
            .iter()
            .filter(|event| event.outcome == "failed")
            .count(),
        1
    );
    let output_audit = trace_audit
        .iter()
        .find(|event| event.details["kind"] == "output")
        .unwrap();
    assert_eq!(output_audit.outcome, "recorded");
    assert!(output_audit.details.get("phase").is_none());
    assert!(output_audit.details.get("revision").is_none());
    assert!(trace_audit
        .iter()
        .all(|event| event.details["trace_id"].is_i64()));
    let serialized = serde_json::to_string(&audit).unwrap();
    for secret in [
        "DEST_SECRET",
        "MODEL_REQUEST_SECRET",
        "MODEL_RESPONSE_SECRET",
        "TOOL_INPUT_SECRET",
        "TOOL_ERROR_SECRET",
        "OUTPUT_SECRET",
        "OUTPUT_NAME_SECRET",
        "CONTEXT_SECRET",
    ] {
        assert!(!serialized.contains(secret));
    }
    assert!(store
        .get_context_state("one", "bot", "chat")
        .await
        .unwrap()
        .is_some());
    assert_eq!(
        store
            .list_delivery_outbox(Some("one"), None, Some(run_id), 10)
            .await
            .unwrap()
            .len(),
        1
    );

    let next = submit(&store, "chat", None, None).await.unwrap();
    store.claim_next_run().await.unwrap().unwrap();
    store
        .finalize_run_success(next, &json!({"ok":true}), None)
        .await
        .unwrap();
    assert!(events(&store, next)
        .await
        .iter()
        .any(|event| event.action == "context.delete" && event.details["deleted"] == true));
}

#[tokio::test]
async fn audit_background_checks_mark_only_unready_checks_skipped() {
    let (_dir, store) = fixture().await;
    let run_id = submit(&store, "check", None, None).await.unwrap();
    store.claim_next_run().await.unwrap().unwrap();
    for kind in ["maintenance_check", "behavior_check"] {
        for ready in [false, true] {
            store
                .append_event(
                    run_id,
                    kind,
                    json!({"ready":ready,"reason":"no_new_samples"}),
                    Utc::now(),
                )
                .await
                .unwrap();
        }
    }
    store
        .append_event(
            run_id,
            "tool",
            json!({"phase":"error","error":"TOOL_SECRET"}),
            Utc::now(),
        )
        .await
        .unwrap();
    let audit = events(&store, run_id).await;
    for kind in ["maintenance_check", "behavior_check"] {
        let checks = audit
            .iter()
            .filter(|event| event.details["kind"] == kind)
            .collect::<Vec<_>>();
        assert_eq!(checks.len(), 2);
        assert_eq!(
            checks
                .iter()
                .filter(|event| event.outcome == "skipped")
                .count(),
            1
        );
        assert_eq!(
            checks
                .iter()
                .filter(|event| event.outcome == "recorded")
                .count(),
            1
        );
    }
    assert!(audit
        .iter()
        .any(|event| event.details["kind"] == "tool" && event.outcome == "failed"));
    assert!(!serde_json::to_string(&audit)
        .unwrap()
        .contains("TOOL_SECRET"));
}

#[tokio::test]
async fn audit_failure_rolls_back_run_submission_claim_trace_and_finalization() {
    let (_dir, store) = fixture().await;
    reject_audit(&store, "run.submit").await;
    assert!(submit(&store, "chat", None, None).await.is_err());
    assert!(store
        .list_runs(&RunListQuery::default())
        .await
        .unwrap()
        .is_empty());
    allow_audit(&store).await;
    let run_id = submit(&store, "chat", None, Some("DEST_SECRET"))
        .await
        .unwrap();
    reject_audit(&store, "run.claim").await;
    assert!(store.claim_next_run().await.is_err());
    let run = store.get_run(run_id).await.unwrap().unwrap();
    assert_eq!(run.status, AgentRunStatus::Queued);
    assert!(run.started_at.is_none());
    allow_audit(&store).await;
    store.claim_next_run().await.unwrap().unwrap();
    reject_audit(&store, "run.trace").await;
    assert!(store
        .append_event(run_id, "model", json!({"phase":"request"}), Utc::now())
        .await
        .is_err());
    assert!(store.list_run_log(run_id).await.unwrap().is_empty());
    allow_audit(&store).await;
    reject_audit(&store, "run.succeed").await;
    let before = events(&store, run_id).await.len();
    assert!(store
        .finalize_run_success(
            run_id,
            &json!({"reply":"secret"}),
            Some(&json!({"messages":[]}))
        )
        .await
        .is_err());
    let run = store.get_run(run_id).await.unwrap().unwrap();
    assert_eq!(run.status, AgentRunStatus::Running);
    assert!(run.output.is_none());
    assert!(store.list_run_log(run_id).await.unwrap().is_empty());
    assert!(store
        .get_context_state("one", "bot", "chat")
        .await
        .unwrap()
        .is_none());
    assert!(store
        .list_delivery_outbox(Some("one"), None, Some(run_id), 10)
        .await
        .unwrap()
        .is_empty());
    assert_eq!(events(&store, run_id).await.len(), before);
}

#[tokio::test]
async fn audit_run_cancel_failure_and_restart_are_atomic_and_restart_has_no_delivery() {
    let (_dir, store) = fixture().await;
    let first = submit(&store, "first", None, Some("DEST_SECRET"))
        .await
        .unwrap();
    store.claim_next_run().await.unwrap().unwrap();
    for action in ["run.cancel", "run.fail", "run.restart"] {
        reject_audit(&store, action).await;
        let result = match action {
            "run.cancel" => store
                .cancel_run_request(first, "CANCEL_SECRET")
                .await
                .map(|_| ()),
            "run.fail" => store.fail_run(first, "ERROR_SECRET").await,
            _ => store.reset_local_runtime_state().await,
        };
        assert!(result.is_err());
        assert_eq!(
            store.get_run(first).await.unwrap().unwrap().status,
            AgentRunStatus::Running
        );
        assert!(store.list_run_log(first).await.unwrap().is_empty());
        assert!(store
            .list_delivery_outbox(Some("one"), None, Some(first), 10)
            .await
            .unwrap()
            .is_empty());
        allow_audit(&store).await;
    }
    let second = submit(&store, "second", None, None).await.unwrap();
    store.claim_next_run().await.unwrap().unwrap();
    let queued = submit(&store, "queued", None, None).await.unwrap();
    store.reset_local_runtime_state().await.unwrap();
    for run_id in [first, second] {
        assert_eq!(
            store.get_run(run_id).await.unwrap().unwrap().status,
            AgentRunStatus::Failed
        );
        let audit = events(&store, run_id).await;
        assert_eq!(
            audit
                .iter()
                .filter(|event| event.action == "run.restart")
                .count(),
            1
        );
        assert!(audit
            .iter()
            .any(|event| event.details["error_code"] == "runtime_restarted"));
        let trace = store.list_run_log(run_id).await.unwrap();
        assert_eq!(
            trace
                .iter()
                .map(|event| event.kind.as_str())
                .collect::<Vec<_>>(),
            ["error", "status"]
        );
    }
    assert_eq!(
        store.get_run(queued).await.unwrap().unwrap().status,
        AgentRunStatus::Queued
    );
    assert!(store
        .list_delivery_outbox(Some("one"), None, None, 10)
        .await
        .unwrap()
        .is_empty());
    let count = events(&store, first).await.len();
    store.reset_local_runtime_state().await.unwrap();
    assert_eq!(events(&store, first).await.len(), count);
}

#[tokio::test]
async fn audit_run_failure_enqueues_once_and_redacts_failure_content() {
    let (_dir, store) = fixture().await;
    let run_id = submit(&store, "chat", None, Some("DEST_SECRET"))
        .await
        .unwrap();
    store.claim_next_run().await.unwrap().unwrap();
    store
        .fail_run(run_id, "provider error ERROR_SECRET")
        .await
        .unwrap();
    store.fail_run(run_id, "second ERROR_SECRET").await.unwrap();
    let audit = events(&store, run_id).await;
    assert_eq!(
        audit
            .iter()
            .filter(|event| event.action == "delivery.enqueue")
            .count(),
        1
    );
    assert!(audit
        .iter()
        .any(|event| event.action == "run.fail" && event.outcome == "noop"));
    assert!(audit.iter().any(
        |event| event.action == "run.fail" && event.details["error_code"] == "execution_failed"
    ));
    let serialized = serde_json::to_string(&audit).unwrap();
    assert!(!serialized.contains("ERROR_SECRET"));
    assert!(!serialized.contains("DEST_SECRET"));
    assert_eq!(
        store
            .list_delivery_outbox(Some("one"), None, Some(run_id), 10)
            .await
            .unwrap()
            .len(),
        1
    );
}

#[tokio::test]
async fn audit_delivery_claim_and_ack_roll_back_on_audit_failure() {
    let (_dir, store) = fixture().await;
    let run_id = submit(&store, "chat", None, Some("DEST_SECRET"))
        .await
        .unwrap();
    store.claim_next_run().await.unwrap().unwrap();
    store
        .finalize_run_success(run_id, &json!({"reply":"OUTPUT_SECRET"}), None)
        .await
        .unwrap();
    let delivery_id = store
        .list_delivery_outbox(Some("one"), None, Some(run_id), 10)
        .await
        .unwrap()[0]
        .delivery_id;
    let now = Utc::now();
    reject_audit(&store, "delivery.claim").await;
    assert!(store
        .claim_delivery_outbox("one", 1, now, Duration::from_secs(60))
        .await
        .is_err());
    let pending = store
        .get_delivery_outbox(delivery_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(pending.status, "pending");
    assert!(pending.claim_token.is_none());
    allow_audit(&store).await;
    let claimed = store
        .claim_delivery_outbox("one", 1, now, Duration::from_secs(60))
        .await
        .unwrap()
        .pop()
        .unwrap();
    let token = claimed.claim_token.unwrap();
    reject_audit(&store, "delivery.ack").await;
    assert!(store
        .ack_delivery(
            "one",
            DeliveryAck {
                delivery_id,
                claim_token: &token,
                outcome: "retry",
                error: Some("ACK_SECRET"),
                retry_after: None,
                now,
            }
        )
        .await
        .is_err());
    let unchanged = store
        .get_delivery_outbox(delivery_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(unchanged.status, "claimed");
    assert_eq!(unchanged.attempt, 0);
    assert_eq!(unchanged.claim_token.as_deref(), Some(token.as_str()));
    assert!(unchanged.last_error.is_none());
    allow_audit(&store).await;
    store
        .ack_delivery(
            "one",
            DeliveryAck {
                delivery_id,
                claim_token: &token,
                outcome: "retry",
                error: Some("ACK_SECRET"),
                retry_after: Some(Duration::from_secs(60)),
                now,
            },
        )
        .await
        .unwrap();
    let count = events(&store, run_id).await.len();
    assert!(store
        .claim_delivery_outbox(
            "one",
            1,
            now + ChronoDuration::seconds(1),
            Duration::from_secs(60)
        )
        .await
        .unwrap()
        .is_empty());
    assert_eq!(events(&store, run_id).await.len(), count);
    let serialized = serde_json::to_string(&events(&store, run_id).await).unwrap();
    for secret in ["DEST_SECRET", "OUTPUT_SECRET", "ACK_SECRET", token.as_str()] {
        assert!(!serialized.contains(secret));
    }
}

#[tokio::test]
async fn audit_delivery_ack_requires_current_lease_and_one_changed_row() {
    let (_dir, store) = fixture().await;
    let run_id = submit(&store, "chat", None, Some("DEST_SECRET"))
        .await
        .unwrap();
    store.claim_next_run().await.unwrap().unwrap();
    store
        .finalize_run_success(run_id, &json!({}), None)
        .await
        .unwrap();
    let now = Utc::now();
    let first = store
        .claim_delivery_outbox("one", 1, now, Duration::from_secs(1))
        .await
        .unwrap()
        .pop()
        .unwrap();
    let reclaimed_at = now + ChronoDuration::seconds(2);
    let second = store
        .claim_delivery_outbox("one", 1, reclaimed_at, Duration::from_secs(60))
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert_ne!(first.claim_token, second.claim_token);
    let old = first.claim_token.unwrap();
    assert!(store
        .ack_delivery(
            "one",
            DeliveryAck {
                delivery_id: first.delivery_id,
                claim_token: &old,
                outcome: "delivered",
                error: None,
                retry_after: None,
                now: reclaimed_at,
            }
        )
        .await
        .is_err());
    let token = second.claim_token.unwrap();
    db::query("CREATE TRIGGER ignore_test_delivery_ack BEFORE UPDATE OF status ON deliveries WHEN NEW.status = 'delivered' BEGIN SELECT RAISE(IGNORE); END")
        .execute(&store.pool).await.unwrap();
    let error = store
        .ack_delivery(
            "one",
            DeliveryAck {
                delivery_id: second.delivery_id,
                claim_token: &token,
                outcome: "delivered",
                error: None,
                retry_after: None,
                now: reclaimed_at,
            },
        )
        .await
        .unwrap_err();
    assert!(error.to_string().contains("claim changed"));
    assert!(events(&store, run_id)
        .await
        .iter()
        .all(|event| event.action != "delivery.ack"));
    db::query("DROP TRIGGER ignore_test_delivery_ack")
        .execute(&store.pool)
        .await
        .unwrap();
    let delivered = store
        .ack_delivery(
            "one",
            DeliveryAck {
                delivery_id: second.delivery_id,
                claim_token: &token,
                outcome: "delivered",
                error: None,
                retry_after: None,
                now: reclaimed_at,
            },
        )
        .await
        .unwrap();
    assert_eq!(delivered.status, "delivered");
    assert_eq!(delivered.attempt, 1);
    assert_eq!(
        events(&store, run_id)
            .await
            .iter()
            .filter(|event| event.action == "delivery.ack")
            .count(),
        1
    );
}
