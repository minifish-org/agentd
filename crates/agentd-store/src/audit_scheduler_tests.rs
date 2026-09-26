use super::*;
use agentd_api::{AgentLimits, AgentSpec, ResourceMeta};
use tempfile::TempDir;

async fn fixture() -> (TempDir, AgentdStore) {
    let dir = TempDir::new().unwrap();
    let store = AgentdStore::new(dir.path().join("audit.db").to_str().unwrap())
        .await
        .unwrap();
    store.create_tenant("demo", &json!({})).await.unwrap();
    for name in ["bot", MEMORY_MAINTAINER_AGENT, BEHAVIOR_LEARNER_AGENT] {
        store
            .apply_agent(&AgentResource {
                metadata: ResourceMeta {
                    tenant: "demo".into(),
                    name: name.into(),
                    labels: BTreeMap::new(),
                },
                spec: AgentSpec {
                    allowed_families: Some(vec![]),
                    limits: AgentLimits {
                        timeout_ms: 1000,
                        max_steps: 64,
                    },
                    system_prompt: None,
                    model: None,
                    temperature: None,
                    max_tokens: None,
                    context_window: Some(0),
                },
            })
            .await
            .unwrap();
    }
    (dir, store)
}

async fn schedule(store: &AgentdStore, name: &str, agent: &str, payload: serde_json::Value) {
    store
        .put_schedule(
            "demo",
            name,
            &ScheduleSpec {
                agent_ref: agent.into(),
                scope: "test".into(),
                payload,
                delivery: None,
                at: None,
                cron: Some("0 4 * * *".into()),
                timezone: Some("UTC".into()),
                enabled: true,
            },
        )
        .await
        .unwrap();
}

async fn due(store: &AgentdStore, name: &str, now: DateTime<Utc>) {
    db::query("UPDATE schedules SET next_trigger_at = ? WHERE tenant = 'demo' AND name = ?")
        .bind((now - ChronoDuration::seconds(1)).to_rfc3339())
        .bind(name)
        .execute(&store.pool)
        .await
        .unwrap();
}

async fn tick(store: &AgentdStore, now: DateTime<Utc>) -> Result<Vec<Uuid>> {
    with_audit_context(
        AuditContext::system("scheduler"),
        store.trigger_due_schedules(now, 32),
    )
    .await
}

async fn events(store: &AgentdStore, action: &str) -> Vec<AuditEvent> {
    store
        .list_audit_events(&AuditQuery {
            tenant: Some("demo".into()),
            action: Some(action.into()),
            limit: Some(200),
            ..Default::default()
        })
        .await
        .unwrap()
        .events
}

async fn seed_memory(store: &AgentdStore, namespace: &str, count: usize) {
    let mut embedding = vec![0.0; MEMORY_EMBEDDING_DIM];
    embedding[0] = 1.0;
    for index in 0..count {
        store
            .put_memory(
                "demo",
                namespace,
                &format!("id-{index}"),
                "PRIVATE_MEMORY_BODY",
                &embedding,
            )
            .await
            .unwrap();
    }
}

async fn source(store: &AgentdStore, scope: &str) -> AgentRun {
    let id = store
        .submit_run(NewRun {
            tenant: "demo",
            name: "source",
            agent_ref: "bot",
            scope,
            source: "test",
            input: &json!({"text":"PRIVATE_SOURCE_BODY"}),
            request_id: None,
            schedule_name: None,
            delivery_destination: None,
        })
        .await
        .unwrap();
    assert_eq!(
        store.claim_next_run().await.unwrap().unwrap().run.run_id,
        id
    );
    store
        .finalize_run_success(id, &json!({"reply":"PRIVATE_OUTPUT_BODY"}), None)
        .await
        .unwrap();
    store.get_run(id).await.unwrap().unwrap()
}

#[tokio::test]
async fn scheduler_audits_no_targets_only_for_due_schedules() {
    let (_dir, store) = fixture().await;
    store.delete_agent("demo", "bot").await.unwrap();
    schedule(
        &store,
        MEMORY_MAINTENANCE_SCHEDULE,
        MEMORY_MAINTAINER_AGENT,
        json!({"namespace":"*"}),
    )
    .await;
    schedule(
        &store,
        BEHAVIOR_LEARNING_SCHEDULE,
        BEHAVIOR_LEARNER_AGENT,
        json!({"target_agent":"*"}),
    )
    .await;
    let now = Utc::now();
    assert!(tick(&store, now).await.unwrap().is_empty());
    assert!(events(&store, "schedule.decision").await.is_empty());
    assert!(events(&store, "schedule.trigger").await.is_empty());
    for name in [MEMORY_MAINTENANCE_SCHEDULE, BEHAVIOR_LEARNING_SCHEDULE] {
        due(&store, name, now).await;
    }
    assert!(tick(&store, now).await.unwrap().is_empty());
    let decisions = events(&store, "schedule.decision").await;
    assert_eq!(decisions.len(), 2);
    for event in decisions {
        assert_eq!(event.outcome, "skipped");
        assert_eq!(event.details["reason"], "no_targets");
        assert_eq!(event.actor_id, "scheduler");
        assert!(event.run_id.is_none());
    }
    assert_eq!(events(&store, "schedule.trigger").await.len(), 2);
    for name in [MEMORY_MAINTENANCE_SCHEDULE, BEHAVIOR_LEARNING_SCHEDULE] {
        let current = store.get_schedule("demo", name).await.unwrap().unwrap();
        assert_eq!(current.last_triggered_at, Some(now));
        assert!(current.next_trigger_at.unwrap() > now);
    }
    assert!(tick(&store, now).await.unwrap().is_empty());
    assert_eq!(events(&store, "schedule.decision").await.len(), 2);
}

#[tokio::test]
async fn scheduler_audits_memory_sparse_queued_pending_and_unchanged_without_content() {
    let (_dir, store) = fixture().await;
    seed_memory(&store, "profile", 1).await;
    schedule(
        &store,
        MEMORY_MAINTENANCE_SCHEDULE,
        MEMORY_MAINTAINER_AGENT,
        json!({"namespace":"*","min_entries":5}),
    )
    .await;
    let now = Utc::now();
    due(&store, MEMORY_MAINTENANCE_SCHEDULE, now).await;
    assert!(tick(&store, now).await.unwrap().is_empty());
    let sparse = events(&store, "schedule.decision").await.remove(0);
    assert_eq!(sparse.details["reason"], "too_few_entries");
    assert_eq!(sparse.details["entries"], 1);
    assert_eq!(sparse.details["min_entries"], 5);
    seed_memory(&store, "profile", 5).await;
    due(&store, MEMORY_MAINTENANCE_SCHEDULE, now).await;
    let runs = tick(&store, now).await.unwrap();
    assert_eq!(runs.len(), 1);
    let queued = events(&store, "schedule.decision").await.remove(0);
    assert_eq!(queued.outcome, "queued");
    assert_eq!(queued.run_id, Some(runs[0]));
    assert_eq!(queued.details["namespace"], "profile");
    assert_eq!(queued.details["entries"], 5);
    let trace = store.list_run_log(runs[0]).await.unwrap().remove(0);
    assert!(events(&store, "run.trace")
        .await
        .iter()
        .any(|event| event.run_id == Some(runs[0]) && event.details["trace_id"] == trace.id));
    due(&store, MEMORY_MAINTENANCE_SCHEDULE, now).await;
    assert!(tick(&store, now).await.unwrap().is_empty());
    let pending = events(&store, "schedule.decision").await.remove(0);
    assert_eq!(pending.details["reason"], "already_pending");
    assert_eq!(pending.details["pending"], true);
    assert_eq!(pending.details["entries"], 5);
    assert_eq!(
        store.claim_next_run().await.unwrap().unwrap().run.run_id,
        runs[0]
    );
    let ready = store.prepare_memory_maintenance(runs[0], 5).await.unwrap();
    assert!(ready.ready);
    store.prepare_memory_maintenance(runs[0], 5).await.unwrap();
    assert_eq!(events(&store, "memory_maintenance.prepare").await.len(), 1);
    assert_eq!(
        events(&store, "memory_maintenance.external_change")
            .await
            .len(),
        5
    );
    let mut embedding = vec![0.0; MEMORY_EMBEDDING_DIM];
    embedding[0] = 1.0;
    store
        .put_memory_with_graph_for_run(
            runs[0],
            "profile",
            "id-0",
            "PRIVATE_MAINTAINED_BODY",
            &embedding,
            &MemoryGraphInput::default(),
        )
        .await
        .unwrap();
    assert_eq!(
        events(&store, "memory_maintenance.external_change")
            .await
            .len(),
        5
    );
    store
        .finalize_run_success(
            runs[0],
            &json!({"reply":"PRIVATE_MAINTENANCE_REPORT"}),
            None,
        )
        .await
        .unwrap();
    let checkpoint = events(&store, "memory_maintenance.checkpoint")
        .await
        .remove(0);
    assert_eq!(checkpoint.run_id, Some(runs[0]));
    assert_eq!(
        checkpoint.details["consumed_revision"],
        ready.external_revision
    );
    due(&store, MEMORY_MAINTENANCE_SCHEDULE, now).await;
    assert!(tick(&store, now).await.unwrap().is_empty());
    assert_eq!(
        events(&store, "schedule.decision").await[0].details["reason"],
        "unchanged"
    );
    assert_eq!(
        store
            .get_schedule("demo", MEMORY_MAINTENANCE_SCHEDULE)
            .await
            .unwrap()
            .unwrap()
            .last_run_id,
        Some(runs[0])
    );
    let all = store
        .list_audit_events(&AuditQuery {
            tenant: Some("demo".into()),
            limit: Some(200),
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(!serde_json::to_string(&all).unwrap().contains("PRIVATE_"));
}

#[tokio::test]
async fn scheduler_audits_learning_readiness_counts_and_pending_target() {
    let (_dir, store) = fixture().await;
    schedule(
        &store,
        BEHAVIOR_LEARNING_SCHEDULE,
        BEHAVIOR_LEARNER_AGENT,
        json!({"target_agent":"*","min_samples":4,"max_samples":4}),
    )
    .await;
    let now = Utc::now();
    due(&store, BEHAVIOR_LEARNING_SCHEDULE, now).await;
    assert!(tick(&store, now).await.unwrap().is_empty());
    let sparse = events(&store, "schedule.decision").await.remove(0);
    assert_eq!(sparse.details["reason"], "insufficient_samples");
    assert_eq!(sparse.details["source_runs"], 0);
    assert_eq!(sparse.details["min_samples"], 4);
    for _ in 0..4 {
        source(&store, "scope/one").await;
    }
    due(&store, BEHAVIOR_LEARNING_SCHEDULE, now).await;
    assert!(tick(&store, now).await.unwrap().is_empty());
    let single_scope = events(&store, "schedule.decision").await.remove(0);
    assert_eq!(
        single_scope.details["reason"],
        "insufficient_independent_scopes"
    );
    assert_eq!(single_scope.details["source_runs"], 4);
    assert_eq!(single_scope.details["independent_scopes"], 1);
    source(&store, "scope/two").await;
    due(&store, BEHAVIOR_LEARNING_SCHEDULE, now).await;
    let runs = tick(&store, now).await.unwrap();
    assert_eq!(runs.len(), 1);
    let queued = events(&store, "schedule.decision").await.remove(0);
    assert_eq!(queued.run_id, Some(runs[0]));
    assert_eq!(queued.outcome, "queued");
    assert_eq!(queued.details["target_agent"], "bot");
    assert_eq!(queued.details["source_runs"], 5);
    assert_eq!(queued.details["independent_scopes"], 2);
    assert_eq!(queued.details["required_scopes"], 2);
    due(&store, BEHAVIOR_LEARNING_SCHEDULE, now).await;
    assert!(tick(&store, now).await.unwrap().is_empty());
    let pending = events(&store, "schedule.decision").await.remove(0);
    assert_eq!(pending.details["reason"], "already_pending");
    assert_eq!(pending.details["source_runs"], 5);
    assert!(store
        .list_run_log(runs[0])
        .await
        .unwrap()
        .iter()
        .all(|event| event.kind != "model"));
}

#[tokio::test]
async fn scheduler_partial_failure_keeps_committed_runs_and_safe_audit_summary() {
    let (_dir, store) = fixture().await;
    for namespace in ["a-good", "z-fail", "zz-aborted"] {
        seed_memory(&store, namespace, 5).await;
    }
    schedule(
        &store,
        MEMORY_MAINTENANCE_SCHEDULE,
        MEMORY_MAINTAINER_AGENT,
        json!({"namespace":"*"}),
    )
    .await;
    db::query("CREATE TRIGGER reject_second_target BEFORE INSERT ON runs WHEN json_extract(NEW.input_json, '$.namespace') = 'z-fail' BEGIN SELECT RAISE(ABORT, 'PRIVATE_DATABASE_ERROR'); END")
        .execute(&store.pool).await.unwrap();
    let now = Utc::now();
    due(&store, MEMORY_MAINTENANCE_SCHEDULE, now).await;
    assert!(tick(&store, now).await.is_err());
    let runs = store
        .list_runs(&RunListQuery {
            tenant: Some("demo".into()),
            agent_ref: Some(MEMORY_MAINTAINER_AGENT.into()),
            status: None,
            limit: 10,
        })
        .await
        .unwrap();
    assert_eq!(runs.len(), 1);
    assert_eq!(runs[0].input["namespace"], "a-good");
    let summary = events(&store, "schedule.trigger").await.remove(0);
    assert_eq!(summary.outcome, "partial");
    assert_eq!(summary.details["reason"], "enqueue_failed");
    assert_eq!(summary.details["queued_count"], 1);
    assert_eq!(summary.details["target_count"], 3);
    assert_eq!(summary.details["skipped_count"], 1);
    assert_eq!(summary.details["run_ids"], json!([runs[0].run_id]));
    let decisions = events(&store, "schedule.decision").await;
    assert!(decisions
        .iter()
        .any(|event| event.outcome == "queued" && event.run_id == Some(runs[0].run_id)));
    assert!(decisions
        .iter()
        .any(|event| event.outcome == "failed" && event.details["namespace"] == "z-fail"));
    assert!(decisions.iter().any(|event| event.outcome == "skipped"
        && event.details["namespace"] == "zz-aborted"
        && event.details["reason"] == "fanout_aborted"));
    assert!(!serde_json::to_string(&decisions)
        .unwrap()
        .contains("PRIVATE_DATABASE_ERROR"));
    let current = store
        .get_schedule("demo", MEMORY_MAINTENANCE_SCHEDULE)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(current.last_run_id, Some(runs[0].run_id));
    assert!(current.next_trigger_at.unwrap() > now);
}

#[tokio::test]
async fn maintenance_prepare_rolls_back_when_its_audit_cannot_commit() {
    let (_dir, store) = fixture().await;
    seed_memory(&store, "profile", 5).await;
    let id = store
        .submit_run(NewRun {
            tenant: "demo",
            name: "maintenance",
            agent_ref: MEMORY_MAINTAINER_AGENT,
            scope: "x",
            source: "test",
            input: &json!({"namespace":"profile"}),
            request_id: None,
            schedule_name: None,
            delivery_destination: None,
        })
        .await
        .unwrap();
    assert_eq!(
        store.claim_next_run().await.unwrap().unwrap().run.run_id,
        id
    );
    db::query("CREATE TRIGGER reject_prepare_audit BEFORE INSERT ON audit_events WHEN NEW.action = 'memory_maintenance.prepare' BEGIN SELECT RAISE(ABORT, 'audit fixture rejected'); END")
        .execute(&store.pool).await.unwrap();
    assert!(store.prepare_memory_maintenance(id, 5).await.is_err());
    assert_eq!(
        db::query_scalar::<i64>("SELECT COUNT(*) FROM memory_maintenance_runs WHERE run_id = ?")
            .bind(id.to_string())
            .fetch_optional(&store.pool)
            .await
            .unwrap(),
        Some(0)
    );
    assert!(events(&store, "memory_maintenance.prepare")
        .await
        .is_empty());
    assert!(
        store
            .memory_maintenance_readiness("demo", "profile", 5)
            .await
            .unwrap()
            .ready
    );
}

async fn learning_cycle(store: &AgentdStore) -> Uuid {
    let id = store
        .submit_run(NewRun {
            tenant: "demo",
            name: "learning",
            agent_ref: BEHAVIOR_LEARNER_AGENT,
            scope: "ignored",
            source: "test",
            input: &json!({"target_agent":"bot"}),
            request_id: None,
            schedule_name: None,
            delivery_destination: None,
        })
        .await
        .unwrap();
    assert_eq!(
        store.claim_next_run().await.unwrap().unwrap().run.run_id,
        id
    );
    id
}

#[tokio::test]
async fn behavior_audits_outcomes_cursor_and_clear_without_private_content() {
    let (_dir, store) = fixture().await;
    let spec = store.get_agent("demo", "bot").await.unwrap().unwrap().spec;
    for (index, (expected_revision, promote, outcome)) in [
        (None, true, "promoted"),
        (Some(1), false, "rejected"),
        (None, true, "stale"),
    ]
    .into_iter()
    .enumerate()
    {
        let source = source(&store, &format!("case/{index}")).await;
        let run_id = learning_cycle(&store).await;
        let result = store.finish_behavior_learning(run_id, BehaviorLearningResult {
            target_agent:"bot",expected_spec:&spec,expected_revision,
            instructions:"PRIVATE_LEARNED_INSTRUCTIONS",
            report:&json!({"source_runs":[source.run_id],"evaluations":[{"reason":"PRIVATE_JUDGE_REASON"}],"private":"PRIVATE_REPORT"}),
            promote,source_updated_at:source.updated_at,source_run_id:source.run_id,
        }).await.unwrap();
        assert_eq!(result.outcome, outcome);
        let event = events(&store, "behavior.finish").await.remove(0);
        assert_eq!(event.outcome, outcome);
        assert_eq!(event.run_id, Some(run_id));
        assert_eq!(event.details["revision"], index + 1);
        assert_eq!(event.details["source_run_id"], source.run_id.to_string());
        assert_eq!(event.details["source_cursor_advanced"], outcome != "stale");
        assert_eq!(event.details["source_count"], 1);
        assert_eq!(event.details["evaluation_count"], 1);
        let traces = store.list_run_log(run_id).await.unwrap();
        let trace_audits = events(&store, "run.trace")
            .await
            .into_iter()
            .filter(|event| event.run_id == Some(run_id))
            .collect::<Vec<_>>();
        assert_eq!(trace_audits.len(), traces.len());
        for trace in traces {
            assert!(trace_audits
                .iter()
                .any(|event| event.resource_id == Some(trace.id.to_string())
                    && event.details["trace_id"] == trace.id));
        }
    }
    assert_eq!(events(&store, "behavior.source_cursor").await.len(), 2);
    assert!(store.clear_behavior_policy("demo", "bot").await.unwrap());
    assert!(!store.clear_behavior_policy("demo", "bot").await.unwrap());
    let clears = events(&store, "behavior.clear").await;
    assert_eq!(clears[0].outcome, "noop");
    assert_eq!(clears[1].outcome, "succeeded");
    assert_eq!(clears[1].details["previous_revision"], 1);
    assert_eq!(
        store
            .list_behavior_revisions("demo", "bot", 20)
            .await
            .unwrap()
            .len(),
        3
    );
    let all = store
        .list_audit_events(&AuditQuery {
            tenant: Some("demo".into()),
            limit: Some(200),
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(!serde_json::to_string(&all).unwrap().contains("PRIVATE_"));
}

#[tokio::test]
async fn behavior_publication_rolls_back_when_its_audit_cannot_commit() {
    let (_dir, store) = fixture().await;
    let spec = store.get_agent("demo", "bot").await.unwrap().unwrap().spec;
    let source = source(&store, "case").await;
    let run_id = learning_cycle(&store).await;
    db::query("CREATE TRIGGER reject_behavior_audit BEFORE INSERT ON audit_events WHEN NEW.action = 'behavior.finish' BEGIN SELECT RAISE(ABORT, 'audit fixture rejected'); END")
        .execute(&store.pool).await.unwrap();
    assert!(store
        .finish_behavior_learning(
            run_id,
            BehaviorLearningResult {
                target_agent: "bot",
                expected_spec: &spec,
                expected_revision: None,
                instructions: "PRIVATE_CANDIDATE",
                report: &json!({}),
                promote: true,
                source_updated_at: source.updated_at,
                source_run_id: source.run_id,
            }
        )
        .await
        .is_err());
    assert!(store
        .get_behavior_snapshot("demo", "bot")
        .await
        .unwrap()
        .unwrap()
        .active_revision
        .is_none());
    assert!(store
        .list_behavior_revisions("demo", "bot", 20)
        .await
        .unwrap()
        .is_empty());
    assert_eq!(
        store
            .list_behavior_source_runs("demo", "bot", 20)
            .await
            .unwrap()
            .len(),
        1
    );
    let run = store.get_run(run_id).await.unwrap().unwrap();
    assert_eq!(run.status, AgentRunStatus::Running);
    assert!(run.output.is_none());
    assert!(store.list_run_log(run_id).await.unwrap().is_empty());
    assert!(events(&store, "behavior.finish").await.is_empty());
    assert!(events(&store, "behavior.source_cursor").await.is_empty());
    assert!(events(&store, "run.trace")
        .await
        .into_iter()
        .all(|event| event.run_id != Some(run_id)));
}
