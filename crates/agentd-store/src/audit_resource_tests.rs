use super::*;
use agentd_api::{AgentLimits, AgentSpec, McpServerSpec, McpTool, McpTransport, ResourceMeta};
use tempfile::TempDir;

const PRIVATE: &str = "AUDIT_PRIVATE_SENTINEL";

async fn fixture() -> (TempDir, AgentdStore) {
    let dir = TempDir::new().unwrap();
    let store = AgentdStore::new(dir.path().join("audit.db").to_str().unwrap())
        .await
        .unwrap();
    (dir, store)
}

fn agent() -> AgentResource {
    AgentResource {
        metadata: ResourceMeta {
            tenant: "demo".into(),
            name: "bot".into(),
            labels: BTreeMap::from([("private".into(), PRIVATE.into())]),
        },
        spec: AgentSpec {
            allowed_families: Some(vec![]),
            limits: AgentLimits {
                timeout_ms: 1000,
                max_steps: 4,
            },
            system_prompt: Some(PRIVATE.into()),
            model: Some(PRIVATE.into()),
            temperature: None,
            max_tokens: None,
            context_window: Some(4),
        },
    }
}

fn embedding() -> Vec<f32> {
    let mut embedding = vec![0.0; MEMORY_EMBEDDING_DIM];
    embedding[0] = 1.0;
    embedding
}

async fn running(store: &AgentdStore) -> Uuid {
    let id = store
        .submit_run(NewRun {
            tenant: "demo",
            name: "turn",
            agent_ref: "bot",
            scope: "chat",
            source: "test",
            input: &json!({"text":PRIVATE}),
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

async fn populate(store: &AgentdStore) -> Uuid {
    store
        .create_tenant("demo", &json!({"private":PRIVATE}))
        .await
        .unwrap();
    store.apply_agent(&agent()).await.unwrap();
    store
        .apply_mcp_server(
            "demo",
            "lookup",
            &McpServerSpec {
                enabled: true,
                transport: McpTransport::Http {
                    url: format!("http://127.0.0.1/mcp?token={PRIVATE}"),
                    headers_from: BTreeMap::from([("Authorization".into(), PRIVATE.into())]),
                },
                allowed_tools: None,
            },
            &[McpTool {
                name: "lookup".into(),
                description: Some(PRIVATE.into()),
                input_schema: json!({"type":"object","description":PRIVATE}),
            }],
            Some(PRIVATE),
        )
        .await
        .unwrap();
    store
        .put_schedule(
            "demo",
            "daily",
            &ScheduleSpec {
                agent_ref: "bot".into(),
                scope: "chat".into(),
                payload: json!({"private":PRIVATE}),
                delivery: None,
                at: None,
                cron: Some("0 4 * * *".into()),
                timezone: Some("UTC".into()),
                enabled: false,
            },
        )
        .await
        .unwrap();
    store
        .put_memory("demo", "profile", "fact", PRIVATE, &embedding())
        .await
        .unwrap();
    store
        .put_artifact(
            "demo",
            "/report.txt",
            PRIVATE.as_bytes(),
            "text/plain",
            Some(&json!({"private":PRIVATE}).to_string()),
        )
        .await
        .unwrap();
    let id = running(store).await;
    store
        .finalize_run_success(
            id,
            &json!({"reply":PRIVATE}),
            Some(&json!({"private":PRIVATE})),
        )
        .await
        .unwrap();
    id
}

async fn history(store: &AgentdStore) -> Vec<AuditEvent> {
    store
        .list_audit_events(&AuditQuery {
            tenant: Some("demo".into()),
            limit: Some(200),
            ..Default::default()
        })
        .await
        .unwrap()
        .events
}

async fn reject_action(store: &AgentdStore, action: &str) {
    db::query("DROP TRIGGER IF EXISTS reject_resource_audit")
        .execute(&store.pool)
        .await
        .unwrap();
    // Only static test action names are passed here.
    db::query(&format!("CREATE TRIGGER reject_resource_audit BEFORE INSERT ON audit_events WHEN NEW.action = '{action}' BEGIN SELECT RAISE(ABORT, 'audit fixture rejected'); END"))
        .execute(&store.pool).await.unwrap();
}

#[tokio::test]
async fn public_resource_audits_keep_request_identity_and_omit_content() {
    let (_dir, store) = fixture().await;
    let request_id = Uuid::new_v4();
    let run_id = with_audit_context(AuditContext::api(true, request_id), async {
        let id = populate(&store).await;
        store
            .patch_tenant_metadata("demo", &json!({"changed":PRIVATE}), None)
            .await
            .unwrap();
        assert!(store
            .delete_context_state("demo", "bot", "chat")
            .await
            .unwrap());
        assert!(!store
            .delete_context_state("demo", "bot", "chat")
            .await
            .unwrap());
        assert!(store
            .delete_memory("demo", "profile", "fact")
            .await
            .unwrap());
        store.delete_artifact("demo", "/report.txt").await.unwrap();
        assert_eq!(
            store.delete_schedule("demo", "daily").await.unwrap()["deleted"],
            true
        );
        assert!(store.delete_mcp_server("demo", "lookup").await.unwrap());
        assert!(store.delete_agent("demo", "bot").await.unwrap());
        id
    })
    .await;
    let events = history(&store).await;
    assert!(!serde_json::to_string(&events).unwrap().contains(PRIVATE));
    assert!(events
        .iter()
        .all(|event| event.request_id == Some(request_id)
            && event.actor_kind == "api"
            && event.actor_id == "shared_api_token"));
    for action in [
        "tenant.create",
        "tenant.metadata",
        "agent.put",
        "agent.delete",
        "mcp.put",
        "mcp.delete",
        "schedule.put",
        "schedule.delete",
        "memory.put",
        "memory.delete",
        "artifact.put",
        "artifact.delete",
        "context.put",
        "context.delete",
    ] {
        assert!(
            events.iter().any(|event| event.action == action),
            "missing {action}"
        );
    }
    let context = events
        .iter()
        .find(|event| event.action == "context.put")
        .unwrap();
    assert_eq!(context.run_id, Some(run_id));
    assert_eq!(context.details["revision"], 1);
    let agent = events
        .iter()
        .find(|event| event.action == "agent.put")
        .unwrap();
    assert_eq!(agent.details["after"]["system_prompt_bytes"], PRIVATE.len());
    assert_eq!(agent.details["after"]["model_configured"], true);
    assert!(events
        .iter()
        .any(|event| event.action == "context.delete" && event.outcome == "noop"));
}

#[tokio::test]
async fn audit_summary_budget_preserves_long_memory_namespace() {
    let (_dir, store) = fixture().await;
    store.create_tenant("demo", &json!({})).await.unwrap();
    let namespace = format!("profile-{}", "字\"\\".repeat(20_000));
    assert!(namespace.len() > 65_536);
    let memory = store
        .put_memory("demo", &namespace, "fact", PRIVATE, &embedding())
        .await
        .unwrap();
    assert_eq!(memory.namespace, namespace);
    assert_eq!(
        store
            .get_memory("demo", &namespace, "fact")
            .await
            .unwrap()
            .unwrap()
            .text,
        PRIVATE
    );
    let events = history(&store).await;
    let event = events
        .iter()
        .find(|event| event.action == "memory.put")
        .unwrap();
    assert_eq!(event.details["namespace"], namespace);
    assert!(!serde_json::to_string(event).unwrap().contains(PRIVATE));
}

#[tokio::test]
async fn audit_summary_budget_preserves_long_context_scope_on_finalization() {
    let (_dir, store) = fixture().await;
    store.create_tenant("demo", &json!({})).await.unwrap();
    store.apply_agent(&agent()).await.unwrap();
    let scope = format!("chat-{}", "字\"\\".repeat(20_000));
    assert!(scope.len() > 65_536);
    let run_id = store
        .submit_run(NewRun {
            tenant: "demo",
            name: "turn",
            agent_ref: "bot",
            scope: &scope,
            source: "test",
            input: &json!({"text":PRIVATE}),
            request_id: None,
            schedule_name: None,
            delivery_destination: None,
        })
        .await
        .unwrap();
    store.claim_next_run().await.unwrap().unwrap();
    let state = json!({"private":PRIVATE});
    store
        .finalize_run_success(run_id, &json!({"reply":PRIVATE}), Some(&state))
        .await
        .unwrap();
    assert_eq!(
        store.get_run(run_id).await.unwrap().unwrap().status,
        AgentRunStatus::Succeeded
    );
    assert_eq!(
        store
            .get_context_state("demo", "bot", &scope)
            .await
            .unwrap()
            .unwrap()
            .state,
        state
    );
    let events = history(&store).await;
    let event = events
        .iter()
        .find(|event| event.action == "context.put")
        .unwrap();
    assert_eq!(event.details["scope"], scope);
    assert_eq!(event.run_id, Some(run_id));
    assert!(!serde_json::to_string(event).unwrap().contains(PRIVATE));
}

#[tokio::test]
async fn audit_summary_budget_rejects_oversized_non_identifier_details() {
    let (_dir, store) = fixture().await;
    let oversized = "x".repeat(65_537);
    for details in [
        json!({"private":oversized}),
        json!({"namespace":oversized,"private":oversized}),
        json!({"nested":{"scope":oversized}}),
        json!({"scope":{"private":oversized}}),
    ] {
        let error = store
            .append_audit(AuditInput::new(
                Some("demo"),
                "budget.test",
                "test",
                None,
                "recorded",
                details,
            ))
            .await
            .unwrap_err();
        assert!(error.to_string().contains("audit details exceed"));
    }
    assert!(store
        .list_audit_events(&AuditQuery {
            action: Some("budget.test".into()),
            ..Default::default()
        })
        .await
        .unwrap()
        .events
        .is_empty());
}

#[tokio::test]
async fn rejected_resource_audits_roll_back_resource_and_related_state() {
    let (_dir, store) = fixture().await;
    populate(&store).await;
    reject_action(&store, "tenant.create").await;
    assert!(store.create_tenant("rejected", &json!({})).await.is_err());
    assert!(store.get_tenant("rejected").await.unwrap().is_none());
    let before = history(&store).await.len();
    reject_action(&store, "agent.put").await;
    let mut changed = agent();
    changed.spec.limits.max_steps = 7;
    assert!(store.apply_agent(&changed).await.is_err());
    assert_eq!(
        store.get_agent("demo", "bot").await.unwrap().unwrap().spec,
        agent().spec
    );
    reject_action(&store, "mcp.put").await;
    let mcp = store
        .get_mcp_server("demo", "lookup")
        .await
        .unwrap()
        .unwrap();
    let mut changed_mcp = mcp.spec.clone();
    changed_mcp.enabled = false;
    assert!(store
        .apply_mcp_server("demo", "lookup", &changed_mcp, &[], None)
        .await
        .is_err());
    assert_eq!(
        store
            .get_mcp_server("demo", "lookup")
            .await
            .unwrap()
            .unwrap()
            .spec,
        mcp.spec
    );
    reject_action(&store, "schedule.put").await;
    let schedule = store.get_schedule("demo", "daily").await.unwrap().unwrap();
    let mut changed_schedule = schedule.spec.clone();
    changed_schedule.enabled = true;
    changed_schedule.payload = json!({"changed":true});
    assert!(store
        .put_schedule("demo", "daily", &changed_schedule)
        .await
        .is_err());
    assert_eq!(
        store
            .get_schedule("demo", "daily")
            .await
            .unwrap()
            .unwrap()
            .spec,
        schedule.spec
    );
    reject_action(&store, "memory.put").await;
    let revision = store
        .memory_maintenance_readiness("demo", "profile", 2)
        .await
        .unwrap()
        .external_revision;
    assert!(store
        .put_memory("demo", "profile", "fact", "changed", &embedding())
        .await
        .is_err());
    assert_eq!(
        store
            .get_memory("demo", "profile", "fact")
            .await
            .unwrap()
            .unwrap()
            .text,
        PRIVATE
    );
    assert_eq!(
        store
            .memory_maintenance_readiness("demo", "profile", 2)
            .await
            .unwrap()
            .external_revision,
        revision
    );
    reject_action(&store, "artifact.put").await;
    assert!(store
        .put_artifact("demo", "/report.txt", b"changed", "text/plain", None)
        .await
        .is_err());
    assert_eq!(
        store
            .get_artifact("demo", "/report.txt")
            .await
            .unwrap()
            .unwrap()
            .0,
        PRIVATE.as_bytes()
    );
    assert_eq!(history(&store).await.len(), before);

    reject_action(&store, "context.put").await;
    let run_id = running(&store).await;
    let before_finalize = history(&store).await.len();
    assert!(store
        .finalize_run_success(
            run_id,
            &json!({"reply":"changed"}),
            Some(&json!({"changed":true}))
        )
        .await
        .is_err());
    let run = store.get_run(run_id).await.unwrap().unwrap();
    assert_eq!(run.status, AgentRunStatus::Running);
    assert!(run.output.is_none());
    assert!(store.list_run_log(run_id).await.unwrap().is_empty());
    let context = store
        .get_context_state("demo", "bot", "chat")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(context.revision, 1);
    assert_eq!(context.state, json!({"private":PRIVATE}));
    assert_eq!(history(&store).await.len(), before_finalize);
}

#[tokio::test]
async fn tenant_delete_is_atomic_and_preserves_counted_audit_history() {
    let (_dir, store) = fixture().await;
    let run_id = populate(&store).await;
    let before = history(&store).await;
    reject_action(&store, "tenant.delete").await;
    assert!(store.delete_tenant("demo").await.is_err());
    assert!(store.get_tenant("demo").await.unwrap().is_some());
    assert!(store.get_agent("demo", "bot").await.unwrap().is_some());
    assert!(store.get_run(run_id).await.unwrap().is_some());
    assert!(store
        .get_memory("demo", "profile", "fact")
        .await
        .unwrap()
        .is_some());
    assert_eq!(history(&store).await, before);
    db::query("DROP TRIGGER reject_resource_audit")
        .execute(&store.pool)
        .await
        .unwrap();
    assert_eq!(store.delete_tenant("demo").await.unwrap()["deleted"], true);
    assert!(store.get_tenant("demo").await.unwrap().is_none());
    assert!(store.get_run(run_id).await.unwrap().is_none());
    let after = history(&store).await;
    assert_eq!(&after[1..], before.as_slice());
    assert_eq!(after[0].action, "tenant.delete");
    assert_eq!(after[0].outcome, "succeeded");
    for table in [
        "agents",
        "mcp_servers",
        "schedules",
        "memory",
        "artifacts",
        "contexts",
        "runs",
        "memory_maintenance_state",
    ] {
        assert_eq!(
            after[0].details["removed"][table], 1,
            "wrong removal count for {table}"
        );
    }
    assert_eq!(after[0].details["removed"]["run_log"], 2);
    assert!(!serde_json::to_string(&after).unwrap().contains(PRIVATE));
    assert_eq!(store.delete_tenant("demo").await.unwrap()["deleted"], false);
    let repeated = history(&store).await;
    assert_eq!(repeated[0].outcome, "noop");
    assert_eq!(repeated[0].details["removed"]["runs"], 0);
    assert_eq!(&repeated[1..], after.as_slice());
}

#[tokio::test]
async fn version_ten_upgrade_retains_resources_and_records_migration_once() {
    let (dir, store) = fixture().await;
    let run_id = populate(&store).await;
    // Recreate the v10 boundary: application tables exist, audit did not yet exist.
    db::query("DROP TABLE audit_events")
        .execute(&store.pool)
        .await
        .unwrap();
    db::query("PRAGMA user_version = 10")
        .execute(&store.pool)
        .await
        .unwrap();
    drop(store);
    let path = dir.path().join("audit.db");
    let migrated = AgentdStore::new(path.to_str().unwrap()).await.unwrap();
    assert_eq!(
        db::query_scalar::<i64>("PRAGMA user_version")
            .fetch_optional(&migrated.pool)
            .await
            .unwrap(),
        Some(11)
    );
    assert_eq!(
        migrated.get_tenant("demo").await.unwrap().unwrap().metadata,
        json!({"private":PRIVATE})
    );
    assert_eq!(
        migrated
            .get_agent("demo", "bot")
            .await
            .unwrap()
            .unwrap()
            .spec,
        agent().spec
    );
    assert!(migrated
        .get_mcp_server("demo", "lookup")
        .await
        .unwrap()
        .is_some());
    assert_eq!(
        migrated
            .get_schedule("demo", "daily")
            .await
            .unwrap()
            .unwrap()
            .spec
            .payload,
        json!({"private":PRIVATE})
    );
    assert_eq!(
        migrated
            .get_memory("demo", "profile", "fact")
            .await
            .unwrap()
            .unwrap()
            .text,
        PRIVATE
    );
    assert_eq!(
        migrated
            .get_artifact("demo", "/report.txt")
            .await
            .unwrap()
            .unwrap()
            .0,
        PRIVATE.as_bytes()
    );
    assert_eq!(
        migrated
            .get_context_state("demo", "bot", "chat")
            .await
            .unwrap()
            .unwrap()
            .state,
        json!({"private":PRIVATE})
    );
    assert_eq!(
        migrated.get_run(run_id).await.unwrap().unwrap().output,
        Some(json!({"reply":PRIVATE}))
    );
    let migrations = migrated
        .list_audit_events(&AuditQuery {
            action: Some("schema.migrate".into()),
            ..Default::default()
        })
        .await
        .unwrap()
        .events;
    assert_eq!(migrations.len(), 1);
    assert_eq!(
        migrations[0].details,
        json!({"from_version":10,"to_version":11})
    );
    assert_eq!(migrations[0].outcome, "succeeded");
    drop(migrated);
    let reopened = AgentdStore::new(path.to_str().unwrap()).await.unwrap();
    assert_eq!(
        reopened
            .list_audit_events(&AuditQuery {
                action: Some("schema.migrate".into()),
                ..Default::default()
            })
            .await
            .unwrap()
            .events
            .len(),
        1
    );
}
