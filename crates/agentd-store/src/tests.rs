use super::*;
use agentd_api::{AgentLimits, AgentSpec, McpServerSpec, McpTool, McpTransport, ResourceMeta};
use tempfile::TempDir;

async fn store() -> (TempDir, AgentdStore) {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("agentd.db");
    let store = AgentdStore::new(path.to_str().unwrap()).await.unwrap();
    (dir, store)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn parallel_connection_teardown_preserves_live_statements_and_transactions() {
    // Exercise native-handle reuse across threads. libsql 0.9.30 used to
    // close the final connection twice, corrupting unrelated allocations.
    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..8 {
        tasks.spawn(async {
            for i in 0..128 {
                let db = Builder::new_local(":memory:").build().await.unwrap();
                let conn = db.connect().unwrap();
                conn.execute("CREATE TABLE t(x INTEGER)", ()).await.unwrap();
                let tx = conn.transaction().await.unwrap();
                tx.execute("INSERT INTO t VALUES (1)", ()).await.unwrap();
                drop(conn);
                if i % 2 == 0 {
                    tx.commit().await.unwrap();
                } else {
                    tx.rollback().await.unwrap();
                }

                let conn = db.connect().unwrap();
                let statement = conn.prepare("SELECT 42").await.unwrap();
                drop(conn);
                let mut rows = statement.query(()).await.unwrap();
                assert_eq!(
                    rows.next().await.unwrap().unwrap().get::<i64>(0).unwrap(),
                    42
                );
            }
        });
    }
    while let Some(result) = tasks.join_next().await {
        result.unwrap();
    }
}

fn test_embedding(axis: usize) -> Vec<f32> {
    let mut embedding = vec![0.0; MEMORY_EMBEDDING_DIM];
    embedding[axis] = 1.0;
    embedding
}

fn graph(value: serde_json::Value) -> MemoryGraphInput {
    serde_json::from_value(value).unwrap()
}

async fn tenant_with_agent(store: &AgentdStore, tenant: &str) {
    store.create_tenant(tenant, &json!({})).await.unwrap();
    store
        .apply_agent(&AgentResource {
            metadata: ResourceMeta {
                name: "bot".into(),
                tenant: tenant.into(),
                labels: BTreeMap::new(),
            },
            spec: AgentSpec {
                allowed_families: None,
                limits: AgentLimits {
                    timeout_ms: 10_000,
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
}

async fn submit(store: &AgentdStore, tenant: &str, scope: &str, request_id: Option<&str>) -> Uuid {
    submit_with_delivery(store, tenant, scope, request_id, None).await
}

async fn submit_with_delivery(
    store: &AgentdStore,
    tenant: &str,
    scope: &str,
    request_id: Option<&str>,
    delivery_destination: Option<&str>,
) -> Uuid {
    store
        .submit_run(NewRun {
            tenant,
            name: "turn",
            agent_ref: "bot",
            scope,
            source: "test",
            input: &json!({"text":"test"}),
            request_id,
            schedule_name: None,
            delivery_destination,
        })
        .await
        .unwrap()
}

#[tokio::test]
async fn tenant_scoped_request_ids_are_idempotent() {
    let (_dir, store) = store().await;
    tenant_with_agent(&store, "one").await;
    tenant_with_agent(&store, "two").await;

    let first = submit(&store, "one", "scope", Some("request-1")).await;
    let duplicate = submit(&store, "one", "other", Some("request-1")).await;
    let other_tenant = submit(&store, "two", "scope", Some("request-1")).await;

    assert_eq!(first, duplicate);
    assert_ne!(first, other_tenant);
    assert!(store
        .get_run(first)
        .await
        .unwrap()
        .is_some_and(|run| run.tenant == "one"));
}

#[tokio::test]
async fn tenant_deletion_rejects_inflight_run_memory_and_artifact_writes() {
    for operation in ["run", "memory", "artifact"] {
        let (_dir, store) = store().await;
        tenant_with_agent(&store, "demo").await;
        let hook = TenantWriteHook::default();
        *store.tenant_write_hook.lock().await = Some(hook.clone());
        let embedding = test_embedding(0);
        let writer = async {
            match operation {
                "run" => store
                    .submit_run(NewRun {
                        tenant: "demo",
                        name: "turn",
                        agent_ref: "bot",
                        scope: "scope",
                        source: "test",
                        input: &json!({"text":"test"}),
                        request_id: Some("inflight-request"),
                        schedule_name: None,
                        delivery_destination: None,
                    })
                    .await
                    .map(|_| ()),
                "memory" => store
                    .put_memory("demo", "profile", "fact", "a fact", &embedding)
                    .await
                    .map(|_| ()),
                "artifact" => {
                    store
                        .put_artifact("demo", "a.txt", b"body", "text/plain", None)
                        .await
                }
                _ => unreachable!(),
            }
        };
        let deleter = async {
            // The write has started but has not reserved the database writer.
            // Commit the real deletion in the former check-to-insert window.
            hook.before_transaction.notified().await;
            assert_eq!(store.delete_tenant("demo").await.unwrap()["deleted"], true);
            hook.resume_writer.notify_one();
        };
        let (result, ()) = tokio::join!(writer, deleter);
        assert!(
            matches!(
                result.unwrap_err().downcast_ref::<StoreError>(),
                Some(StoreError::NotFound(_))
            ),
            "{operation} must reject a tenant deleted before its transaction"
        );
        for table in [
            "agents",
            "runs",
            "memory",
            "entities",
            "edges",
            "artifacts",
            "memory_maintenance_state",
        ] {
            let count =
                db::query_scalar::<i64>(&format!("SELECT COUNT(*) FROM {table} WHERE tenant = ?"))
                    .bind("demo")
                    .fetch_optional(&store.pool)
                    .await
                    .unwrap()
                    .unwrap();
            assert_eq!(count, 0, "{operation} left orphan rows in {table}");
        }
    }
}

#[tokio::test]
async fn agent_deletion_rejects_inflight_run_submission() {
    let (_dir, store) = store().await;
    tenant_with_agent(&store, "demo").await;
    let hook = TenantWriteHook::default();
    *store.tenant_write_hook.lock().await = Some(hook.clone());
    let input = json!({"text":"test"});
    let writer = store.submit_run(NewRun {
        tenant: "demo",
        name: "turn",
        agent_ref: "bot",
        scope: "scope",
        source: "test",
        input: &input,
        request_id: None,
        schedule_name: None,
        delivery_destination: None,
    });
    let deleter = async {
        hook.before_transaction.notified().await;
        assert!(store.delete_agent("demo", "bot").await.unwrap());
        hook.resume_writer.notify_one();
    };
    let (result, ()) = tokio::join!(writer, deleter);
    assert!(matches!(
        result.unwrap_err().downcast_ref::<StoreError>(),
        Some(StoreError::NotFound(_))
    ));
    assert_eq!(
        db::query_scalar::<i64>("SELECT COUNT(*) FROM runs WHERE tenant = ?")
            .bind("demo")
            .fetch_optional(&store.pool)
            .await
            .unwrap()
            .unwrap(),
        0
    );
}

#[tokio::test]
async fn same_scope_serializes_while_other_scope_can_run() {
    let (_dir, store) = store().await;
    tenant_with_agent(&store, "demo").await;
    let first = submit(&store, "demo", "same", None).await;
    let second = submit(&store, "demo", "same", None).await;
    let parallel = submit(&store, "demo", "other", None).await;

    assert_eq!(
        store.claim_next_run().await.unwrap().unwrap().run.run_id,
        first
    );
    assert_eq!(
        store.claim_next_run().await.unwrap().unwrap().run.run_id,
        parallel
    );
    assert!(store.claim_next_run().await.unwrap().is_none());

    store
        .finalize_run_success(first, &json!({"done":true}), None)
        .await
        .unwrap();
    assert_eq!(
        store.claim_next_run().await.unwrap().unwrap().run.run_id,
        second
    );
}

#[tokio::test]
async fn queued_busy_scope_cannot_hide_runnable_work_beyond_candidate_window() {
    let (_dir, store) = store().await;
    tenant_with_agent(&store, "demo").await;
    tenant_with_agent(&store, "excluded").await;
    let active = submit(&store, "demo", "busy", None).await;
    assert_eq!(
        store.claim_next_run().await.unwrap().unwrap().run.run_id,
        active
    );
    for _ in 0..65 {
        submit(&store, "demo", "busy", None).await;
        submit(&store, "excluded", "free", None).await;
    }
    let runnable = submit(&store, "demo", "other", None).await;
    let claimed = store
        .claim_next_run_excluding_tenants(&["excluded".into()])
        .await
        .unwrap()
        .unwrap();
    assert_eq!(claimed.run.run_id, runnable);
}

#[tokio::test]
async fn artifact_pages_preserve_every_key_and_encoded_references() {
    let (_dir, store) = store().await;
    store.create_tenant("demo", &json!({})).await.unwrap();
    let keys = [
        "a.txt",
        "b.txt",
        "c#?.txt",
        "d space.txt",
        "literal%_\\file.txt",
        "literalZZX\\file.txt",
        "报告.txt",
    ];
    for key in keys {
        store
            .put_artifact("demo", key, b"content", "text/plain", None)
            .await
            .unwrap();
    }
    let mut cursor = None;
    let mut seen = Vec::new();
    loop {
        let page = store
            .list_artifacts_page("demo", None, cursor.as_deref(), 2)
            .await
            .unwrap();
        for item in &page.items {
            let reference = ArtifactRef::parse(&item.artifact_ref).unwrap();
            assert_eq!(
                reference.path_for_tenant("demo").unwrap().as_str(),
                item.path
            );
            seen.push(item.path.clone());
        }
        cursor = page.next_cursor;
        if cursor.is_none() {
            break;
        }
    }
    let mut expected = keys.map(str::to_string).to_vec();
    expected.sort();
    assert_eq!(seen, expected);
    let literal_prefix = store
        .list_artifacts_page("demo", Some("literal%_\\"), None, 10)
        .await
        .unwrap();
    assert_eq!(literal_prefix.items.len(), 1);
    assert_eq!(literal_prefix.items[0].path, "literal%_\\file.txt");
    assert!(store
        .get_artifact("demo", "/a.txt")
        .await
        .unwrap()
        .is_some());
    let error = store
        .put_artifact("demo", "a/../b", b"x", "text/plain", None)
        .await
        .unwrap_err();
    assert!(matches!(
        error.downcast_ref::<StoreError>(),
        Some(StoreError::Validation(_))
    ));
}

#[tokio::test]
async fn corrupted_persisted_json_and_identifiers_are_database_errors() {
    let (_dir, store) = store().await;
    tenant_with_agent(&store, "demo").await;
    db::query("UPDATE agents SET spec_json = '{broken' WHERE tenant = 'demo' AND name = 'bot'")
        .execute(&store.pool)
        .await
        .unwrap();
    let error = store.get_agent("demo", "bot").await.unwrap_err();
    assert!(matches!(
        error.downcast_ref::<StoreError>(),
        Some(StoreError::Database(_))
    ));
    db::query("UPDATE agents SET spec_json = ? WHERE tenant = 'demo' AND name = 'bot'")
        .bind(
            serde_json::to_string(&agentd_api::AgentSpec {
                allowed_families: Some(vec![]),
                limits: agentd_api::AgentLimits {
                    timeout_ms: 1000,
                    max_steps: 1,
                },
                system_prompt: None,
                model: None,
                temperature: None,
                max_tokens: None,
                context_window: Some(0),
            })
            .unwrap(),
        )
        .execute(&store.pool)
        .await
        .unwrap();
    let run = submit(&store, "demo", "test", None).await;
    db::query("UPDATE runs SET run_id = 'not-a-uuid' WHERE run_id = ?")
        .bind(run.to_string())
        .execute(&store.pool)
        .await
        .unwrap();
    let error = store
        .list_runs(&RunListQuery {
            tenant: Some("demo".into()),
            ..RunListQuery::default()
        })
        .await
        .unwrap_err();
    assert!(matches!(
        error.downcast_ref::<StoreError>(),
        Some(StoreError::Database(_))
    ));
}

#[tokio::test]
async fn missing_agent_fails_queued_run_without_stranding_its_lane() {
    let (_dir, store) = store().await;
    tenant_with_agent(&store, "demo").await;
    let broken = submit(&store, "demo", "same", None).await;
    assert!(store.delete_agent("demo", "bot").await.unwrap());

    assert!(store.claim_next_run().await.unwrap().is_none());
    let run = store.get_run(broken).await.unwrap().unwrap();
    assert_eq!(run.status, AgentRunStatus::Failed);
    assert!(run
        .error
        .as_deref()
        .is_some_and(|error| error.contains("agent bot not found")));

    tenant_with_agent(&store, "demo").await;
    let healthy = submit(&store, "demo", "same", None).await;
    assert_eq!(
        store.claim_next_run().await.unwrap().unwrap().run.run_id,
        healthy
    );
}

#[tokio::test]
async fn final_output_trace_and_delivery_commit_together() {
    let (_dir, store) = store().await;
    tenant_with_agent(&store, "demo").await;
    let run_id = submit_with_delivery(&store, "demo", "chat:42", None, Some("tg:42")).await;
    let output = json!({"reply":"hello"});
    store.claim_next_run().await.unwrap().unwrap();

    store
        .finalize_run_success(run_id, &output, None)
        .await
        .unwrap();
    assert!(store
        .finalize_run_success(run_id, &json!({"reply":"duplicate"}), None)
        .await
        .is_err());

    assert_eq!(store.get_run_output(run_id).await.unwrap(), Some(output));
    let trace = store.list_run_log(run_id).await.unwrap();
    assert_eq!(trace.len(), 2);
    assert_eq!(trace[0].kind, "output");
    let deliveries = store
        .list_delivery_outbox(Some("demo"), None, Some(run_id), 10)
        .await
        .unwrap();
    assert_eq!(deliveries.len(), 1);
    assert_eq!(deliveries[0].status, "pending");
    assert_eq!(deliveries[0].destination, "tg:42");
    assert_eq!(deliveries[0].payload, json!({"reply":"hello"}));
}

#[tokio::test]
async fn successful_run_without_delivery_stays_pull_only() {
    let (_dir, store) = store().await;
    tenant_with_agent(&store, "demo").await;
    let run_id = submit(&store, "demo", "browser:42", None).await;
    store.claim_next_run().await.unwrap().unwrap();
    store
        .finalize_run_success(run_id, &json!({"reply":"hello"}), None)
        .await
        .unwrap();

    assert_eq!(
        store.get_run_output(run_id).await.unwrap(),
        Some(json!({"reply":"hello"}))
    );
    assert!(store
        .list_delivery_outbox(Some("demo"), None, Some(run_id), 10)
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn failed_run_enqueues_one_immutable_failure_delivery() {
    let (_dir, store) = store().await;
    tenant_with_agent(&store, "demo").await;
    let run_id = submit_with_delivery(&store, "demo", "chat:42", None, Some("tg:42")).await;
    store.claim_next_run().await.unwrap().unwrap();

    store
        .fail_run(run_id, "run timeout exceeded")
        .await
        .unwrap();
    store.fail_run(run_id, "duplicate failure").await.unwrap();

    let run = store.get_run(run_id).await.unwrap().unwrap();
    assert_eq!(run.status, AgentRunStatus::Failed);
    assert_eq!(run.output, None);
    let deliveries = store
        .list_delivery_outbox(Some("demo"), None, Some(run_id), 10)
        .await
        .unwrap();
    assert_eq!(deliveries.len(), 1);
    assert_eq!(deliveries[0].status, "pending");
    assert_eq!(deliveries[0].destination, "tg:42");
    assert_eq!(deliveries[0].payload["error"]["code"], "run_timeout");
    assert!(deliveries[0].payload["reply"]
        .as_str()
        .is_some_and(|reply| reply.contains("too long")));
}

#[tokio::test]
async fn cancelled_run_cannot_commit_output_context_or_delivery() {
    let (_dir, store) = store().await;
    tenant_with_agent(&store, "demo").await;
    let run_id = submit_with_delivery(&store, "demo", "chat:42", None, Some("tg:42")).await;
    store.claim_next_run().await.unwrap().unwrap();
    store.cancel_run_request(run_id, "cancelled").await.unwrap();

    assert!(store
        .finalize_run_success(
            run_id,
            &json!({"reply":"too late"}),
            Some(&json!({"messages":[]})),
        )
        .await
        .is_err());
    assert_eq!(store.get_run_output(run_id).await.unwrap(), None);
    assert!(store
        .get_context_state("demo", "bot", "chat:42")
        .await
        .unwrap()
        .is_none());
    assert!(store
        .list_delivery_outbox(Some("demo"), None, Some(run_id), 10)
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn expired_claim_is_reissued_and_retry_updates_one_row() {
    let (_dir, store) = store().await;
    tenant_with_agent(&store, "demo").await;
    let run_id = submit_with_delivery(&store, "demo", "chat:42", None, Some("tg:42")).await;
    let now = Utc::now();
    store.claim_next_run().await.unwrap().unwrap();
    store
        .finalize_run_success(run_id, &json!({"reply":"hello"}), None)
        .await
        .unwrap();

    let first = store
        .claim_delivery_outbox("demo", 1, now, Duration::from_secs(1))
        .await
        .unwrap()
        .remove(0);
    let later = now + ChronoDuration::seconds(2);
    let second = store
        .claim_delivery_outbox("demo", 1, later, Duration::from_secs(10))
        .await
        .unwrap()
        .remove(0);
    assert_ne!(first.claim_token, second.claim_token);
    assert!(store
        .ack_delivery(
            "demo",
            DeliveryAck {
                delivery_id: second.delivery_id,
                claim_token: first.claim_token.as_deref().unwrap(),
                outcome: "delivered",
                error: None,
                retry_after: None,
                now: later,
            },
        )
        .await
        .is_err());

    let retried = store
        .ack_delivery(
            "demo",
            DeliveryAck {
                delivery_id: second.delivery_id,
                claim_token: second.claim_token.as_deref().unwrap(),
                outcome: "retry",
                error: Some("temporary"),
                retry_after: Some(Duration::from_secs(2)),
                now: later,
            },
        )
        .await
        .unwrap();
    assert_eq!(retried.status, "pending");
    assert_eq!(retried.attempt, 1);
    assert_eq!(
        store
            .list_delivery_outbox(Some("demo"), None, Some(run_id), 10)
            .await
            .unwrap()
            .len(),
        1
    );
}

#[tokio::test]
async fn mcp_catalog_is_tenant_scoped() {
    let (_dir, store) = store().await;
    tenant_with_agent(&store, "one").await;
    tenant_with_agent(&store, "two").await;
    let spec = McpServerSpec {
        enabled: true,
        transport: McpTransport::Http {
            url: "http://127.0.0.1/mcp".into(),
            headers_from: BTreeMap::new(),
        },
        allowed_tools: None,
    };
    store
        .apply_mcp_server(
            "one",
            "alpha",
            &spec,
            &[McpTool {
                name: "ping".into(),
                description: None,
                input_schema: json!({"type":"object"}),
            }],
            None,
        )
        .await
        .unwrap();
    store
        .apply_mcp_server(
            "two",
            "beta",
            &spec,
            &[McpTool {
                name: "ping".into(),
                description: None,
                input_schema: json!({"type":"object"}),
            }],
            None,
        )
        .await
        .unwrap();

    let tools = store
        .list_visible_tools("one", &[ToolFamily::Mcp])
        .await
        .unwrap();
    assert_eq!(tools.len(), 1);
    assert!(tools[0].name.starts_with("mcp_alpha_ping"));
    assert_ne!(
        mcp_exposed_name("a.b", "ping"),
        mcp_exposed_name("a/b", "ping")
    );
}

#[tokio::test]
async fn mcp_exposed_tool_names_must_be_unique_within_a_tenant() {
    let (_dir, store) = store().await;
    tenant_with_agent(&store, "demo").await;
    let spec = McpServerSpec {
        enabled: true,
        transport: McpTransport::Http {
            url: "http://127.0.0.1/mcp".into(),
            headers_from: BTreeMap::new(),
        },
        allowed_tools: None,
    };
    store
        .apply_mcp_server(
            "demo",
            "a_b",
            &spec,
            &[McpTool {
                name: "c".into(),
                description: None,
                input_schema: json!({"type":"object"}),
            }],
            None,
        )
        .await
        .unwrap();

    let error = store
        .apply_mcp_server(
            "demo",
            "a",
            &spec,
            &[McpTool {
                name: "b_c".into(),
                description: None,
                input_schema: json!({"type":"object"}),
            }],
            None,
        )
        .await
        .unwrap_err();
    assert!(error.to_string().contains("mcp_a_b_c"));
    assert!(store.get_mcp_server("demo", "a").await.unwrap().is_none());
}

#[tokio::test]
async fn memory_hybrid_search_is_tenant_and_namespace_scoped() {
    let (_dir, store) = store().await;
    tenant_with_agent(&store, "one").await;
    tenant_with_agent(&store, "two").await;
    let embedding = test_embedding(0);

    store
        .put_memory("one", "profile", "favorite", "likes mangosteen", &embedding)
        .await
        .unwrap();
    store
        .put_memory("one", "other", "favorite", "likes mangosteen", &embedding)
        .await
        .unwrap();
    store
        .put_memory("two", "profile", "favorite", "likes mangosteen", &embedding)
        .await
        .unwrap();

    let matches = store
        .search_memory("one", "profile", "mangosteen", &embedding, 10)
        .await
        .unwrap();
    assert_eq!(matches.len(), 1);
    assert_eq!(matches[0].tenant, "one");
    assert_eq!(matches[0].namespace, "profile");
    assert!(matches[0].score.is_some_and(|score| score > 0.0));
}

#[tokio::test]
async fn semantic_search_ranks_memories_beyond_the_first_read_batch() {
    let (_dir, store) = store().await;
    store.create_tenant("demo", &json!({})).await.unwrap();
    for index in 0..257 {
        store
            .put_memory(
                "demo",
                "profile",
                &format!("fact-{index:03}"),
                "unrelated fact",
                &test_embedding(1),
            )
            .await
            .unwrap();
    }
    store
        .put_memory(
            "demo",
            "profile",
            "zz-winner",
            "semantic winner",
            &test_embedding(0),
        )
        .await
        .unwrap();
    let hits = store
        .search_memory(
            "demo",
            "profile",
            "absentlexicaltoken",
            &test_embedding(0),
            1,
        )
        .await
        .unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].id, "zz-winner");
}

#[tokio::test]
async fn paged_memory_search_keeps_one_snapshot_during_committed_concurrent_writes() {
    let (_dir, store) = store().await;
    store.create_tenant("demo", &json!({})).await.unwrap();
    for index in 0..257 {
        store
            .put_memory(
                "demo",
                "profile",
                &format!("fact-{index:03}"),
                "unrelated fact",
                &test_embedding(1),
            )
            .await
            .unwrap();
    }
    store
        .put_memory(
            "demo",
            "profile",
            "zz-winner",
            "original winner",
            &test_embedding(0),
        )
        .await
        .unwrap();
    let hook = memory::MemorySearchHook::default();
    *store.memory_search_hook.lock().await = Some(hook.clone());
    let embedding = test_embedding(0);
    let reader = store.search_memory("demo", "profile", "absentlexicaltoken", &embedding, 1);
    let writer = async {
        // Synchronize a real write commit after the reader has fetched its
        // first batch, before it reads the winning row in the next batch.
        hook.snapshot_ready.notified().await;
        store
            .put_memory(
                "demo",
                "profile",
                "zz-winner",
                "changed after snapshot",
                &test_embedding(1),
            )
            .await
            .unwrap();
        store
            .put_memory(
                "demo",
                "profile",
                "zz-fresh",
                "fresh winner",
                &test_embedding(0),
            )
            .await
            .unwrap();
        hook.writer_finished.notify_one();
    };
    let (snapshot_result, ()) = tokio::join!(reader, writer);
    let snapshot_result = snapshot_result.unwrap();
    assert_eq!(snapshot_result[0].id, "zz-winner");
    assert_eq!(snapshot_result[0].text, "original winner");
    let current = store
        .search_memory("demo", "profile", "absentlexicaltoken", &embedding, 1)
        .await
        .unwrap();
    assert_eq!(current[0].id, "zz-fresh");
}

#[tokio::test]
async fn graph_query_walks_one_to_three_hops_and_tracks_memory_lifecycle() {
    let (_dir, store) = store().await;
    tenant_with_agent(&store, "one").await;
    tenant_with_agent(&store, "two").await;
    let embedding = test_embedding(0);
    store
        .put_memory_with_graph(
            "one",
            "profile",
            "ownership",
            "Alice owns agentd",
            &embedding,
            &graph(json!({
                "entities":[
                    {"id":"alice","label":"Alice","type":"person"},
                    {"id":"agentd","label":"agentd","type":"project"}
                ],
                "edges":[
                    {"from":"alice","relation":"owns","to":"agentd"},
                    {"from":"agentd","relation":"owned_by","to":"alice"}
                ]
            })),
        )
        .await
        .unwrap();
    store
        .put_memory_with_graph(
            "one",
            "profile",
            "storage",
            "agentd uses libSQL",
            &embedding,
            &graph(json!({
                "entities":[
                    {"id":"agentd","label":"agentd","type":"project"},
                    {"id":"libsql","label":"libSQL","type":"database"}
                ],
                "edges":[{"from":"agentd","relation":"uses","to":"libsql"}]
            })),
        )
        .await
        .unwrap();
    store
        .put_memory_with_graph(
            "two",
            "profile",
            "decoy",
            "Alice owns a secret",
            &embedding,
            &graph(json!({
                "entities":[
                    {"id":"alice","label":"Alice"},
                    {"id":"secret","label":"Secret"}
                ],
                "edges":[{"from":"alice","relation":"owns","to":"secret"}]
            })),
        )
        .await
        .unwrap();

    let outgoing = store
        .query_graph(
            "one",
            "profile",
            GraphQuery {
                entity: "Alice",
                relation: None,
                direction: "outgoing",
                max_hops: 3,
                limit: 100,
            },
        )
        .await
        .unwrap();
    assert_eq!(outgoing.paths.len(), 2);
    assert_eq!(outgoing.paths[0].nodes, ["alice", "agentd"]);
    assert_eq!(outgoing.paths[1].nodes, ["alice", "agentd", "libsql"]);
    assert!(!outgoing.entities.iter().any(|entity| entity.id == "secret"));
    assert_eq!(
        store
            .query_graph(
                "one",
                "profile",
                GraphQuery {
                    entity: "alice",
                    relation: None,
                    direction: "outgoing",
                    max_hops: 3,
                    limit: 1,
                },
            )
            .await
            .unwrap()
            .paths
            .len(),
        1
    );

    let one_hop = store
        .query_graph(
            "one",
            "profile",
            GraphQuery {
                entity: "alice",
                relation: None,
                direction: "outgoing",
                max_hops: 1,
                limit: 100,
            },
        )
        .await
        .unwrap();
    assert_eq!(one_hop.paths.len(), 1);
    let relation = store
        .query_graph(
            "one",
            "profile",
            GraphQuery {
                entity: "alice",
                relation: Some("owns"),
                direction: "outgoing",
                max_hops: 3,
                limit: 100,
            },
        )
        .await
        .unwrap();
    assert_eq!(relation.paths.len(), 1);
    let incoming = store
        .query_graph(
            "one",
            "profile",
            GraphQuery {
                entity: "libSQL",
                relation: None,
                direction: "incoming",
                max_hops: 3,
                limit: 100,
            },
        )
        .await
        .unwrap();
    assert_eq!(incoming.paths.len(), 2);
    assert_eq!(incoming.paths[1].nodes, ["libsql", "agentd", "alice"]);

    store
        .put_memory(
            "one",
            "profile",
            "storage",
            "agentd changed storage",
            &embedding,
        )
        .await
        .unwrap();
    let after_update = store
        .query_graph(
            "one",
            "profile",
            GraphQuery {
                entity: "alice",
                relation: None,
                direction: "outgoing",
                max_hops: 3,
                limit: 100,
            },
        )
        .await
        .unwrap();
    assert_eq!(after_update.paths.len(), 1);
    assert!(!after_update
        .entities
        .iter()
        .any(|entity| entity.id == "libsql"));

    assert!(store
        .delete_memory("one", "profile", "ownership")
        .await
        .unwrap());
    let after_delete = store
        .query_graph(
            "one",
            "profile",
            GraphQuery {
                entity: "alice",
                relation: None,
                direction: "outgoing",
                max_hops: 3,
                limit: 100,
            },
        )
        .await
        .unwrap();
    assert!(after_delete.entities.is_empty());
    assert!(after_delete.paths.is_empty());
}

#[tokio::test]
async fn invalid_graph_does_not_create_memory() {
    let (_dir, store) = store().await;
    tenant_with_agent(&store, "one").await;
    let error = store
        .put_memory_with_graph(
            "one",
            "profile",
            "invalid",
            "Alice owns an undeclared entity",
            &test_embedding(0),
            &graph(json!({
                "entities":[{"id":"alice","label":"Alice"}],
                "edges":[{"from":"alice","relation":"owns","to":"missing"}]
            })),
        )
        .await
        .unwrap_err();
    assert!(error.to_string().contains("must reference entities"));
    assert!(store
        .get_memory("one", "profile", "invalid")
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn memory_list_pages_are_stable_bounded_and_tenant_scoped() {
    let (_dir, store) = store().await;
    tenant_with_agent(&store, "one").await;
    tenant_with_agent(&store, "two").await;
    let embedding = test_embedding(0);
    for index in 0..105 {
        store
            .put_memory(
                "one",
                "profile",
                &format!("fact-{index:03}"),
                &format!("fact {index}"),
                &embedding,
            )
            .await
            .unwrap();
    }
    store
        .put_memory("one", "other", "secret", "other namespace", &embedding)
        .await
        .unwrap();
    store
        .put_memory("two", "profile", "secret", "other tenant", &embedding)
        .await
        .unwrap();

    let clamped = store
        .list_memory_page("one", "profile", None, usize::MAX)
        .await
        .unwrap();
    assert_eq!(clamped.items.len(), 100);
    assert_eq!(clamped.next_after_id.as_deref(), Some("fact-099"));
    assert_eq!(
        store
            .list_memory_page("one", "profile", None, 0)
            .await
            .unwrap()
            .items
            .len(),
        1
    );

    let mut after_id = None;
    let mut ids = Vec::new();
    loop {
        let page = store
            .list_memory_page("one", "profile", after_id.as_deref(), 17)
            .await
            .unwrap();
        ids.extend(page.items.into_iter().map(|item| item.id));
        after_id = page.next_after_id;
        if after_id.is_none() {
            break;
        }
    }
    assert_eq!(ids.len(), 105);
    assert_eq!(ids.first().map(String::as_str), Some("fact-000"));
    assert_eq!(ids.last().map(String::as_str), Some("fact-104"));
    assert!(ids.windows(2).all(|pair| pair[0] < pair[1]));
    assert!(!ids.iter().any(|id| id == "secret"));
}

#[tokio::test]
async fn maintenance_schedule_fans_out_to_populated_tenant_namespaces() {
    let (_dir, store) = store().await;
    tenant_with_agent(&store, "one").await;
    tenant_with_agent(&store, "two").await;
    let bot = store.get_agent("one", "bot").await.unwrap().unwrap();
    let mut metadata = bot.metadata;
    metadata.name = MEMORY_MAINTAINER_AGENT.into();
    let mut spec = bot.spec;
    spec.allowed_families = Some(vec![ToolFamily::Memory]);
    spec.context_window = Some(0);
    store
        .apply_agent(&AgentResource { metadata, spec })
        .await
        .unwrap();

    let now = Utc::now();
    store
        .put_schedule(
            "one",
            MEMORY_MAINTENANCE_SCHEDULE,
            &ScheduleSpec {
                agent_ref: MEMORY_MAINTAINER_AGENT.into(),
                scope: "memory-maintenance/default".into(),
                payload: json!({"namespace":ALL_MEMORY_NAMESPACES}),
                delivery: None,
                at: Some(now + ChronoDuration::minutes(1)),
                cron: None,
                timezone: None,
                enabled: true,
            },
        )
        .await
        .unwrap();
    let embedding = test_embedding(0);
    for namespace in ["bot", "default", "shared", MEMORY_MAINTAINER_AGENT] {
        for index in 0..5 {
            store
                .put_memory(
                    "one",
                    namespace,
                    &format!("fact-{index}"),
                    "tenant one fact",
                    &embedding,
                )
                .await
                .unwrap();
        }
    }
    store
        .put_memory("one", "sparse", "fact", "one fact", &embedding)
        .await
        .unwrap();
    store
        .put_memory("two", "other", "fact", "tenant two fact", &embedding)
        .await
        .unwrap();
    db::query("UPDATE schedules SET next_trigger_at = ? WHERE tenant = ? AND name = ?")
        .bind((now - ChronoDuration::seconds(1) + ChronoDuration::nanoseconds(0)).to_rfc3339())
        .bind("one")
        .bind(MEMORY_MAINTENANCE_SCHEDULE)
        .execute(&store.pool)
        .await
        .unwrap();

    let run_ids = store.trigger_due_schedules(now, 32).await.unwrap();
    assert_eq!(run_ids.len(), 3);
    let mut namespaces = Vec::new();
    for run_id in run_ids {
        let run = store.get_run(run_id).await.unwrap().unwrap();
        assert_eq!(run.tenant, "one");
        assert_eq!(run.agent_ref, MEMORY_MAINTAINER_AGENT);
        assert_eq!(run.input["activation"], "schedule");
        assert_eq!(run.input["namespace"], run.input["input"]["namespace"]);
        namespaces.push(
            run.input["input"]["namespace"]
                .as_str()
                .unwrap()
                .to_string(),
        );
    }
    namespaces.sort();
    assert_eq!(namespaces, vec!["bot", "default", "shared"]);
    // A second trigger cannot stack work for a namespace already queued.
    db::query("UPDATE schedules SET next_trigger_at = ? WHERE tenant = 'one'")
        .bind((now - ChronoDuration::seconds(1) + ChronoDuration::nanoseconds(1)).to_rfc3339())
        .execute(&store.pool)
        .await
        .unwrap();
    assert!(store
        .trigger_due_schedules(now, 32)
        .await
        .unwrap()
        .is_empty());
    while let Some(assigned) = store.claim_next_run().await.unwrap() {
        assert!(
            store
                .prepare_memory_maintenance(assigned.run.run_id, 5)
                .await
                .unwrap()
                .ready
        );
        store
            .finalize_run_success(assigned.run.run_id, &json!({"scanned":5}), None)
            .await
            .unwrap();
    }
    db::query("UPDATE schedules SET next_trigger_at = ? WHERE tenant = 'one'")
        .bind((now - ChronoDuration::seconds(1) + ChronoDuration::nanoseconds(2)).to_rfc3339())
        .execute(&store.pool)
        .await
        .unwrap();
    assert!(store
        .trigger_due_schedules(now, 32)
        .await
        .unwrap()
        .is_empty());
    let last = store
        .get_schedule("one", MEMORY_MAINTENANCE_SCHEDULE)
        .await
        .unwrap()
        .unwrap();
    assert!(last.last_run_id.is_some());
    store
        .put_memory("one", "bot", "new-fact", "new fact", &embedding)
        .await
        .unwrap();
    db::query("UPDATE schedules SET next_trigger_at = ? WHERE tenant = 'one'")
        .bind((now - ChronoDuration::seconds(1) + ChronoDuration::nanoseconds(3)).to_rfc3339())
        .execute(&store.pool)
        .await
        .unwrap();
    let next = store.trigger_due_schedules(now, 32).await.unwrap();
    assert_eq!(next.len(), 1);
    assert_eq!(
        store.get_run(next[0]).await.unwrap().unwrap().input["namespace"],
        "bot"
    );
}

#[tokio::test]
async fn memory_semantic_search_updates_vectors_and_preserves_created_at() {
    let (_dir, store) = store().await;
    tenant_with_agent(&store, "one").await;
    let first_embedding = test_embedding(1);
    let updated_embedding = test_embedding(0);

    let created = store
        .put_memory(
            "one",
            "profile",
            "favorite",
            "likes mangosteen",
            &first_embedding,
        )
        .await
        .unwrap();
    let updated = store
        .put_memory(
            "one",
            "profile",
            "favorite",
            "likes durian",
            &updated_embedding,
        )
        .await
        .unwrap();
    assert_eq!(updated.created_at, created.created_at);
    let matches = store
        .search_memory(
            "one",
            "profile",
            "a spiky tropical preference",
            &updated_embedding,
            1,
        )
        .await
        .unwrap();
    assert_eq!(matches[0].id, "favorite");
    assert_eq!(matches[0].text, "likes durian");
}

#[tokio::test]
async fn memory_rejects_wrong_dimensions_and_oversized_text() {
    let (_dir, store) = store().await;
    tenant_with_agent(&store, "one").await;
    let embedding = test_embedding(0);
    store
        .put_memory("one", "profile", "fact", "short", &embedding)
        .await
        .unwrap();
    let error = store
        .search_memory("one", "profile", "short", &[1.0], 5)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("exactly 384 dimensions"));
    assert!(store
        .put_memory("one", "profile", "long", &"界".repeat(1366), &embedding,)
        .await
        .is_err());
}

#[test]
fn memory_fts_query_is_unicode_safe_and_uses_or() {
    assert_eq!(
        memory_fts_query("red fruit"),
        Some("\"red\" OR \"fruit\"".into())
    );
    assert_eq!(memory_fts_query("喜欢榴莲"), Some("\"喜欢榴莲\"".into()));
    assert_eq!(memory_fts_query("?!"), None);
}

#[test]
fn rrf_rewards_candidates_found_by_both_searches_and_breaks_ties_by_id() {
    fn item(id: &str) -> MemoryItem {
        MemoryItem {
            tenant: "one".into(),
            namespace: "profile".into(),
            id: id.into(),
            text: id.into(),
            created_at: "now".into(),
            updated_at: "now".into(),
            score: None,
        }
    }
    let fused = fuse_memory_candidates(
        vec![item("lexical"), item("both")],
        vec![item("both"), item("semantic")],
        3,
    );
    assert_eq!(fused[0].id, "both");
    assert_eq!(fused[1].id, "lexical");
    assert_eq!(fused[2].id, "semantic");
}

#[tokio::test]
async fn memory_content_and_maintenance_metadata_use_separate_tables() {
    let (_dir, store) = store().await;
    let tables = db::query_scalar::<String>(
        "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'memory%' ORDER BY name",
    )
    .fetch_all(&store.pool)
    .await
    .unwrap();
    assert_eq!(
        tables,
        vec![
            "memory",
            "memory_fts",
            "memory_fts_config",
            "memory_fts_data",
            "memory_fts_docsize",
            "memory_fts_idx",
            "memory_maintenance_runs",
            "memory_maintenance_state"
        ]
    );
}

#[tokio::test]
async fn schema_version_six_migrates_graph_tables_without_resetting_memory() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("agentd.db");
    let store = AgentdStore::new(path.to_str().unwrap()).await.unwrap();
    tenant_with_agent(&store, "one").await;
    store
        .put_memory(
            "one",
            "profile",
            "favorite",
            "likes mangosteen",
            &test_embedding(0),
        )
        .await
        .unwrap();
    db::query("DROP TABLE edges")
        .execute(&store.pool)
        .await
        .unwrap();
    db::query("DROP TABLE entities")
        .execute(&store.pool)
        .await
        .unwrap();
    db::query("ALTER TABLE deliveries DROP COLUMN payload_json")
        .execute(&store.pool)
        .await
        .unwrap();
    db::query("PRAGMA user_version = 6")
        .execute(&store.pool)
        .await
        .unwrap();
    drop(store);

    let migrated = AgentdStore::new(path.to_str().unwrap()).await.unwrap();
    assert_eq!(
        db::query_scalar::<i64>("PRAGMA user_version")
            .fetch_optional(&migrated.pool)
            .await
            .unwrap(),
        Some(11)
    );
    assert!(migrated
        .get_memory("one", "profile", "favorite")
        .await
        .unwrap()
        .is_some());
    assert_eq!(
        db::query_scalar::<String>(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name IN ('entities', 'edges') ORDER BY name",
        )
        .fetch_all(&migrated.pool)
        .await
        .unwrap(),
        vec!["edges", "entities"]
    );
}

#[tokio::test]
async fn schema_version_seven_backfills_immutable_delivery_payloads() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("agentd.db");
    let store = AgentdStore::new(path.to_str().unwrap()).await.unwrap();
    tenant_with_agent(&store, "demo").await;
    let run_id = submit_with_delivery(&store, "demo", "chat:42", None, Some("tg:42")).await;
    store.claim_next_run().await.unwrap().unwrap();
    store
        .finalize_run_success(run_id, &json!({"reply":"preserved"}), None)
        .await
        .unwrap();
    db::query("ALTER TABLE deliveries DROP COLUMN payload_json")
        .execute(&store.pool)
        .await
        .unwrap();
    db::query("PRAGMA user_version = 7")
        .execute(&store.pool)
        .await
        .unwrap();
    drop(store);

    let migrated = AgentdStore::new(path.to_str().unwrap()).await.unwrap();
    let deliveries = migrated
        .list_delivery_outbox(Some("demo"), None, Some(run_id), 10)
        .await
        .unwrap();
    assert_eq!(deliveries.len(), 1);
    assert_eq!(deliveries[0].payload, json!({"reply":"preserved"}));
    assert_eq!(
        db::query_scalar::<i64>("PRAGMA user_version")
            .fetch_optional(&migrated.pool)
            .await
            .unwrap(),
        Some(11)
    );
}

#[tokio::test]
async fn schema_version_mismatch_requires_data_reset() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("agentd.db");
    let store = AgentdStore::new(path.to_str().unwrap()).await.unwrap();
    db::query("PRAGMA user_version = 99")
        .execute(&store.pool)
        .await
        .unwrap();
    drop(store);

    let error = AgentdStore::new(path.to_str().unwrap())
        .await
        .err()
        .expect("schema mismatch must fail");
    assert!(error.to_string().contains("--reset-data"));
}
