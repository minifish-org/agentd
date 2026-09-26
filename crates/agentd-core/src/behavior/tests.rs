use super::*;
use crate::{CapabilityEngine, CapabilityEngineConfig};
use agentd_api::{AgentLimits, AgentResource, ResourceMeta, ToolFamily, BEHAVIOR_LEARNER_AGENT};
use agentd_store::{AgentdStore, NewRun, MEMORY_EMBEDDING_DIM};
use axum::{extract::State, routing::post, Json, Router};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use tempfile::TempDir;
use tokio::{sync::Mutex, task::JoinHandle};

const LESSON: &str = "Check the requested resource and tool arguments before deciding.";
const PERSONA: &str = "Follow the user's requested edits and report only observed results.";

#[derive(Clone, Copy)]
enum JudgeMode {
    Improvement,
    OneRegression,
    InvalidScore,
}

struct MockState {
    mode: JudgeMode,
    requests: Mutex<Vec<Value>>,
    judge_calls: AtomicUsize,
}

fn assistant_response(message: Value) -> Value {
    json!({"choices":[{"finish_reason":"stop","message":message}]})
}

fn text_response(content: Value) -> Value {
    assistant_response(json!({"role":"assistant","content":content.to_string()}))
}

fn tool_decision(checked: bool) -> Value {
    json!({
        "role":"assistant", "content":null, "tool_calls":[
            {"id":"delete-memory","type":"function","function":{
                "name":"memory_delete",
                "arguments":json!({"namespace":"bot","id":"protected"}).to_string()
            }},
            {"id":"write-artifact","type":"function","function":{
                "name":"artifact_write",
                "arguments":json!({"path":"protected.txt","body_text":if checked {"verified update"} else {"draft update"}}).to_string()
            }}
        ]
    })
}

async fn completion(State(state): State<Arc<MockState>>, Json(body): Json<Value>) -> Json<Value> {
    state.requests.lock().await.push(body.clone());
    let response = match body["model"].as_str() {
        Some("proposer-fixture") => text_response(json!({"instructions":LESSON})),
        Some("judge-fixture") => {
            let input: Value =
                serde_json::from_str(body["messages"][1]["content"].as_str().unwrap()).unwrap();
            let candidate_a = input["a"]["message"].to_string().contains("verified");
            let pair = state.judge_calls.fetch_add(1, Ordering::SeqCst) / 2;
            let (baseline, candidate) = match state.mode {
                JudgeMode::OneRegression if pair == 0 => (0.6, 0.4),
                _ => (0.1, 0.9),
            };
            let (a, b) = if candidate_a {
                (candidate, baseline)
            } else {
                (baseline, candidate)
            };
            text_response(json!({
                "a":if matches!(state.mode, JudgeMode::InvalidScore) {2.0} else {a},
                "b":b,
                "evidence":"The decisions differ in whether they verify the requested update."
            }))
        }
        Some("target-fixture") => {
            let checked = body["messages"][0]["content"]
                .as_str()
                .unwrap()
                .contains(LESSON);
            let after_tool = body["messages"]
                .as_array()
                .unwrap()
                .iter()
                .any(|message| message["role"] == "tool");
            if after_tool {
                text_response(json!({"reply":if checked {"verified answer"} else {"draft answer"}}))
            } else {
                assistant_response(tool_decision(checked))
            }
        }
        _ => text_response(json!({"error":"unexpected model"})),
    };
    Json(response)
}

struct Fixture {
    _directory: TempDir,
    store: AgentdStore,
    runtime: RuntimeEngine,
    mock: Arc<MockState>,
    server: JoinHandle<()>,
    source_ids: Vec<Uuid>,
    contexts: Vec<Value>,
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.server.abort();
    }
}

async fn fixture(mode: JudgeMode) -> Fixture {
    fixture_sources(mode, 4, false).await
}

async fn fixture_sources(mode: JudgeMode, source_count: usize, same_scope: bool) -> Fixture {
    let directory = tempfile::tempdir().unwrap();
    let store = AgentdStore::new(directory.path().join("agentd.db").to_str().unwrap())
        .await
        .unwrap();
    store.create_tenant("demo", &json!({})).await.unwrap();
    for (name, families, context_window) in [
        (
            "bot",
            vec![ToolFamily::Memory, ToolFamily::Artifact],
            Some(2),
        ),
        (BEHAVIOR_LEARNER_AGENT, vec![], Some(0)),
    ] {
        store
            .apply_agent(&AgentResource {
                metadata: ResourceMeta {
                    tenant: "demo".into(),
                    name: name.into(),
                    labels: BTreeMap::new(),
                },
                spec: AgentSpec {
                    allowed_families: Some(families),
                    limits: AgentLimits {
                        timeout_ms: 20_000,
                        max_steps: 64,
                    },
                    system_prompt: Some(PERSONA.into()),
                    model: Some("target-fixture".into()),
                    temperature: Some(0.0),
                    max_tokens: Some(512),
                    context_window,
                },
            })
            .await
            .unwrap();
    }
    let mut embedding = vec![0.0; MEMORY_EMBEDDING_DIM];
    embedding[0] = 1.0;
    store
        .put_memory(
            "demo",
            "bot",
            "protected",
            "Original business fact",
            &embedding,
        )
        .await
        .unwrap();
    store
        .put_artifact(
            "demo",
            "protected.txt",
            b"Original document",
            "text/plain",
            None,
        )
        .await
        .unwrap();

    let visible = store
        .list_visible_tools("demo", &[ToolFamily::Memory, ToolFamily::Artifact])
        .await
        .unwrap();
    let tools = visible.iter().map(native_function_tool).collect::<Vec<_>>();
    let mut source_ids = Vec::new();
    let mut contexts = Vec::new();
    for index in 0..source_count {
        let scope = if same_scope {
            "conversation/shared".to_string()
        } else {
            format!("conversation/{index}")
        };
        let input = json!({"text":format!("source-marker-{index}: remove memory protected and update protected.txt.")});
        let run_id = submit(&store, "bot", &scope, &input).await;
        assert_eq!(
            store.claim_next_run().await.unwrap().unwrap().run.run_id,
            run_id
        );
        let mut messages = vec![
            json!({"role":"system","content":runtime_system_prompt(PERSONA,None)}),
            json!({"role":"user","content":input.to_string()}),
        ];
        let request = json!({"model":"target-fixture","messages":messages,"tools":tools,"parallel_tool_calls":false,"temperature":0.0,"max_tokens":512});
        let observed = tool_decision(false);
        append_model_pair(
            &store,
            run_id,
            1,
            request,
            assistant_response(observed.clone()),
        )
        .await;
        messages.push(observed);
        for call_id in ["delete-memory", "write-artifact"] {
            let result = json!({"ok":true,"result":{"historical":true},"error":null});
            store
                .append_event(
                    run_id,
                    "tool",
                    json!({"phase":"result","step":1,"call_id":call_id,"result":result}),
                    Utc::now(),
                )
                .await
                .unwrap();
            messages
                .push(json!({"role":"tool","tool_call_id":call_id,"content":result.to_string()}));
        }
        let request = json!({"model":"target-fixture","messages":messages,"tools":tools,"parallel_tool_calls":false,"temperature":0.0,"max_tokens":512});
        append_model_pair(
            &store,
            run_id,
            2,
            request,
            text_response(json!({"reply":"historical answer"})),
        )
        .await;
        let context = json!({"messages":[
            {"role":"user","content":format!("retained user {index}")},
            {"role":"assistant","content":format!("retained assistant {index}")}
        ]});
        store
            .finalize_run_success(
                run_id,
                &json!({"reply":"historical answer"}),
                Some(&context),
            )
            .await
            .unwrap();
        source_ids.push(run_id);
        contexts.push(
            serde_json::to_value(
                store
                    .get_context_state("demo", "bot", &scope)
                    .await
                    .unwrap()
                    .unwrap(),
            )
            .unwrap(),
        );
    }

    let mock = Arc::new(MockState {
        mode,
        requests: Mutex::new(Vec::new()),
        judge_calls: AtomicUsize::new(0),
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let router = Router::new()
        .route("/v1/chat/completions", post(completion))
        .with_state(mock.clone());
    let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    let caps = CapabilityEngine::new_with_config(
        store.clone(),
        CapabilityEngineConfig {
            llm_api_base: Some(format!("http://{address}/v1")),
            llm_api_key: None,
            llm_model: Some("target-fixture".into()),
            ..CapabilityEngineConfig::default()
        },
    );
    Fixture {
        _directory: directory,
        runtime: RuntimeEngine::new(caps, store.clone()),
        store,
        mock,
        server,
        source_ids,
        contexts,
    }
}

async fn submit(store: &AgentdStore, agent: &str, scope: &str, input: &Value) -> Uuid {
    store
        .submit_run(NewRun {
            tenant: "demo",
            name: "fixture",
            agent_ref: agent,
            scope,
            source: "api",
            input,
            request_id: None,
            schedule_name: None,
            delivery_destination: None,
        })
        .await
        .unwrap()
}

async fn append_model_pair(
    store: &AgentdStore,
    run_id: Uuid,
    step: u64,
    request: Value,
    response: Value,
) {
    for (phase, field, payload) in [
        ("request", "request", request),
        ("response", "response", response),
    ] {
        let mut event = json!({"phase":phase,"step":step});
        event[field] = payload;
        store
            .append_event(run_id, "model", event, Utc::now())
            .await
            .unwrap();
    }
}

async fn run_cycle(fixture: &Fixture) -> (Uuid, crate::ExecutionReport) {
    run_cycle_with_budget(fixture, 32).await
}

async fn run_cycle_with_budget(
    fixture: &Fixture,
    max_model_calls: u32,
) -> (Uuid, crate::ExecutionReport) {
    let options = json!({
        "target_agent":"bot", "proposer_model":"proposer-fixture", "judge_model":"judge-fixture",
        "min_samples":4, "max_samples":4, "max_model_calls":max_model_calls, "max_tokens":512, "min_improvement":0.1
    });
    let id = submit(&fixture.store, BEHAVIOR_LEARNER_AGENT, "learning", &options).await;
    let assigned = fixture.store.claim_next_run().await.unwrap().unwrap();
    assert_eq!(assigned.run.run_id, id);
    let report = fixture
        .runtime
        .execute_assigned_run(&assigned)
        .await
        .unwrap();
    (id, report)
}

#[tokio::test]
async fn behavior_learning_promotes_without_executing_tools_or_changing_foreground_state() {
    let fixture = fixture(JudgeMode::Improvement).await;
    let original_memory = fixture
        .store
        .get_memory("demo", "bot", "protected")
        .await
        .unwrap()
        .unwrap();
    let original_artifact = fixture
        .store
        .get_artifact("demo", "protected.txt")
        .await
        .unwrap()
        .unwrap();
    let (run_id, report) = run_cycle(&fixture).await;
    assert!(report.error.is_none(), "{:?}", report.error);
    let output = fixture.store.get_run_output(run_id).await.unwrap().unwrap();
    assert_eq!(output["status"], "promoted");
    assert_eq!(output["report"]["model_calls"], 17);
    assert_eq!(output["report"]["evaluations"].as_array().unwrap().len(), 4);
    assert_eq!(
        fixture
            .store
            .get_memory("demo", "bot", "protected")
            .await
            .unwrap()
            .unwrap(),
        original_memory
    );
    assert_eq!(
        fixture
            .store
            .get_artifact("demo", "protected.txt")
            .await
            .unwrap()
            .unwrap(),
        original_artifact
    );
    for (index, expected) in fixture.contexts.iter().enumerate() {
        let actual = fixture
            .store
            .get_context_state("demo", "bot", &format!("conversation/{index}"))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(serde_json::to_value(actual).unwrap(), *expected);
    }
    assert!(fixture
        .store
        .list_context_scopes("demo", BEHAVIOR_LEARNER_AGENT)
        .await
        .unwrap()
        .is_empty());
    assert!(fixture
        .store
        .list_delivery_outbox(Some("demo"), None, None, 20)
        .await
        .unwrap()
        .is_empty());

    let trace = fixture.store.list_run_log(run_id).await.unwrap();
    assert!(trace.iter().all(|event| event.kind != "tool"));
    let requests = fixture.mock.requests.lock().await;
    assert_eq!(requests.len(), 17);
    let proposer = requests
        .iter()
        .find(|request| request["model"] == "proposer-fixture")
        .unwrap();
    let proposal_input: Value =
        serde_json::from_str(proposer["messages"][1]["content"].as_str().unwrap()).unwrap();
    assert_eq!(proposer["messages"].as_array().unwrap().len(), 2);
    assert!(proposer.get("tools").is_none());
    assert_eq!(proposer["response_format"], json!({"type":"json_object"}));
    let development = output["report"]["development_runs"].as_array().unwrap();
    let holdout = output["report"]["holdout_runs"].as_array().unwrap();
    assert_eq!(development.len(), 2);
    assert_eq!(holdout.len(), 2);
    assert!(development.iter().all(|id| !holdout.contains(id)));
    let examples = proposal_input["development_examples"].as_array().unwrap();
    assert_eq!(examples.len(), development.len());
    for example in examples {
        assert!(development.contains(&example["source_run_id"]));
        assert_eq!(example["decisions"].as_array().unwrap().len(), 2);
    }
    for (index, source_id) in fixture.source_ids.iter().enumerate() {
        if holdout.contains(&json!(source_id)) {
            assert!(!proposal_input
                .to_string()
                .contains(&format!("source-marker-{index}")));
            assert!(!proposal_input.to_string().contains(&source_id.to_string()));
        }
    }
    let judges = requests
        .iter()
        .filter(|request| request["model"] == "judge-fixture")
        .collect::<Vec<_>>();
    assert_eq!(judges.len(), 8);
    for pair in judges.chunks_exact(2) {
        let mut inputs = Vec::new();
        for request in pair {
            assert!(request.get("tools").is_none());
            assert_eq!(request["response_format"], json!({"type":"json_object"}));
            assert_eq!(request["messages"].as_array().unwrap().len(), 2);
            let input: Value =
                serde_json::from_str(request["messages"][1]["content"].as_str().unwrap()).unwrap();
            assert_eq!(
                input
                    .as_object()
                    .unwrap()
                    .keys()
                    .cloned()
                    .collect::<BTreeSet<_>>(),
                BTreeSet::from(["a".into(), "b".into(), "rubric".into(), "state".into()])
            );
            assert!(!input["state"].to_string().contains(LESSON));
            assert!(input["a"].get("candidate").is_none());
            assert!(input["b"].get("candidate").is_none());
            inputs.push(input);
        }
        assert_eq!(inputs[0]["state"], inputs[1]["state"]);
        assert_eq!(inputs[0]["a"], inputs[1]["b"]);
        assert_eq!(inputs[0]["b"], inputs[1]["a"]);
    }
    drop(requests);
    for stage in ["propose", "baseline", "candidate", "judge"] {
        assert!(trace.iter().any(|event| event.kind == "model"
            && event.payload["stage"] == stage
            && event.payload["phase"] == "request"));
    }
    let snapshot = fixture
        .store
        .get_behavior_snapshot("demo", "bot")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(snapshot.agent.spec.system_prompt.as_deref(), Some(PERSONA));
    assert_eq!(snapshot.active_revision.unwrap().instructions, LESSON);
    submit(&fixture.store, "bot", "future", &json!({"text":"new task"})).await;
    let claimed = fixture.store.claim_next_run().await.unwrap().unwrap();
    assert_eq!(claimed.agent_learned_instructions.as_deref(), Some(LESSON));
    assert_eq!(claimed.agent_behavior_revision, Some(1));
    assert!(fixture
        .store
        .clear_behavior_policy("demo", "bot")
        .await
        .unwrap());
    assert_eq!(claimed.agent_learned_instructions.as_deref(), Some(LESSON));
    submit(
        &fixture.store,
        "bot",
        "future-other",
        &json!({"text":"another task"}),
    )
    .await;
    assert!(fixture
        .store
        .claim_next_run()
        .await
        .unwrap()
        .unwrap()
        .agent_learned_instructions
        .is_none());
}

#[tokio::test]
async fn behavior_learning_rejects_one_regression_despite_positive_average_gain() {
    let fixture = fixture(JudgeMode::OneRegression).await;
    let (run_id, report) = run_cycle(&fixture).await;
    assert!(report.error.is_none(), "{:?}", report.error);
    let output = fixture.store.get_run_output(run_id).await.unwrap().unwrap();
    assert_eq!(output["status"], "rejected");
    assert!(output["report"]["mean_conservative_gain"].as_f64().unwrap() > 0.1);
    assert!(output["report"]["evaluations"]
        .as_array()
        .unwrap()
        .iter()
        .any(|evaluation| evaluation["conservative_gain"].as_f64().unwrap() < 0.0));
    assert!(fixture
        .store
        .get_behavior_snapshot("demo", "bot")
        .await
        .unwrap()
        .unwrap()
        .active_revision
        .is_none());
    let history = fixture
        .store
        .list_behavior_revisions("demo", "bot", 20)
        .await
        .unwrap();
    assert_eq!(history.len(), 1);
    assert_eq!(history[0].outcome, "rejected");
    assert_eq!(fixture.mock.judge_calls.load(Ordering::SeqCst), 8);
}

#[tokio::test]
async fn behavior_learning_invalid_judge_cannot_activate_or_consume_sources() {
    let fixture = fixture(JudgeMode::InvalidScore).await;
    let (run_id, report) = run_cycle(&fixture).await;
    let error = report
        .error
        .expect("out-of-range judge score must fail the cycle");
    assert!(error.contains("score"), "{error}");
    fixture.store.fail_run(run_id, &error).await.unwrap();
    assert_eq!(
        fixture.store.get_run(run_id).await.unwrap().unwrap().status,
        AgentRunStatus::Failed
    );
    assert!(fixture
        .store
        .get_behavior_snapshot("demo", "bot")
        .await
        .unwrap()
        .unwrap()
        .active_revision
        .is_none());
    assert!(fixture
        .store
        .list_behavior_revisions("demo", "bot", 20)
        .await
        .unwrap()
        .is_empty());
    assert_eq!(
        fixture
            .store
            .list_behavior_source_runs("demo", "bot", 20)
            .await
            .unwrap()
            .len(),
        4
    );
    assert_eq!(fixture.mock.judge_calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn behavior_learning_insufficient_budget_skips_without_model_calls_or_consuming_sources() {
    let fixture = fixture(JudgeMode::Improvement).await;
    let (run_id, report) = run_cycle_with_budget(&fixture, 8).await;
    assert!(report.error.is_none(), "{:?}", report.error);
    let output = fixture.store.get_run_output(run_id).await.unwrap().unwrap();
    assert_eq!(output["status"], "skipped");
    assert_eq!(output["reason"], "insufficient_call_budget");
    assert_eq!(output["details"]["required_calls"], 17);
    assert_eq!(output["details"]["call_limit"], 8);
    assert!(fixture.mock.requests.lock().await.is_empty());
    assert!(fixture
        .store
        .get_behavior_snapshot("demo", "bot")
        .await
        .unwrap()
        .unwrap()
        .active_revision
        .is_none());
    assert!(fixture
        .store
        .list_behavior_revisions("demo", "bot", 20)
        .await
        .unwrap()
        .is_empty());
    assert_eq!(
        fixture
            .store
            .list_behavior_source_runs("demo", "bot", 20)
            .await
            .unwrap()
            .len(),
        4
    );
}

async fn assert_source_gate_skips_without_calls(fixture: &Fixture, reason: &str) -> Value {
    let (run_id, report) = run_cycle(fixture).await;
    assert!(report.error.is_none(), "{:?}", report.error);
    let output = fixture.store.get_run_output(run_id).await.unwrap().unwrap();
    assert_eq!(output["status"], "skipped");
    assert_eq!(output["reason"], reason);
    assert!(fixture.mock.requests.lock().await.is_empty());
    assert!(fixture
        .store
        .get_behavior_snapshot("demo", "bot")
        .await
        .unwrap()
        .unwrap()
        .active_revision
        .is_none());
    assert!(fixture
        .store
        .list_behavior_revisions("demo", "bot", 20)
        .await
        .unwrap()
        .is_empty());
    assert_eq!(
        fixture
            .store
            .list_behavior_source_runs("demo", "bot", 20)
            .await
            .unwrap()
            .len(),
        fixture.source_ids.len()
    );
    output
}

#[tokio::test]
async fn behavior_learning_insufficient_samples_skips_without_model_calls() {
    let fixture = fixture_sources(JudgeMode::Improvement, 3, false).await;
    let output = assert_source_gate_skips_without_calls(&fixture, "insufficient_samples").await;
    assert_eq!(output["details"]["usable_runs"], 3);
    assert_eq!(output["details"]["required_runs"], 4);
}

#[tokio::test]
async fn behavior_learning_insufficient_independent_scopes_skips_without_model_calls() {
    let fixture = fixture_sources(JudgeMode::Improvement, 4, true).await;
    let output =
        assert_source_gate_skips_without_calls(&fixture, "insufficient_independent_scopes").await;
    assert_eq!(output["details"]["usable_runs"], 4);
    assert_eq!(output["details"]["usable_scopes"], 1);
    assert_eq!(output["details"]["required_scopes"], 2);
}
