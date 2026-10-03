use super::*;

fn group(scope: &str, id: u128) -> SourceGroup {
    SourceGroup {
        run: AgentRun {
            run_id: Uuid::from_u128(id),
            tenant: "tenant".into(),
            name: "source".into(),
            agent_ref: "target".into(),
            scope: scope.into(),
            source: "test".into(),
            input: json!({}),
            output: None,
            error: None,
            status: AgentRunStatus::Succeeded,
            request_id: None,
            created_at: Utc::now(),
            started_at: None,
            updated_at: Utc::now(),
        },
        cases: Vec::new(),
    }
}

#[test]
fn repeated_turns_in_one_scope_cannot_form_a_holdout_set() {
    assert!(split_source_groups(&[]).is_none());
    let groups = (1..=8).map(|id| group("same-chat", id)).collect::<Vec<_>>();
    assert!(split_source_groups(&groups).is_none());
}

#[test]
fn interleaved_conversation_turns_stay_together_in_balanced_partitions() {
    let groups = ["a", "b", "a", "c", "b", "d", "a"]
        .into_iter()
        .enumerate()
        .map(|(index, scope)| group(scope, index as u128 + 1))
        .collect::<Vec<_>>();
    let (development, holdout) = split_source_groups(&groups).unwrap();
    let development_scopes = development
        .iter()
        .map(|group| group.run.scope.clone())
        .collect::<BTreeSet<_>>();
    let holdout_scopes = holdout
        .iter()
        .map(|group| group.run.scope.clone())
        .collect::<BTreeSet<_>>();
    assert!(development_scopes.is_disjoint(&holdout_scopes));
    assert_eq!(development.len() + holdout.len(), groups.len());
    assert!(development.len().abs_diff(holdout.len()) <= 1);
    let mut reversed = groups;
    reversed.reverse();
    let (again, _) = split_source_groups(&reversed).unwrap();
    assert_eq!(
        development_scopes,
        again
            .iter()
            .map(|group| group.run.scope.clone())
            .collect::<BTreeSet<_>>()
    );
}

#[test]
fn malformed_trial_envelopes_are_not_valid_final_answers() {
    let request = json!({"tools":[{"type":"function","function":{"name":"calc","parameters":{"type":"object"}}}]});
    for message in [
        json!({"role":"user","content":"answer"}),
        json!({"role":"assistant","content":"answer","tool_calls":{}}),
        json!({"role":"assistant","tool_calls":[{"id":"one","type":"other","function":{"name":"calc","arguments":"{}"}}]}),
        json!({"role":"assistant","tool_calls":[{"id":"one","type":"function","function":{"name":"calc","arguments":"[]"}}]}),
    ] {
        let response = json!({"choices":[{"message":message}]});
        assert!(!decision(&response, &request)
            .unwrap()
            .validation_errors
            .is_empty());
    }
    for message in [
        json!({"role":"assistant","content":"answer"}),
        json!({"role":"assistant","content":"answer","tool_calls":null}),
        json!({"role":"assistant","tool_calls":[{"id":"one","type":"function","function":{"name":"calc","arguments":"{}"}}]}),
    ] {
        let response = json!({"choices":[{"message":message}]});
        assert!(decision(&response, &request)
            .unwrap()
            .validation_errors
            .is_empty());
    }
}

#[test]
fn builtin_parser_failures_are_invalid_offline_decisions() {
    let catalog = agentd_api::builtin_tool_catalog();
    for (name, arguments) in [
        ("sandbox_session", json!({"action":"shell"})),
        ("memory_search", json!({"query":"x","limit":"five"})),
        ("memory_search", json!({"query":" "})),
        ("memory_put", json!({"id":"x","text":" "})),
        ("graph_query", json!({"entity":" "})),
        (
            "graph_query",
            json!({"entity":"x","direction":"sideways","max_hops":999}),
        ),
        ("artifact_write", json!({"path":"report.txt"})),
        ("artifact_read", json!({"artifact_ref":"bogus"})),
        ("memory_list", json!({"cursor":"bogus"})),
        ("web_fetch", json!({"url":"file:///tmp/test"})),
        ("calc_eval", json!({"expression":"1/0"})),
    ] {
        let tool = catalog.iter().find(|tool| tool.name == name).unwrap();
        let request = json!({"tools":[native_function_tool(tool)]});
        let response = json!({"choices":[{"message":{"role":"assistant","tool_calls":[{
            "type":"function","id":"test","function":{"name":name,"arguments":arguments.to_string()}
        }]}}]});
        assert!(
            !decision(&response, &request)
                .unwrap()
                .validation_errors
                .is_empty(),
            "{name}: {arguments}"
        );
    }
}
