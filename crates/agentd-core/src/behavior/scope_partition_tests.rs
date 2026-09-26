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
