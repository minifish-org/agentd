# Reliability evidence

agentd is experimental software, not a formally verified system. This matrix
maps important runtime behavior to executable evidence in the repository so a
reader can distinguish tested properties from design intent.

## Tested behavior

| Behavior | Executable evidence | Scope |
| --- | --- | --- |
| The advertised `calc_eval` expression executes through schema validation and dispatch, while malformed and non-finite expressions fail | `calc_eval_executes_the_advertised_expression_contract` | Core integration test |
| A tenant-scoped `request_id` deduplicates turn submission | `tenant_scoped_request_ids_are_idempotent` | Store integration test |
| Runs sharing `(tenant, agent, scope)` serialize while unrelated scopes can proceed | `same_scope_serializes_while_other_scope_can_run` | Store integration test |
| A missing agent fails a queued run without leaving its scope lane occupied | `missing_agent_fails_queued_run_without_stranding_its_lane` | Store integration test |
| Output, terminal trace, context, and optional delivery reference finalize in one transaction | `final_output_trace_and_delivery_commit_together` | Store integration test |
| A cancelled run cannot commit successful output, context, or delivery | `cancelled_run_cannot_commit_output_context_or_delivery` | Store integration test |
| A successful run without a destination remains pull-only | `successful_run_without_delivery_stays_pull_only` | Store integration test |
| Expired delivery claims can be reissued; retry updates the same row and validates the claim token | `expired_claim_is_reissued_and_retry_updates_one_row` | Store integration test |
| The native model/tool loop commits output, context, trace, and delivery through the same path | `native_loop_commits_output_context_trace_and_delivery` | Core integration test with a deterministic provider |
| Three identical consecutive tool failures warn once per streak; success or changed tool/arguments/error resets detection, and successful polling does not warn | `loop_guard_warns_once_per_streak_and_ignores_json_key_order`; `loop_guard_resets_on_success_or_changed_tool_arguments_or_error` | Core unit tests |
| Loop reminders follow complete tool-result batches, are traced, allow a final response, and do not leak between runs | `loop_guard_reminder_follows_complete_tool_batch_and_allows_recovery` | Core integration test with a deterministic provider |
| A cancelled assignment is not executed after dispatch registration | `cancelled_assignment_is_not_executed_after_dispatch_registration` | Server concurrency test |
| Tenant REST paths cover turn, wait, trace, cancellation, artifact access, and removed-route rejection | `tenant_rest_turn_trace_cancel_artifact_and_removed_routes` | In-process HTTP integration test |
| MCP catalogs are tenant-scoped and exposed tool names are unique within a tenant | `mcp_catalog_is_tenant_scoped`; `mcp_exposed_tool_names_must_be_unique_within_a_tenant` | Store integration tests |
| MCP transport configuration is tagged and secrets remain environment-variable references | `mcp_transport_is_strict_and_secret_indirect` | API validation test |
| MCP discovery respects the all-tools or explicit allowlist boundary | `discovery_exposes_all_or_an_allowlist`; `discovery_rejects_unknown_allowed_tools` | Server tests |
| Public web fetch rejects private addresses and returns the socket addresses that were checked | `public_url_resolution_rejects_private_addresses`; `public_url_resolution_returns_the_checked_socket_addresses` | Core network-boundary tests |
| Memory rejects invalid vectors and remains tenant/namespace scoped across hybrid search | `memory_rejects_wrong_dimensions_and_oversized_text`; `memory_hybrid_search_is_tenant_and_namespace_scoped` | Store integration tests |
| Memory enumeration is stable, bounded, tenant/namespace scoped, cursor-bound, and traced through the native loop | `memory_list_pages_are_stable_bounded_and_tenant_scoped`; `memory_list_cursor_is_bound_to_tenant_and_namespace`; `memory_changes_only_through_traced_model_tool_calls` | Store and core integration tests |
| Graph writes are atomic with memory, tenant/namespace scoped, bounded to three hops, cycle-safe, and cleaned up on memory replacement/deletion | `graph_query_walks_one_to_three_hops_and_tracks_memory_lifecycle`; `invalid_graph_does_not_create_memory`; `memory_put_graph_is_queryable_through_the_builtin_tool` | Store and core integration tests |
| The memory-maintenance preset pins a memory-only agent to `standard/chat`, installs a disabled schedule, and fans out enabled work across populated tenant namespaces without including the maintainer's own memory | `memory_maintenance_preset_is_tenant_scoped_restricted_and_disabled`; `maintenance_schedule_fans_out_to_populated_tenant_namespaces` | In-process HTTP and store integration tests |
| The behavior-learning preset is tenant-scoped, starts disabled with no tools or context, validates options, preserves compatible custom settings, and rejects reserved-name conflicts | `behavior_learning_preset_is_tenant_scoped_restricted_and_disabled`; `behavior_learning_preset_validates_options_and_preserves_custom_settings`; `behavior_learning_preset_rejects_incompatible_reserved_resources` | In-process HTTP integration tests |
| Behavior policy inspection and idempotent reset reject unknown tenants or agents and preserve revision history after clearing a promoted policy | `behavior_learning_state_is_tenant_scoped_and_clear_is_idempotent` | In-process HTTP integration test |
| Learning promotion is tenant-scoped, pins instructions at claim, preserves memory, and cannot commit from a cancelled or failed coordinator | `promotion_is_tenant_scoped_pinned_and_separate_from_memory`; `cancelled_or_failed_coordinator_cannot_activate_or_consume` | Store integration tests |
| Changed agent specifications or parent policies make proposals stale; background targets exclude system agents and serialize per target | `changed_spec_and_parent_record_stale_without_consuming`; `behavior_schedule_fanout_excludes_system_and_serializes_targets` | Store integration tests |
| Recreated agents cannot inherit old learning sources or in-flight proposals; invalid learning schedule options are rejected before scheduling | `recreated_agent_cannot_inherit_sources_or_inflight_policy`; `behavior_schedule_validates_options_and_normalizes_wildcard_defaults` | Store integration tests |
| Development and holdout sets keep all turns from each conversation scope together and require independent scopes | `interleaved_conversation_turns_stay_together_in_balanced_partitions`; `repeated_turns_in_one_scope_cannot_form_a_holdout_set` | Core unit tests |
| Offline trials never execute tools or modify foreground context, memory, or artifacts; independent judges receive anonymous decisions in both orders | `behavior_learning_promotes_without_executing_tools_or_changing_foreground_state` | Core integration test with a deterministic model server |
| One regressing decision blocks promotion despite positive mean gain; invalid judge scores cannot activate a policy or consume sources | `behavior_learning_rejects_one_regression_despite_positive_average_gain`; `behavior_learning_invalid_judge_cannot_activate_or_consume_sources` | Core integration tests with a deterministic model server |
| A learning cycle whose complete evaluation cannot fit its call budget skips before any model call or source consumption | `behavior_learning_insufficient_budget_skips_without_model_calls_or_consuming_sources` | Core integration test |
| A maintainer cannot report success, cross namespaces, skip a returned cursor, or mutate memory before completing enumeration | `maintainer_scan_requires_bound_complete_pagination_before_mutation`; `maintainer_cannot_finish_without_calling_memory_list` | Core unit and native-loop integration tests |
| Supported older schemas migrate to v9 without resetting runtime data; unknown versions require explicit data reset | `schema_version_six_migrates_graph_tables_without_resetting_memory`; `schema_eight_migrates_without_resetting_memory_or_delivery`; `schema_version_mismatch_requires_data_reset` | Store integration tests |
| A non-loopback listener requires a non-empty API token | `non_loopback_listener_requires_non_empty_api_token`; `non_loopback_listener_accepts_api_token`; `loopback_listener_allows_missing_api_token` | Configuration tests |
| The pinned E5 model produces normalized 384-dimension vectors | `pinned_model_generates_normalized_384_dimension_vectors` | Real-model smoke test; ignored by default |
| RRF candidates are capped at 10, BGE reranking changes their order, and output is capped at 5 | `memory_search_reranks_only_the_rrf_top_ten` | Core integration test with deterministic embedding and reranker scores |
| The pinned BGE v2-m3 model scores a relevant multilingual passage above an unrelated one | `pinned_model_reranks_multilingual_documents` | Real-model smoke test; ignored by default |
| The deterministic demo provider returns a final response accepted by the native loop contract | `scripts/test-demo-provider.sh` | Loopback protocol smoke test |

Test names are stable documentation targets only while the behavior remains in
scope. A change that intentionally alters one of these properties must update
the implementation, test, this matrix, and relevant architecture/API text in
the same pull request.

## Reproduce the evidence

The default suite is offline after dependencies have been fetched:

```sh
cargo fmt --all -- --check
cargo check --workspace --all-targets
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
bash scripts/test_deployment.sh
```

The deterministic end-to-end demo exercises a real agentd process without an
LLM credential. It is intentionally separate from the default suite because it
loads both retrieval models and starts local processes:

```sh
./scripts/demo-e2e.sh
```

See the [demo boundary](demo.md) for what this fixture does and does not prove.

The real retrieval smoke tests require the pinned assets:

```sh
model_dir="${AGENTD_EMBEDDING_MODEL_DIR:-$HOME/.cache/agentd/models/multilingual-e5-small}"
reranker_dir="${AGENTD_RERANKER_MODEL_DIR:-$HOME/.cache/agentd/models/bge-reranker-v2-m3}"
./scripts/fetch-embedding-model.sh "$model_dir"
./scripts/fetch-reranker-model.sh "$reranker_dir"
AGENTD_EMBEDDING_MODEL_DIR="$model_dir" \
AGENTD_RERANKER_MODEL_DIR="$reranker_dir" \
  cargo test -p agentd-core \
  pinned_model_generates_normalized_384_dimension_vectors -- --ignored
AGENTD_EMBEDDING_MODEL_DIR="$model_dir" \
AGENTD_RERANKER_MODEL_DIR="$reranker_dir" \
  cargo test -p agentd-core \
  pinned_model_reranks_multilingual_documents -- --ignored
```

Dependency advisory, dependency-license/source, and current-tree secret checks
run in the `Security` GitHub Actions workflow. The manual and tag-triggered
`Release check` workflow runs both real retrieval smoke tests and builds and
inspects the complete container image in addition to the default checks.

## Evidence not yet present

The repository does not currently claim evidence for:

- process-kill fault injection at every transaction boundary;
- multi-process or multi-host coordination, failover, or replication;
- compatibility across schema or HTTP API versions;
- load, soak, latency, memory, storage-quota, or denial-of-service limits;
- deterministic replay or rollback of external tool side effects;
- live model-quality or end-to-end task-success improvements from behavior
  learning; its isolated comparisons measure only the next decision against an
  AI judge, and deterministic tests establish protocol behavior rather than
  judge accuracy;
- sandboxing of operator-configured stdio MCP processes;
- end-to-end behavior against every OpenAI-compatible provider, MCP server, or
  transport adapter;
- model safety, factual correctness, or prompt-injection prevention.

These are limitations, not implied roadmap commitments. See the
[threat model](threat-model.md) for the security boundary.
