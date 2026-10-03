# Reliability evidence

agentd is experimental software, not a formally verified system. This matrix
maps important runtime behavior to executable evidence in the repository so a
reader can distinguish tested properties from design intent.

## Tested behavior

| Behavior | Executable evidence | Scope |
| --- | --- | --- |
| The advertised `calc_eval` expression executes through schema validation and dispatch, while malformed and non-finite expressions fail | `calc_eval_executes_the_advertised_expression_contract` | Core integration test |
| A tenant-scoped `request_id` deduplicates turn submission | `tenant_scoped_request_ids_are_idempotent` | Store integration test |
| Writes already in flight reject deleted tenant/agent references without leaving orphan rows | `tenant_deletion_rejects_inflight_run_memory_and_artifact_writes`; `agent_deletion_rejects_inflight_run_submission` | Store concurrency tests |
| Runs sharing `(tenant, agent, scope)` serialize while unrelated scopes can proceed | `same_scope_serializes_while_other_scope_can_run` | Store integration test |
| Busy scopes cannot hide unrelated queued work beyond the claim candidate window | `queued_busy_scope_cannot_hide_runnable_work_beyond_candidate_window` | Store regression test |
| A missing agent fails a queued run without leaving its scope lane occupied | `missing_agent_fails_queued_run_without_stranding_its_lane` | Store integration test |
| Output, terminal trace, context, and optional delivery reference finalize in one transaction | `final_output_trace_and_delivery_commit_together` | Store integration test |
| A cancelled run cannot commit successful output, context, or delivery | `cancelled_run_cannot_commit_output_context_or_delivery` | Store integration test |
| A successful run without a destination remains pull-only | `successful_run_without_delivery_stays_pull_only` | Store integration test |
| Expired delivery claims can be reissued; retry updates the same row and validates the claim token | `expired_claim_is_reissued_and_retry_updates_one_row` | Store integration test |
| The native model/tool loop commits output, context, trace, and delivery through the same path | `native_loop_commits_output_context_trace_and_delivery` | Core integration test with a deterministic provider |
| Three identical consecutive tool failures warn once per streak; success or changed tool/arguments/error resets detection, and successful polling does not warn | `loop_guard_warns_once_per_streak_and_ignores_json_key_order`; `loop_guard_resets_on_success_or_changed_tool_arguments_or_error` | Core unit tests |
| Loop reminders follow complete tool-result batches, are traced, allow a final response, and do not leak between runs | `loop_guard_reminder_follows_complete_tool_batch_and_allows_recovery` | Core integration test with a deterministic provider |
| A cancelled assignment is not executed after dispatch registration | `cancelled_assignment_is_not_executed_after_dispatch_registration` | Server concurrency test |
| Panics and timeouts persist a terminal run state and release scope/capacity; cancellation and shutdown wait for execution cleanup | `panicked_run_becomes_failed_and_unblocks_its_scope`; `timeout_failure_is_persisted_and_releases_capacity`; `cancel_and_shutdown_wait_for_execution_and_persist_terminal_states` | Server supervisor tests |
| Detached tenant deletion finishes after client disconnection, releases its admission gate, and preserves the request audit identity | `disconnected_delete_request_finishes_teardown_and_releases_tenant_gate`; `detached_cancel_and_delete_keep_request_audit_identity` | Server concurrency and audit tests |
| Shutdown joins detached operations; failed cleanup preserves tenant data, and unpersisted failures clear only after a confirmed terminal write | `shutdown_joins_an_operation_after_its_client_disconnects`; `failed_run_cleanup_preserves_tenant_and_allows_a_later_delete`; `pending_failure_is_cleared_only_after_the_terminal_write_succeeds` | Server lifecycle and fault injection tests |
| Listener or database ownership failure leaves the existing database untouched, including reset requests and path aliases | `busy_listener_does_not_reset_or_initialize_existing_database`; `duplicate_database_owner_on_another_port_cannot_reset_database`; `resetting_through_an_alias_preserves_database_identity_and_ownership` | Server startup and lock tests |
| Two databases cannot share an active sandbox directory; aliases have the same ownership identity | `shared_sandbox_owner_rejects_another_database_before_mutating_it`; `sandbox_directory_aliases_have_one_runtime_owner` | Server startup and lock tests |
| Failed sandbox destruction retains a closed session for tenant-scoped retry without affecting another tenant | `failed_destroy_retains_the_closed_session_for_tenant_retry` | Core test with a failing sandbox backend |
| Tenant REST paths cover turn, wait, trace, cancellation, artifact access, and removed-route rejection | `tenant_rest_turn_trace_cancel_artifact_and_removed_routes` | In-process HTTP integration test |
| MCP catalogs are tenant-scoped and exposed tool names are unique within a tenant | `mcp_catalog_is_tenant_scoped`; `mcp_exposed_tool_names_must_be_unique_within_a_tenant` | Store integration tests |
| MCP transport configuration is tagged and secrets remain environment-variable references | `mcp_transport_is_strict_and_secret_indirect` | API validation test |
| MCP discovery respects the all-tools or explicit allowlist boundary | `discovery_exposes_all_or_an_allowlist`; `discovery_rejects_unknown_allowed_tools` | Server tests |
| HTTP MCP calls share initialization without serializing network requests; expired sessions recover once and tenant invalidation stays isolated | `http_calls_share_initialization_without_serializing_network_requests`; `expired_http_session_is_reinitialized_once`; `tenant_invalidation_preserves_other_tenants` | Core tests with a local MCP fixture |
| Tool validation checks nested schemas and local references without external resolution, and applies builtin parsing rules before execution or offline evaluation | `builtin_contract_rejects_invalid_decisions_before_execution`; `validates_nested_values_and_local_refs_without_external_resolution` | Core validation tests |
| Foreground and learning exchanges trace provider success, failure and deadline consistently | `exchanges_trace_success_provider_failure_and_deadline_for_both_callers` | Core test with a local model fixture |
| Artifact pages preserve all keys and references round-trip Unicode, URI delimiters and stored dot segments | `artifact_pages_preserve_every_key_and_encoded_references`; `references_round_trip_database_keys_without_url_normalization` | Store and API regression tests |
| Artifact tool limits above the storage page size return the requested number of items | `artifact_tool_limit_crosses_the_store_page_boundary` | Core integration test |
| Public web fetch rejects private addresses and returns the socket addresses that were checked | `public_url_resolution_rejects_private_addresses`; `public_url_resolution_returns_the_checked_socket_addresses` | Core network-boundary tests |
| Memory rejects invalid vectors and remains tenant/namespace scoped across hybrid search | `memory_rejects_wrong_dimensions_and_oversized_text`; `memory_hybrid_search_is_tenant_and_namespace_scoped` | Store integration tests |
| Semantic ranking includes memories beyond the first bounded read batch | `semantic_search_ranks_memories_beyond_the_first_read_batch` | Store regression test |
| All search batches retain one snapshot while a concurrent writer commits; later searches see the update | `paged_memory_search_keeps_one_snapshot_during_committed_concurrent_writes` | Store concurrency test |
| Memory enumeration is stable, bounded, tenant/namespace scoped, cursor-bound, and traced through the native loop | `memory_list_pages_are_stable_bounded_and_tenant_scoped`; `memory_list_cursor_is_bound_to_tenant_and_namespace`; `memory_changes_only_through_traced_model_tool_calls` | Store and core integration tests |
| Graph writes are atomic with memory, tenant/namespace scoped, bounded to three hops, cycle-safe, and cleaned up on memory replacement/deletion | `graph_query_walks_one_to_three_hops_and_tracks_memory_lifecycle`; `invalid_graph_does_not_create_memory`; `memory_put_graph_is_queryable_through_the_builtin_tool` | Store and core integration tests |
| Native database connections can be torn down concurrently while statements and transactions retain their own handles | `parallel_connection_teardown_preserves_live_statements_and_transactions` | Store regression test; pinned libsql ownership patch |
| Memory maintenance is provisioned automatically with an enabled schedule, preserves explicit disabling, and scopes eligible work to tenant namespaces | `memory_maintenance_is_automatic_and_preserves_explicit_disable`; `maintenance_schedule_fans_out_to_populated_tenant_namespaces` | In-process HTTP and store integration tests |
| Behavior learning is provisioned automatically with an enabled schedule, zero tools/context, option validation, preserved compatible settings, and reserved-name conflict checks | `behavior_learning_preset_is_automatic_tenant_scoped_and_enabled`; `behavior_learning_preset_validates_options_and_preserves_custom_settings`; `behavior_learning_preset_rejects_incompatible_reserved_resources` | In-process HTTP integration tests |
| Preset repairs retain disabled/custom settings and roll back both resources on audit failure | `atomic_preset_upgrade_preserves_disabled_schedule_and_custom_agent_fields`; `preset_schedule_audit_failure_rolls_back_both_resources_with_database_error` | Store transaction tests |
| Memory readiness requires enough entries and external content changes; the maintainer's own writes do not retrigger it, concurrent external writes stay pending, and failure/cancellation consumes nothing | `readiness_requires_enough_entries_and_is_tenant_scoped`; `successful_maintenance_does_not_retrigger_from_its_own_writes`; `external_change_during_maintenance_remains_pending_after_success`; `failed_and_cancelled_maintenance_do_not_consume_external_changes` | Store integration tests |
| Sparse or unchanged memory skips before any model/tool call; stale maintenance writes cannot overwrite concurrent foreground changes | `memory_maintenance_small_and_unchanged_namespaces_skip_without_model_calls`; `stale_maintenance_put_and_delete_cannot_overwrite_external_changes` | Core and store integration tests |
| A failed maintenance write stops the model loop and preserves pending changes; cancellation cannot be replaced with failure or success | `failed_maintenance_mutations_cannot_consume_checkpoint_or_override_cancellation` | Core integration test with mock provider |
| Behavior policy inspection and idempotent reset reject unknown tenants or agents and preserve revision history after clearing a promoted policy | `behavior_learning_state_is_tenant_scoped_and_clear_is_idempotent` | In-process HTTP integration test |
| Learning promotion is tenant-scoped, pins instructions at claim, preserves memory, and cannot commit from a cancelled or failed coordinator | `promotion_is_tenant_scoped_pinned_and_separate_from_memory`; `cancelled_or_failed_coordinator_cannot_activate_or_consume` | Store integration tests |
| Changed agent specifications or parent policies make proposals stale; background targets exclude system agents and serialize per target | `changed_spec_and_parent_record_stale_without_consuming`; `behavior_schedule_fanout_excludes_system_and_serializes_targets` | Store integration tests |
| Recreated agents cannot inherit old learning sources or in-flight proposals; invalid learning schedule options are rejected before scheduling | `recreated_agent_cannot_inherit_sources_or_inflight_policy`; `behavior_schedule_validates_options_and_normalizes_wildcard_defaults` | Store integration tests |
| Development and holdout sets keep all turns from each conversation scope together and require independent scopes | `interleaved_conversation_turns_stay_together_in_balanced_partitions`; `repeated_turns_in_one_scope_cannot_form_a_holdout_set` | Core unit tests |
| Offline trials never execute tools or modify foreground context, memory, or artifacts; independent judges receive anonymous decisions in both orders | `behavior_learning_promotes_without_executing_tools_or_changing_foreground_state` | Core integration test with a deterministic model server |
| One regressing decision blocks promotion despite positive mean gain; invalid judge scores cannot activate a policy or consume sources | `behavior_learning_rejects_one_regression_despite_positive_average_gain`; `behavior_learning_invalid_judge_cannot_activate_or_consume_sources` | Core integration tests with a deterministic model server |
| A learning cycle whose complete evaluation cannot fit its call budget skips before any model call or source consumption | `behavior_learning_insufficient_budget_skips_without_model_calls_or_consuming_sources` | Core integration test |
| Behavior scheduling requires new samples and independent scopes, excludes consumed/previous-lifecycle sources, and suppresses only the same tenant/target's pending work | `behavior_readiness_requires_new_samples_and_independent_scopes`; `behavior_readiness_excludes_consumed_and_previous_agent_lifecycle_sources`; `behavior_readiness_only_blocks_pending_cycles_for_the_same_tenant_and_target` | Store integration tests |
| Ineligible learning ticks advance without queueing; new independent history makes later ticks eligible and pending cycles are not duplicated | `behavior_schedule_validates_options_and_normalizes_wildcard_defaults`; `behavior_schedule_fanout_excludes_system_and_serializes_targets` | Store integration tests |
| Queued/manual learning cycles with insufficient samples or independent scopes call no model and consume no sources | `behavior_learning_insufficient_samples_skips_without_model_calls`; `behavior_learning_insufficient_independent_scopes_skips_without_model_calls` | Core integration tests |
| A maintainer cannot report success, cross namespaces, skip a returned cursor, or mutate memory before completing enumeration | `maintainer_scan_requires_bound_complete_pagination_before_mutation`; `maintainer_cannot_finish_without_calling_memory_list` | Core unit and native-loop integration tests |
| Supported schemas v6–v10 migrate to v11 without resetting runtime data; existing namespaces receive an initial external revision and unknown versions require explicit data reset | `schema_version_six_migrates_graph_tables_without_resetting_memory`; `schema_eight_migrates_without_resetting_memory_or_delivery`; `schema_nine_migration_marks_existing_namespaces_ready`; `schema_version_mismatch_requires_data_reset` | Store integration tests |
| Audit history is append-only, survives tenant deletion, supports stable filtered pages, and isolates nested/concurrent actor context | `audit_events_reject_update_and_delete`; `deleted_tenant_history_remains_queryable`; `pagination_is_stable_and_tenant_filters_are_exact`; `task_context_isolated_between_requests_and_restored_after_nested_scope` | Store audit tests |
| HTTP rejection, invalid input and reads are audited without copying credentials or trusting caller identity; audit failure prevents a handler or reports incomplete response auditing | `rejected_auth_is_audited_without_credential_or_actor_spoofing`; `extractor_rejection_fallback_and_method_errors_are_audited`; `audit_failure_prevents_handler_or_reports_completion_failure` | Server audit tests |
| Due scheduler skips and partial fan-outs retain decisions, counts and committed run IDs without creating runs for ineligible work | `scheduler_audits_no_targets_only_for_due_schedules`; `scheduler_audits_memory_sparse_queued_pending_and_unchanged_without_content`; `scheduler_partial_failure_keeps_committed_runs_and_safe_audit_summary` | Store scheduler audit tests |
| Retrying an occurrence after summary failure reuses committed runs, including pending background targets and targets removed from discovery | `schedule_summary_retry_reuses_occurrence_and_pending_background_targets`; `occurrence_retry_preserves_committed_targets_removed_from_discovery` | Store scheduler fault injection tests |
| Corrupted persisted JSON and identifiers are internal database errors, rather than caller input failures | `corrupted_persisted_json_and_identifiers_are_database_errors` | Store error-classification regression test |
| Maintenance preparation and policy publication roll back if their audit cannot commit | `maintenance_prepare_rolls_back_when_its_audit_cannot_commit`; `behavior_publication_rolls_back_when_its_audit_cannot_commit` | Store fault injection tests |
| Resource writes and tenant deletion roll back on audit failure; secrets remain absent; v10 migration preserves existing data | `public_resource_audits_keep_request_identity_and_omit_content`; `rejected_resource_audits_roll_back_resource_and_related_state`; `tenant_delete_is_atomic_and_preserves_counted_audit_history`; `version_ten_upgrade_retains_resources_and_records_migration_once` | Store resource audit tests |
| Run/trace/finalization and delivery claims/acks roll back on audit failure; lease changes cannot report false acknowledgement success | `audit_failure_rolls_back_run_submission_claim_trace_and_finalization`; `audit_delivery_claim_and_ack_roll_back_on_audit_failure`; `audit_delivery_ack_requires_current_lease_and_one_changed_row` | Store run audit tests |
| Console audit filters, deleted tenants, stable pages, stale-request isolation and literal text rendering work without polling | `scripts/test-console-audit.mjs` | Node DOM fixture tests |
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
node --test scripts/test-console-audit.mjs
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
