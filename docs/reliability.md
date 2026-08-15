# Reliability evidence

agentd is experimental software, not a formally verified system. This matrix
maps important runtime behavior to executable evidence in the repository so a
reader can distinguish tested properties from design intent.

## Tested behavior

| Behavior | Executable evidence | Scope |
| --- | --- | --- |
| A tenant-scoped `request_id` deduplicates turn submission | `tenant_scoped_request_ids_are_idempotent` | Store integration test |
| Runs sharing `(tenant, agent, scope)` serialize while unrelated scopes can proceed | `same_scope_serializes_while_other_scope_can_run` | Store integration test |
| A missing agent fails a queued run without leaving its scope lane occupied | `missing_agent_fails_queued_run_without_stranding_its_lane` | Store integration test |
| Output, terminal trace, context, and optional delivery reference finalize in one transaction | `final_output_trace_and_delivery_commit_together` | Store integration test |
| A cancelled run cannot commit successful output, context, or delivery | `cancelled_run_cannot_commit_output_context_or_delivery` | Store integration test |
| A successful run without a destination remains pull-only | `successful_run_without_delivery_stays_pull_only` | Store integration test |
| Expired delivery claims can be reissued; retry updates the same row and validates the claim token | `expired_claim_is_reissued_and_retry_updates_one_row` | Store integration test |
| The native model/tool loop commits output, context, trace, and delivery through the same path | `native_loop_commits_output_context_trace_and_delivery` | Core integration test with a deterministic provider |
| A cancelled assignment is not executed after dispatch registration | `cancelled_assignment_is_not_executed_after_dispatch_registration` | Server concurrency test |
| Tenant REST paths cover turn, wait, trace, cancellation, artifact access, and removed-route rejection | `tenant_rest_turn_trace_cancel_artifact_and_removed_routes` | In-process HTTP integration test |
| MCP catalogs are tenant-scoped and exposed tool names are unique within a tenant | `mcp_catalog_is_tenant_scoped`; `mcp_exposed_tool_names_must_be_unique_within_a_tenant` | Store integration tests |
| MCP transport configuration is tagged and secrets remain environment-variable references | `mcp_transport_is_strict_and_secret_indirect` | API validation test |
| MCP discovery respects the all-tools or explicit allowlist boundary | `discovery_exposes_all_or_an_allowlist`; `discovery_rejects_unknown_allowed_tools` | Server tests |
| Public web fetch rejects private addresses and returns the socket addresses that were checked | `public_url_resolution_rejects_private_addresses`; `public_url_resolution_returns_the_checked_socket_addresses` | Core network-boundary tests |
| Memory rejects invalid vectors and remains tenant/namespace scoped across hybrid search | `memory_rejects_wrong_dimensions_and_oversized_text`; `memory_hybrid_search_is_tenant_and_namespace_scoped` | Store integration tests |
| Schema mismatch requires explicit data reset | `schema_version_mismatch_requires_data_reset` | Store integration test |
| A non-loopback listener requires a non-empty API token | `non_loopback_listener_requires_non_empty_api_token`; `non_loopback_listener_accepts_api_token`; `loopback_listener_allows_missing_api_token` | Configuration tests |
| The pinned E5 model produces normalized 384-dimension vectors | `pinned_model_generates_normalized_384_dimension_vectors` | Real-model smoke test; ignored by default |
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
loads the embedding model and starts local processes:

```sh
./scripts/demo-e2e.sh
```

See the [demo boundary](demo.md) for what this fixture does and does not prove.

The real embedding smoke test requires the pinned assets:

```sh
model_dir="${AGENTD_EMBEDDING_MODEL_DIR:-$HOME/.cache/agentd/models/multilingual-e5-small}"
./scripts/fetch-embedding-model.sh "$model_dir"
AGENTD_EMBEDDING_MODEL_DIR="$model_dir" \
  cargo test -p agentd-core \
  pinned_model_generates_normalized_384_dimension_vectors -- --ignored
```

Dependency advisory, dependency-license/source, and current-tree secret checks
run in the `Security` GitHub Actions workflow. The manual and tag-triggered
`Release check` workflow runs the real embedding smoke test and builds and
inspects the complete container image in addition to the default checks.

## Evidence not yet present

The repository does not currently claim evidence for:

- process-kill fault injection at every transaction boundary;
- multi-process or multi-host coordination, failover, or replication;
- compatibility across schema or HTTP API versions;
- load, soak, latency, memory, storage-quota, or denial-of-service limits;
- deterministic replay or rollback of external tool side effects;
- sandboxing of operator-configured stdio MCP processes;
- end-to-end behavior against every OpenAI-compatible provider, MCP server, or
  transport adapter;
- model safety, factual correctness, or prompt-injection prevention.

These are limitations, not implied roadmap commitments. See the
[threat model](threat-model.md) for the security boundary.
