# Changelog

All notable changes to agentd are recorded here. The project follows Semantic
Versioning for release labels, but pre-1.0 database and HTTP compatibility are
not preserved unless a release note explicitly says otherwise.

## Unreleased

### Fixed

- Native libsql connections now close their handle only once, through a
  pinned local patch; concurrent connection teardown no longer repeats the
  close on a potentially reused pointer.

- `calc_eval` now reads the advertised `expression` argument, so valid native
  tool calls execute without retrying contradictory `expr`/`expression` errors.
  Its result payload keeps the existing `expr` and `result` fields.

### Added

- Durable append-only audit history for API access/rejection, resource changes,
  run/trace/delivery lifecycle, background scheduling decisions and policy or
  memory-maintenance state. Request/run correlation and global/tenant paginated
  query APIs include history of deleted tenants without duplicating payloads.
- Per-tenant behavior learning with an enabled weekly schedule,
  isolated offline next-decision comparisons, independent AI judging, and
  automatic promotion of learned instruction supplements after held-out
  non-regression and improvement checks. Trials never execute tools.
- Tenant-scoped behavior policy inspection and reset endpoints, immutable
  revision history, and promotion checks against concurrent agent/policy edits.
- Stable, bounded `memory_list` pagination with tenant/namespace-bound cursors.
- Per-tenant memory maintenance whose agent can access only the memory tool
  family, with an enabled weekly schedule and an entry/change threshold.
- Failure deliveries for explicitly addressed runs, including a stable
  machine-readable timeout/failure code and a retry-safe user-facing reply.

### Changed

- New tenants automatically receive both background agents and schedules;
  startup fills missing resources for existing tenants. Compatible settings,
  including an explicit `enabled=false`, are preserved.
- Memory maintenance requires at least five entries by default and an
  unconsumed external text insertion, update, or deletion. Its own changes do
  not retrigger maintenance; successful runs consume their starting revision
  while preserving concurrent external changes. A failed maintenance write
  stops the run without consuming its checkpoint.
- Background schedules check eligibility before queueing. Behavior learning
  requires eight new source runs across two scopes by default, and both tasks
  suppress duplicate pending work for the same target. Ineligible scheduled
  work creates no run or model call; runtime gates remain for queued/manual runs.
- Memory retrieval now uses INT8 multilingual E5 Small embeddings, fuses BM25
  and semantic candidates with RRF to top 10, then applies an INT8 BGE
  reranker-v2-m3 cross-encoder and returns at most the top 5. The database
  remains one 384-dimension embedding per memory row.
- Memory-maintenance agents now use `standard/chat`; the native loop requires
  them to complete tenant-bound `memory_list` pagination before reporting
  success or modifying memory.
- Native model requests now enable JSON object mode, and final output
  normalization repairs malformed multiline or serialized delivery objects
  before they can enter rolling context or reach a transport.
- Delivery rows now capture an immutable payload at terminal commit time;
  schema versions 6–10 migrate in place to schema version 11, which also adds
  behavior-learning state, memory-maintenance checkpoints and audit history.
- The example `simple-bot` keeps ten complete context turns and allows 180
  seconds for tool-heavy runs.

## [0.1.0-alpha.1] - 2026-08-15

### Added

- Sanitized public source history under Apache-2.0.
- A four-crate Rust runtime with tenant-scoped persistence, per-scope
  serialization, native model/tool execution, scheduling, raw traces, and a
  pull delivery outbox.
- Fifteen built-in capabilities plus tenant-scoped stdio and HTTP MCP discovery.
- Hybrid lexical/semantic memory using a pinned multilingual E5 Small ONNX
  model.
- Native and Docker startup paths with checksum-verified model assets.
- A deterministic, loopback-only demo provider and end-to-end turn script that
  require no LLM credentials.
- A macOS Bash 3.2-compatible API end-to-end harness.
- CI, dependency advisory/license/source policy, current-tree secret scanning,
  security policy, threat model, and reliability evidence matrix.

### Known limitations

- Runtime data is disposable; schema changes require `--reset-data`.
- The HTTP API is experimental and may change without a migration path.
- TLS, rate limiting, quotas, HA, and MCP process sandboxing are deployment
  responsibilities.
- Real model behavior and tool-call interoperability require a real
  OpenAI-compatible provider; the credential-free demo is a protocol fixture.
- The real embedding smoke test is not part of the default CI job.

[Unreleased]: https://github.com/minifish-org/agentd/compare/v0.1.0-alpha.1...HEAD
[0.1.0-alpha.1]: https://github.com/minifish-org/agentd/releases/tag/v0.1.0-alpha.1
