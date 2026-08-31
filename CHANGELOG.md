# Changelog

All notable changes to agentd are recorded here. The project follows Semantic
Versioning for release labels, but pre-1.0 database and HTTP compatibility are
not preserved unless a release note explicitly says otherwise.

## Unreleased

### Added

- Stable, bounded `memory_list` pagination with tenant/namespace-bound cursors.
- An explicit per-tenant memory-maintenance preset whose schedule is disabled
  by default and whose agent can access only the memory tool family.
- Failure deliveries for explicitly addressed runs, including a stable
  machine-readable timeout/failure code and a retry-safe user-facing reply.

### Changed

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
  schema versions 6 and 7 migrate in place to schema version 8.
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
