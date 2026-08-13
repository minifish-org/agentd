# Changelog

All notable changes to agentd are recorded here. The project follows Semantic
Versioning for release labels, but pre-1.0 database and HTTP compatibility are
not preserved unless a release note explicitly says otherwise.

## Unreleased

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
- A real OpenAI-compatible provider is required for an end-to-end turn.
- The real embedding smoke test is not part of the default CI job.

[Unreleased]: https://github.com/minifish-org/agentd/commits/main
