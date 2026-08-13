# Deterministic local demo

The demo runs a complete agentd turn without an LLM account or API key. It
starts a loopback-only OpenAI-compatible test provider, starts agentd with a
temporary database, creates a tenant and agent, submits a turn, waits for the
result, inspects its trace, verifies pull-only delivery behavior, and exercises
artifact storage.

Requirements are Rust, Python 3, `curl`, and `jq`. The first run also downloads
about 448 MiB of checksum-verified embedding assets and compiles the workspace.

```sh
./scripts/demo-e2e.sh
```

The script reuses valid embedding assets from
`$AGENTD_EMBEDDING_MODEL_DIR` or
`$HOME/.cache/agentd/models/multilingual-e5-small`. Runtime state and logs live
in a temporary directory; log excerpts are printed on failure and the directory
is removed on exit. Set
`AGENTD_DEMO_PROVIDER_PORT` or `AGENTD_DEMO_PORT` only when fixed ports are
needed; otherwise the script selects loopback ports automatically.

## What this proves

- the documented toolchain can start agentd from a fresh runtime database;
- agentd can call an OpenAI-compatible chat-completions endpoint;
- a queued turn reaches a terminal success with canonical JSON output;
- the output remains pullable and the raw trace records the model/output path;
- a turn without an explicit destination creates no delivery row;
- tenant-scoped artifact write, read, and delete work through REST.

## What this does not prove

The provider is a deterministic protocol fixture, not a language model. It
does not evaluate model quality, tool selection, tool-call interoperability,
prompt injection, provider-specific extensions, streaming, latency, or
production deployment. Use it to reproduce the agentd lifecycle, then point a
separate configuration at a real provider for model behavior.

The fixture intentionally binds only to loopback and returns one fixed JSON
reply. It must not be exposed as a service or used as a provider fallback.
