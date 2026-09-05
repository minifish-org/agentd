# agentd

agentd is an experimental, multi-tenant, single-agent runtime for one host. The
model decides; agentd supplies a native model/tool loop, capability boundaries,
persistence, scheduling, per-scope serialization, raw traces, and one pull
delivery outbox.

Database and HTTP compatibility are intentionally narrow. Runtime data remains
disposable, but schema v6 is migrated in place to v7 so existing memory can gain
the lightweight graph tables. Other schema mismatches require `--reset-data`.

## Runtime shape

The workspace has four crates:

- `agentd-api`: domain and wire types
- `agentd-store`: the single libSQL database
- `agentd-core`: model loop, LLM transport, built-in tools, and MCP execution
- `agentd-server`: REST, scheduling, dispatch, and process lifecycle

Transport adapters live in independent repositories. The
[Telegram adapter](https://github.com/minifish-org/agentd-telegram-adapter)
uses only tenant REST endpoints and the delivery outbox.

Every run follows one path:

```text
claim run → read context → native model/tool loop
          → transaction(output + status + context + optional delivery)
```

The database has tenants, agents, runs, run log, contexts, artifacts, memory,
lightweight entities and edges, schedules, deliveries, and MCP servers. Memory
keeps one FTS5 index and one 384-dimension embedding BLOB per fact. Exact cosine
and lexical ranks are combined with RRF; its top 10 are reranked to a final top
5. Explicit relationships use ordinary SQL joins and bounded recursive CTEs in
the same libSQL database. Replay is exactly the stored `run_log`; there is no
derived replay, audit, inspection, simulation, or export control plane.

## Start

### Demo without LLM credentials

With Rust, Python 3, `curl`, and `jq` installed, run the complete deterministic
turn lifecycle against a loopback-only OpenAI-compatible fixture:

```sh
./scripts/demo-e2e.sh
```

The first run downloads about 690 MiB of checksum-verified retrieval assets.
The fixture proves the runtime/API path, not model quality or real tool-call
compatibility. See the [demo boundary](docs/demo.md) for details.

### Native

Native startup requires the pinned E5 and BGE reranker assets. The fetch scripts
download only fixed revisions, verify every checksum, and install the models'
licenses. They support both GNU `sha256sum` and the `shasum` included with macOS.

```sh
agentd_model_dir="${AGENTD_EMBEDDING_MODEL_DIR:-$HOME/.cache/agentd/models/multilingual-e5-small}"
agentd_reranker_dir="${AGENTD_RERANKER_MODEL_DIR:-$HOME/.cache/agentd/models/bge-reranker-v2-m3}"
./scripts/fetch-embedding-model.sh "$agentd_model_dir"
./scripts/fetch-reranker-model.sh "$agentd_reranker_dir"
export AGENTD_EMBEDDING_MODEL_DIR="$agentd_model_dir"
export AGENTD_RERANKER_MODEL_DIR="$agentd_reranker_dir"

cp configs/agentd.toml ~/.agentd.toml
# Edit ~/.agentd.toml to point at an OpenAI-compatible chat-completions API.
cargo run -p agentd -- --config ~/.agentd.toml --reset-data
```

### Docker

The image includes the same checksum-verified model and its license. This local
example publishes only to host loopback and uses the development bearer token
from `configs/agentd.docker.toml`:

```sh
docker build -t agentd:dev .
docker run --rm --name agentd-dev \
  -p 127.0.0.1:8080:8080 \
  -v "$PWD/configs/agentd.docker.toml:/etc/agentd/agentd.toml:ro" \
  -v agentd-dev-data:/var/lib/agentd \
  agentd:dev
```

On Linux, add `--add-host=host.docker.internal:host-gateway` if the LLM runs on
the host. Before submitting a real turn, edit the provider fields and replace
the development token. Send `Authorization: Bearer local-dev-token` to `/v1/*`
for the unchanged local example.

The browser console at `/` is read-only. It lists tenants, agents, and runs,
then shows the selected run, raw trace, and delivery state. An API token entered
there stays in the browser tab.

## Create a tenant and agent

```sh
curl -X POST http://127.0.0.1:8080/v1/tenants \
  -H 'content-type: application/json' -d '{"name":"demo"}'

curl -X PUT http://127.0.0.1:8080/v1/tenants/demo/agents/simple-bot \
  -H 'content-type: application/toml' \
  --data-binary @agents/simple-bot.toml
```

Agent JSON/TOML is flat: `persona`, `model`, `allowed_families`, `timeout_ms`,
`max_steps`, `temperature`, `max_tokens`, and `context_window`. Omitting
`allowed_families` exposes every baseline family but never the privileged
`sandbox` family; add `sandbox` explicitly to opt an agent into command
execution. `allowed_families = []` exposes none.
`context_window` counts complete user/assistant turns, and `0` disables context.

## Submit a turn

```sh
curl -X POST http://127.0.0.1:8080/v1/tenants/demo/turns \
  -H 'content-type: application/json' \
  -d '{
    "agent":"simple-bot",
    "scope":"chat/42",
    "payload":{"text":"hello"}
  }'
```

Submission always returns `202` with a queued `run_id`. Pull the canonical
result with `GET /v1/tenants/demo/runs/:run_id/wait?timeout_ms=30000`. Run
states are `queued`, `running`, `succeeded`, `failed`, and `cancelled`. The
runtime serializes the same `(tenant, agent, scope)` and permits different
scopes to run concurrently. `request_id` is an optional tenant-scoped
idempotency key.

## Tools

There are 18 built-ins: artifact read/write/list; memory get/search/list/put/delete;
graph query; schedule get/list/put/delete; clock now; public-web search/fetch;
pure arithmetic; and the optional `sandbox_session`. Names are canonical
capability names. Mutating tools execute when their family is allowed; there is
no generic operator execute endpoint or approval workflow. `sandbox_session`
is registered only when the host enables microsandbox and the agent explicitly
allows the `sandbox` family. It offers `exec` and `/bin/bash -lc` actions in one
run-scoped microVM; repeated calls share guest files, and terminal run paths
destroy the VM. There are no model-visible session IDs, host mounts, persistent
cross-run sandboxes, audio, plan, LLM, output, dialog, or context tools.

Memory writes embed the concise canonical text before committing it. The
runtime contains pinned INT8 ONNX builds of `intfloat/multilingual-e5-small`
and `BAAI/bge-reranker-v2-m3`; it does not call an external retrieval provider
or silently fall back to lexical-only results. Queries use the E5 `query:`
prefix and facts use `passage:`; inputs over 512 E5 tokens are rejected in
favor of artifacts. Search fuses BM25 and E5 ranks with RRF, passes only the
top 10 original texts to BGE in one batch, then returns at most the reranked
top 5. The reranker stores no vectors or other database state.
The model decides when to search or write memory, and those actions remain
ordinary traced tool calls.

`memory_put` may include a bounded `graph` object containing canonical entities
and directed edges recognized in that fact. The memory row, embedding, entities,
and edges commit in one transaction; replacing or deleting the memory also
replaces or removes only its graph contribution. `graph_query` matches an entity
ID or exact label and walks incoming, outgoing, or both directions for at most
three hops. It is a separate, read-only, model-selected tool: Graph is not run on
every semantic search, and no entity extractor or graph database is required.

`memory_list` enumerates one namespace with a host-clamped page size and an
opaque cursor bound to the current run's tenant and namespace. It returns only
IDs, text, and timestamps; use `memory_search` for relevance retrieval.

Install the optional per-tenant maintenance resources explicitly with
`POST /v1/tenants/:tenant/presets/memory-maintenance`. The preset creates a
memory-only `system/memory-maintainer` agent pinned to `standard/chat` and a weekly
`system/memory-maintenance` schedule. The schedule starts disabled and has no
delivery destination, so installation alone produces no model calls or memory
changes. Enable or customize it through the normal schedule API. Maintainer runs
cannot succeed or mutate memory until they complete `memory_list` pagination for
the namespace supplied by the run input.

MCP servers are tenant resources at `/v1/tenants/:tenant/mcp/:name`. Transport
is a strict tagged object: stdio contains `command`, `args`, and optional
`env_from`; HTTP contains `url` and optional `headers_from`. The `*_from` maps
store environment-variable names, never secret values. Enabled PUT discovers
tools before saving and validates optional `allowed_tools`; disabled servers
can be saved offline with an empty catalog. Tools appear as
`mcp_<server>_<tool>` in the `mcp` family, and colliding exposed names are
rejected per tenant. The client requests MCP `2025-11-25`, accepts the supported
older versions, matches Streamable HTTP responses by JSON-RPC request ID, and
caps HTTP and stdio messages at 1 MiB. HTTP sessions are reused and
reinitialized once after session expiry. Enabled servers are rediscovered
independently at startup.

## Delivery

Every successful run stores one canonical output and is pullable. Delivery is
optional: a turn or schedule must explicitly provide
`"delivery":{"destination":"tg:42"}`. Scope is never treated as a
destination. Finalization atomically writes the output, terminal trace, rolling
context, and—when requested—one pending delivery referencing the run. Delivery
rows do not copy the output; claim/list responses join it from the run.
Adapters acknowledge `delivered`, `retry`, or `failed`; expired claims are
claimable again and retry updates the same row.

## Project documentation

- [Architecture and deliberate omissions](docs/architecture.md)
- [HTTP API](docs/http-api.md)
- [Deployment](docs/deployment.md)
- [Deterministic local demo](docs/demo.md)
- [Reliability evidence and known gaps](docs/reliability.md)
- [Threat model](docs/threat-model.md)
- [Security policy](SECURITY.md)
- [Contributing](CONTRIBUTING.md)
- [Changelog](CHANGELOG.md)
- [Draft v0.1.0-alpha.1 release notes](docs/releases/v0.1.0-alpha.1.md)

Before exposing an instance beyond loopback, read the threat model and security
policy. Before changing a runtime invariant, read the reliability matrix and
contribution guide.

## License

Licensed under the [Apache License 2.0](LICENSE).
The bundled models remain under their upstream MIT and Apache-2.0 licenses; see
[third-party notices](THIRD_PARTY_NOTICES.md).
