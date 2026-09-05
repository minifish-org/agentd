# Architecture

agentd is a multi-tenant control plane around one native single-agent loop. It
is not a workflow builder and does not run user agent code.

```text
REST turn / due schedule
          |
          v
      queued run -- transactional (tenant, agent, scope) claim
          |
          v
  context → model ⇄ allowed built-in/MCP/sandbox tools
          |
          v
 transaction: output + terminal trace + context + optional delivery reference
```

## Boundaries

- The model chooses real tools and the final JSON; host code validates and
  executes.
- Tenant ownership applies to agents, runs, context, artifacts, memory and its
  graph projection, schedules, MCP servers, and deliveries.
- Scope is both the rolling-context key and serialization key. Different
  scopes may run concurrently.
- Context is a bounded conversation window; memory is explicit durable text
  with lexical and semantic derived indexes; artifacts are payloads; `run_log`
  is the raw execution trace.
- External transports call REST and consume the outbox. They never link core
  code or read the database.
- An enabled `sandbox_session` lazily assigns one microsandbox microVM to a run.
  The run ID is the internal lifecycle key; models see only `exec` and `shell`.
  Guest files persist between calls in that run and are destroyed at every
  terminal path. Sandbox metadata is not stored in agentd's database.

## Persistence

The schema is versioned. Startup creates v7 for an empty database and performs
the one supported in-place migration from v6 to v7; unknown versions request
`--reset-data`.

Important facts are stored once. Runs own activation, final output, and an
optional requested destination; `run_log` owns model/tool/output/status/error
observations; contexts own recent messages; deliveries reference runs and own
only remote delivery state and retry fields. There are no
activation, receipt, worker, step, side-effect, token, lease, RAG metadata, or
replay tables.

Canonical memory remains one logical table. Its text and fixed 384-dimension
little-endian f32 embedding share the same row; one FTS5 index follows the text
with triggers. The embedding representation remains bound to the pinned
multilingual E5 Small model; changing that model requires a schema bump and data
reset. Search scans vectors
only inside the selected tenant/namespace and fuses semantic and lexical ranks
with RRF. A pinned INT8 BGE v2-m3 cross-encoder reranks the RRF top 10 from the
query and original text, and the API returns at most the top 5. The reranker
persists no vectors.

Schema v7 adds `entities` and `edges` as a bounded, provenance-preserving graph
projection of memory. `memory_put` can supply structured entities and relations;
the host validates them and commits them atomically with the memory and its
embedding. Updating or deleting a memory replaces or cascades only that memory's
graph rows. `graph_query` uses ordinary joins and `WITH RECURSIVE`, is isolated
by tenant and namespace, prevents cycles, and clamps traversal to 1–3 hops and
100 paths. Graph retrieval and BM25/E5/RRF/BGE retrieval are complementary,
separately selected tools. There is no automatic entity-extraction model,
automatic recall, or automatic write.

Enumeration uses bounded keyset pages over one tenant and namespace. The
optional memory maintainer is an ordinary tenant agent plus an ordinary
disabled-by-default schedule: due work enters the same queued run, claim,
native tool loop, and `run_log` path as interactive work. There is no background
agent type, heartbeat, cross-tenant maintainer, or host-side consolidation
primitive.

## Deliberate omissions

There is no independent CLI, Controller forwarding layer, general compatibility
parser or migration framework beyond the explicit v6→v7 step, derived audit
database, replay simulator, review/approval queue, host shell, dedicated
arbitrary-HTTP tool, persistent sandbox session, audio tool, or multi-agent
orchestration layer. New abstractions require an observed consumer.
