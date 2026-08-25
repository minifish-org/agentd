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
  context → model ⇄ allowed built-in/MCP tools
          |
          v
 transaction: output + terminal trace + context + optional delivery reference
```

## Boundaries

- The model chooses real tools and the final JSON; host code validates and
  executes.
- Tenant ownership applies to agents, runs, context, artifacts, memory,
  schedules, MCP servers, and deliveries.
- Scope is both the rolling-context key and serialization key. Different
  scopes may run concurrently.
- Context is a bounded conversation window; memory is explicit durable text
  with lexical and semantic derived indexes; artifacts are payloads; `run_log`
  is the raw execution trace.
- External transports call REST and consume the outbox. They never link core
  code or read the database.

## Persistence

The schema has one version and no upgrade path. Startup accepts an empty
database or the exact version; otherwise it requests `--reset-data`.

Important facts are stored once. Runs own activation, final output, and an
optional requested destination; `run_log` owns model/tool/output/status/error
observations; contexts own recent messages; deliveries reference runs and own
only remote delivery state and retry fields. There are no
activation, receipt, worker, step, side-effect, token, lease, RAG metadata, or
replay tables.

Memory remains one logical table. Its canonical text and fixed 384-dimension
little-endian f32 embedding share the same row; one FTS5 index follows the text
with triggers. Schema v6 is bound to the pinned multilingual E5 Small model;
changing that model requires a schema bump and data reset. Search scans vectors
only inside the selected tenant/namespace and fuses semantic and lexical ranks
with RRF. A pinned INT8 BGE v2-m3 cross-encoder reranks the RRF top 10 from the
query and original text, and the API returns at most the top 5. The reranker
persists no vectors. There are no dynamic RAG tables, vector metadata resources,
automatic recall, or automatic writes.

Enumeration uses bounded keyset pages over one tenant and namespace. The
optional memory maintainer is an ordinary tenant agent plus an ordinary
disabled-by-default schedule: due work enters the same queued run, claim,
native tool loop, and `run_log` path as interactive work. There is no background
agent type, heartbeat, cross-tenant maintainer, or host-side consolidation
primitive.

## Deliberate omissions

There is no independent CLI, Controller forwarding layer, compatibility
parser, migration framework, derived audit database, replay simulator,
review/approval queue, shell, arbitrary HTTP tool, audio tool, or multi-agent
orchestration layer. New abstractions require an observed consumer.
