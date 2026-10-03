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
 transaction: output + terminal trace + context + optional delivery + audit
```

## Boundaries

- The model chooses real tools and the final JSON; host code validates and
  executes.
- Runtime tool calls and offline learning decisions share compiled schema
  validation and the built-ins' pure parameter checks. Validators are cached
  within a bounded cache. Local schema references are supported; validation
  never retrieves schemas from the network or filesystem.
- Tenant ownership applies to agents, runs, context, artifacts, memory and its
  graph projection, schedules, MCP servers, deliveries, and behavior policies
  and revision history.
- Scope is both the rolling-context key and serialization key. Different
  scopes may run concurrently.
- Startup acquires the listener and exclusive ownership of the database and,
  when enabled, the sandbox state directory before resetting data or recovering
  interrupted runs. Only the process holding these locks may run the local
  scheduler, dispatcher and sandbox reaper.
- A run supervisor owns execution tasks and observes their completion,
  including panics. Cancellation and shutdown stop execution, persist a
  terminal state, and release run-scoped sandbox resources. Deleting a tenant
  also stops its active tasks before deleting their stored state. Failed
  sandbox destruction remains retryable and prevents tenant deletion from
  reporting success. Shutdown waits for detached cancellation and deletion.
- Context is a bounded conversation window; memory is explicit durable text
  with lexical and semantic derived indexes; artifacts are payloads; `run_log`
  is the raw execution trace.
- External transports call REST and consume the outbox. They never link core
  code or read the database.
- An enabled `sandbox_session` lazily assigns one microsandbox microVM to a run.
  The run ID is the internal lifecycle key; models see only `exec` and `shell`.
  Guest files persist between calls in that run and are destroyed at every
  terminal path. Live sandbox handles stay in memory; cleanup/reaping outcomes
  and run tool evidence are audited in the database.

## Repeated-failure detection

Each run has a small in-memory loop guard. Three consecutive calls with the
same tool, structurally equal JSON arguments, and exactly the same failure
produce one advisory reminder per streak. Success or a change in tool,
arguments, or error resets the streak; identical successful polls do not warn.
The guard retains only the current failure signature and its first three call
IDs. It does not judge task completion, detect alternating loops, or stop a run.
Existing step and timeout limits remain the bounds on execution.

The reminder follows all tool results in the current model response. Its system
text is fixed: tool arguments and errors stay in their original tool messages,
not in system instructions. A `loop_guard` run-log event records the triggering
step, tool name, three call IDs, repeat count, and reminder. The call IDs link
back to the existing raw tool events. Guard state and reminders are not retained
in cross-run conversation history.

## Persistence

The schema is versioned. Startup creates v11 for an empty database and migrates
versions 6–10 in place; unknown versions request `--reset-data`.

Important facts are stored once. Runs own activation, final output, and an
optional requested destination; `run_log` owns model/tool/output/status/error
observations; contexts own recent messages; deliveries reference runs and own
an immutable terminal payload, remote delivery state, and retry fields. There are no
activation, receipt, worker, step, side-effect, token, lease, RAG metadata, or
replay tables.

`audit_events` owns cross-resource access, mutation and scheduling history.
Each business mutation writes its audit record in the same transaction;
task-local actor/request/run identity follows the operation without entering
agent context. HTTP starts precede handlers and completions follow response
creation. Due scheduling decisions are recorded even without a run. Audit rows
have no tenant foreign key, survive tenant deletion, and reject UPDATE/DELETE.
They hold safe summaries and references to raw trace rows; they do not copy
model/tool content. See [audit history](audit.md) for query and failure semantics.

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

Enumeration uses bounded keyset pages over one tenant and namespace. Tenant
creation automatically provisions memory-maintenance and behavior-learning
resources with enabled schedules; startup fills missing resources for existing
tenants. Compatible custom settings and explicit disabled schedules survive
both paths. The memory maintainer is an ordinary tenant agent whose eligible
work enters the queued run, claim, native tool loop, and `run_log` path.

Each scheduled target has a stable occurrence identity bound to the stored
due time, schedule incarnation and specification, and target. A tick reuses
already committed runs after an interrupted fan-out or failed summary commit;
it does not queue the same occurrence twice. Schedule advancement checks that
the schedule has not changed since planning. Preset repairs likewise check
their resource snapshots before committing agent and schedule changes together.

The store keeps one public facade with internal modules for database access,
migrations, resources, retrieval, runs, deliveries, and schedules. Cross-resource
operations own their transactions explicitly; splitting modules does not split
run finalization into separate commits. Semantic ranking reads bounded batches
from one database snapshot and retains only the best candidates in memory.

A memory namespace becomes eligible when it has at least `min_entries` entries
(five by default) and an external content revision newer than its successful
checkpoint. Insertions, changes to canonical text, and deletions advance that
revision; identical text, graph-only changes, and the prepared maintainer's own
writes do not. The scheduler suppresses ineligible or already-pending work.
Before model execution the runtime repeats the check and captures a starting
revision. Success consumes only that revision atomically with finalization, so
concurrent external writes remain pending. Failure or cancellation consumes
nothing. Each eligible namespace gets a separate scope; the maintainer's own
namespace is excluded, and its rolling context is disabled.

## Behavior learning

Behavior learning considers each foreground agent in a tenant. A database
precheck requires new terminal source runs from the current agent lifecycle
(eight by default), two distinct scopes, and no pending cycle for the same
target. It reads bounded source metadata without model calls; the runtime then
checks trace usability, allowed schemas, independent scopes, and budget. Failed
prechecks advance the schedule without creating a run; queued or manual runs
can still finish with `skipped`. Its host-controlled loop uses independent
model contexts to propose an instruction supplement, produce baseline
and candidate next decisions from frozen historical request prefixes, and judge
each pair in both presentation orders. Tool schemas and existing observations
are part of that frozen evidence; generated tool calls are never executed.
This evaluates offline decisions, not complete alternative trajectories.

Development and held-out samples are separated by source run. All held-out
cases must avoid regression and pass a mean improvement threshold before a
proposal can become active. Promotion checks the target specification and
parent revision, and commits the policy revision with the coordinator's
terminal result. Ordinary claimed runs capture the active learned supplement
alongside the owner-authored prompt. Dedicated policy/revision tables keep
learning state outside business memory, artifacts, and rolling context.
See [behavior learning](behavior-learning.md) for defaults and limitations.

## Deliberate omissions

There is no independent CLI, Controller forwarding layer, general compatibility
parser or migration framework beyond the explicit supported schema migrations,
derived audit database, external-side-effect replay simulator, review/approval queue, host shell, dedicated
arbitrary-HTTP tool, persistent sandbox session, audio tool, or multi-agent
orchestration layer. New abstractions require an observed consumer.
