# HTTP API

Business resources use tenant paths. Tenant identity never comes from a body or
query parameter. The operator audit API additionally supports an instance-wide
history query with an optional tenant filter.

| Area | Endpoints |
| --- | --- |
| Tenant | `GET/POST /v1/tenants`, `GET/PATCH/DELETE /v1/tenants/:tenant` |
| Agent | list and `GET/PUT/DELETE .../agents/:agent` |
| Turn | `POST /v1/tenants/:tenant/turns` |
| Run | list/detail plus `wait`, `cancel`, and raw `trace` |
| Context | list/get/delete under `.../contexts/:agent` |
| Artifact | list and `GET/PUT/DELETE .../artifacts/:path` |
| Memory | `GET .../memory/:id`, `GET .../memory/search` |
| Preset | `POST .../presets/memory-maintenance`, `POST .../presets/behavior-learning` |
| Behavior learning | `GET/DELETE .../learning/:agent` |
| Schedule | list and `GET/PUT/DELETE .../schedules/:name` |
| Tool | `GET .../tools` |
| MCP | list and `GET/PUT/DELETE .../mcp/:name` |
| Delivery | list, `POST .../deliveries/claim`, and `POST .../:id/ack` |
| Audit | `GET /v1/audit`, `GET .../audit`; filter by action, outcome, resource, actor, request or run, with bounded cursor pages |

All `/v1` access, including reads and rejected requests, is audited. Responses
include a server-generated `X-Request-Id` linking request and mutation records.
Audit query, identity, privacy, failure and retention semantics are documented
in [audit history](audit.md). Auditing does not require or create agent runs.

Invalid input returns `400`, missing resources return `404`, conflicting
resource state returns `409`, and internal failures return `500`. Audit storage
failures retain the documented `503` semantics.

Turn input:

```json
{
  "agent": "simple-bot",
  "scope": "chat/42",
  "payload": {},
  "request_id": "optional-tenant-scoped-key",
  "delivery": { "destination": "optional-adapter-address" }
}
```

Turn submission is always asynchronous and returns HTTP `202` with
`{"run_id":"...","status":"queued"}`. Read the result from
`GET .../runs/:id/wait?timeout_ms=30000`; every successful run remains
pullable. Terminal wait responses include `error` when a run fails or is
cancelled, so adapters can surface the failure instead of silently dropping a
turn. `delivery` is optional and must be explicit—scope is never a delivery
destination. The trace endpoint returns stored `run_log` rows in insertion
order.

### Inline images

An object payload may include `images`, an array of `{ "url": "data:image/jpeg;base64,...", "caption": "optional source label" }`.
The native loop sends these as actual OpenAI-compatible `image_url` content parts,
alongside the rest of the payload as text, and preserves them in rolling context.
Text-only inputs retain the existing wire representation.

Only inline base64 JPEG, PNG and WebP are accepted (up to 8 images, 128 KiB of
decoded bytes per image, and 4096 bytes per caption). Remote URLs and SVG are
rejected; adapters must retrieve, bound and resize media before submitting it.
The runtime validates MIME signatures and base64, but does not decode images.
The configured provider and model must independently support visual input.
Invalid image input fails the asynchronous run. Image bytes are stored in run
input and, when enabled, context; choose retention and context limits accordingly.

### Schedules and other resources

Schedule PUT accepts `agent_ref`, `scope`, `payload`, `enabled`, optional
`delivery`, and exactly one of `at` or `cron`; cron also requires `timezone`.
Setting `enabled=false` is the only pause mechanism.

MCP PUT uses one of these incompatible transport shapes:

```json
{
  "enabled": true,
  "transport": {
    "type": "stdio",
    "command": "/absolute/path/to/mcp-server",
    "args": [],
    "env_from": { "CHILD_TOKEN": "AGENTD_PROVIDER_TOKEN" }
  },
  "allowed_tools": ["optional_tool_name"]
}
```

```json
{
  "enabled": true,
  "transport": {
    "type": "http",
    "url": "https://mcp.example/mcp",
    "headers_from": { "Authorization": "AGENTD_MCP_AUTHORIZATION" }
  }
}
```

The maps contain source environment-variable names, not credentials. Set the
HTTP environment value to the complete header value (for example, including
the `Bearer ` prefix). Enabled PUT performs discovery before committing;
`enabled=false` is stored without contacting the server. The exposed
`mcp_<server>_<tool>` names must be unique within a tenant; a colliding PUT is
rejected without changing the stored configuration.

Memory get/search accept optional `namespace`; an agent defaults to its own
name, while the operator REST defaults to `default`. Search combines FTS5 and
E5 semantic ranks with RRF, reranks the top 10 original texts with BGE v2-m3,
and returns a positive normalized relevance `score`; its default and maximum
limit are 5. Writes are available only to agents through
`memory_put`/`memory_delete` and fail atomically when embedding fails.

`memory_put` accepts an optional bounded `graph` with `entities` (`id`, `label`,
optional `type`/`properties`) and directed `edges` (`from`, `relation`, `to`,
optional `properties`). Every edge must reference entities declared by that
memory write. The memory text, embedding, entities, and edges commit together.
Replacing or deleting the memory replaces or removes its graph contribution.

The agent-only `graph_query` tool is in the memory capability family and uses
the same default namespace rules. It matches an entity ID or exact label, can
filter one relation and traverses `outgoing`, `incoming`, or `both`. When supplied,
tool arguments require `max_hops` in `1..=3` and `limit` in `1..=100`; storage also
bounds traversal and results. It is selected independently by
the model; `memory_search` does not automatically run Graph, and Graph does not
invoke embedding or reranking.

The agent-only `memory_list` tool enumerates one namespace in ID order. Its
limit must be in `1..=100`; `next_cursor=null` marks completion. Cursors are
opaque and bound to the current run's tenant and requested namespace. List
items contain ID, text, and timestamps, never embeddings or database row IDs.

Tenant creation automatically provisions both background agents and their
weekly schedules; server startup fills missing resources for existing tenants.
New schedules are enabled and have no delivery destination. Existing compatible
settings, including an explicit `enabled=false`, are preserved. Preset POSTs
remain idempotent repair endpoints; schedule PUT changes installed settings or
pauses work. Incompatible resources using reserved names produce `409 Conflict`.

`POST .../presets/memory-maintenance` ensures the memory-only maintainer agent,
pinned to `standard/chat`, and its schedule. Its payload defaults to
`namespace="*"` and `min_entries=5`; `min_entries` accepts integers from 2 to
10000. At trigger time, a namespace must have enough entries and an unconsumed
external content change to enter the queue. Changes mean insertions, changed
canonical text, or deletions; identical text and graph-only changes do not
qualify. The maintainer's own writes do not mark its namespace dirty. Its own
namespace is excluded from wildcard discovery, and a queued/running pass for
the same namespace prevents another scheduled run.

Ineligible work causes no run or model call, but the schedule advances normally.
Its `schedule.decision` audit records the reason and readiness counts; an empty
target set records `no_targets`. Schedule advancement has a separate summary.
For a fan-out trigger, `last_run_id` identifies the last queued run; use the runs
list to see all namespace runs. A trigger that queues nothing preserves the
previous `last_run_id`. Reapplying the preset repairs the reserved agent's model
and upgrades an otherwise unmodified legacy `default`-only schedule while
preserving its enabled state and compatible operator settings.

For `system/memory-maintainer` runs, the native loop binds all memory calls to
the input namespace (including the wrapped input of scheduled runs), requires an
initial cursor-free `memory_list`, and requires
every returned `next_cursor` to be followed until null. Before model execution
it rechecks the entry/change thresholds and records the starting external
revision. An ineligible queued or manual run completes with `skipped` and no
model call. `memory_put` and `memory_delete` are rejected until enumeration
completes, and a premature final response fails the run. Successful finalization
consumes only the starting revision, preserving concurrent external changes for
a later pass. Failed or cancelled runs consume no revision.

`POST .../presets/behavior-learning` accepts an optional JSON object of
`BehaviorLearningOptions`; an empty body uses defaults when creating a missing
schedule. It ensures the automatically provisioned tool-free, context-free
`system/behavior-learner` and enabled weekly `system/behavior-learning` schedule.
`target_agent="*"` considers foreground agents at schedule time. A concrete target
must exist in that tenant and must not be a system agent. Compatible existing
schedule settings are preserved on reapply; the learner's context window is
repaired to zero. Reserved-name conflicts return `409`.

Before queueing, the scheduler checks for `min_samples` new terminal source runs
(default eight), at least two scopes, and no queued/running learning cycle for
the same target. Sources must belong to the current agent lifecycle and be
newer than the consumed cursor. Failed prechecks create no run or model call.
They are necessary conditions only: the runtime still checks usable traces,
tool schemas, independent scopes, and the complete evaluation budget. An
ineligible queued or manual run reports `skipped` before any model call.

`GET .../learning/:agent` returns the active learned revision and a bounded
20-entry revision history. `DELETE` clears the active supplement while retaining
history and returns `{"tenant":"...","agent":"...","cleared":true|false}`.
Unknown tenants or agents return `404`. Clearing is idempotent and does not
disable future scheduled learning.

Learning cycles compare one next assistant decision from frozen historical
inputs, never execute the proposed tools, and automatically promote only
candidates that pass held-out judge comparisons and concurrent-update checks.
For an immediate cycle, submit a turn to `system/behavior-learner` with a
concrete `target_agent` in its payload. See [behavior learning](behavior-learning.md)
for options, budgets, examples, and the scope of these evaluations.

Delivery ack:

```json
{
  "claim_token": "...",
  "outcome": "delivered | retry | failed",
  "error": null,
  "retry_after_ms": null
}
```

Expired or incorrect tokens are rejected. Retry returns the same row to
`pending` and increments its attempt counter. Delivery rows capture an immutable
terminal payload; list and claim return that payload for both successful and
explicitly addressed failed runs.
