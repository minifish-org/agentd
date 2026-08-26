# HTTP API

All resources use tenant paths. Tenant identity never comes from a body or
query parameter.

| Area | Endpoints |
| --- | --- |
| Tenant | `GET/POST /v1/tenants`, `GET/PATCH/DELETE /v1/tenants/:tenant` |
| Agent | list and `GET/PUT/DELETE .../agents/:agent` |
| Turn | `POST /v1/tenants/:tenant/turns` |
| Run | list/detail plus `wait`, `cancel`, and raw `trace` |
| Context | list/get/delete under `.../contexts/:agent` |
| Artifact | list and `GET/PUT/DELETE .../artifacts/:path` |
| Memory | `GET .../memory/:id`, `GET .../memory/search` |
| Preset | `POST .../presets/memory-maintenance` |
| Schedule | list and `GET/PUT/DELETE .../schedules/:name` |
| Tool | `GET .../tools` |
| MCP | list and `GET/PUT/DELETE .../mcp/:name` |
| Delivery | list, `POST .../deliveries/claim`, and `POST .../:id/ack` |

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
pullable. `delivery` is optional and must be explicit—scope is never a delivery
destination. The trace endpoint returns stored `run_log` rows in insertion
order.

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
filter one relation, traverses `outgoing`, `incoming`, or `both`, and clamps
`max_hops` to `1..=3` and results to 100 paths. It is selected independently by
the model; `memory_search` does not automatically run Graph, and Graph does not
invoke embedding or reranking.

The agent-only `memory_list` tool enumerates one namespace in ID order. Its
limit is clamped to `1..=100`; `next_cursor=null` marks completion. Cursors are
opaque and bound to the current run's tenant and requested namespace. List
items contain ID, text, and timestamps, never embeddings or database row IDs.

`POST .../presets/memory-maintenance` idempotently creates the reserved
memory-only maintainer agent, pinned to `standard/chat`, and its weekly schedule.
The schedule is disabled and has no delivery by default. Reapplying the preset
repairs the reserved agent's model while preserving an existing compatible
schedule, including an operator's enabled state, namespace payload, and cron
changes; incompatible resources using the reserved names produce `409 Conflict`.

For `system/memory-maintainer` runs, the native loop binds all memory calls to
the input namespace, requires an initial cursor-free `memory_list`, and requires
every returned `next_cursor` to be followed until null. `memory_put` and
`memory_delete` are rejected until enumeration completes, and a premature final
response fails the run instead of accepting an unverified maintenance report.

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
`pending` and increments its attempt counter. Delivery rows store no payload
copy; list and claim join `payload` from the referenced run output.
