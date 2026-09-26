# Audit history

agentd keeps three complementary records:

| Record | Purpose |
| --- | --- |
| `audit_events` | Durable access, mutation, scheduling and lifecycle history across resources |
| `run_log` | Detailed model requests/responses, tool calls/results and terminal run evidence |
| JSON service logs | Process diagnostics, including errors when durable audit storage is unavailable |

Audit is enabled for all tenants. It does not call a model and is not part of
any agent's context or memory. Schema v11 adds its table and indexes; versions
6–10 migrate in place. Migration records when audit coverage began. Historical
operations are not reconstructed from old resource snapshots.

## Coverage

| Action | Recorded evidence |
| --- | --- |
| `http.request` | Start and response-header completion of every `/v1` request, including reads, authentication rejection, invalid input, unknown routes and delivery polling |
| `tenant.*`, `agent.*`, `mcp.put/delete`, `schedule.put/delete` | Resource creation, configuration changes, deletion and idempotent no-ops where supported |
| `memory.put/delete`, `artifact.put/delete`, `context.put/delete` | Committed state changes, byte/count summaries and namespace/scope identifiers |
| `run.submit/claim/cancel/fail/restart/succeed` | Run lifecycle, including reused submission IDs and recovery of interrupted runs |
| `run.trace` | Reference to the exact `run_log` row, kind, known model phase/stage and numeric counters |
| `delivery.enqueue/claim/ack` | Outbox creation, lease attempts, acknowledgement and retry state |
| `schedule.decision/trigger` | Each due target's queued/skipped/failed decision, readiness counts, fixed reason, queued run IDs and schedule advancement |
| `behavior.*`, `memory_maintenance.*` | Policy promotion/rejection/staleness/reset and maintenance revisions/checkpoints |
| `mcp.discover`, `sandbox.*`, `server.*`, `schema.*`, `preset.install` | External discovery attempts, sandbox cleanup/reaping, process lifecycle, schema changes and preset conflicts |

A due background task records why it was skipped even when it creates no run.
For example, memory can have too few entries or no new external changes;
learning can have too few fresh samples, too few independent scopes, or an
already pending cycle. An empty wildcard target set records `no_targets`.
Scheduler ticks with no due work do not create records. A partially failed
fan-out retains the identities of already committed runs and its failure reason.

Request auditing also covers reads of audit history itself. It does not record
static console/health requests or requests rejected by a proxy or HTTP server
before they reach the application router. A completed HTTP record describes
the response status and time to response headers; it does not prove that a
client received every byte of a streamed response.

## Identity and correlation

Each event has a monotonic `id`, UTC `ts`, optional `tenant`, `actor_kind`,
`actor_id`, optional `request_id` and `run_id`, `action`, `resource_type`, optional
`resource_id`, `outcome`, and a structured `details` object.

- API requests receive a server-generated UUID in `X-Request-Id`. That ID also
  follows store mutations performed within the request. Client-supplied actor
  and request-ID headers are not trusted.
- API identity is `api/shared_api_token` after successful token validation or
  `api/unauthenticated` when no token was validated. The shared operator token
  cannot identify a particular human or adapter. A loopback deployment with no
  token remains unauthenticated even when a request is allowed.
- Runtime operations use `agent/<agent_ref>` and the run ID. Scheduler,
  dispatcher, bootstrap, startup and shutdown use explicit system actors.
- The API audit request ID differs from a turn's client-supplied idempotency
  key. Find `run.submit` under the audit request ID, then follow its `run_id`.
- HTTP `tenant` identifies the requested resource path; rejected authentication
  does not establish ownership of that tenant.

Identifiers are metadata: tenant names, agent references, scopes, namespaces
and artifact paths can appear in audit records. Do not put credentials into
resource names. The audit index excludes request bodies, raw queries/headers,
tokens, memory text, prompts, artifact bytes, delivery addresses/payloads,
MCP commands/URLs/environment mappings, arbitrary exception messages and model
tool names. Context paths retain their scope, and memory reads retain only the
namespace identifier from query parameters, never search text. Full execution
content remains in the protected run trace.

## Query API

The browser console's **Audit** view provides tenant/action/outcome/request/run
filters, manual refresh and **More** pagination. Leave tenant empty for global
history or enter a deleted tenant's name. **View audit** on a selected run opens
its related events. The console does not poll automatically.

Use the ordinary operator bearer token:

```text
GET /v1/audit
GET /v1/tenants/:tenant/audit
```

The global route includes system events and deleted tenants. The tenant route
forces the path tenant, rejects a conflicting query tenant, and remains usable
after that tenant is deleted. This is filtering, not a new authorization model:
the existing token grants instance-wide operator access.

Both routes accept exact-match `action`, `outcome`, `resource_type`,
`resource_id`, `actor_kind`, `actor_id`, `request_id` and `run_id` filters.
The global route additionally accepts `tenant`. Optional `since` and `until`
are inclusive RFC 3339 timestamps. `limit` defaults to 50 and accepts 1–500.
Unknown filters, malformed UUIDs/timestamps, negative `before_id`, invalid
limits and reversed time ranges return an error.

```text
/v1/tenants/demo/audit?action=schedule.decision&outcome=skipped
/v1/audit?request_id=00000000-0000-4000-8000-000000000001
/v1/tenants/demo/audit?run_id=00000000-0000-4000-8000-000000000002
```

Responses have the shape `{"events":[...],"next_before_id":123}`. Events are
ordered by decreasing ID. For the next page, preserve filters and set
`before_id` to `next_before_id`; `null` means the end. Newly arriving events do
not move previously read pages. Start again without a cursor to see new events.

## Durability and limits

Business mutations and their audit records commit in the same transaction.
An audit insertion failure rolls back that mutation. Compound HTTP operations
can contain several transactions; an error response does not mean every prior
change was rolled back. Each committed change retains its own audit evidence.

HTTP requests are recorded before the handler runs. If that write fails,
agentd returns `503` without invoking the handler. If response-completion
auditing fails, it returns `503` with `audit_stage="completed"`; already
committed changes remain, discoverable through their mutation records and
request ID. Audit storage errors also appear in service logs.

Model/tool execution and external services cannot participate in database
transactions. Starts precede external work; results follow it. A `started`
record without completion means interrupted or unknown outcome, never proof
that the external action did not happen. Cancellation/shutdown sandbox cleanup
still releases resources when audit storage fails; that exceptional gap is
reported to the service log. Sandbox creation/commands are covered by their
run tool trace, while cleanup/reaping have lifecycle audit records.

The table is append-only through the application and database UPDATE/DELETE
guards. Deleting a tenant preserves its audit history, but removes its raw run
traces and other business data. There is no audit-delete API or automatic
retention policy. Read polling and run activity grow the database; include it
in capacity planning and backups. Resource identifiers retain their original
values, and page limits bound event counts rather than total response bytes.
Structural details have a 64 KiB budget excluding the explicitly retained
top-level namespace/scope/agent and schedule identifiers. This preserves legacy
long names without relaxing the bound on arbitrary summary content.

These guards do not protect against a host/database administrator, replacement
of the database file, dropped triggers, or `--reset-data`. This is not a signed
or externally replicated audit ledger. Process termination or disk failure can
leave incomplete external-operation records; service logs and backups remain
part of an operational investigation.
