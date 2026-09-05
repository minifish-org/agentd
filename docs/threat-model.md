# Threat model

This document describes the intended security boundary of one agentd instance.
It is not a claim that the implementation is vulnerability-free.

## Deployment assumptions

agentd is a single-host service operated by one trusted administrator. The
administrator controls the process configuration, database, provider
credentials, MCP registrations, host account, reverse proxy, and transport
adapters.

`api_token` is an instance-wide operator credential. There is no per-user or
per-tenant authentication and no RBAC. Tenant names scope stored resources and
prevent accidental cross-tenant access through normal endpoints, but they do
not isolate mutually distrustful users who share the same API token.

For authenticated deployments:

- terminate TLS at a trusted reverse proxy or private network boundary;
- generate a high-entropy token and keep it out of source control, URLs, and
  logs;
- expose the API only to trusted adapters and operators;
- run agentd as a dedicated, unprivileged OS user;
- restrict filesystem and network access at the container/host layer;
- protect the database and backups as sensitive data.

Startup refuses a non-loopback listener when `api_token` is missing or blank.
Loopback without a token is intended only for a trusted local workstation.

## Assets

- LLM and MCP credentials supplied through configuration or environment;
- API bearer token;
- user prompts, model outputs, tool arguments/results, and raw run traces;
- context, memory, artifacts, schedules, deliveries, and MCP configuration;
- host filesystem, network identity, CPU, memory, and child-process authority.

The libSQL database stores application data in plaintext. Raw traces and
provider/tool payloads may contain sensitive content even when secret values
are not deliberately persisted.

## Trust boundaries

### HTTP clients

Every non-OPTIONS `/v1/*` request requires the configured bearer token. The
static console document at `/` and `/console` is public; its data requests use
the same authenticated API. CORS is permissive, so possession of the bearer
token—not browser origin—is the authorization boundary.

Anyone with the token is an operator. They can create/delete tenant resources,
submit turns, read traces and artifacts, configure MCP servers, and cause
stdio MCP processes to execute as the agentd OS user. Do not give this token to
untrusted end users.

### Model provider

The configured OpenAI-compatible provider receives prompts, selected context,
visible tool schemas, and tool results used in later model steps. Treat it as a
trusted data processor. Its output is untrusted input to the runtime and does
not bypass tool schemas or the agent's visible capability families.

Prompt injection is not fully preventable. A malicious prompt, memory item,
web page, or tool result may persuade the model to invoke any exposed tool.
Only expose capabilities whose side effects are acceptable without a human
approval step.

### Built-in tools

Baseline built-ins operate on tenant-scoped database resources, bounded
public-text web fetches, time, and arithmetic. There is no host shell, host
filesystem handle, or hidden host write.

`web_fetch` accepts only HTTP(S), rejects loopback/private/link-local and other
non-public resolved addresses, validates every redirect, disables automatic
redirects, and pins the checked addresses into the request client. Responses
must be text-like and are capped at 1 MiB. These controls reduce SSRF and DNS
rebinding risk; they do not make remote content trustworthy.

Artifact request bodies are capped at 64 MiB. The service does not currently
provide request rate limits, storage quotas, or per-tenant CPU/memory budgets,
so network and host-level resource controls remain necessary.

`memory_list` is bounded to one namespace and 100 items per call. Its cursor is
validated against the current run's tenant and namespace, and list responses
exclude embeddings and internal database identifiers. The optional maintenance
preset installs a memory-only agent and a disabled schedule; it does not grant
cross-tenant access or start model calls until an operator enables the schedule.
The reserved maintainer is pinned to `standard/chat`; its native loop binds
memory calls to the run's input namespace, blocks mutations until enumeration is
complete, and rejects a final response if any `memory_list` page remains unread.

Graph rows inherit the memory's tenant, namespace, and source memory ID.
`memory_put` bounds entity/edge counts and property sizes, rejects undeclared
edge endpoints, and commits graph rows in the same transaction as memory.
`graph_query` is read-only, cycle-safe, limited to three hops and 100 paths, and
does not broaden the caller's namespace. Entity labels, relations, properties,
and graph results remain untrusted model-visible data.

### Sandbox sessions

`sandbox_session` is absent unless the operator enables microsandbox globally,
and agents must explicitly include `sandbox` in `allowed_families`. Each run
gets at most one hardware-isolated microVM with fixed operator-selected CPU,
memory, image, command timeout, and output limits. The model cannot choose a
host mount, image, VM size, session identifier, or lifetime.

The guest has microsandbox's default network access and can therefore contact
addresses reachable from the host network; agentd does not add a domain
allowlist. Do not inject host credentials into guest commands. The integration
does not mount the agentd database, configuration, artifacts, or host working
tree. Command arguments and bounded command output are still stored in the raw
run trace and must be treated as sensitive.

On Linux, the outer agentd container receives only `/dev/kvm` and remains a
non-root process without blanket `--privileged` access. A guest compromise is
contained by the microVM boundary, but hypervisor vulnerabilities, network
side effects, denial of service within configured limits, and sensitive data
explicitly supplied to a command remain residual risks.

### MCP servers

MCP configuration is operator-controlled. Secret values are referenced by
environment-variable name and are not stored in MCP records. Stdio children
start with a cleared environment containing a small base set plus explicitly
mapped variables; messages are bounded and children are killed when their
session is dropped.

Stdio MCP is **not a sandbox**. Its command and arguments are arbitrary
operator configuration, and the child inherits the agentd OS user's host and
network permissions. HTTP MCP endpoints are also operator-selected and may
represent private trusted integrations. Run untrusted MCP servers in a
separate sandbox/container and expose only the minimum credentials they need.

### External transports

Telegram and other adapters are separate services. They authenticate to
agentd with the operator token and translate external identities into tenant,
agent, scope, and delivery destinations. The adapter is responsible for
authenticating its users, preventing confused-deputy routing, protecting its
own tokens, and acknowledging deliveries correctly.

## Security properties

The implementation is designed to preserve these properties:

- URL tenant identity scopes all persisted resources accessed by an endpoint;
- same-scope runs serialize while unrelated scopes may run concurrently;
- successful output, terminal trace, context, and optional delivery reference
  finalize atomically;
- cancelled runs cannot commit a partial successful result;
- delivery claims use random tokens, expiry, and token-checked acknowledgement;
- MCP catalogs, allowlists, and secret references are tenant-scoped;
- the container runs as a non-root user;
- retrieval model assets are pinned by revision and SHA-256 before use.

These properties are covered by tests where practical, but they are not a
substitute for deployment isolation and credential hygiene.

## Out of scope and residual risks

- hostile co-tenants who possess the same operator token;
- compromise of the host, reverse proxy, provider, MCP server, or transport
  adapter;
- side channels between workloads on the same host;
- availability under abusive load;
- deterministic replay or rollback of external side effects;
- safety or factual correctness of model output;
- data recovery and compatibility across experimental schema changes.

If an operator token or provider/MCP credential is exposed, rotate it first,
restart every consumer, verify the old credential is rejected, and then remove
it from reachable files, backups, logs, and any history intended for release.
