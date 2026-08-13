# Security policy

agentd is experimental pre-1.0 software. Only the current `main` branch is
maintained; there are no supported release lines yet.

## Reporting a vulnerability

Please use GitHub's **Report a vulnerability** flow under the repository's
Security tab. Do not open a public issue for a suspected vulnerability or
include secrets, personal data, exploit details, or production addresses in a
public discussion.

Include enough information to reproduce and assess the issue:

- affected commit or image digest;
- deployment shape and relevant sanitized configuration;
- prerequisites and attacker capabilities;
- minimal reproduction and observed impact;
- suggested mitigation, if known.

Reports are handled on a best-effort basis. The maintainer will acknowledge a
complete report within seven days and coordinate disclosure after a fix or
mitigation is available. Experimental status does not reduce the priority of a
credential leak, authentication bypass, tenant-boundary violation, SSRF,
arbitrary command execution outside the documented operator boundary, or
dependency compromise.

The security workflow scans Rust dependencies for advisories, enforces an
explicit dependency-license/source policy, and scans the currently tracked
source tree for secrets. A sanitized public repository must additionally start
from a clean history; current-tree scanning does not make old Git objects safe
to publish.

## Before reporting

The following are documented limitations rather than vulnerabilities by
themselves:

- a holder of the instance API token is a trusted operator, not an end user;
- an operator can register stdio MCP commands that run as the agentd OS user;
- tenants are data namespaces, not separate authentication principals;
- prompt injection may cause any capability explicitly exposed to an agent to
  be used; there is no human approval workflow;
- the service has no built-in TLS, rate limiting, per-tenant quotas, HA, or
  sandbox for operator-configured MCP processes;
- the read-only console HTML is public, while all of its `/v1/*` data requests
  require the configured bearer token.

See the complete [threat model](docs/threat-model.md) and
[deployment guidance](docs/deployment.md).
