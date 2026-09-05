# Deployment

agentd normally treats runtime data as replaceable. Schema v7 has one deliberate
exception: an existing v6 database is migrated in place by adding the graph
tables and indexes without rewriting memory rows. Back up the data directory
before the first v7 start.

The deployment image includes pinned INT8 ONNX and tokenizer assets for
multilingual E5 Small and BGE reranker v2-m3. Startup verifies every asset
checksum, loads both models, and runs inference before accepting traffic. There
is no runtime retrieval network dependency or alternate provider. The image
also includes both model licenses beside their assets and the repository's
`THIRD_PARTY_NOTICES.md`.

1. Stop the local agentd service.
2. Atomically rename the existing data directory as a short-lived rollback
   backup.
3. Start the new server against an empty path so it creates the current schema.
4. Create the required tenants and PUT the agent TOML files through REST.
5. Register required MCP servers through the tenant MCP endpoint.
6. Health-check the empty deployment by creating, reading, and deleting a
   temporary artifact.
7. Deploy or restart the remote Telegram adapter and verify claim/ack delivery.
8. Verify the local and remote services and the Telegram decoy page.
9. Delete the rollback data only after the checks pass.

If startup reports a schema mismatch other than the supported v6→v7 migration,
use `agentd --reset-data`; do not add an ad hoc ALTER, backfill, repair endpoint,
or legacy parser.

Native development may set `AGENTD_EMBEDDING_MODEL_DIR` and
`AGENTD_RERANKER_MODEL_DIR` to directories containing the pinned assets. This
changes only their filesystem locations; checksums prevent selecting different
models. Production images use `/opt/agentd/models/multilingual-e5-small` and
`/opt/agentd/models/bge-reranker-v2-m3`.

For a fresh native checkout, install and verify the pinned files before the
first start:

```sh
agentd_model_dir="$HOME/.cache/agentd/models/multilingual-e5-small"
agentd_reranker_dir="$HOME/.cache/agentd/models/bge-reranker-v2-m3"
./scripts/fetch-embedding-model.sh "$agentd_model_dir"
./scripts/fetch-reranker-model.sh "$agentd_reranker_dir"
export AGENTD_EMBEDDING_MODEL_DIR="$agentd_model_dir"
export AGENTD_RERANKER_MODEL_DIR="$agentd_reranker_dir"
```

For a local container smoke test, `configs/agentd.docker.toml` provides a
generic configuration with a development-only token and no personal service
addresses. Keep the published port bound to host loopback, or replace the token
before exposing the container to another network.

## KVM sandbox deployment

`sandbox_session` requires microsandbox hardware virtualization. The current
production topology must run on a Linux host where `/dev/kvm` exists and is
readable and writable by the container's agentd user. Provision at least 8
logical CPUs and 8 GiB RAM for the initial four-run configuration. Do not try
to compensate for a missing KVM device with `--privileged`.

The production image links microsandbox's SQLx state database to Debian's
shared SQLite library so it can coexist with agentd's bundled libSQL. Linux
source builds outside the image need `libcap-ng-dev`, `libsqlite3-dev`, and
`pkg-config`, with `LIBSQLITE3_SYS_USE_PKG_CONFIG=1` set while compiling.

1. Build and publish `Dockerfile.sandbox` with the container workflow. Copy the
   resulting `ghcr.io/minifish-org/agentd-sandbox@sha256:...` digest into a
   production copy of `configs/agentd.kvm.toml.example`.
2. On the new host, verify `test -r /dev/kvm`, `test -w /dev/kvm`, and record
   the numeric kvm group ID with `getent group kvm`.
3. Stop the old agentd instance before copying its database volume. Never run
   the old and new instances against the same SQLite database.
4. Set `KVM_GID` and `AGENTD_CONFIG_PATH`, then deploy
   `deploy/docker-compose.kvm.yml`. It maps only `/dev/kvm`, adds the kvm group,
   and keeps agentd running as UID 10001 without `--privileged` or extra Linux
   capabilities.
5. Keep the `microsandbox-data` volume across restarts. On the first enabled
   startup the pinned microsandbox SDK installs and verifies its matching
   runtime bundle there; later starts reuse it and the OCI image cache.

Sandbox remains an explicit agent permission. A canary agent must include, for
example, `allowed_families = ["clock", "sandbox"]`; agents that omit
`allowed_families` do not receive `sandbox_session`. Exercise repeated shell,
Python and Node calls, a public `curl`, a non-zero command, a timed-out command,
and cancellation. After each terminal run, verify that microsandbox has no
remaining `agentd-run-*` instance. Existing HTTP/stdio MCP registrations stay
where they are and are not migrated into the sandbox.

On an Apple Silicon development Mac, run `scripts/sandbox-canary.sh` natively
from the repository. It uses an isolated temporary database and microsandbox
state directory, runs the full/isolation/cancellation scenarios, and removes
the temporary state afterward. Set `AGENTD_SANDBOX_TEST_IMAGE` to override its
fixed public Debian canary image.

## Network boundary

An unauthenticated deployment may listen only on a loopback address. Binding
`rest_addr` to `0.0.0.0`, `::`, a LAN address, or a public address requires a
non-empty `api_token`; startup fails before opening the database otherwise.
Every `/v1/*` client must then send `Authorization: Bearer <api_token>`.

The static console remains public at `/`, but it is read-only and its API
requests are subject to the same bearer-token check. Put internet-facing
deployments behind TLS and do not expose the service with an empty token. The
instance token is an operator credential: anyone holding it can configure
stdio MCP commands that execute as the agentd OS user. See the full
[threat model](threat-model.md).
