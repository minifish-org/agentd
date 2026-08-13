# Deployment

agentd deployments replace runtime data rather than migrate it.

The deployment image includes the pinned multilingual E5 Small ONNX and
tokenizer assets. Startup verifies every asset checksum, loads the model, and
runs one inference before accepting traffic. There is no runtime embedding
network dependency or alternate provider. The image also includes the model's
MIT license beside its assets and the repository's `THIRD_PARTY_NOTICES.md`.

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

If startup reports a schema mismatch, use `agentd --reset-data`; do not add an
ALTER, backfill, repair endpoint, or legacy parser.

Native development may set `AGENTD_EMBEDDING_MODEL_DIR` to the directory that
contains the pinned assets. This changes only their filesystem location;
checksums prevent it from selecting a different model. Production images use
`/opt/agentd/models/multilingual-e5-small`.

For a fresh native checkout, install and verify the pinned files before the
first start:

```sh
agentd_model_dir="$HOME/.cache/agentd/models/multilingual-e5-small"
./scripts/fetch-embedding-model.sh "$agentd_model_dir"
export AGENTD_EMBEDDING_MODEL_DIR="$agentd_model_dir"
```

For a local container smoke test, `configs/agentd.docker.toml` provides a
generic configuration with a development-only token and no personal service
addresses. Keep the published port bound to host loopback, or replace the token
before exposing the container to another network.

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
