# Local libsql patch

Source: crates.io `libsql` 0.9.30, upstream commit
`0653c5788d77ef16a97c56ff3e9fdc11717a72d9` (`libsql/`).
The published crate is copied here without its registry cache marker or its
independent Cargo.lock. The upstream root LICENSE.md is included. Upstream trailing whitespace is
normalized in four files so repository whitespace checks remain clean.

## Change

`src/local/connection.rs`: make `Connection::disconnect` idempotent by clearing
the native handle before closing it. Both `LibsqlConnection::drop` and its
inner `Connection::drop` invoke this method. Previously the final owner could
close the same pointer twice; concurrent connection allocation could reuse
that pointer between the closes.

The workspace applies this copy through `[patch.crates-io]`; no API, feature,
or transitive dependency versions are changed. Remove the patch after adopting
an upstream release with equivalent ownership protection.

Regression coverage in agentd-store:
`parallel_connection_teardown_preserves_live_statements_and_transactions`.
Run `cargo test -p agentd-store parallel_connection_teardown` and the normal
parallel workspace suite.
