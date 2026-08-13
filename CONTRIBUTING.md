# Contributing

agentd is experimental pre-1.0 software. Contributions are welcome when they
strengthen the existing single-host runtime and come with evidence for the
behavior they change. Backward compatibility, migrations, and a broad plugin
surface are not current goals.

## Before starting

Open an issue before work that changes the database schema, HTTP contract,
runtime state machine, capability boundary, or dependency footprint. Describe
the observed problem, the smallest useful behavior change, failure semantics,
and how it will be tested. A proposed abstraction without a concrete consumer
may be declined even when the implementation is sound.

Security reports do not belong in public issues. Follow
[`SECURITY.md`](SECURITY.md) instead.

## Development setup

The workspace pins Rust 1.92 through `rust-toolchain.toml`. Native runtime use
also needs `curl`, an OpenAI-compatible chat-completions provider, and the
pinned embedding assets:

```sh
model_dir="${AGENTD_EMBEDDING_MODEL_DIR:-$HOME/.cache/agentd/models/multilingual-e5-small}"
./scripts/fetch-embedding-model.sh "$model_dir"
export AGENTD_EMBEDDING_MODEL_DIR="$model_dir"
```

The default test suite does not call a live LLM provider and does not require
the embedding model. See the [README](README.md) for native and Docker startup.

## Required checks

Run these before opening a pull request:

```sh
cargo fmt --all -- --check
cargo check --workspace --all-targets
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
bash scripts/test_deployment.sh
docker build --check .
```

Changes to embedding behavior must also run the ignored real-model test shown
in [`docs/reliability.md`](docs/reliability.md). Changes to dependencies must
pass the repository's `cargo deny` policy. CI and Security workflows rerun the
applicable checks on pull requests.

## Change expectations

- Keep tenant identity in the URL and preserve tenant-scoped storage access.
- Treat `(tenant, agent, scope)` as both the context and serialization key.
- Keep canonical output pullable; delivery remains optional and explicit.
- Preserve atomic finalization of output, terminal trace, context, and optional
  delivery reference.
- Do not add shell, arbitrary HTTP, filesystem, or hidden operator capabilities
  under a narrower-looking tool name.
- Keep secret values out of API records, examples, tests, logs, and Git history.
- Update the reliability matrix when adding, removing, or changing a tested
  runtime property.
- Document intentional public API or data-reset changes in `CHANGELOG.md`.

Prefer focused commits and tests named after observable behavior. Pull request
descriptions should explain the problem, tradeoffs, failure behavior, and exact
validation performed.

Contributors are responsible for reviewing generated or AI-assisted work,
verifying its licenses, and ensuring it contains no confidential or third-party
material they are not permitted to submit. By submitting a contribution, you
agree that it may be distributed under the repository's Apache-2.0 license.
