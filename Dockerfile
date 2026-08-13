FROM debian:trixie-slim AS embedding-model
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*
COPY scripts/fetch-embedding-model.sh /usr/local/bin/fetch-embedding-model
COPY licenses/multilingual-e5-small.LICENSE /usr/local/share/agentd/multilingual-e5-small.LICENSE
RUN /usr/local/bin/fetch-embedding-model \
    /models/multilingual-e5-small \
    /usr/local/share/agentd/multilingual-e5-small.LICENSE

FROM rust:1.92-trixie AS builder
WORKDIR /src
COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY .cargo ./.cargo
COPY crates ./crates
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/build \
    cargo build --locked --release -p agentd \
    && cp /src/build/release/agentd /tmp/agentd

FROM debian:trixie-slim
LABEL org.opencontainers.image.source="https://github.com/minifish-org/agentd"
LABEL org.opencontainers.image.licenses="Apache-2.0"
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10001 --home-dir /var/lib/agentd agentd \
    && mkdir -p /etc/agentd /var/lib/agentd /opt/agentd/models \
    && chown -R agentd:agentd /var/lib/agentd
COPY --from=builder /tmp/agentd /usr/local/bin/agentd
COPY --from=embedding-model /models/multilingual-e5-small /opt/agentd/models/multilingual-e5-small
COPY LICENSE THIRD_PARTY_NOTICES.md /usr/share/doc/agentd/
ENV AGENTD_CONFIG=/etc/agentd/agentd.toml
EXPOSE 8080
USER agentd
HEALTHCHECK --interval=10s --timeout=3s --start-period=30s --retries=6 \
  CMD curl --fail --silent http://127.0.0.1:8080/ >/dev/null || exit 1
ENTRYPOINT ["/usr/local/bin/agentd"]
