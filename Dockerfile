# Multi-stage build for the trading-system services: one image carrying the contract-service,
# position-service, powerbot-gateway, strategy (mock), crosser, and id-executor binaries, run as
# separate containers — the container command names the service to start (there is no default). One
# workspace released as a unit means one image to build, tag, and pin; cargo compiles the shared
# dependency tree once for all binaries.
#
# protoc is vendored by the crates' build scripts (protoc-bin-vendored), so the builder
# needs no system protobuf-compiler. The services speak plain-HTTP/2 gRPC among themselves;
# the gateway's TLS to PowerBot (websocket + REST) is rustls with bundled webpki roots, but
# ca-certificates is still shipped for any system-cert path at runtime.

FROM rust:1-bookworm AS builder

# The strategy binary embeds a Python interpreter (pyo3) to run the strategy module: the build
# links against bookworm's libpython, so the runtime stage must install the matching python3.
RUN apt-get update && apt-get install -y --no-install-recommends \
        python3-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY . .
# Cache mounts (BuildKit / podman ≥4) keep the cargo registry and target dir across builds,
# so a source edit recompiles only the changed crates, not every dependency. The target dir
# lives in the cache, not the layer, hence the copy out.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/app/target \
    cargo build --release -p contract-service -p position-service -p powerbot-gateway \
        -p strategy -p crosser -p id-executor \
    && cp target/release/contract-service target/release/position-service \
        target/release/powerbot-gateway target/release/strategy target/release/crosser \
        target/release/id-executor /

FROM debian:bookworm-slim AS runtime

# python3 + libpython3: the strategy binary's embedded interpreter (see the builder stage).
RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates \
        python3 \
        libpython3.11 \
    && rm -rf /var/lib/apt/lists/* \
    && useradd -r -u 10001 -m -d /app appuser

WORKDIR /app
COPY --from=builder /contract-service /position-service /powerbot-gateway /strategy /crosser \
    /id-executor /usr/local/bin/
# Config resolves relative to cwd (or CONTRACT_SERVICE_CONFIG / POSITION_SERVICE_CONFIG /
# POWERBOT_CONFIG / STRATEGY_CONFIG / CROSSER_CONFIG / ID_EXECUTOR_CONFIG). Ship the templates;
# ${VAR} references are filled from the container environment at startup.
COPY contract-service.ini position-service.ini powerbot.ini strategy.ini crosser.ini \
    id-executor.ini /app/
# The reference strategy module, at the same repo-relative path strategy.ini defaults to, so
# the default config works unchanged in-container (override via STRATEGY_PYTHON).
COPY crates/strategy/py/strategy.py /app/crates/strategy/py/strategy.py
# Log directory for the id-executor's per-run file mirror (ID_EXECUTOR_LOG_DIR), owned by the
# runtime user: a volume mounted here (compose.yaml) inherits that ownership from the image,
# so the non-root process can create its file.
RUN mkdir -p /logs && chown appuser /logs

USER appuser
# No ENTRYPOINT/CMD on purpose: starting the wrong service silently is worse than "no command
# specified" — name the binary explicitly:
#   docker run <image> contract-service
#   docker run <image> position-service
#   docker run <image> powerbot-gateway
#   docker run <image> strategy
#   docker run <image> crosser
#   docker run <image> id-executor
