#!/usr/bin/env bash
# Dev/test stack launcher: pins the podman socket and delegates to `podman compose` on
# compose.yaml (Postgres + contract-service + position-service + powerbot-gateway + strategy +
# crosser + id-executor, service-DNS networking).
#
#   scripts/dev-stack.sh                          # build the image, then up (foreground)
#   scripts/dev-stack.sh up -d --build            # ...detached
#   scripts/dev-stack.sh up -d                    # start without rebuilding (builds anyway if
#                                                 # the image does not exist yet)
#   scripts/dev-stack.sh build                    # just (re)build the image
#   scripts/dev-stack.sh logs -f id-executor      # follow one service
#   scripts/dev-stack.sh up -d postgres           # just the database, for `cargo test`
#   scripts/dev-stack.sh down                     # stop the stack (the Postgres volume is kept)
#   scripts/dev-stack.sh down -v                  # ...and wipe the trade ledger / execution record
#   DELIVERY_AREA_EIC=10YDK-1--------W scripts/dev-stack.sh
#
# The image build is the launcher's, not compose's: one image carries all six binaries, and
# leaving it to compose means the same build runs once per service, in parallel — six
# concurrent cargo builds through one cache mount exhaust the podman machine. `--build` (and
# the bare invocation) map onto a single `podman build` here; the Dockerfile's cargo cache
# mounts make an unchanged tree a near-instant no-op.
#
# The nordpool-connector is not part of this stack — it has its own repo and its own compose
# stack. The services that subscribe to it dial CONNECTOR_HOST (default
# host.containers.internal), i.e. whatever serves :50051 on the host.
#
# The gateway's POWERBOT_* configuration comes from `gateway.shadow.env` (override: ENV_FILE),
# the gitignored file the old run-gateway.sh launcher used — including POWERBOT_API_KEY, the
# stack's only secret. A shell variable wins over the file; the repo-root `.env` compose reads
# for interpolation works too. Everything else in compose.yaml is endpoint configuration; see
# its header for the variables.
#
# The storage integration tests want the same Postgres this stack runs:
#   scripts/dev-stack.sh up -d postgres
#   export DATABASE_URL=postgres://postgres:postgres@localhost:5433/trading

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${REPO_ROOT}"

command -v podman >/dev/null || { echo "podman not found on PATH" >&2; exit 1; }

# On macOS podman derives the machine's API socket path from TMPDIR — including in
# `podman machine inspect` — so a shell with TMPDIR unset or altered (devenv shells are)
# computes a socket that doesn't exist. getconf returns the real per-user temp dir
# regardless of the environment; resolve the socket under it and pin DOCKER_HOST, which
# `podman compose` honors over its own computation.
if [[ "$(uname -s)" == "Darwin" && -z "${DOCKER_HOST:-}" ]]; then
    machine_sock="$(podman machine inspect --format '{{.ConnectionInfo.PodmanSocket.Path}}' 2>/dev/null || true)"
    if [[ ! -S "${machine_sock}" ]]; then
        machine_sock="$(TMPDIR="$(getconf DARWIN_USER_TEMP_DIR)" \
            podman machine inspect --format '{{.ConnectionInfo.PodmanSocket.Path}}' 2>/dev/null || true)"
    fi
    [[ -S "${machine_sock}" ]] && export DOCKER_HOST="unix://${machine_sock}"
fi

# The one image every service runs; the tag compose.yaml names.
IMAGE="${IMAGE:-localhost/trading-system:latest}"

# The gateway's POWERBOT_* config file (gitignored — it holds the API key), the same one the
# old run-gateway.sh launcher took. compose.yaml hands it to the gateway as an optional
# env_file, but a compose `environment:` entry always wins over env_file — so the keys the
# stack models (the api key, portfolio, delivery area, exchange) would resolve to their compose
# defaults and blank the file's values. Exporting them here, before compose interpolates, is
# what makes the file authoritative for those too. A variable already set in the shell wins
# over the file; the file wins over compose's defaults.
ENV_FILE="${ENV_FILE:-gateway.shadow.env}"
export ENV_FILE
if [[ -f "${ENV_FILE}" ]]; then
    while IFS='=' read -r key value; do
        # Skips comments and blank lines: neither matches a variable name.
        [[ "${key}" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]] || continue
        [[ -v "${key}" ]] && continue
        export "${key}=${value}"
    done < "${ENV_FILE}"
fi

# Default action: build what changed and run the stack in the foreground.
args=("$@")
[[ ${#args[@]} -eq 0 ]] && args=(up --build)

# `--build` is the launcher's flag, not compose's — take it out of what is delegated.
compose_args=()
build_requested=0
for arg in "${args[@]}"; do
    case "${arg}" in
        --build) build_requested=1 ;;
        *)       compose_args+=("${arg}") ;;
    esac
done

# `dev-stack.sh build [anything]` builds the image and stops: there is only the one image, so
# compose has nothing left to do with a build subcommand.
if [[ "${compose_args[0]:-}" == "build" ]]; then
    exec podman build -t "${IMAGE}" "${REPO_ROOT}"
fi

# Selected services are the non-flag arguments after the subcommand; none means all of them.
# `postgres` alone (the `cargo test` database) needs no image and no PowerBot key.
selected=()
for arg in "${compose_args[@]:1}"; do
    [[ "${arg}" == -* ]] || selected+=("${arg}")
done
selects_flow_service=1
if [[ ${#selected[@]} -gt 0 ]] && ! printf '%s\n' "${selected[@]}" | grep -qvx 'postgres'; then
    selects_flow_service=0
fi

case "${compose_args[0]:-}" in
    up|run|create)
        # compose.yaml carries no `build:`, so a missing image would surface as podman's
        # "image not known" mid-`up`. Build it instead — the same build `--build` asks for.
        if [[ ${build_requested} -eq 0 && ${selects_flow_service} -eq 1 ]] \
            && ! podman image exists "${IMAGE}"; then
            echo "${IMAGE} is not built yet — building it first." >&2
            build_requested=1
        fi
        # The gateway is the only service needing a secret, and with a blank key it starts and
        # fails at PowerBot auth rather than at config load — so say so up front. By now the
        # env file has been loaded, so an unset variable means no key anywhere but .env.
        if [[ -z "${POWERBOT_API_KEY:-}" ]] && ! grep -qs '^POWERBOT_API_KEY=' .env; then
            if [[ ${#selected[@]} -eq 0 ]] || printf '%s\n' "${selected[@]}" | grep -qx 'powerbot-gateway'; then
                echo "warning: POWERBOT_API_KEY is set neither in the environment nor in .env —" >&2
                echo "         the gateway will start and fail to authenticate against PowerBot." >&2
            fi
        fi
        ;;
esac

[[ ${build_requested} -eq 1 ]] && podman build -t "${IMAGE}" "${REPO_ROOT}"

exec podman compose "${compose_args[@]}"
