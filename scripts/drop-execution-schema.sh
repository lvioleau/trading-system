#!/usr/bin/env bash
# Drop the gateway's `execution` schema from the local dev Postgres — its tables,
# the enum types, and the `_sqlx_migrations` table (which lives in `execution`),
# so the next gateway boot re-runs the migrations from scratch. Use it after a
# migration changes (sqlx refuses a changed migration with a checksum mismatch).
#
# Dev only. It touches only the `execution` schema — contract-service's
# `reference` schema in the same `trading` database is left alone — and runs
# against the dev stack's shared Postgres container (scripts/dev-stack.sh).
#
# Override via environment:
#   ENGINE  container engine  (default: podman — what compose.yaml runs under)
#   NAME    container name    (default: trading-postgres, pinned in compose.yaml)
#   DB      database          (default: trading)
#
# Usage: scripts/drop-execution-schema.sh

set -euo pipefail

ENGINE="${ENGINE:-podman}"
command -v "${ENGINE}" >/dev/null || { echo "${ENGINE} not found on PATH" >&2; exit 1; }

NAME="${NAME:-trading-postgres}"
DB="${DB:-trading}"

if ! "${ENGINE}" container inspect "${NAME}" >/dev/null 2>&1; then
    echo "container ${NAME} is not running — start it with scripts/dev-stack.sh up -d postgres" >&2
    exit 1
fi

"${ENGINE}" exec "${NAME}" psql --username postgres --dbname "${DB}" \
    --command "drop schema if exists execution cascade"

echo "dropped schema execution from ${DB} (container ${NAME}); next gateway boot re-migrates"
