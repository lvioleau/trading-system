#!/usr/bin/env bash
# Print the gateway's `execution` tables to stdout, for a quick look at the
# durable record during dev. Runs psql inside the shared trading-postgres
# container (the dev stack's Postgres, scripts/dev-stack.sh).
#
# Pass one or more table names to print just those (trades | internal_trades |
# order_revisions); with no argument it prints all three, newest first.
#
# Override via environment:
#   ENGINE  container engine  (default: podman — what compose.yaml runs under)
#   NAME    container name    (default: trading-postgres, pinned in compose.yaml)
#   DB      database          (default: trading)
#
# Usage:
#   scripts/dump-execution.sh                 # all three tables
#   scripts/dump-execution.sh order_revisions # just one

set -euo pipefail

ENGINE="${ENGINE:-podman}"
command -v "${ENGINE}" >/dev/null || { echo "${ENGINE} not found on PATH" >&2; exit 1; }

NAME="${NAME:-trading-postgres}"
DB="${DB:-trading}"

if ! "${ENGINE}" container inspect "${NAME}" >/dev/null 2>&1; then
    echo "container ${NAME} is not running — start it with scripts/dev-stack.sh up -d postgres" >&2
    exit 1
fi

tables=("$@")
if [[ ${#tables[@]} -eq 0 ]]; then
    tables=(trades internal_trades order_revisions)
fi

for t in "${tables[@]}"; do
    echo "== execution.${t} =="
    "${ENGINE}" exec "${NAME}" psql --username postgres --dbname "${DB}" \
        --command "select * from execution.${t} order by recorded_at desc" \
        || echo "(could not read execution.${t} — has the gateway run with a DB yet?)"
    echo
done
