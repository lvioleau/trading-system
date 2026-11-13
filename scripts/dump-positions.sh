#!/usr/bin/env bash
# Dump the position-service ledger for local inspection — a debugging helper only, read-only.
#
# The `positions` schema has one physical table, `positions.trades`: the append-only log the
# service boot-folds into its in-memory positions (see storage.rs). So this prints two views:
#   1. TRADES   — the raw log in `seq` (fold) order, every appended row.
#   2. POSITIONS — the fold: net signed quantity and notional per portfolio + contract,
#                  suppressing the flat (net-zero) ones, which is what the service serves.
#
# There's no host psql, so psql runs inside the dev stack's Postgres container over its local
# socket. Point it at another database with DATABASE_URL instead.
#
# Override via environment:
#   ENGINE    container engine             (default: podman — what compose.yaml runs under)
#   NAME      Postgres container name      (default: trading-postgres, pinned in compose.yaml)
#   DB        database to query            (default: trading)
#   DATABASE_URL  full connection string; when set, psql -d "$DATABASE_URL" runs in the
#                 container instead of the local-socket default (e.g. to hit a remote db)
#
# Usage:
#   scripts/dump-positions.sh

set -euo pipefail

ENGINE="${ENGINE:-podman}"
command -v "${ENGINE}" >/dev/null || { echo "${ENGINE} not found on PATH" >&2; exit 1; }

NAME="${NAME:-trading-postgres}"
DB="${DB:-trading}"

if ! "${ENGINE}" container inspect "${NAME}" >/dev/null 2>&1; then
    echo "container ${NAME} is not running; start it with scripts/dev-stack.sh up -d postgres" >&2
    exit 1
fi

# When DATABASE_URL is set, connect through it; otherwise use the container's local socket.
if [[ -n "${DATABASE_URL:-}" ]]; then
    psql=(psql -d "${DATABASE_URL}")
else
    psql=(psql --username postgres --dbname "${DB}")
fi

# The whole log, fold order. See storage.rs: quantity is signed kW (buy > 0), price is minor
# currency units per MWh.
"${ENGINE}" exec --interactive "${NAME}" "${psql[@]}" <<'SQL'
\echo '== TRADES (positions.trades, grouped by position) =='
select seq, portfolio, market_type, venue_contract_id as contract,
       quantity, price, ccy, venue_trade_id, source, executed_at
from positions.trades
order by portfolio, market_type, contract, executed_at, seq;

\echo ''
\echo '== POSITIONS (net fold, flat suppressed) =='
select portfolio, market_type, venue_contract_id as contract, ccy,
       sum(quantity)                          as net_qty_kw,
       sum(quantity * price)                  as notional,
       count(*)                               as trades
from positions.trades
group by portfolio, market_type, venue_contract_id, ccy
having sum(quantity) <> 0
order by portfolio, market_type, contract;
SQL
