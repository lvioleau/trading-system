#!/usr/bin/env bash
# Run the powerbot-tester TUI natively (cargo run — a TUI wants the host terminal, not a
# container), against the dev stack's published ports. It is the one component outside
# compose.yaml, so this script derives its settings from the same variables the stack reads:
# start the stack with an override and run the tester in the same shell, and the two agree.
#
#   scripts/dev-stack.sh up -d                    # stack on the default ports/area
#   scripts/run-tester.sh                         # tester dialing exactly those
#   DELIVERY_AREA_EIC=10YDK-1--------W scripts/dev-stack.sh up -d
#   DELIVERY_AREA_EIC=10YDK-1--------W scripts/run-tester.sh   # ...same area
#
# Note the connector is not part of the stack (see compose.yaml): CONNECTOR_PORT is where it
# serves on the host, its own stack's published port.
#
# Override via environment:
#   GATEWAY_PORT / CONNECTOR_PORT / POSITION_SERVICE_PORT
#                            service ports, the same variables compose.yaml reads
#                                                  (default: 50052 / 50051 / 50053)
#   DELIVERY_AREA_EIC        delivery area          (default: the stack's 10YFR-RTE------C)
#   EXCHANGE                 AddOrder exchange      (default: nordpool)
#   EXECUTION_PORTFOLIO      portfolio orders are sent for
#                                                   (default: the stack's exec-1)
#   STRATEGY_PORTFOLIO       the strategy portfolio; it and EXECUTION_PORTFOLIO are the
#                            position panes — the same pair position-service accepts
#                                                   (default: the stack's strat-batt-1)
#   TESTER_*                 full endpoint / parameter overrides — win over everything above;
#                            see `powerbot-tester --help` for the list
#
# Anything passed to the script is appended as tester flags (they win over env), e.g.:
#   scripts/run-tester.sh
#   scripts/run-tester.sh --portfolio exec-1 --positions exec-1

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

# An explicit TESTER_* wins over the stack variables; both fall back to the same defaults
# compose.yaml uses, so the plain invocation matches the plain stack.
export TESTER_GATEWAY_ADDR="${TESTER_GATEWAY_ADDR:-http://127.0.0.1:${GATEWAY_PORT:-50052}}"
export TESTER_CONNECTOR_ADDR="${TESTER_CONNECTOR_ADDR:-http://127.0.0.1:${CONNECTOR_PORT:-50051}}"
export TESTER_POSITION_ADDR="${TESTER_POSITION_ADDR:-http://127.0.0.1:${POSITION_SERVICE_PORT:-50053}}"
export TESTER_DELIVERY_AREA_EIC="${TESTER_DELIVERY_AREA_EIC:-${DELIVERY_AREA_EIC:-10YFR-RTE------C}}"
export TESTER_EXCHANGE="${TESTER_EXCHANGE:-${EXCHANGE:-nordpool}}"
export TESTER_PORTFOLIO_ID="${TESTER_PORTFOLIO_ID:-${EXECUTION_PORTFOLIO:-exec-1}}"
# The stack's two portfolios, in the order the crosser moves position: strategy then execution.
# An unconfigured name is not an error on Subscribe — it just seeds empty and never updates —
# so this only decides which panes carry data.
export TESTER_POSITION_PORTFOLIOS="${TESTER_POSITION_PORTFOLIOS:-${STRATEGY_PORTFOLIO:-strat-batt-1},${EXECUTION_PORTFOLIO:-exec-1}}"

exec cargo run --manifest-path "${REPO_ROOT}/Cargo.toml" -p powerbot-tester -- "$@"
