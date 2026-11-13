#!/usr/bin/env bash
# Regenerate crates/powerbot-models/src/models/ from the PowerBot OpenAPI spec.
#
# The spec is not committed: download it from your PowerBot instance and save it
# as api/openapi.yaml (gitignored), or point POWERBOT_OPENAPI_SPEC at it.
#
# Models only, and only the ones the gateway uses (KEEP below): the HTTP client
# code openapi-generator would emit is discarded — the gateway's REST surface is
# small and hand-written against these types (see docs/design.md). Generated
# code is committed; rerun this after bumping the vendored spec and review the
# diff.
#
# Requires openapi-generator-cli.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
spec="${POWERBOT_OPENAPI_SPEC:-$repo_root/api/openapi.yaml}"
[[ -f "$spec" ]] || { echo "PowerBot OpenAPI spec not found at $spec (see header)" >&2; exit 1; }
models_dir="$repo_root/crates/powerbot-models/src/models"

# The models the gateway uses. The generator does NOT pull in referenced models
# automatically, so this list is the transitive closure of the roots; if a spec
# bump adds a reference, the cargo check below fails and the missing model name
# is what the compiler reports.
KEEP=(
  # STOMP push payloads (the messageClass discriminators we subscribe to)
  OwnOrderChanges
  OwnTradeChanges
  MarketStatusChangedEvent
  ContractChangedEvent
  # ... and what they carry
  OwnOrder
  Trade
  InternalTrade
  # REST: order entry, own-order/trade snapshots, topic discovery, market status,
  # contract discovery (the order gate's contract-store seed/sweep)
  OrderEntry
  OrderModify
  Orders
  SubscriptionEndpoint
  MarketStatus
  ContractItem
  ErrorResponse
  # Referenced by the above
  AggressorIndicator
  AtcStatus
  ContractReference
  ContractType
  DeliveryAreaState
  Error
  ErrorCode
  Exchange
  ExchangeCashLimit
  MarketOptions
  MarketState
  OrderAction
  OrderBookEntry
  OrderExecutionRestriction
  OrderSide
  OrderState
  OrderType
  TradeState
  ValidityRestriction
)

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

# `models=<colon-separated list>` restricts generation to those structs;
# `supportingFiles` is needed for src/models/mod.rs, which the generator builds
# from the same filtered list (the extra top-level supporting files land in
# $tmp and are never copied out).
keep_list="$(IFS=:; echo "${KEEP[*]}")"
openapi-generator-cli generate \
  --input-spec "$spec" \
  --generator-name rust \
  --output "$tmp" \
  --global-property "models=$keep_list,supportingFiles,modelDocs=false,modelTests=false" \
  --additional-properties=avoidBoxedModels=true

rm -rf "$models_dir"
cp -r "$tmp/src/models" "$models_dir"

cargo fmt --manifest-path "$repo_root/Cargo.toml" -p powerbot-models
cargo check --manifest-path "$repo_root/Cargo.toml" -p powerbot-models

echo
echo "Regenerated $models_dir from $spec (${#KEEP[@]} models)"
