"""The strategy module contract — what the cradle loads at the path its `python` config key
names (src/py_strategy.rs is the shim that marshals across). This docstring is the whole
contract: the real strategy repo delivers a module defining exactly these two functions, and
the implementation below is the reference mock oscillator standing in until it does.

A strategy module must define exactly two functions:

  strategy_create(params) -> state
      Called once at boot with the cradle's tuning:
        params = {"amplitude_kw": int, "max_contracts": int, "min_time_to_delivery_ms": int}
      Return any object — it is handed back on every call, opaque to the cradle. Return
      None to refuse initialization; the cradle treats that as a fatal boot error.

  strategy_targets(state, input) -> iterable of targets
      One evaluation. The input is the full gathered state at one cradle tick — every contract
      the cradle knows of; the strategy trades what it decides to trade:
        input = {"now_ms": int, "rows": [row, ...]}
      Rows are sorted by (market_type, contract_id) — a canonical order, so a replayed input is
      identical however the cradle gathered it. Each row joins the live contract view, the
      portfolio's position, and the live book; absent parts read as the zero values:
        row = {
          "market_type": int,        # 0 local, 1 XBID
          "contract_id": int,        # venue contract id
          "delivery_start_ms": int,  # unix ms; 0 when the contract is not in the live view
          "active": bool,            # tradable in the cradle's delivery area (False when absent)
          "position_kw": int,        # actual net position, signed kW; 0 means provably flat
          "bids": [(price, quantity), ...],  # best-first; minor currency units per MWh, kW
          "asks": [(price, quantity), ...],
        }
      Each returned target is an absolute position to hold (the last write for a key wins;
      omitting a contract simply emits nothing for it this tick):
        target = {"market_type": int, "contract_id": int, "target_kw": int}

Conventions (the cradle's, restated for the strategy side):
  - Integers end to end: signed kW (buy > 0), minor currency units per MWh. No floats cross the
    boundary — internal floats are fine, returned targets must be ints.
  - Calls are serialized, never concurrent. State lives in the object strategy_create returned.
  - Be deterministic: same params, same inputs -> same targets. No clocks (input["now_ms"] is
    the only clock), no IO, no ambient randomness. The simulator depends on this.
  - The interpreter is embedded in the cradle process: stdlib only, unless the deployment
    provides packages on the module search path.
"""

import math

MARKET_XBID = 1
LOT_KW = 500
PERIOD_MS = 30 * 60 * 1000
PHASE_STRIDE = 137


def strategy_create(params):
    return dict(params)


def strategy_targets(state, input):
    # The tradable set, re-picked each tick: active XBID contracts delivering outside the
    # no-trade window, soonest-delivering first, capped.
    eligible = [
        row
        for row in input["rows"]
        if row["market_type"] == MARKET_XBID
        and row["active"]
        and row["delivery_start_ms"] - input["now_ms"] >= state["min_time_to_delivery_ms"]
    ]
    eligible.sort(key=lambda row: (row["delivery_start_ms"], row["contract_id"]))

    # A 30-minute sine per member, phase-shifted per contract so the set does not move in
    # lockstep, snapped to 500 kW lots. Venue ids run sequentially, so the raw lower digits
    # would put the whole tradable set at nearly the same phase; the stride (coprime with
    # 1000) spreads neighbouring ids far apart on the cycle.
    targets = []
    for row in eligible[: state["max_contracts"]]:
        phase = 2 * math.pi * (
            (input["now_ms"] % PERIOD_MS) / PERIOD_MS
            + (row["contract_id"] * PHASE_STRIDE % 1000) / 1000
        )
        raw_kw = state["amplitude_kw"] * math.sin(phase)
        targets.append(
            {
                "market_type": row["market_type"],
                "contract_id": row["contract_id"],
                "target_kw": LOT_KW * round(raw_kw / LOT_KW),
            }
        )
    return targets
