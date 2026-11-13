# PowerBot Gateway — Datadog dashboard (Bits AI prompt)

Paste the **prompt** below into Datadog **Bits AI** ("Ask Bits AI" → dashboard generation) to
generate the dashboard. It is written against the `powerbot.*` metrics the gateway emits over
DogStatsD (see `crates/powerbot-gateway/src/metrics.rs` and `docs/powerbot-gateway/design.md`, "Observability"). **Dashboard
only — no monitors/alerts** for now.

The metric reference after the prompt is the ground truth; if Bits AI needs a metric that isn't
listed there, it doesn't exist yet — don't invent one.

---

## Prompt for Bits AI

> Create a **dashboard** (not a monitor — do not create any alerts) named **"PowerBot Gateway"**
> describing the health of the `powerbot-gateway` service. All metrics are namespaced `powerbot.*`
> and are tagged `service:powerbot-gateway` and `version:<git-sha>`; scope every widget to
> `$service` and `$env` and let `$version`, `$host`, `$topic`, and `$feed` be template variables
> (defaults: `service:powerbot-gateway`, `env:*`, `version:*`, `host:*`, `topic:*`, `feed:*`).
>
> Lay it out as four labelled groups, top to bottom:
>
> **1. Health (overview row)** — small query-value / status widgets:
> - **Connected**: `powerbot.connected` (gauge, `min` aggregation over the interval). Show as a
>   query value with conditional formatting: green when = 1, red when < 1. Title "PowerBot session
>   up".
> - **Reconnects (last 4h)**: `sum:powerbot.reconnects{$service,$env}.as_count()`, query value.
> - **AddOrder p95**: `powerbot.order.rest_latency_ms.95percentile{$service,$env,outcome:success}`,
>   query value, unit milliseconds.
> - **Decode failures (last 4h)**: `sum:powerbot.decode.failures{$service,$env}.as_count()`, query
>   value.
> - **Order reject rate**: ratio of
>   `sum:powerbot.order.rest_latency_ms.count{outcome:rejected}` to
>   `sum:powerbot.order.rest_latency_ms.count{*}`, query value as a percentage.
> - **Market OK**: `max:powerbot.market.state{$service,$env,state:ok}` (gauge, `min` aggregation
>   over the interval). Query value with conditional formatting: green when = 1, red when < 1.
>   Title "Market status OK".
>
> **2. Market status (REST `getStatus` poll)**:
> - Timeseries **"Market state (one-hot)"**:
>   `max:powerbot.market.state{$service,$env} by {state}`, 0–1 y-axis — the current state's
>   series sits at 1, the others at 0 (states: `ok`, `warning`, `failure`, `logged_out`).
> - Timeseries **"Order-action quota consumed"**:
>   `max:powerbot.market.order_action_quota{$service,$env}`, 0–1 y-axis (1.0 = quota exhausted;
>   add a marker line at 0.8).
> - Timeseries **"Market poll failures"**:
>   `sum:powerbot.market.poll_failures{$service,$env}.as_count()`.
>
> **3. Session & connectivity**:
> - Timeseries **"Session connected"**: `powerbot.connected{$service,$env}` (`min`), 0–1 y-axis.
> - Stacked-bar timeseries **"Reconnects by reason"**:
>   `sum:powerbot.reconnects{$service,$env} by {reason}.as_count()`.
> - Top-list **"Reconnect reasons"**: same metric, grouped by `reason`.
>
> **4. Feed ingest (STOMP push feeds)**:
> - Timeseries **"Frames received / sec by topic"**:
>   `sum:powerbot.websocket.frames_received{$service,$env} by {topic}.as_rate()`.
> - Timeseries **"Bytes received / sec by topic"**:
>   `sum:powerbot.websocket.bytes_received{$service,$env} by {topic}.as_rate()`, unit bytes.
> - Stacked-bar timeseries **"Sequence gaps by topic"**:
>   `sum:powerbot.sequence.gaps{$service,$env} by {topic}.as_count()`.
> - Timeseries **"Sequence rebaselines by topic"**:
>   `sum:powerbot.sequence.rebaselines{$service,$env} by {topic}.as_count()`.
> - Timeseries **"Decode failures"**:
>   `sum:powerbot.decode.failures{$service,$env}.as_count()`.
>
> **5. Order entry (write path)**:
> - Timeseries **"AddOrder latency"** overlaying
>   `powerbot.order.rest_latency_ms.median`, `.95percentile`, and `.max`
>   `{$service,$env,outcome:success}`, unit milliseconds.
> - Stacked-bar timeseries **"AddOrder outcomes / sec"**:
>   `sum:powerbot.order.rest_latency_ms.count{$service,$env} by {outcome}.as_rate()`
>   (outcomes: `success`, `rejected`, `error`).
>
> **6. gRPC serving (read path)**:
> - Timeseries **"Live subscribers by feed"**:
>   `sum:powerbot.grpc.subscribers{$service,$env} by {feed}` (gauge, feeds `own_trades`,
>   `internal_trades`, `own_orders`).
> - Stacked-bar timeseries **"Subscribers dropped by feed & reason"**:
>   `sum:powerbot.grpc.subscribers_dropped{$service,$env} by {feed,reason}.as_count()`
>   (reasons: `limit`, `lagged`).
>
> Add a dashboard description linking to the `powerbot-gateway` repo and noting that the
> `ownorders`/`owntrades` feeds — not the AddOrder response — are the source of truth for order
> state. Use the timeframe global default (past 4 hours).

---

## Metric reference (ground truth)

All series carry global tags `service:powerbot-gateway` and `version:<git-sha>` (client-side
global labels), plus whatever the shared DogStatsD agent adds (`env`, `host`, …).

### Counters

| Metric | Tags | Meaning |
|---|---|---|
| `powerbot.websocket.frames_received` | `topic` | STOMP MESSAGE application frames received |
| `powerbot.websocket.bytes_received` | `topic` | MESSAGE body bytes received (unit: bytes) |
| `powerbot.decode.failures` | — | Frames dropped without decoding (unknown subscription / messageClass / bad body) |
| `powerbot.sequence.gaps` | `topic` | Forward sequence jumps (missed messages) that tore the session down |
| `powerbot.sequence.rebaselines` | `topic` | Backwards/repeated sequence numbers (PowerBot restart or duplicate) |
| `powerbot.reconnects` | `reason` | Session reconnects, by the recoverable reason that ended the session |
| `powerbot.grpc.subscribers_dropped` | `feed`, `reason` | Execution-stream subscribers dropped |
| `powerbot.market.poll_failures` | — | `getStatus` REST market polls that failed (transport error or non-success status) |

### Gauges

| Metric | Tags | Meaning |
|---|---|---|
| `powerbot.connected` | — | 1 while a PowerBot STOMP session is up, 0 otherwise |
| `powerbot.grpc.subscribers` | `feed` | Live execution-stream subscribers |
| `powerbot.market.state` | `state` | REST-polled market state, one-hot: the current state's series is 1, the other three 0 |
| `powerbot.market.order_action_quota` | — | Consumed order entry/modification quota, 0.0–1.0 (only emitted on exchanges that report it) |

### Histogram

| Metric | Tags | Meaning |
|---|---|---|
| `powerbot.order.rest_latency_ms` | `outcome` | AddOrder REST round-trip latency (unit: ms) |

The histogram is emitted over DogStatsD, so Datadog stores it as **sub-metrics**. With the default
agent histogram aggregates that is `powerbot.order.rest_latency_ms.{avg,count,median,max,95percentile}` —
use `.95percentile` for p95 and `.count` for throughput / outcome rates. (If your agent is
configured to treat it as a *distribution* instead, query the base metric with the `p95`/`p99`
aggregators.)

### Tag value enumerations

- **`topic`**: `marketstatus`, `contracts`, `ownorders`, `owntrades` (`unknown` for an
  unrecognized subscription).
- **`reason`** (reconnects): `peer_close`, `stale`, `ws`, `io`, `handshake`, `stomp_error`,
  `connected_timeout`, `sequence_gap` (`protocol` / `transport` / `other` exist but are not
  expected — they classify errors that shouldn't reach the reconnect path).
- **`feed`**: `own_trades`, `internal_trades`, `own_orders`.
- **`reason`** (subscribers_dropped): `limit` (subscriber cap reached), `lagged` (fell behind the
  broadcast).
- **`outcome`** (order latency): `success`, `rejected` (PowerBot 4xx), `error` (transport / 5xx /
  empty response).
- **`state`** (market state): `ok`, `warning`, `failure`, `logged_out`.
