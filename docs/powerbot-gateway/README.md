# powerbot-gateway

The Flow trading system's bridge to [PowerBot](https://www.powerbot-trading.com)
(Volue's hosted webservice for algorithmic intraday power trading): order entry
over REST, execution reporting from PowerBot's STOMP-over-wss push feeds, a
durable transaction record in Postgres, and position reporting to the Position
Service. See [design.md](design.md) for the full design.

## Status

Implemented:

- `powerbot-models` crate: message structs generated from the vendored
  `api/openapi.yaml` (only the models the gateway uses; regenerate with
  `scripts/regen-models.sh` after bumping the spec).
- WebSocket transport (sync tungstenite + rustls): `api_key` auth on the
  upgrade, STOMP heart-beat keepalive, staleness detection.
- STOMP session (codec via the `stomp-parser` crate): CONNECT/SUBSCRIBE/MESSAGE/ERROR.
- Session driver: subscribe to `marketstatus`, `contracts`, `ownorders`,
  `owntrades`; per-topic sequence-number gap detection; reconnect with
  exponential backoff; clean SIGINT/SIGTERM shutdown.
- REST client (`reqwest` + rustls): order entry (`addOrders`) plus paginated
  snapshots (`getOwnOrders`, `getTrades`, `getInternalTrades`), `api_key` auth.
- Reconciliation (`reconcile.rs`): a tokio task that seeds the stores from REST
  at startup, then re-sweeps and evicts past-delivery entries on an interval —
  the stores' self-correction for what the lossy websocket drops.
- Durable record (`storage.rs`, Postgres via sqlx): trades / internal trades /
  order revisions in an `execution` schema (shared `trading` DB, contract-service
  style, `search_path`-isolated migrations; state/action/side as Postgres enum
  types). Each websocket batch is persisted live (a bounded queue drained by a
  task, non-blocking on the ws thread), and each reconciliation sweep persists the
  full REST window as the backstop. Write-only: the record is never read back into
  the stores — on restart they rebuild from the REST sweep (window sized to the
  intraday horizon) plus the live feed. Required — the gateway connects Postgres at
  startup and refuses to run without it (`[gateway.storage] database_url`).
- In-memory execution stores (`stores.rs`): external trades (by `trade_id`),
  internal trades (by `internal_trade_id`), own orders (by `order_id`) — guarded
  upsert, never-regress, a monotonic `seq`, and future-delivery eviction. Each is
  paired with its broadcast in a `Channel` (`grpc/feed.rs`, the nordpool shape);
  the driver applies each decoded batch then broadcasts the delta.
- gRPC server (tonic): `PowerbotOrderService.AddOrder` places a single IOC
  order via REST, gated on the websocket-derived market status and on contract
  tradability (a REST-swept, feed-updated contract store; rejects only on
  definitive knowledge — unknown contracts forward, PowerBot decides) (IOC is
  fill-or-kill, so there is no modify/cancel — no resting order to manage);
  `PowerbotExecutionService.SubscribeOwnTrades` / `SubscribeOwnOrders` stream
  seed-then-deltas from the stores (snapshot first, then live deltas deduped by
  `seq`; slow subscribers dropped); health + reflection. Internal trades are not
  exposed on a stream — they are recorded durably and, on a shadow gateway,
  normalized onto own-trades (on a live gateway a self-match anomaly, alarmed).
- `main`: single process, two runtimes — the sync ws driver on a dedicated
  thread publishing market status into a `watch`, contract changes into the
  shared `ContractStore`, and own trades/orders into the shared
  `ExecutionChannels`, and a tokio runtime serving the gRPC services.
- Metrics (`metrics.rs`, DogStatsD via `metrics-exporter-dogstatsd`): the
  `powerbot.*` surface — frames/bytes per topic, decode failures, sequence
  gaps/rebaselines, reconnects by reason, a `connected` gauge, AddOrder REST
  latency, per-feed gRPC subscriber gauge + drop counters, and a self-trade
  alarm counter (both legs ours — should never happen with IOC-only entry, so
  any occurrence is counted and logged loudly). The recorder is
  installed at boot (`[gateway.metrics] dogstatsd_addr`) and every series is
  tagged with the build's git SHA (`version:`). DogStatsD is UDP, so a local run
  with no agent drops the packets silently.

Missing (in design order):

- REST client: topic discovery via `getSubscriptionTopics`.
- An immediate re-snapshot on ws gap/reconnect (today the periodic sweep heals
  gaps within its interval).
- A market/session status stream (`SubscribeStatus`).
- Pre-trade risk checks (the `check_risk` seam is a pass-through placeholder).
- Position reporting: the Position Service subscribes to the execution stream
  and folds fills into PE (pull-based, idempotent by trade id).
- Deployment (ECS/Terraform), dashboards, and alerting on the metrics.

## Run

Toolchain comes from the devenv shell (`devenv shell`, or direnv).

Configuration is one INI file, `powerbot.ini` (path via `POWERBOT_CONFIG`, default
`./powerbot.ini`) — the single source of truth for every value (there is no
`Default` in the config and no operational constant in the code). Values may embed
`${VAR}` / `${VAR:-default}`, resolved from the environment at load; secrets are
named, never stored (`api_key`, `database_url` inject `${POWERBOT_API_KEY}` /
`${POWERBOT_DATABASE_URL}`). See the shipped `powerbot.ini` for the keys and their
sections (`[gateway]`, `[gateway.storage]`, `[server]`).

```shell
POWERBOT_API_KEY=... \
POWERBOT_DATABASE_URL=postgres://postgres:postgres@localhost:5433/trading \
POWERBOT_PORTFOLIO_ID=... \
cargo run -p powerbot-gateway
```

The durable record is **required**: `[gateway.storage] database_url` must resolve
(e.g. the shared `trading` DB from `scripts/dev-stack.sh`) or the
gateway refuses to start. The gRPC listen address defaults to `127.0.0.1:50052`.
Logging is JSON to stdout, level from `RUST_LOG` (default
`info,powerbot_gateway=debug`). The gRPC surface is discoverable over reflection
(e.g. `grpcurl -plaintext $ADDR list`).

## Tester TUI

`crates/powerbot-tester` (`powerbot-tester`) is a ratatui terminal UI for exercising the
gateway by hand. It is a gRPC client of two servers: the gateway itself (order
entry + the own-order/own-trade feeds) and the nordpool connector's stream-server
(contract reference data + the depth feed). Pick a live contract, see its best
bid/ask, fire an order, and watch the order and trade streams react.

```shell
scripts/run-tester.sh       # endpoints, area and portfolios from the dev stack's variables
# or run cargo directly and point it anywhere:
cargo run -p powerbot-tester -- --gateway 127.0.0.1:50052 --connector 127.0.0.1:50051
```

Flags (each with a `TESTER_*` env var fallback): `--gateway`, `--connector`,
`--position-service` (Position Service endpoint, default `http://127.0.0.1:50053`),
`--portfolio` (default `exec-1`), `--positions` (comma-separated portfolios to
subscribe for net positions, default `strat-batt-1,strat-batt-2,exec-1`), `--area`
(connector delivery-area id, default `40`), `--area-eic` (delivery-area EIC put
on the order; default `10YFR-RTE------C`), and `--exchange` (default `nordpool`).

Each `--positions` portfolio gets its own `flow.v1.PositionService`
`Subscribe` stream (from the Position Service); the tester keeps each
`(portfolio, contract)` net at its highest revision and shows one column per
portfolio in the contract list (net MW, long green / short red). The Position
Service protos share the `flow.v1` package with the gateway proto, so
they compile into their own out_dir / module (see the tester's build.rs).

Two namespace mismatches the tester bridges between the connector and PowerBot:

- **Delivery area.** The connector keys on a numeric area id (`40` = France/RTE);
  PowerBot's order wants that area's EIC string (`10YFR-RTE------C`). These are
  unrelated namespaces, so `--area` and `--area-eic` are set independently.
- **Contract id.** PowerBot keys on a market-type-prefixed id, not the bare
  nordpool id: `NX_<id>` for XBID, `NI_<id>` for local. The tester derives it
  from the selected contract's market type and shows it in the ticket (the
  `→ NI_…` after the contract name).

Connector depth prices arrive as integers scaled ×100; the tester divides by 100
for display and for the gateway's floating-point order price.

Keys: `Tab` switches between the contract picker and the order ticket. In the
picker: `↑/↓` move, `m`/`p`/`w` cycle the market / product / window filters, `a`
toggles active-only, `Enter` selects and jumps to the ticket. In the ticket:
`↑/↓` move between fields, `←/→` (or `b`/`s`) flip side, type to edit price/qty,
`f` refills the price from the book, `Enter` sends. `Esc` or `Ctrl-C` quits.

## Tests

```shell
cargo test
```

Unit tests only, sub-second: STOMP session against a scripted fake transport,
transport against a local self-signed `wss://` server, driver session/reconnect
logic against scripted STOMP frames.

## IDE setup (Rust Rover)

Set
- `Toolchain location` to `PATH_TO_REPO/.devenv/profile/bin`.
- `Standard Library` to the output of `echo $RUST_SRC_PATH`.
