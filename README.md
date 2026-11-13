# trading-system

**An algorithmic trading platform for the European intraday electricity market, written in Rust.**
It turns a trading strategy's decisions into orders on the exchange, and keeps an exact,
auditable record of every contract, trade and position along the way.

> **About this repository.** I designed and wrote this system in 2026 while working with an
> energy-trading startup. This copy contains the application code only: company deployment
> infrastructure (Terraform, CI/CD) and internal configuration have been removed, and the commit
> history has been squashed.

| | |
|---|---|
| **Scale** | ~33,000 lines of Rust in 10 crates (excluding generated code) · ~400 automated tests · 6 services |
| **Techniques** | Service-oriented architecture over gRPC, event streaming, PostgreSQL, optimistic concurrency, idempotent ledgers, embedded Python (pyo3) |
| **Companion project** | [`nordpool-connector`](https://github.com/lvioleau/nordpool-connector) — the real-time market-data system this platform consumes |

## Background

On the continuous intraday electricity market, contracts for each 15-minute or hourly delivery
period trade right up to shortly before delivery (see the
[connector's README](https://github.com/lvioleau/nordpool-connector#background) for a primer).
A trading firm running, for example, a battery wants to hold the right position in each of
these contracts at each moment: so much bought, so much sold.

Trading software that does this is hard to get right for reasons that have little to do with the
strategy itself:
- orders can be partly filled, rejected, or answered ambiguously;
- network connections drop and messages arrive late or out of order;
- several components act on the same positions at the same time;
- and a single double-counted trade means the firm's books are wrong.

The goal of this platform is to let a strategy express only *what position it wants*, and to make
everything between that wish and the exchange correct, observable and recoverable.

## What I built

![Architecture](docs/trading-system-architecture.png)

<sub>Boxes with a grey double border are external systems. The auction executors and balancing
platforms on the right are the planned next stage and are not part of this repository.</sub>

The core idea is a **control loop**:

```mermaid
flowchart LR
    S[Strategy] -- "target position" --> C[Crosser]
    C -- "transfer, priced at the live mid" --> P[(Position Service)]
    P -- "execution gap" --> X[ID Executor]
    X -- "IOC orders" --> G[PowerBot Gateway]
    G -- "orders" --> V[(Exchange via PowerBot)]
    V -- "fills" --> G
    G -- "fills" --> P
```

1. The **strategy** publishes the absolute position it wants in each contract. It is a Python
   module embedded through a Rust host, so strategy authors never touch the plumbing.
2. The **Crosser** compares the target with the actual position and books an internal transfer.
   The transfer moves the strategy's portfolio to its target and hands the difference to an
   *execution portfolio*, priced at the live market mid.
3. The **ID Executor** sees the execution portfolio's open position and works it down to zero by
   sending immediate-or-cancel (IOC) orders to the exchange.
4. The **PowerBot Gateway** is the connection to the exchange's trading interface, provided by the
   vendor PowerBot. It submits the orders and relays the resulting fills.
5. The **Position Service** books those fills, and the loop closes.

Two supporting services hold the shared facts:
- The **Contract Service** is the system of record for every tradable contract and its
  lifecycle.
- The **Position Service** is the system of record for every trade and position: an append-only
  ledger in PostgreSQL.

## Key design decisions

**Separate *what* to trade from *how* to trade it.** The strategy only says "I want to be
+10 MW in this contract". Execution, order management and failure handling all live
downstream. Strategy research can then move quickly without risking the trading machinery, and
the same strategy code can run against a replay of historical data instead of the live market:
its inputs are defined as interfaces that a simulator can implement.

**Reconcile positions, not trades.** The Crosser never replays a list of intended trades. Each
time, it computes the difference between where a portfolio *is* and where it *should be*. A lost,
duplicated or reordered message therefore cannot accumulate into an error. The next update
simply corrects it. This idea of a *self-healing control loop* runs through the whole system.

**Never double-count a trade.**
- The ledger is append-only, and every trade has a deterministic ID derived from the exchange's
  own trade ID. Replaying the same fill after a reconnect is a harmless no-op.
- Internal transfers are zero-sum: both sides are written in one database transaction.
- Transfers are guarded by optimistic concurrency, so two writers can never silently overwrite
  each other.
- Quantities and prices are integers end to end (kW, cents per MWh), never floating point.

**Durable before visible.** A service writes a change to PostgreSQL before it updates its
in-memory state or announces the change. A crash can therefore never leave other services
believing something the database does not record. Reads are served entirely from memory, rebuilt
from the database at startup, so the database is never on the hot path.

**Trust the exchange's event stream, not its replies.** The exchange's direct reply to an order is
treated as advisory. The source of truth is the stream of order and trade events. The gateway
detects sequence gaps in that stream and periodically reconciles against the exchange's REST API
to repair anything a dropped connection missed.

**Safe by absence.** When information is missing the system holds still. It does not guess:
- With no reliable market price (an empty or crossed order book), the Crosser does not price a
  transfer.
- With no confirmed position, the strategy does not run.
- An unknown market area or a misconfigured portfolio stops a service at startup, not in the
  middle of trading.

**One owner per piece of data.** Each service owns one database schema and is its only writer.
Every other service reads that data through the owner's gRPC stream, which sends a snapshot
followed by live updates. Ownership stays clear, and every component can restart independently.

## Status

| Component | State |
|---|---|
| Contract Service, Position Service | Built and tested |
| PowerBot Gateway | Order entry, execution feeds and reconciliation built and tested; production deployment pending |
| Crosser, strategy host with a reference strategy | Built; end-to-end tests of the target → transfer loop |
| ID Executor | Core built and unit-tested; end-to-end test of the assembled loop in progress |
| Auction executors (day-ahead, balancing markets) | Designed (diagram above), not started |

The design documents in [`docs/`](docs/) describe each component's invariants and implementation
plan, including where the implementation deliberately departs from the original design and why.

## Correctness and testing

About 400 automated tests:
- the pure decision logic (crossing, execution, ledger) is unit-tested with no I/O;
- the services are tested black-box over real gRPC;
- the Crosser's tests run the whole loop: strategy, a scripted order book and an in-memory
  Position Service, including restart and broken-book scenarios;
- the gateway is tested against scripted exchange messages and a local TLS server;
- storage tests run against a real PostgreSQL database.

## Where to look first

| If you want to see… | Look at |
|---|---|
| The position-reconciliation logic | [`crates/crosser/src/crosser.rs`](crates/crosser/src/crosser.rs) |
| The execution decision | [`crates/id-executor/src/executor.rs`](crates/id-executor/src/executor.rs) |
| The idempotent trade ledger | [`crates/position-service/src/ledger.rs`](crates/position-service/src/ledger.rs) |
| Exchange connectivity and recovery | [`crates/powerbot-gateway/src/driver.rs`](crates/powerbot-gateway/src/driver.rs), [`reconcile.rs`](crates/powerbot-gateway/src/reconcile.rs) |
| The strategy plug-in boundary | [`crates/strategy/py/strategy.py`](crates/strategy/py/strategy.py), [`crates/strategy/src/py_strategy.rs`](crates/strategy/src/py_strategy.rs) |
| Design documents | [`docs/crosser-implementation.md`](docs/crosser-implementation.md), [`docs/position-service-implementation.md`](docs/position-service-implementation.md), [`docs/powerbot-gateway/design.md`](docs/powerbot-gateway/design.md) |

---

# Technical reference

The rest of this document is the operational reference for the individual services.

## Components

First component: the **Contract Service** — the system of record for contract reference data. It
syncs the connector's contract feed into Postgres (the durable record, boot-folded into an
in-memory index that answers every read), keeps full history (including expired contracts the
live feed no longer carries), and serves it over gRPC: a seed-then-deltas subscription carrying
the live set plus a unary delivery-window query over the full history.

Second component: the **Position Service** — the system of record for portfolio positions. It
appends an idempotent priced trade ledger to Postgres (transfers via its `Transfer` RPC, fills
folded from the configured order gateways' execution streams) and serves the in-memory
position projection over gRPC: atomic zero-sum transfers with optimistic concurrency, plus a
snapshot-then-updates `Subscribe`. See `docs/position-service-implementation.md`.

Third component: the **PowerBot Gateway** — the bridge to
[PowerBot](https://www.powerbot-trading.com) (Volue's hosted intraday power-trading
webservice): IOC order entry over REST, execution reporting relayed from PowerBot's
STOMP-over-wss push feeds as seed-then-deltas gRPC streams (which the Position Service folds
into positions), and a durable transaction record in Postgres (schema `execution`). Comes with
`powerbot-tester`, a ratatui TUI for exercising it by hand. See `docs/powerbot-gateway/`
(README, design.md).

Fourth component: the **Crosser** — the seam between strategy and execution on the continuous
intraday path. It subscribes to the Strategy Layer's stream of absolute **target positions**, the
connector's depth feed (for the live top-of-book mid), and the Position Service's actual positions,
then issues zero-sum `Transfer`s that move each strategy portfolio to its target and inject the gap
into the mapped execution portfolio (priced at the live mid, OCC-guarded). It reconciles
**positions, not trades** — so a dropped or reordered target self-heals on the next one — and is a
stateless worker (no Postgres; its state is a projection of its three feeds, rebuilt on restart).
See `docs/crosser-implementation.md`.

Alongside it, the mock **Strategy** (`crates/strategy`) stands in for the real Strategy Layer until
that lands: this repo owns the `flow.v1.StrategyService` proto (the `SubscribeTargets` →
`TargetRecord` feed) for now, and the mock is its reference server and the crosser's test double. It
subscribes to the Contract Service, maintains a configured-size set of tradable contracts (chosen by
the program — active, not near expiry — and refreshed as they roll off), and each tick emits an
**oscillating target** per contract: a 30-minute sine of a configured amplitude, quantized to
500 kW lots and phase-shifted per contract so they don't all move together. Subscriptions are scoped
to the requested portfolios and rejected (`INVALID_ARGUMENT`) for any it does not produce, so a
crosser/strategy config mismatch fails fast at boot. Stateless, needs no Postgres.

## Layout

```
proto/flow/v1/                  this repo's gRPC APIs
proto/nordpool/marketdata/v1/   vendored connector feed protos (owned by nordpool-connector)
crates/contract-service/                the Contract Service binary: feed client + Postgres storage + in-memory store + gRPC API
crates/contract-service/migrations/     sqlx migrations for the contract tables (schema `reference`)
crates/position-service/                the Position Service binary: gateway feed clients + trade-log storage + in-memory ledger + gRPC API
crates/position-service/migrations/     sqlx migrations for the trade ledger (schema `positions`)
crates/powerbot-gateway/                the PowerBot Gateway binary: PowerBot REST/websocket bridge + execution storage + gRPC API
crates/powerbot-gateway/migrations/     sqlx migrations for the execution record (schema `execution`)
crates/powerbot-models/                 PowerBot message structs generated from PowerBot's OpenAPI spec (scripts/regen-models.sh)
crates/powerbot-tester/                 a ratatui TUI gRPC client for exercising the gateway by hand
crates/strategy/                        the mock Strategy binary: contract-service feed + tradable-set logic + StrategyService gRPC API (the crosser's test double)
crates/crosser/                         the Crosser binary: strategy + depth + position feeds → reconciles strategy targets against actual positions into transfers
docs/powerbot-gateway/                  the gateway's component docs (README, design.md, observability)
```

## Build, test, run

Prerequisites: a Rust toolchain (pinned by `rust-toolchain.toml`), `protoc`, and podman (or Docker)
for the dev stack. The services need
a Postgres for their durable tables — the dev stack's `postgres` service (port 5433) is what
both the config defaults and the tests expect:

```sh
./scripts/dev-stack.sh up -d postgres   # start just the database
export DATABASE_URL=postgres://postgres:postgres@localhost:5433/trading
cargo check
cargo test --workspace      # the storage integration tests skip unless DATABASE_URL is set
cargo run -p contract-service
cargo run -p position-service
cargo run -p powerbot-gateway # needs POWERBOT_* env (see docs/powerbot-gateway/README.md)
cargo run -p strategy         # the mock Strategy (stateless; needs no Postgres)
cargo run -p crosser          # the Crosser (stateless; needs no Postgres)
cargo run -p powerbot-tester  # the gateway's TUI test client
```

Configuration comes from `contract-service.ini` / `position-service.ini` / `powerbot.ini` /
`strategy.ini` / `crosser.ini` (paths overridable via `CONTRACT_SERVICE_CONFIG` /
`POSITION_SERVICE_CONFIG` / `POWERBOT_CONFIG` / `STRATEGY_CONFIG` / `CROSSER_CONFIG`), same scheme
as the connector's `nordpool.ini`: values
may embed `${VAR}` / `${VAR:-default}` references resolved from the environment at load time.
The checked-in files default to host-local endpoints; override per-run via the referenced
variables, e.g. `DELIVERY_AREA_IDS=40,41 cargo run -p contract-service`.

The service binaries also run as containers, from one image carrying all of them (the
`Dockerfile`; the container command names the binary to start — there is no default). That is
what the dev stack runs: `compose.yaml`, driven by `scripts/dev-stack.sh` on `podman compose`.

```sh
./scripts/dev-stack.sh                     # build what changed and run the whole stack
./scripts/dev-stack.sh up -d --build       # ...detached
./scripts/dev-stack.sh logs -f id-executor # follow one service
./scripts/dev-stack.sh down                # stop (the Postgres volume is kept; -v wipes it)
```

Postgres and the six services share one compose network and address each other by service name
(`contract-service:50054`, `position-service:50053`, `powerbot-gateway:50052`,
`strategy:50071`), identically on the macOS podman machine and on Linux; every port is also
published to the host, so `grpcurl`, `grpcui`, `scripts/run-tester.sh` and `cargo test` work
against the stack unchanged.

The `nordpool-connector` is **not** part of this stack — it lives in its own repo and its own
compose stack. Everything that subscribes to it (contract-service, strategy, crosser,
id-executor) dials `CONNECTOR_HOST`, which defaults to `host.containers.internal`: whatever
serves `:50051` on the host, be that the connector's stack or a native `cargo run`.

The gateway's `POWERBOT_*` configuration — including `POWERBOT_API_KEY`, the stack's only
secret — comes from `gateway.shadow.env` (gitignored; override the path with `ENV_FILE`), the
same file the old `run-gateway.sh` launcher read. A shell variable wins over the file, and the
file wins over `compose.yaml`'s defaults. Everything else is endpoint configuration —
`compose.yaml`'s header lists the variables (delivery area, portfolios, ports, log level).

The service subscribes to the connector's `SubscribeContracts` filtered to the configured
delivery areas (reconnecting with backoff) and applies every update durable-before-visible:
upsert into Postgres (`reference.contracts` / `reference.contract_areas`, key `database_url`;
migrations run at boot), then into the shared contract store, then broadcast — each step
guarded by venue `updated_at` so replays and re-seeds never regress state. At boot the whole
table is folded back into the store, so history survives restarts; the DB is never on the
read path.

It serves the store as `flow.v1.ContractService` (default `0.0.0.0:50052`, key
`listen_addr`): `SubscribeContracts` streams a per-area seed then each changed batch — the
live set only, contracts whose delivery window has passed are excluded — and `ListContracts`
answers delivery-window overlap queries over the full history, optionally narrowed to one
contract id. `FindContracts` is its containment sibling: contracts whose whole delivery window
lies between the given bounds (exact bounds find exactly that window's contracts), optionally
narrowed by contract id, delivery area, and/or product type. `GetContractHistory` returns one
contract's record plus its recorded lifecycle
transitions (the append-only `reference.contract_state_changes` table — the one query answered
from Postgres rather than memory). The endpoint also exposes gRPC server reflection and `grpc.health.v1`
(the connector stream-server conventions), so it can be explored interactively:

```sh
grpcurl -plaintext localhost:50052 list
grpcurl -plaintext -d '{"delivery_area_ids": [40], "delivery_start_ms": 0, "delivery_end_ms": 1900000000000}' \
  localhost:50052 flow.v1.ContractService/ListContracts
```
