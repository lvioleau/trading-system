# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

The trading system: strategy-decoupled execution services for Nord Pool intraday markets. A Rust workspace with three service binaries — **contract-service** (system of record for contract reference data, synced from the `nordpool-connector` feed), **position-service** (system of record for portfolio positions, fed by transfers and order-gateway fills), and **powerbot-gateway** (the bridge to PowerBot, Volue's hosted intraday trading webservice: IOC order entry over REST, execution feeds relayed as gRPC streams, durable `execution` record) — plus the mock **strategy**, the **crosser**, and **powerbot-tester** (a ratatui TUI gRPC client for exercising the gateway by hand).

Delivery areas are identified by ENTSO-E EIC code everywhere — never by Nord Pool's numeric
area id, which only the connector knows at its own wire boundary. The `eic` crate holds the
shared `EicCode` enum — a hardcoded, append-only copy of the connector repo's
`crates/marketdata/delivery_areas.csv`, with that CSV vendored into the crate and a unit test
pinning the enum to it (no numeric ids by design): configs and requests parse
through `EicCode::from_eic` (unknown EIC = fatal, catching typos at boot), storage and wire
encode via `eic_code()`, labels via `area_code()`.

`docs/position-service-implementation.md` maps the position-service design onto this repo and records the deliberate deviations (portfolio-as-string instead of posblock, integer quantities/prices instead of `numeric`, multi-gateway config). Read it before changing position-service semantics. The gateway's authoritative design is `docs/powerbot-gateway/design.md` (kept current — read it for anything non-obvious there).

## Commands

Prerequisites: a Rust toolchain (pinned by `rust-toolchain.toml`) and `protoc`.

```sh
./scripts/dev-stack.sh           # the whole dev stack via `podman compose` on compose.yaml:
                                 # Postgres + every service, reaching each other by service name.
                                 # `up -d postgres` starts just the database (port 5433, the
                                 # storage tests' layout); `down` stops, `down -v` wipes the data.
cargo test --workspace           # storage integration tests SKIP unless DATABASE_URL is set
                                 # (export DATABASE_URL=postgres://postgres:postgres@localhost:5433/trading)
cargo test -p position-service --test position_storage   # one integration-test file
cargo run -p contract-service    # config from contract-service.ini (override: CONTRACT_SERVICE_CONFIG)
cargo run -p position-service    # config from position-service.ini (override: POSITION_SERVICE_CONFIG)
cargo run -p powerbot-gateway    # config from powerbot.ini (override: POWERBOT_CONFIG); needs
                                 # POWERBOT_API_KEY + POWERBOT_DATABASE_URL + POWERBOT_PORTFOLIO_ID
cargo run -p strategy            # mock strategy, config from strategy.ini (override: STRATEGY_CONFIG); stateless
cargo run -p crosser             # config from crosser.ini (override: CROSSER_CONFIG); stateless, no Postgres
cargo run -p powerbot-tester     # the gateway's TUI test client

cargo fmt --all -- --check                           
cargo clippy --workspace --all-targets -- -D warnings
buf lint                         # proto lint (buf.yaml; vendored nordpool protos are ignored)
scripts/regen-models.sh          # regenerate powerbot-models (needs PowerBot's spec; see script header)

```


The stack is `compose.yaml` run through `scripts/dev-stack.sh` (podman is the engine; the script pins the podman socket, which devenv's TMPDIR otherwise breaks on macOS). One image carries every binary and the container command names the service; services address each other by compose service name (`contract-service:50054`, `position-service:50053`, `powerbot-gateway:50052`, `strategy:50071`), and every port is also published to the host for native tools. The **nordpool-connector is not in this stack** — it has its own repo and compose stack; subscribers dial `CONNECTOR_HOST` (default `host.containers.internal`, i.e. whatever serves `:50051` on the host). The gateway's `POWERBOT_*` config (incl. `POWERBOT_API_KEY`, the only secret) comes from the gitignored `gateway.shadow.env` (override: `ENV_FILE`) — compose hands it to the gateway as an optional `env_file`, *and* dev-stack.sh exports its keys before compose interpolates, because a compose `environment:` entry otherwise wins over env_file and would blank the file's value. Precedence: shell > env file > compose default. Read `compose.yaml`'s header before changing the wiring.

Other helpers: `scripts/dump-positions.sh` / `dump-execution.sh` (read-only table dumps via psql-in-container), `scripts/drop-execution-schema.sh` (dev-only schema reset after a gateway migration change), `scripts/run-tester.sh` (the tester TUI natively via cargo — the one component outside compose, with its endpoints, area and portfolios derived from the same variables the stack reads, so a stack override carries). All services expose gRPC reflection and health, so `grpcurl -plaintext localhost:50052 list` works for exploration (powerbot-gateway defaults to :50052).

Config files are INI with `${VAR}` / `${VAR:-default}` references resolved from the environment at load time; override per-run via those variables, not by editing the checked-in files.

## Architecture

### Shared shape of both services

Contract-service is the template; position-service deliberately mirrors it. Same stack (tokio / tonic / sqlx / INI config), same module roles in each crate's `src/`:

| module | role |
|---|---|
| `config.rs` + `config/raw.rs` | INI + `${VAR}` resolution |
| `feed.rs` | upstream gRPC subscription client(s): seed-then-deltas, reconnect with backoff; wire→domain conversion happens here, at the boundary |
| `storage.rs` | sqlx Postgres layer; migrations run at boot |
| `store.rs` (contract) / `ledger.rs` (position) | in-memory domain state — the serving authority. `ledger.rs` is pure domain: no IO, no protobuf |
| `channel.rs` | shared state handle + update broadcast types |
| `server.rs` | the gRPC service + health + reflection |
| `main.rs` | boot: config → storage connect → boot fold from Postgres → feed tasks + server |

**The DB is never on the read path.** At boot the whole table/log is folded into memory; reads and subscriptions are answered from memory (single exception: contract-service `GetContractHistory` queries Postgres).

**Write-ordering disciplines** (the core invariants — don't reorder these):
- Contract-service feed updates are *durable-before-visible*: Postgres upsert → in-memory store → broadcast, each step guarded by venue `updated_at` so replays/re-seeds never regress state.
- Position-service `Transfer` is *commit-then-memory-then-broadcast*: the ack returns only after the Postgres commit. This is intentional — the trade log is the system of record, so memory follows the commit here.

### Position-service specifics

- The ledger is **append-only, not upsert-guarded**: idempotency comes from the trade `id` (UUID) primary key with `on conflict (id) do nothing` — a replay is a 0-rows no-op, still acked. Fill ids derive deterministically via UUID v5 from `(exchange, trade_id)`, never from the gateway's config name.
- A transfer inserts both legs in one transaction (zero-sum, same contract and price); OCC via `expected_from_revision`. Boot fold replays the log in `seq` order so revisions are deterministic across restarts.
- Portfolios are plain string identifiers listed in config; each configured gateway maps its fills to one portfolio. One feed task per gateway. A `shadow = true` gateway additionally folds its internal-trades feed as ordinary external fills.
- Quantities and prices are venue-native fixed-point integers end to end (kW, minor currency units per MWh; `bigint` in Postgres) — never floats, no scale conversion at any boundary.

### The shared `trading` database

One Postgres database, one schema per service, single writer per schema: `reference` (contract-service), `positions` (position-service), `execution` (powerbot-gateway). Each service creates its schema and pins the connection `search_path` to it before migrating, so each `_sqlx_migrations` ledger lands in its own schema; queries still schema-qualify all table names — the search_path exists for the migration ledger only.

`positions.trades` carries a composite FK into `reference.contracts` — the write-time integrity link. A fill racing the contract sync fails and the feed's reconnect-and-replay loop is the recovery mechanism (expected to heal, not a tripwire). This is also why position-service's storage tests run the contract-service migrations first (see its dev-dependency on `contract-service`).

### Protos

`proto/flow/v1/` holds this repo's APIs, `powerbot_gateway.proto` included. Everything under `proto/nordpool/` is vendored from nordpool-connector — do not edit it here; it is linted in its owning repo (buf ignores the nordpool tree).

Each crate's `build.rs` compiles its protos with tonic-build, falling back to a vendored protoc when none is on PATH (CI). `powerbot_gateway.proto` shares package `flow.v1` with `position.proto` (and imports `types.proto` for the shared `MarketType`), so position-service and powerbot-tester compile the gateway protos into a separate out_dir and include them as a distinct module (`crate::gateway` / `proto::position`) — reflection serves only each service's own API.

### PowerBot gateway specifics

Two runtimes in one process: a synchronous websocket driver on a dedicated OS thread (tungstenite + rustls carrying a STOMP session; per-topic sequence-gap detection, reconnect with backoff) bridged by channels to a tokio runtime serving the gRPC services. The in-memory stores are the serving truth; Postgres persistence (required), REST reconciliation sweeps, and a market-status poll are the lossy-websocket safety net. Two load-bearing caveats: PowerBot's synchronous order-action responses are **advisory** — the own-orders/own-trades feeds are the source of truth, never add logic that trusts the response; and TLS is **rustls throughout, no OpenSSL**. The driver deliberately mirrors the sibling `nordpool-connectors` repo's patterns — check there before adding streaming/driver code. `powerbot-models` is generated code: regenerate via `scripts/regen-models.sh`, don't hand-edit.

### Crosser / strategy specifics

The **crosser** is a client-only, stateless worker (no Postgres, no serving API): its state — the latest target, the live mid, and the actual position per contract — is a projection of three upstreams (`strategy_feed.rs`, `depth_feed.rs`, `position_client.rs`), rebuilt on restart, with `crosser.rs` the pure domain (gap → `Transfer`, no IO) and `crossing.rs` the serialized engine `run(config)` boots. Authoritative design: `docs/crosser-implementation.md`. The load-bearing invariants — don't weaken these:
- **Reconcile positions, not trades.** The crosser crosses whatever quantity reaches the *absolute* `target_quantity` from the *actual* Position Service position; it never replays the strategy's `current`/`delta` (those are logged as a drift metric only). A dropped or reordered target self-heals on the next record, which the strategy's ~30s **heartbeat** bounds — so no strategy snapshot is needed.
- **Price at the live top-of-book mid, or withhold.** Both transfer legs are priced at the connector depth feed's mid at cross time (half-to-even rounding), never the strategy's `mid_price`. A crossed or one-sided book yields no mid → the cross is held, not priced off a broken book.
- **The `Subscribe` stream is the sole writer of the actual position.** The `Transfer` ack is logged, never folded — position state moves only when the subscription echo arrives. `transfer_id` is minted fresh per attempt; safety comes from the OCC `expected_from_revision` guard, not id reuse (an `ABORTED` abandons the cross, and the next heartbeat recomputes). One crosser instance is the single writer of its strategy portfolios.

The **strategy** (`crates/strategy`) is the strategy **cradle** with a mock decision function, standing in for the real Strategy Layer: it defines and serves `flow.v1.StrategyService` (the `SubscribeTargets` → `TargetRecord` feed) — this repo owns that proto until the real layer lands, at which point both move out and the proto is vendored back (the `powerbot_gateway.proto` pattern). `driver.rs` is the cradle: each tick it gathers three upstream views — the live contract set from the Contract Service (`contract_feed.rs`, behind the `ContractSource` trait), every live book from the connector's depth stream (`depth_feed.rs`, behind `DepthSource`), and the produced portfolio's actual positions from the Position Service (`position_feed.rs`, behind `PositionSource`) — into one `StrategyInput` (now, all contract views, positions, full-depth books), runs the injected `Strategy` in **one call with all contracts**, and broadcasts each returned target, booking the live top-of-book mid. **What to trade is the strategy's decision** — the cradle pre-selects nothing and imposes no tradable set. The three source traits are the sim seam: a parquet replay implements them and the cradle doesn't change. The strategy proper is a Python module loaded at the path the required `python` config key names — the contract the real strategy repo delivers a module against; `src/py_strategy.rs` is the pyo3 shim (`PyStrategy` implements `Strategy`: flattens the input's maps into one sorted per-contract row list — canonical order, so replays are identical — calls the module's `strategy_targets`, translates back; a tick-time Python exception logs the traceback and yields no targets, `strategy_create` failures are fatal at boot). The whole module contract (dict shapes, integer conventions, the determinism rules) is the docstring of `py/strategy.py`, whose body is today's reference implementation: it picks its tradable set each tick (XBID, active, delivery ≥ the configured window out, capped, soonest-delivering first) and emits a 30-min sine per member (configured amplitude, 500 kW lots, phase-shifted per contract). `driver.rs` is strategy-agnostic — no mock logic lives there; the mock's behavior is pinned by black-box tests in `py_strategy` driving the module through the shim. The embedded interpreter (pyo3 `auto-initialize`) links libpython — the devenv shell and the Docker builder/runtime stages provide it; strategy modules get the base interpreter's stdlib only unless the deployment provides packages. Two withhold gates, safe by absence: no live position baseline withholds the whole tick (the strategy never runs blind); no live mid withholds that contract's record. Exactly **one portfolio per strategy process** (one strategy per portfolio, by design — `portfolio`, singular, in config); `server.rs` fans records out, scoped to each subscriber's requested portfolios and rejecting (`INVALID_ARGUMENT`) any it does not produce, so a crosser/strategy config mismatch fails fast at boot. `driver.rs` is pure domain (no protobuf); the feeds translate wire→domain at the boundary. Same integer encoding (kW, minor units per MWh) and INI-config stack as the services.

### Tests

Black-box integration tests live in each crate's `tests/` (feed, server, storage). The storage tests need a real Postgres (`DATABASE_URL`, the dev-stack.sh `postgres` service on :5433) and skip silently without it — CI provides one, so "all tests pass locally" without DATABASE_URL is not full coverage. The gateway's tests are unit-level and sub-second (scripted STOMP frames, a local self-signed `wss://` server) — no Postgres needed. The crosser's tests are black-box but Postgres-free too: they drive the `crates/strategy` mock and a scripted depth mock against a memory-only position-service server (end to end — first cross, the −10→−9 reconcile leg, two portfolios, withhold-on-broken-book, restart safety), plus a recording `PositionService` mock for the transfer wire shape.
