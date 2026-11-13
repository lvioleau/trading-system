# Position Service — implementation plan

Implementation breakdown for the **Position Service**, the system of record for portfolio
positions in the continuous intraday market. Derived from the
Position Service — technical design (internal design doc)
(Notion, the design authority; see also the
PowerBot Gateway — design (internal design doc)
for the fills side); this document maps that design onto this repo — crate layout,
conventions, and PR-sized implementation steps. Where the two disagree, the design doc wins,
except for the deviations listed below.

**Status:** steps 0–5 are implemented (`crates/position-service`: config with the gateway →
portfolio mapping, the pure-domain ledger, the gRPC server — `Transfer` with idempotent
replay and the `expected_from_revision` guard, snapshot-then-updates `Subscribe`, health,
reflection — the durable trade log: the `positions.trades` migration with the pinned
`search_path` isolation, `append`/`load_log` storage, commit-then-memory-then-broadcast
`Transfer`, the boot fold with the unconfigured-portfolio tripwire — and the gateway feeds:
one reconnect-loop own-trades task per gateway over the gateway protos, seed/delta
`seq` discipline, durable-before-visible folding). Step 7 landed 2026-07-11 (the shared
Dockerfile, the launcher scripts, the README sections — decision note under step 7). Steps 6
and 8 pending. *(2026-07-13: the shadow/internal-trade concept moved out of this service
entirely — a shadow gateway now normalizes internal trades onto its own-trades stream, so
this service folds one stream and one fill id namespace; see the deviations section and
docs/powerbot-gateway/design.md, "Shadow instances".)*

## Deviations from the design doc

**Portfolio replaces posblock.** The design's `posblock` (a UUID-keyed entity with `kind`,
`owner`, `delivery_area`, held in a `posblocks` table) is replaced by **`portfolio`: a plain
string identifier** (e.g. `execution-a`, `strategy-x`). Consequences:

- **No `posblocks` table.** The `trades` table carries `portfolio text not null` directly.
  A portfolio exists by being configured and by appearing in the ledger — there is no
  create/delete lifecycle to manage.
- **The accepted portfolios are configuration.** The INI lists the portfolios the service
  handles (`portfolios = execution-a, execution-b, strategy-x`). What the design encoded as
  `kind = 'EXECUTION'` becomes the gateway → portfolio mapping below.
- **Validation at the boundary.** `Transfer` rejects a leg naming an unconfigured portfolio
  (`INVALID_ARGUMENT`). The boot fold accepts *any* portfolio found in the ledger (the log is
  the truth; conservation must hold over all of it) but warns — tripwire — on portfolios
  missing from config. `Subscribe` mirrors the Contract Service's filter semantics: an
  unknown portfolio is not an error, it seeds empty and never updates.
- The design's `owner` / `delivery_area` metadata is dropped. If it is ever needed, it can
  return as config or as a table without touching the ledger.

**Multiple order gateways, mapped to portfolios in config.** The design sketches one PowerBot
Gateway feeding one Execution posblock; in practice the service subscribes to the execution
streams of **several order gateways**, each scoped to one exchange and a set of PowerBot
portfolios by *its* config (see `proto/flow/v1/powerbot_gateway.proto` in the
`powerbot-gateway` repo). The Position Service's config therefore names the gateways and maps
**each gateway subscription to the portfolio its fills fold into** — a "gateway" section here is
one portfolio-scoped subscription, so several sections may point at the *same* gateway endpoint
with different `powerbot_portfolio_id`s (a shared multi-portfolio gateway instance):

```ini
[position-service]
portfolios = execution-a, execution-b, strategy-x
gateways   = nordpool-a, nordpool-b

[position-service.gateway.nordpool-a]
endpoint  = ${GATEWAY_NORDPOOL_A_ENDPOINT:-http://localhost:50061}
portfolio = execution-a
powerbot_portfolio_id = PB-A  # our PowerBot portfolio id on this gateway — the required
#                     # subscription scope (the gateway rejects a mismatch: fail-fast
#                     # config-drift check)
```

Each gateway's `portfolio` must be a member of `portfolios` (fatal config error otherwise);
several gateways may map to the same portfolio (the design's shared/pooled execution
posblock). A `powerbot_portfolio_id`, however, may appear in **only one** gateway section
(fatal otherwise): fill ids derive from `(exchange, trade_id)`, so the same venue portfolio
subscribed twice would deliver identical trades to two feeds — folded once, into whichever
feed applies first, which with different `portfolio` targets is a race for which book gets
each fill. One feed task runs per configured gateway, each with its own reconnect loop.
*(Updated 2026-07-12: the gateway's subscribe RPCs now require a `portfolio_id` scope —
originally they took no parameters — so `powerbot_portfolio_id` is required on **every**
gateway section, no longer only on shadow ones. The gateway validates the scope against its
configured portfolio set and rejects a mismatch with `INVALID_ARGUMENT`, turning a
this-service-vs-gateway config drift into a loud subscribe failure instead of misattributed
fills.)*

**This service knows no PowerBot internal-trade concept.** PowerBot is never used for
internal transfers between our portfolios — transfers exist only as this service's `Transfer`
RPC — and shadow executions (which a PowerBot *shadow* instance reports as internal trades)
are the **gateway's** concern: a gateway configured `shadow = true` normalizes each our-side
leg onto its own-trades stream as an ordinary trade before it reaches us
(docs/powerbot-gateway/design.md, "Shadow instances"). This service therefore folds exactly
one stream per gateway (own-trades) into exactly one fill id namespace,
`(exchange, trade_id)`. *(Until 2026-07-13 the shadow flag, a dedicated internal-trades
subscription, a separate internal fill id namespace, and the leg-sign rule all lived here —
deleted with the gateway-side normalization, which also unbroke the executor in shadow mode:
it watches own-trades only.)*

**Integer quantities and prices, end to end.** The design's `Decimal`/`numeric` becomes
venue-native fixed-point integers everywhere — kW quantities and minor-currency-unit prices
(the Nord Pool wire encoding) on the API, in the domain ledger, and as `bigint` in Postgres.
Same intent (exact, never floats), simpler means: the fold is signed addition, under which
integers are closed, so no scale conversion exists at any boundary. See the data-model
section for the revisit condition.

Everything else follows the design: append-only priced trade ledger in Postgres, in-memory
position projection as the serving authority, transfers as zero-sum two-leg transactions,
fills folded from the gateways' own-trades feeds, OCC via `expected_from_revision`,
commit-then-memory-then-broadcast ordering.

## The shared `trading` database

Three services share one Postgres database (`trading`), one schema each, single writer per
schema:

| schema | owner (writer) | this service's relationship |
|---|---|---|
| `reference` | contract-service (this repo) | FK target: `trades` references `reference.contracts` |
| `execution` | powerbot-gateway (this repo) | none at the DB level — fills arrive over gRPC, never by reading these tables; the `venue_trade_id` reconciliation join reads them, later |
| `positions` | position-service (this repo) | owned |

The sqlx migration-ledger collision is already solved in the wild: powerbot-gateway creates
its schema and pins the connection `search_path` to it before migrating, so its
`_sqlx_migrations` lands in `execution.` while contract-service's stays in `public.` (see
`crates/powerbot-gateway/src/storage.rs`). Position-service adopts the same pattern:
create `positions`, pin `search_path`, migrate — its ledger lands in
`positions._sqlx_migrations`. Queries still schema-qualify table names (the contract-service
style); the search_path exists for the migration ledger, not for name resolution.

## Shape of the service

The Contract Service is the template — same stack (tokio / tonic / sqlx / INI config), same
module roles, same ordering disciplines. The mapping:

| concern | contract-service | position-service |
|---|---|---|
| domain state | `store.rs` — contract index, `updated_at` guards | `ledger.rs` — position fold, per-`(portfolio, contract)` `revision` |
| upstream feed | `feed.rs` — connector `SubscribeContracts` | `feed.rs` — one own-trades subscription per configured gateway (seed-then-deltas, backoff, keepalive) |
| persistence | `storage.rs` — guarded upserts | `storage.rs` — append-only inserts, `on conflict (id) do nothing` |
| shared plumbing | `channel.rs` | `channel.rs` — shared projection + position broadcast |
| gRPC surface | `server.rs` + health + reflection | `server.rs` + health + reflection |
| config | INI, `${VAR:-default}` | same scheme, `position-service.ini` |

Key differences to keep in mind while porting patterns:

- **The ledger is append-only, not upsert-guarded.** Idempotency comes from the trade `id`
  primary key, not from `updated_at` comparisons. Replays are `on conflict do nothing`
  no-ops; there is no out-of-order merge problem.
- **The service accepts mutations** (`Transfer`), which the Contract Service never does.
  Mutation serialization is in-process (single writer instance); every mutation follows
  *commit, then memory, then broadcast* — the ack returns only after the Postgres commit.
- **Boot fold is a log replay**, in `seq` order, so revisions are deterministic across
  restarts — not a table fold.

## Target layout

```
proto/flow/v1/position.proto        this service's gRPC API
proto/flow/v1/powerbot_gateway.proto  the order gateway's gRPC API (owned by crates/powerbot-gateway)
crates/position-service/
  src/main.rs                               boot: config, storage connect, log fold, feed + server
  src/lib.rs                                module tree + generated proto modules
  src/config.rs, src/config/raw.rs          INI + ${VAR} resolution (copied from contract-service, see step 0)
  src/ledger.rs                             pure domain: Trade, Position, the fold, revisions — no IO, no protobuf
  src/channel.rs                            SharedPositions + position-update broadcast types
  src/feed.rs                               gateway own-trades clients, one task per gateway (wire → domain at the boundary)
  src/storage.rs                            sqlx: append, transfer txn, load_log
  src/server.rs                             PositionService gRPC: Transfer, Subscribe (+ health, reflection)
  migrations/0001_trades.sql                schema `positions`, the trades table
  tests/…                                   black-box integration tests, per the contract-service pattern
position-service.ini                        shipped config template
```

## Data model

One table (schema `positions`), per the design §6 minus `posblocks`:

```sql
create schema if not exists positions;

create table positions.trades (
  id                uuid primary key,                                -- idempotency key
  seq               bigint generated always as identity unique,     -- total order = fold order
  portfolio         text not null,
  venue             text not null,                                   -- contract natural key,
  market_type       text not null,                                   --   carried verbatim
  venue_contract_id bigint not null,
  quantity          bigint not null,                                 -- kW, signed: buy > 0
  price             bigint not null,                                 -- minor currency units per MWh
  ccy               text not null default 'EUR',
  venue_trade_id    text,                    -- fills: the venue's trade id; null for transfer legs
  transfer_id       text,                    -- transfer legs: the originating transfer id; null for fills
  cl_order_id       text,                    -- fills: the originating client order id (gateway Trade.cl_order_id); null for transfer legs
  source            text not null,           -- submitting component
  executed_at       timestamptz not null,    -- when the fill executed / the cross decided
  created_at        timestamptz not null default now(),
  foreign key (venue, market_type, venue_contract_id) references reference.contracts
);
```

- A transfer is one transaction inserting both legs (`-quantity` on `from`, `+quantity` on `to`, same
  contract and price) with `on conflict (id) do nothing` — 0 rows affected means replay,
  commit anyway, ack idempotently.
- A fill is the same transaction shape with a single row, `venue_trade_id` set, portfolio =
  the one configured for the gateway that delivered it. Fill trade ids derive
  deterministically from the trade's *own* identity — UUID v5 over `(exchange, trade_id)`
  under a fixed service namespace — never from the gateway's config name, so renaming a
  gateway in config cannot mint new ids and double-count replays.
- `venue_trade_id` and `transfer_id` are the two *natural external key* columns, mirrors of
  each other and each null for the other kind: a fill records the venue's trade id (the
  reconciliation join key), a transfer leg the submitter's transfer id. Neither is the
  idempotency guard (the primary-key `id` derives deterministically from them) — they are
  stored for the `Subscribe` stream's write-attribution and for queryability
  (`dump-positions.sh` joins without recomputing UUIDs).
- `cl_order_id` is *additional* fill attribution, not a key: the client order id the
  submitting component stamped on the order that produced the fill (the gateway's
  `Trade.cl_order_id`, the ID Executor's per-order nonce). It plays no part in id derivation or idempotency — it is
  carried through so a submitter can recognize a fill as *its own order's* directly on the
  stream, complementing the `venue_trade_id` match (the executor still learns the fill-id set
  from the gateway own-trades feed; this is the same correlation on the authoritative side).
  Null for transfer legs.
- `bigint` for quantity/price — the venue-native fixed-point integers (kW, minor currency
  units per MWh), kept verbatim from wire to disk. A deliberate deviation from the design
  doc's `numeric`: the intent there was exactness (never floats), and integers satisfy it
  inherently — the fold is signed addition, under which integers are closed. Revisit only
  if a venue ever introduces sub-kW or sub-minor-unit granularity (a migration, and worth
  noticing explicitly).
- The composite FK into `reference.contracts` is the write-time integrity link to the
  Contract Service (shared Postgres, no runtime dependency). A fill racing the contract sync
  fails loudly and the feed session retries — never silent position loss. Note the feed
  implication: unlike the contract feed, a failed insert here is *expected to heal* (the
  contract sync catches up), so the reconnect-and-replay loop is the recovery mechanism, not
  just a tripwire.

## API sketch

`flow.v1.PositionService` (final field layout to be settled in step 3):

```protobuf
service PositionService {
  // Atomic, idempotent, validated against the configured portfolios. Acks only after the
  // Postgres commit, carrying the resulting revisions of both touched positions.
  rpc Transfer(TransferRequest) returns (TransferResponse);
  // Snapshot, then the full value of each position as it changes (no deltas to fold —
  // clients replace by highest revision). The snapshot replays each pair's attributed
  // history: one Position per fold, the same wire shape as a live change.
  rpc Subscribe(SubscribeRequest) returns (stream PositionUpdate);
}
```

- `TransferRequest`: `transfer_id` (UUID, the idempotency key; leg ids derive from it),
  `from_portfolio`, `to_portfolio`, contract identity (`market_type` + `contract_id`),
  `quantity` (signed, non-zero — negative reverses the flow), `price`, `source`,
  `executed_at_nanos` (unix nanos, matching the gateway wire's `exec_time_nanos`), and the
  optional OCC guard `expected_from_revision`: by convention the *from* side is the
  portfolio a read-derived quantity was computed from (the signed quantity makes that
  orientation always possible — design §10's guarded side, without naming a portfolio in a
  second field). Conflict returns `ABORTED`; the caller re-reads and retries; absent means
  unguarded/additive.
- Wire encoding for quantities and prices: **integers in the venue's native units**,
  matching Nord Pool's own PMD encoding — quantities in kW (1 MW = 1000 kW), unit prices in
  market currency minor units per MWh (EUR cents today). Exact by construction; the units
  are documented on the fields (not in the names, matching the PMD style). The same
  integers are the domain and storage representation (`Quantity`/`Price` aliases in the
  ledger, `bigint` columns) — no conversion at any boundary. The PowerBot gateway protos
  use the same encoding, so fills are verbatim too — no float rounding anywhere.
- `Subscribe` follows the Contract Service's stream conventions verbatim: join broadcast
  before snapshot, duplicates deduped by `revision`, lagging subscribers dropped with
  `RESOURCE_EXHAUSTED` to reconnect and re-seed, streams ended by the shutdown watch so
  graceful drain completes.
- Every `Position` on the wire — seed entry or streamed change — is the same thing: the
  full value one trade's fold produced, carrying that trade's write attribution (the
  causing `venue_trade_id` for fills or `transfer_id` for transfer legs — exactly one
  set — plus `cl_order_id` (fills only, the originating client order id / nonce),
  `source`, and `executed_at_nanos`). This is how a submitter recognizes its own writes
  (the crosser matches `transfer_id`; the ID Executor matches its fills'
  `venue_trade_id`, and can also read the `cl_order_id` its own order stamped) without
  recomputing ledger UUIDs. The one attribution-less `Position` is the
  `TransferResponse`'s, which already identifies its transfer.
- The *snapshot* replays each pair's entire history of those changes, oldest first — a
  (re-)subscribe is indistinguishable from having been subscribed since the pair's first
  trade, and the client's keep-highest-revision fold lands on the current value with no
  snapshot-specific handling.
  - Complete on purpose, no retention horizon: these are intraday contracts, so a
    position stops accruing when its contract expires — the log's own natural bound. The
    ledger retains the history per pair (the `history` map, next to `seen`), rebuilt by
    the boot fold from the durable attribution columns — which therefore carry seed
    correctness, not just queryability.
  - A consumer whose subscription was down at fold time — either side restarted, or
    dropped for lagging — retires a still-awaited trade the moment it finds its id in
    the re-seed: attribution survives the stream's broadcast-exactly-once (a change
    streams once; the seed replays it forever).
- There is deliberately **no fold point-query** (an earlier `CheckApplied` pair was built
  and then removed): the seed replays every fold's attribution, and a trade not yet
  applied at subscribe time arrives with attribution on the open stream (join-before-
  snapshot makes that race-free) — so the `Subscribe` stream is the API's one carrier of
  position state, and a consumer needs no side channel and no resync watermarks. Drift
  investigations query the durable log (`positions.trades` carries the natural keys), not
  the API.

## Implementation steps

Each step is a PR-sized milestone that builds, passes CI, and leaves the service runnable.
Follow contract-service conventions throughout: doc-comment-heavy modules stating the
invariants, black-box integration tests with Given/When/Then comments, per-test databases,
clippy `-D warnings` clean.

### Step 0 — decide the shared-code seam

One question to settle before code (the shared-database migration question is already
answered — adopt the powerbot-gateway `search_path` pattern, see the shared-database section
above, and prove it in step 4's tests):

**Shared plumbing.** `config/raw.rs` (INI + `${VAR}` resolution) and the feed
reconnect/backoff shape are about to be duplicated — and powerbot-gateway carries its own
third copy of the same conventions. Options: extract a small `crates/flow-util` (or
`flow-config`) workspace crate now, or copy and extract later (a cross-repo share with the
gateway would need a published crate — out of scope). Recommendation: **copy now, extract
later** — the feed loops differ enough (guarded upserts vs append-heal) that premature
abstraction would fight the next step.

Deliverable: a short decision note appended to this document; no service code yet.

> **Decided (2026-07-10):** copied. `config/raw.rs` is a verbatim copy of the
> contract-service module; extraction waits for a third in-repo consumer. The same call was
> made in step 2 for `ContractKey`/`MarketType`: mirrored in `ledger.rs` rather than
> importing the contract-service crate, which would drag its proto build (tonic, protoc)
> into this crate for two tiny types (rationale in the `ledger.rs` module docs).

### Step 1 — skeleton crate, config, CI

- `crates/position-service` workspace member: `main.rs` that loads config, logs it, and
  idles; `lib.rs`; `config.rs` + `config/raw.rs` per step 0.
- Config keys (`${VAR:-default}` scheme; the gateway mapping uses one INI section per
  gateway, the `[gateway.storage]`-style dotted sections powerbot-gateway already uses):
  - `[position-service]` — `portfolios` (comma-separated accepted portfolios; at least one,
    entries non-empty and unique), `gateways` (comma-separated gateway names, possibly
    empty for a transfers-only deployment), `listen_addr`, `database_url`, and the
    `feed_*_ms` tuning keys (same five as contract-service, same positive-integer
    validation, shared by all gateway feeds)
  - `[position-service.gateway.<name>]` per entry in `gateways` — `endpoint` (gateway gRPC
    address) and `portfolio` (where that gateway's fills fold; must be a member of
    `portfolios`)
- Ship `position-service.ini`; extend CI (`test.yaml` already runs the workspace; nothing to
  add beyond the crate) and the Dockerfile story in step 7.
- Tests: config parsing — full sample with two gateways, portfolio list validation, a
  gateway naming an unlisted `portfolio` is fatal, a `gateways` entry without its section is
  fatal.

### Step 2 — the ledger (pure domain)

`ledger.rs`, no IO, no protobuf — the `store.rs` analogue and the heart of the service:

- Types: `Trade` (id, portfolio, `ContractKey`, integer quantity/price (kW, minor units), executed_at, optional
  venue_trade_id, source), `Position` (portfolio, contract, `net_quantity`, `revision`),
  `PositionLedger` — the fold.
- `apply(trade) -> Option<Position>`: signed add on `(portfolio, contract)`, revision
  incremented, returning the new full position; applying a trade id already seen is a no-op
  returning `None` (the in-memory mirror of `on conflict do nothing` — keep a seen-id set,
  which also makes boot replay + feed re-seed overlap harmless).
- Transfer legs are just two trades; the ledger itself is portfolio-agnostic (config
  validation lives at the server boundary, per the deviations above).
- Reuse `contract_service::store::ContractKey`/`MarketType` if trivially importable
  (contract-service is already a lib in this workspace); otherwise mirror the type — decide
  here, note it in the module docs.
- Tests: fold determinism (same log ⇒ same revisions), idempotent re-apply, zero-sum
  transfer pairs conserve total per-contract quantity, revision monotonicity per position.

### Step 3 — proto + gRPC server over the in-memory ledger

The contract-service build order (server before storage) applies — everything below runs
memory-only, `storage: None`:

- `proto/flow/v1/position.proto`; extend the position-service `build.rs` (own
  descriptor set for reflection, separate from the gateway protos).
- `channel.rs`: `SharedLedger` (`Arc<RwLock<PositionLedger>>`), `PositionUpdate` broadcast.
- `server.rs`: `Transfer` (validate portfolios against config, distinct from/to, quantity ≠ 0,
  parse decimals; apply both legs under one write lock; broadcast both updated positions)
  and `Subscribe` (snapshot-then-updates per the stream conventions, portfolio filter),
  plus health and reflection, shutdown watch, graceful drain — lift the scaffolding from
  contract-service `server.rs`.
- OCC: `expected_from_revision` checked under the write lock before applying; mismatch ⇒
  `ABORTED`.
- Tests (black-box, real client over a local socket, per `tests/contract_server.rs`):
  snapshot-then-delta ordering, replay of the same `transfer_id` acks idempotently with
  unchanged revisions, OCC conflict, unconfigured portfolio rejected, unknown subscribed
  portfolio seeds empty, lag ⇒ `RESOURCE_EXHAUSTED`, shutdown ends streams.

### Step 4 — storage: migrations, transfer transaction, boot fold

- `migrations/0001_trades.sql` as above; `connect_with` creates the `positions` schema and
  pins `search_path` before migrating (the powerbot-gateway pattern), so the migration
  ledger lands in `positions._sqlx_migrations`.
- `storage.rs`: `connect`/`connect_with` (runs migrations), `append(&[Trade])` in one
  transaction with `on conflict (id) do nothing`, `load_log()` streaming the table in `seq`
  order for the boot fold.
- Wire the write path: *commit, then memory, then broadcast* — `Transfer` acks after commit;
  a failed commit leaves memory untouched and returns `UNAVAILABLE`/`INTERNAL`.
- `main.rs` boot: connect, fold `load_log()` into the ledger, then serve (the contract
  service's boot-fold shape).
- Tests (per-test databases, `DATABASE_URL` skip-guard with the CI tripwire, like
  `tests/contract_storage.rs`): round-trip, replayed batch is a no-op, transfer legs land
  atomically (inject a failure between legs via a bad second row and assert nothing
  persisted), boot fold reproduces revisions, FK rejection on an unknown contract. These
  tests must set up `reference.contracts` first — call contract-service's
  `ContractStorage::connect_with` in the fixture to run its migrations, which also proves
  the two migration ledgers coexist on one database (the `search_path` isolation, verified;
  powerbot-gateway's `execution` schema needs no fixture — this service never touches it).

### Step 5 — the gateway own-trades feeds

- Vendor `proto/flow/v1/powerbot_gateway.proto` from the `powerbot-gateway` repo
  (it exists — milestone 4 there is built), marked do-not-edit like the marketdata protos.
  Build.rs wrinkle: it shares package `flow.v1` with `position.proto`, so two
  separate `tonic_build` invocations would write the same `flow.v1.rs` into
  `OUT_DIR` — give the vendored compile its own `out_dir` and include it as a distinct
  module (e.g. `pub mod gateway`), keeping the reflection descriptor set scoped to
  `position.proto` only.
- `feed.rs`: **one task per configured gateway**, each running the reconnect/backoff loop,
  subscribing to `PowerbotExecutionService.SubscribeOwnTrades` with the gateway's configured
  `powerbot_portfolio_id` as the required subscription scope (added 2026-07-12 — originally
  the RPC took no parameters; the gateway rejects an empty or mismatched scope with
  `INVALID_ARGUMENT`, a fail-fast config-drift check). Honor the stream's dedup contract:
  the first frame is the seed; forward only deltas with `seq > seed_seq`. Idempotent trade
  ids make any residual overlap harmless anyway.
- Translate wire `Trade` → domain at the boundary: id = UUID v5 over `(exchange,
  trade_id)`; portfolio = the gateway's configured portfolio; signed quantity from `side` +
  `quantity` (BUY > 0). The gateway wire carries the same integer encoding as the Position
  API and the domain (kW quantities, minor-unit prices), so fills fold verbatim — no
  conversion, no rounding anywhere. Then per batch: append to Postgres, apply to
  the ledger, broadcast — durable before visible.
- Contract identity: the wire trade carries a string `contract_id` and `exchange`, not the
  Contract Service's `(venue, market_type, venue_contract_id)` key — resolving one to the
  other is this step's real design work (open question 2).
- A failed insert (e.g. FK racing the contract sync) fails that gateway's session and
  reconnects; the gateway re-seeds and the idempotent ids make the replay a no-op for
  everything already applied. Unlike contract-service, FK failures here are *retried by
  reconnect*, not dropped: a dropped fill is silent position loss (log loudly on repeated
  failure of the same trade id). One gateway's outage must not disturb the other feeds.
- **No shadow special-casing** *(since 2026-07-13)*: shadow executions arrive as ordinary
  own-trades fills, normalized by the gateway (see the deviations section); this service
  never opens the internal-trades stream. *(Step 5 originally carried a per-gateway
  `shadow` flag, an internal-trades subscription, a separate `internal_fill_id` UUID v5
  namespace, and a leg-sign rule — all deleted with the gateway-side normalization.)*
- Tests: mock gateway (the `tests/feed_subscription.rs` pattern) — two mock gateways fold
  into their respective configured portfolios, seed/delta `seq` overlap is dropped,
  re-delivered fills dedupe, a subscriber sees the resulting position updates, one gateway
  failing does not stall the other.

### Step 6 — reconciliation worker (alarm-only)

Deferrable — the last mile of design §12, needs Powerbot portfolio read access decisions:

- Periodic task comparing the folded venue-aligned position per contract against Powerbot's
  reported portfolio — per gateway, using its configured portfolio mapping; on drift, log at
  error / emit a metric. No adjustments, ever.
- Can ship well after steps 1–5; the `venue_trade_id` join backstop lives in the gateway.

### Step 7 — ops parity

- Multi-service Dockerfile: either a build `ARG` selecting the binary or one image carrying
  both services — decide when this lands; extend `scripts/run-position-service.sh` from the
  contract-service launcher.
- README: add the Position Service to the layout and run sections.
- Update this document's status as steps land.

> **Decided (2026-07-11):** one image carrying both binaries, run as **separate
> containers** — the container command names the service (`docker run <image>
> position-service`); no default command, so starting the wrong system-of-record service
> silently is impossible. One image because the workspace releases as a unit (one build and
> tag to pin, cargo compiles the shared dependency tree once) and because the `ARG` variant
> needs a shell-expansion entrypoint hack for no real gain. Separate containers because the
> lifecycles must stay independent: a contract-service redeploy must not bounce the position
> ledger's subscribers or its OCC clients, each service keeps its own SIGTERM drain and gRPC
> health, and single-writer-per-schema stays enforceable per container. The builder stage
> needs no system protobuf-compiler anymore — the build scripts fall back to the vendored
> protoc. `scripts/run-position-service.sh` extends the contract launcher;
> `run-contract-service.sh` now passes the binary name explicitly.

### Step 8 — bound the boot fold: the expired-contract cutoff

Deferrable — a "before it hurts, not now" item (a year of intraday trades still folds in
seconds); decided 2026-07-11, lands any time after step 5. The trade log grows without
bound while most of it concerns contracts whose delivery long passed, so the boot fold and
the in-memory projection should stop carrying them. The durable log stays complete — this
bounds memory and boot time, never the record (downstream P&L/audit reads are unaffected).

- **Filter by joining `reference.contracts`, not by connecting to the Contract Service.**
  The contract data is in the same database, guaranteed present by the FK, and readable in
  the same snapshot as the log; a boot-time gRPC dependency would invert the shared-DB
  design principle (no runtime dependency on the contract service) and couple this
  service's boot to that service's availability. `load_log` grows the join and a cutoff:
  `delivery_end > now() - margin`, with the margin a config key (a few days — late
  corrections and post-delivery reconciliation still have a window).
- **The filter is all-or-nothing per contract, which is what keeps it sound:** revisions
  are per-position counters, so dropping *every* trade of an expired contract disturbs no
  surviving position's revisions. Never filter by trade time — cutting a position's
  history mid-way breaks its served quantity.
- **Filtering at boot requires freezing expired contracts at the write boundary.** Once a
  contract's history is absent from memory, a fresh transfer on it would fold onto a
  partial position (acked, wrong quantity), and a replayed transfer or re-delivered old
  fill is no longer in the seen-id set — Postgres dedupes, but memory would resurrect a
  phantom partial position. So, in the same step: `Transfer` rejects a contract past the
  cutoff (`FAILED_PRECONDITION`, checkable inside the append transaction already paid
  for), and the feed translation drops fills past it (log loudly — a live fill on an
  expired contract is a tripwire). The business rule stated plainly: after delivery ends
  plus the margin, the book on that contract is closed.
- **Eviction sweep for Subscribe consistency.** A long-running instance would keep serving
  expired positions it folded while live; a restarted one would not. A periodic sweep
  evicts positions past the same cutoff from the live ledger (and their trade ids from the
  seen set), so what subscribers see does not depend on the last restart time.
- Tests: boot fold skips expired contracts and reproduces live revisions exactly; a
  transfer on an expired contract is rejected and persists nothing; a replayed transfer on
  an evicted contract does not resurrect a position; the sweep evicts and a subscriber
  re-seed reflects it; the margin keeps a just-expired contract foldable.

## Open questions

Still open at time of writing:

1. **Local/Xbid netting** — if one delivery obligation trades under one id in both market
   segments, the natural-key fold splits one economic position across two keys. Confirm the
   netting semantics before step 2 hardens the fold (Contract Service design §3).
2. ~~**Fill → contract-key resolution**~~ — resolved by the powerbot gateway: the wire
   `Trade` (and `InternalTrade`) now carry `market_type` (same numeric vocabulary as the
   connector's) and the bare integer `contract_id` (PowerBot's `NX_`/`NI_` prefix stripped
   gateway-side), so the natural key maps verbatim; no parsing or `reference.contracts`
   lookup anywhere. The feed validates `exchange == "nordpool"` (the storage venue
   constant) as a tripwire.
3. ~~**Float → `Decimal` policy for fills**~~ — resolved: the PowerBot gateway protos now
   carry the same integer encoding as the Position API and the domain (kW quantities,
   minor-unit prices), so fills fold verbatim; no rounding policy exists anywhere.
4. ~~**Shadow internal-trade sign derivation**~~ — dissolved 2026-07-13: the question moved
   out of this service with the concept. The gateway now normalizes shadow internal trades
   onto its own-trades stream — leg selection, side, and identity are its concern
   (docs/powerbot-gateway/design.md, "Shadow instances"); this service folds the result as
   ordinary fills. The residual validation item (confirm leg semantics against a real
   shadow feed sample — the PowerBot shadow-trading docs are behind the vendor login) now
   lives with the gateway.
