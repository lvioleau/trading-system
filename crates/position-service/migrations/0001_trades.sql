-- The durable trade ledger. Lives in the `positions` schema of the shared `trading`
-- database (contract-service owns `reference`, powerbot-gateway owns `execution`); this
-- service is the schema's single writer. Rows are appended, never updated or deleted — the
-- log is the truth, and the in-memory position projection is its fold.
--
-- The schema also exists before this migration runs: connect_with creates it and pins the
-- connection search_path to it, so sqlx's own `_sqlx_migrations` lands in `positions.`,
-- isolated from contract-service's ledger in `public.` (the powerbot-gateway pattern).
create schema if not exists positions;

-- One row per ledger trade: a signed quantity of one contract on one portfolio, at a price.
-- A market fill is a single row with `venue_trade_id` set; a transfer is two rows written in
-- one transaction (-quantity on from, +quantity on to, same contract and price), both with
-- `venue_trade_id` null. `id` is the idempotency key — deterministic at every producer
-- (transfer legs derive from the transfer id, fill ids from the wire trade's identity), so a
-- replay is an `on conflict do nothing` no-op.
create table positions.trades (
    -- Idempotency key: the primary key IS the dedup guard (no updated_at merge problem).
    id                uuid primary key,
    -- The total order the boot fold replays in. The service serializes mutations
    -- in-process (single writer), so insert order, seq order, and fold order agree.
    seq               bigint generated always as identity unique,
    -- A plain configured string (e.g. 'execution-a') — the ledger's aggregation boundary.
    -- No posblocks table: a portfolio exists by being configured and by appearing here.
    portfolio         text not null,
    -- The contract's natural key, carried verbatim from reference.contracts.
    venue             text not null,
    market_type       text not null,        -- LOCAL | XBID
    venue_contract_id bigint not null,
    -- Venue-native fixed-point integers, kept verbatim from wire to disk: the fold is
    -- signed addition, under which integers are closed (revisit only if a venue introduces
    -- sub-kW or sub-minor-unit granularity).
    quantity          bigint not null,      -- kW, signed: buy > 0 (1 MW = 1000 kW)
    price             bigint not null,      -- minor currency units per MWh; may be negative
    ccy               text not null default 'EUR',
    -- Fills: the venue's trade id (the reconciliation join key); null for transfer legs.
    venue_trade_id    text,
    -- Transfer legs: the originating transfer id, mirroring venue_trade_id — the two
    -- "natural external key" columns, each null for the other kind. Not needed for
    -- idempotency (the leg's primary-key id already derives from the transfer id) — stored
    -- for the Subscribe stream's write-attribution and for queryability.
    transfer_id       text,
    -- Fills: the client order id (nonce) the submitter stamped on the order that produced the
    -- fill, threaded from the gateway's Trade.txt; null for transfer legs. Additional
    -- attribution, not a key (plays no part in id derivation or idempotency) — carried so a
    -- submitter can recognize a fold as its own order's on the Subscribe stream.
    cl_order_id       text,
    -- Submitting component (crosser id, gateway name, ...).
    source            text not null,
    -- When the fill executed / the cross decided.
    executed_at       timestamptz not null,
    created_at        timestamptz not null default now(),
    -- The write-time integrity link to the Contract Service (shared database, no runtime
    -- dependency): a fill racing the contract sync fails loudly and the feed session
    -- retries — never silent position loss. Consequence: the contract-service migrations
    -- must have run on this database before this one (boot order on a fresh database).
    foreign key (venue, market_type, venue_contract_id) references reference.contracts
);
