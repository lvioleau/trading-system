-- The durable home of contract reference data. Lives in the neutral `reference` schema
-- (shared reference data, not positions): this service is the single writer, the Position
-- Service and analytics read it. Rows are upserted, never deleted — contracts change state
-- and their delivery window passes, they do not disappear.
create schema if not exists reference;

-- Venue-global identity + intrinsic fields. The natural key is (venue, market_type,
-- venue_contract_id) — no surrogate id: Nord Pool ids are stable per venue but not assumed
-- unique across the Local and Xbid market segments, so market_type is part of the identity.
create table reference.contracts (
    venue             text not null default 'nordpool',
    market_type       text not null,        -- LOCAL | XBID
    venue_contract_id bigint not null,      -- Nord Pool contract id
    name              text not null,
    market_id         text not null,
    product_type      text not null,        -- P15 | P30 | P60 | OTHER
    delivery_start    timestamptz not null, -- delivery window [delivery_start, delivery_end)
    delivery_end      timestamptz not null,
    duration_seconds  int not null,
    updated_at        timestamptz not null, -- venue update time; the upsert guard
    created_at        timestamptz not null default now(),
    primary key (venue, market_type, venue_contract_id)
);

-- Native overlap (`&&`) for SQL-side time-window readers; the service itself answers window
-- queries from the in-memory index, never the table.
create index contracts_delivery_window on reference.contracts
    using gist (tstzrange(delivery_start, delivery_end));

-- Per-area lifecycle state, mirroring the wire model: identity, product shape, and delivery
-- window are venue-global; `state` is per delivery area, each row guarded by its own venue
-- update time (the same contract can arrive from more than one source, each lagging
-- independently).
create table reference.contract_areas (
    venue             text not null,
    market_type       text not null,
    venue_contract_id bigint not null,
    delivery_area     text not null,        -- ENTSO-E EIC code, e.g. 10YFI-1--------U
    state             text not null,        -- ACTIVE | HIBERNATED | INACTIVE | FROZEN | UNKNOWN
    updated_at        timestamptz not null, -- venue update time; the per-area upsert guard
    primary key (venue, market_type, venue_contract_id, delivery_area),
    foreign key (venue, market_type, venue_contract_id) references reference.contracts
);
