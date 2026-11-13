-- Durable execution record for the PowerBot gateway. Lives in the `execution`
-- schema (private execution flow — the Position Service reads fills over gRPC,
-- not these tables), sharing the `trading` database with contract-service's
-- `reference` schema. The schema is created here, not by provisioning; the
-- gateway sets its connection search_path to `execution` so its
-- `_sqlx_migrations` table stays separate from contract-service's (in `public`).
--
-- Columns mirror the PowerBot models we relay, so the record is queryable (the
-- durable log settlement/drift investigations read). State/action/side are
-- Postgres enum types holding PowerBot's wire codes — the DB validates them, and
-- because the model enums are closed we only ever persist a valid member (a spec
-- bump that adds a code needs a companion `alter type … add value`). Timestamps
-- are the model's RFC3339 strings parsed to `timestamptz`; each table carries the
-- natural key, `api_timestamp` for the never-regress guard, and `delivery_end`
-- for the future-delivery boot load.
create schema if not exists execution;

-- Enum types (idempotent — `create type` has no `if not exists`).
do $$ begin
    create type execution.trade_state as enum
        ('CNCL','RREJ','RGRA','RREQ','ACTI','CREQ','CREJ','RSFA');
exception when duplicate_object then null; end $$;

do $$ begin
    create type execution.order_state as enum ('ACTI','HIBE','IACT','UNKW');
exception when duplicate_object then null; end $$;

do $$ begin
    create type execution.order_side as enum ('SELL','BUY');
exception when duplicate_object then null; end $$;

do $$ begin
    create type execution.order_action as enum (
        'UADD','UHIB','UMOD','UDEL','UREJ','AADD','AHIB','AMOD','ADEL','AREJ',
        'SADD','SHIB','SMOD','SDEL','SREJ','FEXE','PEXE','IADD','SERR','SNAV',
        'QADD','AFEX','QPEX','RLVI');
exception when duplicate_object then null; end $$;

-- External trades, keyed by trade_id. Upsert; never-regress on api_timestamp
-- (a stale reconciliation snapshot can't overwrite a fresher one). The per-side
-- (buy_/sell_) columns are kept raw — our side is derived on read, like the feed.
create table if not exists execution.trades (
    trade_id          text primary key,
    state             execution.trade_state,
    exchange          text,
    delivery_area     text,
    contract_id       text,
    contract_name     text,
    delivery_start    timestamptz,
    delivery_end      timestamptz,
    price             double precision,
    quantity          double precision,
    buy               boolean,
    sell              boolean,
    self_trade        boolean,
    buy_order_id      text,
    sell_order_id     text,
    buy_portfolio_id  text,
    sell_portfolio_id text,
    buy_strategy_id   text,
    sell_strategy_id  text,
    buy_unit_id       text,
    sell_unit_id      text,
    exec_time         timestamptz,
    api_timestamp     timestamptz,
    recorded_at       timestamptz not null default now()
);
create index if not exists trades_delivery_end on execution.trades (delivery_end);

-- Internal trades (a match between two of our own portfolios), keyed by
-- internal_trade_id. Same guard.
create table if not exists execution.internal_trades (
    internal_trade_id text primary key,
    contract_id       text,
    contract_name     text,
    delivery_start    timestamptz,
    delivery_end      timestamptz,
    price             double precision,
    quantity          double precision,
    buy_portfolio_id  text,
    sell_portfolio_id text,
    buy_order_id      text,
    sell_order_id     text,
    exec_time         timestamptz,
    api_timestamp     timestamptz,
    recorded_at       timestamptz not null default now()
);
create index if not exists internal_trades_delivery_end on execution.internal_trades (delivery_end);

-- Own-order revisions, append-only, keyed by (order_id, revision_no). A revision
-- is immutable, so conflicts are dropped; the full audit trail is retained.
create table if not exists execution.order_revisions (
    order_id       text                    not null,
    revision_no    bigint                  not null,
    state          execution.order_state   not null,
    action         execution.order_action  not null,
    side           execution.order_side    not null,
    delivery_area  text                    not null,
    contract_id    text,
    contract_name  text,
    delivery_start timestamptz,
    delivery_end   timestamptz,
    portfolio_id   text,
    price          double precision,
    quantity       double precision,
    txt            text,
    api_timestamp  timestamptz,
    recorded_at    timestamptz not null default now(),
    primary key (order_id, revision_no)
);
create index if not exists order_revisions_delivery_end on execution.order_revisions (delivery_end);
