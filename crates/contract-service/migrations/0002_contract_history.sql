-- Append-only lifecycle history: one row per recorded state *transition* per delivery area,
-- including the contract's first appearance there. Only state is expected to change over a
-- contract's life, so this is the only history kept — an intrinsic-field correction from the
-- venue overwrites `contracts` in place (the connector's archive keeps the raw frames).
--
-- `contract_areas` stays the live truth: it carries the latest venue update time per area —
-- the out-of-order guard — which advances on *every* applied frame, while a row lands here
-- only when the state value actually changed. The natural key works because recorded
-- transitions are strictly newer per area: updated_at never repeats.
create table reference.contract_state_changes (
    venue             text not null,
    market_type       text not null,
    venue_contract_id bigint not null,
    delivery_area     text not null,        -- ENTSO-E EIC code
    state             text not null,        -- the state the area changed *to*
    updated_at        timestamptz not null, -- venue update time of the frame that carried it
    recorded_at       timestamptz not null default now(),
    primary key (venue, market_type, venue_contract_id, delivery_area, updated_at)
);

-- Backfill: contracts stored before history existed enter it with their current state as the
-- first transition (stamped with the live rows' venue update time; recorded_at marks the
-- backfill).
insert into reference.contract_state_changes
    (venue, market_type, venue_contract_id, delivery_area, state, updated_at)
select venue, market_type, venue_contract_id, delivery_area, state, updated_at
from reference.contract_areas;
