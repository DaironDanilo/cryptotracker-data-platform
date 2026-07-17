-- Phase 4: recurring incremental ETL support (Cloud Scheduler + Cloud Run Jobs).
-- Written to be safely re-runnable, same discipline as 0001_init.sql.

-- The original CHECK only allowed 'binance_ws' | 'coingecko', written when a
-- WebSocket worker was assumed to be the live price source. That worker is
-- now a deferred later phase (Phase 9); the Binance *incremental REST* job
-- (Phase 4) is the only price writer for now, so it needs its own value.
-- 'binance_ws' is kept for when Phase 9 is eventually built.
ALTER TABLE coin_snapshots DROP CONSTRAINT IF EXISTS coin_snapshots_price_source_check;
ALTER TABLE coin_snapshots ADD CONSTRAINT coin_snapshots_price_source_check
    CHECK (price_source IS NULL OR price_source IN ('binance_ws', 'binance_rest', 'coingecko'));

CREATE TABLE IF NOT EXISTS ingestion_watermarks (
    coin_id                 text NOT NULL,
    interval                text NOT NULL,
    last_loaded_close_time  timestamptz NOT NULL,
    updated_at              timestamptz NOT NULL,
    PRIMARY KEY (coin_id, interval)
);
