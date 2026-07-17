-- Phase 8: candle rollup serving tables (Lambda-architecture speed/batch
-- layers for the custom backend's history endpoint, replacing Cube/BigQuery
-- on that hot path, which proved too slow/fragile for it).
--
-- Two tables, not one per UI range: candle_rollups_hourly is the "speed
-- layer" (short retention, refreshed every 5 min by candle-hourly-sync-job,
-- serves the 1D/5D ranges directly). candle_rollups_daily is the "batch
-- layer" (13-month retention matching the app's max 1Y range, refreshed
-- once daily by candle-daily-sync-job, serves 1M/6M/YTD/1Y). The custom
-- backend merges today's hourly rows into a synthetic "today" daily candle
-- at read time so the daily-granularity ranges still feel fresh between
-- daily-sync runs.
--
-- Written to be safely re-runnable, same discipline as prior migrations.

CREATE TABLE IF NOT EXISTS candle_rollups_hourly (
    coin_id       text NOT NULL REFERENCES coins (coin_id),
    bucket_start  timestamptz NOT NULL,
    open          numeric(24, 10) NOT NULL,
    high          numeric(24, 10) NOT NULL,
    low           numeric(24, 10) NOT NULL,
    close         numeric(24, 10) NOT NULL,
    volume        numeric(28, 10) NOT NULL,
    is_closed     boolean NOT NULL,
    updated_at    timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (coin_id, bucket_start)
);

CREATE TABLE IF NOT EXISTS candle_rollups_daily (
    coin_id       text NOT NULL REFERENCES coins (coin_id),
    bucket_start  timestamptz NOT NULL,
    open          numeric(24, 10) NOT NULL,
    high          numeric(24, 10) NOT NULL,
    low           numeric(24, 10) NOT NULL,
    close         numeric(24, 10) NOT NULL,
    volume        numeric(28, 10) NOT NULL,
    is_closed     boolean NOT NULL,
    updated_at    timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (coin_id, bucket_start)
);

-- Range queries ("last N hours/days for coin X") are the only access
-- pattern; the primary key's own btree already supports
-- "WHERE coin_id = ? AND bucket_start >= ? ORDER BY bucket_start" well, but
-- an explicit index makes that intent clear and covers a coin-only lookup
-- (e.g. pruning) without the leading bucket_start column getting in the way.
CREATE INDEX IF NOT EXISTS idx_candle_rollups_hourly_coin ON candle_rollups_hourly (coin_id, bucket_start DESC);
CREATE INDEX IF NOT EXISTS idx_candle_rollups_daily_coin ON candle_rollups_daily (coin_id, bucket_start DESC);
