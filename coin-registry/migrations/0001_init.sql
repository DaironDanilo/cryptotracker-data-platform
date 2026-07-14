-- Phase 2: coin registry + live snapshot + market listings.
-- Written to be safely re-runnable: every DDL statement is guarded.

CREATE TABLE IF NOT EXISTS coins (
    coin_id        text PRIMARY KEY,       -- CoinGecko id, e.g. "bitcoin"
    symbol         text NOT NULL,
    name           text NOT NULL,
    binance_symbol text NULL               -- e.g. "BTCUSDT"; NULL if not tradeable on Binance
);

CREATE TABLE IF NOT EXISTS coin_snapshots (
    coin_id             text PRIMARY KEY REFERENCES coins (coin_id),
    price_usd           numeric(24, 10) NOT NULL,
    market_cap_usd      numeric(24, 2),
    rank                int,
    change_percent_24h  numeric(10, 4),
    updated_at          timestamptz NOT NULL,
    price_source        text,               -- 'binance_ws' | 'coingecko' -- last writer of price_usd
    CONSTRAINT coin_snapshots_price_source_check
        CHECK (price_source IS NULL OR price_source IN ('binance_ws', 'coingecko'))
);

CREATE TABLE IF NOT EXISTS markets (
    coin_id         text NOT NULL REFERENCES coins (coin_id),
    exchange_id     text NOT NULL,          -- CoinGecko's market.identifier
    base_symbol     text NOT NULL,
    target_symbol   text NOT NULL,
    price_usd       numeric(24, 10),
    volume_usd_24h  numeric(28, 2),
    trust_score     text,
    last_traded_at  timestamptz,
    updated_at      timestamptz NOT NULL,
    PRIMARY KEY (coin_id, exchange_id, base_symbol, target_symbol)
);

CREATE INDEX IF NOT EXISTS idx_coins_binance_symbol ON coins (binance_symbol) WHERE binance_symbol IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_coin_snapshots_rank ON coin_snapshots (rank);
CREATE INDEX IF NOT EXISTS idx_markets_coin_id ON markets (coin_id);
