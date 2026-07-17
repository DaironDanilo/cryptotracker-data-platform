# coin-registry (Phase 2)

PostgreSQL schema + coin registry seed job. Implemented in Python (not Kotlin)
since it's a one-shot schema + batch job, not a long-running JVM service.

## Schema

- **coins** — registry, one row per tracked coin (CoinGecko id as PK).
- **coin_snapshots** — current/live state, one row per coin, NOT history.
  Not populated by the seed job in this phase. Live: kept current by
  `recurring/binance-incremental-job` (price_usd, every 5 min, for
  Binance-tradeable coins) and `recurring/coingecko-incremental-job`
  (market_cap_usd/rank/change_percent_24h, hourly, and price_usd for coins
  with no Binance USDT pair) — see `recurring/README.md`. A WebSocket-worker
  / reconciliation-job path was originally planned for this (Phase 5/6,
  `ws-worker/`, `reconciliation-job/`) but is deferred and not yet built;
  the REST-based recurring jobs above are the actual live price source
  today.
- **markets** — exchange-level pair listings for a "Markets" tab, keyed on
  (coin_id, exchange_id, base_symbol, target_symbol). Kept current by
  `coingecko-incremental-job`.

All price/volume columns are `numeric`, never float/double.

See [`migrations/`](migrations/) for the full DDL: `0001_init.sql` (this
phase's schema above), `0002_recurring_etl.sql` (Phase 4 — adds
`ingestion_watermarks`, extends the `price_source` check constraint for the
live recurring jobs), and `0003_candle_rollups.sql` (Phase 8 — the
`candle_rollups_hourly`/`candle_rollups_daily` Lambda-architecture tables
the app's `:server` backend reads).

## Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
# edit .env: set DATABASE_URL (and COINGECKO_API_KEY if you have a demo/pro key)
export $(grep -v '^#' .env | xargs)
```

## Run migrations

```bash
python3 migrations/run_migrations.py
```

Safe to re-run: applied migrations are tracked in `schema_migrations` and
skipped, and every DDL statement also uses `IF NOT EXISTS` guards.

## Seed the coins table

```bash
python3 src/seed_coins.py --top-n 150
```

- Fetches the top N coins by market cap from CoinGecko `/coins/markets`
  (`--top-n`, env-configurable via the CLI flag, default 150).
- Cross-references Binance `/api/v3/exchangeInfo`, matching each coin's
  symbol as the **base asset** against **USDT-quoted, status=TRADING**
  pairs, to fill `binance_symbol`.
- Coins with no such pair (stablecoins like USDT/USDC, RWA-style tokens,
  etc.) are left with `binance_symbol = NULL` -- this is expected and
  correct, not a bug.
- Upserts by `coin_id`, so re-running is safe and picks up rank/registry
  changes.

### Notes on the Binance match

Verified directly against a live pull of `/api/v3/exchangeInfo` (3636 symbols
at check time): among USDT-quoted pairs with `status == "TRADING"`, no base
asset has more than one such pair, so the match is unambiguous -- no
tie-breaking logic was needed.
