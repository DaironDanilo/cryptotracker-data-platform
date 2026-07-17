# backfill/binance-ohlcv (Phase 3)

One-time historical OHLCV backfill from [data.binance.vision](https://data.binance.vision)
(the archive documented at [github.com/binance/binance-public-data](https://github.com/binance/binance-public-data))
into BigQuery `bronze_candles`. Runs for every coin in the Postgres `coins`
table (Phase 2) that has a non-null `binance_symbol`.

## What it does

1. Reads `coin_id, binance_symbol` from Postgres where `binance_symbol IS NOT NULL`.
2. For each symbol, lists available **monthly** kline archives (full months)
   via the public S3 listing API, plus **daily** archives for the current,
   not-yet-monthly-archived month.
3. Downloads each zip, parses the CSV, and appends rows to BigQuery
   `bronze_candles` (created on first run, partitioned on `open_time`).
4. Sets `is_closed = true` on every row (these are all closed historical
   candles) and `publish_time` to this run's start time.

`bronze_candles` is a plain append-only log -- a later real-time phase writes
to the same table continuously, and dedup happens downstream in a silver
view, not here. This job's own idempotency comes from a local SQLite ledger
(`.progress.sqlite3`, gitignored) that records every source file once loaded;
re-running skips files already recorded there.

## A critical gotcha found while building this

Binance's kline CSVs are **not consistently in milliseconds**. Diffing a few
monthly archives directly: `2017-08` and `2023-01` use 13-digit millisecond
epoch timestamps; `2025-01` onward uses 16-digit **microsecond** epoch
timestamps. The switchover happened sometime in 2023-2025. `to_epoch_seconds()`
detects the unit per value by magnitude (`> 10**14` => microseconds) rather
than assuming a fixed format -- get this wrong and every backfilled candle
before or after the switch lands on the wrong date.

## Setup

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env   # fill in DATABASE_URL, GCP_PROJECT_ID, GOOGLE_APPLICATION_CREDENTIALS
export $(grep -v '^#' .env | xargs)
```

## Run

```bash
python3 src/backfill_ohlcv.py --interval 1h
```

- `--interval` (default `1h`): any Binance kline interval (`1m`, `1h`, `1d`, ...).
  Pulls full available history for that interval by default.
- `--start-year-month YYYY-MM`: skip archives before this month, if you don't
  want the full history.
- `--limit-coins N`: only process the first N coins (useful for a quick test run).

Safe to re-run or Ctrl-C and resume -- already-loaded files are skipped via
`.progress.sqlite3`.
