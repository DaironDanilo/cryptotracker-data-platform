#!/usr/bin/env python3
"""Recurring Binance incremental job -- Phase 4.

Runs every 5 minutes as a Cloud Run Job (triggered by Cloud Scheduler). This
is NOT the one-time historical backfill (backfill/binance-ohlcv, migration/
binance-ingest-function) -- those remain manual-only and are never
auto-triggered. This job's only purpose is keeping bronze_candles and
coin_snapshots current between backfills, using Binance's REST klines
endpoint (not the static data.binance.vision archives).

For every coin in Supabase Postgres `coins` with a non-null binance_symbol:
  1. Read `last_loaded_close_time` from `ingestion_watermarks` (default: 24h
     lookback if no watermark row exists yet -- this job is for staying
     current, not backfilling).
  2. Fetch klines from (watermark - overlap) forward to now. The overlap
     re-fetches the last couple of already-loaded candles, since the most
     recent candle in any prior window may have still been in-progress when
     last fetched.
  3. Sanity-check each row (price > 0, high >= low, volume >= 0); skip and
     log anything that fails rather than writing bad data.

All coins' new rows are accumulated in memory and loaded into bronze_candles
as ONE BigQuery load job for the whole run (not one job per coin) --
deliberate, to stay far away from the partition-modification-per-table-per-
day quota wall hit during the Phase 3 backfill. In steady state at this
interval, a run's data spans at most one or two MONTH partitions, so this
is a non-issue at this cadence regardless.

Each coin's latest CLOSED candle updates coin_snapshots.price_usd, guarded
against out-of-order writes (`WHERE coin_id = %s AND updated_at < %s`) --
never an unconditional SET. ingestion_watermarks is only updated after the
BigQuery load succeeds, so a mid-run failure can't advance the watermark
past data that was never actually persisted.

Env vars:
  DATABASE_URL                required, Supabase Postgres connection string
  GCP_PROJECT_ID               required
  GOOGLE_APPLICATION_CREDENTIALS  path to a service account key JSON (or
                                   ambient credentials on Cloud Run)
  BQ_DATASET                    optional, default "bronze"
  BQ_TABLE_NAME                optional, default "bronze_candles"
  INTERVAL                     optional, default "1h"
  DEFAULT_LOOKBACK_HOURS       optional, default 24
  OVERLAP_CANDLES               optional, default 2
"""
from __future__ import annotations

import os
import sys
import time
from datetime import datetime, timedelta, timezone

import psycopg
import requests
from google.cloud import bigquery

# NOT api.binance.com: Binance geo-blocks that host (HTTP 451) for GCP's
# outbound IP ranges regardless of region -- confirmed empirically when this
# job first ran on Cloud Run. data-api.binance.vision is Binance's own
# official public-market-data-only mirror (same domain family as
# data.binance.vision, which migration/binance-ingest-function already uses
# from Cloud Run without issue) -- same response format, not geo-blocked.
BINANCE_KLINES_URL = "https://data-api.binance.vision/api/v3/klines"
KLINES_LIMIT = 1000

# Binance kline array indices (per api.binance.com/api/v3/klines docs).
IDX_OPEN_TIME = 0
IDX_OPEN = 1
IDX_HIGH = 2
IDX_LOW = 3
IDX_CLOSE = 4
IDX_VOLUME = 5
IDX_CLOSE_TIME = 6
IDX_QUOTE_VOLUME = 7
IDX_TRADE_COUNT = 8


def env(name: str, default: str | None = None, required: bool = False) -> str:
    val = os.environ.get(name, default)
    if required and not val:
        print(f"{name} environment variable is required", file=sys.stderr)
        sys.exit(1)
    return val


def fetch_coins(database_url: str) -> list[dict]:
    with psycopg.connect(database_url, prepare_threshold=None) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT coin_id, binance_symbol FROM coins WHERE binance_symbol IS NOT NULL ORDER BY coin_id"
            )
            return [{"coin_id": r[0], "binance_symbol": r[1]} for r in cur.fetchall()]


def fetch_watermarks(database_url: str, interval: str) -> dict[str, datetime]:
    with psycopg.connect(database_url, prepare_threshold=None) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT coin_id, last_loaded_close_time FROM ingestion_watermarks WHERE interval = %s",
                (interval,),
            )
            return {r[0]: r[1] for r in cur.fetchall()}


def fetch_klines(symbol: str, interval: str, start_ms: int, end_ms: int) -> list[list]:
    rows: list[list] = []
    cursor = start_ms
    while cursor < end_ms:
        resp = requests.get(
            BINANCE_KLINES_URL,
            params={
                "symbol": symbol,
                "interval": interval,
                "startTime": cursor,
                "endTime": end_ms,
                "limit": KLINES_LIMIT,
            },
            timeout=30,
        )
        if resp.status_code == 429:
            time.sleep(5)
            continue
        resp.raise_for_status()
        batch = resp.json()
        if not batch:
            break
        rows.extend(batch)
        last_open = batch[-1][IDX_OPEN_TIME]
        if last_open <= cursor:
            break  # safety: avoid an infinite loop if Binance ever returns no forward progress
        cursor = last_open + 1
        if len(batch) < KLINES_LIMIT:
            break
    return rows


def row_is_sane(k: list) -> bool:
    try:
        price_open, high, low, close = float(k[IDX_OPEN]), float(k[IDX_HIGH]), float(k[IDX_LOW]), float(k[IDX_CLOSE])
        volume = float(k[IDX_VOLUME])
    except (TypeError, ValueError):
        return False
    if price_open <= 0 or high <= 0 or low <= 0 or close <= 0:
        return False
    if high < low:
        return False
    if volume < 0:
        return False
    return True


def kline_to_bq_row(k: list, coin_id: str, interval: str, publish_time: str, now_ms: int) -> dict:
    return {
        "coin_id": coin_id,
        "interval": interval,
        "open_time": k[IDX_OPEN_TIME] / 1000.0,
        "close_time": k[IDX_CLOSE_TIME] / 1000.0,
        "open": k[IDX_OPEN],
        "high": k[IDX_HIGH],
        "low": k[IDX_LOW],
        "close": k[IDX_CLOSE],
        "volume": k[IDX_VOLUME],
        "quote_volume": k[IDX_QUOTE_VOLUME],
        "trade_count": int(k[IDX_TRADE_COUNT]),
        "is_closed": k[IDX_CLOSE_TIME] < now_ms,
        "publish_time": publish_time,
    }


def load_rows_to_bigquery(project_id: str, dataset: str, table: str, rows: list[dict]) -> None:
    if not rows:
        print("no new rows to load")
        return
    client = bigquery.Client(project=project_id)
    table_ref = f"{project_id}.{dataset}.{table}"
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        # Explicit CREATE_NEVER: this job should only ever append to the
        # existing bronze_candles table, never create one. Without this,
        # BigQuery's default CREATE_IF_NEEDED disposition requires
        # bigquery.tables.create on the *dataset* even when the table
        # already exists -- broader than this job's runtime SA should need.
        create_disposition=bigquery.CreateDisposition.CREATE_NEVER,
    )
    job = client.load_table_from_json(rows, table_ref, job_config=job_config)
    job.result()
    print(f"loaded {len(rows)} rows into {table_ref}")


def upsert_snapshot_price(database_url: str, coin_id: str, price_usd: str, updated_at: datetime) -> None:
    with psycopg.connect(database_url, prepare_threshold=None) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO coin_snapshots (coin_id, price_usd, updated_at, price_source)
                VALUES (%s, %s, %s, 'binance_rest')
                ON CONFLICT (coin_id) DO UPDATE SET
                    price_usd = EXCLUDED.price_usd,
                    updated_at = EXCLUDED.updated_at,
                    price_source = EXCLUDED.price_source
                WHERE coin_snapshots.updated_at < EXCLUDED.updated_at
                """,
                (coin_id, price_usd, updated_at),
            )
        conn.commit()


def update_watermark(database_url: str, coin_id: str, interval: str, last_loaded_close_time: datetime) -> None:
    with psycopg.connect(database_url, prepare_threshold=None) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO ingestion_watermarks (coin_id, interval, last_loaded_close_time, updated_at)
                VALUES (%s, %s, %s, now())
                ON CONFLICT (coin_id, interval) DO UPDATE SET
                    last_loaded_close_time = EXCLUDED.last_loaded_close_time,
                    updated_at = now()
                """,
                (coin_id, interval, last_loaded_close_time),
            )
        conn.commit()


def main() -> None:
    database_url = env("DATABASE_URL", required=True)
    project_id = env("GCP_PROJECT_ID", required=True)
    dataset = env("BQ_DATASET", "bronze")
    table = env("BQ_TABLE_NAME", "bronze_candles")
    interval = env("INTERVAL", "1h")
    default_lookback_hours = int(env("DEFAULT_LOOKBACK_HOURS", "24"))
    overlap_candles = int(env("OVERLAP_CANDLES", "2"))

    now = datetime.now(timezone.utc)
    now_ms = int(now.timestamp() * 1000)
    publish_time = now.isoformat()

    coins = fetch_coins(database_url)
    print(f"tracking {len(coins)} coins with a binance_symbol")
    watermarks = fetch_watermarks(database_url, interval)

    interval_hours = 1 if interval == "1h" else None
    if interval_hours is None:
        print(f"unsupported INTERVAL={interval!r} for overlap calculation", file=sys.stderr)
        sys.exit(1)

    all_rows: list[dict] = []
    latest_close_per_coin: dict[str, tuple[datetime, str]] = {}
    new_watermark_per_coin: dict[str, datetime] = {}
    skipped = 0

    for coin in coins:
        coin_id, symbol = coin["coin_id"], coin["binance_symbol"]
        watermark = watermarks.get(coin_id)
        if watermark is None:
            start = now - timedelta(hours=default_lookback_hours)
        else:
            start = watermark - timedelta(hours=overlap_candles * interval_hours)
        start_ms = int(start.timestamp() * 1000)

        try:
            klines = fetch_klines(symbol, interval, start_ms, now_ms)
        except requests.RequestException as e:
            print(f"  {coin_id}: FAILED to fetch klines: {e}", file=sys.stderr)
            continue

        coin_rows = []
        max_close_ms = None
        for k in klines:
            if not row_is_sane(k):
                skipped += 1
                continue
            coin_rows.append(kline_to_bq_row(k, coin_id, interval, publish_time, now_ms))
            close_ms = k[IDX_CLOSE_TIME]
            if close_ms < now_ms and (max_close_ms is None or close_ms > max_close_ms):
                max_close_ms = close_ms
                latest_close_per_coin[coin_id] = (
                    datetime.fromtimestamp(close_ms / 1000.0, tz=timezone.utc),
                    k[IDX_CLOSE],
                )

        if coin_rows:
            all_rows.extend(coin_rows)
            last_row_close_ms = max(k[IDX_CLOSE_TIME] for k in klines if row_is_sane(k))
            new_watermark_per_coin[coin_id] = datetime.fromtimestamp(last_row_close_ms / 1000.0, tz=timezone.utc)

    print(f"fetched {len(all_rows)} sane rows across {len(coins)} coins ({skipped} rows skipped as insane)")

    load_rows_to_bigquery(project_id, dataset, table, all_rows)

    for coin_id, watermark_close in new_watermark_per_coin.items():
        update_watermark(database_url, coin_id, interval, watermark_close)

    updated = 0
    for coin_id, (close_time, close_price) in latest_close_per_coin.items():
        upsert_snapshot_price(database_url, coin_id, close_price, close_time)
        updated += 1
    print(f"upserted coin_snapshots.price_usd for {updated} coins")


if __name__ == "__main__":
    main()
