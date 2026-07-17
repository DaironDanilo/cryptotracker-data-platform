#!/usr/bin/env python3
"""One-time historical market-cap/price/volume backfill: CoinGecko -> BigQuery market_cap_history.

For every coin in the Postgres `coins` table, calls CoinGecko's
`/coins/{id}/market_chart/range`, chunked into <=90-day windows, covering up
to 365 days back from today (the free tier does not allow querying further
back than that). Upserts into BigQuery `market_cap_history` via a staging
table + MERGE, keyed on (coin_id, timestamp) -- safe to re-run.

Coins are processed concurrently (bounded by --max-concurrent-coins), all
sharing a single rate limiter capped at --requests-per-minute (default 40,
within CoinGecko's free-tier ~100/min budget) so concurrent workers can't
collectively exceed it. It also tracks a persistent per-calendar-month
request counter so repeated runs within the same month don't blow through
the 10k/month cap, and backs off exponentially on HTTP 429/5xx.

Each merge uses its own uniquely-named staging table (created and dropped
per call) rather than one shared staging table -- with concurrent coins in
flight, a shared staging table would have one coin's WRITE_TRUNCATE load
clobber another coin's rows before its MERGE ran. The final MERGE into the
shared `market_cap_history` table is additionally serialized behind a lock:
BigQuery surfaces concurrent mutating DML against the same table as a 400
"concurrent update" error, so only one coin's MERGE runs at a time -- fetching
from CoinGecko and loading into each coin's own staging table stay concurrent.

Env vars:
  DATABASE_URL                     required, Postgres connection string
  GCP_PROJECT_ID                   required
  GOOGLE_APPLICATION_CREDENTIALS   path to a service account key JSON
  BQ_DATASET                    optional, default "bronze"
  COINGECKO_API_KEY                optional; sent as the x-cg-demo-api-key header
  COINGECKO_MONTHLY_REQUEST_CAP    optional, default 10000
"""
from __future__ import annotations

import argparse
import io
import json
import os
import sqlite3
import sys
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import psycopg
import requests
from google.cloud import bigquery

COINGECKO_BASE_URL = "https://api.coingecko.com/api/v3"
STAGING_TABLE_PREFIX = "_market_cap_history_staging"
TARGET_TABLE_NAME = "market_cap_history"
PROGRESS_DB_PATH = Path(__file__).parent.parent / ".progress.sqlite3"
CHUNK_DAYS = 90
DEFAULT_TOTAL_DAYS = 365
DEFAULT_REQUESTS_PER_MINUTE = 40
DEFAULT_MAX_CONCURRENT_COINS = 10


@dataclass
class Coin:
    coin_id: str


def load_coins(database_url: str) -> list[Coin]:
    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT coin_id FROM coins ORDER BY coin_id")
            return [Coin(coin_id=r[0]) for r in cur.fetchall()]


class RateLimiter:
    """Shared across all concurrent coin workers: caps requests/minute and
    tracks a persistent monthly request budget. All state mutation happens
    under a single lock, so concurrent workers can't collectively burst past
    the per-minute cap or the monthly cap."""

    def __init__(self, db_path: Path, requests_per_minute: int, monthly_cap: int):
        self._lock = threading.Lock()
        self.conn = sqlite3.connect(db_path, check_same_thread=False, timeout=30)
        self.min_interval = 60.0 / requests_per_minute
        self.monthly_cap = monthly_cap
        self._last_request_at = 0.0
        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS api_calls (year_month TEXT PRIMARY KEY, count INTEGER NOT NULL)"
        )
        self.conn.commit()

    def before_request(self) -> None:
        with self._lock:
            ym = datetime.now(timezone.utc).strftime("%Y-%m")
            row = self.conn.execute("SELECT count FROM api_calls WHERE year_month=?", (ym,)).fetchone()
            if (row[0] if row else 0) >= self.monthly_cap:
                raise RuntimeError(
                    f"monthly CoinGecko request cap ({self.monthly_cap}) reached -- "
                    "stopping to avoid exceeding the free-tier quota; resume next month "
                    "or raise COINGECKO_MONTHLY_REQUEST_CAP"
                )
            elapsed = time.monotonic() - self._last_request_at
            if elapsed < self.min_interval:
                time.sleep(self.min_interval - elapsed)
            self._last_request_at = time.monotonic()
            self.conn.execute(
                "INSERT INTO api_calls (year_month, count) VALUES (?, 1) "
                "ON CONFLICT(year_month) DO UPDATE SET count = count + 1",
                (ym,),
            )
            self.conn.commit()


def fetch_market_chart_range(
    coin_id: str, from_ts: int, to_ts: int, api_key: str | None, limiter: RateLimiter
) -> dict:
    headers = {"x-cg-demo-api-key": api_key} if api_key else {}
    url = f"{COINGECKO_BASE_URL}/coins/{coin_id}/market_chart/range"
    params = {"vs_currency": "usd", "from": from_ts, "to": to_ts}

    max_attempts = 6
    for attempt in range(1, max_attempts + 1):
        limiter.before_request()
        resp = requests.get(url, headers=headers, params=params, timeout=30)
        if resp.status_code == 429 or resp.status_code >= 500:
            wait = min(2**attempt, 60)
            print(f"    {coin_id}: got HTTP {resp.status_code}, backing off {wait}s (attempt {attempt}/{max_attempts})", flush=True)
            time.sleep(wait)
            continue
        resp.raise_for_status()
        return resp.json()
    resp.raise_for_status()
    return {}


def merge_rows_into_target(
    client: bigquery.Client, dataset_id: str, rows: list[dict], merge_lock: threading.Lock
) -> None:
    if not rows:
        return
    dataset_ref = bigquery.DatasetReference(client.project, dataset_id)
    # Unique per call: concurrent coins must not share a staging table, or one
    # coin's WRITE_TRUNCATE load would clobber another's rows before MERGE runs.
    staging_ref = dataset_ref.table(f"{STAGING_TABLE_PREFIX}_{uuid.uuid4().hex}")

    try:
        buf = "\n".join(json.dumps(r) for r in rows)
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            schema=_market_cap_schema(),
        )
        load_job = client.load_table_from_file(io.StringIO(buf), staging_ref, job_config=job_config)
        load_job.result()

        merge_sql = f"""
            MERGE `{client.project}.{dataset_id}.{TARGET_TABLE_NAME}` T
            USING `{client.project}.{dataset_id}.{staging_ref.table_id}` S
            ON T.coin_id = S.coin_id AND T.timestamp = S.timestamp
            WHEN MATCHED THEN UPDATE SET
                price_usd = S.price_usd,
                market_cap_usd = S.market_cap_usd,
                total_volume_usd = S.total_volume_usd
            WHEN NOT MATCHED THEN
                INSERT (coin_id, timestamp, price_usd, market_cap_usd, total_volume_usd)
                VALUES (S.coin_id, S.timestamp, S.price_usd, S.market_cap_usd, S.total_volume_usd)
        """
        # BigQuery does not reliably support concurrent mutating DML against
        # the same table -- two coins' MERGEs racing here surface as a 400
        # "Could not serialize access ... due to concurrent update". Loads
        # into each coin's own staging table stay concurrent; only the
        # write into the shared target table is serialized.
        with merge_lock:
            client.query(merge_sql).result()
    finally:
        client.delete_table(staging_ref, not_found_ok=True)


def _market_cap_schema() -> list[bigquery.SchemaField]:
    return [
        bigquery.SchemaField("coin_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("timestamp", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("price_usd", "NUMERIC"),
        bigquery.SchemaField("market_cap_usd", "NUMERIC"),
        bigquery.SchemaField("total_volume_usd", "NUMERIC"),
    ]


def ensure_tables(client: bigquery.Client, dataset_id: str) -> None:
    dataset_ref = bigquery.DatasetReference(client.project, dataset_id)
    client.create_dataset(bigquery.Dataset(dataset_ref), exists_ok=True)

    target = bigquery.Table(dataset_ref.table(TARGET_TABLE_NAME), schema=_market_cap_schema())
    target.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY, field="timestamp"
    )
    client.create_table(target, exists_ok=True)


def response_to_rows(coin_id: str, payload: dict) -> list[dict]:
    prices = {p[0]: p[1] for p in payload.get("prices", [])}
    market_caps = {p[0]: p[1] for p in payload.get("market_caps", [])}
    volumes = {p[0]: p[1] for p in payload.get("total_volumes", [])}
    rows = []
    for ts_ms in prices:
        rows.append(
            {
                "coin_id": coin_id,
                "timestamp": ts_ms / 1000,
                "price_usd": _fmt_numeric(prices.get(ts_ms)),
                "market_cap_usd": _fmt_numeric(market_caps.get(ts_ms)),
                "total_volume_usd": _fmt_numeric(volumes.get(ts_ms)),
            }
        )
    return rows


def _fmt_numeric(value: float | None) -> str | None:
    # BigQuery NUMERIC caps scale at 9 decimal digits, but CoinGecko's raw
    # float serialization emits up to ~18 for low-priced coins (e.g.
    # 0.013277540018905517) -- format to a fixed 9-decimal string up front
    # rather than passing the raw float and letting the load job reject it.
    if value is None:
        return None
    return f"{value:.9f}"


def build_windows(total_days: int, chunk_days: int, end_date: date) -> list[tuple[date, date]]:
    """Calendar-date windows, newest first, computed from a fixed end_date so
    resumed runs on a different day still line up with previously completed
    windows."""
    windows = []
    cursor_end = end_date
    remaining = total_days
    while remaining > 0:
        span = min(chunk_days, remaining)
        cursor_start = cursor_end - timedelta(days=span)
        windows.append((cursor_start, cursor_end))
        cursor_end = cursor_start
        remaining -= span
    return windows


class ProgressLedger:
    """Thread-safe: its own sqlite connection + lock, independent of RateLimiter's."""

    def __init__(self, db_path: Path):
        self._lock = threading.Lock()
        self.conn = sqlite3.connect(db_path, check_same_thread=False, timeout=30)
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS completed_windows (
                coin_id text NOT NULL,
                window_start text NOT NULL,
                window_end text NOT NULL,
                completed_at text NOT NULL,
                PRIMARY KEY (coin_id, window_start, window_end)
            )
            """
        )
        self.conn.commit()

    def is_done(self, coin_id: str, window_start: date, window_end: date) -> bool:
        with self._lock:
            cur = self.conn.execute(
                "SELECT 1 FROM completed_windows WHERE coin_id=? AND window_start=? AND window_end=?",
                (coin_id, window_start.isoformat(), window_end.isoformat()),
            )
            return cur.fetchone() is not None

    def mark_done(self, coin_id: str, window_start: date, window_end: date) -> None:
        with self._lock:
            self.conn.execute(
                "INSERT OR IGNORE INTO completed_windows (coin_id, window_start, window_end, completed_at) "
                "VALUES (?, ?, ?, ?)",
                (coin_id, window_start.isoformat(), window_end.isoformat(), datetime.now(timezone.utc).isoformat()),
            )
            self.conn.commit()


def backfill_coin(
    coin: Coin,
    windows: list[tuple[date, date]],
    api_key: str | None,
    ledger: ProgressLedger,
    limiter: RateLimiter,
    client: bigquery.Client,
    dataset_id: str,
    merge_lock: threading.Lock,
) -> str:
    new_windows = 0
    skip_windows = 0
    for window_start, window_end in windows:
        if ledger.is_done(coin.coin_id, window_start, window_end):
            skip_windows += 1
            continue
        from_ts = int(datetime(window_start.year, window_start.month, window_start.day, tzinfo=timezone.utc).timestamp())
        to_ts = int(datetime(window_end.year, window_end.month, window_end.day, tzinfo=timezone.utc).timestamp())
        try:
            payload = fetch_market_chart_range(coin.coin_id, from_ts, to_ts, api_key, limiter)
        except requests.HTTPError as e:
            print(f"  {coin.coin_id}: HTTP error on window {window_start}..{window_end}: {e} -- skipping coin for now", flush=True)
            break
        rows = response_to_rows(coin.coin_id, payload)
        merge_rows_into_target(client, dataset_id, rows, merge_lock)
        ledger.mark_done(coin.coin_id, window_start, window_end)
        new_windows += 1

    summary = f"  {coin.coin_id}: {new_windows} new windows merged, {skip_windows} already done"
    print(summary, flush=True)
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--days", type=int, default=DEFAULT_TOTAL_DAYS, help="Total days of history to backfill (default: 365)")
    parser.add_argument(
        "--requests-per-minute",
        type=int,
        default=DEFAULT_REQUESTS_PER_MINUTE,
        help=f"Shared rate cap across all concurrent coins (default: {DEFAULT_REQUESTS_PER_MINUTE}, well under CoinGecko's 100/min)",
    )
    parser.add_argument("--limit-coins", type=int, default=None, help="For testing: only process the first N coins")
    parser.add_argument(
        "--max-concurrent-coins",
        type=int,
        default=DEFAULT_MAX_CONCURRENT_COINS,
        help=f"How many coins to backfill in parallel (default: {DEFAULT_MAX_CONCURRENT_COINS}); "
        "throughput is actually bounded by --requests-per-minute, shared across all of them",
    )
    args = parser.parse_args()

    if args.days > 365:
        print("warning: CoinGecko's free tier does not serve data older than 365 days; clamping to 365", file=sys.stderr)
        args.days = 365

    database_url = os.environ.get("DATABASE_URL")
    project_id = os.environ.get("GCP_PROJECT_ID")
    dataset_id = os.environ.get("BQ_DATASET", "bronze")
    api_key = os.environ.get("COINGECKO_API_KEY")
    monthly_cap = int(os.environ.get("COINGECKO_MONTHLY_REQUEST_CAP", "10000"))
    if not database_url or not project_id:
        print("DATABASE_URL and GCP_PROJECT_ID environment variables are required", file=sys.stderr)
        sys.exit(1)

    coins = load_coins(database_url)
    if args.limit_coins:
        coins = coins[: args.limit_coins]
    print(
        f"backfilling market cap history for {len(coins)} coins, {args.days} days back, "
        f"max_concurrent_coins={args.max_concurrent_coins}, requests_per_minute={args.requests_per_minute}"
    )

    client = bigquery.Client(project=project_id)
    ensure_tables(client, dataset_id)

    ledger = ProgressLedger(PROGRESS_DB_PATH)
    limiter = RateLimiter(PROGRESS_DB_PATH, args.requests_per_minute, monthly_cap)

    end_date = datetime.now(timezone.utc).date()
    windows = build_windows(args.days, CHUNK_DAYS, end_date)
    merge_lock = threading.Lock()

    with ThreadPoolExecutor(max_workers=args.max_concurrent_coins) as pool:
        futures = {
            pool.submit(backfill_coin, coin, windows, api_key, ledger, limiter, client, dataset_id, merge_lock): coin
            for coin in coins
        }
        for future in as_completed(futures):
            coin = futures[future]
            try:
                future.result()
            except Exception as e:
                print(f"  {coin.coin_id}: FAILED: {e}", file=sys.stderr, flush=True)

    print("backfill complete")


if __name__ == "__main__":
    main()
