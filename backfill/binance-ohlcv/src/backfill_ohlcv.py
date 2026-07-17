#!/usr/bin/env python3
"""One-time historical OHLCV backfill: data.binance.vision -> BigQuery bronze_candles.

For every coin in the Postgres `coins` table with a non-null `binance_symbol`,
downloads Binance's public monthly kline archives (full months) plus daily
archives (to cover the current, not-yet-archived-monthly month), parses the
CSVs, and appends them to BigQuery `bronze_candles`. is_closed is always true
here -- these are all closed historical candles.

Coins are processed concurrently (bounded by --max-concurrent-coins):
data.binance.vision is static file hosting with no meaningful per-IP rate
limit, so this is safe to parallelize aggressively.

Re-run safety: this job does NOT rely on the BigQuery table to dedupe (a
later phase appends to this same table continuously; bronze_candles is a
plain append-only log). Instead, a local SQLite ledger records every source
file (e.g. "BTCUSDT-1h-2023-01.zip") once it's been successfully loaded, and
a resumed run skips files already recorded there.

Rows for all of a coin's not-yet-loaded files are accumulated in memory and
loaded into BigQuery in a single job per coin (rather than one job per file),
to avoid paying BigQuery's job-scheduling/polling overhead per file. A file
is only marked done in the ledger *after* that coin-level load succeeds --
not as soon as its rows are accumulated -- so a kill mid-coin can't leave the
ledger claiming a file is loaded when its rows were actually never persisted.
The cost of that safety is that a kill mid-coin re-downloads that coin's
files on resume (cheap: it's just re-fetching static zips); other, already-
fully-loaded coins are unaffected.

Env vars:
  DATABASE_URL                    required, Postgres connection string
  GCP_PROJECT_ID                  required
  GOOGLE_APPLICATION_CREDENTIALS  path to a service account key JSON
  BQ_DATASET                    optional, default "bronze"
  BQ_TABLE_NAME                   optional, default "bronze_candles"
"""
from __future__ import annotations

import argparse
import io
import json
import os
import sqlite3
import sys
import threading
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from xml.etree import ElementTree

import psycopg
import requests
from google.cloud import bigquery

ARCHIVE_HOST = "https://data.binance.vision"
LISTING_HOST = "https://s3-ap-northeast-1.amazonaws.com/data.binance.vision"
TABLE_NAME = os.environ.get("BQ_TABLE_NAME", "bronze_candles")
PROGRESS_DB_PATH = Path(__file__).parent.parent / ".progress.sqlite3"
DEFAULT_MAX_CONCURRENT_COINS = 12

# Binance switched historical kline timestamps from milliseconds to
# microseconds partway through 2025 (confirmed by diffing 2023-01 vs 2025-01
# monthly archives: 13-digit vs 16-digit open_time values). Millisecond epoch
# values stay 13 digits until the year 2286; microsecond epoch values are
# already 16 digits. So: anything with more than 14 digits is microseconds.
MICROSECOND_THRESHOLD = 10**14

S3_NS = "{http://s3.amazonaws.com/doc/2006-03-01/}"


@dataclass
class Coin:
    coin_id: str
    binance_symbol: str


def load_coins(database_url: str) -> list[Coin]:
    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT coin_id, binance_symbol FROM coins "
                "WHERE binance_symbol IS NOT NULL ORDER BY coin_id"
            )
            return [Coin(coin_id=r[0], binance_symbol=r[1]) for r in cur.fetchall()]


def list_archive_files(prefix: str) -> list[str]:
    """Lists all object keys under a data.binance.vision prefix (paginated)."""
    keys: list[str] = []
    marker = ""
    while True:
        params = {"delimiter": "/", "prefix": prefix, "max-keys": "1000"}
        if marker:
            params["marker"] = marker
        resp = requests.get(LISTING_HOST, params=params, timeout=30)
        resp.raise_for_status()
        root = ElementTree.fromstring(resp.content)
        page_keys = [c.findtext(f"{S3_NS}Key") for c in root.findall(f"{S3_NS}Contents")]
        keys.extend(page_keys)
        is_truncated = root.findtext(f"{S3_NS}IsTruncated") == "true"
        if not is_truncated or not page_keys:
            break
        marker = page_keys[-1]
    return [k for k in keys if k.endswith(".zip")]


def download_and_parse_csv(key: str) -> list[list[str]]:
    resp = requests.get(f"{ARCHIVE_HOST}/{key}", timeout=60)
    resp.raise_for_status()
    with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
        csv_name = next(n for n in zf.namelist() if n.endswith(".csv"))
        text = zf.read(csv_name).decode("utf-8")
    rows = [line.split(",") for line in text.splitlines() if line.strip()]
    return rows


def to_epoch_seconds(raw: str) -> float:
    value = int(raw)
    if value > MICROSECOND_THRESHOLD:
        return value / 1_000_000
    return value / 1_000


def rows_to_bq_json(
    rows: list[list[str]], coin_id: str, interval: str, publish_time: str
) -> list[dict]:
    out = []
    for r in rows:
        # CSV columns: open_time,open,high,low,close,volume,close_time,
        #              quote_volume,count,taker_buy_vol,taker_buy_quote_vol,ignore
        out.append(
            {
                "coin_id": coin_id,
                "interval": interval,
                "open_time": to_epoch_seconds(r[0]),
                "close_time": to_epoch_seconds(r[6]),
                "open": r[1],
                "high": r[2],
                "low": r[3],
                "close": r[4],
                "volume": r[5],
                "quote_volume": r[7],
                "trade_count": int(r[8]),
                "is_closed": True,
                "publish_time": publish_time,
            }
        )
    return out


class ProgressLedger:
    """Thread-safe: every method call is serialized behind a single lock.
    These are quick, infrequent sqlite ops (now one batch of marks per coin
    load, not per file) -- the real bottleneck is network I/O, so this lock
    costs nothing in practice."""

    def __init__(self, db_path: Path):
        self._lock = threading.Lock()
        self.conn = sqlite3.connect(db_path, check_same_thread=False, timeout=30)
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS loaded_files (
                coin_id text NOT NULL,
                interval text NOT NULL,
                source_file text NOT NULL,
                loaded_at text NOT NULL,
                PRIMARY KEY (coin_id, interval, source_file)
            )
            """
        )
        self.conn.commit()

    def is_done(self, coin_id: str, interval: str, source_file: str) -> bool:
        with self._lock:
            cur = self.conn.execute(
                "SELECT 1 FROM loaded_files WHERE coin_id=? AND interval=? AND source_file=?",
                (coin_id, interval, source_file),
            )
            return cur.fetchone() is not None

    def mark_done_batch(self, coin_id: str, interval: str, source_files: list[str]) -> None:
        if not source_files:
            return
        now = datetime.now(timezone.utc).isoformat()
        with self._lock:
            self.conn.executemany(
                "INSERT OR IGNORE INTO loaded_files (coin_id, interval, source_file, loaded_at) "
                "VALUES (?, ?, ?, ?)",
                [(coin_id, interval, f, now) for f in source_files],
            )
            self.conn.commit()


def ensure_table(client: bigquery.Client, dataset_id: str) -> bigquery.TableReference:
    dataset_ref = bigquery.DatasetReference(client.project, dataset_id)
    client.create_dataset(bigquery.Dataset(dataset_ref), exists_ok=True)

    table_ref = dataset_ref.table(TABLE_NAME)
    schema = [
        bigquery.SchemaField("coin_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("interval", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("open_time", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("close_time", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("open", "NUMERIC"),
        bigquery.SchemaField("high", "NUMERIC"),
        bigquery.SchemaField("low", "NUMERIC"),
        bigquery.SchemaField("close", "NUMERIC"),
        bigquery.SchemaField("volume", "NUMERIC"),
        bigquery.SchemaField("quote_volume", "NUMERIC"),
        bigquery.SchemaField("trade_count", "INT64"),
        bigquery.SchemaField("is_closed", "BOOL"),
        bigquery.SchemaField("publish_time", "TIMESTAMP", mode="REQUIRED"),
    ]
    table = bigquery.Table(table_ref, schema=schema)
    # Partitioned on event time (open_time), not ingestion time: a later
    # real-time phase writes to this same table continuously, and event-time
    # partitioning is what keeps late-arriving reconciliation rows landing in
    # their correct historical partition instead of piling into "today".
    #
    # MONTH granularity, not DAY: with DAY partitioning, a single coin's full
    # history (e.g. bitcoin's ~9 years) touches ~3,250 distinct partitions in
    # one load job. Loading many coins hits BigQuery's per-table daily quota
    # on partition modifications (documented at 30,000/day) after only
    # ~20-25 coins, regardless of concurrency -- it's cumulative across every
    # job that touches the table, not a per-job limit. MONTH partitioning
    # cuts that by ~30x, keeping the full ~87-coin backfill comfortably
    # under quota in a single run.
    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.MONTH, field="open_time"
    )
    client.create_table(table, exists_ok=True)
    return table_ref


def load_rows(client: bigquery.Client, table_ref: bigquery.TableReference, rows: list[dict]) -> None:
    buf = io.StringIO()
    for row in rows:
        buf.write(json.dumps(row))
        buf.write("\n")
    buf.seek(0)
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )
    job = client.load_table_from_file(buf, table_ref, job_config=job_config)
    job.result()


def backfill_coin(
    coin: Coin,
    interval: str,
    start_year_month: str | None,
    ledger: ProgressLedger,
    client: bigquery.Client,
    table_ref: bigquery.TableReference,
    publish_time: str,
) -> str:
    symbol = coin.binance_symbol
    monthly_prefix = f"data/spot/monthly/klines/{symbol}/{interval}/"
    monthly_keys = sorted(k for k in list_archive_files(monthly_prefix) if not k.endswith(".CHECKSUM"))
    if start_year_month:
        monthly_keys = [k for k in monthly_keys if _year_month_from_key(k) >= start_year_month]

    daily_prefix = f"data/spot/daily/klines/{symbol}/{interval}/"
    daily_keys = sorted(k for k in list_archive_files(daily_prefix) if not k.endswith(".CHECKSUM"))
    # Only use daily files for the partial month not yet covered by a monthly archive,
    # to avoid double-loading the same candles from both archive granularities.
    last_monthly_ym = _year_month_from_key(monthly_keys[-1]) if monthly_keys else None
    if last_monthly_ym:
        daily_keys = [k for k in daily_keys if _year_month_from_key(k) > last_monthly_ym]

    all_keys = monthly_keys + daily_keys
    skip_count = 0
    pending_rows: list[dict] = []
    pending_files: list[str] = []
    for key in all_keys:
        filename = key.rsplit("/", 1)[-1]
        if ledger.is_done(coin.coin_id, interval, filename):
            skip_count += 1
            continue
        rows = download_and_parse_csv(key)
        if rows:
            pending_rows.extend(rows_to_bq_json(rows, coin.coin_id, interval, publish_time))
        pending_files.append(filename)

    if pending_rows:
        load_rows(client, table_ref, pending_rows)
    # Only mark files done once their rows are actually persisted in BigQuery
    # (see module docstring) -- not while they were merely accumulated.
    ledger.mark_done_batch(coin.coin_id, interval, pending_files)

    summary = (
        f"  {coin.coin_id} ({symbol}): {len(all_keys)} files total, "
        f"{len(pending_files)} newly loaded, {skip_count} already done"
    )
    print(summary, flush=True)
    return summary


def _year_month_from_key(key: str) -> str:
    # Filenames are "SYMBOL-INTERVAL-YYYY-MM.zip" (monthly) or
    # "SYMBOL-INTERVAL-YYYY-MM-DD.zip" (daily) -- year/month are always at
    # positions 2/3, regardless of whether a day suffix follows.
    stem = key.rsplit("/", 1)[-1].removesuffix(".zip")
    parts = stem.split("-")
    return f"{parts[2]}-{parts[3]}"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--interval", default="1h", help="Binance kline interval (default: 1h)")
    parser.add_argument(
        "--start-year-month",
        default=None,
        help="Skip monthly archives before this YYYY-MM (default: all available history)",
    )
    parser.add_argument("--limit-coins", type=int, default=None, help="For testing: only process the first N coins")
    parser.add_argument(
        "--max-concurrent-coins",
        type=int,
        default=DEFAULT_MAX_CONCURRENT_COINS,
        help=f"How many coins to backfill in parallel (default: {DEFAULT_MAX_CONCURRENT_COINS})",
    )
    args = parser.parse_args()

    database_url = os.environ.get("DATABASE_URL")
    project_id = os.environ.get("GCP_PROJECT_ID")
    dataset_id = os.environ.get("BQ_DATASET", "bronze")
    if not database_url or not project_id:
        print("DATABASE_URL and GCP_PROJECT_ID environment variables are required", file=sys.stderr)
        sys.exit(1)

    coins = load_coins(database_url)
    if args.limit_coins:
        coins = coins[: args.limit_coins]
    print(f"backfilling {len(coins)} coins at interval={args.interval}, "
          f"max_concurrent_coins={args.max_concurrent_coins}")

    client = bigquery.Client(project=project_id)
    table_ref = ensure_table(client, dataset_id)
    ledger = ProgressLedger(PROGRESS_DB_PATH)
    publish_time = datetime.now(timezone.utc).isoformat()

    with ThreadPoolExecutor(max_workers=args.max_concurrent_coins) as pool:
        futures = {
            pool.submit(
                backfill_coin, coin, args.interval, args.start_year_month, ledger, client, table_ref, publish_time
            ): coin
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
