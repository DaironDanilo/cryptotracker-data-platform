#!/usr/bin/env python3
"""Recurring candle daily sync job -- Phase 8 (Lambda architecture batch layer).

Runs once a day as a Cloud Run Job (triggered by Cloud Scheduler). Keeps
`candle_rollups_daily` in Supabase Postgres current from BigQuery's
`gold.daily_candle_metrics`, so the custom backend's history endpoint can
serve the 1M/6M/YTD/1Y chart ranges with a plain indexed Postgres query
instead of going through Cube/BigQuery on every request.

This is the "batch layer" half of the Lambda-architecture split: long
lookback (mirrors daily_candle_metrics.sqlx's own 3-day revision window,
with margin), long retention (13 months -- matches the app's max 1Y range
plus a little slack, and matches Cube's own pre-aggregation build range so
the two stay conceptually aligned even though Cube is no longer on this
serving path). The custom backend merges today's still-forming daily bar
from `candle_rollups_hourly` at read time, since this job only runs once a
day and today's row here can otherwise look stale/incomplete until the next
run.

Idempotent: upserts on (coin_id, bucket_start), safe to re-run or overlap
with a slow previous invocation.

Env vars:
  DATABASE_URL      required, Supabase Postgres connection string
  GCP_PROJECT_ID    required
  BQ_DATASET        optional, default "gold"
  BQ_TABLE_NAME     optional, default "daily_candle_metrics"
  INTERVAL          optional, default "1h" -- the interval daily_candle_metrics rolls up from
  LOOKBACK_DAYS     optional, default 5 -- must cover daily_candle_metrics.sqlx's own
                    3-day revision window with margin, not the full retention window
  RETENTION_MONTHS  optional, default 13 -- matches the app's max 1Y range plus slack
"""
from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta, timezone

import psycopg
from google.cloud import bigquery


def env(name: str, default: str | None = None, required: bool = False) -> str:
    val = os.environ.get(name, default)
    if required and not val:
        print(f"{name} environment variable is required", file=sys.stderr)
        sys.exit(1)
    return val


def fetch_daily_rollups(project_id: str, dataset: str, table: str, interval: str, lookback_days: int) -> list[dict]:
    client = bigquery.Client(project=project_id)
    query = f"""
        SELECT coin_id, day, open, high, low, close, volume
        FROM `{project_id}.{dataset}.{table}`
        WHERE `interval` = @interval
          AND day >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @lookback_days DAY)
        ORDER BY coin_id, day
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("interval", "STRING", interval),
            bigquery.ScalarQueryParameter("lookback_days", "INT64", lookback_days),
        ]
    )
    return [dict(row) for row in client.query(query, job_config=job_config).result()]


def upsert_rollups(database_url: str, rows: list[dict]) -> None:
    if not rows:
        print("no rows to upsert")
        return
    now = datetime.now(timezone.utc)
    values = [
        (
            row["coin_id"],
            row["day"],
            row["open"],
            row["high"],
            row["low"],
            row["close"],
            row["volume"],
            (row["day"] + timedelta(days=1)) <= now,
        )
        for row in rows
    ]
    with psycopg.connect(database_url, prepare_threshold=None) as conn:
        with conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO candle_rollups_daily
                    (coin_id, bucket_start, open, high, low, close, volume, is_closed, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, now())
                ON CONFLICT (coin_id, bucket_start) DO UPDATE SET
                    open = EXCLUDED.open,
                    high = EXCLUDED.high,
                    low = EXCLUDED.low,
                    close = EXCLUDED.close,
                    volume = EXCLUDED.volume,
                    is_closed = EXCLUDED.is_closed,
                    updated_at = now()
                """,
                values,
            )
        conn.commit()
    print(f"upserted {len(values)} daily rollup rows")


def prune_old_rows(database_url: str, retention_months: int) -> None:
    with psycopg.connect(database_url, prepare_threshold=None) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM candle_rollups_daily WHERE bucket_start < now() - (%s * interval '1 month')",
                (retention_months,),
            )
            deleted = cur.rowcount
        conn.commit()
    print(f"pruned {deleted} rows older than {retention_months} months")


def main() -> None:
    database_url = env("DATABASE_URL", required=True)
    project_id = env("GCP_PROJECT_ID", required=True)
    dataset = env("BQ_DATASET", "gold")
    table = env("BQ_TABLE_NAME", "daily_candle_metrics")
    interval = env("INTERVAL", "1h")
    lookback_days = int(env("LOOKBACK_DAYS", "5"))
    retention_months = int(env("RETENTION_MONTHS", "13"))

    rows = fetch_daily_rollups(project_id, dataset, table, interval, lookback_days)
    print(f"fetched {len(rows)} daily rollup rows from BigQuery (lookback={lookback_days}d)")

    upsert_rollups(database_url, rows)
    prune_old_rows(database_url, retention_months)


if __name__ == "__main__":
    main()
