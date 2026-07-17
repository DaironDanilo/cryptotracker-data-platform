#!/usr/bin/env python3
"""Recurring candle hourly sync job -- Phase 8 (Lambda architecture speed layer).

Runs every 5 minutes as a Cloud Run Job (triggered by Cloud Scheduler).
Keeps `candle_rollups_hourly` in Supabase Postgres current from BigQuery's
`gold.hourly_candle_metrics`, so the custom backend's history endpoint can
serve the 1D/5D chart ranges with a plain indexed Postgres query instead of
going through Cube/BigQuery on every request (which proved too slow/fragile
that was slow and fragile).

This is the "speed layer" half of the Lambda-architecture split: short
lookback (only the hours that could still be revised), short retention
(only as far back as the longest hourly-granularity range -- 5D -- plus a
safety margin, since `candle_rollups_daily`'s once-a-day sync covers
everything older, and the custom backend merges today's hourly rows into a
synthetic "today" daily candle at read time to stay fresh between daily
syncs).

Idempotent: upserts on (coin_id, bucket_start), safe to re-run or overlap
with a slow previous invocation.

Env vars:
  DATABASE_URL      required, Supabase Postgres connection string
  GCP_PROJECT_ID    required
  BQ_DATASET        optional, default "gold"
  BQ_TABLE_NAME     optional, default "hourly_candle_metrics"
  INTERVAL          optional, default "1h" -- must match binance-incremental-job's INTERVAL
  LOOKBACK_HOURS    optional, default 8 -- must cover hourly_candle_metrics.sqlx's own
                    6-hour revision window with margin, not the full retention window
  RETENTION_DAYS    optional, default 10 -- covers the 5D range plus margin
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


def fetch_hourly_rollups(project_id: str, dataset: str, table: str, interval: str, lookback_hours: int) -> list[dict]:
    client = bigquery.Client(project=project_id)
    query = f"""
        SELECT coin_id, hour, open, high, low, close, volume
        FROM `{project_id}.{dataset}.{table}`
        WHERE `interval` = @interval
          AND hour >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @lookback_hours HOUR)
        ORDER BY coin_id, hour
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("interval", "STRING", interval),
            bigquery.ScalarQueryParameter("lookback_hours", "INT64", lookback_hours),
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
            row["hour"],
            row["open"],
            row["high"],
            row["low"],
            row["close"],
            row["volume"],
            (row["hour"] + timedelta(hours=1)) <= now,
        )
        for row in rows
    ]
    with psycopg.connect(database_url, prepare_threshold=None) as conn:
        with conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO candle_rollups_hourly
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
    print(f"upserted {len(values)} hourly rollup rows")


def prune_old_rows(database_url: str, retention_days: int) -> None:
    with psycopg.connect(database_url, prepare_threshold=None) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM candle_rollups_hourly WHERE bucket_start < now() - (%s * interval '1 day')",
                (retention_days,),
            )
            deleted = cur.rowcount
        conn.commit()
    print(f"pruned {deleted} rows older than {retention_days} days")


def main() -> None:
    database_url = env("DATABASE_URL", required=True)
    project_id = env("GCP_PROJECT_ID", required=True)
    dataset = env("BQ_DATASET", "gold")
    table = env("BQ_TABLE_NAME", "hourly_candle_metrics")
    interval = env("INTERVAL", "1h")
    lookback_hours = int(env("LOOKBACK_HOURS", "8"))
    retention_days = int(env("RETENTION_DAYS", "10"))

    rows = fetch_hourly_rollups(project_id, dataset, table, interval, lookback_hours)
    print(f"fetched {len(rows)} hourly rollup rows from BigQuery (lookback={lookback_hours}h)")

    upsert_rollups(database_url, rows)
    prune_old_rows(database_url, retention_days)


if __name__ == "__main__":
    main()
