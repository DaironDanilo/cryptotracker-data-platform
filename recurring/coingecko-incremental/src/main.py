#!/usr/bin/env python3
"""Recurring CoinGecko incremental job -- Phase 4.

Runs hourly as a Cloud Run Job (triggered by Cloud Scheduler). This is NOT
the one-time historical market-cap backfill (migration/coingecko-ingest-job)
-- that remains manual-only. This job keeps the *live snapshot* current:
Postgres `coins` (master list), `coin_snapshots` (market_cap_usd, rank,
change_percent_24h -- never price_usd/price_source, which belong to the
Binance incremental job), and `markets` (per-exchange ticker listings). It
does not write to BigQuery -- BigQuery holds historical data for the chart
(fed by the Binance job and the one-time backfills); the custom backend's
REST endpoints for the coin list and markets tab read Postgres directly.

Split into two cadences within the same hourly run, NOT two separate jobs,
because of CoinGecko's own request cost asymmetry: /coins/markets is one
call regardless of coin count, but /coins/{id}/tickers is one call PER coin
-- at 150 tracked coins that's ~150 calls, which alone would exhaust the
Demo tier's entire 10,000/month quota in under 3 days if fetched every
hour (confirmed: this is what actually happened -- 57 hourly runs in ~2.5
days burned ~8,600 calls). Exchange listings don't meaningfully change
hour to hour, unlike price/market-cap, so only the cheap call needs to run
every hour:
  1. GET /coins/markets (paginated) for the top N coins by market cap --
     EVERY run. Provides id/symbol/name/market_cap_rank/market_cap/
     price_change_percentage_24h in one call.
  2. Upsert `coins` (coin_id, symbol, name) -- EVERY run, keeping the
     master list current as new coins enter/leave the top N. binance_symbol
     is preserved as-is (re-cross-referencing against Binance's
     exchangeInfo is coin-registry/src/seed_coins.py's job, not this one's).
  3. Upsert `coin_snapshots` (market_cap_usd, rank, change_percent_24h) --
     EVERY run. Explicitly does not touch price_usd/price_source.
  4. GET /coins/{id}/tickers per coin, upsert into `markets` -- ONLY on the
     hour matching MARKETS_REFRESH_HOUR_UTC (once/day). `rank` there is
     computed in this job (ROW_NUMBER() OVER (PARTITION BY coin_id ORDER BY
     volume_usd_24h DESC) equivalent), not in BigQuery/SQL.

At 150 coins this keeps monthly usage to roughly 24*30 (step 1, every run)
+ 150*30 (step 4, once/day) =~ 5,220 calls/month -- about half the 10,000
Demo-tier quota, leaving headroom.

Self-throttled to stay under the free-tier Demo rate limit; see
RateLimiter below.

Env vars:
  DATABASE_URL             required, Supabase Postgres connection string
  COINGECKO_API_KEY         optional but strongly recommended (anonymous
                              tier is far stricter than the documented Demo
                              tier's ~100 requests/minute, ~10,000/month)
  TOP_N_COINS               optional, default 150
  REQUESTS_PER_MINUTE        optional, default 40
  MARKETS_REFRESH_HOUR_UTC   optional, default 0 -- the one hour per day
                              (UTC, matching this job's own hourly Cloud
                              Scheduler trigger) that step 4 runs on
"""
from __future__ import annotations

import os
import sys
import time
from datetime import datetime, timezone

import psycopg
import requests

COINGECKO_BASE_URL = "https://api.coingecko.com/api/v3"
COINGECKO_PAGE_SIZE = 250


def env(name: str, default: str | None = None, required: bool = False) -> str:
    val = os.environ.get(name, default)
    if required and not val:
        print(f"{name} environment variable is required", file=sys.stderr)
        sys.exit(1)
    return val


class RateLimiter:
    def __init__(self, requests_per_minute: int):
        self.min_interval = 60.0 / requests_per_minute
        self.last_request_at = 0.0

    def wait(self) -> None:
        elapsed = time.monotonic() - self.last_request_at
        if elapsed < self.min_interval:
            time.sleep(self.min_interval - elapsed)
        self.last_request_at = time.monotonic()


def cg_get(path: str, params: dict, api_key: str | None, limiter: RateLimiter) -> requests.Response:
    headers = {"x-cg-demo-api-key": api_key} if api_key else {}
    for attempt in range(6):
        limiter.wait()
        resp = requests.get(f"{COINGECKO_BASE_URL}{path}", headers=headers, params=params, timeout=30)
        if resp.status_code == 429 or resp.status_code >= 500:
            wait = min(2 ** attempt, 60)
            print(f"  {path}: HTTP {resp.status_code}, backing off {wait}s (attempt {attempt + 1}/6)")
            time.sleep(wait)
            continue
        resp.raise_for_status()
        return resp
    raise RuntimeError(f"{path}: exhausted retries")


def fetch_markets(top_n: int, api_key: str | None, limiter: RateLimiter) -> list[dict]:
    coins: list[dict] = []
    page = 1
    while len(coins) < top_n:
        per_page = min(COINGECKO_PAGE_SIZE, top_n - len(coins))
        resp = cg_get(
            "/coins/markets",
            {"vs_currency": "usd", "order": "market_cap_desc", "per_page": per_page, "page": page, "sparkline": "false"},
            api_key,
            limiter,
        )
        batch = resp.json()
        if not batch:
            break
        coins.extend(batch)
        page += 1
        if len(batch) < per_page:
            break
    return coins[:top_n]


def fetch_tickers(coin_id: str, api_key: str | None, limiter: RateLimiter) -> list[dict]:
    resp = cg_get(f"/coins/{coin_id}/tickers", {"include_exchange_logo": "false"}, api_key, limiter)
    return resp.json().get("tickers", [])


def upsert_coins(conn: psycopg.Connection, markets: list[dict]) -> None:
    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO coins (coin_id, symbol, name)
            VALUES (%s, %s, %s)
            ON CONFLICT (coin_id) DO UPDATE SET symbol = EXCLUDED.symbol, name = EXCLUDED.name
            """,
            [(m["id"], m["symbol"], m["name"]) for m in markets],
        )
    conn.commit()


def upsert_coin_snapshot_market_data(conn: psycopg.Connection, m: dict, now: datetime, is_binance_tradeable: bool) -> None:
    market_cap = m.get("market_cap")
    rank = m.get("market_cap_rank")
    change_24h = m.get("price_change_percentage_24h")
    price = m.get("current_price") or 0
    with conn.cursor() as cur:
        if is_binance_tradeable:
            # price_usd/price_source belong to the Binance job for these coins --
            # only seed a price on first insert (so the row isn't left at a
            # meaningless placeholder before Binance's first run), never overwrite
            # an existing value.
            cur.execute(
                """
                INSERT INTO coin_snapshots (coin_id, price_usd, market_cap_usd, rank, change_percent_24h, updated_at, price_source)
                VALUES (%s, %s, %s, %s, %s, %s, 'coingecko')
                ON CONFLICT (coin_id) DO UPDATE SET
                    market_cap_usd = EXCLUDED.market_cap_usd,
                    rank = EXCLUDED.rank,
                    change_percent_24h = EXCLUDED.change_percent_24h
                """,
                (m["id"], price, market_cap, rank, change_24h, now),
            )
        else:
            # CoinGecko is the *only* price source for coins with no Binance
            # USDT pair (stablecoins, RWA-style tokens, etc.) -- the Binance
            # job will never touch these, so this job must keep price_usd
            # current for them too, not just seed it once.
            cur.execute(
                """
                INSERT INTO coin_snapshots (coin_id, price_usd, market_cap_usd, rank, change_percent_24h, updated_at, price_source)
                VALUES (%s, %s, %s, %s, %s, %s, 'coingecko')
                ON CONFLICT (coin_id) DO UPDATE SET
                    price_usd = EXCLUDED.price_usd,
                    market_cap_usd = EXCLUDED.market_cap_usd,
                    rank = EXCLUDED.rank,
                    change_percent_24h = EXCLUDED.change_percent_24h,
                    updated_at = EXCLUDED.updated_at,
                    price_source = 'coingecko'
                """,
                (m["id"], price, market_cap, rank, change_24h, now),
            )
    conn.commit()


def upsert_markets(conn: psycopg.Connection, coin_id: str, tickers: list[dict], now: datetime) -> int:
    rows = []
    ranked = sorted(tickers, key=lambda t: t.get("converted_volume", {}).get("usd") or 0, reverse=True)
    for t in ranked:
        exchange_id = t.get("market", {}).get("identifier")
        base = t.get("base")
        target = t.get("target")
        if not exchange_id or not base or not target:
            continue
        rows.append(
            (
                coin_id,
                exchange_id,
                base,
                target,
                t.get("converted_last", {}).get("usd"),
                t.get("converted_volume", {}).get("usd"),
                t.get("trust_score"),
                t.get("last_traded_at"),
                now,
            )
        )
    if not rows:
        return 0
    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO markets (coin_id, exchange_id, base_symbol, target_symbol, price_usd, volume_usd_24h, trust_score, last_traded_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (coin_id, exchange_id, base_symbol, target_symbol) DO UPDATE SET
                price_usd = EXCLUDED.price_usd,
                volume_usd_24h = EXCLUDED.volume_usd_24h,
                trust_score = EXCLUDED.trust_score,
                last_traded_at = EXCLUDED.last_traded_at,
                updated_at = EXCLUDED.updated_at
            """,
            rows,
        )
    conn.commit()
    return len(rows)


def main() -> None:
    database_url = env("DATABASE_URL", required=True)
    api_key = env("COINGECKO_API_KEY")
    top_n = int(env("TOP_N_COINS", "150"))
    requests_per_minute = int(env("REQUESTS_PER_MINUTE", "40"))
    markets_refresh_hour = int(env("MARKETS_REFRESH_HOUR_UTC", "0"))

    limiter = RateLimiter(requests_per_minute)
    now = datetime.now(timezone.utc)

    print(f"fetching top {top_n} coins from /coins/markets...")
    markets = fetch_markets(top_n, api_key, limiter)
    print(f"fetched {len(markets)} coins")

    # prepare_threshold=None: Supabase's pooler (port 6543, transaction-mode
    # PgBouncer) can route different transactions on this same connection to
    # different backend Postgres processes. psycopg3 auto-switches repeated
    # queries to server-side PREPARE after a few uses by default -- a
    # prepared statement created on one backend doesn't exist on another,
    # so this loop (which reuses one connection across up to 150 coins'
    # executemany calls) intermittently crashed with
    # "prepared statement ... does not exist" once the pooler rotated the
    # backend mid-run. Confirmed as the actual cause via Cloud Run logs.
    with psycopg.connect(database_url, prepare_threshold=None) as conn:
        upsert_coins(conn, markets)
        with conn.cursor() as cur:
            cur.execute("SELECT coin_id FROM coins WHERE binance_symbol IS NOT NULL")
            binance_tradeable = {r[0] for r in cur.fetchall()}
        for m in markets:
            upsert_coin_snapshot_market_data(conn, m, now, m["id"] in binance_tradeable)
        print(f"upserted coins + coin_snapshots market data for {len(markets)} coins")

        if now.hour != markets_refresh_hour:
            print(
                f"skipping markets/tickers refresh (current UTC hour {now.hour} != "
                f"MARKETS_REFRESH_HOUR_UTC {markets_refresh_hour}) -- runs once/day, not hourly, "
                f"to stay within CoinGecko's Demo-tier monthly quota"
            )
            return

        total_market_rows = 0
        failed = 0
        for m in markets:
            coin_id = m["id"]
            try:
                tickers = fetch_tickers(coin_id, api_key, limiter)
            except (requests.RequestException, RuntimeError) as e:
                print(f"  {coin_id}: FAILED to fetch tickers: {e}", file=sys.stderr)
                failed += 1
                continue
            n = upsert_markets(conn, coin_id, tickers, now)
            total_market_rows += n

    print(f"upserted {total_market_rows} market rows across {len(markets) - failed} coins ({failed} coins failed)")


if __name__ == "__main__":
    main()
