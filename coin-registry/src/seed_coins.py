#!/usr/bin/env python3
"""Seeds the `coins` registry table.

Pulls the top N coins by market cap from CoinGecko's /coins/markets endpoint,
then cross-references Binance's /api/v3/exchangeInfo to fill in
binance_symbol (matched on base asset + USDT quote, TRADING status only).
Coins with no Binance USDT market (stablecoins, RWA-style tokens, etc.) are
left with binance_symbol = NULL -- that's the expected, correct outcome, not
an error.

Re-running is safe: coins are upserted by coin_id.

Env vars:
  DATABASE_URL       required, e.g. postgresql://user:pass@host:5432/dbname
  COINGECKO_API_KEY   optional; sent as the x-cg-demo-api-key header
"""
import argparse
import os
import sys
import time

import psycopg
import requests

COINGECKO_BASE_URL = "https://api.coingecko.com/api/v3"
BINANCE_EXCHANGE_INFO_URL = "https://api.binance.com/api/v3/exchangeInfo"
COINGECKO_PAGE_SIZE = 250


def fetch_top_coins(top_n: int, api_key: str | None) -> list[dict]:
    headers = {"x-cg-demo-api-key": api_key} if api_key else {}
    coins: list[dict] = []
    page = 1
    while len(coins) < top_n:
        per_page = min(COINGECKO_PAGE_SIZE, top_n - len(coins))
        resp = requests.get(
            f"{COINGECKO_BASE_URL}/coins/markets",
            headers=headers,
            params={
                "vs_currency": "usd",
                "order": "market_cap_desc",
                "per_page": per_page,
                "page": page,
                "sparkline": "false",
            },
            timeout=30,
        )
        resp.raise_for_status()
        batch = resp.json()
        if not batch:
            break
        coins.extend(batch)
        page += 1
        if len(batch) < per_page:
            break  # ran out of coins before reaching top_n
        time.sleep(1.5)  # be polite to the free-tier rate limit
    return coins[:top_n]


def fetch_binance_usdt_map() -> dict[str, str]:
    """Maps base asset (upper-cased) -> Binance symbol, for TRADING USDT pairs."""
    resp = requests.get(BINANCE_EXCHANGE_INFO_URL, timeout=30)
    resp.raise_for_status()
    symbols = resp.json()["symbols"]
    return {
        s["baseAsset"].upper(): s["symbol"]
        for s in symbols
        if s["quoteAsset"] == "USDT" and s["status"] == "TRADING"
    }


def upsert_coins(database_url: str, rows: list[tuple[str, str, str, str | None]]) -> None:
    with psycopg.connect(database_url, prepare_threshold=None) as conn:
        with conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO coins (coin_id, symbol, name, binance_symbol)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (coin_id) DO UPDATE SET
                    symbol = EXCLUDED.symbol,
                    name = EXCLUDED.name,
                    binance_symbol = EXCLUDED.binance_symbol
                """,
                rows,
            )
        conn.commit()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--top-n",
        type=int,
        default=150,
        help="Number of top-by-market-cap coins to seed (default: 150)",
    )
    args = parser.parse_args()

    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        print("DATABASE_URL environment variable is required", file=sys.stderr)
        sys.exit(1)
    coingecko_api_key = os.environ.get("COINGECKO_API_KEY")

    print(f"fetching top {args.top_n} coins from CoinGecko...")
    coins = fetch_top_coins(args.top_n, coingecko_api_key)
    print(f"fetched {len(coins)} coins")

    print("fetching Binance exchangeInfo...")
    binance_map = fetch_binance_usdt_map()
    print(f"found {len(binance_map)} tradeable USDT pairs on Binance")

    rows = []
    matched = 0
    for c in coins:
        symbol = c["symbol"]
        binance_symbol = binance_map.get(symbol.upper())
        if binance_symbol:
            matched += 1
        rows.append((c["id"], symbol, c["name"], binance_symbol))

    upsert_coins(database_url, rows)
    print(f"seeded {len(rows)} coins ({matched} matched a Binance USDT pair, "
          f"{len(rows) - matched} left NULL)")


if __name__ == "__main__":
    main()
