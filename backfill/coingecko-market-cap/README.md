# backfill/coingecko-market-cap (Phase 3)

One-time historical price/market-cap/volume backfill from CoinGecko's
`/coins/{id}/market_chart/range` into BigQuery `market_cap_history`. Runs for
every coin in the Postgres `coins` table (Phase 2).

## What it does

1. Reads all `coin_id`s from Postgres.
2. For each coin, requests up to 365 days of history (CoinGecko's free-tier
   lookback limit), chunked into <=90-day windows so each request stays in
   the endpoint's hourly-granularity range (a >90-day window degrades to
   daily granularity).
3. Each window's rows are loaded into a staging table and **MERGE**d into
   `market_cap_history` on `(coin_id, timestamp)` -- upsert-safe, so
   re-running never creates duplicates.
4. Windows are calendar-date based (computed from today's date, not a
   precise timestamp), so a resumed run on a later day still lines up with
   previously completed windows instead of drifting.

## Rate limiting

The free/demo CoinGecko tier allows roughly 100 requests/minute and 10,000/month.
This job:
- self-throttles to `--requests-per-minute` (default 25, well under the limit),
- backs off exponentially on HTTP 429/5xx,
- tracks a persistent per-calendar-month request counter in `.progress.sqlite3`
  and refuses to make further calls once `COINGECKO_MONTHLY_REQUEST_CAP`
  (default 10000) is reached that month.

At 150 coins x ~5 windows (365 days / 90-day chunks) = ~750 requests total,
this comfortably fits in a single month's budget even with resumed re-runs.

## Setup

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env   # fill in DATABASE_URL, GCP_PROJECT_ID, GOOGLE_APPLICATION_CREDENTIALS
export $(grep -v '^#' .env | xargs)
```

## Run

```bash
python3 src/backfill_market_cap.py --days 365
```

- `--days` (default 365, clamped to 365): total history to backfill.
- `--requests-per-minute` (default 25): self-imposed throttle.
- `--limit-coins N`: only process the first N coins (useful for a quick test run).

Safe to re-run or Ctrl-C and resume -- completed (coin, window) pairs are
skipped via `.progress.sqlite3`, and the MERGE itself is idempotent even if a
window is somehow reprocessed.
