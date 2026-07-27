# Phase 4 + Phase 8: recurring scheduled ETL

Four independent Cloud Run Jobs, each triggered by its own Cloud Scheduler
trigger. The first two (Phase 4) keep data current between the (manual-only,
never auto-triggered) one-time backfills in `migration/`.
The last two (Phase 8, added later) are the Lambda-architecture sync jobs
that feed the app's `:server` backend directly -- see their own doc comments
in `candle-hourly-sync/src/main.py` / `candle-daily-sync/src/main.py` for
the full Lambda-architecture rationale. Postgres lives on Supabase (managed,
internet-reachable -- see `coin-registry/migrations/` for the schema,
applied there via `DATABASE_URL` pointed at Supabase).

| Job | Schedule | Reads from | Writes to | Purpose |
|---|---|---|---|---|
| `binance-incremental-job` | every 3h (`0 */3 * * *`) -- see note below, normally every 5 min | `data-api.binance.vision` | BigQuery `bronze_candles`, Postgres `coin_snapshots.price_usd` | Sole price-writing path (no WebSocket worker exists yet). Watermark-driven: reads `ingestion_watermarks` to know where to resume. |
| `coingecko-incremental-job` | hourly (`0 * * * *`) | CoinGecko REST | Postgres `coins`, `coin_snapshots` (market_cap_usd/rank/change_percent_24h) every run; `markets` once/day only | Live snapshot only -- does not write to BigQuery. Never touches `price_usd`/`price_source` for Binance-tradeable coins (that's the Binance job's job); *is* the sole price source for coins with no Binance USDT pair. `markets` refresh (`/coins/{id}/tickers`, ~1 call/coin) is gated to once/day via `MARKETS_REFRESH_HOUR_UTC` -- run hourly like the rest it would alone exhaust CoinGecko's Demo-tier 10,000/month quota in under 3 days (confirmed: this happened on 2026-07-17). |
| `candle-hourly-sync-job` | every 3h, offset (`20 */3 * * *`) -- see note below, normally every 5 min | BigQuery `gold.hourly_candle_metrics` (Dataform-managed) | Postgres `candle_rollups_hourly` | Lambda-architecture speed layer -- lets `:server`'s history endpoint serve 1D/5D chart ranges from a plain indexed Postgres query instead of BigQuery/Cube on every request. |
| `candle-daily-sync-job` | once daily (`15 0 * * *`) | BigQuery `gold.daily_candle_metrics` (Dataform-managed) | Postgres `candle_rollups_daily` | Lambda-architecture batch layer -- serves 1M/6M/YTD/1Y chart ranges; `:server` merges in a synthetic "today" candle from `candle_rollups_hourly` at read time to stay fresh between daily runs. |

All four are idempotent -- safe to re-run, safe if a scheduled invocation
overlaps with a slow previous one still finishing.

### Ingestion cadence: every 3h, and everything downstream now matches it (2026-07-20, refined 2026-07-27)

`binance-incremental-job` was dropped from `*/5 * * * *` to `0 */3 * * *` on
2026-07-20 to keep Cloud Run Jobs' CPU usage under the free tier
(240,000 vCPU-s/month, shared across the whole billing account) -- at the
normal 5-minute cadence this job alone runs ~1,308,285 vCPU-s/month, ~5.5x
the entire free tier. `coingecko-incremental-job` and `candle-daily-sync-job`
were left untouched (not part of the original spike, and
`coingecko-incremental-job` alone is already ~45% of the free tier at its
normal hourly cadence, so there wasn't much room to also throttle it without
losing coin/market freshness entirely).

Once `binance-incremental-job` was down to every 3h, everything downstream
that ultimately depends on its output needed to either match that cadence
or waste real cost reprocessing unchanged data -- confirmed empirically,
not assumed:

- **`silver.silver_candles`** is a native BigQuery Materialized View over
  `bronze.bronze_candles` (`refreshIntervalMs: 300000` -- checks every 5
  min) -- but BigQuery only actually *executes* (and bills) a refresh when
  the base table has new rows, so in practice it already tracks
  `binance-incremental-job`'s real cadence exactly on its own (observed:
  refreshes fire ~8 min after each 3-hourly ingestion run, ~0.02 GiB each,
  negligible -- no action needed here).
- **Dataform's `gold-rollups-schedule`** (rebuilds
  `gold.hourly_candle_metrics`/`gold.daily_candle_metrics` from
  `silver.silver_candles`) was originally just reverted from `*/5 * * * *`
  back to its pre-2026-07-17 default of `*/15 * * * *` to stay under
  BigQuery's separate 1 TiB/month on-demand query free tier (also
  billing-account-scoped) -- but since `silver_candles` itself only
  actually changes every 3h right now, running Dataform every 15 min meant
  11 out of every 12 runs were reprocessing byte-identical source data for
  zero new information, at full cost (~0.27 GiB/run, confirmed via
  `INFORMATION_SCHEMA.JOBS_BY_PROJECT` -- not query inefficiency, both SQLX
  definitions already use a bounded 6h/3-day incremental lookback against a
  MONTH-partitioned source table). **Changed again to `12 */3 * * *`**
  (12 min after each 3-hour boundary -- buffers `binance-incremental-job`'s
  ~3.3 min average runtime plus `silver_candles`' ~8 min refresh lag, so
  Dataform never rebuilds from stale/pre-refresh source data). At 8
  runs/day instead of 96, this is ~6.6% of the 1 TiB free tier/month --
  far more margin than the 80%-utilization 15-minute compromise, because
  it eliminates genuinely wasted computation rather than just budgeting
  for it.
- **`candle-hourly-sync-job`** was originally throttled to the same
  `0 */3 * * *` as `binance-incremental-job` -- which meant it fired at the
  exact same instant as ingestion, syncing Postgres from gold data that was
  still a full cycle stale (Dataform hadn't rebuilt yet). **Offset to
  `20 */3 * * *`** -- 8 min after Dataform's rebuild (which itself only
  takes ~13s), comfortable margin so Postgres always picks up the
  freshly-rebuilt gold data from the same cycle, not the previous one.

Real cost of all this: chart data is up to ~3h stale instead of ~5min while
`binance-incremental-job` stays throttled, not zero cost -- but no step in
the chain wastes computation reprocessing data that hasn't changed.

**Important: these three schedules are now coupled.** If
`binance-incremental-job` ever reverts to a different cadence (e.g. back to
every 5 min), Dataform's `gold-rollups-schedule` and
`candle-hourly-sync-trigger` need to be reconsidered *together* with it, not
left at their 3h-tuned offsets -- otherwise Dataform goes back to
reprocessing unchanged data (if left at a sub-3h interval while ingestion
is still slow) or gold/Postgres lag behind fresh ingestion (if left at a
3h-scale interval after ingestion speeds back up).

**Revert all three together once ingestion frequency changes:**

```bash
gcloud scheduler jobs update http binance-incremental-trigger \
  --location=us-central1 --project=gcp-crypto-tracker --schedule="*/5 * * * *"

TOKEN=$(gcloud auth print-access-token)
curl -X PATCH \
  -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  "https://dataform.googleapis.com/v1/projects/gcp-crypto-tracker/locations/us-central1/repositories/crypto-tracker-gold/workflowConfigs/gold-rollups-schedule?updateMask=cronSchedule" \
  -d '{
    "cronSchedule": "*/15 * * * *",
    "releaseConfig": "projects/gcp-crypto-tracker/locations/us-central1/repositories/crypto-tracker-gold/releaseConfigs/gold-rollups"
  }'

gcloud scheduler jobs update http candle-hourly-sync-trigger \
  --location=us-central1 --project=gcp-crypto-tracker --schedule="*/5 * * * *"
```

(`gcloud dataform` doesn't exist on this gcloud install, not even in
alpha/beta -- Dataform schedule changes only reachable via the
`workflowConfigs.patch` REST endpoint. Must include `releaseConfig` in the
PATCH body even though the `updateMask` only covers `cronSchedule`, or the
API 400s with "release_config is not specified".)

## Two real bugs found and fixed while building this (read before touching either job)

1. **Binance geo-blocks Cloud Run's outbound IP.** `api.binance.com` returned
   HTTP 451 ("Unavailable For Legal Reasons") for every single coin when
   this job first ran on Cloud Run -- confirmed this wasn't per-symbol
   (all 87 coins failed identically). This is a known, documented issue:
   Binance enforces "restricted location" blocks that flag Google Cloud's
   outbound IP ranges regardless of which region you deploy to (confirmed via
   research -- changing regions does not help, GCP's shared egress
   consistently geolocates as a restricted location to Binance).
   **Fix**: `binance-incremental-job` calls `data-api.binance.vision`
   instead of `api.binance.com` -- Binance's own official public-market-data-only
   mirror (same domain family as `data.binance.vision`, which
   `migration/binance-ingest-function` already used successfully from Cloud
   Run). Identical response format, not geo-blocked. Confirmed empirically on
   real Cloud Run infrastructure, not just locally.
2. **BigQuery load jobs need `bigquery.tables.create` on the dataset by
   default, even when loading into a table that already exists.** This is
   because the default `create_disposition` is `CREATE_IF_NEEDED`, and
   BigQuery checks permissions for that disposition regardless of whether
   the table is actually missing. Fixed by setting
   `create_disposition=CREATE_NEVER` explicitly in `load_rows_to_bigquery()`
   -- this is also the *correct* least-privilege behavior: this job should
   never create a table, only append to the existing `bronze_candles`.

Also worth knowing: **both jobs need a `Procfile` with a `web:` process
line** (`web: python3 src/main.py`) for Cloud Run's Python buildpack to find
the entrypoint -- it doesn't auto-detect `src/main.py` the way it would a
root-level `main.py`, and the process name must literally be `web`, not
`job` or anything else, even though this is a batch job, not a web server.

## Infrastructure

- Service accounts (least-privilege, one per job, distinct from the Phase 3
  `migration/` jobs' own service accounts):
  - `binance-incremental-runtime` -- `roles/bigquery.dataEditor` scoped to
    the `bronze_candles` *table* (not dataset-wide), `roles/bigquery.jobUser`
    project-wide (required to run load jobs at all), `roles/secretmanager.secretAccessor`
    on `supabase-database-url` only.
  - `coingecko-incremental-runtime` -- `roles/secretmanager.secretAccessor`
    on `supabase-database-url` and `coingecko-api-key` only. No BigQuery
    access (this job never touches BigQuery).
  - `cloud-scheduler-job-invoker` -- `roles/run.invoker` on both jobs only,
    used solely by the two Cloud Scheduler triggers.
  - `candle-hourly-sync-runtime` / `candle-daily-sync-runtime` (Phase 8,
    same least-privilege pattern) -- `roles/secretmanager.secretAccessor` on
    `supabase-database-url`, plus `roles/bigquery.dataViewer` scoped to
    their respective `gold.*_candle_metrics` table and project-wide
    `roles/bigquery.jobUser`. Deploy/schedule commands mirror the two
    above (`--source=recurring/candle-hourly-sync` /
    `--source=recurring/candle-daily-sync`); see each job's own
    `src/main.py` docstring for its env vars.
- Both `DATABASE_URL` (Supabase connection string) and `COINGECKO_API_KEY`
  are injected via Secret Manager (`--set-secrets`), never as plain
  environment variables in the job definition.
- Deploy commands (idempotent -- re-running `gcloud run jobs deploy` updates
  the existing job):
  ```bash
  gcloud run jobs deploy binance-incremental-job \
    --source=recurring/binance-incremental \
    --region=us-central1 --project=gcp-crypto-tracker \
    --service-account=binance-incremental-runtime@gcp-crypto-tracker.iam.gserviceaccount.com \
    --set-secrets=DATABASE_URL=supabase-database-url:latest \
    --set-env-vars=GCP_PROJECT_ID=gcp-crypto-tracker \
    --memory=512Mi --cpu=1 --task-timeout=280s --max-retries=0

  gcloud run jobs deploy coingecko-incremental-job \
    --source=recurring/coingecko-incremental \
    --region=us-central1 --project=gcp-crypto-tracker \
    --service-account=coingecko-incremental-runtime@gcp-crypto-tracker.iam.gserviceaccount.com \
    --set-secrets=DATABASE_URL=supabase-database-url:latest,COINGECKO_API_KEY=coingecko-api-key:latest \
    --memory=512Mi --cpu=1 --task-timeout=600s --max-retries=0
  ```
- Cloud Scheduler triggers (HTTP target hitting the Cloud Run Admin API's
  `:run` endpoint directly, OAuth-authenticated as `cloud-scheduler-job-invoker`):
  ```bash
  gcloud scheduler jobs create http binance-incremental-trigger \
    --location=us-central1 --project=gcp-crypto-tracker \
    --schedule="*/5 * * * *" \
    --uri="https://us-central1-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/gcp-crypto-tracker/jobs/binance-incremental-job:run" \
    --http-method=POST \
    --oauth-service-account-email=cloud-scheduler-job-invoker@gcp-crypto-tracker.iam.gserviceaccount.com

  gcloud scheduler jobs create http coingecko-incremental-trigger \
    --location=us-central1 --project=gcp-crypto-tracker \
    --schedule="0 * * * *" \
    --uri="https://us-central1-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/gcp-crypto-tracker/jobs/coingecko-incremental-job:run" \
    --http-method=POST \
    --oauth-service-account-email=cloud-scheduler-job-invoker@gcp-crypto-tracker.iam.gserviceaccount.com
  ```

## Continuous deployment

Each job (plus the two `migration/` deployables) has its own GitHub Actions
workflow in `.github/workflows/`, path-scoped to that job's directory only
-- pushing a change to `recurring/binance-incremental/` triggers *only*
`deploy-binance-incremental.yml`, the other 5 don't run. Deliberately 6
independent workflow files instead of one shared workflow with a build
matrix: these deployables share no code (mixed Python/Kotlin, different
Cloud Run resources), so there's nothing to factor out, and independent
files keep each one's blast radius obvious.

Auth is Workload Identity Federation (keyless, no service-account key
stored in GitHub), scoped to this exact repo and the `main` branch only.
Each workflow builds its own Dockerfile plainly on the GitHub-hosted
runner (free for public repos) and pushes straight to Artifact Registry --
deliberately NOT `gcloud run jobs deploy --source`, which always routes
through Cloud Build regardless of whether a Dockerfile exists. Images are
tagged with the git SHA, not `latest`, for exact traceability and easy
rollback -- compatible with the keep-3-most-recent-per-package Artifact
Registry cleanup policy already applied to this project.

## Manually triggering a job to verify it works

Before relying on the schedule, trigger each job once directly (bypasses
Cloud Scheduler, calls the Cloud Run Job the same way it would):
```bash
gcloud run jobs execute binance-incremental-job --project=gcp-crypto-tracker --region=us-central1 --wait
gcloud run jobs execute coingecko-incremental-job --project=gcp-crypto-tracker --region=us-central1 --wait
```
Or trigger the *Scheduler* job itself once, to test the full Scheduler ->
Cloud Run Admin API -> Job execution path (useful for catching IAM issues
specific to that path, since it uses a different identity than a direct
`jobs execute` call):
```bash
gcloud scheduler jobs run binance-incremental-trigger --project=gcp-crypto-tracker --location=us-central1
```
Expect a real cold start delay here -- a scheduler-triggered execution took
over 2 minutes just to reach the `Started` condition in testing, notably
slower than a `jobs execute --wait` invocation. This is normal, not a sign
of a stuck job; give it a few minutes before assuming something's wrong.

Check logs for a specific execution:
```bash
gcloud logging read 'resource.type="cloud_run_job" resource.labels.job_name="binance-incremental-job" labels."run.googleapis.com/execution_name"="EXECUTION_NAME"' \
  --project=gcp-crypto-tracker --format="value(timestamp,textPayload)" --order=asc
```

## Confirming data is actually flowing

```bash
export PGCONN="postgresql://postgres.kvzxhhoowmyvouskueta:<password>@aws-0-us-east-1.pooler.supabase.com:6543/postgres"
```

- **Binance job**: watermarks should be recent, and `coin_snapshots` for
  Binance-tradeable coins should have `price_source = 'binance_rest'` with a
  fresh `updated_at`:
  ```sql
  SELECT coin_id, last_loaded_close_time, updated_at FROM ingestion_watermarks ORDER BY updated_at DESC LIMIT 5;
  SELECT coin_id, price_usd, updated_at, price_source FROM coin_snapshots WHERE price_source = 'binance_rest' ORDER BY updated_at DESC LIMIT 5;
  ```
  And in BigQuery, confirm new rows are landing (publish_time should track
  each run):
  ```sql
  SELECT coin_id, open_time, publish_time FROM bronze.bronze_candles ORDER BY publish_time DESC LIMIT 5
  ```
- **CoinGecko job**: `coins`/`coin_snapshots`/`markets` should all have
  recent `updated_at` values, and non-Binance-tradeable coins (e.g.
  `tether`) should show `price_source = 'coingecko'`:
  ```sql
  SELECT coin_id, market_cap_usd, rank, change_percent_24h, updated_at FROM coin_snapshots ORDER BY updated_at DESC LIMIT 5;
  SELECT coin_id, exchange_id, volume_usd_24h, updated_at FROM markets ORDER BY updated_at DESC LIMIT 5;
  ```

## Local development

Each job has its own venv (`python3 -m venv .venv`, `pip install -r requirements.txt`)
and runs the same way locally as in the container:
```bash
cd recurring/binance-incremental && source .venv/bin/activate
export DATABASE_URL="..." GCP_PROJECT_ID=gcp-crypto-tracker GOOGLE_APPLICATION_CREDENTIALS=../../.secrets/cryptotracker-backend.json
python3 src/main.py
```
Note the geo-block above only affects Cloud Run's IP, not a local machine's
-- a local run hitting `api.binance.com` directly may succeed even though
the deployed job needs the `data-api.binance.vision` workaround. Both job's
source already uses the working endpoint, so this doesn't require any
different configuration locally vs. deployed, just don't be surprised if
you test a *reverted* version locally and see it "work" there but fail
identically to before once deployed.
