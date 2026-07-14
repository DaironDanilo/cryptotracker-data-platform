# cryptotracker-data-platform

Backend data platform for the crypto tracker: ingestion, storage, and the
analytics layer. This is a separate repo from the client-facing app repo
(Compose Multiplatform app + `:server`/`:shared` modules). The two repos share
*conventions* (env-var driven config, model shapes) but no code — `common/`
here is deliberately a standalone module, not a shared dependency across repos.

## Phase index

| Phase | Directory | Status |
|---|---|---|
| 2 | `coin-registry/` — Postgres schema + coin/binance_symbol seed job | **implemented** |
| 3 | `backfill/` — one-time historical backfill (Binance OHLCV, CoinGecko market cap) | scaffold only |
| 4 | `pubsub-infra/` — GCP Pub/Sub topic/subscription provisioning | scaffold only |
| 5 | `ws-worker/`, `reconciliation-job/`, `common/` — Binance WS → Pub/Sub | scaffold only |
| 6 | `postgres-subscriber/`, `redis-subscriber/` — Pub/Sub consumers | scaffold only |
| 7 | `bigquery/`, `cube/` — warehouse DDL/views + semantic layer | scaffold only |
| 8 | `airflow/` — orchestration DAGs | scaffold only |
| 10 | `infra/` — Server A consolidated docker-compose | scaffold only |

Directories for unimplemented phases exist as placeholders per the intended
repo layout; they contain no working code yet.

## Phase 2 — coin-registry

See [`coin-registry/README.md`](coin-registry/README.md) for schema details,
setup, and how to run the migrations and seed job.

Quick start:

```bash
cd coin-registry
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env   # fill in DATABASE_URL and (optionally) COINGECKO_API_KEY
python3 migrations/run_migrations.py
python3 src/seed_coins.py --top-n 150
```

## Kotlin modules (future phases)

`settings.gradle.kts` / `build.gradle.kts` / `gradle/libs.versions.toml` are in
place with shared plugin versions and a version catalog (Ktor, kotlinx.serialization,
GCP Pub/Sub client, Postgres driver, Jedis), but no modules are registered yet —
`coin-registry` (this phase) is plain Python, not a Gradle module. Modules get
uncommented in `settings.gradle.kts` as each later phase actually adds Kotlin code.
