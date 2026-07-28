# cryptotracker-data-platform

GCP data platform for the CryptoTracker project: ingestion, BigQuery
medallion architecture (bronze/silver/gold via Dataform), Cloud Run Jobs
for scheduled ETL, Supabase Postgres, Cube Core/Store (currently stopped).
Full architecture: `README.md`. Recurring jobs detail: `recurring/README.md`
(also has the most current live-schedule state — read it, don't assume
the schedules match their own "designed" defaults, see `HANDOFF.md`).

This file is agent-operating rules kept in the open, cross-tool AGENTS.md
format (per the sibling `cryptoTracker` repo's established convention) so
any AI coding tool can read it, not just Claude Code.

## `HANDOFF.md` — read this if your tool doesn't auto-load it

This repo's live GCP state changes independently of its code (schedules,
which services are running, cost-incident history) — `HANDOFF.md` is a
dated snapshot of that state, updated at the end of significant sessions.
Claude Code loads it automatically every session (imported from
`CLAUDE.md`); other tools should read it explicitly before assuming any
schedule/service is at its "normal" value.

## Git / commit rules (standing — do not ask, just follow)

- **Never** add Claude/Claude Code (or any AI tool) as a co-author or
  contributor in any commit.
- **Never** push unless explicitly asked in that turn — committing is not
  the same as authorization to push.
- Prefer many small, focused commits over one large one for multi-step work.

## Deployment — no Cloud Build, ever

All 6 deployables (`binance-incremental-job`, `coingecko-incremental-job`,
`candle-hourly-sync-job`, `candle-daily-sync-job`, `coingecko-ingest-job`,
`binance-ingest`) build with plain `docker build`/`docker push` (GitHub
Actions runners, path-scoped CI/CD, one workflow per deployable), never
`gcloud run deploy --source=`/`gcloud builds submit`. Deliberate
cost-control choice, not an oversight.

## Cost discipline — this project has had 3 real incidents

BigQuery, Cloud Run Jobs CPU, and Cloud Storage all have separate, real,
billing-account-scoped (not per-project) free tiers that have each been
exceeded at least once. Before changing any schedule/service's frequency or
leaving anything running "for later use," check `HANDOFF.md` and
`recurring/README.md`'s incident history first, and when calculating a safe
frequency: measure real usage via `INFORMATION_SCHEMA`/live GCP APIs, get
exact current free-tier numbers from live docs (not training-data recall),
and target a real safety margin (80% of the free tier has been the
established target) — don't estimate.

## GCP resource access

`gcloud`/`bq` CLIs are the primary tools; `gcloud dataform` doesn't exist on
this install (not even alpha/beta) — Dataform schedule changes go through
the REST API directly (`workflowConfigs.patch`; must include `releaseConfig`
in the PATCH body even when the `updateMask` only covers `cronSchedule`, or
it 400s). A separate Ubuntu box (ask the user for current SSH access) runs
Cube Store/Core via Docker Compose in `cube/` — currently stopped.
