# Handoff — current live state

**Last updated: 2026-07-27.** This file is a dated snapshot of live
GCP/infrastructure state that changes independently of code — update it at
the end of any session that changes a schedule, starts/stops a service, or
resolves a cost incident. Stale entries are worse than no entry, so prefer
deleting/rewriting a section over leaving it half-true.

## Cube Store/Core: stopped

All 6 containers (`cube-api`, `cube-api-dev`, `cube-refresh-worker`,
`cubestore-router`, `cubestore-worker-1`, `cubestore-worker-2`) stopped via
`docker compose stop` in `cube/` on the Ubuntu box (192.168.1.110) —
config/volumes preserved, not deleted. Root cause: a dev-mode instance
(`cube-api-dev`) was OOM-crash-looping (1,448+ restarts) and re-running
BigQuery pre-aggregation refreshes on every crash cycle; `cube-refresh-worker`
was separately doing the same on its own 5-min schedule. Neither had a real
consumer — `:server` (sibling repo) moved off Cube to Postgres rollup
tables months ago. **Bring back with `docker compose start` only once a
real chatbot/analytics consumer is actually being built.**

## Three coupled Cloud Scheduler / Dataform schedules — not at their normal values

All three currently run on a **3-hour cadence** instead of their designed
5-15 min defaults, to stay under GCP's real, billing-account-scoped free
tiers (Cloud Run Jobs CPU: 240,000 vCPU-s/month; BigQuery on-demand query:
1 TiB/month) after two separate incidents this month:

| Trigger | Current | Normal/designed | Why |
|---|---|---|---|
| `binance-incremental-trigger` (Cloud Scheduler) | `0 */3 * * *` | `*/5 * * * *` | Cloud Run Jobs CPU free tier |
| Dataform `gold-rollups-schedule` | `12 */3 * * *` | `*/15 * * * *` | BigQuery query free tier; also cadence-matched to real ingestion frequency, not just cost |
| `candle-hourly-sync-trigger` (Cloud Scheduler) | `20 */3 * * *` | `*/5 * * * *` | Offset after Dataform's rebuild so it never syncs stale gold data |

**These three are coupled — if `binance-incremental-job`'s cadence changes,
reconsider all three together**, not just the one you're touching. Full
math and exact revert commands: `recurring/README.md`'s "Ingestion cadence"
section. The original throttle (2026-07-20) was meant to last through a
6-day trip that has likely already passed as of this writing — worth
asking whether it's still needed, or whether reverting toward the normal
5-minute cadence (redoing the safety-margin math for the *current* actual
month-to-date usage, not assuming last month's numbers still apply) is
appropriate now.

## Git state

Clean and fully pushed as of 2026-07-27 (last commit `e13d47f`). The
sibling `cryptoTracker` repo has one unpushed commit
(`5a39ad9`) — see that repo's own state, not tracked here.

## Cost incidents resolved this month (chronological)

1. Cloud Storage — a 54.9GB Cube Store pre-aggregation bucket, deleted
   after Cube Store moved to local-disk (`CUBESTORE_REMOTE_DIR`) storage.
2. Cube stack — see above.
3. Cloud Run Jobs CPU free tier — see above.
4. BigQuery on-demand query free tier — see above.

If a new cost spike shows up, check `INFORMATION_SCHEMA.JOBS_BY_PROJECT`
(BigQuery) and `docker inspect <container> --format='{{.RestartCount}}'`
(crash-loop detection) before assuming anything — every incident so far had
a precise, findable root cause, never a mystery.
