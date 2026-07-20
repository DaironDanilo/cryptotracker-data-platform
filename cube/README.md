# Cube Core + Cube Store (Phase 5 semantic layer)

## Research summary (Cube Store production topology)

Read directly from `docs.cube.dev` (the GitHub README itself is high-level
and doesn't cover this) before writing any config, per instruction:

- **Cube Core** (the API/orchestration layer, `cubejs/cube:latest` image) is
  what defines the data model (cubes, dimensions, measures,
  pre-aggregations) and serves the REST/SQL/GraphQL APIs. It connects
  directly to the source warehouse (BigQuery here) to run non-cached
  queries and to build pre-aggregations, and delegates pre-aggregation
  *storage and cached-query serving* to Cube Store.
- **Cube Store** is a separate, purpose-built distributed query engine just
  for pre-aggregations -- not a general cache, a real columnar store with
  its own router/worker cluster model:
  - **Router node**: coordinates queries and cluster metadata (listens on
    `CUBESTORE_META_PORT`). Official minimum sizing: 6GB RAM / 4 CPU cores,
    rated for 50-100 queries/sec.
  - **Worker node(s)**: actually store data partitions and execute
    distributed scans (`CUBESTORE_WORKER_PORT` set = the node becomes a
    worker). Official guidance: **at least two worker nodes**, 8GB RAM / 4
    CPU cores each, roughly one worker per pre-aggregation partition or per
    1M rows scanned in a query.
  - Cube Core talks to the **router only** (`CUBEJS_CUBESTORE_HOST` /
    `CUBEJS_CUBESTORE_PORT`, default port 3030), never to workers directly.
- **External remote storage for the pre-aggregation cache** (instead of
  local disk) is a first-class, documented feature: Cube Store supports S3,
  GCS, or Azure Blob as the durable backing store (specifically because they
  provide the strong consistency guarantees Cube Store's design requires --
  the docs are explicit that these three are "the only known
  implementations" that qualify). This was the original setup here (GCS,
  via `CUBESTORE_GCS_BUCKET`/`CUBESTORE_GCP_KEY_FILE`) -- **since reverted
  to local storage** (see callout below) after it turned out to cost real
  money for a cache with zero consumers.

**Reverted to local-disk storage (`CUBESTORE_REMOTE_DIR`), not GCS.** All 3
Cube Store nodes syncing continuously against one GCS bucket generated
~53,600 Storage API calls/day even with Cube completely unqueried (nothing
uses it yet -- it's kept for the future chatbot use case) -- blew straight
past GCS's free tier (5,000 Class A ops/month) and was on a clear trajectory
to cost real money for a cache nobody was reading from. Cube's own docs
call local-path "remote" storage the right setup specifically "if all nodes
of a cluster run on a single machine" -- exactly this deployment (all 3
Cube Store containers live permanently on one box with persistent Docker
volumes already, not real distributed/ephemeral machines that need a
network-durable shared store). `CUBESTORE_REMOTE_DIR` points all 3 nodes at
the same shared named Docker volume instead of GCS, keeping the "one shared
durable store all nodes see" property GCS was providing, at zero ongoing
cost. Explicitly **not recommended by Cube's docs for production use** --
acceptable trade here because this is a personal project optimizing for
near-zero cost, not a production deployment with uptime/durability SLAs.
`CUBESTORE_DATA_DIR` is unrelated either way: that's always been each
node's own local scratch space, never the durable store itself.

**Deliberate deviation from the official sizing**: those RAM/CPU minimums
(effectively 30GB+ RAM total across 1 API instance + 1 refresh worker + 1
router + 2 workers) are enterprise-scale guidance, and don't fit this
project's actual hardware (a home server, not a 30GB-class cluster) or data
volume (a few million rows, nowhere near the 1M-rows-per-worker guideline
that sizing is built around). The topology below keeps the *shape* you
asked for -- a real router + 2 workers, not the single-process stripped-down
setup from an earlier phase -- but with memory limits scaled down
accordingly (768MB-1GB per container, ~4.3GB total). If query latency
becomes a real problem, this is the first knob to revisit.

## BigQuery objects this points at

| Dataset.Table | Layer | Role |
|---|---|---|
| `silver.silver_candles` | silver (materialized view) | Backs the `candles` cube -- fine-grained, deduped OHLCV for "candles for coin X between A and B" chart queries. |
| `gold.daily_candle_metrics` | gold (Dataform-managed table) | Backs the `daily_candle_metrics` cube. |
| `gold.hourly_candle_metrics` | gold (Dataform-managed table) | Backs the `hourly_candle_metrics` cube. |

Cube's own BigQuery service account (`cube-core-bigquery-runtime@gcp-crypto-tracker.iam.gserviceaccount.com`)
is scoped to table-level `dataViewer` on exactly these three tables plus
`bronze.bronze_candles` (querying a materialized view still requires read
access to its underlying base table -- a real BigQuery permission
propagation quirk confirmed empirically while wiring this up, not
documented anywhere obvious) and project-wide `bigquery.jobUser` (required
to run any query at all). Its key lives at
`../.secrets/cube-core-bigquery-runtime.json`, mounted read-only into the
containers -- never baked into the image or committed.

## Current status: not on the app's request path

The app's custom `:server` backend (separate repo) originally planned to
read history from Cube, but that path was replaced with `:server` reading
Postgres rollup tables directly (see that repo's `server/README.md` --
Cube/BigQuery proved too slow/fragile for `:server`'s fixed six-range
chart). `:server` also moved off this home server entirely and now runs on
Cloud Run, so it no longer shares a Docker host with this stack at all.

This stack exists for a **future analytics/chatbot use case** -- ad-hoc,
semantic-layer-style querying against BigQuery is a better fit for Cube's
actual strength than the fixed chart endpoint ever was. It currently has no
consumer; the `crypto-tracker-net` external network below exists for
whenever that future consumer is built.

### Stopped as of 2026-07-19 -- was silently burning BigQuery cost

All 6 containers (`docker compose stop`, config/volumes preserved) were
stopped after a BigQuery cost spike investigation traced ~250 GiB/day of
`Analysis` billing to this stack, with **no consumer reading the result**:

- `cube-api-dev` (dev-mode instance, port 4001) had restarted **1,448
  times** from a Node.js heap-out-of-memory crash, and re-ran its
  dev-mode scheduler's pre-aggregation refresh queries against BigQuery on
  every single restart cycle before crashing again.
- `cube-refresh-worker` was separately running its own legitimate 5-minute
  scheduled refresh for the `candles` pre-aggregation, hitting a
  `Table ... Not found` error and intermittent DNS/network failures
  reaching `bigquery.googleapis.com` from this box, retrying repeatedly.

Both were keeping pre-aggregations "fresh" for a consumer that doesn't
exist. Bring the stack back with `docker compose start` (from this
directory) once the chatbot/analytics feature is actually being built --
don't leave it running idle in the meantime, since this is exactly how it
started costing money with nothing reading its output.

## Networking

1. **`crypto-tracker-net`, an external Docker network** (create once with
   `docker network create crypto-tracker-net` before first bringing this
   stack up) -- the intended path for a future same-host consumer to reach
   Cube Core at `http://cube-api:4000` by service name, avoiding a
   round-trip through the host network stack. Nothing joins this network
   today.
2. **Published to the LAN**, not just `127.0.0.1` -- prod on
   `0.0.0.0:4000:4000`, dev on `0.0.0.0:4001:4000` (see "Two instances"
   below). Opened up from the original `127.0.0.1`-only binding at the
   user's request, for direct browser/API access from other machines on
   the home network. There is still no auth in front of either beyond
   `CUBEJS_API_SECRET` (and dev mode has none at all -- see below), so this
   is only appropriate on a trusted LAN.

## Two instances: prod and dev

`docker-compose.yml` runs two Cube API instances against the **same**
Cube Store cluster and the same `./model` (no reason to duplicate storage
or the refresh worker for an identical data model):

| Instance | Container | Port | Mode |
|---|---|---|---|
| Prod | `cube-api` | `4000` | `CUBEJS_DEV_MODE=false` -- REST API gated by a real JWT (`CUBEJS_API_SECRET`), no Developer Playground |
| Dev | `cube-api-dev` | `4001` | `CUBEJS_DEV_MODE=true` -- Developer Playground UI available, but **authentication and RBAC are disabled entirely** while in this mode |

Both are LAN-exposed (`0.0.0.0`), so the dev instance's lack of auth is a
real, accepted risk on this trusted home network -- do not replicate this
setup anywhere less trusted.

## Bringing it up

```bash
docker network create crypto-tracker-net   # once, if it doesn't already exist
cd cube
docker compose up -d
```

Verify against prod (`4000`) or dev (`4001`), from the server itself or
anywhere on the LAN via `http://YOUR_SERVER_IP:<port>`:
`curl -H "Authorization: $(cat ../.secrets or your token)" http://localhost:4000/cubejs-api/v1/meta`
should list the three cubes (`candles`, `daily_candle_metrics`,
`hourly_candle_metrics`). Pre-aggregations build in the background via
`cube-refresh-worker`; the first build after a fresh start can take a few
minutes depending on how much of `bronze_candles`'/`silver_candles`' history
falls inside each pre-aggregation's partition range.

## Querying it (production mode has no Developer Playground)

Cube's Developer Playground UI only exists in dev mode
(`CUBEJS_DEV_MODE=true`) -- and dev mode disables authentication and
role-based access control entirely, which isn't worth it just to get a
visual query browser. Running in production mode (as this stack does)
means the REST API is the only interface, gated by a real JWT.

`query.sh` wraps the token generation so you don't have to do it by hand
every time:
```bash
./query.sh '{"measures":["candles.count","candles.high"],"dimensions":["candles.coin_id"],"filters":[{"member":"candles.coin_id","operator":"equals","values":["bitcoin"]}],"timeDimensions":[{"dimension":"candles.open_time","dateRange":"last 7 days"}]}'
```
Run it from the server itself (`ssh ubuntu-server`, `cd
crypto-tracker-data-platform/cube`), or from any machine on the LAN with
`CUBE_HOST=http://YOUR_SERVER_IP:4000 ./query.sh '...'` (needs `docker`
installed locally too, since it shells out to `docker compose exec` to mint
the token -- from a machine without Docker, generate the JWT directly with
any JWT library instead, signing an empty payload with `CUBEJS_API_SECRET`
from `.env`).
