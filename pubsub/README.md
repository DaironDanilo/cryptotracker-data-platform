# Pub/Sub ingestion backbone (Phase 4 / Phase 9 groundwork)

A single Pub/Sub topic, `candle-events`, is the central ingestion log for candle
data (Kappa-architecture style): every producer of candle data -- the future
real-time WebSocket worker and a future periodic REST reconciliation job --
writes into this one topic, and every consumer reads from it independently via
its own subscription. Producers are **not built in this phase**. This phase
only provisions the topic, subscriptions, dead-lettering, retry policy, and
monitoring, and proves they're wired correctly end-to-end with a manually
published test message.

**Status check (current as of this writing): zero live producers, zero real
traffic.** The only messages that have ever flowed through `candle-events`
are the manual test message in "Manual verification" below. In particular,
don't confuse the "periodic REST reconciliation job" planned here with
`recurring/binance-incremental-job`, which *is* live today (every 5 min) but
writes directly to BigQuery/Postgres via client libraries, bypassing Pub/Sub
entirely -- it is not, and was never meant to be, this topic's reconciliation
producer. `reconciliation-job/` (still an empty scaffold) is the actual
planned producer this README refers to, and it does not exist yet. This
means `candle-events-postgres` and `candle-events-redis` currently have no
consumer either -- both subscriptions exist and are monitored, but nothing
is pulling from them, so messages would just accumulate against retry/DLQ
policy if anything were ever published to the topic outside of manual
testing.

## Resources

| Resource | Purpose |
|---|---|
| `candle-events` | Main topic. 7-day **topic-level** message retention (not just subscription-level unacked retention) -- this is what makes `seek`-based replay of the full log possible regardless of ack state, the actual mechanism behind Kappa's "reprocess by replaying" promise. |
| `candle-events-bigquery` | BigQuery subscription -- Pub/Sub writes directly into `bronze_candles`, no consumer code. |
| `candle-events-postgres` | Plain pull subscription. Consumer process built in a later phase. |
| `candle-events-redis` | Plain pull subscription. Consumer process built in a later phase. |
| `candle-events-{bigquery,postgres,redis}-dlq` | One dead-letter topic per subscription. |
| `candle-events-test` | Least-privilege service account for manual verification only (see below) -- publisher on the topic, subscriber on the two pull subscriptions. Not meant for production producers/consumers, which should get their own dedicated service accounts when built. |

All three subscriptions have **message ordering enabled** (ordering key will be
`coin_id`, set by producers in a later phase), a **dead-letter policy**
(5 delivery attempts, then forward to that subscription's dedicated DLQ topic),
and a **retry policy** (10s-600s exponential backoff).

Provisioning: `./provision.sh` (idempotent, safe to re-run -- see the script
for the exact `gcloud` commands and prerequisites).

## A hard platform constraint this design ran into (read before changing the BigQuery subscription)

Getting `candle-events-bigquery` working correctly required two non-obvious
fixes, both confirmed empirically against the real service, not just docs:

1. **Pub/Sub's publish timestamp can only land in a column literally named
   `publish_time`.** The BigQuery subscription's "write metadata" feature
   uses fixed, non-aliasable column names
   (`publish_time`, `message_id`, `subscription_name`, `attributes`, `data`).
   `bronze_candles`'s write-time column was previously called `ingestion_time`
   -- it was renamed to `publish_time` for this reason (see
   `migration/load_binance_ndjson.sql`, which documents the rename). The batch
   backfill path and the streaming path now share one canonical column.
2. **`write_metadata`'s injected fields don't satisfy a `REQUIRED` column.**
   Even with `publish_time` correctly named, messages failed with `JSON is
   missing required field: publish_time` until the column was relaxed to
   `NULLABLE`. The metadata injection and BigQuery's "are all required fields
   present in the payload" pre-check apparently don't compose -- the
   required-field check runs against the raw JSON payload before metadata is
   merged in. `bronze_candles.publish_time` and the three other
   write-metadata sidecar columns (`subscription_name`, `message_id`,
   `attributes` -- also all required to exist, even though only
   `publish_time` is actually used downstream) are all `NULLABLE`.

Separately, also confirmed empirically (see "Manual verification" below):
**a bare JSON number for a TIMESTAMP column is interpreted as epoch
*seconds*, not milliseconds.** This is why the message schema below sends
`open_time`/`close_time` as RFC3339 strings, not the epoch-ms integers a
first draft of this schema used -- sending epoch-ms directly into a
BigQuery-subscription-mapped TIMESTAMP column silently produces an
out-of-range value and the message dead-letters.

## Message schema

JSON (no Avro/Protobuf at this scale):

```json
{
  "coin_id": "bitcoin",
  "interval": "1h",
  "open_time": "2026-07-14T22:00:00Z",
  "close_time": "2026-07-14T22:59:59Z",
  "open": "118000.50",
  "high": "118500.00",
  "low": "117900.25",
  "close": "118250.75",
  "volume": "1234.56789012",
  "quote_volume": "145678901.23",
  "trade_count": 45231,
  "is_closed": true,
  "source": "binance_ws"
}
```

- `open_time` / `close_time`: **RFC3339 / ISO-8601 strings**, not epoch
  milliseconds -- see the constraint above. (Binance's own wire format uses
  epoch-ms; producers built in a later phase need to convert.)
- `open` / `high` / `low` / `close` / `volume` / `quote_volume`: strings, to
  preserve precision from Binance's own string-typed REST/WS payloads --
  don't convert to float before publishing.
- `source`: `"binance_ws"` or `"binance_rest"`, identifying which producer
  (real-time WS worker vs. periodic reconciliation job) sent it -- both
  write into the same topic per the Kappa design.
- `publish_time` is **not** part of the payload -- it's populated on the
  BigQuery path from Pub/Sub's own publish timestamp (see above), and the
  Postgres/Redis consumers (built later) should use the Pub/Sub message's own
  `publishTime` field the client library exposes, for the same reason: it
  should reflect when the message was actually published, not something a
  producer claims.

**This schema will gain fields later. Consumers must ignore unknown fields
rather than fail on them** (the BigQuery subscription is already configured
with `--drop-unknown-fields`; Postgres/Redis consumers built later should do
the equivalent). Future changes should be additive-only unless every
producer and consumer is redeployed together.

## Manual verification

This is the exact sequence used to prove the wiring above actually works
(not hypothetical -- every step here was run for real against the live
project while building this phase).

1. **Publish a test message**, impersonating the least-privilege
   `candle-events-test` service account (requires the account running this
   command to have `roles/iam.serviceAccountTokenCreator` on that SA --
   `provision.sh` does not grant this to your own user automatically; grant
   it to yourself once with:
   ```bash
   gcloud iam service-accounts add-iam-policy-binding \
     candle-events-test@gcp-crypto-tracker.iam.gserviceaccount.com \
     --member="user:YOUR_EMAIL" --role="roles/iam.serviceAccountTokenCreator"
   ```
   -- newly-granted IAM bindings can take up to ~30-60s to propagate; if
   impersonation fails immediately after granting, wait and retry).

   Use a `coin_id` that can't collide with real tracked coins if you're
   testing against a live environment -- something like
   `"coin_id": "pubsub-wiring-test"` -- so you can unambiguously tell your
   test row apart from real data and clean it up safely afterward.

   ```bash
   gcloud pubsub topics publish candle-events \
     --project=gcp-crypto-tracker \
     --impersonate-service-account=candle-events-test@gcp-crypto-tracker.iam.gserviceaccount.com \
     --ordering-key=pubsub-wiring-test \
     --message='{
       "coin_id": "pubsub-wiring-test",
       "interval": "1h",
       "open_time": "2026-07-14T22:00:00Z",
       "close_time": "2026-07-14T22:59:59Z",
       "open": "118000.50",
       "high": "118500.00",
       "low": "117900.25",
       "close": "118250.75",
       "volume": "1234.56789012",
       "quote_volume": "145678901.23",
       "trade_count": 45231,
       "is_closed": true,
       "source": "binance_ws"
     }'
   ```

2. **Pull it from the two plain subscriptions** (each is independently
   acked -- pulling from one doesn't affect the other):
   ```bash
   gcloud pubsub subscriptions pull candle-events-postgres --project=gcp-crypto-tracker \
     --impersonate-service-account=candle-events-test@gcp-crypto-tracker.iam.gserviceaccount.com \
     --auto-ack --limit=5

   gcloud pubsub subscriptions pull candle-events-redis --project=gcp-crypto-tracker \
     --impersonate-service-account=candle-events-test@gcp-crypto-tracker.iam.gserviceaccount.com \
     --auto-ack --limit=5
   ```
   You should see the message, its `orderingKey`, and its `publishTime`.

3. **Confirm it landed in BigQuery** (allow up to a minute or two for the
   BigQuery subscription to write it; a first-time cold start can take
   longer):
   ```sql
   SELECT coin_id, open_time, open, close, publish_time, subscription_name, message_id
   FROM bronze.bronze_candles
   WHERE coin_id = 'pubsub-wiring-test'
   ```
   `publish_time` should be close to when you ran step 1 (Pub/Sub's own
   timestamp), and `message_id`/`subscription_name` should be populated --
   confirming the write-metadata path, not the payload, supplied them.

4. **If it doesn't show up**, check whether it dead-lettered instead. There's
   no direct "peek" at a topic -- create a temporary pull subscription on the
   relevant DLQ topic, inspect it, then delete the temporary subscription
   when done:
   ```bash
   gcloud pubsub subscriptions create candle-events-bigquery-dlq-debug \
     --project=gcp-crypto-tracker --topic=candle-events-bigquery-dlq
   gcloud pubsub subscriptions pull candle-events-bigquery-dlq-debug --project=gcp-crypto-tracker \
     --auto-ack --limit=10
   # the dead-lettered message's attributes include the failure reason, e.g.:
   #   CloudPubSubDeadLetterSourceDeliveryErrorMessage: "JSON is missing required field: publish_time"
   gcloud pubsub subscriptions delete candle-events-bigquery-dlq-debug --project=gcp-crypto-tracker --quiet
   ```
   A message only reaches the DLQ after 5 failed delivery attempts with
   backoff -- this can take several minutes, not seconds. The monitoring
   alert (below) fires as soon as it does, faster than manually polling.

5. **Clean up your test row** once verified:
   ```sql
   DELETE FROM bronze.bronze_candles WHERE coin_id = 'pubsub-wiring-test'
   ```

## Monitoring

Two alerting policies (email notification channel, configurable via
`NOTIFICATION_EMAIL` in `provision.sh`), each covering all three
subscriptions independently:

- **`oldest_unacked_message_age > 600s` for 5 minutes** -- a rising value on
  any subscription means that subscription's consumer is stuck, crashed, or
  can't keep up.
- **`dead_letter_message_count > 0`** -- fires immediately on any message
  reaching a dead-letter topic. This should never happen in normal
  operation; when it fires, check the named subscription's consumer and
  inspect the dead-letter topic's messages (see step 4 above).

Both were confirmed firing for real during this phase's own verification
work (the second fix above was found *because* the dead-letter alert fired
and pointed at the exact error).
