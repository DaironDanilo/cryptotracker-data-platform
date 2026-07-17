#!/usr/bin/env bash
# Provisions the Phase 4 Pub/Sub ingestion backbone: one topic (candle-events),
# a 7-day topic-level retention (enables seek-based replay regardless of ack
# state), three independently-acked subscriptions (one per future consumer),
# each with message ordering (ordering_key = coin_id, set by producers in a
# later phase), a dead-letter policy, and a retry policy.
#
# Idempotent: safe to re-run. Every resource is existence-checked before
# creation.
#
# Prerequisite (already applied to bronze_candles by this phase, documented
# here for reproducibility): the candle-events-bigquery subscription writes
# Pub/Sub metadata into the target table, which requires the destination
# table to have these exact columns in addition to the payload columns:
#   publish_time       TIMESTAMP   (populated from Pub/Sub's publish time)
#   subscription_name  STRING      (required by write-metadata, unused downstream)
#   message_id         STRING      (required by write-metadata, unused downstream)
#   attributes         STRING      (required by write-metadata, unused downstream)
# bronze_candles already has these (ingestion_time was renamed to
# publish_time; the other three were added new).

set -euo pipefail

PROJECT_ID="${PROJECT_ID:-gcp-crypto-tracker}"
REGION="${REGION:-us-central1}"
BQ_TABLE="${BQ_TABLE:-gcp-crypto-tracker:bronze.bronze_candles}"
NOTIFICATION_EMAIL="${NOTIFICATION_EMAIL:-ddanilodorado@gmail.com}"

PROJECT_NUMBER="$(gcloud projects describe "$PROJECT_ID" --format='value(projectNumber)')"
PUBSUB_SA="service-${PROJECT_NUMBER}@gcp-sa-pubsub.iam.gserviceaccount.com"

topic_exists() { gcloud pubsub topics describe "$1" --project="$PROJECT_ID" >/dev/null 2>&1; }
sub_exists() { gcloud pubsub subscriptions describe "$1" --project="$PROJECT_ID" >/dev/null 2>&1; }
sa_exists() { gcloud iam service-accounts describe "$1" --project="$PROJECT_ID" >/dev/null 2>&1; }

echo "== main topic: candle-events (7d retention) =="
if topic_exists candle-events; then
  echo "  already exists, skipping create"
else
  gcloud pubsub topics create candle-events --project="$PROJECT_ID" --message-retention-duration=7d
fi

echo "== dead-letter topics =="
for dlq in candle-events-bigquery-dlq candle-events-postgres-dlq candle-events-redis-dlq; do
  if topic_exists "$dlq"; then
    echo "  $dlq already exists, skipping create"
  else
    gcloud pubsub topics create "$dlq" --project="$PROJECT_ID"
  fi
  gcloud pubsub topics add-iam-policy-binding "$dlq" --project="$PROJECT_ID" \
    --member="serviceAccount:${PUBSUB_SA}" --role="roles/pubsub.publisher" >/dev/null
done

echo "== BigQuery write access for the pubsub service agent (table-scoped, least-privilege) =="
bq add-iam-policy-binding --member="serviceAccount:${PUBSUB_SA}" --role="roles/bigquery.dataEditor" "$BQ_TABLE" >/dev/null
gcloud projects add-iam-policy-binding "$PROJECT_ID" --member="serviceAccount:${PUBSUB_SA}" \
  --role="roles/bigquery.jobUser" --condition=None >/dev/null

echo "== candle-events-bigquery (BigQuery subscription -> $BQ_TABLE) =="
if sub_exists candle-events-bigquery; then
  echo "  already exists, skipping create"
else
  gcloud pubsub subscriptions create candle-events-bigquery \
    --project="$PROJECT_ID" \
    --topic=candle-events \
    --bigquery-table="$BQ_TABLE" \
    --use-table-schema \
    --write-metadata \
    --drop-unknown-fields \
    --enable-message-ordering \
    --dead-letter-topic=candle-events-bigquery-dlq \
    --max-delivery-attempts=5 \
    --min-retry-delay=10s \
    --max-retry-delay=600s
fi
gcloud pubsub subscriptions add-iam-policy-binding candle-events-bigquery --project="$PROJECT_ID" \
  --member="serviceAccount:${PUBSUB_SA}" --role="roles/pubsub.subscriber" >/dev/null

echo "== candle-events-postgres / candle-events-redis (plain pull subscriptions) =="
for name in postgres redis; do
  sub="candle-events-$name"
  if sub_exists "$sub"; then
    echo "  $sub already exists, skipping create"
  else
    gcloud pubsub subscriptions create "$sub" \
      --project="$PROJECT_ID" \
      --topic=candle-events \
      --enable-message-ordering \
      --dead-letter-topic="candle-events-$name-dlq" \
      --max-delivery-attempts=5 \
      --min-retry-delay=10s \
      --max-retry-delay=600s
  fi
  gcloud pubsub subscriptions add-iam-policy-binding "$sub" --project="$PROJECT_ID" \
    --member="serviceAccount:${PUBSUB_SA}" --role="roles/pubsub.subscriber" >/dev/null
done

echo "== least-privilege test/verification service account =="
if sa_exists "candle-events-test@${PROJECT_ID}.iam.gserviceaccount.com"; then
  echo "  candle-events-test already exists, skipping create"
else
  gcloud iam service-accounts create candle-events-test --project="$PROJECT_ID" \
    --display-name="candle-events manual verification (publisher on topic, subscriber on pull subs)"
  sleep 15  # new service accounts take a few seconds to propagate to IAM
fi
TEST_SA="candle-events-test@${PROJECT_ID}.iam.gserviceaccount.com"
gcloud pubsub topics add-iam-policy-binding candle-events --project="$PROJECT_ID" \
  --member="serviceAccount:${TEST_SA}" --role="roles/pubsub.publisher" >/dev/null
for name in postgres redis; do
  gcloud pubsub subscriptions add-iam-policy-binding "candle-events-$name" --project="$PROJECT_ID" \
    --member="serviceAccount:${TEST_SA}" --role="roles/pubsub.subscriber" >/dev/null
done

echo "== monitoring: notification channel + alert policies =="
CHANNEL="$(gcloud alpha monitoring channels list --project="$PROJECT_ID" \
  --filter='displayName="candle-events pipeline alerts"' --format='value(name)' | head -1)"
if [ -z "$CHANNEL" ]; then
  CHANNEL="$(gcloud alpha monitoring channels create --project="$PROJECT_ID" \
    --display-name="candle-events pipeline alerts" --type=email \
    --channel-labels="email_address=${NOTIFICATION_EMAIL}" --format='value(name)')"
fi
for policy_file in "$(dirname "$0")/oldest-unacked-policy.json" "$(dirname "$0")/dead-letter-policy.json"; do
  name="$(python3 -c "import json;print(json.load(open('$policy_file'))['displayName'])")"
  existing="$(gcloud alpha monitoring policies list --project="$PROJECT_ID" \
    --filter="displayName=\"$name\"" --format='value(name)' | head -1)"
  if [ -n "$existing" ]; then
    echo "  policy '$name' already exists, skipping create"
  else
    gcloud alpha monitoring policies create --project="$PROJECT_ID" \
      --policy-from-file="$policy_file" --notification-channels="$CHANNEL" >/dev/null
  fi
done

echo "== done =="
gcloud pubsub subscriptions list --project="$PROJECT_ID" \
  --format="table(name.basename(), topic.basename(), deadLetterPolicy.deadLetterTopic.basename(), enableMessageOrdering)"
