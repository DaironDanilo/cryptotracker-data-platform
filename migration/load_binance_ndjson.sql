-- Loads Binance kline NDJSON files staged in GCS into a bronze_candles-shaped
-- table. Called by ingest-workflow.yaml's binance_branch after binance-ingest
-- writes raw files to gs://gcp-crypto-tracker-raw-binance/.
--
-- Lives in the `bronze` dataset (medallion architecture, Phase 5) -- moved
-- from `crypto_tracker` along with bronze_candles/market_cap_history.
--
-- `publish_time` (renamed from `ingestion_time`) must match the column name
-- Pub/Sub's BigQuery-subscription "write metadata" feature uses -- see
-- pubsub/README.md. Keeping one canonical column name lets both the batch
-- (this proc) and streaming (Pub/Sub) paths populate bronze_candles the same
-- way.
CREATE OR REPLACE PROCEDURE `gcp-crypto-tracker.bronze.load_binance_ndjson`(
  gcs_uri STRING,
  target_table STRING
)
BEGIN
  DECLARE sql STRING;
  SET sql = FORMAT("""
    LOAD DATA INTO `gcp-crypto-tracker.bronze.%s`
    (
      coin_id STRING,
      `interval` STRING,
      open_time TIMESTAMP,
      close_time TIMESTAMP,
      open NUMERIC,
      high NUMERIC,
      low NUMERIC,
      close NUMERIC,
      volume NUMERIC,
      quote_volume NUMERIC,
      trade_count INT64,
      is_closed BOOL,
      publish_time TIMESTAMP
    )
    FROM FILES (
      format = "NEWLINE_DELIMITED_JSON",
      uris = [%s]
    )
  """, target_table, TO_JSON_STRING(gcs_uri));
  EXECUTE IMMEDIATE sql;
END
