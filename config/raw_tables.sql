-- config/raw_tables.sql
-- Dataset: dialer-dw-prod.dialer_dw_raw
-- Purpose: Ingestion tables for verbatim API payload storage
-- Note for Maintainers: Created in a sandbox environment; data has a maximum 60-day expiration policy.

-- -----------------------------------------------------------------------------
-- Ensure Raw Dataset Exists
-- -----------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS `dialer-dw-prod.dialer_dw_raw`
OPTIONS(
  location="US",
  description="Raw layer for ingested dialer JSON payloads (Sandbox environment: 60-day retention)."
);

-- -----------------------------------------------------------------------------
-- Table 1: raw_call_log
-- Grain: One row per call log entry
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `dialer-dw-prod.dialer_dw_raw.raw_call_log`
(
  payload STRING NOT NULL OPTIONS(description="Source record verbatim stored as a raw JSON string"),
  _extraction_date DATE NOT NULL OPTIONS(description="Business date requested for data extraction - partition key"),
  _loaded_at TIMESTAMP NOT NULL OPTIONS(description="Timestamp indicating when the ingestion batch was written"),
  _batch_id STRING NOT NULL OPTIONS(description="Unique UUID generated per extractor run"),
  _source STRING NOT NULL OPTIONS(description="Data source identifier (readymode.call_log)")
)
PARTITION BY _extraction_date
CLUSTER BY _source
OPTIONS(
  description="Raw table holding verbatim API JSON payloads for Readymode call logs. Partitioned daily by business date. SANDBOX RETENTION: 60-day max expiry date.",
  require_partition_filter = FALSE
);

-- -----------------------------------------------------------------------------
-- Table 2: raw_dialer_report
-- Grain: One row per campaign per day
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `dialer-dw-prod.dialer_dw_raw.raw_dialer_report`
(
  payload STRING NOT NULL OPTIONS(description="Source record verbatim stored as a raw JSON string"),
  _extraction_date DATE NOT NULL OPTIONS(description="Business date requested for data extraction - partition key"),
  _loaded_at TIMESTAMP NOT NULL OPTIONS(description="Timestamp indicating when the ingestion batch was written"),
  _batch_id STRING NOT NULL OPTIONS(description="Unique UUID generated per extractor run"),
  _source STRING NOT NULL OPTIONS(description="Data source identifier (readymode.dialer_report)")
)
PARTITION BY _extraction_date
CLUSTER BY _source
OPTIONS(
  description="Raw table holding verbatim API JSON payloads for Readymode dialer reports. Partitioned daily by business date. SANDBOX RETENTION: 60-day max expiry date.",
  require_partition_filter = FALSE
);