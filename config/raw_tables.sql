-- config/raw_tables.sql
-- Dataset: dialer-dw-prod.dialer_dw_raw
-- Purpose: Ingestion tables for verbatim API payload storage and run audit metadata logs.
-- Note for Maintainers: Created in a sandbox environment; data has a maximum 60-day expiration policy.

-- -----------------------------------------------------------------------------
-- Ensure Raw Dataset Exists
-- -----------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS `dialer-dw-prod.dialer_dw_raw`
OPTIONS(
  location="US",
  description="Raw layer for ingested dialer JSON payloads and execution metrics (Sandbox environment: 60-day retention)."
);

-- -----------------------------------------------------------------------------
-- Table 1: raw_call_log
-- Grain: One row per call log entry
-- Write Disposition: WRITE_TRUNCATE per partition key (_extraction_date)
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
  require_partition_filter = FALSE,
  partition_expiration_days = 60
);

-- -----------------------------------------------------------------------------
-- Table 2: raw_dialer_report
-- Grain: One row per campaign per day
-- Write Disposition: WRITE_TRUNCATE per partition key (_extraction_date)
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
  require_partition_filter = FALSE,
  partition_expiration_days = 60
);

-- -----------------------------------------------------------------------------
-- Table 3: extraction_runs
-- Grain: One row per extraction unit execution attempt (accumulative audit log)
-- Write Disposition: WRITE_APPEND (Never truncate)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `dialer-dw-prod.dialer_dw_raw.extraction_runs`
(
  _batch_id STRING NOT NULL OPTIONS(description="Ties back to rows in the raw tables for lineage tracking"),
  _source STRING NOT NULL OPTIONS(description="Identifier for target source (readymode.call_log / readymode.dialer_report)"),
  _extraction_date DATE NOT NULL OPTIONS(description="The target business date for this extraction unit attempt"),
  row_count INT64 OPTIONS(description="Total rows fetched and loaded into destination (0 is meaningful)"),
  status STRING NOT NULL OPTIONS(description="Status outcome of attempt (e.g., success / failed)"),
  error_type STRING OPTIONS(description="Class or categorization of error if attempt failed (nullable)"),
  error_message STRING OPTIONS(description="Verbatim exception or error response string (nullable)"),
  started_at TIMESTAMP NOT NULL OPTIONS(description="Timestamp indicating when the run unit commenced - partition key"),
  finished_at TIMESTAMP NOT NULL OPTIONS(description="Timestamp indicating when the run unit completed")
)
PARTITION BY DATE(started_at)
CLUSTER BY _source, status
OPTIONS(
  description="Audit trail accumulator table for tracking extractor executions, retries, row counts, and failures. Partitioned by execution date.",
  require_partition_filter = FALSE,
  partition_expiration_days = 60
);