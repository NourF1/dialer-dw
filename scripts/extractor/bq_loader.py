import json
from datetime import date, datetime, timezone
from google.api_core.exceptions import NotFound
from google.cloud import bigquery


class RawTableMissingError(RuntimeError):
    """A raw table doesn't exist. Run config/raw_tables.sql."""


# Standard 5-column ingestion schema matching raw_tables.sql DDL exactly
RAW_SCHEMA = [
    bigquery.SchemaField("payload", "STRING", mode="REQUIRED", description="Source record verbatim stored as a raw JSON string"),
    bigquery.SchemaField("_extraction_date", "DATE", mode="REQUIRED", description="Business date requested for data extraction - partition key"),
    bigquery.SchemaField("_loaded_at", "TIMESTAMP", mode="REQUIRED", description="Timestamp indicating when the ingestion batch was written"),
    bigquery.SchemaField("_batch_id", "STRING", mode="REQUIRED", description="Unique UUID generated per extractor run"),
    bigquery.SchemaField("_source", "STRING", mode="REQUIRED", description="Data source identifier (readymode.call_log)"),
]


def load_partition(
    client: bigquery.Client,
    table_id: str,          # "dialer-dw-prod.dialer_dw_raw.raw_call_log"
    rows: list[dict],       # verbatim dict records from readymode_client
    extraction_date: date,
    source: str,            # "readymode.call_log"
    batch_id: str,          # UUID for this run
) -> int:                   # rows written
    """
    Loads daily extracted API payloads into a specific BigQuery table partition.
    Uses WRITE_TRUNCATE with partition decorators to enforce idempotent partition replacement.
    """
    # 1. Option C Behavior: Guard check for table existence
    try:
        client.get_table(table_id)
    except NotFound:
        raise RawTableMissingError(
            f"Table '{table_id}' does not exist. Run config/raw_tables.sql to create missing raw tables."
        ) from None

    # 2. Zero Rows Behavior Handling
    if not rows:
        formatted_rows = []
    else:
        # Timezone-aware UTC ISO timestamp
        loaded_at_str = datetime.now(timezone.utc).isoformat()
        extraction_date_str = extraction_date.isoformat()

        formatted_rows = [
            {
                "payload": json.dumps(row),
                "_extraction_date": extraction_date_str,
                "_loaded_at": loaded_at_str,
                "_batch_id": batch_id,
                "_source": source,
            }
            for row in rows
        ]

    # Partition-scoped target table specifier
    # CRITICAL: Omitting '${extraction_date:%Y%m%d}' while using WRITE_TRUNCATE will trigger a full table
    # wipe, purging every partition in this table instead of updating a single day.
    partition_target = f"{table_id}${extraction_date:%Y%m%d}"

    job_config = bigquery.LoadJobConfig(
        schema=RAW_SCHEMA,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        autodetect=False,  # Enforce explicit schema mapping
    )

    # Execute load job using partition decorator
    load_job = client.load_table_from_json(
        json_rows=formatted_rows,
        destination=partition_target,
        job_config=job_config,
    )

    # Wait for execution completion
    load_job.result()

    # TODO: Implement run-level observability by writing batch metadata, row counts, 
    # source names, and extraction dates into a dedicated `extraction_runs` audit table.

    return len(formatted_rows)