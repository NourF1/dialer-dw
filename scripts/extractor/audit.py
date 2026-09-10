import logging
from datetime import date, datetime
from typing import Optional
from google.cloud import bigquery

logger = logging.getLogger(__name__)

# Module-level schema constant matching DDL in config/raw_tables.sql
AUDIT_SCHEMA = [
    bigquery.SchemaField("_batch_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("_source", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("_extraction_date", "DATE", mode="REQUIRED"),
    bigquery.SchemaField("row_count", "INT64", mode="NULLABLE"),
    bigquery.SchemaField("status", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("error_type", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("error_message", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("started_at", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("finished_at", "TIMESTAMP", mode="REQUIRED"),
]


def log_extraction_run(
    client: bigquery.Client,
    project_id: str,
    dataset_id: str,
    batch_id: str,
    source: str,
    extraction_date: date,
    row_count: int,
    status: str,
    started_at: datetime,
    finished_at: datetime,
    error_type: Optional[str] = None,
    error_message: Optional[str] = None,
) -> None:
    """
    Appends execution audit records to dialer_dw_raw.extraction_runs using WRITE_APPEND.
    
    Enforces CREATE_NEVER to prevent auto-creating unpartitioned tables if DDL has not run.
    Accepts extraction_date as a datetime.date object.
    Isolated with internal exception handling so logging failures never disrupt the extraction pipeline.
    """
    table_ref = f"{project_id}.{dataset_id}.extraction_runs"

    job_config = bigquery.LoadJobConfig(
        schema=AUDIT_SCHEMA,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        create_disposition=bigquery.CreateDisposition.CREATE_NEVER,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    )

    run_record = [{
        "_batch_id": batch_id,
        "_source": source,
        "_extraction_date": extraction_date.isoformat(),
        "row_count": row_count,
        "status": status,
        "error_type": error_type,
        "error_message": error_message,
        "started_at": started_at.isoformat(),
        "finished_at": finished_at.isoformat(),
    }]

    try:
        job = client.load_table_from_json(run_record, table_ref, job_config=job_config)
        job.result()  # Wait for BigQuery write completion
    except Exception as audit_err:
        # Failure isolation: log error, but NEVER raise into orchestrator
        logger.error(
            f"[AUDIT LOG FAILURE] Failed to write run audit record to BigQuery for "
            f"batch_id={batch_id}, source={source}, date={extraction_date.isoformat()}. "
            f"Error: {audit_err}",
            exc_info=True
        )