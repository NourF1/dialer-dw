import uuid
from datetime import date
import pytest
from google.cloud import bigquery

from extractor.bq_loader import RawTableMissingError, load_partition

# Sandbox test parameters
PROJECT_ID = "dialer-dw-prod"
DATASET_ID = "dialer_dw_raw"
TABLE_NAME = "raw_call_log"
TEST_TABLE_ID = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_NAME}"
INVALID_TABLE_ID = f"{PROJECT_ID}.{DATASET_ID}.non_existent_table"


@pytest.fixture(scope="module")
def bq_client():
    """Provides an active BigQuery client."""
    return bigquery.Client(project=PROJECT_ID)


def query_partition_rows(client: bigquery.Client, extraction_date: date) -> list[dict]:
    """Helper utility to fetch records for a specific extraction date."""
    query = f"""
        SELECT payload, _extraction_date, _loaded_at, _batch_id, _source
        FROM `{TEST_TABLE_ID}`
        WHERE _extraction_date = '{extraction_date.isoformat()}'
    """
    query_job = client.query(query)
    return [dict(row) for row in query_job.result()]


def test_bq_loader_validation_sequence(bq_client):
    # Setup test dates
    scratch_date = date(2099, 9, 1)
    second_date = date(2099, 9, 2)
    source = "readymode.call_log"

    # -------------------------------------------------------------------------
    # Scenario 1: 3 fake rows, scratch date -> SELECT returns 3
    # -------------------------------------------------------------------------
    batch_1 = str(uuid.uuid4())
    rows_batch_1 = [
        {"call_id": "101", "agent": "Alice", "duration": 120},
        {"call_id": "102", "agent": "Bob", "duration": 45},
        {"call_id": "103", "agent": "Charlie", "duration": 300},
    ]

    written_1 = load_partition(
        client=bq_client,
        table_id=TEST_TABLE_ID,
        rows=rows_batch_1,
        extraction_date=scratch_date,
        source=source,
        batch_id=batch_1,
    )

    results_1 = query_partition_rows(bq_client, scratch_date)

    assert written_1 == 3
    assert len(results_1) == 3
    assert all(r["_batch_id"] == batch_1 for r in results_1)

    # -------------------------------------------------------------------------
    # Scenario 2: 3 DIFFERENT rows, SAME date -> still 3, new content, new _batch_id
    # -------------------------------------------------------------------------
    batch_2 = str(uuid.uuid4())
    rows_batch_2 = [
        {"call_id": "201", "agent": "Dave", "duration": 15},
        {"call_id": "202", "agent": "Eve", "duration": 90},
        {"call_id": "203", "agent": "Frank", "duration": 210},
    ]

    written_2 = load_partition(
        client=bq_client,
        table_id=TEST_TABLE_ID,
        rows=rows_batch_2,
        extraction_date=scratch_date,
        source=source,
        batch_id=batch_2,
    )

    results_2 = query_partition_rows(bq_client, scratch_date)

    assert written_2 == 3
    assert len(results_2) == 3
    assert all(r["_batch_id"] == batch_2 for r in results_2)
    # Confirm old batch_1 content was overwritten cleanly by WRITE_TRUNCATE
    assert not any(r["_batch_id"] == batch_1 for r in results_2)

    # -------------------------------------------------------------------------
    # Scenario 3: table_id that doesn't exist -> RawTableMissingError, one clean line
    # -------------------------------------------------------------------------
    with pytest.raises(RawTableMissingError) as exc_info:
        load_partition(
            client=bq_client,
            table_id=INVALID_TABLE_ID,
            rows=rows_batch_1,
            extraction_date=scratch_date,
            source=source,
            batch_id=str(uuid.uuid4()),
        )

    assert "does not exist. Run config/raw_tables.sql" in str(exc_info.value)
    # Check that exception chaining was suppressed (from None)
    assert exc_info.value.__cause__ is None

    # -------------------------------------------------------------------------
    # Scenario 4: rows = [] -> Wipes partition, returns 0 rows
    # -------------------------------------------------------------------------
    batch_3 = str(uuid.uuid4())
    written_3 = load_partition(
        client=bq_client,
        table_id=TEST_TABLE_ID,
        rows=[],
        extraction_date=scratch_date,
        source=source,
        batch_id=batch_3,
    )

    results_3 = query_partition_rows(bq_client, scratch_date)

    assert written_3 == 0
    assert len(results_3) == 0

    # -------------------------------------------------------------------------
    # Scenario 5: second date -> first partition untouched
    # -------------------------------------------------------------------------
    # Re-populate scratch_date with batch_1
    load_partition(
        client=bq_client,
        table_id=TEST_TABLE_ID,
        rows=rows_batch_1,
        extraction_date=scratch_date,
        source=source,
        batch_id=batch_1,
    )

    # Load data for second_date
    batch_4 = str(uuid.uuid4())
    rows_second_date = [
        {"call_id": "301", "agent": "Grace", "duration": 60},
        {"call_id": "302", "agent": "Heidi", "duration": 180},
    ]

    written_4 = load_partition(
        client=bq_client,
        table_id=TEST_TABLE_ID,
        rows=rows_second_date,
        extraction_date=second_date,
        source=source,
        batch_id=batch_4,
    )

    results_scratch = query_partition_rows(bq_client, scratch_date)
    results_second = query_partition_rows(bq_client, second_date)

    assert written_4 == 2
    assert len(results_scratch) == 3  # scratch_date is completely untouched
    assert len(results_second) == 2
