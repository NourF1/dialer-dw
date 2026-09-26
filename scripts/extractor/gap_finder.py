"""Self-healing gap detection for extract partitions."""

import logging
from datetime import date, timedelta
from typing import TYPE_CHECKING, List, Set

if TYPE_CHECKING:
    from google.cloud import bigquery

logger = logging.getLogger(__name__)


def generate_expected_weekdays(start_date: date, through_date: date) -> List[date]:
    """Pure. Generates a list of weekday dates (Monday-Friday) inclusive of start_date and through_date."""
    expected = []
    curr = start_date
    while curr <= through_date:
        if curr.weekday() < 5:  # 0=Mon, 4=Fri, 5=Sat, 6=Sun
            expected.append(curr)
        curr += timedelta(days=1)
    return expected


def compute_missing(
    expected: List[date],
    completed: Set[date],
    max_dates: int,
) -> List[date]:
    """Pure. Dates in `expected` absent from `completed`, oldest first,
    truncated to max_dates.
    """
    missing = [d for d in expected if d not in completed]
    return missing[:max_dates]


def find_missing_dates(
    client: "bigquery.Client",
    project_id: str,
    dataset_id: str,
    through_date: date,
    n_sources: int = 2,
    lookback_days: int = 45,
    max_dates: int = 10,
) -> List[date]:
    """Query extraction_runs for completed dates, build expected weekdays, delegate to compute_missing."""
    from google.cloud import bigquery

    start_date = through_date - timedelta(days=lookback_days)
    table_ref = f"{project_id}.{dataset_id}.extraction_runs"

    query = f"""
    select _extraction_date as d
    from `{table_ref}`
    where status = 'success'
      and _extraction_date between @start_date and @through_date
    group by d
    having count(distinct _source) >= @n_sources
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("start_date", "DATE", start_date),
            bigquery.ScalarQueryParameter("through_date", "DATE", through_date),
            bigquery.ScalarQueryParameter("n_sources", "INT64", n_sources),
        ]
    )

    logger.info(
        "Querying %s for completed dates from %s through %s (n_sources=%d)",
        table_ref,
        start_date,
        through_date,
        n_sources,
    )

    query_job = client.query(query, job_config=job_config)
    rows = query_job.result()

    # I/O boundary: extract BigQuery date results into Python set
    completed_dates: Set[date] = {row.d for row in rows}

    # Delegate logic to pure functions
    expected_weekdays = generate_expected_weekdays(start_date, through_date)

    return compute_missing(
        expected=expected_weekdays,
        completed=completed_dates,
        max_dates=max_dates,
    )