import logging
from datetime import date, timedelta
import pendulum

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator

logger = logging.getLogger("airflow.task")


def _extract(ds: str) -> None:
    """Adapter function finding missing weekday partitions up to ds and extracting them.

    Imports modules inside callable to keep DAG parsing lightweight.
    """
    from google.cloud import bigquery
    from extractor.config import Config
    from extractor.gap_finder import find_missing_dates
    from extractor.run_extract import run_extraction_for_date

    config = Config.from_env()
    bq_client = bigquery.Client(project=config.gcp_project)
    through = date.fromisoformat(ds)

    missing = find_missing_dates(
        client=bq_client,
        project_id=config.gcp_project,
        dataset_id=config.raw_dataset,
        through_date=through,
    )

    if not missing:
        logger.info("No gaps through %s; nothing to extract.", ds)
        return

    logger.info("Extracting %d date(s): %s", len(missing), missing)
    run_extraction_for_date(target_date=missing[0], target_dates=missing)


def _check_weekday_freshness(ds: str) -> bool:
    """Short-circuits source freshness check on weekend execution dates."""
    return date.fromisoformat(ds).weekday() < 5


default_args = {
    "owner": "airflow",
    "retries": 4,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "execution_timeout": timedelta(minutes=30),
}

with DAG(
    dag_id="dialer_dw_daily",
    start_date=pendulum.datetime(2026, 8, 1, tz="America/New_York"),
    schedule="0 6 * * *",
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    dagrun_timeout=timedelta(minutes=90),
    default_args=default_args,
    tags=["dialer", "bq", "dbt"],
) as dag:

    # 1. Extraction Task
    extract = PythonOperator(
        task_id="extract",
        python_callable=_extract,
        op_kwargs={"ds": "{{ ds }}"},
    )

    # 2. Weekend Short-Circuit Gate for Freshness Check
    freshness_gate = ShortCircuitOperator(
        task_id="freshness_gate",
        python_callable=_check_weekday_freshness,
        op_kwargs={"ds": "{{ ds }}"},
    )

    # 3. Freshness Check Execution
    dbt_source_freshness = BashOperator(
        task_id="dbt_source_freshness",
        bash_command="dbt source freshness --target prod",
    )

    # 4. dbt Operations
    dbt_seed = BashOperator(
        task_id="dbt_seed",
        bash_command="dbt seed --target prod",
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="python -m dbt_retry.run_with_retry run --target prod",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="python -m dbt_retry.run_with_retry test --target prod",
    )

    # Dependencies
    extract >> freshness_gate >> dbt_source_freshness
    extract >> dbt_seed >> dbt_run >> dbt_test