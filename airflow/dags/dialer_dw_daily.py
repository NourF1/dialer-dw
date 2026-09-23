from datetime import date, timedelta
import pendulum

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator


def _extract(ds: str) -> None:
    """Adapter function converting Airflow template string to python date object.
    
    Imported inside callable to keep DAG parsing lightweight.
    """
    from extractor.run_extract import run_extraction_for_date

    run_extraction_for_date(target_date=date.fromisoformat(ds))


def _check_weekday_freshness(ds: str) -> bool:
    """Short-circuits source freshness check on weekend execution dates."""
    return date.fromisoformat(ds).weekday() < 5


default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),
}

with DAG(
    dag_id="dialer_dw_daily",
    start_date=pendulum.datetime(2026, 8, 1, tz="America/New_York"),
    schedule="0 6 * * *",         
    catchup=False,
    max_active_runs=1,
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