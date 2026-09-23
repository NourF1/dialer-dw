import argparse
import logging
import sys
import uuid
from datetime import date, datetime, timedelta, timezone
from typing import List, Optional, Tuple

from google.cloud import bigquery

from extractor.audit import log_extraction_run
from extractor.bq_loader import (
    RawTableMissingError,
    load_partition,
)
from extractor.config import Config, ConfigError
from extractor.readymode_client import (
    LoginError,
    ReadymodeClient,
    ReadymodeFormatError,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("run_extract")

SOURCE_CONFIG = {
    "call_log": (
        "fetch_call_log",
        "raw_call_log",
        "readymode.call_log",
    ),
    "dialer_report": (
        "fetch_dialer_report",
        "raw_dialer_report",
        "readymode.dialer_report",
    ),
}

AVAILABLE_SOURCES = list(SOURCE_CONFIG.keys())


class ExtractionError(Exception):
    """Raised when one or more extraction units fail."""


def daterange(start_date: date, end_date: date):
    """Generates dates sequentially inclusive of start_date and end_date."""
    curr = start_date
    while curr <= end_date:
        yield curr
        curr += timedelta(days=1)


def run_extraction_for_date(
    target_date: date,
    source: Optional[str] = None,
    backfill_to: Optional[date] = None,
) -> None:
    """Callable entry point for Airflow or programmatic execution.

    Raises ExtractionError on failure to ensure Airflow tasks fail correctly.
    """
    config = Config.from_env()
    batch_id = str(uuid.uuid4())
    logger.info(f"Starting run. Batch ID: {batch_id}")

    # Determine dates and sources
    if backfill_to:
        cutoff_date = date.today() - timedelta(days=60)
        if target_date < cutoff_date:
            logger.warning(
                f"Backfill start date ({target_date}) precedes 60-day BigQuery partition expiry window ({cutoff_date})."
            )
        target_dates = list(daterange(target_date, backfill_to))
    else:
        target_dates = [target_date]

    target_sources = [source] if source else AVAILABLE_SOURCES
    units_to_run = [(d, src) for d in target_dates for src in target_sources]

    successful_units: List[Tuple[date, str]] = []
    failed_units: List[Tuple[date, str, str]] = []

    client = ReadymodeClient(
        config.readymode_url,
        config.readymode_user,
        config.readymode_password,
    )
    bq_client = bigquery.Client(project=config.gcp_project)

    try:
        logger.info("Logging in to ReadyMode...")
        client.login()
    except LoginError as err:
        logger.critical(
            f"Pre-execution login failed: {err}. Writing failure audit rows for all queued units."
        )
        now = datetime.now(timezone.utc)
        for t_date, src in units_to_run:
            _, _, source_label = SOURCE_CONFIG[src]
            log_extraction_run(
                client=bq_client,
                project_id=config.gcp_project,
                dataset_id=config.raw_dataset,
                batch_id=batch_id,
                source=source_label,
                extraction_date=t_date,
                row_count=None,
                status="failed",
                started_at=now,
                finished_at=now,
                error_type="LoginError",
                error_message=f"Run aborted during pre-execution login: {err}",
            )
            failed_units.append((t_date, src, f"LoginError: {err}"))

        print_summary(batch_id, units_to_run, successful_units, failed_units, aborted_early=True)
        raise ExtractionError(f"Pre-execution login failed: {err}") from err

    try:
        for t_date, src in units_to_run:
            started_at = datetime.now(timezone.utc)
            date_str = t_date.isoformat()

            method_name, raw_table, source_label = SOURCE_CONFIG[src]
            table_id = f"{config.gcp_project}.{config.raw_dataset}.{raw_table}"
            fetch_fn = getattr(client, method_name)

            row_count: Optional[int] = None
            status: str = "failed"
            error_type: Optional[str] = None
            error_message: Optional[str] = None
            fatal_exception: Optional[Exception] = None

            try:
                rows = fetch_fn(t_date)
                rows_fetched = len(rows)

                rows_loaded = load_partition(
                    client=bq_client,
                    table_id=table_id,
                    rows=rows,
                    extraction_date=t_date,
                    source=source_label,
                    batch_id=batch_id,
                )

                row_count = rows_loaded
                status = "success"
                successful_units.append((t_date, src))

            except (LoginError, RawTableMissingError) as fatal_err:
                status = "failed"
                error_type = type(fatal_err).__name__
                error_message = str(fatal_err)
                fatal_exception = fatal_err
                failed_units.append((t_date, src, str(fatal_err)))

            except ReadymodeFormatError as err:
                status = "failed"
                error_type = "ReadymodeFormatError"
                error_message = str(err)
                failed_units.append((t_date, src, str(err)))

            except Exception as err:
                status = "failed"
                error_type = type(err).__name__
                error_message = str(err)
                failed_units.append((t_date, src, str(err)))

            finally:
                finished_at = datetime.now(timezone.utc)
                duration = (finished_at - started_at).total_seconds()

                log_extraction_run(
                    client=bq_client,
                    project_id=config.gcp_project,
                    dataset_id=config.raw_dataset,
                    batch_id=batch_id,
                    source=source_label,
                    extraction_date=t_date,
                    row_count=row_count,
                    status=status,
                    started_at=started_at,
                    finished_at=finished_at,
                    error_type=error_type,
                    error_message=error_message,
                )

                if status == "success":
                    logger.info(
                        f"source={src}, date={date_str}, rows_fetched={rows_fetched}, "
                        f"rows_loaded={rows_loaded}, batch_id={batch_id}, duration_s={duration:.2f}"
                    )
                else:
                    logger.error(
                        f"Failed processing source={src} for date={date_str} after {duration:.2f}s "
                        f"[{error_type}]: {error_message}"
                    )

            if fatal_exception:
                raise fatal_exception

    except (LoginError, RawTableMissingError) as err:
        logger.critical(f"Critical pipeline error: {err}. Aborting execution.")
        print_summary(batch_id, units_to_run, successful_units, failed_units, aborted_early=True)
        raise ExtractionError(f"Fatal run error: {err}") from err

    print_summary(batch_id, units_to_run, successful_units, failed_units, aborted_early=False)

    if failed_units:
        raise ExtractionError(f"Extraction completed with {len(failed_units)} failed unit(s).")

    print("All tasks completed successfully.\n")


def print_summary(
    batch_id: str,
    units_to_run: List[Tuple[date, str]],
    successful_units: List[Tuple[date, str]],
    failed_units: List[Tuple[date, str, str]],
    aborted_early: bool = False,
) -> None:
    """Prints execution summary and retry commands to stdout."""
    print("\n" + "=" * 60)
    print("EXTRACTION SUMMARY" + (" (ABORTED EARLY)" if aborted_early else ""))
    print("=" * 60)
    print(f"Batch ID:              {batch_id}")
    print(f"Total Units Scheduled: {len(units_to_run)}")
    print(f"Successful:            {len(successful_units)}")
    print(f"Failed:                {len(failed_units)}")

    attempted_count = len(successful_units) + len(failed_units)
    unattempted_count = len(units_to_run) - attempted_count
    if unattempted_count > 0:
        print(f"Unattempted (Aborted): {unattempted_count}")
    if aborted_early and units_to_run:
        print(f"Aborted after unit:    {attempted_count} of {len(units_to_run)}")

    if failed_units:
        print("\nFailed Units Breakdown:")
        for f_date, f_src, reason in failed_units:
            print(f"  - Date: {f_date.isoformat()} | Source: {f_src} | Reason: {reason}")

        print("\nTo re-run ONLY failed units, execute the following commands:")
        for f_date, f_src, _ in failed_units:
            print(
                f"  python -m extractor.run_extract --date {f_date.isoformat()} --source {f_src}"
            )

    print("=" * 60 + "\n")


def parse_args():
    parser = argparse.ArgumentParser(
        description="ReadyMode Data Extraction and BigQuery Load Script"
    )

    parser.add_argument(
        "--date",
        type=date.fromisoformat,
        default=date.today() - timedelta(days=1),
        help="Date to extract (YYYY-MM-DD). Defaults to yesterday.",
    )
    parser.add_argument(
        "--source",
        choices=AVAILABLE_SOURCES,
        help="Extract a single specific source. If omitted, extracts all sources.",
    )
    parser.add_argument(
        "--backfill-from",
        type=date.fromisoformat,
        help="Start date for backfill range (YYYY-MM-DD). Must be paired with --backfill-to.",
    )
    parser.add_argument(
        "--backfill-to",
        type=date.fromisoformat,
        help="End date for backfill range (YYYY-MM-DD). Must be paired with --backfill-from.",
    )

    args = parser.parse_args()

    if bool(args.backfill_from) != bool(args.backfill_to):
        parser.error("Both --backfill-from and --backfill-to must be specified together.")

    if args.backfill_from and args.backfill_from > args.backfill_to:
        parser.error("--backfill-from cannot be after --backfill-to.")

    return args


def main() -> int:
    args = parse_args()

    start_date = args.backfill_from if args.backfill_from else args.date
    backfill_to = args.backfill_to if args.backfill_from else None

    try:
        run_extraction_for_date(
            target_date=start_date,
            source=args.source,
            backfill_to=backfill_to,
        )
        return 0
    except (ExtractionError, ConfigError) as err:
        logger.error(f"Execution failed: {err}")
        return 1


if __name__ == "__main__":
    sys.exit(main())