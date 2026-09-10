import argparse
import logging
import sys
import uuid
from datetime import date, datetime, timedelta, timezone
from typing import List, Tuple

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

# Source mapping dictionary: source -> (client_method_attr, raw_table_name, source_label)
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


def daterange(start_date: date, end_date: date):
    """Generates dates sequentially inclusive of start_date and end_date."""
    curr = start_date
    while curr <= end_date:
        yield curr
        curr += timedelta(days=1)


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
        parser.error(
            "Both --backfill-from and --backfill-to must be specified together."
        )

    if args.backfill_from and args.backfill_from > args.backfill_to:
        parser.error("--backfill-from cannot be after --backfill-to.")

    return args


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
        # Name where the run stopped so the arithmetic above is not confusing:
        # "80 scheduled / 12 succeeded / 1 failed" leaves 67 rows unexplained.
        print(f"Aborted after unit:    {attempted_count} of {len(units_to_run)}")

    if failed_units:
        print("\nFailed Units Breakdown:")
        for f_date, f_src, reason in failed_units:
            print(
                f"  - Date: {f_date.isoformat()} | Source: {f_src} | Reason: {reason}"
            )

        print("\nTo re-run ONLY failed units, execute the following commands:")
        for f_date, f_src, _ in failed_units:
            print(
                f"  python -m extractor.run_extract --date {f_date.isoformat()} --source {f_src}"
            )

    print("=" * 60 + "\n")


def execute_extraction() -> int:
    args = parse_args()

    try:
        config = Config.from_env()
    except ConfigError as err:
        logger.error(f"Configuration Initialization Failed: {err}")
        return 1

    batch_id = str(uuid.uuid4())
    logger.info(f"Starting run. Batch ID: {batch_id}")

    # Determine date list
    if args.backfill_from and args.backfill_to:
        cutoff_date = date.today() - timedelta(days=60)
        if args.backfill_from < cutoff_date:
            logger.warning(
                f"Backfill start date ({args.backfill_from}) precedes the 60-day BigQuery partition expiry "
                f"window ({cutoff_date}). Data older than 60 days may be auto-expired."
            )
        target_dates = list(daterange(args.backfill_from, args.backfill_to))
    else:
        target_dates = [args.date]

    target_sources = [args.source] if args.source else AVAILABLE_SOURCES

    units_to_run = [(d, src) for d in target_dates for src in target_sources]

    successful_units: List[Tuple[date, str]] = []
    failed_units: List[Tuple[date, str, str]] = []

    # Instantiate clients
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
        for target_date, source in units_to_run:
            _, _, source_label = SOURCE_CONFIG[source]
            log_extraction_run(
                client=bq_client,
                project_id=config.gcp_project,
                dataset_id=config.raw_dataset,
                batch_id=batch_id,
                source=source_label,
                extraction_date=target_date,
                row_count=None,
                status="failed",
                started_at=now,
                finished_at=now,
                error_type="LoginError",
                error_message=f"Run aborted during pre-execution login: {err}",
            )
            failed_units.append((target_date, source, f"LoginError: {err}"))

        print_summary(batch_id, units_to_run, successful_units, failed_units, aborted_early=True)
        return 1

    try:
        for target_date, source in units_to_run:
            started_at = datetime.now(timezone.utc)
            date_str = target_date.isoformat()

            method_name, raw_table, source_label = SOURCE_CONFIG[source]
            table_id = f"{config.gcp_project}.{config.raw_dataset}.{raw_table}"
            fetch_fn = getattr(client, method_name)

            row_count: int | None = None
            status: str = "failed"
            error_type: str | None = None
            error_message: str | None = None
            fatal_exception: Exception | None = None

            try:
                # 1. Extract from ReadyMode
                rows = fetch_fn(target_date)
                rows_fetched = len(rows)

                # 2. Load into BigQuery raw layer
                rows_loaded = load_partition(
                    client=bq_client,
                    table_id=table_id,
                    rows=rows,
                    extraction_date=target_date,
                    source=source_label,
                    batch_id=batch_id,
                )

                # Set verified loaded count on success
                row_count = rows_loaded
                status = "success"
                successful_units.append((target_date, source))

            except (LoginError, RawTableMissingError) as fatal_err:
                # Captured rather than re-raised here purely for readability: the
                # `finally` block below would run before a bare `raise` propagated
                # anyway. Storing it makes the "audit first, then abort" order
                # explicit at the point where the abort actually happens.
                status = "failed"
                error_type = type(fatal_err).__name__
                error_message = str(fatal_err)
                fatal_exception = fatal_err
                failed_units.append((target_date, source, str(fatal_err)))

            except ReadymodeFormatError as err:
                status = "failed"
                error_type = "ReadymodeFormatError"
                error_message = str(err)
                failed_units.append((target_date, source, str(err)))

            except Exception as err:
                status = "failed"
                error_type = type(err).__name__
                error_message = str(err)
                failed_units.append((target_date, source, str(err)))

            finally:
                finished_at = datetime.now(timezone.utc)
                duration = (finished_at - started_at).total_seconds()

                # Audit log run attempt (row_count remains None on failure)
                log_extraction_run(
                    client=bq_client,
                    project_id=config.gcp_project,
                    dataset_id=config.raw_dataset,
                    batch_id=batch_id,
                    source=source_label,
                    extraction_date=target_date,
                    row_count=row_count,
                    status=status,
                    started_at=started_at,
                    finished_at=finished_at,
                    error_type=error_type,
                    error_message=error_message,
                )

                if status == "success":
                    logger.info(
                        f"source={source}, date={date_str}, rows_fetched={rows_fetched}, "
                        f"rows_loaded={rows_loaded}, batch_id={batch_id}, duration_s={duration:.2f}"
                    )
                else:
                    logger.error(
                        f"Failed processing source={source} for date={date_str} after {duration:.2f}s "
                        f"[{error_type}]: {error_message}"
                    )

            # Abort only AFTER the finally block has written the audit row.
            if fatal_exception:
                raise fatal_exception

    except LoginError as err:
        logger.critical(
            f"Login session invalidated mid-run: {err}. Aborting execution immediately."
        )
        print_summary(batch_id, units_to_run, successful_units, failed_units, aborted_early=True)
        return 1
    except RawTableMissingError as err:
        logger.critical(
            f"Target BigQuery raw table missing: {err}. Aborting execution immediately."
        )
        print_summary(batch_id, units_to_run, successful_units, failed_units, aborted_early=True)
        return 1

    # Normal Completion Summary
    print_summary(batch_id, units_to_run, successful_units, failed_units, aborted_early=False)

    if failed_units:
        return 1

    print("All tasks completed successfully.\n")
    return 0


if __name__ == "__main__":
    sys.exit(execute_extraction())