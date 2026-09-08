import argparse
import logging
import sys
import uuid
from datetime import date, datetime, timedelta
from typing import List, Tuple

from google.cloud import bigquery

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

        for target_date, source in units_to_run:
            start_time = datetime.now()
            date_str = target_date.isoformat()

            method_name, raw_table, source_label = SOURCE_CONFIG[source]
            table_id = f"{config.gcp_project}.{config.raw_dataset}.{raw_table}"
            fetch_fn = getattr(client, method_name)

            try:
                # Extract
                rows = fetch_fn(target_date)
                rows_fetched = len(rows)

                # Load
                rows_loaded = load_partition(
                    client=bq_client,
                    table_id=table_id,
                    rows=rows,
                    extraction_date=target_date,
                    source=source_label,
                    batch_id=batch_id,
                )

                duration = (datetime.now() - start_time).total_seconds()

                logger.info(
                    f"source={source}, date={date_str}, rows_fetched={rows_fetched}, "
                    f"rows_loaded={rows_loaded}, batch_id={batch_id}, duration_s={duration:.2f}"
                )
                successful_units.append((target_date, source))

            except (LoginError, RawTableMissingError):
                # Re-raise fatal errors so they bypass the generic Exception block
                # and bubble up to abort the entire extraction run.
                raise

            except ReadymodeFormatError as err:
                duration = (datetime.now() - start_time).total_seconds()
                logger.error(
                    f"Format error processing source={source} for date={date_str} after {duration:.2f}s: {err}"
                )
                failed_units.append((target_date, source, str(err)))

            except Exception as err:
                duration = (datetime.now() - start_time).total_seconds()
                logger.error(
                    f"Unexpected error processing source={source} for date={date_str} after {duration:.2f}s: {err}"
                )
                failed_units.append((target_date, source, str(err)))

    except LoginError as err:
        logger.critical(
            f"Login failed: {err}. Aborting execution immediately."
        )
        return 1
    except RawTableMissingError as err:
        logger.critical(
            f"Target BigQuery raw table missing: {err}. Aborting execution immediately."
        )
        return 1

    # Final Execution Summary
    print("\n" + "=" * 60)
    print("EXTRACTION SUMMARY")
    print("=" * 60)
    print(f"Batch ID: {batch_id}")
    print(f"Total Units Attempted: {len(units_to_run)}")
    print(f"Successful:            {len(successful_units)}")
    print(f"Failed:                {len(failed_units)}")

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
        return 1

    print("All tasks completed successfully.")
    print("=" * 60 + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(execute_extraction())