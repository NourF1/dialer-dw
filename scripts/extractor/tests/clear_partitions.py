"""Clear BigQuery partitions in the raw layer.

DESTRUCTIVE. BigQuery sandbox has no DML, so a WRITE_TRUNCATE load with zero
rows is the only way to empty a partition -- and there is no undo.

Reuses extractor.bq_loader.load_partition rather than writing a second
truncation path: if the loader ever breaks, this breaks with it instead of
quietly diverging.

Dry-run by default. --confirm actually writes.

    python -m extractor.tests.clear_partitions --table raw_call_log --list
    python -m extractor.tests.clear_partitions --table raw_call_log --dates 2099-01-01
    python -m extractor.tests.clear_partitions --table raw_call_log --dates 2099-01-01 --confirm
"""
from __future__ import annotations

import argparse
import os
import sys
from datetime import date, datetime, timezone

from google.cloud import bigquery

from extractor.bq_loader import load_partition

PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "dialer-dw-prod")
DATASET_ID = os.environ.get("BQ_RAW_DATASET", "dialer_dw_raw")

# load_partition requires a _source value even when writing zero rows.
SOURCE_BY_TABLE = {
    "raw_call_log": "readymode.call_log",
    "raw_dialer_report": "readymode.dialer_report",
}


def list_partitions(client: bigquery.Client, table_name: str) -> dict[str, int]:
    """Return {partition_id: total_rows} for one table.

    Reads INFORMATION_SCHEMA.PARTITIONS -- metadata only, so it scans zero
    bytes and costs nothing. partition_id is 'YYYYMMDD' for daily partitions.
    """
    query = f"""
        SELECT partition_id, total_rows
        FROM `{PROJECT_ID}.{DATASET_ID}.INFORMATION_SCHEMA.PARTITIONS`
        WHERE table_name = @table_name
        ORDER BY partition_id
    """
    job = client.query(
        query,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("table_name", "STRING", table_name)
            ]
        ),
    )
    return {r.partition_id: r.total_rows for r in job.result()}


def parse_dates(values: list[str]) -> list[date]:
    """Parse YYYY-MM-DD strings. Fails on the first bad one.

    A typo'd date on a destructive tool must never be silently skipped -- the
    user would see 'done' and assume the partition they meant was cleared.
    """
    out = []
    for v in values:
        try:
            out.append(datetime.strptime(v, "%Y-%m-%d").date())
        except ValueError:
            raise SystemExit(f"error: '{v}' is not a valid YYYY-MM-DD date")
    return out


def is_test_date(d: date) -> bool:
    """True if d is in the future.

    Test fixtures live in 2099; real extractions never target the future. That
    asymmetry is what lets this script recognise a dangerous request.
    """
    return d > date.today()


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--table", required=True, choices=sorted(SOURCE_BY_TABLE))
    p.add_argument("--dates", nargs="+", metavar="YYYY-MM-DD",
                   help="partitions to clear")
    p.add_argument("--list", action="store_true",
                   help="show existing partitions and exit")
    p.add_argument("--confirm", action="store_true",
                   help="actually clear (default is dry run)")
    p.add_argument("--force", action="store_true",
                   help="required to clear a non-future date")
    args = p.parse_args()

    client = bigquery.Client()
    table_id = f"{PROJECT_ID}.{DATASET_ID}.{args.table}"
    existing = list_partitions(client, args.table)

    def show_existing() -> None:
        print(f"\nExisting partitions in {args.table}:")
        if not existing:
            print("  (none)")
        for pid, rows in sorted(existing.items()):
            flag = "  <-- unexpected" if pid.startswith("__") else ""
            print(f"  {pid}  {rows:>10,} rows{flag}")

    if args.list:
        show_existing()
        return 0

    if not args.dates:
        p.error("--dates is required unless --list is given")

    targets = parse_dates(args.dates)

    # --- plan ------------------------------------------------------------
    print(f"Table: {table_id}")
    show_existing()
    print("\nRequested:")

    plan: list[tuple[date, str, int]] = []
    blocked = False
    for d in targets:
        pid = d.strftime("%Y%m%d")
        rows = existing.get(pid)
        if rows is None:
            print(f"  {d}  partition does not exist -- nothing to clear")
            continue
        if not is_test_date(d) and not args.force:
            print(f"  {d}  {rows:,} rows  ** REFUSED **")
            print(f"        {d} is not a future date, so this looks like real "
                  f"data.\n        Re-run with --force if you are sure.")
            blocked = True
            continue
        note = "  (non-future, --force given)" if not is_test_date(d) else ""
        print(f"  {d}  would clear {rows:,} rows{note}")
        plan.append((d, pid, rows))

    if blocked:
        return 2
    if not plan:
        print("\nNothing to do.")
        return 0

    if not args.confirm:
        print("\nDRY RUN -- no changes made. Re-run with --confirm to clear.")
        return 0

    # --- execute ---------------------------------------------------------
    batch_id = f"manual-cleanup-{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}"
    source = SOURCE_BY_TABLE[args.table]
    print(f"\nClearing (batch_id={batch_id}):")
    for d, pid, before in plan:
        load_partition(client, table_id, [], d, source, batch_id)
        after = list_partitions(client, args.table).get(pid, 0)
        print(f"  {d}  {before:,} rows -> {after:,} rows")
        if after != 0:
            print(f"  WARNING: {pid} still reports {after} rows", file=sys.stderr)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
