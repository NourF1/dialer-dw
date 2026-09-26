# dialer-dw

An end-to-end analytics warehouse for outbound dialer operations: ReadyMode call
data extracted over HTTP, landed in BigQuery, modeled with dbt, and orchestrated
with Airflow.

## Stack

| Layer | Tool |
|---|---|
| Orchestration | Apache Airflow 2.8.1 (LocalExecutor, Docker Compose) |
| Ingestion | Python — session-auth HTTP client (`requests` + `tenacity`) |
| Warehouse | Google BigQuery |
| Transformation | dbt-bigquery 1.12 (staging → intermediate → marts) |
| Data quality | dbt schema tests + source freshness on `_loaded_at` |
| Resilience | `dbt_retry` wrapper — retries transient failures, never data failures |

## Architecture

```
  ReadyMode (noaccentcallers4)
       │  session auth · tenacity retries · schema-drift detection
       ▼
  scripts/extractor/  ──►  dialer_dw_raw
                             raw_call_log       106,412 rows / 40 days
                             raw_dialer_report      591 rows / 40 days
                             extraction_runs        219 rows  audit trail
                           payload = verbatim JSON, nothing cast
       │
       ▼  dbt source + freshness (2h warn / 6h error, post-load)
  staging (view, 1:1 with source)
       stg_readymode__call_log · stg_readymode__dialer_report
       │
       ▼
  intermediate (view)
       int_dialer_report_by_campaign   alias → aggregate to (date, campaign)
       │
       ▼
  marts (table)
       fct_campaign_daily              854 rows
       dim_campaign                    134 rows   ← FK target for the fact
       dim_agent                       100 rows   ← FK target for staging

  Orchestrated by one Airflow DAG (dialer_dw_daily):

       extract ──┬──► freshness_gate ──► dbt_source_freshness
                 └──► dbt_seed ──► dbt_run ──► dbt_test
```

Data window: 2026-08-03 → 2026-09-23 — 40 weekday partitions, no gaps.
Weekend partitions are absent by design.

## Quickstart

```bash
cp .env.template .env
# fill in ReadyMode credentials and generate AIRFLOW_FERNET_KEY (see comment in file)
# place your GCP service-account key at keys/gcp_key.json

docker compose build
docker compose up airflow-init      # runs DB migrations, creates admin user
docker compose up -d

open http://localhost:8081          # login: airflow / airflow
```

Verify the dbt ↔ BigQuery connection:

```bash
docker compose run --rm --no-deps scheduler bash -c "dbt debug"
```

> The Airflow image's entrypoint prepends `airflow` to its arguments, so any
> non-Airflow command must be wrapped in `bash -c "..."`.

## Environments

`profiles.yml` defines two targets against the same BigQuery project:

| | `dev` (default) | `prod` |
|---|---|---|
| Datasets | `dialer_dw_dev_*` | `dialer_dw_*` |
| Run by | you, interactively | the Airflow DAG |
| Threads | 2 | **1** — see [Connection flakiness](#connection-flakiness) |

```bash
dbt run                  # dev
dbt run --target prod    # prod — used only by the DAG
```

## Orchestration

`airflow/dags/dialer_dw_daily.py`.

**`extract` is self-healing.** Rather than extracting only `{{ ds }}`, it asks
`extraction_runs` which weekday dates in the last 45 days lack a successful row
for *both* sources, and fills them — up to 10 per run, oldest first, in a single
ReadyMode session. On a normal day that set is just `[ds]`, so the healing path
and the ordinary path are the same code. On a weekend it is empty and the task
no-ops.

Why `extraction_runs` and not the raw tables: a weekend legitimately returns
zero rows and writes no partition, so a raw-table check would see it as missing
every run, forever. The audit table records a `success` with `row_count = 0`.

The upper bound is `ds`, never `date.today()` — that keeps `extract` a function
of its logical date, so re-running an old DagRun cannot reach forward into dates
it does not own.

| Setting | Value | Why |
|---|---|---|
| `schedule` | `0 6 * * *` **ET** | Every `{{ ds }}` is genuinely yesterday. 06:00 ET is clear of the 1 PM ET ReadyMode collision with the ListKit pipeline by seven hours, and is a DST-safe hour (the spring gap is 02:00-03:00, the fall overlap 01:00-02:00). |
| `start_date` | static literal, `tz="America/New_York"` | The cron is interpreted in the `start_date`'s timezone, so the DAG follows ET across DST. A hardcoded UTC cron would drift an hour twice a year. A moving `start_date` (`days_ago`, `now()`) makes interval arithmetic non-deterministic. |
| `catchup` | `False` | With `True`, unpausing queues ~47 runs, each calling `login()` — and each login force-evicts the previous session. |
| `max_active_tasks` | `1` | Serializes tasks *within* a run. Without it Airflow ran `dbt_seed` and `dbt_source_freshness` concurrently, and two simultaneous dbt processes contending for auth turned a 7-row seed into an 18-minute task. See [Connection flakiness](#connection-flakiness). |
| `max_active_runs` | `1` | ReadyMode allows one session per user, so the DAG must serialize itself. `catchup=False` alone doesn't stop a manual trigger overlapping. |
| `dagrun_timeout` | 90 min | Otherwise one hung run holds the only slot forever and later days are silently skipped. Raised from 45 after two real runs (46.0 and 62.1 min) tripped it. |
| `retries` / `retry_delay` | 2 / 5 min | Fresh task → fresh auth token. Outer net for a dead *process*. |
| `execution_timeout` | 30 min | Bounds a single task so a hang triggers a retry instead of consuming the whole run. |

**Daily, not `2-6`.** An earlier design used `0 6 * * 2-6`. Because Airflow sets
`logical_date` to the *previous* fire time, the Tuesday run's `{{ ds }}` resolved
to Saturday and **Monday was never extracted by any run**. A daily schedule makes
`ds` unambiguous; the weekend runs load zero rows, which costs nothing.

`freshness_gate` is a `ShortCircuitOperator` returning
`date.fromisoformat(ds).weekday() < 5`. Weekends have no dialer activity, so
`max(_loaded_at)` would be 24–48h old and trip `error_after: 6 hour` every Sunday
and Monday. Gating keeps the tight threshold meaningful on weekdays instead of
loosening it to 78h.

> The gate sits on its own branch descending from `extract`, **not** upstream of
> the dbt chain. `ShortCircuitOperator` defaults to
> `ignore_downstream_trigger_rules=True`, so putting the dbt tasks behind it
> would skip the entire pipeline every weekend.

## Data model

| Model | Layer | Grain | Description |
|---|---|---|---|
| `stg_readymode__call_log` | staging | one call-log event (`call_log_id`) | JSON parsed, typed. 10 fields. |
| `stg_readymode__dialer_report` | staging | one (date, queue, playlist) | 15 columns, rates as fractions |
| `campaign_aliases` | seed | one campaign alias | variant → canonical; `is_test` flag. Exceptions only |
| `int_dialer_report_by_campaign` | intermediate | one (date, campaign) | aliases applied, counts summed, **rates recomputed from sums** |
| `fct_campaign_daily` | marts | one (date, campaign) | dialer metrics FULL OUTER JOIN disposition pivot |
| `dim_campaign` | marts | one campaign | `is_test`, source-presence flags, first/last seen. Built from staging + seed, **never** from the fact |
| `dim_agent` | marts | one `agent_login` | Type-1: current `agent_name`, first/last seen |

**The reconciliation invariant:** `sum(disposition_count)` in
`fct_campaign_daily` must equal the row count of `stg_readymode__call_log`, and
the fact's summed dialer counts must equal their sums in
`stg_readymode__dialer_report` — per date, not just in total.

Fan-out is zero at both layers: `int_dialer_report_by_campaign` and
`fct_campaign_daily` each have a row count equal to their own distinct
`(date, campaign)` count.

Enforced by `tests/assert_fct_reconciles_to_staging.sql` (severity `error`),
which compares fact against source **per date** and returns one row per failing
`(date, metric)`. Verified 2026-09-23: `raw_call_log` 104,599 rows,
`fct disposition_count` 104,599, 39 days on both sides.

> It was written against a live breach. On 2026-09-23 the fact was a full day
> stale — 102,314 vs 104,599, a 2,285-row gap that was exactly 09-22's
> dispositions, because the 09-22 DAG run failed before `dbt_run`. **All 64
> other tests passed in that state.** `not_null`, `unique`, grain and
> `relationships` tests validate rows that *are* present; none can see a day
> that should exist and doesn't.
>
> Two design points it depends on: the join between fact dates and source dates
> is a `full outer join` (an inner join has no row to compare for a missing
> date, so the test would pass while blind to the one failure it exists to
> catch), and the comparison is per-date rather than a grand total (which would
> let a double-count on one day cancel a gap on another).

## Data quality

- **65 dbt tests**, including two `relationships` tests enforcing referential
  integrity (`fct_campaign_daily.campaign_name` → `dim_campaign`, and
  `stg_readymode__call_log.agent_login` → `dim_agent`) and one singular
  reconciliation test asserting fact-to-source equality per date.
- **14 pytest**: 7 in `scripts/dbt_retry/tests/` (retry classifier), 6 in
  `scripts/extractor/tests/test_gap_finder.py` (gap detection), 1 partition
  idempotency test against the live sandbox.
- `scripts/extractor/tests/test_bq_loader.py` — partition idempotency against the
  live sandbox.

### Known exclusions

`is_test` campaigns (`Campaign 1`, `Local Lead Outscraper Test`, `Genesis`) are
flagged, not deleted. `Genesis` was confirmed a test campaign on 2026-09-16.
Campaigns appearing in dispositions but never in the dialer report account for
~4.8% of dispositions; `tests/warn_unmapped_campaigns.sql` warns (never fails)
when one exceeds 10 dispositions in the recent window, and suggests the closest
existing campaign via `EDIT_DISTANCE` above 0.45. It currently returns 6 rows —
expected.

### Known gaps

**None currently**, and gaps now close themselves. All 40 weekdays from
2026-08-03 to 2026-09-23 are present; weekend partitions are absent by design.

`extract` fills any missing weekday within a 45-day lookback on its next run, so
a day the pipeline sleeps through is recovered automatically rather than needing
the manual backfill below. That command remains the tool for anything outside
the lookback, or for deliberately re-extracting a day.

Because `catchup=False`, the DAG will never fill a day it missed, so gaps are
closed by hand with one login for the whole range:

```bash
docker exec dialer-dw-scheduler-1 bash -c \
  "python -m extractor.run_extract --backfill-from YYYY-MM-DD --backfill-to YYYY-MM-DD"
```

To find them, diff `generate_date_array` against `distinct _extraction_date`,
excluding weekends. Four weekdays (09-10, 09-11, 09-14, 09-17) were closed this
way on 2026-09-21.

Note Aug 1–2 have already aged out under the sandbox's 60-day partition
expiry. The window shrinks from the back regardless of what you load.

## Connection flakiness

Long dbt commands intermittently fail with `RemoteDisconnected` or
`SSLEOFError` — **always against Google's auth endpoints**
(`oauth2.googleapis.com/token`, `iamcredentials.../allowedLocations`), never
against BigQuery itself. Measured evidence: `fct_campaign_daily` materialises
28 MiB in 3.71s while a `max(_loaded_at)` freshness query on the same connection
took 40.65s. Execution time doesn't vary like that; connection setup does.

Two mitigations, at different layers:

0. **`max_active_tasks: 1`.** The DAG serializes its own tasks. Measured
   2026-09-25 → 09-26, same work, only this changed:

   ```
   dbt_seed  1076s  (concurrent with dbt_source_freshness)
   dbt_seed    23s  (serialized)              47x
   run       117min  →  4min05
   ```

   `threads: 1` removed concurrency *inside* dbt; this removes it *between*
   tasks. Both were needed. Note the fix is a DAG-level setting and **not** a
   dependency edge — chaining `dbt_seed` behind `dbt_source_freshness` would put
   the dbt chain downstream of `freshness_gate`, and the weekend short-circuit
   would then skip the entire pipeline.

1. **`prod` runs `threads: 1`.** At 4 every `dbt test` failed. The tell that it
   isn't a data problem: the failing test *name moves between attempts*, and
   errors are `Database Error`, never `Got N results`.
2. **`scripts/dbt_retry/`** wraps `dbt run` and `dbt test` in the DAG. On a
   non-zero exit it classifies `target/run_results.json` and calls `dbt retry`
   — which re-executes only the failed nodes — but **only** for transient
   errors.

```
status=fail                              → never retry (data is wrong)
status=error + connection signature      → retry
status=error + anything else             → never retry (compilation, permissions)
status=warn                              → not a failure
```

`dbt_seed` and `dbt_source_freshness` stay on plain `dbt` as a control.

> `run_results.json` is one file per project, so this only works while the dbt
> tasks run **sequentially**. Don't parallelise the dbt chain without revisiting
> it. Note also that after a retry the final `Done.` line reports only the
> retried subset (`PASS=3`, not `PASS=50`) — judge success by exit code, not by
> grepping a count.

## Repository layout

```
airflow/dags/           dialer_dw_daily.py — the one DAG
scripts/extractor/      ReadyMode client, BigQuery loader, audit, gap finder, CLI
  tests/                pytest + clear_partitions.py maintenance tool
scripts/dbt_retry/      transient-failure retry wrapper
  tests/                pytest + run_results.json fixtures
dbt_project/
  models/staging/       1:1 with sources — parse, cast, rename. No joins.
  models/intermediate/  aliasing + aggregation (view)
  models/marts/         fct_campaign_daily (dim_* pending)
  seeds/                campaign_aliases.csv — edit in a TEXT editor, not Numbers
  tests/                singular tests (warn_unmapped_campaigns.sql)
config/raw_tables.sql   raw-layer DDL — run once, by hand, in the console
docs/architecture/      lineage, DAG graph and resilience-layer diagrams
keys/                   GCP service-account key (gitignored)
```

## Project status

- [x] **Phase 0 — Setup & stack.** Docker, Airflow 2.8.1, Postgres, dbt-bigquery. Pinned deps.
- [x] **Phase 1 — HTTP extractor.** Session auth, both sources, idempotent partition loads,
      audit table, pytest, `clear_partitions.py`, tenacity retries.
- [x] **Phase 2 — dbt modeling.** Staging → intermediate → `fct_campaign_daily`.
- [x] **Phase 3 — Data quality gates.** 51 tests, grain enforced at three layers,
      source freshness, warn-only unmapped-campaign check.
- [x] **Phase 4 — Orchestration.** Airflow DAG, `dbt_retry` wrapper,
      `dim_campaign` / `dim_agent`, and enforced referential integrity.
- [ ] **Phase 5 — Docs & CI.** Reconciliation test, architecture diagram, GitHub Actions.

### Where to pick up

Phase 4 is complete and verified; the DAG is still **paused**.

The one remaining step is to **unpause** it, after clearing any non-terminal task
instances left from testing (see the operational notes below — run-level "Mark
Failed" does not clear `NULL`-state tasks).

Then Phase 5:

1. ~~Reconciliation test~~ — done, `tests/assert_fct_reconciles_to_staging.sql`.
2. **Architecture diagram** in `docs/architecture/`.
3. **CI** — GitHub Actions running `pytest` and `dbt build` against `dev`.

Deferred, worth knowing:

- The seed alias `coalesce` is duplicated in three models
  (`int_dialer_report_by_campaign`, `fct_campaign_daily`, `dim_campaign`). A
  macro or a shared aliased-campaign model would remove the drift risk.
- `dim_agent` has no fact referencing it yet; `fct_agent_daily` is the obvious
  next fact, and its dimension and integrity test already exist.
- `dim_agent` is Type-1. One agent has had a name change. Type-2 can be derived
  retroactively from `raw_call_log` if point-in-time names are ever needed — no
  re-extraction required, and no `dbt snapshot`.

### Operational notes

- **BigQuery sandbox**: no DML, 60-day partition expiry. `clear_partitions.py`
  (dry-run by default) is the only repair path.
- **Idempotency** is `table$YYYYMMDD` + `WRITE_TRUNCATE`. Dropping the partition
  decorator wipes the whole table *and the job still succeeds*. Every retry layer
  in this project depends on that being true.
- `raw_tables.sql` provisions; the pipeline only writes (`CREATE_NEVER`).
- **Never run `airflow tasks test` while the DAG is unpaused.** The scheduler
  resets the same `task_instance` row; the local job dies with "state externally
  set to None", a SIGTERM, and a `task_fail` FK violation — and dbt's summary
  line can be lost from the log, so a run that *passed* looks like a mid-suite
  kill. Pause first.
- **`airflow dags test` leaves persistent state.** It creates a real DagRun, so an
  interrupted invocation can wedge a task in `scheduled` forever — the loop then
  prints "no tasks to run" indefinitely. Mark the run failed or delete it;
  re-invoking won't recover it.
- **Run-level "Mark Failed" does not clear `NULL`-state task instances**, only
  `running`/`queued` ones. Mark those tasks individually, or delete the DagRun.
- **Airflow is hosted on a laptop, deliberately.** Docker Desktop's VM suspends
  when the Mac sleeps, so the scheduler does not fire and `catchup=False` means
  a slept-through day is never created as a DagRun. Historically this is how
  four weekdays went missing. Mitigated two ways: the self-healing `extract`
  recovers gaps on the next run, and `sudo pmset repeat wakeorpoweron MTWRF
  05:55:00` wakes the machine before the schedule. The production answer is a
  always-on host or Cloud Composer; local hosting is a scoping choice for this
  project, not an oversight.
- **`ps` is not installed** in the Airflow image. To check for live processes,
  read `/proc/[0-9]*/cmdline` — an empty `ps` result is a broken probe, not an
  idle container.
