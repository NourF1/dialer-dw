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

## Architecture

```
  ReadyMode (noaccentcallers4)
       │  session auth · tenacity retries · schema-drift detection
       ▼
  scripts/extractor/  ──►  dialer_dw_raw
                             raw_call_log        84,266 rows / 40 days
                             raw_dialer_report      462 rows / 40 days
                             extraction_runs         audit trail
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
       fct_campaign_daily              629 rows

  Orchestrated by a single Airflow DAG (Phase 4, not yet built):
  extract → dbt_seed → dbt_run → dbt_test → source_freshness
```

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
| Threads | 2 | 4 |

```bash
dbt run                  # dev
dbt run --target prod    # prod — used only by the DAG
```

## Data model

| Model | Layer | Grain | Description |
|---|---|---|---|
| `stg_readymode__call_log` | staging | one call-log event (`call_log_id`) | JSON parsed, typed. 10 fields. |
| `stg_readymode__dialer_report` | staging | one (date, queue, playlist) | 15 columns, rates as fractions |
| `campaign_aliases` | seed | one campaign alias | variant → canonical; `is_test` flag. Exceptions only |
| `int_dialer_report_by_campaign` | intermediate | one (date, queue, campaign) | aliases applied, counts summed, **rates recomputed from sums** |
| `fct_campaign_daily` | marts | one (date, campaign) | dialer metrics FULL OUTER JOIN disposition pivot |

Reconciliation held by tests: `sum(total_calls) = 2,164,276` and
`sum(disposition_count) = 84,266` match their sources exactly.

### Known exclusions

`is_test` campaigns (`Campaign 1`, `Local Lead Outscraper Test`, `Genesis`) are
flagged, not deleted. Campaigns appearing in dispositions but never in the dialer
report account for ~4.8% of dispositions; `tests/warn_unmapped_campaigns.sql`
warns (never fails) when one exceeds 10 dispositions in the recent window, and
suggests the closest existing campaign via `EDIT_DISTANCE` above 0.45.

## Repository layout

```
airflow/dags/           Airflow DAG definitions (empty — Phase 4)
scripts/extractor/      ReadyMode client, BigQuery loader, audit, CLI
  tests/                pytest + clear_partitions.py maintenance tool
dbt_project/
  models/staging/       1:1 with sources — parse, cast, rename. No joins.
  models/intermediate/  aliasing + aggregation (view)
  models/marts/         fct_campaign_daily (dim_* pending)
  seeds/                campaign_aliases.csv — edit in a TEXT editor, not Numbers
  tests/                singular tests (warn_unmapped_campaigns.sql)
config/raw_tables.sql   raw-layer DDL — run once, by hand, in the console
docs/architecture/      diagrams (empty — Phase 5)
keys/                   GCP service-account key (gitignored)
```

## Project status

- [x] **Phase 0 — Setup & stack.** Docker, Airflow 2.8.1, Postgres, dbt-bigquery. Pinned deps.
- [x] **Phase 1 — HTTP extractor.** Session auth, both sources, idempotent partition loads,
      audit table, pytest, `clear_partitions.py`, tenacity retries.
- [x] **Phase 2 — dbt modeling.** Staging → intermediate → `fct_campaign_daily`.
      Dimensions deferred to Phase 4.
- [x] **Phase 3 — Data quality gates.** 51 tests, grain enforced at three layers,
      source freshness, warn-only unmapped-campaign check.
- [ ] **Phase 4 — Orchestration.** ← next. DAG, `dim_campaign`, `dim_agent`.
- [ ] **Phase 5 — Docs & CI.** Architecture diagram, GitHub Actions.

### Where to pick up

`airflow/dags/` is empty. Phase 4 builds:

1. DAG on `0 6 * * 2-6` — Tue–Sat, clear of the 1 PM ET single-session collision
   with the ListKit pipeline (both authenticate as the same ReadyMode user).
2. `PythonOperator` → `run_extract` with `{{ ds }}`; `BashOperator` → dbt steps
   (`DBT_PROFILES_DIR`/`DBT_PROJECT_DIR` are set in the image, so no flags needed).
3. `retries=2, retry_delay=5min` — a fresh task gets a fresh auth token, which
   absorbs the intermittent `oauth2.googleapis.com` timeouts (network-side, not code).
4. `dim_campaign`, `dim_agent`, and the `relationships` tests they enable.

### Operational notes

- **BigQuery sandbox**: no DML, 60-day partition expiry. `clear_partitions.py`
  (dry-run by default) is the only repair path.
- **Idempotency** is `table$YYYYMMDD` + `WRITE_TRUNCATE`. Dropping the partition
  decorator wipes the whole table *and the job still succeeds*.
- `raw_tables.sql` provisions; the pipeline only writes (`CREATE_NEVER`).
- Run long dbt commands with `--threads 1` if the network is flaky; intermittent
  `RemoteDisconnected` comes from Google's auth endpoints, not from queries.
  ERROR ≠ FAIL: a real data failure names the same test every run.
