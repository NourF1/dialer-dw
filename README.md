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
  ReadyMode  ──HTTP──►  Python extractor  ──►  BigQuery raw
  (session auth)        (retry + paginate)     (append-only)
                                                    │
                                                    ▼
                                    dbt:  staging (view)
                                            │
                                            ▼
                                       intermediate (ephemeral)
                                            │
                                            ▼
                                          marts (table)

  All steps orchestrated by a single Airflow DAG:
  extract_readymode → load_to_bq_raw → dbt_run → dbt_test
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
| Threads | 4 | 8 |

```bash
dbt run                  # dev
dbt run --target prod    # prod — used only by the DAG
```

## Data model

| Model | Layer | Grain | Description |
|---|---|---|---|
| _TBD_ | staging | | |
| _TBD_ | intermediate | | |
| _TBD_ | marts | | |

## Repository layout

```
airflow/dags/           Airflow DAG definitions
scripts/extractor/      ReadyMode HTTP extraction logic
dbt_project/
  models/staging/       1:1 with sources — renames, casts, no joins
  models/intermediate/  dedupe + joins (ephemeral, no BigQuery objects)
  models/marts/         dim_/fct_ star schema
config/                 pipeline configuration
docs/architecture/      diagrams
keys/                   GCP service-account key (gitignored)
```

## Project status

- [x] Phase 0 — Setup & stack
- [ ] Phase 1 — HTTP extractor
- [ ] Phase 2 — dbt modeling
- [ ] Phase 3 — Data quality gates
- [ ] Phase 4 — Orchestration
- [ ] Phase 5 — Docs & CI
