# dialer-dw architecture

Three diagrams, each answering a different question: what the model is, how it
runs, and what happens when it breaks.

## 1. Lineage — Data Flow & Granularity

```mermaid
flowchart LR
    subgraph Raw ["Raw Layer"]
        raw_call_log["raw_call_log<br/><i>Grain: one call-log event</i>"]
        raw_dialer_report["raw_dialer_report<br/><i>Grain: one dialer-report row</i>"]
    end

    subgraph Staging ["Staging Layer"]
        stg_readymode__call_log["stg_readymode__call_log<br/><i>Grain: one call-log event (typed)</i>"]
        stg_readymode__dialer_report["stg_readymode__dialer_report<br/><i>Grain: one dialer-report row (typed)</i>"]
    end

    campaign_aliases["campaign_aliases<br/><i>Campaign mapping / aliases</i>"]

    subgraph Intermediate ["Intermediate Layer"]
        int_dialer_report_by_campaign["int_dialer_report_by_campaign<br/><i>Grain: one (date, campaign)</i>"]
    end

    subgraph Marts ["Marts Layer (Star Schema)"]
        fct_campaign_daily["fct_campaign_daily<br/><i>Grain: one (date, campaign)</i>"]
        dim_campaign["dim_campaign<br/><i>Grain: one campaign</i>"]
        dim_agent["dim_agent<br/><i>Grain: one agent_login</i>"]
    end

    raw_call_log --> stg_readymode__call_log
    raw_dialer_report --> stg_readymode__dialer_report

    stg_readymode__dialer_report --> int_dialer_report_by_campaign
    campaign_aliases --> int_dialer_report_by_campaign

    int_dialer_report_by_campaign --> fct_campaign_daily
    stg_readymode__call_log --> fct_campaign_daily
    campaign_aliases --> fct_campaign_daily

    stg_readymode__dialer_report --> dim_campaign
    stg_readymode__call_log --> dim_campaign
    campaign_aliases --> dim_campaign

    stg_readymode__call_log --> dim_agent

    fct_campaign_daily -->|"campaign FK"| dim_campaign
```

The call log bypasses the intermediate layer and joins the fact directly — which
is why `fct_campaign_daily` is a FULL OUTER JOIN rather than a simple aggregate.

`dim_campaign` is built from staging and the seed, never from the fact. Were it
derived from the fact, the `relationships` test between them could not fail.

`dim_agent`'s foreign key is not drawn: its `relationships` test is on
`stg_readymode__call_log.agent_login`, and that node pair already carries a
lineage edge — the dimension is built from the same table that references it.

## 2. DAG Task Graph — Execution Flow

```mermaid
flowchart LR
    extract["extract"]
    freshness_gate["freshness_gate"]
    dbt_source_freshness["dbt_source_freshness"]
    dbt_seed["dbt_seed"]
    dbt_run["dbt_run"]
    dbt_test["dbt_test"]

    extract --> freshness_gate
    freshness_gate --> dbt_source_freshness

    extract --> dbt_seed
    dbt_seed --> dbt_run
    dbt_run --> dbt_test

    classDef note fill:#fff2cc,stroke:#d6b656,stroke-width:1px,color:#000;

    note1["<b>Architecture Note:</b><br/>freshness_gate sits on a parallel branch.<br/>ShortCircuitOperator defaults to ignore_downstream_trigger_rules=True.<br/>Placing it upstream of the dbt chain would skip the entire pipeline on weekends."]:::note

    note2["<b>Concurrency Note:</b><br/>The branches are topologically independent,<br/>but max_active_tasks=1 serializes task execution.<br/>Therefore, the branches do not actually execute concurrently."]:::note

    freshness_gate -.- note1
    dbt_seed -.- note2
```

## 3. Resilience Layers — Fault Isolation

```mermaid
flowchart TB
    subgraph L4 ["Layer 4: dagrun_timeout"]
        direction TB
        L4_desc["<b>Scope:</b> Entire DAG run | <b>Bound:</b> 90 min max execution"]

        subgraph L3 ["Layer 3: Airflow Task Retries"]
            direction TB
            L3_desc["<b>Scope:</b> One whole task | <b>Triggers on:</b> Any non-zero exit | <b>Strategy:</b> Exponential backoff (5 min to 30 min)"]

            subgraph L2 ["Layer 2: dbt_retry Wrapper"]
                direction TB
                L2_desc["<b>Scope:</b> Failed dbt nodes | <b>Triggers on:</b> status=error + connection signature"]

                subgraph L1 ["Layer 1: Tenacity"]
                    direction TB
                    L1_desc["<b>Scope:</b> Single HTTP call | <b>Triggers on:</b> requests.RequestException<br/><i>*Requires explicit request timeouts; a hung socket never raises.</i>"]
                end
            end
        end
    end
```

Each layer catches what the one inside it could not, at increasing cost. A
dropped query is absorbed in seconds by layer 2; a dead process costs a layer-3
retry and minutes. A `status=fail` — data that is genuinely wrong — is retried by
none of them and fails immediately.
