{{ config(severity='error') }}

with fact_daily as (
    select
        _extraction_date,
        sum(disposition_count) as total_disposition_count,
        sum(total_calls)        as total_calls,
        sum(connect_count)      as total_connect_count
    from {{ ref('fct_campaign_daily') }}
    group by 1
),

stg_call_log as (
    select
        _extraction_date,
        count(*) as total_disposition_count
    from {{ ref('stg_readymode__call_log') }}
    group by 1
),

stg_dialer_report as (
    select
        _extraction_date,
        sum(total_calls)   as total_calls,
        sum(connect_count) as total_connect_count
    from {{ ref('stg_readymode__dialer_report') }}
    group by 1
),

stg_combined as (
    select
        coalesce(cl._extraction_date, dr._extraction_date) as _extraction_date,
        coalesce(cl.total_disposition_count, 0)            as total_disposition_count,
        coalesce(dr.total_calls, 0)                        as total_calls,
        coalesce(dr.total_connect_count, 0)                as total_connect_count
    from stg_call_log cl
    full outer join stg_dialer_report dr
        on cl._extraction_date = dr._extraction_date
),

reconciliation_wide as (
    select
        coalesce(f._extraction_date, s._extraction_date) as _extraction_date,
        
        -- Metric 1: disposition_count
        coalesce(f.total_disposition_count, 0) as fact_disposition_count,
        coalesce(s.total_disposition_count, 0) as source_disposition_count,
        
        -- Metric 2: total_calls
        coalesce(f.total_calls, 0)             as fact_total_calls,
        coalesce(s.total_calls, 0)             as source_total_calls,
        
        -- Metric 3: connect_count
        coalesce(f.total_connect_count, 0)     as fact_connect_count,
        coalesce(s.total_connect_count, 0)     as source_connect_count
    from fact_daily f
    full outer join stg_combined s
        on f._extraction_date = s._extraction_date
),

reconciliation_long as (
    select
        _extraction_date,
        'disposition_count' as metric_name,
        fact_disposition_count as fact_value,
        source_disposition_count as source_value,
        fact_disposition_count - source_disposition_count as difference
    from reconciliation_wide

    union all

    select
        _extraction_date,
        'total_calls' as metric_name,
        fact_total_calls as fact_value,
        source_total_calls as source_value,
        fact_total_calls - source_total_calls as difference
    from reconciliation_wide

    union all

    select
        _extraction_date,
        'connect_count' as metric_name,
        fact_connect_count as fact_value,
        source_connect_count as source_value,
        fact_connect_count - source_connect_count as difference
    from reconciliation_wide
)

select
    _extraction_date,
    metric_name,
    fact_value,
    source_value,
    difference
from reconciliation_long
where difference != 0