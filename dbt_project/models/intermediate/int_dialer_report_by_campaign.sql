{{ config(materialized='view') }}

with dialer_report as (
    select * from {{ ref('stg_readymode__dialer_report') }}
),

aliases as (
    select * from {{ ref('campaign_aliases') }}
),

joined as (
    select
        r._extraction_date,
        r.queue,
        coalesce(a.campaign_canonical, r.playlist) as campaign_name,
        coalesce(a.is_test, false) as is_test,

        -- Raw volume counts to aggregate
        r.total_calls,
        r.no_answer_count,
        r.nis_count,
        r.answered_count,
        r.machine_count,
        r.connect_count,
        r.abandoned_count
    from dialer_report r
    left join aliases a
        on r.playlist = a.campaign_alias
),

aggregated as (
    select
        _extraction_date,
        queue,
        campaign_name,
        is_test,

        -- Aggregate volume counts
        sum(total_calls) as total_calls,
        sum(no_answer_count) as no_answer_count,
        sum(nis_count) as nis_count,
        sum(answered_count) as answered_count,
        sum(machine_count) as machine_count,
        sum(connect_count) as connect_count,
        sum(abandoned_count) as abandoned_count,

        -- Recomputed rates & ratios from aggregate sums
        safe_divide(sum(answered_count), sum(total_calls)) as answer_rate,
        safe_divide(sum(machine_count), sum(answered_count)) as machine_rate,
        safe_divide(sum(total_calls), sum(connect_count)) as calls_to_connect,
        safe_divide(sum(abandoned_count), sum(total_calls)) as abandoned_rate

    from joined
    group by
        _extraction_date,
        queue,
        campaign_name,
        is_test
)

select * from aggregated