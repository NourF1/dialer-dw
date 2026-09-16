{{ config(materialized='table') }}

with dialer_summary as (
    select * from {{ ref('int_dialer_report_by_campaign') }}
),

call_logs as (
    select * from {{ ref('stg_readymode__call_log') }}
),

aliases as (
    select * from {{ ref('campaign_aliases') }}
),

-- Step 1: Apply seed aliasing to call log campaign names, carry is_test, and perform disposition pivot
aliased_and_pivoted_call_logs as (
    select
        c._extraction_date,
        coalesce(a.campaign_canonical, c.current_campaign) as campaign_name,
        
        -- Carry is_test flag from seed
        logical_or(coalesce(a.is_test, false)) as is_test,

        -- Disposition Counts
        countif(c.log_type = 'Voicemail') as voicemail_count,
        countif(c.log_type = 'Not interested') as not_interested_count,
        countif(c.log_type = 'Wrong Number') as wrong_number_count,
        countif(c.log_type = 'Do Not Call') as do_not_call_count,
        countif(c.log_type = 'Qualified Lead') as qualified_lead_count,
        countif(c.log_type = 'Callback') as callback_count,

        -- Total Dispositions & Recording Metrics
        count(*) as disposition_count,
        sum(c.recording_seconds) as total_recording_seconds

    from call_logs c
    left join aliases a
        on c.current_campaign = a.campaign_alias
    group by
        c._extraction_date,
        coalesce(a.campaign_canonical, c.current_campaign)
),

-- Step 2: FULL OUTER JOIN combining dialer reports and call log dispositions
joined as (
    select
        -- Keys
        coalesce(d._extraction_date, c._extraction_date) as _extraction_date,
        coalesce(d.campaign_name, c.campaign_name) as campaign_name,

        -- Data Source Flags
        d.campaign_name is not null as has_dialer_data,
        c.campaign_name is not null as has_disposition_data,

        -- Campaign Attributes (coalescing dialer seed flag AND call log seed flag)
        coalesce(d.is_test, c.is_test, false) as is_test,

        -- Dialer Volume Metrics
        coalesce(d.total_calls, 0) as total_calls,
        coalesce(d.no_answer_count, 0) as no_answer_count,
        coalesce(d.nis_count, 0) as nis_count,
        coalesce(d.answered_count, 0) as answered_count,
        coalesce(d.machine_count, 0) as machine_count,
        coalesce(d.connect_count, 0) as connect_count,
        coalesce(d.abandoned_count, 0) as abandoned_count,

        -- Recomputed Dialer Rates & Ratios (operating on raw d.* metrics to preserve NULL semantics)
        safe_divide(d.answered_count, d.total_calls) as answer_rate,
        safe_divide(d.machine_count, d.answered_count) as machine_rate,
        safe_divide(d.total_calls, d.connect_count) as calls_to_connect,
        safe_divide(d.abandoned_count, d.total_calls) as abandoned_rate,

        -- Call Log Disposition Counts
        coalesce(c.voicemail_count, 0) as voicemail_count,
        coalesce(c.not_interested_count, 0) as not_interested_count,
        coalesce(c.wrong_number_count, 0) as wrong_number_count,
        coalesce(c.do_not_call_count, 0) as do_not_call_count,
        coalesce(c.qualified_lead_count, 0) as qualified_lead_count,
        coalesce(c.callback_count, 0) as callback_count,
        coalesce(c.disposition_count, 0) as disposition_count,

        -- Call Log Duration
        coalesce(c.total_recording_seconds, 0) as total_recording_seconds,

        -- Derived Conversion Metrics (requires dialer calls > 0 and qualified leads > 0)
        safe_divide(nullif(d.total_calls, 0), nullif(c.qualified_lead_count, 0)) as calls_per_qualified_lead

    from dialer_summary d
    full outer join aliased_and_pivoted_call_logs c
        on d._extraction_date = c._extraction_date
       and d.campaign_name = c.campaign_name
)

select * from joined