with dialer_canonical as (
    select
        coalesce(a.campaign_canonical, d.playlist) as campaign_name,
        logical_or(coalesce(a.is_test, false)) as is_test,
        min(d._extraction_date) as first_seen_date,
        max(d._extraction_date) as last_seen_date
    from {{ ref('stg_readymode__dialer_report') }} d
    left join {{ ref('campaign_aliases') }} a
        on d.playlist = a.campaign_alias
    where d.playlist is not null
    group by 1
),

call_log_canonical as (
    select
        coalesce(a.campaign_canonical, c.current_campaign) as campaign_name,
        logical_or(coalesce(a.is_test, false)) as is_test,
        min(c._extraction_date) as first_seen_date,
        max(c._extraction_date) as last_seen_date
    from {{ ref('stg_readymode__call_log') }} c
    left join {{ ref('campaign_aliases') }} a
        on c.current_campaign = a.campaign_alias
    where c.current_campaign is not null
    group by 1
),

combined as (
    select
        coalesce(d.campaign_name, c.campaign_name) as campaign_name,
        coalesce(d.is_test, c.is_test, false) as is_test,
        d.campaign_name is not null as appears_in_dialer_report,
        c.campaign_name is not null as appears_in_call_log,
        least(
            coalesce(d.first_seen_date, c.first_seen_date),
            coalesce(c.first_seen_date, d.first_seen_date)
        ) as first_seen_date,
        greatest(
            coalesce(d.last_seen_date, c.last_seen_date),
            coalesce(c.last_seen_date, d.last_seen_date)
        ) as last_seen_date
    from dialer_canonical d
    full outer join call_log_canonical c
        on d.campaign_name = c.campaign_name
)

select
    campaign_name,
    is_test,
    appears_in_dialer_report,
    appears_in_call_log,
    first_seen_date,
    last_seen_date
from combined