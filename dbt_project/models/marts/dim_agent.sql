with agent_dates as (
    select
        agent_login,
        min(_extraction_date) as first_seen_date,
        max(_extraction_date) as last_seen_date
    from {{ ref('stg_readymode__call_log') }}
    where agent_login is not null
    group by 1
),

latest_agent_name as (
    select
        agent_login,
        agent_name
    from {{ ref('stg_readymode__call_log') }}
    where agent_login is not null
    qualify row_number() over (
        partition by agent_login 
        order by logged_at desc, call_log_id desc
    ) = 1
)

select
    l.agent_login,
    l.agent_name,
    d.first_seen_date,
    d.last_seen_date
from latest_agent_name l
inner join agent_dates d
    on l.agent_login = d.agent_login