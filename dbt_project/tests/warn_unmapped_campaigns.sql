{{ config(
    severity = 'warn',
    store_failures = true,
    description = 'Warns on unmapped campaigns accumulating >10 dispositions in the last 7 days of available data, providing top fuzzy candidates.'
) }}

with max_date as (
    select max(_extraction_date) as max_extraction_date 
    from {{ ref('fct_campaign_daily') }}
),

recent_fct as (
    select
        f.campaign_name,
        f._extraction_date,
        f.disposition_count,
        f.has_dialer_data,
        f.is_test
    from {{ ref('fct_campaign_daily') }} f
    cross join max_date m
    where f._extraction_date >= date_sub(m.max_extraction_date, interval 7 day)
),

-- Step 1: Identify unmapped campaigns with >10 dispositions in the latest 7-day data window (excluding test campaigns)
unmapped_campaigns as (
    select
        campaign_name as unmapped_campaign,
        sum(disposition_count) as recent_disposition_count,
        min(_extraction_date) as first_seen_recent,
        max(_extraction_date) as last_seen_recent
    from recent_fct
    where has_dialer_data = false
      and is_test = false
    group by campaign_name
    having sum(disposition_count) > 10
),

-- Step 2: Get distinct candidate campaigns that actually have dialer report data
dialer_candidates as (
    select distinct
        campaign_name as candidate_campaign
    from {{ ref('fct_campaign_daily') }}
    where has_dialer_data = true
),

-- Step 3: Compute string similarity; apply LEFT JOIN and >= 0.45 threshold
scored_matches as (
    select
        u.unmapped_campaign,
        u.recent_disposition_count,
        u.first_seen_recent,
        u.last_seen_recent,
        c.candidate_campaign,
        case 
            when c.candidate_campaign is not null then
                1.0 - (
                    edit_distance(lower(u.unmapped_campaign), lower(c.candidate_campaign)) /
                    greatest(length(u.unmapped_campaign), length(c.candidate_campaign))
                )
            else null
        end as similarity_score
    from unmapped_campaigns u
    left join dialer_candidates c
        on (
            1.0 - (
                edit_distance(lower(u.unmapped_campaign), lower(c.candidate_campaign)) /
                greatest(length(u.unmapped_campaign), length(c.candidate_campaign))
            )
        ) >= 0.45
),

-- Step 4: Extract top 2 suggestions per unmapped campaign while preserving unmatched rows
top_suggestions as (
    select
        unmapped_campaign,
        recent_disposition_count,
        first_seen_recent,
        last_seen_recent,
        candidate_campaign as suggested_campaign_alias,
        round(similarity_score, 4) as match_similarity
    from scored_matches
    qualify row_number() over (
        partition by unmapped_campaign 
        order by similarity_score desc nulls last
    ) <= 2
)

select * from top_suggestions