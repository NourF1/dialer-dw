with source_data as (
    select
        _extraction_date,
        _loaded_at,
        _batch_id,
        _source,
        safe.parse_json(payload) as json_payload
    from {{ source('readymode', 'raw_dialer_report') }}
),

parsed_fields as (
    select
        -- Metadata
        _extraction_date,
        _loaded_at,
        _batch_id,
        _source,

        -- Dimensions
        string(json_payload.`Week`) as week_raw,
        string(json_payload.`Queue`) as queue,
        string(json_payload.`Playlist`) as playlist,

        -- Volume Metrics
        coalesce(safe_cast(string(json_payload.`Avg CPA`) as numeric), 0) as avg_cpa,
        coalesce(safe_cast(string(json_payload.`Calls`) as int64), 0) as total_calls,
        coalesce(safe_cast(string(json_payload.`No Answer`) as int64), 0) as no_answer_count,
        coalesce(safe_cast(string(json_payload.`NIS`) as int64), 0) as nis_count,
        coalesce(safe_cast(string(json_payload.`Answered`) as int64), 0) as answered_count,
        coalesce(safe_cast(string(json_payload.`Machines`) as int64), 0) as machine_count,
        coalesce(safe_cast(string(json_payload.`Connects`) as int64), 0) as connect_count,
        coalesce(safe_cast(string(json_payload.`Abandoned`) as int64), 0) as abandoned_count,

        -- Rates & Derived Ratios
        coalesce(safe_cast(replace(string(json_payload.`Answer %`), '%', '') as numeric) / 100.0, 0) as answer_rate,
        coalesce(safe_cast(replace(string(json_payload.`Machine %`), '%', '') as numeric) / 100.0, 0) as machine_rate,
        safe_cast(string(json_payload.`Calls to Connect`) as numeric) as calls_to_connect,
        coalesce(safe_cast(replace(string(json_payload.`Abandoned %`), '%', '') as numeric) / 100.0, 0) as abandoned_rate
    from source_data
)

select * from parsed_fields