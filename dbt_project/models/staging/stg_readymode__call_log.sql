with source_data as (
    select
        _extraction_date,
        _loaded_at,
        _batch_id,
        _source,
        safe.parse_json(payload) as json_payload
    from {{ source('readymode', 'raw_call_log') }}
),

parsed_fields as (
    select
        -- Metadata
        _extraction_date,
        _loaded_at,
        _batch_id,
        _source,

        -- Primary Keys & Timestamps
        string(json_payload.`Call Log ID`) as call_log_id,
        safe.parse_timestamp('%m/%d/%Y %I:%M:%S %p', string(json_payload.`Log Time`)) as logged_at,
        safe.parse_date('%m/%d/%Y', string(json_payload.`Log Time (Date)`)) as call_date,

        -- Agent & Campaign Metadata
        string(json_payload.`Agent login`) as agent_login,
        trim(string(json_payload.`Agent name`)) as agent_name,
        string(json_payload.`Original campaign`) as original_campaign,
        string(json_payload.`Current campaign`) as current_campaign,
        string(json_payload.`Call type`) as call_type,

        -- Dispositions & Metrics
        string(json_payload.`Log Type`) as log_type,
        coalesce(safe_cast(string(json_payload.`Recording Length (Seconds)`) as int64), 0) as recording_seconds
    from source_data
)

select * from parsed_fields