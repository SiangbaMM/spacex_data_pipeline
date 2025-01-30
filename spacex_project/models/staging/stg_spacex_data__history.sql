
{{ config(
    schema = 'cmp_spacex_data',
    alias = 'vw_stg_spacex_data_history'
    ) 
}}

with history as 
(
    select 
        history_id as history_id,
	    title as title,
	    event_date_utc as event_date_utc,
	    event_date_unix as event_date_unix,
	    details as details,
	    links as links,
	    flight_number as flight_number,
        created_at as created_at,
	    updated_at as updated_at,
	    raw_data as raw_data,
	    _sdc_extracted_at as sdc_extracted_at,
        _sdc_received_at as sdc_received_at,
        _sdc_batched_at as sdc_batched_at,
        _sdc_deleted_at as sdc_deleted_at,
        _sdc_sequence as sdc_sequence,
        _sdc_table_version as sdc_table_version,
        _sdc_sync_started_at as sdc_sync_started_at
    
    from {{ source('stg_spacex_data', 'stg_spacex_data_history') }}
)

select * from history
