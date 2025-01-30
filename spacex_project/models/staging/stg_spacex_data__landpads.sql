
{{ config(
    schema = 'cmp_spacex_data',
    alias = 'vw_stg_spacex_data_landpads'
    ) 
}}

with landpads as 
(
    select 
        landpad_id as landpad_id,
	    name as name,
	    full_name as full_name,
	    status as status,
	    type as type,
	    locality as locality,
	    region as region,
	    latitude as latitude,
	    longitude as longitude,
	    landing_attempts as landing_attempts,
	    landing_successes as landing_successes,
	    wikipedia as wikipedia,
	    details as details,
	    launches as launches,
	    images as images,
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
    
    from {{ source('stg_spacex_data', 'stg_spacex_data_landpads') }}
)

select * from landpads
