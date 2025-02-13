
{{ config(
    alias = 'vw_stg_spacex_data_landpads'
    )
}}

with landpads as
(
    select
        landpad_id as landpad_id,
	    name as landpad_name,
	    full_name as landpad_full_name,
	    status as landpad_status,
	    type as landpad_type,
	    locality as landpad_locality,
	    region as landpad_region,
	    nullif(latitude, -999999999) as landpad_latitude,
	    nullif(longitude, -999999999) as landpad_longitude,
	    nullif(landing_attempts, -999999999) as landpad_landing_attempts,
	    nullif(landing_successes, -999999999) as landpad_landing_successes,
	    wikipedia as landpad_wikipedia,
	    details as landpad_details,
	    launches as landpad_launches,
	    images as landpad_images,
        created_at as landpad_created_at,
	    updated_at as landpad_updated_at,
	    raw_data as landpad_raw_data,
	    _sdc_extracted_at as landpad_sdc_extracted_at,
        _sdc_received_at as landpad_sdc_received_at,
        _sdc_batched_at as landpad_sdc_batched_at,
        _sdc_deleted_at as landpad_sdc_deleted_at,
        _sdc_sequence as landpad_sdc_sequence,
        _sdc_table_version as landpad_sdc_table_version,
        _sdc_sync_started_at as landpad_sdc_sync_started_at

    from {{ source('stg_spacex_data', 'stg_spacex_data_landpads') }}
)

select * from landpads
