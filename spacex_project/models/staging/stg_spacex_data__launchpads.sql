
{{ config(
    alias = 'vw_stg_spacex_data_launchpads'
    ) 
}}

with launchpads as 
(
    select    
	    launchpad_id as launchpad_id,
	    name as launchpad_name,
	    full_name as launchpad_full_name,
	    status as launchpad_status,
	    locality as launchpad_locality,
	    region as launchpad_region,
	    timezone as launchpad_timezone,
	    latitude as launchpad_latitude,
	    longitude as launchpad_longitude,
	    launch_attempts as launchpad_launch_attempts,
	    launch_successes as launchpad_launch_successes,
	    rockets as launchpad_rockets,
	    launches as launchpad_launches,
	    details as launchpad_details,
	    images as launchpad_images,
        created_at as launchpad_created_at,
	    updated_at as launchpad_updated_at,
	    raw_data as launchpad_raw_data,
	    _sdc_extracted_at as launchpad_sdc_extracted_at,
        _sdc_received_at as launchpad_sdc_received_at,
        _sdc_batched_at as launchpad_sdc_batched_at,
        _sdc_deleted_at as launchpad_sdc_deleted_at,
        _sdc_sequence as launchpad_sdc_sequence,
        _sdc_table_version as launchpad_sdc_table_version,
        _sdc_sync_started_at as launchpad_sdc_sync_started_at
    
    from {{ source('stg_spacex_data', 'stg_spacex_data_launchpads') }}
)

select * from launchpads
