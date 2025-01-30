
{{ config(
    schema = 'cmp_spacex_data',
    alias = 'vw_stg_spacex_data_launchpads'
    ) 
}}

with launchpads as 
(
    select    
	    launch_id as launch_id,
	    flight_number as flight_number,
	    name as name,
	    date_utc as date_utc,
	    date_unix as date_unix,
	    date_local as date_local,
	    date_precision as date_precision,
	    static_fire_date_utc as static_fire_date_utc,
	    static_fire_date_unix as static_fire_date_unix,
	    net as net,
	    window as window,
	    rocket as rocket,
	    success as success,
	    failures as failures,
	    upcoming as upcoming,
	    details as details,
	    fairings as fairings,
	    crew as crew,
	    ships as ships,
	    capsules as capsules,
	    payloads as payloads,
	    launchpad as launchpad,
	    cores as cores,
	    links as links,
	    auto_update as auto_update,
	    launch_library_id as launch_library_id,
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
    
    from {{ source('stg_spacex_data', 'stg_spacex_data_launchpads') }}
)

select * from launchpads
