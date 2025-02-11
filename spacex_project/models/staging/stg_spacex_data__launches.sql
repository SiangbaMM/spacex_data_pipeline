
{{ config(
    alias = 'vw_stg_spacex_data_launches'
    )
}}

with launches as
(
    select
	    launch_id as launch_id,
	    flight_number as launch_flight_number,
	    name as launch_mission_name,
	    convert_timezone('UTC', date_utc) as launch_date_utc,
	    date_unix as launch_date_unix,
	    convert_timezone('UTC', date_local) as launch_date_local,
	    date_precision as launch_date_precision,
	    static_fire_date_utc as launch_static_fire_date_utc,
	    static_fire_date_unix as launch_static_fire_date_unix,
	    net as launch_net,
	    window as launch_window,
	    rocket as launch_rocket_id,
	    success as launch_is_success,
	    failures as launch_failures,
	    upcoming as launch_is_upcoming,
	    details as launch_mission_details,
	    fairings as launch_fairings,
	    try_parse_json(crew) as launch_crew,
	    ships as launch_ships,
	    capsules as launch_capsules,
	    payloads as launch_payloads,
	    launchpad as launch_launchpad_id,
		try_parse_json(cores) as launch_cores,
		try_parse_json(links) as launch_links,
	    auto_update as launch_auto_update,
	    launch_library_id as launch_library_id,
        created_at as launch_created_at,
	    updated_at as launch_updated_at,
	    raw_data as launch_raw_data,
	    _sdc_extracted_at as launch_sdc_extracted_at,
        _sdc_received_at as launch_sdc_received_at,
        _sdc_batched_at as launch_sdc_batched_at,
        _sdc_deleted_at as launch_sdc_deleted_at,
        _sdc_sequence as launch_sdc_sequence,
        _sdc_table_version as launch_sdc_table_version,
        _sdc_sync_started_at as launch_sdc_sync_started_at

    from {{ source('stg_spacex_data', 'stg_spacex_data_launches') }}
)

select * from launches
