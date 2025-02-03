
{{ config(
    alias = 'vw_stg_spacex_data_ships'
    ) 
}}

with ships as 
(
    select   
	    ship_id as ship_id,
	    name as ship_name,
	    legacy_id as ship_legacy_id,
	    model as ship_model,
	    type as ship_type,
	    active as ship_is_active,
	    imo as ship_imo,
	    mmsi as ship_mmsi,
	    abs as ship_abs,
	    class as ship_class,
	    mass_kg as ship_mass_kg,
	    mass_lbs as ship_mass_lbs,
	    year_built as ship_year_built,
	    home_port as ship_home_port,
	    status as ship_status,
	    speed_kn as ship_speed_kn,
	    course_deg as ship_course_deg,
	    latitude as ship_latitude,
	    longitude as ship_longitude,
	    last_ais_update as ship_last_ais_update,
	    link as ship_link,
	    image as ship_image,
	    launches as ship_launch_id,
	    roles as ship_roles,
        created_at as ship_created_at,
	    updated_at as ship_updated_at,
	    raw_data as ship_raw_data,
	    _sdc_extracted_at as ship_sdc_extracted_at,
        _sdc_received_at as ship_sdc_received_at,
        _sdc_batched_at as ship_sdc_batched_at,
        _sdc_deleted_at as ship_sdc_deleted_at,
        _sdc_sequence as ship_sdc_sequence,
        _sdc_table_version as ship_sdc_table_version,
        _sdc_sync_started_at as ship_sdc_sync_started_at
    
    from {{ source('stg_spacex_data', 'stg_spacex_data_ships') }}
)

select * from ships
