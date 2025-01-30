
{{ config(
    schema = 'cmp_spacex_data',
    alias = 'vw_stg_spacex_data_ships'
    ) 
}}

with ships as 
(
    select   
	    ship_id as ship_id,
	    name as name,
	    legacy_id as legacy_id,
	    model as model,
	    type as type,
	    active as active,
	    imo as imo,
	    mmsi as mmsi,
	    abs as abs,
	    class as class,
	    mass_kg as mass_kg,
	    mass_lbs as mass_lbs,
	    year_built as year_built,
	    home_port as home_port,
	    status as status,
	    speed_kn as speed_kn,
	    course_deg as course_deg,
	    latitude as latitude,
	    longitude as longitude,
	    last_ais_update as last_ais_update,
	    link as link,
	    image as image,
	    launches as launches,
	    roles as roles,
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
    
    from {{ source('stg_spacex_data', 'stg_spacex_data_ships') }}
)

select * from ships
