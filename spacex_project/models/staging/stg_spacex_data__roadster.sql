
{{ config(
    schema = 'cmp_spacex_data',
    alias = 'vw_stg_spacex_data_roadster'
    ) 
}}

with roadster as 
(
    select  
	    roadster_id as roadster_id,
	    name as name,
	    launch_date_utc as launch_date_utc,
	    launch_date_unix as launch_date_unix,
	    launch_mass_kg as launch_mass_kg,
	    launch_mass_lbs as launch_mass_lbs,
	    norad_id as norad_id,
	    epoch_jd as epoch_jd,
	    orbit_type as orbit_type,
	    apoapsis_au as apoapsis_au,
	    periapsis_au as periapsis_au,
	    semi_major_axis_au as semi_major_axis_au,
	    eccentricity as eccentricity,
	    inclination as inclination,
	    longitude as longitude,
	    period_days as period_days,
	    speed_kph as speed_kph,
	    speed_mph as speed_mph,
	    earth_distance_km as earth_distance_km,
	    earth_distance_mi as earth_distance_mi,
	    mars_distance_km as mars_distance_km,
	    mars_distance_mi as mars_distance_mi,
	    wikipedia as wikipedia,
	    details as details,
	    video as video,
	    flickr_images as flickr_images,
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
    
    from {{ source('stg_spacex_data', 'stg_spacex_data_roadster') }}
)

select * from roadster
