
{{ config(
    alias = 'vw_stg_spacex_data_roadster'
    ) 
}}

with roadster as 
(
    select  
	    roadster_id as roadster_id,
	    name as roadster_name,
	    launch_date_utc as roadster_launch_date_utc,
	    launch_date_unix as roadster_launch_date_unix,
	    launch_mass_kg as roadster_launch_mass_kg,
	    launch_mass_lbs as roadster_launch_mass_lbs,
	    norad_id as roadster_norad_id,
	    epoch_jd as roadster_epoch_jd,
	    orbit_type as roadster_orbit_type,
	    apoapsis_au as roadster_apoapsis_au,
	    periapsis_au as roadster_periapsis_au,
	    semi_major_axis_au as roadster_semi_major_axis_au,
	    eccentricity as roadster_eccentricity,
	    inclination as roadster_inclination,
	    longitude as roadster_longitude,
	    period_days as roadster_period_days,
	    speed_kph as roadster_speed_kph,
	    speed_mph as roadster_speed_mph,
	    earth_distance_km as roadster_earth_distance_km,
	    earth_distance_mi as roadster_earth_distance_mi,
	    mars_distance_km as roadster_mars_distance_km,
	    mars_distance_mi as roadster_mars_distance_mi,
	    wikipedia as roadster_wikipedia,
	    details as roadster_details,
	    video as roadster_video,
	    flickr_images as roadster_flickr_images,
        created_at as roadster_created_at,
	    updated_at as roadster_updated_at,
	    raw_data as roadster_raw_data,
	    _sdc_extracted_at as roadster_sdc_extracted_at,
        _sdc_received_at as roadster_sdc_received_at,
        _sdc_batched_at as roadster_sdc_batched_at,
        _sdc_deleted_at as roadster_sdc_deleted_at,
        _sdc_sequence as roadster_sdc_sequence,
        _sdc_table_version as roadster_sdc_table_version,
        _sdc_sync_started_at as roadster_sdc_sync_started_at
    
    from {{ source('stg_spacex_data', 'stg_spacex_data_roadster') }}
)

select * from roadster
