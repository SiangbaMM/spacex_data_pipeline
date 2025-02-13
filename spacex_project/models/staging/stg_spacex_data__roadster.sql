
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
	    nullif(launch_date_unix, -999999999) as roadster_launch_date_unix,
	    nullif(launch_mass_kg, -999999999) as roadster_launch_mass_kg,
	    nullif(launch_mass_lbs, -999999999) as roadster_launch_mass_lbs,
	    nullif(norad_id, -999999999) as roadster_norad_id,
	    nullif(epoch_jd, -999999999) as roadster_epoch_jd,
	    orbit_type as roadster_orbit_type,
	    nullif(apoapsis_au, -999999999) as roadster_apoapsis_au,
	    nullif(periapsis_au, -999999999) as roadster_periapsis_au,
	    nullif(semi_major_axis_au, -999999999) as roadster_semi_major_axis_au,
	    nullif(eccentricity, -999999999) as roadster_eccentricity,
	    nullif(inclination, -999999999) as roadster_inclination,
	    nullif(longitude, -999999999) as roadster_longitude,
	    nullif(period_days, -999999999) as roadster_period_days,
	    nullif(speed_kph, -999999999) as roadster_speed_kph,
	    nullif(speed_mph, -999999999) as roadster_speed_mph,
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
