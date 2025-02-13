
{{ config(
    alias = 'vw_stg_spacex_data_starlink'
    )
}}

with starlink as
(
    select
	    starlink_id as starlink_id,
	    version as starlink_satellite_version,
	    launch as starlink_launch_id,
	    nullif(longitude, -999999999) as starlink_longitude,
	    nullif(latitude, -999999999) as starlink_latitude,
	    nullif(height_km, -999999999) as starlink_height_km,
	    nullif(velocity_kms, -999999999) as starlink_velocity_kms,
	    try_parse_json(spacetrack) as starlink_spacetrack,
	    launch_date as starlink_launch_date_utc,
	    object_name as starlink_object_name,
	    object_id as starlink_object_id,
	    epoch as starlink_epoch,
	    nullif(period_min, -999999999) as starlink_period_min,
	    nullif(inclination_deg, -999999999) as starlink_inclination_deg,
	    nullif(apoapsis_km, -999999999) as starlink_apoapsis_km,
	    nullif(periapsis_km, -999999999) as starlink_periapsis_km,
	    nullif(eccentricity, -999999999) as starlink_eccentricity,
	    nullif(mean_motion, -999999999) as starlink_mean_motion,
	    nullif(mean_anomaly, -999999999) as starlink_mean_anomaly,
	    nullif(arg_of_pericenter, -999999999) as starlink_arg_of_pericenter,
	    nullif(raan, -999999999) as starlink_raan,
	    nullif(semi_major_axis_km, -999999999) as starlink_semi_major_axis_km,
        created_at as starlink_created_at,
	    updated_at as starlink_updated_at,
	    raw_data as starlink_raw_data,
	    _sdc_extracted_at as starlink_sdc_extracted_at,
        _sdc_received_at as starlink_sdc_received_at,
        _sdc_batched_at as starlink_sdc_batched_at,
        _sdc_deleted_at as starlink_sdc_deleted_at,
        _sdc_sequence as starlink_sdc_sequence,
        _sdc_table_version as starlink_sdc_table_version,
        _sdc_sync_started_at as starlink_sdc_sync_started_at

    from {{ source('stg_spacex_data', 'stg_spacex_data_starlink') }}
)

select * from starlink
