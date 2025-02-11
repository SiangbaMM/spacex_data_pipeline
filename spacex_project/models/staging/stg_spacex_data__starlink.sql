
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
	    longitude as starlink_longitude,
	    latitude as starlink_latitude,
	    height_km as starlink_height_km,
	    velocity_kms as starlink_velocity_kms,
	    try_parse_json(spacetrack) as starlink_spacetrack,
	    launch_date as starlink_launch_date_utc,
	    object_name as starlink_object_name,
	    object_id as starlink_object_id,
	    epoch as starlink_epoch,
	    period_min as starlink_period_min,
	    inclination_deg as starlink_inclination_deg,
	    apoapsis_km as starlink_apoapsis_km,
	    periapsis_km as starlink_periapsis_km,
	    eccentricity as starlink_eccentricity,
	    mean_motion as starlink_mean_motion,
	    mean_anomaly as starlink_mean_anomaly,
	    arg_of_pericenter as starlink_arg_of_pericenter,
	    raan as starlink_raan,
	    semi_major_axis_km as starlink_semi_major_axis_km,
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
