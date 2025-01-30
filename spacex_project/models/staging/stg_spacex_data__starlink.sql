
{{ config(
    schema = 'cmp_spacex_data',
    alias = 'vw_stg_spacex_data_starlink'
    ) 
}}

with starlink as 
(
    select    
	    starlink_id as starlink_id,
	    version as version,
	    launch as launch,
	    longitude as longitude,
	    latitude as latitude,
	    height_km as height_km,
	    velocity_kms as velocity_kms,
	    spacetrack as spacetrack,
	    launch_date as launch_date,
	    object_name as object_name,
	    object_id as object_id,
	    epoch as epoch,
	    period_min as period_min,
	    inclination_deg as inclination_deg,
	    apoapsis_km as apoapsis_km,
	    periapsis_km as periapsis_km,
	    eccentricity as eccentricity,
	    mean_motion as mean_motion,
	    mean_anomaly as mean_anomaly,
	    arg_of_pericenter as arg_of_pericenter,
	    raan as raan,
	    semi_major_axis_km as semi_major_axis_km,
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
    
    from {{ source('stg_spacex_data', 'stg_spacex_data_starlink') }}
)

select * from starlink
