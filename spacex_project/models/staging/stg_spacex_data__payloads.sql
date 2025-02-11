
{{ config(
    alias = 'vw_stg_spacex_data_payloads'
    )
}}

with payloads as
(
    select
	    payload_id as payload_id,
	    name as payload_name,
	    type as payload_type,
	    reused as payload_reused,
	    launch as payload_launch_id,
	    customers as payload_customers,
	    norad_ids as payload_norad_ids,
	    nationalities as payload_nationalities,
	    manufacturers as payload_manufacturers,
	    mass_kg as payload_mass_kg,
	    mass_lbs as payload_mass_lbs,
	    orbit as payload_orbit,
	    reference_system as payload_reference_system,
	    regime as payload_regime,
	    longitude as payload_longitude,
	    semi_major_axis_km as payload_semi_major_axis_km,
	    eccentricity as payload_eccentricity,
	    periapsis_km as payload_periapsis_km,
	    apoapsis_km as payload_apoapsis_km,
	    inclination_deg as payload_inclination_deg,
	    period_min as payload_period_min,
	    lifespan_years as payload_lifespan_years,
	    epoch as payload_epoch,
	    mean_motion as payload_mean_motion,
	    raan as payload_raan,
	    arg_of_pericenter as payload_arg_of_pericenter,
	    mean_anomaly as payload_mean_anomaly,
	    dragon as payload_dragon,
        created_at as payload_created_at,
	    updated_at as payload_updated_at,
	    raw_data as payload_raw_data,
	    _sdc_extracted_at as payload_sdc_extracted_at,
        _sdc_received_at as payload_sdc_received_at,
        _sdc_batched_at as payload_sdc_batched_at,
        _sdc_deleted_at as payload_sdc_deleted_at,
        _sdc_sequence as payload_sdc_sequence,
        _sdc_table_version as payload_sdc_table_version,
        _sdc_sync_started_at as payload_sdc_sync_started_at

    from {{ source('stg_spacex_data', 'stg_spacex_data_payloads') }}
)

select * from payloads
