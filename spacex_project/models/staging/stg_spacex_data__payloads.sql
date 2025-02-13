
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
	    nullif(mass_kg, -999999999) as payload_mass_kg,
	    nullif(mass_lbs, -999999999) as payload_mass_lbs,
	    orbit as payload_orbit,
	    reference_system as payload_reference_system,
	    regime as payload_regime,
	    nullif(longitude, -999999999) as payload_longitude,
	    semi_major_axis_km as payload_semi_major_axis_km,
	    nullif(eccentricity, -999999999) as payload_eccentricity,
	    nullif(periapsis_km, -999999999) as payload_periapsis_km,
	    nullif(apoapsis_km, -999999999) as payload_apoapsis_km,
	    nullif(inclination_deg, -999999999) as payload_inclination_deg,
	    nullif(period_min, -999999999) as payload_period_min,
	    nullif(lifespan_years, -999999999) as payload_lifespan_years,
	    epoch as payload_epoch,
	    nullif(mean_motion, -999999999) as payload_mean_motion,
	    nullif(raan, -999999999) as payload_raan,
	    nullif(arg_of_pericenter, -999999999) as payload_arg_of_pericenter,
	    nullif(mean_anomaly, -999999999) as payload_mean_anomaly,
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
