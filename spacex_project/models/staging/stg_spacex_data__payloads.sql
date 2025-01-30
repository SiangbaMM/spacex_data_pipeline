
{{ config(
    schema = 'cmp_spacex_data',
    alias = 'vw_stg_spacex_data_payloads'
    ) 
}}

with payloads as 
(
    select 
	    payload_id as payload_id,
	    name as name,
	    type as type,
	    reused as reused,
	    launch as launch,
	    customers as customers,
	    norad_ids as norad_ids,
	    nationalities as nationalities,
	    manufacturers as manufacturers,
	    mass_kg as mass_kg,
	    mass_lbs as mass_lbs,
	    orbit as orbit,
	    reference_system as reference_system,
	    regime as regime,
	    longitude as longitude,
	    semi_major_axis_km as semi_major_axis_km,
	    eccentricity as eccentricity,
	    periapsis_km as periapsis_km,
	    apoapsis_km as apoapsis_km,
	    inclination_deg as inclination_deg,
	    period_min as period_min,
	    lifespan_years as lifespan_years,
	    epoch as epoch,
	    mean_motion as mean_motion,
	    raan as raan,
	    arg_of_pericenter as arg_of_pericenter,
	    mean_anomaly as mean_anomaly,
	    dragon as dragon,
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
    
    from {{ source('stg_spacex_data', 'stg_spacex_data_payloads') }}
)

select * from payloads
