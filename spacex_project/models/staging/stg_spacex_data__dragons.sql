
{{ config(
    alias = 'vw_stg_spacex_data_dragons'
    )
}}

with dragons as
(
    select
        dragon_id as dragon_id,
	    name as dragon_name,
	    type as dragon_type,
	    active as dragon_active,
	    nullif(crew_capacity, -999999999) as dragon_crew_capacity,
	    nullif(sidewall_angle_deg, -999999999) as dragon_sidewall_angle_deg,
	    nullif(orbit_duration_yr, -999999999) as dragon_orbit_duration_yr,
	    nullif(dry_mass_kg, -999999999) as dragon_dry_mass_kg,
	    nullif(dry_mass_lb, -999999999) as dragon_dry_mass_lb,
	    first_flight as dragon_first_flight,
	    try_parse_json(heat_shield) as dragon_heat_shield,
	    thrusters as dragon_thrusters_number,
	    launch_payload_mass as dragon_launch_payload_mass,
	    launch_payload_vol as dragon_launch_payload_vol,
	    return_payload_mass as dragon_return_payload_mass,
	    return_payload_vol as dragon_return_payload_vol,
	    pressurized_capsule as dragon_pressurized_capsule,
	    trunk as dragon_trunk,
	    height_w_trunk as dragon_height_w_trunk,
	    diameter as dragon_diameter,
	    wikipedia as dragon_wikipedia,
	    description as dragon_description,
	    flickr_images as dragon_flickr_images,
        created_at as dragon_created_at,
	    updated_at as dragon_updated_at,
	    raw_data as dragon_raw_data,
	    _sdc_extracted_at as dragon_sdc_extracted_at,
        _sdc_received_at as dragon_sdc_received_at,
        _sdc_batched_at as dragon_sdc_batched_at,
        _sdc_deleted_at as dragon_sdc_deleted_at,
        _sdc_sequence as dragon_sdc_sequence,
        _sdc_table_version as dragon_sdc_table_version,
        _sdc_sync_started_at as dragon_sdc_sync_started_at

    from {{ source('stg_spacex_data', 'stg_spacex_data_dragons') }}
)

select * from dragons
