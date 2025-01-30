
{{ config(
    schema = 'cmp_spacex_data',
    alias = 'vw_stg_spacex_data_dragons'
    ) 
}}

with dragons as 
(
    select 
        dragon_id as dragon_id,
	    name as name,
	    type as type,
	    active as active,
	    crew_capacity as crew_capacity,
	    sidewall_angle_deg as sidewall_angle_deg,
	    orbit_duration_yr as orbit_duration_yr,
	    dry_mass_kg as dry_mass_kg,
	    dry_mass_lb as dry_mass_lb,
	    first_flight as first_flight,
	    heat_shield as heat_shield,
	    thrusters as thrusters,
	    launch_payload_mass as launch_payload_mass,
	    launch_payload_vol as launch_payload_vol,
	    return_payload_mass as return_payload_mass,
	    return_payload_vol as return_payload_vol,
	    pressurized_capsule as pressurized_capsule,
	    trunk as trunk,
	    height_w_trunk as height_w_trunk,
	    diameter as diameter,
	    wikipedia as wikipedia,
	    description as description,
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
    
    from {{ source('stg_spacex_data', 'stg_spacex_data_dragons') }}
)

select * from dragons
