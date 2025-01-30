
{{ config(
    schema = 'cmp_spacex_data',
    alias = 'vw_stg_spacex_data_rockets'
    ) 
}}

with rocket as 
(
    select 
        rocket_id as rocket_id,
	    name as name,
	    type as type,
	    active as active,
	    stages as stages,
	    boosters as boosters,
	    cost_per_launch as cost_per_launch,
	    success_rate_pct as success_rate_pct,
	    first_flight as first_flight,
	    country as country,
	    company as company,
	    height_meters as height_meters,
	    height_feet as height_feet,
	    diameter_meters as diameter_meters,
	    diameter_feet as diameter_feet,
	    mass_kg as mass_kg,
	    mass_lbs as mass_lbs,
	    payload_weights as payload_weights,
	    first_stage as first_stage,
	    second_stage as second_stage,
	    engines as engines,
	    landing_legs as landing_legs,
	    flickr_images as flickr_images,
	    wikipedia as wikipedia,
	    description as description,
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
    
    from {{ source('stg_spacex_data', 'stg_spacex_data_rockets') }}
)

select * from rocket
