
{{ config(
    alias = 'vw_stg_spacex_data_rockets'
    )
}}

with rocket as
(
    select
        rocket_id as rocket_id,
	    name as rocket_name,
	    type as rocket_type,
	    active as rocket_is_active,
	    stages as rocket_stages,
	    boosters as rocket_boosters,
	    cost_per_launch as rocket_cost_per_launch,
	    success_rate_pct as rocket_success_rate_pct,
	    first_flight as rocket_first_flight,
	    country as rocket_country,
	    company as rocket_company,
	    height_meters as rocket_height_meters,
	    height_feet as rocket_height_feet,
	    diameter_meters as rocket_diameter_meters,
	    diameter_feet as rocket_diameter_feet,
	    mass_kg as rocket_mass_kg,
	    mass_lbs as rocket_mass_lbs,
	    payload_weights as rocket_payload_weights,
	    try_parse_json(first_stage) as rocket_first_stage,
        try_parse_json(second_stage) as rocket_second_stage,
	    try_parse_json(engines) as rocket_engine,
	    landing_legs as rocket_landing_legs,
	    flickr_images as rocket_flickr_images,
	    wikipedia as rocket_wikipedia,
	    description as rocket_description,
        created_at as rocket_created_at,
	    updated_at as rocket_updated_at,
	    raw_data as rocket_raw_data,
	    _sdc_extracted_at as rocket_sdc_extracted_at,
        _sdc_received_at as rocket_sdc_received_at,
        _sdc_batched_at as rocket_sdc_batched_at,
        _sdc_deleted_at as rocket_sdc_deleted_at,
        _sdc_sequence as rocket_sdc_sequence,
        _sdc_table_version as rocket_sdc_table_version,
        _sdc_sync_started_at as rocket_sdc_sync_started_at

    from {{ source('stg_spacex_data', 'stg_spacex_data_rockets') }}
)

select * from rocket
