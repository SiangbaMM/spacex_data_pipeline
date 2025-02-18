
{{ config(
    alias = 'vw_stg_spacex_data_rocket'
    )
}}

with rocket as
(
    select
        rocket_id as rocket_id,
	    name as rocket_name,
	    type as rocket_type,
	    active as rocket_is_active,
	    nullif(stages, -999999999) as rocket_stages,
	    nullif(boosters, -999999999) as rocket_boosters,
	    nullif(cost_per_launch, -999999999) as rocket_cost_per_launch,
	    nullif(success_rate_pct, -999999999) as rocket_success_rate_pct,
	    first_flight as rocket_first_flight,
	    country as rocket_country,
	    company as rocket_company,
	    nullif(height_meters, -999999999) as rocket_height_meters,
	    nullif(height_feet, -999999999) as rocket_height_feet,
	    nullif(diameter_meters, -999999999) as rocket_diameter_meters,
	    nullif(diameter_feet, -999999999) as rocket_diameter_feet,
	    nullif(mass_kg, -999999999) as rocket_mass_kg,
	    nullif(mass_lbs, -999999999) as rocket_mass_lbs,
	    payload_weights as rocket_payload_weights,
	    try_parse_json(first_stage) as rocket_first_stage,
        try_parse_json(second_stage) as rocket_second_stage,
	    try_parse_json(engines) as rocket_engine,
	    landing_legs as rocket_landing_legs,
	    flickr_images as rocket_flickr_images,
	    wikipedia as rocket_wikipedia,
	    description as rocket_description,
        created_at as rocket_created_at,
	    raw_data as rocket_raw_data

    from {{ source('stg_spacex_data', 'stg_spacex_data_rocket') }}
)

select * from rocket
