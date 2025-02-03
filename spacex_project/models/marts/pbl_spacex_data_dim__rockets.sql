{{
    config(
        materialized='table',
        alias='dim__rocket',
        unique_key='rocket_id'
    )
}}

with rockets as (
    select
        rocket_id as rocket_id,
        rocket_name as rocket_name,
        rocket_type as rocket_type,
        rocket_description as rocket_description,
        rocket_height_meters as rocket_height_meters,
        rocket_diameter_meters as rocket_diameter_meters,
        rocket_mass_kg as rocket_mass_kg,
        rocket_stages as rocket_stages,
        rocket_boosters as rocket_boosters,
        rocket_cost_per_launch as rocket_cost_per_launch,
        rocket_success_rate_pct as rocket_success_rate_pct,
        rocket_first_flight as rocket_first_flight,
        rocket_country as rocket_country,
        rocket_company as rocket_company,
        rocket_is_active as rocket_is_active,
        rocket_sdc_extracted_at as rocket_sdc_extracted_at,
        rocket_created_at as rocket_created_at,
	    rocket_updated_at as rocket_updated_at
    
    from {{ ref('stg_spacex_data__rockets') }}
)

select 
    *,
    current_timestamp() as dbt_loaded_at
from rockets
