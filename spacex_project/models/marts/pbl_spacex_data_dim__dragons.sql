{{
    config(
        materialized='table',
        unique_key='dragon_id'
    )
}}

with dragons as (
    select
        dragon_id as dragon_id,
        dragon_name as dragon_name,
        dragon_type as dragon_type,
        dragon_crew_capacity as dragon_crew_capacity,
        dragon_orbit_duration_yr as dragon_orbit_duration_yr,
        dragon_dry_mass_kg as dragon_dry_mass_kg,
        dragon_first_flight as dragon_first_flight,
        dragon_heat_shield:material as dragon_heat_shield_material,
        dragon_heat_shield:size_meters as dragon_heat_shield_size_meters,
        dragon_heat_shield:temp_degrees as dragon_heat_shield_temp_degrees,
        dragon_thrusters_number as dragon_thrusters_number,
        dragon_sdc_extracted_at as dragon_sdc_extracted_at,
        dragon_created_at as dragon_created_at,
	    dragon_updated_at as dragon_updated_at
        
    from {{ ref('stg_spacex_data__dragons') }}
)

select 
    *,
    current_timestamp() as dbt_loaded_at
from dragons
