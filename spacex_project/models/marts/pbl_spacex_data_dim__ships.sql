{{
    config(
        materialized='table',
        unique_key='ship_id'
    )
}}

with ships as (
    select
        ship_id as ship_id,
        ship_name as ship_name,
        ship_type as ship_type,
        ship_roles as ship_roles,
        ship_is_active as ship_is_active,
        ship_mass_kg as ship_mass_kg,
        ship_year_built as ship_year_built,
        ship_home_port as ship_home_port,
        ship_status as ship_status,
        ship_sdc_extracted_at as ship_sdc_extracted_at,
        ship_created_at as ship_created_at,
	    ship_updated_at as ship_updated_at

    from {{ ref('stg_spacex_data__ships') }}
)

select
    * ,
    current_timestamp() as dbt_loaded_at
from ships
