{{
    config(
        materialized='table',
        unique_key='capsule_id'
    )
}}

select
    capsule_id as capsule_id,
    capsule_serial as capsule_serial,
    capsule_status as capsule_status,
    capsule_last_update as capsule_last_update,
    capsule_reuse_count as capsule_reuse_count,
    capsule_water_landings as capsule_water_landings,
    capsule_land_landings as capsule_land_landings,
    capsule_launches as capsule_launches, -- Array of launch IDs
    capsule_sdc_extracted_at as capsule_sdc_extracted_at,
    capsule_created_at as capsule_created_at,
	capsule_updated_at as capsule_updated_at

from {{ ref('stg_spacex_data__capsules') }}
