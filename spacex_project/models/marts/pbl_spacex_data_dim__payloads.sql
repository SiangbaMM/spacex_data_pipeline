{{
    config(
        materialized='table',
        unique_key='payload_id'
    )
}}

select
    payload_id as payload_id,
    payload_name as payload_name,
    payload_type as payload_type,
    payload_reused as payload_reused,
    payload_launch_id as payload_launch_id,
    payload_customers as payload_customers,
    payload_nationalities as payload_nationalities,
    payload_manufacturers as payload_manufacturers,
    payload_mass_kg as payload_mass_kg,
    payload_mass_lbs as payload_mass_lbs,
    payload_regime as payload_regime,
    payload_orbit as payload_orbit,
    payload_reference_system as payload_reference_system,
    payload_dragon as payload_dragon_id,
    payload_sdc_extracted_at  as payload_sdc_extracted_at,
    payload_created_at as payload_created_at,
	payload_updated_at as payload_updated_at

from {{ ref('stg_spacex_data__payloads') }}
