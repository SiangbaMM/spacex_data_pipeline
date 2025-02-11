{{
    config(
        materialized='table',
        unique_key='launchpad_id'
    )
}}

with launchpads as (
    select
        launchpad_id as launchpad_id,
        launchpad_name as launchpad_name,
        launchpad_full_name as launchpad_full_name,
        launchpad_locality as launchpad_locality,
        launchpad_region as launchpad_region,
        launchpad_latitude as launchpad_latitude,
        launchpad_longitude as launchpad_longitude,
        launchpad_launch_attempts as launchpad_launch_attempts,
        launchpad_launch_successes as launchpad_launch_successes,
        launchpad_status as launchpad_status,
        launchpad_sdc_extracted_at as launchpad_sdc_extracted_at,
        launchpad_created_at as launchpad_created_at,
	    launchpad_updated_at as launchpad_updated_at

    from {{ ref('stg_spacex_data__launchpads') }}
)

select
    *,
    current_timestamp() as dbt_loaded_at
from launchpads
