{{
    config(
        materialized='table',
        unique_key='landpad_id'
    )
}}

select
    landpad_id as landpad_id,
    landpad_name as landpad_name,
    landpad_full_name as landpad_full_name,
    landpad_status as landpad_status,
    landpad_type as landpad_type,
    landpad_locality as landpad_locality,
    landpad_region as landpad_region,
	landpad_latitude as landpad_latitude,
	landpad_longitude as landpad_longitude,
    landpad_landing_attempts as landpad_landing_attempts,
	landpad_landing_successes as landpad_landing_successes,
    landpad_wikipedia as landpad_wikipedia,
	landpad_details as landpad_details,
    landpad_launches as landpad_launches_id, -- Array of launch IDs
    landpad_sdc_extracted_at as landpad_sdc_extracted_at,
    landpad_created_at as landpad_created_at,
	landpad_updated_at as landpad_updated_at
    
from {{ ref('stg_spacex_data__landpads') }}

