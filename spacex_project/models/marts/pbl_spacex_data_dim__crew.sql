{{
    config(
        materialized='table',
        alias='dim__crew',
        unique_key='crew_id'
    )
}}

with crew as (
    select
        crew_id as crew_id,
        crew_name as crew_name,
        crew_agency as crew_agency,
        crew_image as crew_image_url,
        crew_wikipedia as crew_wikipedia_url,
        crew_status as crew_status,
        crew_sdc_extracted_at as crew_sdc_extracted_at,
        crew_created_at as crew_created_at,
	    crew_updated_at as crew_updated_at
        
    from {{ ref('stg_spacex_data__crew') }}
)

select 
    *,
    current_timestamp() as dbt_loaded_at
from crew
