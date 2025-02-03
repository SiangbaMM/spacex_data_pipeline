{{
    config(
        materialized='table',
        alias='dim__core',
        unique_key='core_id'
    )
}}

select
    core_id as core_id,
    core_serial as core_serial,
    core_block as core_block,
    core_reuse_count as core_reuse_count,
    core_rtls_attempts as core_rtls_attempts,
    core_rtls_landings as core_rtls_landings,
    core_asds_attempts as core_asds_attempts,
    core_asds_landings as core_asds_landings,
    core_last_update as core_last_update,
    core_status as core_status,
    core_sdc_extracted_at as core_sdc_extracted_at,
    core_created_at as core_created_at,
	core_updated_at as core_updated_at  

from {{ ref('stg_spacex_data__cores') }}
