
{{ config(
    schema = 'cmp_spacex_data',
    alias = 'vw_stg_spacex_data_capsules'
    ) 
}}

with capsules as 
(
    select 
        capsule_id as capsule_id,
        serial as serial,
        status as status,
        dragon as dragon,
        reuse_count as reuse_count, 
        water_landings as water_landings,
        land_landings as land_landings,
        last_update as last_update,
        launches as launches,
        created_at as created_at,
        updated_at as updated_at,
        raw_data as raw_data,
        _sdc_extracted_at as sdc_extracted_at,
        _sdc_received_at as sdc_received_at,
        _sdc_batched_at as sdc_batched_at,
        _sdc_deleted_at as sdc_deleted_at,
        _sdc_sequence as sdc_sequence,
        _sdc_table_version as sdc_table_version,
        _sdc_sync_started_at as sdc_sync_started_at
    
    from {{ source('stg_spacex_data', 'stg_spacex_data_capsules') }}
)

select * from capsules
