
{{ config(
    alias = 'vw_stg_spacex_data_capsules'
    )
}}

with capsules as
(
    select
        capsule_id as capsule_id,
        serial as capsule_serial,
        status as capsule_status,
        dragon as capsule_dragon_id,
        reuse_count as capsule_reuse_count,
        nullif(water_landings, -999999999) as capsule_water_landings,
        nullif(land_landings, -999999999) as capsule_land_landings,
        last_update as capsule_last_update,
        launches as capsule_launches,
        created_at as capsule_created_at,
        updated_at as capsule_updated_at,
        raw_data as capsule_raw_data,
        _sdc_extracted_at as capsule_sdc_extracted_at,
        _sdc_received_at as capsule_sdc_received_at,
        _sdc_batched_at as capsule_sdc_batched_at,
        _sdc_deleted_at as capsule_sdc_deleted_at,
        _sdc_sequence as capsule_sdc_sequence,
        _sdc_table_version as capsule_sdc_table_version,
        _sdc_sync_started_at as capsule_sdc_sync_started_at

    from {{ source('stg_spacex_data', 'stg_spacex_data_capsules') }}
)

select * from capsules
