
{{ config(
    alias = 'vw_stg_spacex_data_capsule'
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
        raw_data as capsule_raw_data

    from {{ source('stg_spacex_data', 'stg_spacex_data_capsule') }}
)

select * from capsules
