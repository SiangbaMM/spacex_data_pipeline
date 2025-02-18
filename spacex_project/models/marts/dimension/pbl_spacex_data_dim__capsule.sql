{%- set schema_name = generate_schema_name(this.schema) -%}

{{
    config(
        alias='pbl_spacex_data_dim_capsule',
        unique_key='capsule_surrogate_key'
    )
}}

with source_capsule as (
    select
        {{ dbt_utils.star(ref('stg_spacex_data__capsule')) }}
    from {{ ref('stg_spacex_data__capsule') }}
),

current_capsule as (
    {% if adapter.get_relation(this.database, schema_name, this.table) is not none %}
        select *
        from  {{ source('pbl_spacex_data', 'pbl_spacex_data_dim_capsule') }}
        where is_current
    {% else %}
        select null as capsule_surrogate_key,
               null as capsule_id,
               null as capsule_launches,
               null as capsule_serial,
               null as capsule_status,
               null as capsule_reuse_count,
               null as capsule_water_landings,
               null as capsule_land_landings,
               null as capsule_created_at,
               null as dbt_updated_at,
               null::boolean as is_current,
               null::timestamp as valid_from,
               null::timestamp as valid_to
        where false
    {% endif %}
),

change_capsule as (
    select
        source.*,
        case
            when cur.capsule_surrogate_key is null
                then 'INSERT'
            when (
                source.capsule_id != cur.capsule_id or
                source.capsule_serial != cur.capsule_serial or
                source.capsule_status != cur.capsule_status or
                source.capsule_reuse_count != cur.capsule_reuse_count or
                source.capsule_water_landings != cur.capsule_water_landings or
                source.capsule_land_landings != cur.capsule_land_landings
            )
                then 'UPDATE'
            else 'NO_CHANGE'
        end as change_type

    from source_capsule as source
        left join current_capsule as cur
            on source.capsule_id = cur.capsule_id
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['capsule_id', 'capsule_serial']) }} as capsule_surrogate_key,
        capsule_id,
        capsule_launches,
        capsule_serial,
        capsule_status,
        capsule_reuse_count,
        capsule_water_landings,
        capsule_land_landings,
        capsule_created_at,
        current_timestamp() as dbt_updated_at,
        True as is_current,
        current_timestamp() as valid_from,
        '9999-12-31'::timestamp as valid_to

    from change_capsule

    where change_type in ('INSERT', 'UPDATE')

    union all

    select
        capsule_surrogate_key,
        capsule_id,
        capsule_launches,
        capsule_serial,
        capsule_status,
        capsule_reuse_count,
        capsule_water_landings,
        capsule_land_landings,
        capsule_created_at,
        dbt_updated_at,
        is_current,
        valid_from,
        valid_to

    from current_capsule

    where capsule_id not in (select capsule_id from change_capsule where change_type in ('UPDATE', 'INSERT'))
)

select * from final
