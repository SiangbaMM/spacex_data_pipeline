{%- set schema_name = generate_schema_name(this.schema) -%}

{{
    config(
        alias='pbl_spacex_data_dim_core',
        unique_key='core_surrogate_key'
    )
}}

with source_core as (
    select *
    from {{ ref('stg_spacex_data__core') }}
),

current_core as (
    {% if adapter.get_relation(this.database, schema_name, this.table) is not none %}
        select *
        from {{ source('pbl_spacex_data', 'pbl_spacex_data_dim_core') }}
        where is_current
    {% else %}
        select null as core_surrogate_key,
               null as core_id,
               null as core_serial,
               null as core_block,
               null as core_status,
               null as core_reuse_count,
               null as core_rtls_attempts,
               null as core_rtls_landings,
               null as core_asds_attempts,
               null as core_asds_landings,
               null as core_last_update,
               null as core_created_at,
               null as dbt_updated_at,
               null::boolean as is_current,
               null::timestamp as valid_from,
               null::timestamp as valid_to
        where false
    {% endif %}
),

change_core as (
    select
        source.*,
        case
            when cur.core_surrogate_key is null
                then 'INSERT'
            when (
                source.core_serial != cur.core_serial or
                source.core_block != cur.core_block or
                source.core_status != cur.core_status or
                source.core_reuse_count != cur.core_reuse_count or
                source.core_rtls_attempts != cur.core_rtls_attempts or
                source.core_rtls_landings != cur.core_rtls_landings or
                source.core_asds_attempts != cur.core_asds_attempts or
                source.core_asds_landings != cur.core_asds_landings
            )
                then 'UPDATE'
            else 'NO_CHANGE'
        end as change_type

    from source_core as source
        left join current_core as cur
            on source.core_id = cur.core_id
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['core_id', 'core_serial']) }} as core_surrogate_key,
        core_id,
        core_serial,
        core_block,
        core_status,
        core_reuse_count,
        core_rtls_attempts,
        core_rtls_landings,
        core_asds_attempts,
        core_asds_landings,
        core_last_update,
        core_created_at,
        current_timestamp() as dbt_updated_at,
        True as is_current,
        current_timestamp() as valid_from,
        '9999-12-31'::timestamp as valid_to

    from change_core

    where change_type in ('INSERT', 'UPDATE')

    union all

    select
        core_surrogate_key,
        core_id,
        core_serial,
        core_block,
        core_status,
        core_reuse_count,
        core_rtls_attempts,
        core_rtls_landings,
        core_asds_attempts,
        core_asds_landings,
        core_last_update,
        core_created_at,
        dbt_updated_at,
        is_current,
        valid_from,
        valid_to

    from current_core

    where core_id not in (select core_id from change_core where change_type in ('UPDATE', 'INSERT'))
)

select * from final
