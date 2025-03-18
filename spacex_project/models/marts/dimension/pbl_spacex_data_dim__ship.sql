{%- set schema_name = generate_schema_name(this.schema) -%}

{{
    config(
        materialized='table',
        alias='pbl_spacex_data_dim_ship',
        unique_key='ship_surrogate_key'
    )
}}

with source_ship as (
    select *
    from {{ ref('stg_spacex_data__ship') }}
),

current_ship as (
    {% if adapter.get_relation(this.database, schema_name, this.table) is not none %}
        select *
        from {{ source('pbl_spacex_data', 'pbl_spacex_data_dim_ship') }}
        where is_current
    {% else %}
        select null as ship_surrogate_key,
               null as ship_id,
               null as ship_name,
               null as ship_type,
               null as ship_roles,
               null as ship_is_active,
               null as ship_mass_kg,
               null as ship_year_built,
               null as ship_home_port,
               null as ship_status,
               null as ship_created_at,
               null as dbt_updated_at,
               null::boolean as is_current,
               null::timestamp as valid_from,
               null::timestamp as valid_to
        where false
    {% endif %}
),

change_ship as (
    select
        source.*,
        case
            when cur.ship_surrogate_key is null
                then 'INSERT'
            when (
                source.ship_name != cur.ship_name or
                source.ship_type != cur.ship_type or
                source.ship_roles != cur.ship_roles or
                source.ship_is_active != cur.ship_is_active or
                source.ship_mass_kg != cur.ship_mass_kg or
                source.ship_year_built != cur.ship_year_built or
                source.ship_home_port != cur.ship_home_port or
                source.ship_status != cur.ship_status
            )
                then 'UPDATE'
            else 'NO_CHANGE'
        end as change_type

    from source_ship as source
        left join current_ship as cur
            on source.ship_id = cur.ship_id
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['ship_id', 'ship_name', 'ship_type']) }} as ship_surrogate_key,
        ship_id,
        ship_name,
        ship_type,
        ship_roles,
        ship_is_active,
        ship_mass_kg,
        ship_year_built,
        ship_home_port,
        ship_status,
        ship_created_at,
        current_timestamp() as dbt_updated_at,
        True as is_current,
        current_timestamp() as valid_from,
        '9999-12-31'::timestamp as valid_to

    from change_ship

    where change_type in ('INSERT', 'UPDATE')

    union all

    select
        ship_surrogate_key,
        ship_id,
        ship_name,
        ship_type,
        ship_roles,
        ship_is_active,
        ship_mass_kg,
        ship_year_built,
        ship_home_port,
        ship_status,
        ship_created_at,
        dbt_updated_at,
        is_current,
        valid_from,
        valid_to

    from current_ship

    where ship_id not in (select ship_id from change_ship where change_type in ('UPDATE', 'INSERT'))
)

select * from final
