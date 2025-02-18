{%- set schema_name = generate_schema_name(this.schema) -%}

{{
    config(
        materialized='table',
        alias='pbl_spacex_data_dim_rocket',
        unique_key='rocket_surrogate_key'
    )
}}

with source_rocket as (
    select *
    from {{ ref('stg_spacex_data__rocket') }}
),

current_rocket as (
    {% if adapter.get_relation(this.database, schema_name, this.table) is not none %}
        select *
        from {{ ref('pbl_spacex_data_dim__rocket') }}
        where is_current
    {% else %}
        select null as rocket_surrogate_key,
               null as rocket_id,
               null as rocket_name,
               null as rocket_type,
               null as rocket_is_active,
               null as rocket_description,
               null as rocket_height_meters,
               null as rocket_diameter_meters,
               null as rocket_mass_kg,
               null as rocket_stages,
               null as rocket_boosters,
               null as rocket_cost_per_launch,
               null as rocket_success_rate_pct,
               null as rocket_first_flight,
               null as rocket_country,
               null as rocket_company,
               null as rocket_created_at,
               null as dbt_updated_at,
               null::boolean as is_current,
               null::timestamp as valid_from,
               null::timestamp as valid_to
        where false
    {% endif %}
),

change_rocket as (
    select
        source.*,
        case
            when cur.rocket_surrogate_key is null
                then 'INSERT'
            when (
                source.rocket_name != cur.rocket_name or
                source.rocket_type != cur.rocket_type or
                source.rocket_is_active != cur.rocket_is_active or
                source.rocket_description != cur.rocket_description or
                source.rocket_height_meters != cur.rocket_height_meters or
                source.rocket_diameter_meters != cur.rocket_diameter_meters or
                source.rocket_mass_kg != cur.rocket_mass_kg or
                source.rocket_stages != cur.rocket_stages or
                source.rocket_boosters != cur.rocket_boosters or
                source.rocket_cost_per_launch != cur.rocket_cost_per_launch or
                source.rocket_success_rate_pct != cur.rocket_success_rate_pct or
                source.rocket_first_flight != cur.rocket_first_flight or
                source.rocket_country != cur.rocket_country or
                source.rocket_company != cur.rocket_company
            )
                then 'UPDATE'
            else 'NO_CHANGE'
        end as change_type

    from source_rocket as source
        left join current_rocket as cur
            on source.rocket_id = cur.rocket_id
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['rocket_id', 'rocket_name']) }} as rocket_surrogate_key,
        rocket_id,
        rocket_name,
        rocket_type,
        rocket_is_active,
        rocket_description,
        rocket_height_meters,
        rocket_diameter_meters,
        rocket_mass_kg,
        rocket_stages,
        rocket_boosters,
        rocket_cost_per_launch,
        rocket_success_rate_pct,
        rocket_first_flight,
        rocket_country,
        rocket_company,
        rocket_created_at,
        current_timestamp() as dbt_updated_at,
        True as is_current,
        current_timestamp() as valid_from,
        '9999-12-31'::timestamp as valid_to

    from change_rocket

    where change_type in ('INSERT', 'UPDATE')

    union all

    select
        rocket_surrogate_key,
        rocket_id,
        rocket_name,
        rocket_type,
        rocket_is_active,
        rocket_description,
        rocket_height_meters,
        rocket_diameter_meters,
        rocket_mass_kg,
        rocket_stages,
        rocket_boosters,
        rocket_cost_per_launch,
        rocket_success_rate_pct,
        rocket_first_flight,
        rocket_country,
        rocket_company,
        rocket_created_at,
        dbt_updated_at,
        is_current,
        valid_from,
        valid_to

    from current_rocket

    where rocket_id not in (select rocket_id from change_rocket where change_type in ('UPDATE', 'INSERT'))
)

select * from final
