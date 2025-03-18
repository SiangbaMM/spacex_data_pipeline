{%- set schema_name = generate_schema_name(this.schema) -%}

{{
    config(
        alias='pbl_spacex_data_dim_dragon',
        unique_key='dragon_surrogate_key'
    )
}}

with source_dragon as (
    select *
    from {{ ref('stg_spacex_data__dragon') }}
),

current_dragon as (
    {% if adapter.get_relation(this.database, schema_name, this.table) is not none %}
        select *
        from {{ source('pbl_spacex_data', 'pbl_spacex_data_dim_dragon') }}
        where is_current
    {% else %}
        select null as dragon_surrogate_key,
               null as dragon_id,
               null as dragon_name,
               null as dragon_type,
               null as dragon_crew_capacity,
               null as dragon_orbit_duration_yr,
               null as dragon_dry_mass_kg,
               null as dragon_first_flight_at,
               null as dragon_heat_shield_material,
               null as dragon_heat_shield_size_meters,
               null as dragon_heat_shield_temp_degrees,
               null as dragon_thrusters_number,
               null as dragon_created_at,
               null as dbt_updated_at,
               null::boolean as is_current,
               null::timestamp as valid_from,
               null::timestamp as valid_to
        where false
    {% endif %}
),

change_dragon as (
    select
        source.*,
        case
            when cur.dragon_surrogate_key is null
                then 'INSERT'
            when (
                source.dragon_name != cur.dragon_name or
                source.dragon_type != cur.dragon_type or
                source.dragon_crew_capacity != cur.dragon_crew_capacity or
                source.dragon_orbit_duration_yr != cur.dragon_orbit_duration_yr or
                source.dragon_dry_mass_kg != cur.dragon_dry_mass_kg or
                source.dragon_first_flight != cur.dragon_first_flight_at or
                source.dragon_heat_shield:material::string != cur.dragon_heat_shield_material or
                source.dragon_heat_shield:size_meters::float != cur.dragon_heat_shield_size_meters or
                source.dragon_heat_shield:temp_degrees::float != cur.dragon_heat_shield_temp_degrees or
                source.dragon_thrusters_number != cur.dragon_thrusters_number
            )
                then 'UPDATE'
            else 'NO_CHANGE'
        end as change_type

    from source_dragon as source
        left join current_dragon as cur
            on source.dragon_id = cur.dragon_id
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['dragon_id', 'dragon_name']) }} as dragon_surrogate_key,
        dragon_id,
        dragon_name,
        dragon_type,
        dragon_crew_capacity,
        dragon_orbit_duration_yr,
        dragon_dry_mass_kg,
        dragon_first_flight as dragon_first_flight_at,
        dragon_heat_shield:material::string as dragon_heat_shield_material,
        dragon_heat_shield:size_meters::float as dragon_heat_shield_size_meters,
        dragon_heat_shield:temp_degrees::float as dragon_heat_shield_temp_degrees,
        dragon_thrusters_number,
        dragon_created_at,
        current_timestamp() as dbt_updated_at,
        True as is_current,
        current_timestamp() as valid_from,
        '9999-12-31'::timestamp as valid_to

    from change_dragon
    where change_type in ('INSERT', 'UPDATE')

    union all

    select
        dragon_surrogate_key,
        dragon_id,
        dragon_name,
        dragon_type,
        dragon_crew_capacity,
        dragon_orbit_duration_yr,
        dragon_dry_mass_kg,
        dragon_first_flight_at,
        dragon_heat_shield_material,
        dragon_heat_shield_size_meters,
        dragon_heat_shield_temp_degrees,
        dragon_thrusters_number,
        dragon_created_at,
        dbt_updated_at,
        is_current,
        valid_from,
        valid_to

    from current_dragon

    where dragon_id not in (select dragon_id from change_dragon where change_type in ('UPDATE', 'INSERT'))
)

select * from final
