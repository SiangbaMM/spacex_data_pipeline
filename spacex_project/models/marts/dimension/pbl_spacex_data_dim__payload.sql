{%- set schema_name = generate_schema_name(this.schema) -%}

{{
    config(
        alias='pbl_spacex_data_dim_payload',
        unique_key='payload_surrogate_key'
    )
}}

with source_payload as (
    select *
    from {{ ref('stg_spacex_data__payload') }}
),

current_payload as (
    {% if adapter.get_relation(this.database, schema_name, this.table) is not none %}
        select *
        from {{ ref('pbl_spacex_data_dim__payload') }}
        where is_current
    {% else %}
        select null as payload_surrogate_key,
               null as payload_id,
               null as payload_name,
               null as payload_type,
               null as payload_reused,
               null as payload_launch_id,
               null as payload_customers,
               null as payload_nationalities,
               null as payload_manufacturers,
               null as payload_mass_kg,
               null as payload_mass_lbs,
               null as payload_regime,
               null as payload_orbit,
               null as payload_reference_system,
               null as payload_dragon_id,
               null as payload_created_at,
               null as dbt_updated_at,
               null::boolean as is_current,
               null::timestamp as valid_from,
               null::timestamp as valid_to
        where false
    {% endif %}
),

change_payload as (
    select
        source.*,
        case
            when cur.payload_surrogate_key is null
                then 'INSERT'
            when (
                source.payload_name != cur.payload_name or
                source.payload_type != cur.payload_type or
                source.payload_reused != cur.payload_reused or
                source.payload_launch_id != cur.payload_launch_id or
                source.payload_customers != cur.payload_customers or
                source.payload_nationalities != cur.payload_nationalities or
                source.payload_manufacturers != cur.payload_manufacturers or
                source.payload_mass_kg != cur.payload_mass_kg or
                source.payload_mass_lbs != cur.payload_mass_lbs or
                source.payload_regime != cur.payload_regime or
                source.payload_orbit != cur.payload_orbit or
                source.payload_reference_system != cur.payload_reference_system or
                source.payload_dragon != cur.payload_dragon_id
            )
                then 'UPDATE'
            else 'NO_CHANGE'
        end as change_type

    from source_payload as source
        left join current_payload as cur
            on source.payload_id = cur.payload_id
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['payload_id', 'payload_name', 'payload_type']) }} as payload_surrogate_key,
        payload_id,
        payload_name,
        payload_type,
        payload_reused,
        payload_launch_id,
        payload_customers,
        payload_nationalities,
        payload_manufacturers,
        payload_mass_kg,
        payload_mass_lbs,
        payload_regime,
        payload_orbit,
        payload_reference_system,
        payload_dragon as payload_dragon_id,
        payload_created_at,
        current_timestamp() as dbt_updated_at,
        True as is_current,
        current_timestamp() as valid_from,
        '9999-12-31'::timestamp as valid_to

    from change_payload

    where change_type in ('INSERT', 'UPDATE')

    union all

    select
        payload_surrogate_key,
        payload_id,
        payload_name,
        payload_type,
        payload_reused,
        payload_launch_id,
        payload_customers,
        payload_nationalities,
        payload_manufacturers,
        payload_mass_kg,
        payload_mass_lbs,
        payload_regime,
        payload_orbit,
        payload_reference_system,
        payload_dragon_id,
        payload_created_at,
        dbt_updated_at,
        is_current,
        valid_from,
        valid_to

    from current_payload

    where payload_id not in (select payload_id from change_payload where change_type in ('UPDATE', 'INSERT'))
)

select * from final
