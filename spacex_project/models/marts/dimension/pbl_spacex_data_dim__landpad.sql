{%- set schema_name = generate_schema_name(this.schema) -%}

{{
    config(
        alias='pbl_spacex_data_dim_landpad',
        unique_key='landpad_surrogate_key'
    )
}}

with source_landpad as (
    select *
    from {{ ref('stg_spacex_data__landpad') }}
),

current_landpad as (
    {% if adapter.get_relation(this.database, schema_name, this.table) is not none %}
        select *
        from {{ source('pbl_spacex_data', 'pbl_spacex_data_dim_landpad') }}
        where is_current
    {% else %}
        select null as landpad_surrogate_key,
               null as landpad_id,
               null as landpad_name,
               null as landpad_full_name,
               null as landpad_status,
               null as landpad_type,
               null as landpad_locality,
               null as landpad_region,
               null as landpad_latitude,
               null as landpad_longitude,
               null as landpad_landing_attempts,
               null as landpad_landing_successes,
               null as landpad_wikipedia,
               null as landpad_details,
               null as landpad_launches_id,
               null as landpad_created_at,
               null as dbt_updated_at,
               null::boolean as is_current,
               null::timestamp as valid_from,
               null::timestamp as valid_to
        where false
    {% endif %}
),

change_landpad as (
    select
        source.*,
        case
            when cur.landpad_surrogate_key is null
                then 'INSERT'
            when (
                source.landpad_name != cur.landpad_name or
                source.landpad_full_name != cur.landpad_full_name or
                source.landpad_status != cur.landpad_status or
                source.landpad_type != cur.landpad_type or
                source.landpad_locality != cur.landpad_locality or
                source.landpad_region != cur.landpad_region or
                source.landpad_latitude != cur.landpad_latitude or
                source.landpad_longitude != cur.landpad_longitude or
                source.landpad_landing_attempts != cur.landpad_landing_attempts or
                source.landpad_landing_successes != cur.landpad_landing_successes or
                source.landpad_wikipedia != cur.landpad_wikipedia or
                source.landpad_details != cur.landpad_details
            )
                then 'UPDATE'
            else 'NO_CHANGE'
        end as change_type

    from source_landpad as source
        left join current_landpad as cur
            on source.landpad_id = cur.landpad_id
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['landpad_id', 'landpad_name']) }} as landpad_surrogate_key,
        landpad_id,
        landpad_name,
        landpad_full_name,
        landpad_status,
        landpad_type,
        landpad_locality,
        landpad_region,
        landpad_latitude,
        landpad_longitude,
        landpad_landing_attempts,
        landpad_landing_successes,
        landpad_wikipedia,
        landpad_details,
        landpad_launches as landpad_launches_id,
        landpad_created_at,
        current_timestamp() as dbt_updated_at,
        True as is_current,
        current_timestamp() as valid_from,
        '9999-12-31'::timestamp as valid_to

    from change_landpad

    where change_type in ('INSERT', 'UPDATE')

    union all

    select
        landpad_surrogate_key,
        landpad_id,
        landpad_name,
        landpad_full_name,
        landpad_status,
        landpad_type,
        landpad_locality,
        landpad_region,
        landpad_latitude,
        landpad_longitude,
        landpad_landing_attempts,
        landpad_landing_successes,
        landpad_wikipedia,
        landpad_details,
        landpad_launches_id,
        landpad_created_at,
        dbt_updated_at,
        is_current,
        valid_from,
        valid_to

    from current_landpad

    where landpad_id not in (select landpad_id from change_landpad where change_type in ('UPDATE', 'INSERT'))
)

select * from final
