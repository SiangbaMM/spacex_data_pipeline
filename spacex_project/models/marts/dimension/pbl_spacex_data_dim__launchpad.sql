{%- set schema_name = generate_schema_name(this.schema) -%}

{{
    config(
        alias='pbl_spacex_data_dim_launchpad',
        unique_key='launchpad_surrogate_key'
    )
}}

with source_launchpad as (
    select *
    from {{ ref('stg_spacex_data__launchpad') }}
),

current_launchpad as (
    {% if adapter.get_relation(this.database, schema_name, this.table) is not none %}
        select *
        from {{ ref('pbl_spacex_data_dim__launchpad') }}
        where is_current
    {% else %}
        select null as launchpad_surrogate_key,
               null as launchpad_id,
               null as launchpad_name,
               null as launchpad_full_name,
               null as launchpad_locality,
               null as launchpad_region,
               null as launchpad_latitude,
               null as launchpad_longitude,
               null as launchpad_launch_attempts,
               null as launchpad_launch_successes,
               null as launchpad_status,
               null as launchpad_created_at,
               null as dbt_updated_at,
               null::boolean as is_current,
               null::timestamp as valid_from,
               null::timestamp as valid_to
        where false
    {% endif %}
),

change_launchpad as (
    select
        source.*,
        case
            when cur.launchpad_surrogate_key is null
                then 'INSERT'
            when (
                source.launchpad_name != cur.launchpad_name or
                source.launchpad_full_name != cur.launchpad_full_name or
                source.launchpad_locality != cur.launchpad_locality or
                source.launchpad_region != cur.launchpad_region or
                source.launchpad_latitude != cur.launchpad_latitude or
                source.launchpad_longitude != cur.launchpad_longitude or
                source.launchpad_launch_attempts != cur.launchpad_launch_attempts or
                source.launchpad_launch_successes != cur.launchpad_launch_successes or
                source.launchpad_status != cur.launchpad_status
            )
                then 'UPDATE'
            else 'NO_CHANGE'
        end as change_type

    from source_launchpad as source
        left join current_launchpad as cur
            on source.launchpad_id = cur.launchpad_id
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['launchpad_id', 'launchpad_name']) }} as launchpad_surrogate_key,
        launchpad_id,
        launchpad_name,
        launchpad_full_name,
        launchpad_locality,
        launchpad_region,
        launchpad_latitude,
        launchpad_longitude,
        launchpad_launch_attempts,
        launchpad_launch_successes,
        launchpad_status,
        launchpad_created_at,
        current_timestamp() as dbt_updated_at,
        True as is_current,
        current_timestamp() as valid_from,
        '9999-12-31'::timestamp as valid_to

    from change_launchpad

    where change_type in ('INSERT', 'UPDATE')

    union all

    select
        launchpad_surrogate_key,
        launchpad_id,
        launchpad_name,
        launchpad_full_name,
        launchpad_locality,
        launchpad_region,
        launchpad_latitude,
        launchpad_longitude,
        launchpad_launch_attempts,
        launchpad_launch_successes,
        launchpad_status,
        launchpad_created_at,
        dbt_updated_at,
        is_current,
        valid_from,
        valid_to

    from current_launchpad

    where launchpad_id not in (select launchpad_id from change_launchpad where change_type in ('UPDATE', 'INSERT'))
)

select * from final
