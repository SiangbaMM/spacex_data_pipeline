{%- set schema_name = generate_schema_name(this.schema) -%}

{{
    config(
        materialized='table',
        alias='pbl_spacex_data_dim_starlink',
        unique_key='starlink_surrogate_key'
    )
}}

with source_starlink as (
    select *
    from {{ ref('stg_spacex_data__starlink') }}
),

current_starlink as (
    {% if adapter.get_relation(this.database, schema_name, this.table) is not none %}
        select *
        from {{ source('pbl_spacex_data', 'pbl_spacex_data_dim_starlink') }}
        where is_current
    {% else %}
        select null as starlink_surrogate_key,
               null as starlink_id,
               null as starlink_launch_id,
               null as starlink_longitude,
               null as starlink_latitude,
               null as starlink_height_km,
               null as starlink_velocity_kms,
               null as starlink_spaceTrack_object_name,
               null as starlink_spaceTrack_latitude,
               null as starlink_spaceTrack_longitude,
               null as starlink_spaceTrack_height_km,
               null as starlink_spaceTrack_velocity_kms,
               null as starlink_created_at,
               null as dbt_updated_at,
               null::boolean as is_current,
               null::timestamp as valid_from,
               null::timestamp as valid_to
        where false
    {% endif %}
),

change_starlink as (
    select
        source.*,
        case
            when cur.starlink_surrogate_key is null
                then 'INSERT'
            when (
                source.starlink_launch_id != cur.starlink_launch_id or
                source.starlink_longitude != cur.starlink_longitude or
                source.starlink_latitude != cur.starlink_latitude or
                source.starlink_height_km != cur.starlink_height_km or
                source.starlink_velocity_kms != cur.starlink_velocity_kms or
                source.starlink_spaceTrack:OBJECT_NAME::string != cur.starlink_spaceTrack_object_name or
                source.starlink_spaceTrack:LATITUDE::float != cur.starlink_spaceTrack_latitude or
                source.starlink_spaceTrack:LONGITUDE::float != cur.starlink_spaceTrack_longitude or
                source.starlink_spaceTrack:HEIGHT_KM::float != cur.starlink_spaceTrack_height_km or
                source.starlink_spaceTrack:VELOCITY_KMS::float != cur.starlink_spaceTrack_velocity_kms
            )
                then 'UPDATE'
            else 'NO_CHANGE'
        end as change_type

    from source_starlink as source
        left join current_starlink as cur
            on source.starlink_id = cur.starlink_id
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['starlink_id', 'starlink_launch_id']) }} as starlink_surrogate_key,
        starlink_id,
        starlink_launch_id,
        starlink_longitude,
        starlink_latitude,
        starlink_height_km,
        starlink_velocity_kms,
        starlink_spaceTrack:OBJECT_NAME::string as starlink_spaceTrack_object_name,
        starlink_spaceTrack:LATITUDE::float as starlink_spaceTrack_latitude,
        starlink_spaceTrack:LONGITUDE::float as starlink_spaceTrack_longitude,
        starlink_spaceTrack:HEIGHT_KM::float as starlink_spaceTrack_height_km,
        starlink_spaceTrack:VELOCITY_KMS::float as starlink_spaceTrack_velocity_kms,
        starlink_created_at,
        current_timestamp() as dbt_updated_at,
        True as is_current,
        current_timestamp() as valid_from,
        '9999-12-31'::timestamp as valid_to

    from change_starlink

    where change_type in ('INSERT', 'UPDATE')

    union all

    select
        starlink_surrogate_key,
        starlink_id,
        starlink_launch_id,
        starlink_longitude,
        starlink_latitude,
        starlink_height_km,
        starlink_velocity_kms,
        starlink_spaceTrack_object_name,
        starlink_spaceTrack_latitude,
        starlink_spaceTrack_longitude,
        starlink_spaceTrack_height_km,
        starlink_spaceTrack_velocity_kms,
        starlink_created_at,
        dbt_updated_at,
        is_current,
        valid_from,
        valid_to

    from current_starlink

    where starlink_id not in (select starlink_id from change_starlink where change_type in ('UPDATE', 'INSERT'))
)

select * from final
