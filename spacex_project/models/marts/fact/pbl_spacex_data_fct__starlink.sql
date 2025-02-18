{{
    config(
        alias='pbl_spacex_data_fct_starlink',
        unique_key='starlink_id'
    )
}}

with starlink as (
    select
        starlink_id,
        starlink_launch_id,
        starlink_satellite_version,
        starlink_height_km,
        starlink_latitude,
        starlink_longitude,
        starlink_velocity_kms,
        starlink_spacetrack,
        starlink_created_at

    from {{ ref('stg_spacex_data__starlink') }}
),

launches as (
    select
        launch_id,
        launch_date_utc,
        launch_is_success,
        launch_mission_details

    from {{ ref('stg_spacex_data__launch') }}
),

final as (
    select
        -- Keys
        s.starlink_id,
        s.starlink_launch_id,

        -- Starlink details
        s.starlink_satellite_version,
        s.starlink_height_km,
        s.starlink_latitude,
        s.starlink_longitude,
        s.starlink_velocity_kms,
        s.starlink_spacetrack,

        -- Launch details
        l.launch_date_utc,
        l.launch_is_success,
        l.launch_mission_details,

        -- Operational metrics
        case
            when s.starlink_height_km between 540 and 560 then 'Operational'
            when s.starlink_height_km < 540 then 'Below Operational'
            when s.starlink_height_km > 560 then 'Above Operational'
            else 'Unknown'
        end as orbital_status,

        case
            when s.starlink_velocity_kms between 7.5 and 7.8 then 'Nominal'
            when s.starlink_velocity_kms < 7.5 then 'Sub-nominal'
            when s.starlink_velocity_kms > 7.8 then 'Super-nominal'
            else 'Unknown'
        end as velocity_status,

        -- Metadata
        s.starlink_created_at,
        current_timestamp() as dbt_loaded_at

    from starlink s
    left join launches l on s.starlink_launch_id = l.launch_id
)

select * from final

{% if is_incremental() %}
    where starlink_created_at > (select max(starlink_created_at) from {{ this }})
{% endif %}
