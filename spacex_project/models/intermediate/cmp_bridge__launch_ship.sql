{{
    config(
        materialized='incremental',
        alias='cmp_spacex_data_bridge_launch_ship',
        unique_key='bridge_launch_ship_key'
    )
}}

with launch_ships as (
    select
        launch_id,
        regexp_replace(launch_ships, '\\[|\\]|"', '') as launch_ship_id,
        'booster_recovery' as launch_role,
        true as launch_is_active,  -- Assuming active by default since it's participating in a launch
        launch_created_at

    from {{ ref('stg_spacex_data__launch') }}

),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['launch_id', 'launch_ship_id', 'launch_role']) }} as bridge_launch_ship_key,
        launch_id as bridge_launch_ship_launch_id,
        launch_ship_id as bridge_launch_ship_id,
        launch_role as bridge_launch_ship_role,
        launch_is_active as bridge_launch_ship_is_active,
        launch_created_at as bridge_launch_ship_created_at

    from launch_ships
)

select * from final

{% if is_incremental() %}
where bridge_launch_ship_created_at > (select max(bridge_launch_ship_created_at) from {{ this }})
{% endif %}
