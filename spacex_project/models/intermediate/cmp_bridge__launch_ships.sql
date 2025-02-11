{{
    config(
        alias='cmp_spacex_data_bridge_launch_ship',
        unique_key='bridge_launch_ship_launch_ship_id'
    )
}}

with launch_ships as (
    select
        {{ dbt_utils.generate_surrogate_key(['launch.launch_id', 'ship.ship_id']) }} as bridge_launch_ship_launch_ship_id,
        launch.launch_id as bridge_launch_ship_launch_id,
        ship.ship_id as bridge_launch_ship_id,
        ship.ship_roles as bridge_launch_ship_roles,
        ship.ship_sdc_extracted_at as bridge_launch_ship_sdc_extracted_at
    from {{ ref('stg_spacex_data__launches') }} launch
        join {{ ref('stg_spacex_data__ships') }} ship
            on launch.launch_id = ship.ship_launch_id
)

select * from launch_ships

{% if is_incremental() %}
    where bridge_launch_ship_sdc_extracted_at > (select max(bridge_launch_ship_sdc_extracted_at) from {{ this }})
{% endif %}
