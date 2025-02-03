{{
    config(
        alias='bridge_launch_ship',
        unique_key='launch_ship_id'
    )
}}

with launch_ships as (
    select
        {{ dbt_utils.generate_surrogate_key(['launch.launch_id', 'ship.ship_id']) }} as launch_ship_id,
        launch.launch_id as launch_id,
        ship.ship_id as ship_id,
        ship.ship_roles as ship_roles,
        ship.sdc_extracted_at
    from {{ ref('stg_spacex_data__launches') }} launch
        join {{ ref('stg_spacex_data__ships') }} ship
            on launch.launch_id = ship.launch_id
)

select * from launch_ships

{% if is_incremental() %}
    where sdc_extracted_at > (select max(sdc_extracted_at) from {{ this }})
{% endif %}
