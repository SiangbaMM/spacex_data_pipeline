{{
    config(
        alias='pbl_spacex_data_fct_launch_costs',
        materialized='incremental',
        unique_key='launch_id'
    )
}}

with launches as (
    select *
    from {{ ref('stg_spacex_data__launches') }}
),

launch_cores as (
    select
        bridge_launch_core_launch_id,
        count(distinct bridge_launch_core_id) as bridge_launch_core_count,
        sum(case when bridge_launch_core_reused then 1 else 0 end) as bridge_launch_core_reused_count,
        sum(case when bridge_launch_core_landing_success then 1 else 0 end) as bridge_launch_core_successful_landings
    from {{ ref('cmp_bridge__launch_cores') }}
    group by 1
),

launch_crew as (
    select
        bridge_launch_crew_launch_id,
        count(distinct bridge_launch_crew_id) as bridge_launch_crew_count
    from {{ ref('cmp_bridge__launch_crew') }}
    group by 1
),

launch_payloads as (
    select
        bridge_launch_payload_launch_id,
        count(distinct bridge_launch_payload_id) as bridge_launch_payload_count,
        sum(bridge_launch_payload_mass_kg) as total_bridge_launch_payload_mass_kg
    from {{ ref('cmp_bridge__launch_payloads') }}
    group by 1
),

launch_ships as (
    select
        bridge_launch_ship_launch_id,
        count(distinct bridge_launch_ship_id) as bridge_launch_ship_count
    from {{ ref('cmp_bridge__launch_ships') }}
    group by 1
),

final as (
    select
        launches.launch_id,
        launches.launch_flight_number,
        launches.launch_mission_name,
        launches.launch_date_utc,
        launches.launch_date_local,
        launches.launch_date_precision,
        launches.launch_rocket_id,
        launches.launch_launchpad_id,
        launches.launch_is_success,
        launches.launch_mission_details,
        launches.launch_is_upcoming,
        launches.launch_static_fire_date_utc,
        launches.launch_window,
        coalesce(launch_cores.bridge_launch_core_count, 0) as core_count,
        coalesce(launch_cores.bridge_launch_core_reused_count, 0) as reused_core_count,
        coalesce(launch_cores.bridge_launch_core_successful_landings, 0) as successful_landings,
        coalesce(launch_crew.bridge_launch_crew_count, 0) as crew_count,
        coalesce(launch_payloads.bridge_launch_payload_count, 0) as payload_count,
        coalesce(launch_payloads.total_bridge_launch_payload_mass_kg, 0) as total_payload_mass_kg,
        coalesce(launch_ships.bridge_launch_ship_count, 0) as ship_count,
        launches.launch_sdc_extracted_at as launch_sdc_extracted_at,
        current_timestamp() as dbt_loaded_at
        
    from launches
    left join launch_cores 
        on launches.launch_id = launch_cores.bridge_launch_core_launch_id
    left join launch_crew 
        on launches.launch_id = launch_crew.bridge_launch_crew_launch_id
    left join launch_payloads
        on launches.launch_id = launch_payloads.bridge_launch_payload_launch_id
    left join launch_ships
        on launches.launch_id = launch_ships.bridge_launch_ship_launch_id
)

select * from final

{% if is_incremental() %}
    where launches.launch_sdc_extracted_at > (select max(launch_sdc_extracted_at) from {{ this }})
{% endif %}
