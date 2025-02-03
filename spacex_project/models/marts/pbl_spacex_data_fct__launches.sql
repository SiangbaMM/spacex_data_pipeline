{{
    config(
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
        launch_id,
        count(distinct core_id) as core_count,
        sum(case when reused then 1 else 0 end) as reused_core_count,
        sum(case when landing_success then 1 else 0 end) as successful_landings
    from {{ ref('cmp_bridge__launch_cores') }}
    group by 1
),

launch_crew as (
    select
        launch_id,
        count(distinct crew_id) as crew_count
    from {{ ref('cmp_bridge__launch_crew') }}
    group by 1
),

launch_payloads as (
    select
        launch_id,
        count(distinct payload_id) as payload_count,
        sum(payload_mass_kg) as total_payload_mass_kg
    from {{ ref('cmp_bridge__launch_payloads') }}
    group by 1
),

launch_ships as (
    select
        launch_id,
        count(distinct ship_id) as ship_count
    from {{ ref('cmp_bridge__launch_ships') }}
    group by 1
),

final as (
    select
        l.launch_id,
        l.flight_number,
        l.mission_name,
        l.launch_date_utc,
        l.launch_date_local,
        l.date_precision,
        l.rocket_id,
        l.launchpad_id,
        l.is_success,
        l.mission_details,
        l.is_upcoming,
        l.static_fire_date_utc,
        l.launch_window,
        coalesce(lc.core_count, 0) as core_count,
        coalesce(lc.reused_core_count, 0) as reused_core_count,
        coalesce(lc.successful_landings, 0) as successful_landings,
        coalesce(lcr.crew_count, 0) as crew_count,
        coalesce(lp.payload_count, 0) as payload_count,
        coalesce(lp.total_payload_mass_kg, 0) as total_payload_mass_kg,
        coalesce(ls.ship_count, 0) as ship_count,
        l.sdc_extracted_at,
        current_timestamp() as dbt_loaded_at
    from launches l
    left join launch_cores lc
        on l.launch_id = lc.launch_id
    left join launch_crew lcr
        on l.launch_id = lcr.launch_id
    left join launch_payloads lp
        on l.launch_id = lp.launch_id
    left join launch_ships ls
        on l.launch_id = ls.launch_id
)

select * from final

{% if is_incremental() %}
    where sdc_extracted_at > (select max(sdc_extracted_at) from {{ this }})
{% endif %}
