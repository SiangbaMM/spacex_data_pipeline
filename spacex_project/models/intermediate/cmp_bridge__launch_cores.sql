{{
    config(
        alias='cmp_spacex_data_bridge_launch_core',
        unique_key='bridge_launch_core_launch_core_id'
    )
}}

with launch_cores as (
    select
        launch_id as launch_cores_launch_id,
        launch_cores.value:core::string as launch_cores_id,
        launch_cores.value:flight::int as flight_number,
        launch_cores.value:gridfins::boolean as gridfins,
        launch_cores.value:legs::boolean as legs,
        launch_cores.value:reused::boolean as reused,
        launch_cores.value:landing_attempt::boolean as landing_attempt,
        launch_cores.value:landing_success::boolean as landing_success,
        launch_cores.value:landing_type::string as landing_type,
        launch_cores.value:landpad::string as landpad_id,
        launch_sdc_extracted_at as launch_sdc_extracted_at
    from {{ ref('stg_spacex_data__launches') }} ,
        lateral flatten(input => launch_cores) as launch_cores
)

select 
    {{ dbt_utils.generate_surrogate_key(['launch_cores.launch_cores_launch_id', 'cores.core_id']) }} as bridge_launch_core_launch_core_id,
    launch_cores.launch_cores_launch_id as bridge_launch_core_launch_id,
    launch_cores.launch_cores_id as bridge_launch_core_id,
    launch_cores.flight_number as bridge_launch_core_flight_number,
    launch_cores.gridfins as bridge_launch_core_gridfins,
    launch_cores.legs as bridge_launch_core_legs,
    launch_cores.reused as bridge_launch_core_reused,
    launch_cores.landing_attempt as bridge_launch_core_landing_attempt,
    launch_cores.landing_success as bridge_launch_core_landing_success,
    launch_cores.landing_type as bridge_launch_core_landing_type,
    launch_cores.landpad_id as bridge_launch_core_landpad_id,
    launch_cores.launch_sdc_extracted_at as bridge_launch_core_sdc_extracted_at
    
from launch_cores 
    inner join {{ ref('stg_spacex_data__cores') }} cores
        on launch_cores.launch_cores_id = cores.core_id

{% if is_incremental() %}
    where launch_cores.launch_sdc_extracted_at > (select max(bridge_launch_core_sdc_extracted_at) from {{ this }})
{% endif %}
