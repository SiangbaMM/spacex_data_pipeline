{{
    config(
        alias='bridge_launch_core',
        unique_key='launch_core_id'
    )
}}

with launch_cores as (
    select
        launch_id as launch_id,
        core.value:core::string as core_id,
        core.value:flight::int as flight_number,
        core.value:gridfins::boolean as gridfins,
        core.value:legs::boolean as legs,
        core.value:reused::boolean as reused,
        core.value:landing_attempt::boolean as landing_attempt,
        core.value:landing_success::boolean as landing_success,
        core.value:landing_type::string as landing_type,
        core.value:landpad::string as landpad_id,
        sdc_extracted_at as sdc_extracted_at
    from {{ ref('stg_spacex_data__launches') }} ,
        lateral flatten(input => cores) as core
)

select 
    {{ dbt_utils.generate_surrogate_key(['launch_cores.launch_id', 'cores.core_id']) }} as launch_core_id,
    launch_cores.launch_id as launch_id,
    launch_cores.core_id as core_id,
    launch_cores.flight_number as flight_number,
    launch_cores.gridfins as gridfins,
    launch_cores.legs as legs,
    launch_cores.reused as reused,
    launch_cores.landing_attempt as landing_attempt,
    launch_cores.landing_success as landing_success,
    launch_cores.landing_type as landing_type,
    launch_cores.landpad_id as landpad_id,
    cores.sdc_extracted_at
    
from launch_cores 
    inner join {{ ref('stg_spacex_data__cores') }} cores
        on launch_cores.core_id = cores.core_id

{% if is_incremental() %}
    where sdc_extracted_at > (select max(sdc_extracted_at) from {{ this }})
{% endif %}
