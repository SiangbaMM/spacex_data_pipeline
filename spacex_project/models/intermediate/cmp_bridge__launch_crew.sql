{{
    config(
        alias='cmp_spacex_data_bridge_launch_crew',
        unique_key='launch_crew_id'
    )
}}

with launch_crew as (
    select
        launch.launch_id as launch_id,
        crew_member.value:crew::string as crew_id,
        crew_member.value:role::string as crew_role,
        launch.launch_sdc_extracted_at
    from {{ ref('stg_spacex_data__launches') }} launch,
        lateral flatten(input => launch_crew) as crew_member

)

select
    {{ dbt_utils.generate_surrogate_key(['launch_crew.launch_id', 'crew.crew_id']) }} as bridge_launch_crew_launch_crew_id,
    launch_crew.launch_id as bridge_launch_crew_launch_id,
    crew.crew_id as bridge_launch_crew_id,
    crew.crew_name as bridge_launch_crew_name,
    launch_crew.crew_role as bridge_launch_crew_role,
    crew.crew_status as bridge_launch_crew_status,
    crew.crew_sdc_extracted_at as bridge_launch_crew_sdc_extracted_at

from launch_crew
    inner join {{ ref('stg_spacex_data__crew') }} crew
        on launch_crew.crew_id = crew.crew_id

{% if is_incremental() %}
    where crew.crew_sdc_extracted_at > (select max(bridge_launch_crew_sdc_extracted_at) from {{ this }})
{% endif %}
