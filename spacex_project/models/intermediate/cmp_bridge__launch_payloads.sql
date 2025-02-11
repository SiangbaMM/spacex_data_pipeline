{{
    config(
        alias='cmp_spacex_data_bridge_launch_payload',
        unique_key='bridge_launch_payload_launch_payload_id'
    )
}}

with launch_payloads as (
    select
        {{ dbt_utils.generate_surrogate_key(['launch.launch_id', 'payload.payload_id']) }} as bridge_launch_payload_launch_payload_id,
        launch.launch_id as bridge_launch_payload_launch_id,
        payload.payload_id as bridge_launch_payload_id,
        payload.payload_type as bridge_launch_payload_type,
        payload.payload_mass_kg as bridge_launch_payload_mass_kg,
        payload.payload_orbit as bridge_launch_payload_orbit,
        payload.payload_reference_system as bridge_launch_payload_reference_system,
        payload.payload_regime as bridge_launch_payload_regime,
        payload.payload_sdc_extracted_at as bridge_launch_payload_sdc_extracted_at

    from {{ ref('stg_spacex_data__launches') }} launch
        inner join {{ ref('stg_spacex_data__payloads') }} payload
            on launch.launch_id = payload.payload_launch_id
    
    {% if is_incremental() %}
    where payload.payload_sdc_extracted_at > (select max(bridge_launch_payload_sdc_extracted_at) from {{ this }})
    {% endif %}
)

select * from launch_payloads
