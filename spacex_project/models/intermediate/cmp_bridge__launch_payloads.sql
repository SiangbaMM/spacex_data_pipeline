{{
    config(
        alias='bridge_launch_payload',
        unique_key='launch_payload_id'
    )
}}

with launch_payloads as (
    select
        {{ dbt_utils.generate_surrogate_key(['launch.launch_id', 'payload.payload_id']) }} as launch_payload_id,
        launch.launch_id as launch_id,
        payload.payload_id as payload_id,
        payload.payload_type as payload_type,
        payload.payload_mass_kg as payload_mass_kg,
        payload.orbit as payload_orbit,
        payload.reference_system as reference_system,
        payload.regime as payload_regime,
        payload.sdc_extracted_at as sdc_extracted_at

    from {{ ref('stg_spacex_data__launches') }} launch
        inner join {{ ref('stg_spacex_data__payloads') }} payload
            on launch.launch_id = payload.launch_id
)

select * from launch_payloads

{% if is_incremental() %}
    where sdc_extracted_at > (select max(sdc_extracted_at) from {{ this }})
{% endif %}
