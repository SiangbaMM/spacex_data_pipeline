{{
    config(
        alias='pbl_spacex_data_fct_payload',
        unique_key='payload_id'
    )
}}

with payloads as (
    select
        payload_id,
        payload_launch_id,
        payload_type,
        payload_mass_kg,
        payload_mass_lbs,
        payload_orbit,
        payload_reference_system,
        payload_regime,
        payload_customers,
        payload_nationalities,
        payload_manufacturers,
        payload_created_at

    from {{ ref('stg_spacex_data__payload') }}
),

launches as (
    select
        launch_id,
        launch_date_utc,
        launch_is_success,
        launch_mission_details

    from {{ ref('stg_spacex_data__launch') }}
),

final as (
    select
        -- Keys
        p.payload_id,
        p.payload_launch_id,

        -- Payload details
        p.payload_type,
        p.payload_mass_kg,
        p.payload_mass_lbs,
        p.payload_orbit,
        p.payload_reference_system,
        p.payload_regime,
        p.payload_customers,
        p.payload_nationalities,
        p.payload_manufacturers,

        -- Launch details
        l.launch_date_utc,
        l.launch_is_success,
        l.launch_mission_details,

        -- Metrics
        case
            when l.launch_is_success then p.payload_mass_kg
            else 0
        end as payload_successful_mass_kg,

        case
            when l.launch_is_success then 1
            else 0
        end as payload_successful_delivery,

        -- Metadata
        p.payload_created_at,
        current_timestamp() as dbt_loaded_at

    from payloads p
    left join launches l on p.payload_launch_id = l.launch_id
)

select * from final

{% if is_incremental() %}
    where payload_created_at > (select max(payload_created_at) from {{ this }})
{% endif %}
