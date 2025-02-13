-- Test case for launch costs calculation
with
    mock_launches as (
        select
            1 as launch_id,
            1000000 as launch_cost_usd,
            true as success,
            'F9' as rocket_type
    ),
    mock_payloads as (
        select
            1 as launch_id,
            1000 as payload_mass_kg,
            'LEO' as orbit
    ),
    -- Act: Run the same logic as the fact table
    test_calculation as (
        select
            l.launch_id,
            l.launch_cost_usd,
            p.payload_mass_kg,
            case
                when l.success = true then l.launch_cost_usd
                else 0
            end as successful_launch_cost,
            case
                when p.payload_mass_kg > 0 then round(
                    CAST(l.launch_cost_usd AS DECIMAL(38, 2)) / p.payload_mass_kg,
                    2
                )
                else 0
            end as cost_per_kg
        from
            mock_launches l
            left join mock_payloads p on l.launch_id = p.launch_id
    )
    -- Assert: Check if calculations match expected results
select
    case
        when launch_cost_usd = 1000000
        and successful_launch_cost = 1000000
        and cost_per_kg = 1000.00 then 1
    end as test_status
from
    test_calculation minus
select
    1
