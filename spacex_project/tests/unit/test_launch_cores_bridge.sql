-- Test case for launch cores bridge table transformation
with
    mock_launches as (
        select
            1 as launch_id,
            ARRAY_CONSTRUCT('core1', 'core2') as core_serial_numbers,
            ARRAY_CONSTRUCT(true, false) as core_reused,
            ARRAY_CONSTRUCT(true, true) as core_landing_success
    ),
    -- Act: Run the same logic as the bridge table
    test_transformation as (
        select
            l.launch_id,
            f1.value::STRING as core_serial,
            f2.value::BOOLEAN as is_reused,
            f3.value::BOOLEAN as landing_success
        from
            mock_launches l,
            table(flatten(input => l.core_serial_numbers)) f1,
            table(flatten(input => l.core_reused)) f2,
            table(flatten(input => l.core_landing_success)) f3
        where
            f1.index = f2.index
            and f2.index = f3.index
    ),
    -- Assert: Check if the unnesting works correctly
    test_assertions as (
        select
            ARRAY_SIZE(ARRAY_AGG(DISTINCT core_serial)) = 2 as correct_row_count,
            sum(case when core_serial in ('core1', 'core2') then 1 else 0 end) = 2 as valid_core_serials,
            sum(case when is_reused = true then 1 else 0 end) = 1 as correct_reuse_count,
            sum(case when landing_success = true then 1 else 0 end) = 2 as correct_landing_count
        from
            test_transformation
    )
select
    case
        when correct_row_count
        and valid_core_serials
        and correct_reuse_count
        and correct_landing_count then 1
    end as test_status
from
    test_assertions
minus
select 1
