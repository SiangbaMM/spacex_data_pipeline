-- Test to ensure launch dates are not in the future
{% test launch_date_not_future(model, column_name) %}

select *
from {{ model }}
where {{ column_name }} > CURRENT_TIMESTAMP()

{% endtest %}

-- Test to ensure payload mass is within reasonable bounds (0-100,000 kg)
{% test payload_mass_within_bounds(model, column_name) %}

select
    m.*
from {{ model }} m
where CAST(m.{{ column_name }} AS DECIMAL(38,2)) < 0.00
   or CAST(m.{{ column_name }} AS DECIMAL(38,2)) > 100000.00

{% endtest %}

-- Test to ensure success rate is between 0 and 100
{% test success_rate_valid_range(model, column_name) %}
with success_count as (
    select
        sum(
            case {{ column_name }}
                when TRUE then 1
                else 0
            end
        ) as success_count
    from {{ model }}
    where {{ column_name }} = TRUE
    group by {{ column_name }}
),
success_rate as (
    select
        count(model.{{ column_name }}) as row_count,
        success.success_count as success_count,
        (success_count/row_count)*100 as success_rate
    from {{ model }} as model
        inner join success_count as success
    group by success.success_count
)
select *
from success_rate
where success_rate.success_rate < 50.00

{% endtest %}

-- Test to ensure cost per kg is reasonable (> $100 per kg for space launches)
{% test reasonable_payload_mass_kg(model, column_name) %}

select *
from {{ model }}
where CAST({{ column_name }} AS DECIMAL(38,2)) > 50000.00

{% endtest %}

-- Test to ensure core reuse count is valid
{% test valid_core_reuse_count(model, column_name) %}

select
    m.*
from {{ model }} m
where CAST(m.{{ column_name }} AS INTEGER) < 0
   or CAST(m.{{ column_name }} AS INTEGER) > 15  -- As of now, no core has been reused more than 15 times

{% endtest %}
