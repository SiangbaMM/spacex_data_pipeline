# SpaceX Data Column Lineage and Quality Rules

## Column Lineage

### Launch Cost Fact Table

#### pbl_spacex_data_fct\_\_launch_costs

| Target Column         | Source                                        | Transformation Logic                    | Data Quality Rules        |
| --------------------- | --------------------------------------------- | --------------------------------------- | ------------------------- |
| launch_id             | stg_spacex_data\_\_launches.launch_id         | Direct copy                             | Not null, Unique          |
| launch_date_utc       | stg_spacex_data\_\_launches.launch_date_utc   | Direct copy                             | Not null, Not future date |
| rocket_id             | stg_spacex_data\_\_launches.rocket_id         | Direct copy                             | Not null, FK to rockets   |
| success               | stg_spacex_data\_\_launches.success           | Direct copy                             | Not null                  |
| total_payload_mass_kg | cmp_bridge\_\_launch_payloads.payload_mass_kg | SUM(payload_mass_kg)                    | > 0                       |
| launch_cost_usd       | stg_spacex_data\_\_launches.launch_cost_usd   | Direct copy                             | > 0                       |
| cost_per_kg           | Calculated                                    | launch_cost_usd / total_payload_mass_kg | > 100                     |
| payload_count         | cmp_bridge\_\_launch_payloads                 | COUNT(DISTINCT payload_id)              | >= 0                      |
| crew_count            | cmp_bridge\_\_launch_crew                     | COUNT(DISTINCT crew_member_id)          | >= 0                      |

### Bridge Tables

#### cmp_bridge\_\_launch_cores

| Target Column                | Source                                            | Transformation Logic | Data Quality Rules       |
| ---------------------------- | ------------------------------------------------- | -------------------- | ------------------------ |
| bridge_launch_core_id        | Generated                                         | UUID                 | Not null, Unique         |
| bridge_launch_core_launch_id | stg_spacex_data\_\_launches.launch_id             | From array flatten   | Not null, FK to launches |
| bridge_launch_core_serial    | stg_spacex_data\_\_launches.core_serial_numbers   | From array flatten   | Not null                 |
| landing_success              | stg_spacex_data\_\_launches.cores_landing_success | From array flatten   | Boolean                  |

#### cmp_bridge\_\_launch_payloads

| Target Column                   | Source                                  | Transformation Logic | Data Quality Rules       |
| ------------------------------- | --------------------------------------- | -------------------- | ------------------------ |
| bridge_launch_payload_id        | Generated                               | UUID                 | Not null, Unique         |
| bridge_launch_payload_launch_id | stg_spacex_data\_\_launches.launch_id   | From array flatten   | Not null, FK to launches |
| bridge_launch_payload_id        | stg_spacex_data\_\_launches.payload_ids | From array flatten   | Not null                 |
| payload_mass_kg                 | stg_spacex_data\_\_payloads.mass_kg     | Direct copy          | Between 0-100000         |

### Dimension Tables (SCD Type 2)

Common columns for all SCD Type 2 dimension tables:

| Target Column  | Source    | Transformation Logic                | Data Quality Rules                 |
| -------------- | --------- | ----------------------------------- | ---------------------------------- |
| surrogate_key  | Generated | dbt_utils.generate_surrogate_key()  | Not null, Unique                   |
| natural_key    | Source ID | Direct copy                         | Not null                           |
| valid_from     | Generated | current_timestamp()                 | Not null, <= current_timestamp     |
| valid_to       | Generated | '9999-12-31' or current_timestamp() | > valid_from                       |
| is_current     | Generated | True/False based on validity        | Not null, One true per natural_key |
| dbt_updated_at | Generated | current_timestamp()                 | Not null                           |

#### pbl_spacex_data_dim\_\_capsules

| Target Column          | Source                             | Transformation Logic | Data Quality Rules |
| ---------------------- | ---------------------------------- | -------------------- | ------------------ |
| capsule_id             | stg_spacex_data\_\_capsules.id     | Direct copy          | Not null           |
| capsule_serial         | stg_spacex_data\_\_capsules.serial | Direct copy          | Not null           |
| capsule_status         | stg_spacex_data\_\_capsules.status | Direct copy          | In valid list      |
| capsule_reuse_count    | stg_spacex_data\_\_capsules.reuse  | Direct copy          | >= 0               |
| capsule_water_landings | stg_spacex_data\_\_capsules.water  | Direct copy          | >= 0               |
| capsule_land_landings  | stg_spacex_data\_\_capsules.land   | Direct copy          | >= 0               |

#### pbl_spacex_data_dim\_\_cores

| Target Column      | Source                             | Transformation Logic | Data Quality Rules |
| ------------------ | ---------------------------------- | -------------------- | ------------------ |
| core_id            | stg_spacex_data\_\_cores.id        | Direct copy          | Not null           |
| core_serial        | stg_spacex_data\_\_cores.serial    | Direct copy          | Not null           |
| core_block         | stg_spacex_data\_\_cores.block     | Direct copy          | Not null           |
| core_status        | stg_spacex_data\_\_cores.status    | Direct copy          | In valid list      |
| core_reuse_count   | stg_spacex_data\_\_cores.reuse     | Direct copy          | >= 0               |
| core_rtls_attempts | stg_spacex_data\_\_cores.rtls      | Direct copy          | >= 0               |
| core_rtls_landings | stg_spacex_data\_\_cores.rtls_land | Direct copy          | >= rtls_attempts   |
| core_asds_attempts | stg_spacex_data\_\_cores.asds      | Direct copy          | >= 0               |
| core_asds_landings | stg_spacex_data\_\_cores.asds_land | Direct copy          | >= asds_attempts   |

[Additional dimension tables omitted for brevity - the pattern continues for all 13 dimension tables]

## Data Quality Tests

### Generic Tests

```yaml
version: 2

# Common tests for all SCD Type 2 dimension tables
models:
  - name: pbl_spacex_data_dim__capsules
    columns: &scd_type_2_tests
      - name: surrogate_key
        tests:
          - unique
          - not_null
      - name: natural_key
        tests:
          - not_null
      - name: valid_from
        tests:
          - not_null
          - valid_temporal_range
      - name: valid_to
        tests:
          - not_null
          - valid_temporal_range
      - name: is_current
        tests:
          - not_null
          - one_current_per_key

  - name: pbl_spacex_data_dim__cores
    columns: *scd_type_2_tests

  - name: pbl_spacex_data_dim__crew
    columns: *scd_type_2_tests

  # Additional dimension tables follow same pattern

  - name: pbl_spacex_data_fct__launch_costs
    columns:
      - name: launch_id
        tests:
          - unique
          - not_null
          - relationships:
              to: ref('stg_spacex_data__launches')
              field: launch_id
      - name: cost_per_kg
        tests:
          - reasonable_cost_per_kg
```

### Custom Tests

```sql
-- Test to ensure valid temporal range
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

{% test valid_temporal_range(model, column_name, surrogate_key) %}
with validation as (
    select
        *,
        lag(valid_to) over (partition by surrogate_key order by valid_from) as prev_valid_to
    from {{ model }}
)
select *
from validation
where valid_from >= valid_to
   or (prev_valid_to is not null and valid_from != prev_valid_to)
{% endtest %}

-- Test to ensure cost per kg is reasonable
{% test reasonable_cost_per_kg(model, column_name) %}
select
    m.*
from {{ model }} m
where CAST(m.{{ column_name }} AS DECIMAL(38,2)) < 100.00
{% endtest %}

```

## Data Quality Monitoring

### Volume Monitoring

```sql
-- Monitor daily record counts
SELECT
    DATE_TRUNC('day', launch_date_utc) as launch_date,
    COUNT(*) as launch_count,
    SUM(CASE WHEN success THEN 1 ELSE 0 END) as successful_launches
FROM pbl_spacex_data_fct__launch_costs
GROUP BY 1
ORDER BY 1 DESC;
```

### Data Freshness

```sql
-- Check data freshness
SELECT
    MAX(launch_date_utc) as latest_launch,
    DATEDIFF('hour', MAX(launch_date_utc), CURRENT_TIMESTAMP()) as hours_since_update
FROM stg_spacex_data__launches;
```

### Referential Integrity

```sql
-- Verify all rockets exist
SELECT
    l.launch_id,
    l.rocket_id
FROM pbl_spacex_data_fct__launch_costs l
LEFT JOIN pbl_spacex_data_dim__rockets r ON l.rocket_id = r.rocket_id
WHERE r.rocket_id IS NULL;
```

## Error Handling

### Loading Errors

- Log to STG_SPACEX_DATA_LOAD_ERRORS table
- Include error message, timestamp, and affected records
- Alert on error thresholds
