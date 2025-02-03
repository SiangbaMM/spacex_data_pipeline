# SpaceX DBT Models Documentation

## Mart Models

### pbl_spacex_data_dim\_\_rockets

**Purpose**: Dimension table containing rocket specifications and characteristics.

**Materialization**: Table

```markdown
| Column Name      | Data Type | Description                        | Example    |
| ---------------- | --------- | ---------------------------------- | ---------- |
| rocket_id        | UUID      | Primary key for rocket             | uuid-1234  |
| rocket_name      | STRING    | Name of the rocket                 | Falcon 9   |
| rocket_type      | STRING    | Type/variant of rocket             | v1.2       |
| height_meters    | FLOAT     | Height of rocket in meters         | 70.0       |
| diameter_meters  | FLOAT     | Diameter of rocket in meters       | 3.7        |
| mass_kg          | FLOAT     | Mass of rocket in kilograms        | 549054.0   |
| stages           | INTEGER   | Number of stages                   | 2          |
| cost_per_launch  | FLOAT     | Base cost per launch in USD        | 62000000.0 |
| success_rate_pct | FLOAT     | Historical success rate percentage | 98.0       |
```

### pbl_spacex_data_dim\_\_dragons

**Purpose**: Dimension table for Dragon spacecraft specifications.

**Materialization**: Table

```markdown
| Column Name          | Data Type | Description                       | Example   |
| -------------------- | --------- | --------------------------------- | --------- |
| dragon_id            | UUID      | Primary key for Dragon spacecraft | uuid-1234 |
| dragon_name          | STRING    | Name of the Dragon spacecraft     | Dragon 2  |
| dragon_type          | STRING    | Type of Dragon spacecraft         | Crew      |
| crew_capacity        | INTEGER   | Maximum crew capacity             | 7         |
| orbit_duration_yr    | FLOAT     | Maximum orbit duration in years   | 2.0       |
| dry_mass_kg          | FLOAT     | Dry mass in kilograms             | 6350.0    |
| heat_shield_material | STRING    | Heat shield material type         | PICA-X    |
```

### pbl_spacex_data_dim\_\_launchpads

**Purpose**: Dimension table for launch facilities.

**Materialization**: Table

```markdown
| Column Name      | Data Type | Description                         | Example         |
| ---------------- | --------- | ----------------------------------- | --------------- |
| launchpad_id     | UUID      | Primary key for launchpad           | uuid-1234       |
| launchpad_name   | STRING    | Name of the launchpad               | SLC-40          |
| full_name        | STRING    | Full name/description               | Space Launch... |
| locality         | STRING    | Location city/area                  | Cape Canaveral  |
| region           | STRING    | Geographic region                   | Florida         |
| latitude         | FLOAT     | Latitude coordinates                | 28.5618         |
| longitude        | FLOAT     | Longitude coordinates               | -80.577         |
| launch_attempts  | INTEGER   | Total number of launch attempts     | 100             |
| launch_successes | INTEGER   | Total number of successful launches | 98              |
```

### pbl_spacex_data_fct\_\_launches

**Purpose**: Primary fact table containing launch details and metrics.

**Materialization**: Incremental

```markdown
| Column Name           | Data Type | Description                        | Example       |
| --------------------- | --------- | ---------------------------------- | ------------- |
| launch_id             | UUID      | Primary key for launch             | uuid-1234     |
| flight_number         | INTEGER   | Sequential flight number           | 100           |
| mission_name          | STRING    | Name of the mission                | Starlink-15   |
| launch_date_utc       | TIMESTAMP | Launch date/time in UTC            | 2020-10-24... |
| rocket_id             | UUID      | Reference to rocket used           | uuid-5678     |
| launchpad_id          | UUID      | Reference to launchpad used        | uuid-9012     |
| is_success            | BOOLEAN   | Whether launch was successful      | true          |
| core_count            | INTEGER   | Number of cores used               | 3             |
| reused_core_count     | INTEGER   | Number of reused cores             | 2             |
| successful_landings   | INTEGER   | Number of successful core landings | 3             |
| crew_count            | INTEGER   | Number of crew members             | 4             |
| payload_count         | INTEGER   | Number of payloads                 | 60            |
| total_payload_mass_kg | FLOAT     | Total mass of all payloads         | 15400.0       |
```

### pbl_spacex_data_fct\_\_launch_costs

**Purpose**: Fact table focusing on launch cost analysis.

**Materialization**: Incremental

```markdown
| Column Name           | Data Type | Description                            | Example    |
| --------------------- | --------- | -------------------------------------- | ---------- |
| launch_id             | UUID      | Primary key for launch                 | uuid-1234  |
| base_launch_cost      | FLOAT     | Base cost without reusability          | 62000000.0 |
| estimated_launch_cost | FLOAT     | Estimated actual cost with reusability | 43400000.0 |
| cost_per_kg           | FLOAT     | Cost per kilogram of payload           | 2818.18    |
| total_payload_mass_kg | FLOAT     | Total payload mass                     | 15400.0    |
```

## Key Metrics and Calculations

### Launch Success Rate

```sql
success_rate = SUM(CASE WHEN is_success THEN 1 ELSE 0 END) * 100.0 / COUNT(*)
```

### Core Reuse Rate

```sql
reuse_rate = SUM(reused_core_count) * 100.0 / SUM(core_count)
```

### Cost Efficiency

```sql
cost_efficiency = (base_launch_cost - estimated_launch_cost) / base_launch_cost * 100.0
```

## Data Quality Tests

### Primary Key Tests

- Unique constraints on all ID fields
- Not null constraints on all ID fields

### Foreign Key Tests

- Referential integrity checks between fact and dimension tables
- Validation of all reference fields

### Business Rule Tests

- Launch dates must be in valid range
- Payload mass must be positive when present
- Cost calculations must be positive
- Success rate percentages must be between 0 and 100

## Usage Examples

### Basic Launch Metrics

```sql
SELECT
    COUNT(*) as total_launches,
    SUM(CASE WHEN is_success THEN 1 ELSE 0 END) as successful_launches,
    AVG(total_payload_mass_kg) as avg_payload_mass
FROM {{ ref('pbl_spacex_data_fct__launches') }}
WHERE NOT is_upcoming;
```

### Cost Analysis

```sql
SELECT
    r.rocket_name,
    AVG(lc.cost_per_kg) as avg_cost_per_kg,
    SUM(lc.estimated_launch_cost) as total_launch_costs
FROM {{ ref('pbl_spacex_data_fct__launch_costs') }} lc
JOIN {{ ref('pbl_spacex_data_dim__rockets') }} r ON lc.rocket_id = r.rocket_id
GROUP BY r.rocket_name;
```

## Incremental Processing Details

### Update Strategy

- All fact tables use incremental processing
- Updates based on \_etl_loaded_at timestamp
- Full refresh triggered by dbt flags if needed

### Merge Keys

- Launch facts: launch_id
- Cost facts: launch_id
- Intermediate tables: respective composite keys
