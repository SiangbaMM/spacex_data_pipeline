# SpaceX Data Model Documentation

## Table of Contents

1. [Storage Specifications](#storage-specifications)
2. [Performance Optimizations](#performance-optimizations)
3. [Physical Data Model](#data-types-and-constraints)
4. [Processing Model](#processing-model)

## Technical Specifications

### Storage Specifications

#### Staging Layer

- Format: Internal Snowflake tables
- Retention: 7 days rolling
- Compression: Default Snowflake compression
- Clustering Keys:
  ```sql
  ALTER TABLE RAW_LAUNCHES CLUSTER BY (DATE_UTC, ROCKET);
  ALTER TABLE RAW_PAYLOADS CLUSTER BY (LAUNCH);
  ```

#### Warehouse Layer

- Format: Snowflake tables with Time Travel enabled
- Retention: 90 days for Time Travel
- Data Protection: Zero-copy cloning enabled
- Micro-partitions: Automated by Snowflake

### Performance Optimizations

#### Clustering Strategy

```sql
-- Launches fact table clustering
ALTER TABLE FACT_LAUNCHES
CLUSTER BY (DATE_ID, ROCKET_ID);

-- Payloads fact table clustering
ALTER TABLE FACT_PAYLOADS
CLUSTER BY (LAUNCH_ID);

-- Starlink fact table clustering
ALTER TABLE FACT_STARLINK
CLUSTER BY (LAUNCH_ID, LATITUDE, LONGITUDE);
```

#### Materialized Views

```sql
-- Launch success rates by rocket
CREATE MATERIALIZED VIEW MV_LAUNCH_SUCCESS AS
SELECT
    r.NAME AS rocket_name,
    COUNT(*) as total_launches,
    SUM(CASE WHEN l.SUCCESS = TRUE THEN 1 ELSE 0 END) as successful_launches,
    (successful_launches / total_launches * 100)::NUMERIC(5,2) as success_rate
FROM FACT_LAUNCHES l
JOIN DIM_ROCKETS r ON l.ROCKET_ID = r.ROCKET_ID
GROUP BY r.NAME;

-- Active satellites by version
CREATE MATERIALIZED VIEW MV_ACTIVE_STARLINK AS
SELECT
    VERSION,
    COUNT(*) as satellite_count,
    AVG(HEIGHT_KM) as avg_height
FROM FACT_STARLINK
WHERE OPERATIONAL = TRUE
GROUP BY VERSION;
```

### Data Types and Constraints

#### Standard Column Specifications

```sql
-- Primary Keys
[table_name]_ID        INT IDENTITY(1,1)

-- Foreign Keys
[dimension_name]_ID    INT NOT NULL

-- Dates and Times
[event_name]_DATE       DATE
[event_name]_TIMESTAMP  TIMESTAMP_NTZ(9)

-- Measures
[measure_name]_QTY      NUMBER(38,0)
[measure_name]_AMT      NUMBER(38,2)
PERCENTAGE              NUMBER(5,2)

-- Text
NAME                   VARCHAR(100)
DESCRIPTION            VARCHAR(1000)
URL                    VARCHAR(500)
```

#### Variant Data Handling

```sql
-- JSON Parsing Example
CREATE OR REPLACE FUNCTION PARSE_LAUNCH_FAILURES(failures VARIANT)
RETURNS TABLE (
    failure_time INT,
    failure_altitude INT,
    failure_reason STRING
) AS
$$
    SELECT
        f:time::INT as failure_time,
        f:altitude::INT as failure_altitude,
        f:reason::STRING as failure_reason
    FROM TABLE(FLATTEN(input => failures))
$$;
```
