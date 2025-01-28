# SpaceX Data Model Documentation

## Table of Contents

1. [Data Quality Monitoring](#data-quality-monitoring)
2. [Performance Monitoring](#performance-monitoring)
3. [Alerting System](#alerting-system)
4. [Processing Model](#processing-model)

## Monitoring and Quality Control

### Data Quality Monitoring

#### Quality Metrics

```sql
CREATE OR REPLACE VIEW VW_DATA_QUALITY_METRICS AS
SELECT
    current_date as check_date,

    -- Completeness
    (SELECT COUNT(*)
     FROM FACT_LAUNCHES
     WHERE ROCKET_ID IS NULL) as missing_rocket_count,

    -- Consistency
    (SELECT COUNT(*)
     FROM FACT_LAUNCHES l
     JOIN DIM_ROCKETS r ON l.ROCKET_ID = r.ROCKET_ID
     WHERE l.DATE_UTC < r.FIRST_FLIGHT) as invalid_date_count,

    -- Accuracy
    (SELECT COUNT(*)
     FROM FACT_PAYLOADS
     WHERE MASS_KG < 0
     OR MASS_KG > 1000000) as suspicious_mass_count,

    -- Timeliness
    (SELECT AVG(DATEDIFF('minute', CREATED_AT, CURRENT_TIMESTAMP))
     FROM RAW_LAUNCHES) as avg_processing_delay
;
```

### Performance Monitoring

#### Query Performance

```sql
CREATE OR REPLACE VIEW VW_QUERY_PERFORMANCE AS
SELECT
    QUERY_ID,
    USER_NAME,
    WAREHOUSE_NAME,
    DATABASE_NAME,
    SCHEMA_NAME,
    QUERY_TEXT,
    TOTAL_ELAPSED_TIME,
    BYTES_SCANNED,
    ROWS_PRODUCED,
    EXECUTION_STATUS,
    ERROR_MESSAGE,
    START_TIME,
    END_TIME
FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY())
WHERE START_TIME >= DATEADD(day, -1, CURRENT_TIMESTAMP())
ORDER BY START_TIME DESC;
```

#### Load Monitoring

```sql
CREATE OR REPLACE VIEW VW_LOAD_MONITORING AS
WITH load_stats AS (
    SELECT
        TABLE_NAME,
        COUNT(*) as record_count,
        MIN(CREATED_AT) as first_record,
        MAX(CREATED_AT) as last_record,
        COUNT(DISTINCT CREATED_AT) as batch_count
    FROM SPACEX_STAGING.LOAD_AUDIT
    WHERE CREATED_AT >= DATEADD(day, -1, CURRENT_TIMESTAMP())
    GROUP BY TABLE_NAME
)
SELECT
    ls.*,
    e.error_count,
    e.last_error_time,
    e.last_error_message
FROM load_stats ls
LEFT JOIN (
    SELECT
        TABLE_NAME,
        COUNT(*) as error_count,
        MAX(ERROR_TIME) as last_error_time,
        LAST_VALUE(ERROR_MESSAGE) OVER (
            PARTITION BY TABLE_NAME
            ORDER BY ERROR_TIME
        ) as last_error_message
    FROM SPACEX_STAGING.LOAD_ERRORS
    WHERE ERROR_TIME >= DATEADD(day, -1, CURRENT_TIMESTAMP())
    GROUP BY TABLE_NAME
) e ON ls.TABLE_NAME = e.TABLE_NAME;
```

### Alerting System

#### Alert Definitions

```sql
CREATE OR REPLACE PROCEDURE SET_DATA_QUALITY_ALERTS()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    alert_message STRING;
BEGIN
    -- Check for data completeness
    INSERT INTO ALERT_LOG (ALERT_TYPE, ALERT_MESSAGE, SEVERITY)
    SELECT
        'DATA_COMPLETENESS',
        'Missing rocket references: ' || COUNT(*),
        'HIGH'
    FROM FACT_LAUNCHES
    WHERE ROCKET_ID IS NULL;

    -- Check for data freshness
    INSERT INTO ALERT_LOG (ALERT_TYPE, ALERT_MESSAGE, SEVERITY)
    SELECT
        'DATA_FRESHNESS',
        'No new launches loaded in last 24 hours',
        'MEDIUM'
    WHERE NOT EXISTS (
        SELECT 1
        FROM FACT_LAUNCHES
        WHERE CREATED_AT >= DATEADD(hour, -24, CURRENT_TIMESTAMP())
    );

    -- Return alert summary
    SELECT LISTAGG(ALERT_TYPE || ': ' || ALERT_MESSAGE, '; ')
    INTO alert_message
    FROM ALERT_LOG
    WHERE CREATED_AT >= DATEADD(minute, -5, CURRENT_TIMESTAMP());

    RETURN COALESCE(alert_message, 'No new alerts');
END;
$$;
```
