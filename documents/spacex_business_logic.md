# SpaceX Data Model Documentation

## Table of Contents

1. [Data Quality Rules](#data-quality-rules)
2. [Business Logic Rules](#business-logic-rules)

## Business Rules and Constraints

### Data Quality Rules

#### Entity Integrity Rules

1. **Launches**

   ```sql
   -- All launches must have a rocket
   ALTER TABLE FACT_LAUNCHES
   ADD CONSTRAINT CHK_LAUNCH_ROCKET
   CHECK (ROCKET_ID IS NOT NULL);

   -- Launch dates must be after company founding
   ALTER TABLE FACT_LAUNCHES
   ADD CONSTRAINT CHK_LAUNCH_DATE
   CHECK (DATE_UTC >= '2002-03-14');
   ```

2. **Payloads**

   ```sql
   -- Mass must be positive if specified
   ALTER TABLE FACT_PAYLOADS
   ADD CONSTRAINT CHK_PAYLOAD_MASS
   CHECK (MASS_KG IS NULL OR MASS_KG > 0);

   -- Orbit parameters validation
   ALTER TABLE FACT_PAYLOADS
   ADD CONSTRAINT CHK_ORBIT_PARAMS
   CHECK (
       (ORBIT IN ('LEO', 'ISS', 'PO', 'GTO', 'ES-L1', 'SSO', 'HEO'))
       AND
       (PERIAPSIS_KM IS NULL OR PERIAPSIS_KM > 0)
       AND
       (APOAPSIS_KM IS NULL OR APOAPSIS_KM > PERIAPSIS_KM)
   );
   ```

3. **Cores**
   ```sql
   -- Reuse count validation
   ALTER TABLE DIM_CORES
   ADD CONSTRAINT CHK_CORE_REUSE
   CHECK (
       REUSE_COUNT >= 0
       AND
       REUSE_COUNT = (RTLS_LANDINGS + ASDS_LANDINGS)
   );
   ```

### Business Logic Rules

#### Status Transitions

```sql
-- Valid status transitions for cores
CREATE TABLE VALID_CORE_STATUS_TRANSITIONS (
    current_status STRING,
    next_status STRING,
    CONSTRAINT PK_STATUS_TRANSITION PRIMARY KEY (current_status, next_status)
);

INSERT INTO VALID_CORE_STATUS_TRANSITIONS VALUES
    ('active', 'retired'),
    ('active', 'lost'),
    ('lost', 'retired'),
    ('unknown', 'active');
```

#### Calculation Rules

```sql
-- Launch success rate calculation
CREATE OR REPLACE PROCEDURE CALC_SUCCESS_RATES()
RETURNS TABLE (
    rocket_name STRING,
    success_rate FLOAT,
    confidence_level FLOAT
)
LANGUAGE SQL
AS
$$
BEGIN
    RETURN TABLE(
        SELECT
            r.NAME,
            SUM(CASE WHEN l.SUCCESS THEN 1 ELSE 0 END)::FLOAT / COUNT(*) as success_rate,
            1 - (1 / (1 + SQRT(COUNT(*)))) as confidence_level
        FROM FACT_LAUNCHES l
        JOIN DIM_ROCKETS r ON l.ROCKET_ID = r.ROCKET_ID
        GROUP BY r.NAME
        HAVING COUNT(*) >= 5
    );
END;
$$;
```
