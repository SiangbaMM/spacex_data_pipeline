# SpaceX Data Model Documentation

## Table of Contents

1. [Business Context Hypotheses](#business-context-hypotheses)
2. [Conceptual Data Model](#conceptual-data-model)
3. [Logical Data Model](#logical-data-model)
4. [Physical Data Model](#physical-data-model)
5. [Processing Model](#processing-model)
6. [Processing Model](#processing-model)

## Business Context Hypotheses

### 1. Core Business Entity Relationships

1. **Company Structure**

   - SpaceX operates as a single company entity
   - All rockets and dragons are owned and operated by SpaceX
   - Hypothesis: Single company dimension is sufficient as there's no multi-company scenario

2. **Launch Operations**
   - Each launch uses exactly one rocket
   - A launch can use multiple dragon capsules
   - A launch can deploy multiple payloads
   - Hypothesis: Launch is the primary fact table as it represents the main business event

### 2. Asset Management Hypotheses

1. **Reusable Components**

   - Cores and capsules are reused across multiple launches
   - Components have a lifecycle (active, retired, lost, etc.)
   - Hypothesis: Need to track historical usage and status changes

2. **Vehicle Configuration**
   - Rockets can have multiple configurations over time
   - Dragons evolve through different versions
   - Hypothesis: Need to handle vehicle versioning and configurations

### 3. Data Architecture Decisions

#### Snowflake Schema Choice

1. **Why Snowflake over Star Schema**

   - Complex hierarchical relationships between entities
   - Need for normalized reference data
   - Multiple levels of dimension tables
   - Hypothesis: Benefits of normalization outweigh query complexity

2. **Dimension Hierarchies**
   - Company → Rockets → Cores
   - Company → Dragons → Capsules
   - Hypothesis: Natural hierarchy exists in vehicle management

#### JSON/Array Handling

1. **Use of VARIANT Type**

   - Complex nested structures (thrusters, heat shields)
   - Dynamic arrays (crew assignments, ships)
   - Hypothesis: Some data elements are too dynamic for fixed schemas

2. **Normalization Decisions**
   - Primary entities are normalized (rockets, dragons)
   - Secondary data stored as JSON (technical specifications)
   - Hypothesis: Balance between query performance and data flexibility

## Conceptual Data Model

### Overview

The SpaceX data model represents the complete operational ecosystem of SpaceX, including launches, vehicles, facilities, and missions.

### Core Business Concepts

#### Mission Operations

- **Launches**: Primary operational events
- **Payloads**: Cargo and satellites deployed
- **Crew**: Personnel involved in missions
- **Vehicles**: Hardware used in missions

#### Assets and Infrastructure

- **Rockets**: Launch vehicles
- **Dragons**: Spacecraft
- **Launch Facilities**: Launch sites and support infrastructure
- **Recovery Assets**: Ships and landing pads

#### Company Information

- **Company Details**: Corporate information
- **Historical Events**: Significant milestones
- **Starlink**: Satellite constellation program

### Entity Relationships

```mermaid
erDiagram
    COMPANY ||--o{ ROCKETS : operates
    COMPANY ||--o{ DRAGONS : operates
    LAUNCHES ||--|{ PAYLOADS : carries
    LAUNCHES ||--o{ CREW : carries
    LAUNCHES }o--|| ROCKETS : uses
    DRAGONS ||--o{ LAUNCHES : uses
    LAUNCHES }o--|| LAUNCHPADS : launches_from
    LAUNCHES }o--o{ SHIPS : supported_by
    ROCKETS ||--|{ CORES : consists_of
    DRAGONS ||--|{ CAPSULES : consists_of
```

## Logical Data Model

```mermaid
erDiagram
    COMPANY ||--o{ ROCKETS : operates
    COMPANY ||--o{ DRAGONS : operates
    ROCKETS ||--|{ CORES : has
    DRAGONS ||--|{ CAPSULES : has
    ROCKETS ||--o{ LAUNCHES : used_in
    DRAGONS ||--o{ LAUNCHES : used_in
    LAUNCHES ||--|| LAUNCHPADS : launched_from
    LAUNCHES ||--|{ PAYLOADS : carries
    LAUNCHES ||--o{ CREW : carries
    LAUNCHES }|--o{ SHIPS : supported_by
    PAYLOADS ||--o{ STARLINK : includes
    LAUNCHES ||--o{ BRIDGE_LAUNCH_DRAGONS : tracks
    DRAGONS ||--o{ BRIDGE_LAUNCH_DRAGONS : missions
    CAPSULES ||--o{ BRIDGE_LAUNCH_DRAGONS : deployed_in
```

### Dimension Tables

#### Core Dimensions

1. **DIM_COMPANY**

   - Company information
   - Key metrics and valuations
   - Leadership structure

2. **DIM_ROCKETS**

   - Rocket specifications
   - Performance metrics
   - Manufacturing details

3. **DIM_DRAGONS**
   - Dragon spacecraft details
   - Capabilities
   - Technical specifications

#### Support Dimensions

1. **DIM_LAUNCHPADS**

   - Location details
   - Operational status
   - Launch capabilities

2. **DIM_SHIPS**
   - Recovery vessel information
   - Operational status
   - Capabilities

#### Component Dimensions

1. **DIM_CORES**

   - Reusable core details
   - Flight history
   - Status tracking

2. **DIM_CAPSULES**
   - Capsule specifications
   - Reuse information
   - Mission compatibility

### Fact Tables

1. **FACT_LAUNCHES**

   - Launch events
   - Mission outcomes
   - Related dimensions

2. **FACT_PAYLOADS**

   - Payload details
   - Orbital parameters
   - Customer information

3. **FACT_STARLINK**
   - Satellite tracking
   - Operational status
   - Orbital position

## Physical Data Model

```mermaid
erDiagram
    DIM_COMPANY {
        int company_key PK
        string company_id
        string name
        string founder
        int founded
        int employees
        decimal valuation
        string summary
        timestamp created_at
        timestamp updated_at
    }

    DIM_ROCKETS {
        int rocket_key PK
        string rocket_id
        string name
        string type
        boolean active
        int stages
        int boosters
        decimal cost_per_launch
        int success_rate_pct
        date first_flight
        string country
        int company_key FK
        timestamp created_at
        timestamp updated_at
    }

    DIM_DRAGONS {
        int dragon_key PK
        string dragon_id
        string name
        string type
        boolean active
        int crew_capacity
        int orbit_duration_yr
        int dry_mass_kg
        date first_flight
        int company_key FK
        timestamp created_at
        timestamp updated_at
    }

    DIM_LAUNCHPADS {
        int launchpad_key PK
        string launchpad_id
        string name
        string full_name
        string status
        string locality
        string region
        float latitude
        float longitude
        int launch_attempts
        int launch_successes
        variant rockets_launched
        timestamp created_at
        timestamp updated_at
    }

    FACT_LAUNCHES {
        int launch_key PK
        string launch_id
        int date_key FK
        int rocket_key FK
        int launchpad_key FK
        boolean success
        variant failures
        string details
        variant crew
        variant ships
        variant capsules
        variant payloads
        variant cores
        timestamp created_at
        timestamp updated_at
    }

    FACT_PAYLOADS {
        int payload_key PK
        string payload_id
        int launch_key FK
        string type
        string name
        float mass_kg
        string orbit
        string reference_system
        string regime
        variant customers
        variant nationalities
        variant manufacturers
        timestamp created_at
        timestamp updated_at
    }

    FACT_STARLINK {
        int starlink_key PK
        string starlink_id
        int launch_key FK
        string version
        float height_km
        float latitude
        float longitude
        float velocity_kms
        variant spacetrack_data
        timestamp created_at
        timestamp updated_at
    }

    DIM_COMPANY ||--o{ DIM_ROCKETS : "has"
    DIM_COMPANY ||--o{ DIM_DRAGONS : "has"
    FACT_LAUNCHES }|--|| DIM_ROCKETS : "uses"
    FACT_LAUNCHES }|--|| DIM_LAUNCHPADS : "launches_from"
    FACT_LAUNCHES ||--|{ FACT_PAYLOADS : "carries"
    FACT_LAUNCHES ||--|{ FACT_STARLINK : "deploys"
```

### Data Types and Conventions

#### Standard Data Types

- Strings: VARCHAR/STRING
- Dates: TIMESTAMP_NTZ
- Numbers: INTEGER/FLOAT
- Complex Data: VARIANT

#### Naming Conventions

- Tables: UPPER_CASE
- Primary Keys: [table_name]\_KEY
- Foreign Keys: Referenced dimension key name
- Timestamps: \_AT suffix

## Processing Model

### Data Flow

```mermaid
   graph LR
       A[SpaceX API] --> B[Staging Tables]
       B --> C[Compute Tables]
       C --> D[Published Tables]
```

1. **Extraction Layer**

   - SpaceX API connections
   - Rate limiting handling
   - Error management

2. **Staging Layer**

   - Data cleansing
   - Type conversion
   - Relationship mapping

3. **Transform Layer**

   - Dimension loading
   - Fact table population
   - Historical tracking

4. **Published Layer**

   - Views creation on top of dimension tables
   - Views creation on top of fact tables
   - Views creation on top of historical tracking

### ETL Process Flow

```mermaid
graph TD
    A[Extract from API] --> B[Load to STG_SPACEX_DATA]
    B --> C[Perform some Transformations/validations in STG_SPACEX_DATA]
    C --> D[Load Dimensions in CMP_SPACEX_DATA]
    D --> E[Load/validate Facts in CMP_SPACEX_DATA]
    E --> F[Load PBL_SPACEX_DATA]
    F --> G[Update Statistics]
```

### Incremental Processing

1. **Change Detection**

   - Timestamp-based tracking
   - Hash comparison
   - Delta identification

2. **Update Strategy**

   - Dimension: Slowly Changing Dimension (Type 2)
   - Facts: Append-only
   - History: Full preservation

3. **Error Handling**
   ```sql
   CREATE TABLE LOAD_ERRORS (
       TABLE_NAME STRING,
       ERROR_MESSAGE STRING,
       ERROR_DATA STRING,
       ERROR_TIME TIMESTAMP_NTZ
   );
   ```

### Quality Control

1. **Data Validation**

   - Schema compliance
   - Referential integrity
   - Business rules

2. **Monitoring**

   - Load statistics
   - Error tracking
   - Performance metrics

3. **Recovery Procedures**
   - Error logging
   - Retry logic
   - Rollback capabilities
