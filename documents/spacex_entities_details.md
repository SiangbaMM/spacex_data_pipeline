# SpaceX Entities - Detailed Explanation

## 1. Dragons

Dragon is SpaceX's spacecraft designed for both cargo and crew missions.

### Business Context

- Primary vehicle for NASA Commercial Crew Program
- Used for International Space Station (ISS) resupply missions
- Capable of both cargo and crew transportation
- Directly linked to specific launches for mission tracking

### Technical Characteristics

- **Versions**:
  - Dragon 1: Cargo-only variant (retired)
  - Dragon 2 (Crew Dragon): Current human-rated spacecraft
  - Cargo Dragon 2: Modified crew variant for cargo

### Key Attributes

```sql
CREATE TABLE DIM_DRAGONS (
    dragon_key INT,
    name STRING,          -- e.g., "Dragon 2"
    type STRING,          -- "crew" or "cargo"
    active BOOLEAN,       -- operational status
    crew_capacity INT,    -- typically 7 for Crew Dragon
    orbit_duration_yr INT,-- designed mission duration
    dry_mass_kg INT,      -- mass without cargo/fuel
    first_flight DATE,    -- inaugural mission date
    last_update DATE      -- latest status update
);
```

### Relationships

- One Dragon can be used for multiple missions
- Links to capsules (reusable components)
- Associated with specific launches

```sql
CREATE TABLE BRIDGE_LAUNCH_DRAGONS (
    launch_key INT,
    dragon_key INT,
    capsule_key INT,
    mission_type STRING,     -- CRS/CREW/COMMERCIAL
    cargo_mass_kg FLOAT,     -- for cargo missions
    crew_count INT,          -- for crewed missions
    docking_date TIMESTAMP,  -- ISS arrival
    undocking_date TIMESTAMP,-- ISS departure
    mission_status STRING    -- mission phase tracking
);
```

## 2. Capsules

Physical Dragon capsule units that are reused across missions.

### Business Context

- Reusable components of Dragon spacecraft
- Track flight history and reuse statistics
- Manage maintenance and refurbishment

### Key Attributes

```sql
CREATE TABLE DIM_CAPSULES (
    capsule_key INT,
    serial STRING,        -- e.g., "C201"
    status STRING,        -- active/retired/destroyed
    dragon_id STRING,     -- reference to dragon type
    reuse_count INT,      -- number of flights
    water_landings INT,   -- splashdown count
    land_landings INT     -- ground landing count
);
```

## 3. Rockets

The launch vehicles used by SpaceX, primarily the Falcon family.

### Business Context

- Primary launch vehicles for all missions
- Revolutionary reusable first stages
- Different variants for various mission profiles

### Technical Characteristics

- **Types**:
  - Falcon 9: Primary workhorse
  - Falcon Heavy: Heavy-lift variant
  - Starship: Next-generation vehicle (in development)

### Key Attributes

```sql
CREATE TABLE DIM_ROCKETS (
    rocket_key INT,
    name STRING,          -- e.g., "Falcon 9"
    type STRING,          -- vehicle variant
    active BOOLEAN,       -- operational status
    stages INT,           -- number of stages
    cost_per_launch INT,  -- estimated cost
    success_rate_pct INT, -- launch success rate
    first_flight DATE     -- inaugural flight date
);
```

## 4. Cores

Individual rocket first stages that are designed for reuse.

### Business Context

- Key to SpaceX's cost reduction strategy
- Track reuse and landing statistics
- Manage refurbishment cycles

### Key Attributes

```sql
CREATE TABLE DIM_CORES (
    core_key INT,
    serial STRING,        -- e.g., "B1051"
    block INT,           -- design iteration
    status STRING,       -- active/retired/lost
    reuse_count INT,     -- number of flights
    rtls_attempts INT,   -- Return To Launch Site attempts
    rtls_landings INT,   -- successful RTLS landings
    asds_attempts INT,   -- drone ship landing attempts
    asds_landings INT    -- successful drone ship landings
);
```

## 5. Launches

Individual missions carrying payloads to space.

### Business Context

- Primary business events
- Combine multiple components (rocket, payload, etc.)
- Track mission success and statistics
- Link to Dragon spacecraft for specific mission types

### Key Attributes

```sql
CREATE TABLE FACT_LAUNCHES (
    launch_key INT,
    flight_number INT,    -- sequential mission number
    name STRING,          -- mission name
    date_utc TIMESTAMP,   -- launch time
    success BOOLEAN,      -- mission success
    rocket_id STRING,     -- rocket used
    launchpad_id STRING,  -- launch location
    details STRING        -- mission details
);
```

## 6. Launchpads

Facilities used for launching rockets.

### Business Context

- Critical infrastructure
- Different capabilities for different missions
- Location affects mission parameters

### Key Attributes

```sql
CREATE TABLE DIM_LAUNCHPADS (
    launchpad_key INT,
    name STRING,          -- e.g., "KSC LC-39A"
    locality STRING,      -- geographic location
    region STRING,        -- state/region
    status STRING,        -- operational status
    launch_attempts INT,  -- total launches attempted
    launch_successes INT  -- successful launches
);
```

## 7. Payloads

Cargo carried on missions.

### Business Context

- Customer satellites
- Space station resupply cargo
- Starlink satellites

### Key Attributes

```sql
CREATE TABLE FACT_PAYLOADS (
    payload_key INT,
    name STRING,          -- payload name
    type STRING,          -- satellite/cargo/crew
    mass_kg FLOAT,       -- payload mass
    orbit STRING,         -- intended orbit
    customers ARRAY,      -- customer names
    nationalities ARRAY   -- countries involved
);
```

## 8. Ships

Recovery vessels for rockets and capsules.

### Business Context

- Essential for reusability program
- Drone ships for booster landing
- Recovery ships for capsules

### Key Attributes

```sql
CREATE TABLE DIM_SHIPS (
    ship_key INT,
    name STRING,          -- e.g., "Of Course I Still Love You"
    type STRING,          -- drone ship/recovery vessel
    active BOOLEAN,       -- operational status
    roles ARRAY,          -- ship capabilities
    launches ARRAY        -- supported missions
);
```

## 9. Starlink

SpaceX's satellite internet constellation.

### Business Context

- Internal SpaceX program
- Satellite internet service
- Largest satellite constellation

### Key Attributes

```sql
CREATE TABLE FACT_STARLINK (
    starlink_key INT,
    version STRING,       -- satellite version
    launch_id STRING,     -- deployment mission
    height_km FLOAT,      -- orbital altitude
    velocity_kms FLOAT,   -- orbital velocity
    longitude FLOAT,      -- current position
    latitude FLOAT       -- current position
);
```
