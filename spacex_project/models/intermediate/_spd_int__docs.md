# SpaceX DBT Models Documentation

## Intermediate Models

### cmp_bridge\_\_launch_cores

**Purpose**: Processes core usage data for each launch, tracking reusability metrics.

**Materialization**: Incremental

```markdown
| Column Name     | Data Type | Description                                   | Example   |
| --------------- | --------- | --------------------------------------------- | --------- |
| launch_core_id  | UUID      | Unique identifier for launch-core combination | uuid-1234 |
| launch_id       | UUID      | Reference to the launch                       | uuid-5678 |
| core_serial     | STRING    | Serial number of the core                     | B1049     |
| core_flight     | INTEGER   | Flight number for this core                   | 3         |
| core_block      | INTEGER   | Block version of the core                     | 5         |
| gridfins        | BOOLEAN   | Whether gridfins were used                    | true      |
| legs            | BOOLEAN   | Whether landing legs were deployed            | true      |
| reused          | BOOLEAN   | Whether this core was reused                  | true      |
| land_success    | BOOLEAN   | Whether landing was successful                | true      |
| landing_type    | STRING    | Type of landing attempted                     | ASDS      |
| landing_vehicle | STRING    | Name of landing vehicle/platform              | OCISLY    |
```

**Key Business Rules**:

- Each core can be used in multiple launches
- Landing success is only recorded for attempted landings
- Core block indicates technological version/generation

### cmp_bridge\_\_launch_crew

**Purpose**: Links crew members to specific launches with their roles.

**Materialization**: Incremental

```markdown
| Column Name    | Data Type | Description                                   | Example   |
| -------------- | --------- | --------------------------------------------- | --------- |
| launch_crew_id | UUID      | Unique identifier for launch-crew combination | uuid-1234 |
| launch_id      | UUID      | Reference to the launch                       | uuid-5678 |
| crew_id        | UUID      | Reference to the crew member                  | uuid-9012 |
| role           | STRING    | Role of crew member in the mission            | Commander |
| crew_status    | STRING    | Status of crew member during mission          | Active    |
```

**Key Business Rules**:

- Each crew member can have only one role per launch
- Status must be one of: Active, Reserve, Inactive

### cmp_bridge\_\_launch_ships

**Purpose**: Tracks recovery ships assigned to each launch.

**Materialization**: Incremental

```markdown
| Column Name    | Data Type | Description                                   | Example   |
| -------------- | --------- | --------------------------------------------- | --------- |
| launch_ship_id | UUID      | Unique identifier for launch-ship combination | uuid-1234 |
| launch_id      | UUID      | Reference to the launch                       | uuid-5678 |
| ship_id        | UUID      | Reference to the ship                         | uuid-9012 |
| ship_role      | STRING    | Role of ship in the mission                   | Support   |
```

**Key Business Rules**:

- Ships can support multiple roles in a single launch
- Standard roles include: Support, Recovery, Dragon Recovery

### cmp_bridge\_\_launch_payloads

**Purpose**: Details payload information for each launch.

**Materialization**: Incremental

```markdown
| Column Name       | Data Type | Description                                | Example    |
| ----------------- | --------- | ------------------------------------------ | ---------- |
| launch_payload_id | UUID      | Unique identifier for launch-payload combo | uuid-1234  |
| launch_id         | UUID      | Reference to the launch                    | uuid-5678  |
| payload_id        | UUID      | Reference to the payload                   | uuid-9012  |
| payload_type      | STRING    | Type of payload                            | Satellite  |
| payload_mass_kg   | FLOAT     | Mass of payload in kilograms               | 5400.5     |
| orbit             | STRING    | Target orbit for payload                   | LEO        |
| reference_system  | STRING    | Orbital reference system                   | geocentric |
| regime            | STRING    | Orbital regime                             | low-earth  |
```
