# SpaceX Data Model - Design Hypotheses and Decisions

## 1. Business Context Hypotheses

### Core Business Entity Relationships

1. **Company Structure**

   - SpaceX operates as a single company entity
   - All rockets and dragons are owned and operated by SpaceX
   - Hypothesis: Single company dimension is sufficient as there's no multi-company scenario

2. **Launch Operations**
   - Each launch uses exactly one rocket
   - A launch can use multiple dragon capsules
   - A launch can deploy multiple payloads
   - Hypothesis: Launch is the primary fact table as it represents the main business event

### Asset Management Hypotheses

1. **Reusable Components**

   - Cores and capsules are reused across multiple launches
   - Components have a lifecycle (active, retired, lost, etc.)
   - Hypothesis: Need to track historical usage and status changes

2. **Vehicle Configuration**
   - Rockets can have multiple configurations over time
   - Dragons evolve through different versions
   - Hypothesis: Need to handle vehicle versioning and configurations

## 2. Data Architecture Decisions

### Snowflake Schema Choice

1. **Why Snowflake over Star Schema**

   - Complex hierarchical relationships between entities
   - Need for normalized reference data
   - Multiple levels of dimension tables
   - Hypothesis: Benefits of normalization outweigh query complexity

2. **Dimension Hierarchies**
   - Company → Rockets → Cores
   - Company → Dragons → Capsules
   - Hypothesis: Natural hierarchy exists in vehicle management

### JSON/Array Handling

1. **Use of VARIANT Type**

   - Complex nested structures (thrusters, heat shields)
   - Dynamic arrays (crew assignments, ships)
   - Hypothesis: Some data elements are too dynamic for fixed schemas

2. **Normalization Decisions**
   - Primary entities are normalized (rockets, dragons)
   - Secondary data stored as JSON (technical specifications)
   - Hypothesis: Balance between query performance and data flexibility

## 3. Technical Design Decisions

### Primary Keys

1. **Key Strategy**

   - Surrogate keys for all dimension tables
   - Natural keys preserved as business keys
   - Hypothesis: Need for efficient joins and historical tracking

2. **Foreign Key Handling**
   - Enforced FKs for core relationships
   - Relaxed constraints for optional relationships
   - Hypothesis: Balance between data integrity and loading flexibility

### Performance Considerations

1. **Partitioning Strategy**

   - Launches partitioned by year
   - Large dimensions partitioned by status
   - Hypothesis: Time-based queries are most common

2. **Clustering Keys**
   - Launches clustered by date and rocket
   - Payloads clustered by launch and type
   - Hypothesis: Optimize for common query patterns

## 4. Business Rules Implementation

### Launch Success Tracking

1. **Success Metrics**

   - Launch success is boolean
   - Partial successes handled in details
   - Hypothesis: Need simple success flag with detailed context

2. **Failure Analysis**
   - Failures stored with launch record
   - Detailed failure data in JSON
   - Hypothesis: Failure analysis requires flexible data structure

### Asset Utilization

1. **Reuse Tracking**

   - Core reuse count maintained
   - Landing success tracking
   - Hypothesis: Reusability is key performance metric

2. **Maintenance Windows**
   - Status changes tracked in dimension tables
   - Maintenance periods preserved
   - Hypothesis: Need to track asset availability
