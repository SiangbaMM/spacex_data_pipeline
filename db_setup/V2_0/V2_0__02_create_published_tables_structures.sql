USE DATABASE SPACEX_DATA_DEV;

USE SCHEMA PBL_SPACEX_DATA;

-- Dimension Tables (Level 1)
CREATE TABLE
    IF NOT EXISTS pbl_spacex_data_dim_company (
        company_key INT IDENTITY (1, 1) PRIMARY KEY,
        company_id VARCHAR(50),
        name VARCHAR(100),
        founder VARCHAR(100),
        founded INT,
        employees INT,
        valuation DECIMAL(15, 2),
        summary TEXT,
        created_at TIMESTAMP_NTZ,
        updated_at TIMESTAMP_NTZ
    );

CREATE TABLE
    IF NOT EXISTS pbl_spacex_data_dim_time (
        date_key INT PRIMARY KEY,
        full_date DATE,
        year INT,
        quarter INT,
        month INT,
        week INT,
        day INT,
        day_of_week INT,
        is_weekend BOOLEAN,
        created_at TIMESTAMP_NTZ,
        updated_at TIMESTAMP_NTZ
    );

-- Dimension Tables (Level 2)
CREATE TABLE
    IF NOT EXISTS pbl_spacex_data_dim_rockets (
        rocket_key INT IDENTITY (1, 1) PRIMARY KEY,
        rocket_id VARCHAR(50),
        name VARCHAR(100),
        type VARCHAR(50),
        active BOOLEAN,
        stages INT,
        boosters INT,
        cost_per_launch DECIMAL(15, 2),
        success_rate_pct INT,
        first_flight DATE,
        country VARCHAR(100),
        company_key INT REFERENCES pbl_spacex_data_dim_company (company_key),
        created_at TIMESTAMP_NTZ,
        updated_at TIMESTAMP_NTZ
    );

CREATE TABLE
    IF NOT EXISTS pbl_spacex_data_dim_dragons (
        dragon_key INT IDENTITY (1, 1) PRIMARY KEY,
        dragon_id VARCHAR(50),
        name VARCHAR(100),
        type VARCHAR(50),
        active BOOLEAN,
        crew_capacity INT,
        orbit_duration_yr INT,
        dry_mass_kg INT,
        first_flight DATE,
        company_key INT REFERENCES pbl_spacex_data_dim_company (company_key),
        created_at TIMESTAMP_NTZ,
        updated_at TIMESTAMP_NTZ
    );

-- Dimension Tables (Level 3)
CREATE TABLE
    IF NOT EXISTS pbl_spacex_data_dim_launchpads (
        launchpad_key INT IDENTITY (1, 1) PRIMARY KEY,
        launchpad_id VARCHAR(50),
        name VARCHAR(256),
        full_name VARCHAR(512),
        status VARCHAR(50),
        locality VARCHAR(256),
        region VARCHAR(256),
        latitude FLOAT,
        longitude FLOAT,
        launch_attempts INT,
        launch_successes INT,
        rockets_launched VARIANT, -- Array of rocket_keys
        created_at TIMESTAMP_NTZ,
        updated_at TIMESTAMP_NTZ
    );

CREATE TABLE
    IF NOT EXISTS pbl_spacex_data_dim_crew (
        crew_key INT IDENTITY (1, 1) PRIMARY KEY,
        crew_id VARCHAR(50),
        name VARCHAR(256),
        agency VARCHAR(256),
        status VARCHAR(50),
        nationality STRING,
        created_at TIMESTAMP_NTZ,
        updated_at TIMESTAMP_NTZ
    );

-- Ships dimension table
CREATE TABLE
    IF NOT EXISTS pbl_spacex_data_dim_ships (
        ship_key INT PRIMARY KEY,
        ship_id STRING,
        name STRING,
        type STRING, -- drone ship, recovery ship, support ship
        active BOOLEAN,
        home_port STRING,
        created_at TIMESTAMP_NTZ,
        updated_at TIMESTAMP_NTZ
    );

-- Subdimension Tables
CREATE TABLE
    IF NOT EXISTS pbl_spacex_data_dim_capsules (
        capsule_key INT IDENTITY (1, 1) PRIMARY KEY,
        capsule_id VARCHAR(50),
        serial VARCHAR(50),
        status VARCHAR(50),
        dragon_key INT REFERENCES pbl_spacex_data_dim_dragons (dragon_key),
        reuse_count INT,
        water_landings INT,
        land_landings INT,
        last_update TIMESTAMP_NTZ,
        created_at TIMESTAMP_NTZ,
        updated_at TIMESTAMP_NTZ
    );

CREATE TABLE
    IF NOT EXISTS pbl_spacex_data_dim_cores (
        core_key INT IDENTITY (1, 1) PRIMARY KEY,
        core_id VARCHAR(50),
        serial VARCHAR(50),
        block INT,
        status VARCHAR(50),
        reuse_count INT,
        rtls_attempts INT,
        rtls_landings INT,
        asds_attempts INT,
        asds_landings INT,
        last_update TIMESTAMP_NTZ,
        created_at TIMESTAMP_NTZ,
        updated_at TIMESTAMP_NTZ
    );

-- Fact Tables
CREATE TABLE
    IF NOT EXISTS pbl_spacex_data_fact_launches (
        launch_key INT IDENTITY (1, 1) PRIMARY KEY,
        launch_id VARCHAR(50),
        date_key INT REFERENCES pbl_spacex_data_dim_time (date_key),
        rocket_key INT REFERENCES pbl_spacex_data_dim_rockets (rocket_key),
        launchpad_key INT REFERENCES pbl_spacex_data_dim_launchpads (launchpad_key),
        success BOOLEAN,
        failures VARIANT, -- Array of failure details
        details TEXT,
        crew VARIANT, -- Array of crew_keys
        ships VARIANT, -- Array of ship references
        capsules VARIANT, -- Array of capsule_keys
        payloads VARIANT, -- Array of payload references
        cores VARIANT, -- Array of core information
        created_at TIMESTAMP_NTZ,
        updated_at TIMESTAMP_NTZ
    );

CREATE TABLE
    IF NOT EXISTS pbl_spacex_data_fact_payloads (
        payload_key INT IDENTITY (1, 1) PRIMARY KEY,
        payload_id VARCHAR(50),
        launch_key INT REFERENCES pbl_spacex_data_fact_launches (launch_key),
        type VARCHAR(100),
        name VARCHAR(256),
        mass_kg FLOAT,
        orbit VARCHAR(50),
        reference_system VARCHAR(50),
        regime VARCHAR(50),
        customers VARIANT, -- Array of customer names
        nationalities VARIANT, -- Array of nationality strings
        manufacturers VARIANT, -- Array of manufacturer names
        created_at TIMESTAMP_NTZ,
        updated_at TIMESTAMP_NTZ
    );

CREATE TABLE
    IF NOT EXISTS pbl_spacex_data_fact_starlink (
        starlink_key INT IDENTITY (1, 1) PRIMARY KEY,
        starlink_id VARCHAR(50),
        launch_key INT REFERENCES pbl_spacex_data_fact_launches (launch_key),
        version VARCHAR(50),
        height_km FLOAT,
        latitude FLOAT,
        longitude FLOAT,
        velocity_kms FLOAT,
        spacetrack_data VARIANT, -- JSON object with space-track.org data
        created_at TIMESTAMP_NTZ,
        updated_at TIMESTAMP_NTZ
    );

-- Bridge Tables
CREATE TABLE
    IF NOT EXISTS pbl_spacex_data_bridge_launch_crew (
        launch_key INT,
        crew_key INT,
        role STRING, -- Commander, Pilot, Mission Specialist
        seat_number INT, -- Position in Dragon capsule
        mission_status STRING, -- Assigned, In-Flight, Completed
        launch_order INT, -- For crew members with multiple flights
        created_at TIMESTAMP_NTZ,
        updated_at TIMESTAMP_NTZ,
        PRIMARY KEY (launch_key, crew_key),
        FOREIGN KEY (launch_key) REFERENCES pbl_spacex_data_fact_launches (launch_key),
        FOREIGN KEY (crew_key) REFERENCES pbl_spacex_data_dim_crew (crew_key)
    );

-- Bridge table for Launch-Ship relationship
CREATE TABLE
    IF NOT EXISTS pbl_spacex_data_bridge_launch_ships (
        launch_key INT,
        ship_key INT,
        role STRING, -- booster recovery, fairing recovery, support
        status STRING, -- success, failure, canceled
        created_at TIMESTAMP_NTZ,
        updated_at TIMESTAMP_NTZ,
        PRIMARY KEY (launch_key, ship_key, role),
        FOREIGN KEY (launch_key) REFERENCES pbl_spacex_data_fact_launches (launch_key),
        FOREIGN KEY (ship_key) REFERENCES pbl_spacex_data_dim_ships (ship_key)
    );

CREATE TABLE
    IF NOT EXISTS pbl_spacex_data_bridge_launch_dragons (
        launch_key INT,
        dragon_key INT,
        capsule_key INT,
        mission_type VARCHAR(50), -- CRS, CREW, COMMERCIAL
        cargo_mass_kg DECIMAL(10, 2), -- For cargo missions
        crew_count INT, -- For crew missions
        docking_date TIMESTAMP_NTZ,
        undocking_date TIMESTAMP_NTZ,
        mission_duration_hours INT,
        mission_status VARCHAR(50),
        PRIMARY KEY (launch_key, dragon_key)
    );
