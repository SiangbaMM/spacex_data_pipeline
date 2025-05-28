#!/usr/bin/env python3
"""
Snowflake Data Mesh Architecture Setup Script

This script automates the creation of databases, roles, users, and warehouses
for a data mesh architecture in Snowflake.

Author: Data Engineering Team
Version: 1.0.0
"""

import argparse
import logging
import os
import sys
from dataclasses import dataclass
from datetime import datetime
from getpass import getpass
from typing import Dict, List, Optional

import snowflake.connector
from snowflake.connector import DictCursor
from snowflake.connector.errors import DatabaseError, ProgrammingError


# Configuration
@dataclass
class SnowflakeConfig:
    """Configuration for Snowflake connection"""

    account: str
    user: str
    password: str
    role: str = "SYSADMIN"
    warehouse: str = "COMPUTE_WH"

    @classmethod
    def from_args(cls, args: argparse.Namespace) -> "SnowflakeConfig":
        """Create SnowflakeConfig from command line arguments"""
        # If password not provided via args, prompt for it securely
        password = args.password
        if not password:
            password = getpass("Enter Snowflake password: ")

        return cls(
            account=args.account,
            user=args.user,
            password=password,
            role=args.role,
            warehouse=args.warehouse,
        )


@dataclass
class DomainConfig:
    """Configuration for a business domain"""

    name: str
    description: Optional[str] = None


class SnowflakeDataMeshManager:
    """Manages Snowflake data mesh setup operations"""

    def __init__(self, config: SnowflakeConfig):
        """Initialize the snowflake data mesh manager"""
        self.config = config
        self.connection = None
        self.cursor = None
        self.logger = self._setup_logging()
        self.dry_run = False  # Can be set externally for dry-run mode

    def _setup_logging(self) -> logging.Logger:
        """Configure logging for the application"""
        return logging.getLogger(__name__)

    def connect(self) -> None:
        """Establish connection to Snowflake"""
        if self.dry_run:
            self.logger.info("DRY RUN MODE: Skipping Snowflake connection")
            return

        try:
            self.connection = snowflake.connector.connect(
                account=self.config.account,
                user=self.config.user,
                password=self.config.password,
                role=self.config.role,
                warehouse=self.config.warehouse,
            )
            if isinstance(self.connection, snowflake.connector):
                self.cursor = self.connection.cursor(DictCursor)
                self.logger.info(f"Connected to Snowflake as {self.config.user}")
        except Exception as e:
            self.logger.error(f"Failed to connect to Snowflake: {e}")
            raise

    def disconnect(self) -> None:
        """Close Snowflake connection"""
        if self.dry_run:
            self.logger.info("DRY RUN MODE: Skipping disconnection")
            return

        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        self.logger.info("Disconnected from Snowflake")

    def execute_sql(self, sql: str, params: Optional[Dict] = None) -> List[Dict]:
        """Execute SQL command with error handling"""
        if self.dry_run:
            self.logger.info(f"DRY RUN: {sql}")
            if params:
                self.logger.info(f"DRY RUN PARAMS: {params}")
            return []

        try:
            if isinstance(self.cursor, snowflake.connector.cursor):
                if params:
                    self.cursor.execute(sql, params)
                else:
                    self.cursor.execute(sql)

                # Fetch results if it's a SELECT statement
                if sql.strip().upper().startswith("SELECT"):
                    return self.cursor.fetchall()

            return []
        except (DatabaseError, ProgrammingError) as e:
            self.logger.error(f"SQL execution failed: {e}")
            self.logger.error(f"SQL: {sql}")
            raise

    def create_domain_database(
        self, domain: DomainConfig, domain_env_prefix: str
    ) -> None:
        """Create database for a business domain in specified environment"""
        database_name = domain_env_prefix
        self.logger.info(
            f"Creating database for domain: {domain.name} with prefix: {domain_env_prefix}"
        )

        # Use SYSADMIN role for database creation
        self.execute_sql("USE ROLE SYSADMIN")

        # Create database with environment suffix
        sql = f"CREATE DATABASE IF NOT EXISTS {database_name}"
        if domain.description:
            sql += f"COMMENT = '{domain.description} - Environment prefix: {domain_env_prefix}'"

        self.execute_sql(sql)
        self.logger.info(f"Database {database_name} created successfully")

        # Grant ownership on database to domain sysadmin role
        self.execute_sql(
            f"GRANT OWNERSHIP ON DATABASE {database_name} TO ROLE {domain_env_prefix}_SYSADMIN REVOKE CURRENT GRANTS"
        )

    def create_custom_roles(self, domain_name: str, domain_env_prefix: str) -> None:
        """Create custom roles for the domain database in specified environment"""
        self.logger.info(
            f"Creating custom roles for domain: {domain_name} \
                with prefix: {domain_env_prefix}"
        )

        # Switch to SECURITYADMIN to create roles
        self.execute_sql("USE ROLE SECURITYADMIN")

        # Define roles to create
        roles = [
            f"{domain_env_prefix}_SECADMIN",
            f"{domain_env_prefix}_SYSADMIN",
            f"{domain_env_prefix}_CICD",
        ]

        for role in roles:
            try:
                self.execute_sql(f"CREATE ROLE IF NOT EXISTS {role}")
                self.logger.info(f"Role {role} created successfully")
            except Exception as e:
                self.logger.error(f"Failed to create role {role}: {e}")
                raise

        self.execute_sql(
            f"GRANT CREATE USER, CREATE ROLE ON ACCOUNT, \
                MANAGE GRANTS ON ACCOUNT TO ROLE {domain_env_prefix}_SECADMIN"
        )
        self.execute_sql(
            f"GRANT MANAGE GRANTS ON ACCOUNT TO ROLE {domain_env_prefix}_SYSADMIN"
        )

        # Grant role hierarchy
        # CICD can substitute SECADMIN and SYSADMIN
        self.execute_sql(
            f"GRANT ROLE {domain_env_prefix}_SECADMIN \
                TO ROLE {domain_env_prefix}_CICD"
        )
        self.execute_sql(
            f"GRANT ROLE {domain_env_prefix}_SYSADMIN \
                TO ROLE {domain_env_prefix}_CICD"
        )

        self.logger.info(
            f"Role hierarchy configured for domain: {domain_name} \
                with prefix: {domain_env_prefix}"
        )

        # Create CICD user
        user_sql = f"""
        CREATE USER IF NOT EXISTS {domain_env_prefix}_CICD
        PASSWORD = 'TempPassword123!'
        MUST_CHANGE_PASSWORD = TRUE
        DEFAULT_ROLE = '{domain_env_prefix}_CICD'
        COMMENT = 'User for CICD operations in {domain_name} \
            domain with prefix {domain_env_prefix}'
        """
        self.execute_sql(user_sql)

        self.execute_sql(
            f"GRANT ROLE {domain_env_prefix}_CICD \
                TO USER {domain_env_prefix}_CICD"
        )

        self.logger.info(
            f"User for CICD operations configured for domain: {domain_name} \
                with prefix: {domain_env_prefix}"
        )

    def grant_domain_secadmin_permissions(
        self, domain_name: str, domain_env_prefix: str
    ) -> None:
        """Grant domain-specific SECADMIN permissions (limited to domain only)"""
        database_name = domain_env_prefix
        self.logger.info(
            f"Granting domain SECADMIN permissions for domain: {domain_name} with prefix: {domain_env_prefix}"
        )

        # Use SYSADMIN role first to ensure we have access to the database
        self.execute_sql(f"USE ROLE {domain_env_prefix}_SYSADMIN")

        # Create a schema for domain administration utilities
        self.execute_sql(f"USE DATABASE {database_name}")
        self.execute_sql(f"CREATE SCHEMA IF NOT EXISTS {database_name}.ADMIN")

        # Grant privilege on schema for domain administration utilities
        securityadmin_database_privileges = ["USAGE", "MODIFY", "MONITOR"]
        for privilege in securityadmin_database_privileges:
            self.execute_sql(
                f"GRANT {privilege} ON DATABASE {database_name} TO ROLE {domain_env_prefix}_SECADMIN"
            )

        securityadmin_schema_privileges = [
            "USAGE",
            "MONITOR",
            "CREATE FUNCTION",
            "CREATE PROCEDURE",
            "CREATE VIEW",
        ]
        for privilege in securityadmin_schema_privileges:
            self.execute_sql(
                f"GRANT {privilege} ON SCHEMA {database_name}.ADMIN TO ROLE {domain_env_prefix}_SECADMIN"
            )

        # Switch to SECURITYADMIN to create the procedure with proper privileges
        self.execute_sql(f"USE ROLE {domain_env_prefix}_SECADMIN")

        # Create a procedure that allows domain creation of users/roles with domain prefix
        user_role_management_procedure = f"""
        CREATE OR REPLACE PROCEDURE {database_name}.ADMIN.MANAGE_DOMAIN_USER_ROLE(
            ACTION STRING,
            OBJECT_TYPE STRING,
            OBJECT_NAME STRING,
            ADDITIONAL_PARAMS VARIANT
        )
        RETURNS STRING
        LANGUAGE SQL
        EXECUTE AS CALLER
        AS
        $$
        DECLARE
            result STRING;
            full_object_name STRING;
            domain_prefix STRING := '{domain_env_prefix}_';
        BEGIN
            -- Ensure object name starts with domain prefix
            IF (OBJECT_NAME NOT LIKE domain_prefix || '%') THEN
                full_object_name := domain_prefix || OBJECT_NAME;
            ELSE
                full_object_name := OBJECT_NAME;
            END IF;

            IF (ACTION = 'CREATE' AND OBJECT_TYPE = 'USER') THEN
                EXECUTE IMMEDIATE 'CREATE USER IF NOT EXISTS ' || full_object_name ||
                    ' PASSWORD = ' || PARSE_JSON(ADDITIONAL_PARAMS):password ||
                    ' MUST_CHANGE_PASSWORD = TRUE' ||
                    ' DEFAULT_ROLE = ' || PARSE_JSON(ADDITIONAL_PARAMS):default_role;
                result := 'User ' || full_object_name || ' created successfully';

            ELSEIF (ACTION = 'CREATE' AND OBJECT_TYPE = 'ROLE') THEN
                EXECUTE IMMEDIATE 'CREATE ROLE IF NOT EXISTS ' || full_object_name;
                result := 'Role ' || full_object_name || ' created successfully';

            ELSEIF (ACTION = 'DROP' AND OBJECT_TYPE = 'USER') THEN
                EXECUTE IMMEDIATE 'DROP USER IF EXISTS ' || full_object_name;
                result := 'User ' || full_object_name || ' dropped successfully';

            ELSEIF (ACTION = 'DROP' AND OBJECT_TYPE = 'ROLE') THEN
                EXECUTE IMMEDIATE 'DROP ROLE IF EXISTS ' || full_object_name;
                result := 'Role ' || full_object_name || ' dropped successfully';

            ELSEIF (ACTION = 'GRANT' AND OBJECT_TYPE = 'ROLE') THEN
                EXECUTE IMMEDIATE 'GRANT ROLE ' || full_object_name || \
                    ' TO ROLE ' || PARSE_JSON(ADDITIONAL_PARAMS):updated_role ;
                result := 'Role ' || full_object_name || ' added successfully to role ' \
                    || PARSE_JSON(ADDITIONAL_PARAMS):updated_role ;

            ELSEIF (ACTION = 'ALTER' AND OBJECT_TYPE = 'USER') THEN
                EXECUTE IMMEDIATE 'ALTER USER ' || full_object_name || ' SET PASSWORD = ' ||
                    PARSE_JSON(ADDITIONAL_PARAMS):password;
                result := 'User ' || full_object_name || ' password updated successfully';

            ELSE
                result := 'Invalid action or object type';
            END IF;

            RETURN result;
        END;
        $$
        ;
        """

        # Create the procedure
        self.execute_sql(user_role_management_procedure)

    def create_domain_users(self, domain_name: str, domain_env_prefix: str) -> None:
        """Create users for domain-specific operations in specified environment"""
        self.logger.info(
            f"Creating users for domain: {domain_name} with prefix: {domain_env_prefix}"
        )

        # Use SECURITYADMIN role to create initial users and roles
        self.execute_sql("USE ROLE SECURITYADMIN")

        # Define users to create with their roles
        users_config = [
            ("load", f"{domain_env_prefix}_LOAD_ROLE"),
            ("transform", f"{domain_env_prefix}_TRANSFORM_ROLE"),
            ("analyst", f"{domain_env_prefix}_ANALYST_ROLE"),
        ]

        for user_name, role_name in users_config:
            full_user_name = f"{domain_env_prefix}_{user_name.upper()}"

            # Create role first
            self.execute_sql(f"CREATE ROLE IF NOT EXISTS {role_name}")
            self.logger.info(f"Role {role_name} created")

            # Create user
            user_sql = f"""
            CREATE USER IF NOT EXISTS {full_user_name}
            PASSWORD = 'TempPassword123!'
            MUST_CHANGE_PASSWORD = TRUE
            DEFAULT_ROLE = '{role_name}'
            COMMENT = 'User for {user_name} operations in {domain_name} domain with prefix {domain_env_prefix}'
            """
            self.execute_sql(user_sql)

            # Grant role to user
            self.execute_sql(f"GRANT ROLE {role_name} TO USER {full_user_name}")

            # Grant ownership of the role to the domain SECADMIN to allow future modifications
            # This gives the domain SECADMIN full control over the roles it creates
            self.execute_sql(
                f"GRANT OWNERSHIP ON ROLE {role_name} TO ROLE {domain_env_prefix}_SECADMIN"
            )
            self.execute_sql(
                f"GRANT ROLE {role_name} TO ROLE {domain_env_prefix}_SYSADMIN"
            )

            # Grant ownership of the user to the domain SECADMIN to allow future modifications
            # This gives the domain SECADMIN full control over the users it creates
            self.execute_sql(
                f"GRANT OWNERSHIP ON USER {full_user_name} TO ROLE {domain_env_prefix}_SECADMIN"
            )

            self.logger.info(
                f"User {full_user_name} created with role {role_name}, ownership granted to domain SECADMIN"
            )

    def create_warehouses(self, domain_name: str, domain_env_prefix: str) -> None:
        """Create warehouses for different workloads in specified environment"""
        self.logger.info(
            f"Creating warehouses for domain: {domain_name} \
                with prefix: {domain_env_prefix}"
        )

        # Use domain SYSADMIN role
        self.execute_sql("USE ROLE SYSADMIN")

        # Define warehouses
        warehouses = ["load", "transform", "adhoc"]

        for wh_name in warehouses:
            full_wh_name = f"{domain_env_prefix}_{wh_name.upper()}_WH"

            wh_sql = f"""
            CREATE WAREHOUSE IF NOT EXISTS {full_wh_name}
            WITH
            WAREHOUSE_SIZE = 'X-SMALL'
            AUTO_SUSPEND = 60
            AUTO_RESUME = TRUE
            INITIALLY_SUSPENDED = TRUE
            COMMENT = '{wh_name.title()} warehouse for {domain_name} domain with prefix {domain_env_prefix}'
            """
            self.execute_sql(wh_sql)
            self.logger.info(f"Warehouse {full_wh_name} created")

            self.execute_sql(
                f"GRANT OWNERSHIP ON WAREHOUSE {full_wh_name} \
                    TO ROLE {domain_env_prefix}_SYSADMIN REVOKE CURRENT GRANTS"
            )
            self.logger.info(
                f"Granted OWNERSHIP on {full_wh_name} to {domain_env_prefix}_SYSADMIN"
            )

    def grant_warehouse_permissions(
        self, domain_name: str, domain_env_prefix: str
    ) -> None:
        """Grant appropriate permissions on warehouses (limited to domain warehouses)"""
        self.logger.info(
            f"Granting warehouse permissions for domain: {domain_name} with prefix: {domain_env_prefix}"
        )

        # Use domain SYSADMIN role to grant warehouse permissions
        self.execute_sql(f"USE ROLE {domain_env_prefix}_SYSADMIN")

        # Define warehouse permissions (only for domain-specific warehouses)
        permissions = [
            (f"{domain_env_prefix}_LOAD_WH", f"{domain_env_prefix}_LOAD_ROLE"),
            (
                f"{domain_env_prefix}_TRANSFORM_WH",
                f"{domain_env_prefix}_TRANSFORM_ROLE",
            ),
            (f"{domain_env_prefix}_ADHOC_WH", f"{domain_env_prefix}_CICD"),
            (f"{domain_env_prefix}_ADHOC_WH", f"{domain_env_prefix}_ANALYST_ROLE"),
            (f"{domain_env_prefix}_ADHOC_WH", f"{domain_env_prefix}_SECADMIN"),
        ]

        for warehouse, role in permissions:
            self.execute_sql(f"GRANT USAGE ON WAREHOUSE {warehouse} TO ROLE {role}")
            self.execute_sql(f"GRANT OPERATE ON WAREHOUSE {warehouse} TO ROLE {role}")
            self.logger.info(f"Granted USAGE and OPERATE on {warehouse} to {role}")

    def grant_database_permissions(
        self, domain_name: str, domain_env_prefix: str
    ) -> None:
        """Grant database permissions to roles (limited to domain database)"""
        database_name = domain_env_prefix
        self.logger.info(
            f"Granting database permissions for domain: {domain_name} with prefix: {domain_env_prefix}"
        )

        # Use SYSADMIN role to grant database permissions
        self.execute_sql("USE ROLE SYSADMIN")

        # Grant ownership of the database to domain SYSADMIN
        # This gives full control over the database objects
        self.execute_sql(
            f"GRANT OWNERSHIP ON DATABASE {database_name} TO ROLE {domain_env_prefix}_SYSADMIN REVOKE CURRENT GRANTS"
        )

        # Grant limited privileges to domain SECADMIN (only what's needed for user/role management)
        self.execute_sql(f"USE ROLE {domain_env_prefix}_SYSADMIN")

        domain_secadmin_privileges = ["USAGE"]

        for privilege in domain_secadmin_privileges:
            self.execute_sql(
                f"GRANT {privilege} ON DATABASE {database_name} TO ROLE {domain_env_prefix}_SECADMIN"
            )

        # Grant specific permissions to other roles
        roles_permissions = {
            f"{domain_env_prefix}_LOAD_ROLE": ["USAGE", "CREATE SCHEMA"],
            f"{domain_env_prefix}_TRANSFORM_ROLE": ["USAGE", "CREATE SCHEMA"],
            f"{domain_env_prefix}_ANALYST_ROLE": ["USAGE"],
        }

        for role, permissions in roles_permissions.items():
            for permission in permissions:
                self.execute_sql(
                    f"GRANT {permission} ON DATABASE {database_name} TO ROLE {role}"
                )
                self.logger.info(f"Granted {permission} on {database_name} to {role}")

        self.logger.info(
            f"Database permissions configured with domain isolation for: {domain_name} with prefix: {domain_env_prefix}"
        )

    def configure_user_warehouses(
        self, domain_name: str, domain_env_prefix: str
    ) -> None:
        """Configure default warehouses for users (limited to domain warehouses)"""
        self.logger.info(
            f"Configuring user warehouses for domain: {domain_name} with prefix: {domain_env_prefix}"
        )

        # Use domain SECADMIN role to modify users it owns
        self.execute_sql(f"USE ROLE {domain_env_prefix}_SECADMIN")

        # Define user-warehouse mappings (only domain-specific warehouses)
        user_warehouse_mapping = [
            (f"{domain_env_prefix}_LOAD", f"{domain_env_prefix}_LOAD_WH"),
            (f"{domain_env_prefix}_TRANSFORM", f"{domain_env_prefix}_TRANSFORM_WH"),
            (f"{domain_env_prefix}_ANALYST", f"{domain_env_prefix}_ADHOC_WH"),
        ]

        for user, warehouse in user_warehouse_mapping:
            self.execute_sql(f"ALTER USER {user} SET DEFAULT_WAREHOUSE = '{warehouse}'")
            self.logger.info(f"Set default warehouse {warehouse} for user {user}")

    def create_domain_isolation_views(
        self, domain_name: str, domain_env_prefix: str
    ) -> None:
        """Create views and functions for domain isolation monitoring"""
        database_name = domain_env_prefix
        self.logger.info(
            f"Creating domain isolation views for domain: {domain_name} with prefix: {domain_env_prefix}"
        )

        # Create MONITORING schema for domain isolation views
        self.execute_sql(f"USE ROLE {domain_env_prefix}_SYSADMIN")
        self.execute_sql(f"CREATE SCHEMA IF NOT EXISTS {database_name}.MONITORING")

        # We need to use SHOW statements to populate the view with current data
        # First, create a procedure to refresh the current view
        refresh_procedure = f"""
        CREATE OR REPLACE PROCEDURE {database_name}.MONITORING.REFRESH_CURRENT_OBJECTS()
        RETURNS STRING
        LANGUAGE SQL
        EXECUTE AS CALLER
        AS
        $$
        BEGIN
            -- Create temporary table for warehouses
            SHOW WAREHOUSES LIKE '{domain_env_prefix}_%' ;

            CREATE OR REPLACE TRANSIENT TABLE {database_name}.MONITORING.TEMP_WAREHOUSES AS
            SELECT
                'WAREHOUSE' as OBJECT_TYPE,
                "name" as OBJECT_NAME,
                "created_on" as CREATED_ON,
                "owner" as OWNER,
                '{domain_env_prefix}' as DOMAIN_ENV_PREFIX
            FROM TABLE(RESULT_SCAN(LAST_QUERY_ID(-1))) ;

            -- Create temporary table for roles
            SHOW ROLES LIKE '{domain_env_prefix}_%' ;

            CREATE OR REPLACE TRANSIENT TABLE {database_name}.MONITORING.TEMP_ROLES AS
            SELECT
                'ROLE' as OBJECT_TYPE,
                "name" as OBJECT_NAME,
                "created_on" as CREATED_ON,
                "owner" as OWNER,
                '{domain_env_prefix}' as DOMAIN_ENV_PREFIX
            FROM TABLE(RESULT_SCAN(LAST_QUERY_ID(-1))) ;

            -- Create temporary table for users
            SHOW USERS LIKE '{domain_env_prefix}_%' ;

            CREATE OR REPLACE TRANSIENT TABLE {database_name}.MONITORING.TEMP_USERS AS
            SELECT
                'USER' as OBJECT_TYPE,
                "name" as OBJECT_NAME,
                "created_on" as CREATED_ON,
                "owner" as OWNER,
                '{domain_env_prefix}' as DOMAIN_ENV_PREFIX
            FROM TABLE(RESULT_SCAN(LAST_QUERY_ID(-1))) ;

            -- Create temporary table for schemas
            SHOW SCHEMAS IN DATABASE {domain_env_prefix} ;

            CREATE OR REPLACE TRANSIENT TABLE {database_name}.MONITORING.TEMP_SCHEMAS AS
            SELECT
                'SCHEMA' as OBJECT_TYPE,
                "name" as OBJECT_NAME,
                "created_on" as CREATED_ON,
                "owner" as OWNER,
                '{domain_env_prefix}' as DOMAIN_ENV_PREFIX
            FROM TABLE(RESULT_SCAN(LAST_QUERY_ID(-1))) ;

            CREATE OR REPLACE VIEW {database_name}.MONITORING.DOMAIN_OBJECTS AS
                SELECT
                    OBJECT_TYPE,
                    OBJECT_NAME,
                    CREATED_ON,
                    OWNER,
                    DOMAIN_ENV_PREFIX
                FROM {database_name}.MONITORING.TEMP_USERS

                UNION ALL

                SELECT
                    OBJECT_TYPE,
                    OBJECT_NAME,
                    CREATED_ON,
                    OWNER,
                    DOMAIN_ENV_PREFIX
                FROM {database_name}.MONITORING.TEMP_ROLES

                UNION ALL

                SELECT
                    OBJECT_TYPE,
                    OBJECT_NAME,
                    CREATED_ON,
                    OWNER,
                    DOMAIN_ENV_PREFIX
                FROM {database_name}.MONITORING.TEMP_WAREHOUSES

                UNION ALL

                SELECT
                    OBJECT_TYPE,
                    OBJECT_NAME,
                    CREATED_ON,
                    OWNER,
                    DOMAIN_ENV_PREFIX
                FROM {database_name}.MONITORING.TEMP_SCHEMAS
                ;

            RETURN 'Tables refreshed successfully';
        END;
        $$
        ;
        """

        self.execute_sql(refresh_procedure)

        # Call refresh procedure
        call_refresh_procedure = (
            f"CALL {domain_env_prefix}.MONITORING.REFRESH_CURRENT_OBJECTS();"
        )
        self.execute_sql(call_refresh_procedure)

        self.logger.info(
            f"Domain isolation views created for: {domain_name} with prefix: {domain_env_prefix}"
        )

    def setup_domain(self, domain: DomainConfig, environment: str) -> None:
        """Complete setup for a single domain in specified environment"""
        # Create domain-environment prefix once
        domain_env_prefix = f"{domain.name.upper()}_{environment.upper()}"

        self.logger.info(
            f"Starting complete setup for domain: {domain.name} \
                in environment: {environment} with prefix: {domain_env_prefix}"
        )

        try:
            # Step 1: Create database
            self.create_domain_database(domain, domain_env_prefix)

            # Step 2: Create custom roles
            self.create_custom_roles(domain.name, domain_env_prefix)

            # Step 3: Grant domain-specific SECADMIN permissions
            self.grant_domain_secadmin_permissions(domain.name, domain_env_prefix)

            # Step 4: Create users and their roles
            self.create_domain_users(domain.name, domain_env_prefix)

            # Step 5: Create warehouses
            self.create_warehouses(domain.name, domain_env_prefix)

            # Step 6: Grant warehouse permissions
            self.grant_warehouse_permissions(domain.name, domain_env_prefix)

            # Step 7: Grant database permissions
            self.grant_database_permissions(domain.name, domain_env_prefix)

            # Step 8: Configure user warehouses
            self.configure_user_warehouses(domain.name, domain_env_prefix)

            # Step 9: Create domain isolation monitoring views
            self.create_domain_isolation_views(domain.name, domain_env_prefix)

            self.logger.info(
                f"Domain {domain.name} setup completed successfully in \
                    {environment} environment with proper isolation"
            )

        except Exception as e:
            self.logger.error(
                f"Failed to setup domain {domain.name} in environment {environment}: {e}"
            )
            raise

    def setup_multiple_domains(
        self, domains: List[DomainConfig], environment: str
    ) -> None:
        """Set up multiple domains in specified environment"""
        self.logger.info(
            f"Setting up {len(domains)} domains in {environment} environment"
        )

        for domain in domains:
            try:
                self.setup_domain(domain, environment)
            except Exception as e:
                self.logger.error(
                    f"Failed to setup domain {domain.name} in environment \
                        {environment}, continuing with next domain : {e}"
                )
                continue

        self.logger.info(
            f"Data mesh setup completed successfully for {environment} environment!"
        )

        self.logger.info(f"Multi-domain setup completed for {environment} environment")


def load_config_from_env() -> SnowflakeConfig:
    """Load Snowflake configuration from environment variables"""
    required_vars = ["SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD"]

    for var in required_vars:
        if not os.getenv(var):
            raise ValueError(f"Environment variable {var} is required")

    return SnowflakeConfig(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        role=os.getenv("SNOWFLAKE_ROLE", "SYSADMIN"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
    )


def parse_arguments() -> argparse.Namespace:
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description="Setup Snowflake Data Mesh Architecture",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
        Examples:
        # Basic usage with prompts for dev environment
        python snowflake_database_setup.py --account myaccount --user myuser --env dev

        # Full configuration via arguments for production
        python snowflake_database_setup.py --account myaccount --user myuser --password mypass --env prd --role SYSADMIN

        # Multiple domains for UAT environment
        python snowflake_database_setup.py --account myaccount --user myuser --env uat --domains sales finance

        # Using environment variables (legacy mode)
        export SNOWFLAKE_ACCOUNT=myaccount
        python snowflake_database_setup.py --use-env --env dev
        """,
    )

    # Connection arguments
    parser.add_argument(
        "--account", "-a", type=str, help="Snowflake account identifier"
    )

    parser.add_argument("--user", "-u", type=str, help="Snowflake username")

    parser.add_argument(
        "--password",
        "-p",
        type=str,
        help="Snowflake password (if not provided, will prompt securely)",
    )

    parser.add_argument(
        "--role",
        "-r",
        type=str,
        default="SYSADMIN",
        help="Snowflake role to use (default: SYSADMIN)",
    )

    parser.add_argument(
        "--warehouse",
        "-w",
        type=str,
        default="COMPUTE_WH",
        help="Snowflake warehouse to use (default: COMPUTE_WH)",
    )

    # Environment configuration
    parser.add_argument(
        "--env",
        "-e",
        type=str,
        required=True,
        choices=["dev", "uat", "prd"],
        help="Target environment (required): dev, uat, or prd",
    )

    # Configuration mode
    parser.add_argument(
        "--use-env",
        action="store_true",
        help="Use environment variables for connection config (legacy mode)",
    )

    # Domain configuration
    parser.add_argument(
        "--domains",
        type=str,
        nargs="+",
        help="List of domain names to setup (e.g., sales marketing finance)",
    )

    parser.add_argument(
        "--config-file",
        type=str,
        help="Path to YAML/JSON configuration file with domain definitions",
    )

    # Execution options
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print SQL commands without executing them",
    )

    parser.add_argument(
        "--verbose", "-v", action="store_true", help="Enable verbose logging"
    )

    return parser.parse_args()


def main() -> None:
    """Execute the data mesh setup"""
    # Parse command line arguments
    args = parse_arguments()

    # Configure logging based on verbosity
    today = datetime.now().strftime("%Y-%m-%d")
    log_dir = os.path.join(os.path.dirname(__file__), "..")
    log_file = os.path.join(
        log_dir, "logs", f"snowflake_database_setup_{args.env}_{today}.log"
    )

    try:
        with open(log_file, "x") as file:
            file.write(f"{today} - snowflake_database_setup log file \n")
    except FileExistsError:
        print(f"The file '{log_file}' already exists.")

    log_level = logging.DEBUG if args.verbose else logging.INFO

    logging.basicConfig(
        level=log_level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[logging.FileHandler(log_file), logging.StreamHandler(sys.stdout)],
    )

    try:
        # Load configuration
        if args.use_env:
            config = load_config_from_env()
        else:
            # Validate required arguments
            if not args.account or not args.user:
                print(
                    "Error: --account and --user are required when not using --use-env"
                )
                print("Use --help for more information")
                sys.exit(1)
            config = SnowflakeConfig.from_args(args)

        # Define domains to setup
        if args.domains:
            # Use domains from command line
            domains = [DomainConfig(domain.lower()) for domain in args.domains]
        elif args.config_file:
            # TODO: Load domains from configuration file
            print("Configuration file loading not implemented yet")
            sys.exit(1)
        else:
            # Use default domains
            domains = [
                DomainConfig(
                    "sales",
                    f"Sales domain for revenue and customer data - {args.env.upper()}",
                ),
                DomainConfig(
                    "marketing",
                    f"Marketing domain for campaigns and analytics - {args.env.upper()}",
                ),
                DomainConfig(
                    "finance",
                    f"Finance domain for financial reporting - {args.env.upper()}",
                ),
                DomainConfig(
                    "operations",
                    f"Operations domain for supply chain data - {args.env.upper()}",
                ),
            ]

        # Initialize manager and connect
        manager = SnowflakeDataMeshManager(config)

        # Add dry-run support to manager
        manager.dry_run = args.dry_run

        manager.connect()

        try:
            # Setup all domains with environment
            manager.setup_multiple_domains(domains, args.env)

        finally:
            manager.disconnect()

    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
