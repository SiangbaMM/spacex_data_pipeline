#!/usr/bin/env python3
"""
Snowflake Schema Setup Script for Data Mesh Architecture

This script automates the creation of schemas (staging, compute, published)
with their associated roles and permissions for each domain.

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
class SchemaConfig:
    """Configuration for a schema"""

    name: str
    description: Optional[str] = None


@dataclass
class DomainEnvironment:
    """Domain and environment configuration"""

    domain_name: str
    environment: str

    @property
    def prefix(self) -> str:
        """Get the domain environment prefix"""
        return f"{self.domain_name.upper()}_{self.environment.upper()}"

    @property
    def database_name(self) -> str:
        """Get the database name"""
        return self.prefix


class SnowflakeSchemaManager:
    """Manages Snowflake schema setup operations"""

    def __init__(self, config: SnowflakeConfig):
        """Initialize Snowflake schema manager."""
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

    def check_schema_exists_and_permissions(
        self, domain_env: DomainEnvironment, schema_name: str
    ) -> Dict[str, bool]:
        """Check if schema exists and verify permissions using SHOW SCHEMAS"""
        self.logger.info(
            f"Checking schema {schema_name} existence and permissions for domain {domain_env.prefix}"
        )

        # Use domain SYSADMIN role
        self.execute_sql(f"USE ROLE {domain_env.prefix}_SYSADMIN")
        self.execute_sql(f"USE DATABASE {domain_env.database_name}")
        self.execute_sql(f"USE WAREHOUSE {domain_env.prefix}_ADHOC_WH")

        result = {
            "exists": False,
            "has_privileges": False,
            "needs_ownership": False,
            "owner": None,
        }

        try:
            # Show schemas in the database
            self.execute_sql(f"SHOW SCHEMAS IN DATABASE {domain_env.database_name}")

            # Get the last query result to check for our schema
            schemas_result = self.execute_sql(
                "SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))"
            )

            # Look for our schema in the results
            schema_upper = schema_name.upper()
            schema_found = None

            for row in schemas_result:
                if row.get("name", "").upper() == schema_upper:
                    schema_found = row
                    break

            if schema_found:
                result["exists"] = True
                result["owner"] = schema_found.get("owner", "")

                # Check if we are the owner or have privileges
                current_role = f"{domain_env.prefix}_SYSADMIN"
                if result["owner"] == current_role:
                    result["has_privileges"] = True
                    self.logger.info(
                        f"Schema {schema_upper} exists and is owned by {current_role}"
                    )
                else:
                    # Schema exists but owned by someone else
                    result["has_privileges"] = False
                    result["needs_ownership"] = True
                    self.logger.warning(
                        f"Schema {schema_upper} exists but is owned by {result['owner']}, not {current_role}"
                    )
            else:
                # Schema not found in the list
                result["exists"] = False
                result["has_privileges"] = True  # We can create it
                self.logger.info(
                    f"Schema {schema_upper} doesn't exist in database {domain_env.database_name}"
                )

        except (DatabaseError, ProgrammingError) as e:
            # If SHOW SCHEMAS fails, it might be a permissions issue
            error_msg = str(e).lower()
            self.logger.error(f"Error checking schemas: {e}")

            # If we can't even show schemas, we have bigger permission issues
            if (
                "insufficient privileges" in error_msg
                or "permission denied" in error_msg
            ):
                self.logger.error(
                    f"Insufficient privileges to show schemas in database {domain_env.database_name}"
                )
                raise
            else:
                # For other errors, re-raise
                raise

        return result

    def verify_schema_access(
        self, domain_env: DomainEnvironment, schema_name: str
    ) -> bool:
        """Verify we can actually access the schema after ownership change"""
        try:
            # Try to use the schema to verify access
            self.execute_sql(f"USE ROLE {domain_env.prefix}_SYSADMIN")
            self.execute_sql(f"USE DATABASE {domain_env.database_name}")
            self.execute_sql(
                f"USE SCHEMA {domain_env.database_name}.{schema_name.upper()}"
            )

            # Try to show objects in the schema to confirm full access
            self.execute_sql(
                f"SHOW TABLES IN SCHEMA {domain_env.database_name}.{schema_name.upper()}"
            )

            self.logger.info(f"Verified access to schema {schema_name.upper()}")

            return True

        except Exception as e:
            self.logger.warning(
                f"Cannot verify full access to schema {schema_name.upper()}: {e}"
            )
            return False

    def take_schema_ownership(
        self, domain_env: DomainEnvironment, schema_name: str
    ) -> None:
        """Take ownership of existing schema using SYSADMIN role"""
        self.logger.info(
            f"Attempting to take ownership of schema {schema_name.upper()}"
        )

        # Use SYSADMIN role (highest privilege) to take ownership
        self.execute_sql("USE ROLE SYSADMIN")

        schema_full_name = f"{domain_env.database_name}.{schema_name.upper()}"

        try:
            # Grant ownership to domain SYSADMIN
            self.execute_sql(
                f"GRANT OWNERSHIP ON SCHEMA {schema_full_name} TO ROLE {domain_env.prefix}_SYSADMIN"
            )
            self.logger.info(
                f"Successfully took ownership of schema {schema_name.upper()}"
            )

            # Verify the ownership change worked
            if self.verify_schema_access(domain_env, schema_name):
                self.logger.info(
                    f"Ownership transfer verified for schema {schema_name.upper()}"
                )
            else:
                self.logger.warning(
                    f"Ownership transfer may not be complete for schema {schema_name.upper()}"
                )

        except Exception as e:
            self.logger.error(
                f"Failed to take ownership of schema {schema_name.upper()}: {e}"
            )
            raise

    def create_schema(
        self, domain_env: DomainEnvironment, schema_config: SchemaConfig
    ) -> None:
        """Create a schema using domain SYSADMIN role with existence check"""
        self.logger.info(
            f"Creating schema {schema_config.name} for domain {domain_env.prefix}"
        )

        # Check schema existence and permissions
        schema_status = self.check_schema_exists_and_permissions(
            domain_env, schema_config.name
        )

        if schema_status["exists"] and not schema_status["has_privileges"]:
            # Schema exists but no privileges - try to take ownership
            if schema_status["needs_ownership"]:
                self.take_schema_ownership(domain_env, schema_config.name)

        # Use domain SYSADMIN role to create/manage schema
        self.execute_sql(f"USE ROLE {domain_env.prefix}_SYSADMIN")
        self.execute_sql(f"USE DATABASE {domain_env.database_name}")

        # Create schema (this will be a no-op if it already exists and we have
        # privileges)
        schema_sql = f"CREATE SCHEMA IF NOT EXISTS {domain_env.database_name}.{schema_config.name.upper()}"
        if schema_config.description:
            schema_sql += f" COMMENT = '{schema_config.description}'"

        try:
            self.execute_sql(schema_sql)
            if schema_status["exists"]:
                self.logger.info(
                    f"Schema {schema_config.name.upper()} already existed and is now accessible"
                )
            else:
                self.logger.info(
                    f"Schema {schema_config.name.upper()} created successfully"
                )

        except Exception as e:
            self.logger.error(
                f"Failed to create/access schema {schema_config.name.upper()}: {e}"
            )
            raise

    def create_schema_roles(
        self, domain_env: DomainEnvironment, schema_name: str
    ) -> Dict[str, str]:
        """Create schema-specific roles using domain SECADMIN"""
        self.logger.info(
            f"Creating schema roles for {schema_name} in domain {domain_env.prefix}"
        )

        # Use domain SECADMIN role to create roles
        self.execute_sql(f"USE ROLE {domain_env.prefix}_SECADMIN")

        # Define roles to create
        schema_upper = schema_name.upper()
        roles = {
            "read": f"{domain_env.prefix}_{schema_upper}_SR",  # Schema Read
            "write": f"{domain_env.prefix}_{schema_upper}_SW",  # Schema Write
            "full": f"{domain_env.prefix}_{schema_upper}_SF",  # Schema Full
        }

        # Create roles
        for role_type, role_name in roles.items():
            try:
                self.execute_sql(f"CREATE ROLE IF NOT EXISTS {role_name}")
                self.logger.info(f"Role {role_name} ({role_type}) created successfully")
            except Exception as e:
                self.logger.error(f"Failed to create role {role_name}: {e}")
                raise

        return roles

    def setup_role_hierarchy(
        self, domain_env: DomainEnvironment, roles: Dict[str, str]
    ) -> None:
        """Set up role hierarchy - SW can substitute SR, SF can substitute SW"""
        self.logger.info(
            f"Setup role hierarchy for schema roles in domain {domain_env.prefix}"
        )

        # Use domain SECADMIN role to manage hierarchy
        self.execute_sql(f"USE ROLE {domain_env.prefix}_SECADMIN")

        # SW can substitute SR
        self.execute_sql(f"GRANT ROLE {roles['read']} TO ROLE {roles['write']}")

        # SF can substitute SW (which already includes SR)
        self.execute_sql(f"GRANT ROLE {roles['write']} TO ROLE {roles['full']}")

        # Domain SYSADMIN can substitute SF (which already includes SW)
        self.execute_sql(
            f"GRANT ROLE {roles['write']} TO ROLE {domain_env.prefix}_SYSADMIN"
        )

        self.logger.info(
            f"Role hierarchy established: {roles['full']} > {roles['write']} > {roles['read']}"
        )

    def grant_schema_permissions(
        self, domain_env: DomainEnvironment, schema_name: str, roles: Dict[str, str]
    ) -> None:
        """Grant appropriate permissions on schema to roles"""
        self.logger.info(
            f"Granting schema permissions for {schema_name} in database {domain_env.prefix}"
        )

        # Use domain SYSADMIN role to grant permissions
        self.execute_sql(f"USE ROLE {domain_env.prefix}_SYSADMIN")
        self.execute_sql(f"USE DATABASE {domain_env.database_name}")

        schema_full_name = f"{domain_env.database_name}.{schema_name.upper()}"

        # Grant READ permissions to SR role
        read_permissions = ["USAGE"]
        for permission in read_permissions:
            self.execute_sql(
                f"GRANT {permission} ON SCHEMA {schema_full_name} TO ROLE {roles['read']}"
            )

        # Grant WRITE permissions to SW role (includes CREATE privileges)
        self.execute_sql(
            f"REVOKE {permission} ON SCHEMA {schema_full_name} FROM ROLE {roles['write']}"
        )

        write_permissions = [
            "USAGE",
            "CREATE TABLE",
            "CREATE VIEW",
            "CREATE FUNCTION",
            "CREATE PROCEDURE",
        ]
        for permission in write_permissions:
            self.execute_sql(
                f"GRANT {permission} ON SCHEMA {schema_full_name} TO ROLE {roles['write']}"
            )

        # Grant FULL permissions to SF role (ownership for full control)
        self.execute_sql(
            f"GRANT ALL PRIVILEGES ON SCHEMA {schema_full_name} TO ROLE {roles['full']}"
        )

        # Grant privileges on objects that will be created in the schema
        dml_privileges = [
            ("TABLES", "SELECT", roles["read"]),
            ("VIEWS", "SELECT", roles["read"]),
            ("TABLES", "INSERT,UPDATE,DELETE,TRUNCATE", roles["write"]),
            ("VIEWS", "SELECT", roles["write"]),
        ]

        for object_type, privilege, role in dml_privileges:
            self.execute_sql(
                f"GRANT {privilege} ON ALL {object_type} IN SCHEMA {schema_full_name} TO ROLE {role}"
            )
            self.execute_sql(
                f"GRANT {privilege} ON FUTURE {object_type} IN SCHEMA {schema_full_name} TO ROLE {role}"
            )

        self.logger.info(f"Schema permissions granted for {schema_name}")

    def assign_predefined_role_permissions(
        self, domain_env: DomainEnvironment, schema_roles: Dict[str, Dict[str, str]]
    ) -> None:
        """Assign specific permissions to LOAD, TRANSFORM, and ANALYST roles"""
        self.logger.info(
            f"Assigning predefined role permissions for domain {domain_env.prefix}"
        )

        # Use domain SECLADMIN to grant roles
        self.execute_sql(f"USE ROLE {domain_env.prefix}_SECADMIN")

        # LOAD_ROLE permissions
        load_role = f"{domain_env.prefix}_LOAD_ROLE"
        if "staging" in schema_roles:
            # LOAD_ROLE gets FULL access to STAGING
            self.execute_sql(
                f"GRANT ROLE {
                    schema_roles['staging']['full']} TO ROLE {load_role}"
            )
            self.logger.info(
                f"Granted {schema_roles['staging']['full']} to {load_role}"
            )

        # LOAD_ROLE gets READ access to COMPUTE and PUBLISHED
        for schema_name in ["compute", "published"]:
            if schema_name in schema_roles:
                self.execute_sql(
                    f"GRANT ROLE {schema_roles[schema_name]['read']} TO ROLE {load_role}"
                )
                self.logger.info(
                    f"Granted {schema_roles[schema_name]['read']} to {load_role}"
                )

        # TRANSFORM_ROLE permissions
        transform_role = f"{domain_env.prefix}_TRANSFORM_ROLE"
        if "staging" in schema_roles:
            # TRANSFORM_ROLE gets READ access to STAGING
            self.execute_sql(
                f"GRANT ROLE {schema_roles['staging']['read']} TO ROLE {transform_role}"
            )
            self.logger.info(
                f"Granted {schema_roles['staging']['read']} to {transform_role}"
            )

        # TRANSFORM_ROLE gets FULL access to COMPUTE and PUBLISHED
        for schema_name in ["compute", "published"]:
            if schema_name in schema_roles:
                self.execute_sql(
                    f"GRANT ROLE {schema_roles[schema_name]['full']} TO ROLE {transform_role}"
                )
                self.logger.info(
                    f"Granted {schema_roles[schema_name]['full']} to {transform_role}"
                )

        # ANALYST_ROLE permissions
        analyst_role = f"{domain_env.prefix}_ANALYST_ROLE"
        if "published" in schema_roles:
            # ANALYST_ROLE gets READ access to PUBLISHED
            self.execute_sql(
                f"GRANT ROLE {schema_roles['published']['read']} TO ROLE {analyst_role}"
            )
            self.logger.info(
                f"Granted {schema_roles['published']['read']} to {analyst_role}"
            )

    def setup_schema(
        self, domain_env: DomainEnvironment, schema_config: SchemaConfig
    ) -> Dict[str, str]:
        """Complete setup for a single schema"""
        self.logger.info(
            f"Starting complete setup for schema: {schema_config.name} in domain {domain_env.prefix}"
        )

        try:
            # Step 1: Create schema
            self.create_schema(domain_env, schema_config)

            # Step 2: Create schema-specific roles
            roles = self.create_schema_roles(domain_env, schema_config.name)

            # Step 3: Setup role hierarchy
            self.setup_role_hierarchy(domain_env, roles)

            # Step 4: Grant schema permissions
            self.grant_schema_permissions(domain_env, schema_config.name, roles)

            self.logger.info(
                f"Schema {schema_config.name} setup completed successfully in domain {domain_env.prefix}"
            )
            return roles

        except Exception as e:
            self.logger.error(
                f"Failed to setup schema {schema_config.name} in domain {domain_env.prefix}: {e}"
            )
            raise

    def setup_multiple_schemas(
        self, domain_env: DomainEnvironment, schemas: List[SchemaConfig]
    ) -> None:
        """Set up multiple schemas for a domain"""
        self.logger.info(
            f"Setting up {len(schemas)} schemas for domain {domain_env.prefix}"
        )

        # Keep track of all schema roles for predefined role assignments
        all_schema_roles = {}

        for schema_config in schemas:
            try:
                roles = self.setup_schema(domain_env, schema_config)
                all_schema_roles[schema_config.name.lower()] = roles
            except Exception as e:
                self.logger.error(
                    f"Failed to setup schema {schema_config.name}, continuing with next schema : {e}"
                )
                continue

        # Assign permissions to predefined roles (LOAD, TRANSFORM, ANALYST)
        try:
            self.assign_predefined_role_permissions(domain_env, all_schema_roles)
        except Exception as e:
            self.logger.error(f"Failed to assign predefined role permissions: {e}")

        self.logger.info(f"Schema setup completed for domain {domain_env.prefix}")


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
        description="Setup Snowflake Schemas for Data Mesh Architecture",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Basic usage - create all default schemas (staging, compute, published)
  python snowflake_schema_setup.py --account myaccount --user myuser --domain sales --env dev

  # Create specific schemas
  python snowflake_schema_setup.py --account myaccount --user myuser --domain sales --env dev --schemas staging compute

  # Dry run to see what would be created
  python snowflake_schema_setup.py --account myaccount --user myuser --domain sales --env dev --dry-run

  # Using environment variables
  export SNOWFLAKE_ACCOUNT=myaccount
  python snowflake_schema_setup.py --use-env --domain sales --env dev
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

    # Domain and environment arguments
    parser.add_argument(
        "--domain", "-d", type=str, required=True, help="Domain name (required)"
    )

    parser.add_argument(
        "--env",
        "-e",
        type=str,
        required=True,
        choices=["dev", "uat", "prd"],
        help="Target environment (required): dev, uat, or prd",
    )

    # Schema configuration
    parser.add_argument(
        "--schemas",
        "-s",
        type=str,
        nargs="+",
        help="List of schema names to setup (default: staging, compute, published)",
    )

    # Configuration mode
    parser.add_argument(
        "--use-env",
        action="store_true",
        help="Use environment variables for connection config",
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
    """Execute the schema setup"""
    # Parse command line arguments
    args = parse_arguments()

    # Configure logging based on verbosity
    today = datetime.now().strftime("%Y-%m-%d")
    log_dir = os.path.join(os.path.dirname(__file__), "..")
    log_file = os.path.join(
        log_dir, "logs", f"snowflake_schema_setup_{args.domain}_{args.env}_{today}.log"
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

        # Create domain environment object
        domain_env = DomainEnvironment(args.domain, args.env)

        # Define schemas to setup
        if args.schemas:
            # Use schemas from command line
            schemas = [SchemaConfig(schema.lower()) for schema in args.schemas]
        else:
            # Use default schemas
            schemas = [
                SchemaConfig("staging", "Schema for staging data ingestion"),
                SchemaConfig(
                    "compute", "Schema for data transformation and computation"
                ),
                SchemaConfig("published", "Schema for published data products"),
            ]

        # Initialize manager and connect
        manager = SnowflakeSchemaManager(config)

        # Add dry-run support to manager
        manager.dry_run = args.dry_run

        manager.connect()

        try:
            # Setup all schemas
            manager.setup_multiple_schemas(domain_env, schemas)
            print(
                f"Schema setup completed successfully for domain {domain_env.prefix}!"
            )

        finally:
            manager.disconnect()

    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
