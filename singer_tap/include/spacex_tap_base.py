import json
from datetime import datetime
from typing import Any, Dict, List

import pytz  # type: ignore
import singer  # type: ignore
import snowflake.connector  # type: ignore
from snowflake.connector.errors import DatabaseError  # type: ignore
from snowflake.connector.errors import Error as SnowflakeError  # type: ignore


class SpaceXTapBase:
    """Base tap class containing the following chore functions"""

    def __init__(self, base_url: str, config_path: str):
        """Class constructor

        Args:
        - base_url (str) : The root url of v4 SpaceX API
        - config_path (str) : Config file that contains database credentials

        Raises:
        - ValueError: If base_url is empty or invalid
        - FileNotFoundError: If config_path is empty or file doesn't exist
        """
        if not base_url or not base_url.strip():
            raise ValueError("base_url cannot be empty")
        if not config_path or not config_path.strip():
            raise ValueError("config_path cannot be empty")

        self.base_url = base_url.rstrip("/") + "/"  # Ensure single trailing slash
        self.snowflake_config = self._load_config(config_path)
        self.conn = self._create_snowflake_connection()
        self.state: Dict[str, Dict] = {"bookmarks": {}}
        self._buffer: Dict[str, List[Dict]] = {}  # Buffer for bulk loading
        self.BATCH_SIZE = 1000  # Number of records to accumulate before bulk loading
        self._truncated_tables: set = set()  # Track which tables have been truncated

    def get_state(self) -> Dict[str, Dict]:
        """Get the current state."""
        return self.state

    def set_state(self, state: Dict) -> None:
        """Set the current state.

        Args:
            state (Dict): The state to set
        """
        self.state = state

    def get_current_time(self) -> datetime:
        """Get current time with UTC timezone."""
        return datetime.now(pytz.UTC)

    def _load_config(self, config_path: str) -> Dict:
        """Load Snowflake configuration."""
        with open(config_path, "r") as f:
            config: Dict = json.load(f)
            # Validate required Snowflake configuration
            required_fields = [
                "user",
                "password",
                "account",
                "warehouse",
                "database",
                "schema",
            ]
            missing_fields = [field for field in required_fields if field not in config]
            if missing_fields:
                raise ValueError(
                    f"Missing required Snowflake\
                        configuration fields: {', '.join(missing_fields)}"
                )
            return config

    def _create_snowflake_connection(self) -> snowflake.connector:
        """Create Snowflake connection."""
        try:
            # Establish connection
            conn = snowflake.connector.connect(
                user=self.snowflake_config["user"],
                password=self.snowflake_config["password"],
                account=self.snowflake_config["account"],
                warehouse=self.snowflake_config["warehouse"],
                database=self.snowflake_config["database"],
                schema=self.snowflake_config["schema"],
                ocsp_response_cache_filename=None,
                validate_default_parameters=True,
            )

            # Test connection with a simple query
            cursor = conn.cursor()
            cursor.execute("SELECT current_version()")
            version = cursor.fetchone()[0]

            return conn if version is not None else conn.close()

        except DatabaseError as e:
            print(f"Failed to connect to Snowflake. Error: {str(e)}")
            return False

    def _prepare_value_for_snowflake(self, value: Any, is_numeric: bool = False) -> str:
        """Prepare a value for insertion into Snowflake.

        Args:
            value: The value to prepare
            is_numeric: Whether the value should be treated as numeric

        Returns:
            str: The prepared value as a string
        """
        if value is None:
            return "-999999999" if is_numeric else "NULL"
        elif isinstance(value, (dict, list)):
            json_str = json.dumps(value).replace("'", "''")
            return f"PARSE_JSON('{json_str}')"
        elif isinstance(value, str):
            escaped_value = value.replace("'", "''")
            return f"'{escaped_value}'"
        elif isinstance(value, (int, float)):
            return str(value)
        else:
            raise TypeError(f"Unsupported type for value: {type(value)}")

    def _flush_buffer(self, stream_name: str) -> None:
        """Flush the buffer for a given stream to Snowflake using bulk insert.

        Args:
            stream_name (str): The name of the stream/table
        """
        if not self._buffer.get(stream_name):
            return

        try:
            cursor = self.conn.cursor()
            records = self._buffer[stream_name]

            if not records:
                return

            # Get columns from first record
            columns = list(records[0].keys())

            # Prepare values for all records
            values_list = []
            for record in records:
                record_values = [
                    self._prepare_value_for_snowflake(record[col]) for col in columns
                ]
                values_list.append(f"SELECT {', '.join(record_values)}")

            # Construct and execute bulk insert query
            insert_query = (
                f"INSERT INTO {stream_name} ({', '.join(columns)}) "
                f"{' UNION ALL '.join(values_list)}"
            )
            cursor.execute(insert_query)
            self.conn.commit()

            # Clear buffer after successful insert
            self._buffer[stream_name] = []

        except Exception as e:
            print(f"Error in bulk insert: {str(e)}")
            raise
        finally:
            cursor.close()

    def _truncate_table(self, table_name: str) -> None:
        """Truncate a Snowflake table.

        Args:
            table_name (str): The name of the table to truncate

        Raises:
            SnowflakeError: If truncate operation fails
        """
        try:
            cursor = self.conn.cursor()
            # Log truncate operation
            singer.get_logger().info(f"Truncating table {table_name}")

            # Execute truncate
            cursor.execute(f"TRUNCATE TABLE {table_name}")
            self.conn.commit()

            # Mark table as truncated
            self._truncated_tables.add(table_name)
            singer.get_logger().info(f"Successfully truncated table {table_name}")

        except SnowflakeError as e:
            error_msg = f"Error truncating table {table_name}: {str(e)}"
            singer.get_logger().error(error_msg)
            self.log_error(table_name, error_msg)
            raise
        finally:
            cursor.close()

    def insert_into_snowflake(self, stream_name: str, record: Dict) -> None:
        """Buffer a record for bulk insert into Snowflake.

        Args:
            stream_name (str): The name of the target table
            record (Dict): The record to insert
        """
        # Truncate table before first insertion if not already truncated
        if stream_name not in self._truncated_tables:
            self._truncate_table(stream_name)

        # Initialize buffer for stream if not exists
        if stream_name not in self._buffer:
            self._buffer[stream_name] = []

        # Add record to buffer
        self._buffer[stream_name].append(record)

        # Flush if buffer reaches batch size
        if len(self._buffer[stream_name]) >= self.BATCH_SIZE:
            self._flush_buffer(stream_name)

    def log_error(
        self, table_name: str, error_message: str, error_data: Dict = {}
    ) -> None:
        """Log error to Snowflake STG_SPACEX_DATA_LOAD_ERRORS table."""
        try:
            cursor = self.conn.cursor()
            error_time = datetime.now(pytz.UTC).isoformat()

            insert_query = (
                "INSERT INTO STG_SPACEX_DATA_LOAD_ERRORS ("
                "TABLE_NAME, ERROR_TIME, ERROR_MESSAGE, ERROR_DATA"
                ") VALUES (%s, %s, %s, %s)"
            )

            error_data_json = json.dumps(error_data) if error_data else None
            cursor.execute(
                insert_query,
                (table_name, error_time, str(error_message), error_data_json),
            )

            self.conn.commit()

        except SnowflakeError as e:
            singer.get_logger().error(
                f"Error logging to STG_SPACEX_DATA_LOAD_ERRORS table: {str(e)}"
            )
        finally:
            cursor.close()

    def close_connection(self) -> None:
        """Close Snowflake connection after flushing any remaining buffered records."""
        try:
            # Flush any remaining records in buffers
            for stream_name in list(self._buffer.keys()):
                self._flush_buffer(stream_name)
        finally:
            if self.conn:
                self.conn.close()
