import json
from datetime import datetime
from typing import Dict, List

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
        """
        self.base_url = base_url
        self.snowflake_config = self._load_config(config_path)
        self.conn = self._create_snowflake_connection()
        self.state: Dict[str, Dict] = {"bookmarks": {}}
        self._buffer: Dict[str, List[Dict]] = {}  # Buffer for bulk loading
        self.BATCH_SIZE = 1000  # Number of records to accumulate before bulk loading

    def get_state(self):
        """Get the current state."""
        return self.state

    def set_state(self, state: Dict):
        """Set the current state.

        Args:
            state (Dict): The state to set
        """
        self.state = state

    def get_current_time(self):
        """Get current time with UTC timezone."""
        return datetime.now(pytz.UTC)

    def _load_config(self, config_path: str) -> Dict:
        """Load Snowflake configuration."""
        with open(config_path, "r") as f:
            return json.load(f)

    def _create_snowflake_connection(self):
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

    def _prepare_value_for_snowflake(self, value) -> str:
        """Prepare a value for insertion into Snowflake.

        Args:
            value: The value to prepare

        Returns:
            str: The prepared value as a string
        """
        if isinstance(value, (dict, list)):
            json_str = json.dumps(value).replace("'", "''")
            return f"PARSE_JSON('{json_str}')"
        elif isinstance(value, str):
            escaped_value = value.replace("'", "''")
            return f"'{escaped_value}'"
        elif value is None:
            return "NULL"
        else:
            return str(value)

    def _flush_buffer(self, stream_name: str):
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

    def insert_into_snowflake(self, stream_name: str, record: Dict):
        """Buffer a record for bulk insert into Snowflake.

        Args:
            stream_name (str): The name of the target table
            record (Dict): The record to insert
        """
        # Initialize buffer for stream if not exists
        if stream_name not in self._buffer:
            self._buffer[stream_name] = []

        # Add record to buffer
        self._buffer[stream_name].append(record)

        # Flush if buffer reaches batch size
        if len(self._buffer[stream_name]) >= self.BATCH_SIZE:
            self._flush_buffer(stream_name)

    def log_error(self, table_name: str, error_message: str, error_data: Dict = {}):
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

    def close_connection(self):
        """Close Snowflake connection after flushing any remaining buffered records."""
        try:
            # Flush any remaining records in buffers
            for stream_name in list(self._buffer.keys()):
                self._flush_buffer(stream_name)
        finally:
            if self.conn:
                self.conn.close()
