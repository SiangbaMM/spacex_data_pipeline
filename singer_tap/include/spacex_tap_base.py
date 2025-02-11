import singer                               # type: ignore                                
import snowflake.connector                  # type: ignore
import json
import pytz                                 # type: ignore
from datetime import datetime
from typing import Dict, Any
from snowflake.connector.errors import DatabaseError, Error as SnowflakeError          # type: ignore



class SpaceXTapBase:
    def __init__(self, base_url: str, config_path: str):
        self.base_url = base_url
        self.snowflake_config = self._load_config(config_path)
        self.conn = self._create_snowflake_connection()
    
    def get_current_time(self):
        """
        Get current time with UTC timezone.
        """
        return datetime.now(pytz.UTC)


    def _load_config(self, config_path: str) -> Dict:
        """Load Snowflake configuration."""
        with open(config_path, 'r') as f:
            return json.load(f)

    def _create_snowflake_connection(self):
        """
        Create Snowflake connection.
        """
        
        try:
        # Establish connection
            conn = snowflake.connector.connect(
                user=self.snowflake_config['user'],
                password=self.snowflake_config['password'],
                account=self.snowflake_config['account'],
                warehouse=self.snowflake_config['warehouse'],
                database=self.snowflake_config['database'],
                schema=self.snowflake_config['schema'],
                ocsp_response_cache_filename=None,
                validate_default_parameters=True
            )
            
            # Test connection with a simple query
            cursor = conn.cursor()
            cursor.execute('SELECT current_version()')
            version = cursor.fetchone()[0]
            
            return conn if version is not None else conn.close()
        
        except DatabaseError as e:
            print(f"Failed to connect to Snowflake. Error: {str(e)}")
            return False
        

    def log_error(self, table_name: str, error_message: str, error_data: Dict = None):
        """Log error to Snowflake STG_SPACEX_DATA_LOAD_ERRORS table."""
        try:
            cursor = self.conn.cursor()
            error_time = datetime.now(pytz.UTC).isoformat()
            
            insert_query = """
            INSERT INTO STG_SPACEX_DATA_LOAD_ERRORS (
                TABLE_NAME,
                ERROR_TIME,
                ERROR_MESSAGE,
                ERROR_DATA
            )
            VALUES (
                %s,
                %s,
                %s,
                %s
            )
            """
            
            error_data_json = json.dumps(error_data) if error_data else None
            cursor.execute(
                insert_query,
                (
                    table_name,
                    error_time,
                    str(error_message),
                    error_data_json
                )
            )
            
            self.conn.commit()
        except SnowflakeError as e:
            singer.get_logger().error(f"Error logging to STG_SPACEX_DATA_LOAD_ERRORS table: {str(e)}")
        finally:
            cursor.close()

    def close_connection(self):
        """Close Snowflake connection."""
        if self.conn:
            self.conn.close()
