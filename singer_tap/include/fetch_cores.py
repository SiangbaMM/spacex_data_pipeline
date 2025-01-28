import singer                                           # type: ignore
import requests                                         # type: ignore
import json
from include.spacex_tap_base import SpaceXTapBase


class CoresTap(SpaceXTapBase):
    def __init__(self, base_url: str, config_path: str):
        super().__init__(base_url, config_path)

    def fetch_cores(self):
        """Fetch and process core spacex data."""
        stream_name = "STG_SPACEX_DATA_CORES"
        
        try:
            # Fetch data from the cores endpoint
            response = requests.get(self.base_url + "cores")
            response.raise_for_status()
            cores_data = response.json()
        
            # Schema definition with Snowflake-compatible types
            schema = {
                "type": "object",
                "properties": {
                    "CORE_ID": {
                        "type": ["string", "null"],
                        "description": "Unique identifier for the core"
                    },
                    "SERIAL": {
                        "type": ["string", "null"],
                        "maxLength": 256
                    },
                    "BLOCK": {
                        "type": ["integer", "null"]
                    },
                    "STATUS": {
                        "type": ["string", "null"],
                        "maxLength": 50
                    },
                    "REUSE_COUNT": {
                        "type": ["integer", "null"]
                    },
                    "RTLS_ATTEMPTS": {
                        "type": ["integer", "null"]
                    },
                    "RTLS_LANDINGS": {
                        "type": ["integer", "null"]
                    },
                    "ASDS_ATTEMPTS": {
                        "type": ["integer", "null"]
                    },
                    "ASDS_LANDINGS": {
                        "type": ["integer", "null"]
                    },
                    "LAST_UPDATE": {
                        "type": ["string", "null"]
                    },
                    "LAUNCHES": {
                        "type": ["string", "null"],
                        "description": "Array of launch IDs stored as JSON string"
                    },
                    "CREATED_AT": {"type": ["string", "null"]},
                    "UPDATED_AT": {"type": ["string", "null"]},
                    "RAW_DATA": {"type": ["string", "null"]}
                }
            }
        
            # Write schema
            singer.write_schema(
                stream_name=stream_name,
                schema=schema,
                key_properties=["CORE_ID"]
            )
        
            # Get current time with timezone
            current_time = self.get_current_time()
            current_time_str = current_time.isoformat()
        
            # Process and write each core record
            for core in cores_data:
                # Transform data for Snowflake compatibility
                try:
                    transformed_core = {
                        "CORE_ID": core.get("id"),
                        "SERIAL": core.get("serial"),
                        "BLOCK": core.get("block"),
                        "STATUS": core.get("status"),
                        "REUSE_COUNT": core.get("reuse_count"),
                        "RTLS_ATTEMPTS": core.get("rtls_attempts"),
                        "RTLS_LANDINGS": core.get("rtls_landings"),
                        "ASDS_ATTEMPTS": core.get("asds_attempts"),
                        "ASDS_LANDINGS": core.get("asds_landings"),
                        "LAST_UPDATE": core.get("last_update"),
                        "LAUNCHES": json.dumps(core.get("launches", [])),
                        "CREATED_AT": current_time_str,
                        "UPDATED_AT": current_time_str,
                        "RAW_DATA": json.dumps(core)
                    }
            
                    # Write record with timezone-aware timestamp
                    singer.write_record(
                        stream_name=stream_name,
                        record=transformed_core,
                    )
                
                except Exception as transform_error:
                    self.log_error(
                        table_name=stream_name,
                        error_message=f"Data transformation error: {str(transform_error)}",
                        error_data=core
                )
                continue  # Continue processing other capsules
        
            # Write state
            state = {
                "CORES": {
                    "last_sync": current_time.isoformat()
                }
            }
            singer.write_state(state)

        except requests.exceptions.RequestException as api_error:
            self.log_error(
                table_name=stream_name,
                error_message=f"API request error: {str(api_error)}"
            )
            raise

        except Exception as e:
            self.log_error(
                table_name=stream_name,
                error_message=f"Unexpected error: {str(e)}"
            )
            raise
