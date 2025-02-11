import singer                                           # type: ignore
import requests                                         # type: ignore
import json
from include.spacex_tap_base import SpaceXTapBase


class LandpadsTap(SpaceXTapBase):

    def fetch_landpads(self) -> None:
        """
        Fetch and process landpads data from SpaceX API with Snowflake-compatible schema.
        """
        
        stream_name = "STG_SPACEX_DATA_LANDPADS"
        
        try:
            # Fetch data from the landpads endpoint
            response = requests.get(self.base_url + "landpads")
            response.raise_for_status()
            landpads_data = response.json()
        
            # Schema definition with Snowflake-compatible types
            schema = {
                "type": "object",
                "properties": {
                    "LANDPAD_ID": {
                        "type": ["string", "null"],
                        "description": "Unique identifier for the landing pad"
                    },
                    "NAME": {
                        "type": ["string", "null"],
                        "maxLength": 256
                    },
                    "FULL_NAME": {
                        "type": ["string", "null"],
                        "maxLength": 512
                    },
                    "STATUS": {
                        "type": ["string", "null"],
                        "maxLength": 50
                    },
                    "TYPE": {
                        "type": ["string", "null"],
                        "maxLength": 50
                    },
                    "LOCALITY": {
                        "type": ["string", "null"],
                        "maxLength": 256
                    },
                    "REGION": {
                        "type": ["string", "null"],
                        "maxLength": 256
                    },
                    "LATITUDE": {
                        "type": ["number", "null"]
                    },
                    "LONGITUDE": {
                        "type": ["number", "null"]
                    },
                    "LANDING_ATTEMPTS": {
                        "type": ["integer", "null"]
                    },
                    "LANDING_SUCCESSES": {
                        "type": ["integer", "null"]
                    },
                    "WIKIPEDIA": {
                        "type": ["string", "null"]
                    },
                    "DETAILS": {
                        "type": ["string", "null"]
                    },
                    "LAUNCHES": {
                        "type": ["string", "null"],
                        "description": "Array of launch IDs stored as JSON string"
                    },
                    "IMAGES": {
                        "type": ["string", "null"],
                        "description": "Image URLs stored as JSON string"
                    },
                    "CREATED_AT": {
                        "type": ["string", "null"],
                        "format": "date-time"
                    },
                    "UPDATED_AT": {
                        "type": ["string", "null"],
                        "format": "date-time"
                    },
                    "RAW_DATA": {"type": ["string", "null"]}
                }
            }
        
            # Write schema
            singer.write_schema(
                stream_name=stream_name,
                schema=schema,
                key_properties=["LANDPAD_ID"]
            )
        
            # Get current time with timezone
            current_time = self.get_current_time()
            current_time_str = current_time.isoformat()
        
            # Process and write each landpad record
            for landpad in landpads_data:
                try:
                    # Handle images object specifically
                    images = {}
                    if "images" in landpad:
                        images = {
                            "large": landpad["images"].get("large", []),
                            "small": landpad["images"].get("small", [])
                        }
            
                    # Transform data for Snowflake compatibility
                    transformed_landpad = {
                        "LANDPAD_ID": landpad.get("id"),
                        "NAME": landpad.get("name"),
                        "FULL_NAME": landpad.get("full_name"),
                        "STATUS": landpad.get("status"),
                        "TYPE": landpad.get("type"),
                        "LOCALITY": landpad.get("locality"),
                        "REGION": landpad.get("region"),
                        "LATITUDE": landpad.get("latitude"),
                        "LONGITUDE": landpad.get("longitude"),
                        "LANDING_ATTEMPTS": landpad.get("landing_attempts"),
                        "LANDING_SUCCESSES": landpad.get("landing_successes"),
                        "WIKIPEDIA": landpad.get("wikipedia"),
                        "DETAILS": landpad.get("details"),
                        "LAUNCHES": json.dumps(landpad.get("launches", [])),
                        "IMAGES": json.dumps(images),
                        "CREATED_AT": current_time_str,
                        "UPDATED_AT": current_time_str,
                        "RAW_DATA": json.dumps(landpad)
                    }
            
                    # Write record with timezone-aware timestamp
                    singer.write_record(
                        stream_name=stream_name,
                        record=transformed_landpad,
                        time_extracted=current_time
                    )
                except Exception as transform_error:
                    self.log_error(
                        table_name=stream_name,
                        error_message=f"Data transformation error: {str(transform_error)}",
                        error_data=landpad
                    )
                    continue  # Continue processing other landpad
        
            # Write state
            state = {
                "STG_SPACEX_DATA_LANDPADS": {
                    "last_sync": current_time_str
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
