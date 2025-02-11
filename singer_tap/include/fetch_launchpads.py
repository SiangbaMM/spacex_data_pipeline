import json

import requests  # type: ignore
import singer  # type: ignore
from include.spacex_tap_base import SpaceXTapBase


class LaunchpadsTap(SpaceXTapBase):
    """LaunchpadsTap is a SpaceXTapBase sub class in charge of \
        SpaceX Launchpads entity ingestion

    Args:
    - base_url (str) : The root url of v4 SpaceX API
    - config_path (str) : Config file that contains database credentials
    """

    def __init__(self, base_url: str, config_path: str):
        """Inherit base_url and config_path from SpaceXTapBase"""
        super().__init__(base_url, config_path)

    def fetch_launchpads(self) -> None:
        """Fetch and process launchpads data from SpaceX API with \
            Snowflake-compatible schema."""
        stream_name = "STG_SPACEX_DATA_LAUNCHPADS"

        try:
            # Fetch data from the launchpads endpoint
            response = requests.get(self.base_url + "launchpads")
            response.raise_for_status()
            launchpads_data = response.json()

            # Schema definition with Snowflake-compatible types
            schema = {
                "type": "object",
                "properties": {
                    "LAUNCHPAD_ID": {
                        "type": ["string", "null"],
                        "description": "Unique identifier for the launch pad",
                    },
                    "NAME": {"type": ["string", "null"], "maxLength": 256},
                    "FULL_NAME": {"type": ["string", "null"], "maxLength": 512},
                    "STATUS": {"type": ["string", "null"], "maxLength": 50},
                    "LOCALITY": {"type": ["string", "null"], "maxLength": 256},
                    "REGION": {"type": ["string", "null"], "maxLength": 256},
                    "TIMEZONE": {"type": ["string", "null"], "maxLength": 50},
                    "LATITUDE": {"type": ["number", "null"]},
                    "LONGITUDE": {"type": ["number", "null"]},
                    "LAUNCH_ATTEMPTS": {"type": ["integer", "null"]},
                    "LAUNCH_SUCCESSES": {"type": ["integer", "null"]},
                    "ROCKETS": {
                        "type": ["string", "null"],
                        "description": "Array of rocket IDs stored as JSON string",
                    },
                    "LAUNCHES": {
                        "type": ["string", "null"],
                        "description": "Array of launch IDs stored as JSON string",
                    },
                    "DETAILS": {"type": ["string", "null"]},
                    "IMAGES": {
                        "type": ["string", "null"],
                        "description": "Image URLs stored as JSON string",
                    },
                    "CREATED_AT": {"type": ["string", "null"]},
                    "UPDATED_AT": {"type": ["string", "null"]},
                    "RAW_DATA": {"type": ["string", "null"]},
                },
            }

            # Write schema
            singer.write_schema(
                stream_name=stream_name, schema=schema, key_properties=["LAUNCHPAD_ID"]
            )

            # Get current time with timezone
            current_time = self.get_current_time()
            current_time_str = current_time.isoformat()

            # Process and write each launchpad record
            for launchpad in launchpads_data:
                try:
                    # Handle images object specifically
                    images = {}
                    if "images" in launchpad:
                        images = {
                            "large": launchpad["images"].get("large", []),
                            "small": launchpad["images"].get("small", []),
                        }

                    # Transform data for Snowflake compatibility
                    transformed_launchpad = {
                        "LAUNCHPAD_ID": launchpad.get("id"),
                        "NAME": launchpad.get("name"),
                        "FULL_NAME": launchpad.get("full_name"),
                        "STATUS": launchpad.get("status"),
                        "LOCALITY": launchpad.get("locality"),
                        "REGION": launchpad.get("region"),
                        "TIMEZONE": launchpad.get("timezone"),
                        "LATITUDE": launchpad.get("latitude"),
                        "LONGITUDE": launchpad.get("longitude"),
                        "LAUNCH_ATTEMPTS": launchpad.get("launch_attempts"),
                        "LAUNCH_SUCCESSES": launchpad.get("launch_successes"),
                        "ROCKETS": json.dumps(launchpad.get("rockets", [])),
                        "LAUNCHES": json.dumps(launchpad.get("launches", [])),
                        "DETAILS": launchpad.get("details"),
                        "IMAGES": json.dumps(images),
                        "CREATED_AT": current_time_str,
                        "UPDATED_AT": current_time_str,
                        "RAW_DATA": json.dumps(launchpad),
                    }

                    # Write record with timezone-aware timestamp
                    singer.write_record(
                        stream_name=stream_name, record=transformed_launchpad
                    )

                except Exception as transform_error:
                    self.log_error(
                        table_name=stream_name,
                        error_message=f"Data transformation error: \
                            {str(transform_error)}",
                        error_data=launchpad,
                    )
                    continue  # Continue processing other launchpad

            # Write state
            state = {"STG_SPACEX_DATA_LAUNCHPADS": {"last_sync": current_time_str}}
            singer.write_state(state)

        except requests.exceptions.RequestException as api_error:
            self.log_error(
                table_name=stream_name,
                error_message=f"API request error: {str(api_error)}",
            )
            raise

        except Exception as e:
            self.log_error(
                table_name=stream_name, error_message=f"Unexpected error: {str(e)}"
            )
            raise
