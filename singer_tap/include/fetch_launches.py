import json

import requests  # type: ignore
import singer  # type: ignore

from .spacex_tap_base import SpaceXTapBase


class LaunchesTap(SpaceXTapBase):
    """LaunchesTap is a SpaceXTapBase sub class in charge of \
        SpaceX Launches entity ingestion

    Args:
    - base_url (str) : The root url of v4 SpaceX API
    - config_path (str) : Config file that contains database credentials
    """

    def __init__(self, base_url: str, config_path: str):
        """Inherit base_url and config_path from SpaceXTapBase"""
        super().__init__(base_url, config_path)

    def fetch_launches(self) -> None:
        """Fetch and process launches data from SpaceX API with \
            Snowflake-compatible schema."""
        stream_name = "STG_SPACEX_DATA_LAUNCHES"
        try:
            # Fetch data from the launches endpoint
            response = requests.get(self.base_url + "launches")
            response.raise_for_status()
            launches_data = response.json()

            # Schema definition with Snowflake-compatible types
            schema = {
                "type": "object",
                "properties": {
                    "LAUNCH_ID": {
                        "type": ["string", "null"],
                        "description": "Unique identifier for the launch",
                    },
                    "FLIGHT_NUMBER": {"type": ["integer", "null"]},
                    "NAME": {"type": ["string", "null"], "maxLength": 256},
                    "DATE_UTC": {"type": ["string", "null"], "format": "date-time"},
                    "DATE_UNIX": {"type": ["integer", "null"]},
                    "DATE_LOCAL": {"type": ["string", "null"]},
                    "DATE_PRECISION": {"type": ["string", "null"]},
                    "STATIC_FIRE_DATE_UTC": {
                        "type": ["string", "null"],
                        "format": "date-time",
                    },
                    "STATIC_FIRE_DATE_UNIX": {"type": ["integer", "null"]},
                    "NET": {"type": ["boolean", "null"]},
                    "WINDOW": {"type": ["integer", "null"]},
                    "ROCKET": {"type": ["string", "null"]},
                    "SUCCESS": {"type": ["boolean", "null"]},
                    "FAILURES": {
                        "type": ["string", "null"],
                        "description": "Array of failure details stored as JSON string",
                    },
                    "UPCOMING": {"type": ["boolean", "null"]},
                    "DETAILS": {"type": ["string", "null"]},
                    "FAIRINGS": {
                        "type": ["string", "null"],
                        "description": "Fairings details stored as JSON string",
                    },
                    "CREW": {
                        "type": ["string", "null"],
                        "description": "Array of crew details stored as JSON string",
                    },
                    "SHIPS": {
                        "type": ["string", "null"],
                        "description": "Array of ship IDs stored as JSON string",
                    },
                    "CAPSULES": {
                        "type": ["string", "null"],
                        "description": "Array of capsule IDs stored as JSON string",
                    },
                    "PAYLOADS": {
                        "type": ["string", "null"],
                        "description": "Array of payload IDs stored as JSON string",
                    },
                    "LAUNCHPAD": {"type": ["string", "null"]},
                    "CORES": {
                        "type": ["string", "null"],
                        "description": "Array of core details stored as JSON string",
                    },
                    "LINKS": {
                        "type": ["string", "null"],
                        "description": "Related links stored as JSON string",
                    },
                    "AUTO_UPDATE": {"type": ["boolean", "null"]},
                    "LAUNCH_LIBRARY_ID": {"type": ["string", "null"]},
                    "CREATED_AT": {"type": ["string", "null"]},
                    "UPDATED_AT": {"type": ["string", "null"]},
                    "RAW_DATA": {"type": ["string", "null"]},
                },
            }

            # Write schema
            singer.write_schema(
                stream_name=stream_name, schema=schema, key_properties=["LAUNCH_ID"]
            )

            # Get current time with timezone
            current_time = self.get_current_time()
            current_time_str = current_time.isoformat()

            # Process and write each launch record
            for launch in launches_data:
                try:
                    # Transform data for Snowflake compatibility
                    transformed_launch = {
                        "LAUNCH_ID": launch.get("id"),
                        "FLIGHT_NUMBER": launch.get("flight_number"),
                        "NAME": launch.get("name"),
                        "DATE_UTC": launch.get("date_utc"),
                        "DATE_UNIX": launch.get("date_unix"),
                        "DATE_LOCAL": launch.get("date_local"),
                        "DATE_PRECISION": launch.get("date_precision"),
                        "STATIC_FIRE_DATE_UTC": launch.get("static_fire_date_utc"),
                        "STATIC_FIRE_DATE_UNIX": launch.get("static_fire_date_unix"),
                        "NET": launch.get("net"),
                        "WINDOW": launch.get("window"),
                        "ROCKET": launch.get("rocket"),
                        "SUCCESS": launch.get("success"),
                        "FAILURES": json.dumps(launch.get("failures", [])),
                        "UPCOMING": launch.get("upcoming"),
                        "DETAILS": launch.get("details"),
                        "FAIRINGS": json.dumps(launch.get("fairings", {})),
                        "CREW": json.dumps(launch.get("crew", [])),
                        "SHIPS": json.dumps(launch.get("ships", [])),
                        "CAPSULES": json.dumps(launch.get("capsules", [])),
                        "PAYLOADS": json.dumps(launch.get("payloads", [])),
                        "LAUNCHPAD": launch.get("launchpad"),
                        "CORES": json.dumps(launch.get("cores", [])),
                        "LINKS": json.dumps(launch.get("links", {})),
                        "AUTO_UPDATE": launch.get("auto_update"),
                        "LAUNCH_LIBRARY_ID": launch.get("launch_library_id"),
                        "CREATED_AT": current_time_str,
                        "UPDATED_AT": current_time_str,
                        "RAW_DATA": json.dumps(launch),
                    }

                    # Write record with timezone-aware timestamp
                    singer.write_record(
                        stream_name=stream_name, record=transformed_launch
                    )

                    # Insert data into Snowflake
                    self.insert_into_snowflake(stream_name, transformed_launch)
                except Exception as transform_error:
                    self.log_error(
                        table_name=stream_name,
                        error_message=f"Data transformation error: \
                            {str(transform_error)}",
                        error_data=launch,
                    )
                    continue  # Continue processing other launches

            # Write state
            state = {"STG_SPACEX_DATA_LAUNCHES": {"last_sync": current_time_str}}
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
