import json

import requests  # type: ignore
import singer  # type: ignore
from include.spacex_tap_base import SpaceXTapBase


class StarlinkTap(SpaceXTapBase):
    """StarlinkTap is a SpaceXTapBase sub class in charge of \
        SpaceX Starlink entity ingestion

    Args:
    - base_url (str) : The root url of v4 SpaceX API
    - config_path (str) : Config file that contains database credentials
    """

    def __init__(self, base_url: str, config_path: str):
        """Inherit base_url and config_path from SpaceXTapBase"""
        super().__init__(base_url, config_path)

    def fetch_starlink(self) -> None:
        """Fetch and process Starlink satellites data from SpaceX API with \
            Snowflake-compatible schema."""
        stream_name = "STG_SPACEX_DATA_STARLINK"

        try:
            # Fetch data from the starlink endpoint
            response = requests.get(self.base_url + "starlink")
            response.raise_for_status()
            starlink_data = response.json()

            # Schema definition with Snowflake-compatible types
            schema = {
                "type": "object",
                "properties": {
                    "STARLINK_ID": {
                        "type": ["string", "null"],
                        "description": "Unique identifier for the Starlink satellite",
                    },
                    "VERSION": {"type": ["string", "null"], "maxLength": 50},
                    "LAUNCH": {
                        "type": ["string", "null"],
                        "description": "Associated launch ID",
                    },
                    "LONGITUDE": {"type": ["number", "null"]},
                    "LATITUDE": {"type": ["number", "null"]},
                    "HEIGHT_KM": {"type": ["number", "null"]},
                    "VELOCITY_KMS": {"type": ["number", "null"]},
                    "SPACETRACK": {
                        "type": ["string", "null"],
                        "description": "Space-Track.org data stored as JSON string",
                    },
                    "LAUNCH_DATE": {"type": ["string", "null"], "format": "date-time"},
                    "OBJECT_NAME": {"type": ["string", "null"], "maxLength": 100},
                    "OBJECT_ID": {"type": ["string", "null"], "maxLength": 50},
                    "EPOCH": {"type": ["string", "null"], "format": "date-time"},
                    "PERIOD_MIN": {"type": ["number", "null"]},
                    "INCLINATION_DEG": {"type": ["number", "null"]},
                    "APOAPSIS_KM": {"type": ["number", "null"]},
                    "PERIAPSIS_KM": {"type": ["number", "null"]},
                    "ECCENTRICITY": {"type": ["number", "null"]},
                    "MEAN_MOTION": {"type": ["number", "null"]},
                    "MEAN_ANOMALY": {"type": ["number", "null"]},
                    "ARG_OF_PERICENTER": {"type": ["number", "null"]},
                    "RAAN": {
                        "type": ["number", "null"],
                        "description": "Right Ascension of the Ascending Node",
                    },
                    "SEMI_MAJOR_AXIS_KM": {"type": ["number", "null"]},
                    "CREATED_AT": {"type": ["string", "null"], "format": "date-time"},
                    "UPDATED_AT": {"type": ["string", "null"], "format": "date-time"},
                    "RAW_DATA": {"type": ["string", "null"]},
                },
            }

            # Write schema
            singer.write_schema(
                stream_name=stream_name, schema=schema, key_properties=["STARLINK_ID"]
            )

            # Get current time with timezone
            current_time = self.get_current_time()
            current_time_str = current_time.isoformat()

            # Process and write each Starlink satellite record
            for satellite in starlink_data:
                try:
                    # Extract spacetrack data if available
                    spacetrack = satellite.get("spaceTrack", {})

                    # Transform data for Snowflake compatibility
                    transformed_satellite = {
                        "STARLINK_ID": satellite.get("id"),
                        "VERSION": satellite.get("version"),
                        "LAUNCH": satellite.get("launch"),
                        "LONGITUDE": satellite.get("longitude"),
                        "LATITUDE": satellite.get("latitude"),
                        "HEIGHT_KM": satellite.get("height_km"),
                        "VELOCITY_KMS": satellite.get("velocity_kms"),
                        "SPACETRACK": json.dumps(spacetrack),
                        "LAUNCH_DATE": spacetrack.get("LAUNCH_DATE"),
                        "OBJECT_NAME": spacetrack.get("OBJECT_NAME"),
                        "OBJECT_ID": spacetrack.get("OBJECT_ID"),
                        "EPOCH": spacetrack.get("EPOCH"),
                        "PERIOD_MIN": spacetrack.get("PERIOD"),
                        "INCLINATION_DEG": spacetrack.get("INCLINATION"),
                        "APOAPSIS_KM": spacetrack.get("APOAPSIS"),
                        "PERIAPSIS_KM": spacetrack.get("PERIAPSIS"),
                        "ECCENTRICITY": spacetrack.get("ECCENTRICITY"),
                        "MEAN_MOTION": spacetrack.get("MEAN_MOTION"),
                        "MEAN_ANOMALY": spacetrack.get("MEAN_ANOMALY"),
                        "ARG_OF_PERICENTER": spacetrack.get("ARG_OF_PERICENTER"),
                        "RAAN": spacetrack.get("RAAN"),
                        "SEMI_MAJOR_AXIS_KM": spacetrack.get("SEMI_MAJOR_AXIS"),
                        "CREATED_AT": current_time_str,
                        "UPDATED_AT": current_time_str,
                        "RAW_DATA": json.dumps(satellite),
                    }

                    # Write record with timezone-aware timestamp
                    singer.write_record(
                        stream_name=stream_name,
                        record=transformed_satellite,
                        time_extracted=current_time,
                    )

                except Exception as transform_error:
                    self.log_error(
                        table_name=stream_name,
                        error_message=f"Data transformation error: \
                            {str(transform_error)}",
                        error_data=satellite,
                    )
                    continue  # Continue processing other satellite

            # Write state
            state = {"STG_SPACEX_DATA_STARLINK": {"last_sync": current_time_str}}
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
