import json

import requests  # type: ignore
import singer  # type: ignore

from .spacex_tap_base import SpaceXTapBase


class RoadsterTap(SpaceXTapBase):
    """RoadsterTap is a SpaceXTapBase sub class in charge of \
        SpaceX Roadsters entity ingestion

    Args:
    - base_url (str) : The root url of v4 SpaceX API
    - config_path (str) : Config file that contains database credentials
    """

    def __init__(self, base_url: str, config_path: str):
        """Inherit base_url and config_path from SpaceXTapBase"""
        super().__init__(base_url, config_path)

    def fetch_roadster(self) -> None:
        """Fetch and process roadster data from SpaceX API with \
            Snowflake-compatible schema."""
        stream_name = "STG_SPACEX_DATA_ROADSTER"

        try:
            # Fetch data from the roadster endpoint
            response = requests.get(self.base_url + "roadster")
            response.raise_for_status()
            roadster_data = response.json()

            # Schema definition with Snowflake-compatible types
            schema = {
                "type": "object",
                "properties": {
                    "ROADSTER_ID": {
                        "type": ["string", "null"],
                        "description": "Unique identifier for the roadster",
                    },
                    "NAME": {"type": ["string", "null"], "maxLength": 256},
                    "LAUNCH_DATE_UTC": {
                        "type": ["string", "null"],
                        "format": "date-time",
                    },
                    "LAUNCH_DATE_UNIX": {"type": ["integer", "null"]},
                    "LAUNCH_MASS_KG": {"type": ["number", "null"]},
                    "LAUNCH_MASS_LBS": {"type": ["number", "null"]},
                    "NORAD_ID": {"type": ["integer", "null"]},
                    "EPOCH_JD": {
                        "type": ["number", "null"],
                        "description": "Julian Date of epoch",
                    },
                    "ORBIT_TYPE": {"type": ["string", "null"]},
                    "APOAPSIS_AU": {"type": ["number", "null"]},
                    "PERIAPSIS_AU": {"type": ["number", "null"]},
                    "SEMI_MAJOR_AXIS_AU": {"type": ["number", "null"]},
                    "ECCENTRICITY": {"type": ["number", "null"]},
                    "INCLINATION": {"type": ["number", "null"]},
                    "LONGITUDE": {"type": ["number", "null"]},
                    "PERIOD_DAYS": {"type": ["number", "null"]},
                    "SPEED_KPH": {"type": ["number", "null"]},
                    "SPEED_MPH": {"type": ["number", "null"]},
                    "EARTH_DISTANCE_KM": {"type": ["number", "null"]},
                    "EARTH_DISTANCE_MI": {"type": ["number", "null"]},
                    "MARS_DISTANCE_KM": {"type": ["number", "null"]},
                    "MARS_DISTANCE_MI": {"type": ["number", "null"]},
                    "WIKIPEDIA": {"type": ["string", "null"]},
                    "DETAILS": {"type": ["string", "null"]},
                    "VIDEO": {
                        "type": ["string", "null"],
                        "description": "URL of video",
                    },
                    "FLICKR_IMAGES": {
                        "type": ["string", "null"],
                        "description": "Array of image URLs stored as JSON string",
                    },
                    "CREATED_AT": {"type": ["string", "null"]},
                    "UPDATED_AT": {"type": ["string", "null"]},
                    "RAW_DATA": {"type": ["string", "null"]},
                },
            }

            # Write schema
            singer.write_schema(
                stream_name=stream_name, schema=schema, key_properties=["ROADSTER_ID"]
            )

            # Get current time with timezone
            current_time = self.get_current_time()
            current_time_str = current_time.isoformat()

            # Transform data for Snowflake compatibility
            transformed_roadster = {
                "ROADSTER_ID": roadster_data.get("id"),
                "NAME": roadster_data.get("name"),
                "LAUNCH_DATE_UTC": roadster_data.get("launch_date_utc"),
                "LAUNCH_DATE_UNIX": self._prepare_value_for_snowflake(
                    roadster_data.get("launch_date_unix"), is_numeric=True
                ),
                "LAUNCH_MASS_KG": self._prepare_value_for_snowflake(
                    roadster_data.get("launch_mass_kg"), is_numeric=True
                ),
                "LAUNCH_MASS_LBS": self._prepare_value_for_snowflake(
                    roadster_data.get("launch_mass_lbs"), is_numeric=True
                ),
                "NORAD_ID": self._prepare_value_for_snowflake(
                    roadster_data.get("norad_id"), is_numeric=True
                ),
                "EPOCH_JD": self._prepare_value_for_snowflake(
                    roadster_data.get("epoch_jd"), is_numeric=True
                ),
                "ORBIT_TYPE": roadster_data.get("orbit_type"),
                "APOAPSIS_AU": self._prepare_value_for_snowflake(
                    roadster_data.get("apoapsis_au"), is_numeric=True
                ),
                "PERIAPSIS_AU": self._prepare_value_for_snowflake(
                    roadster_data.get("periapsis_au"), is_numeric=True
                ),
                "SEMI_MAJOR_AXIS_AU": self._prepare_value_for_snowflake(
                    roadster_data.get("semi_major_axis_au"), is_numeric=True
                ),
                "ECCENTRICITY": self._prepare_value_for_snowflake(
                    roadster_data.get("eccentricity"), is_numeric=True
                ),
                "INCLINATION": self._prepare_value_for_snowflake(
                    roadster_data.get("inclination"), is_numeric=True
                ),
                "LONGITUDE": self._prepare_value_for_snowflake(
                    roadster_data.get("longitude"), is_numeric=True
                ),
                "PERIOD_DAYS": self._prepare_value_for_snowflake(
                    roadster_data.get("period_days"), is_numeric=True
                ),
                "SPEED_KPH": self._prepare_value_for_snowflake(
                    roadster_data.get("speed_kph"), is_numeric=True
                ),
                "SPEED_MPH": self._prepare_value_for_snowflake(
                    roadster_data.get("speed_mph"), is_numeric=True
                ),
                "EARTH_DISTANCE_KM": self._prepare_value_for_snowflake(
                    roadster_data.get("earth_distance_km"), is_numeric=True
                ),
                "EARTH_DISTANCE_MI": self._prepare_value_for_snowflake(
                    roadster_data.get("earth_distance_mi"), is_numeric=True
                ),
                "MARS_DISTANCE_KM": self._prepare_value_for_snowflake(
                    roadster_data.get("mars_distance_km"), is_numeric=True
                ),
                "MARS_DISTANCE_MI": self._prepare_value_for_snowflake(
                    roadster_data.get("mars_distance_mi"), is_numeric=True
                ),
                "WIKIPEDIA": roadster_data.get("wikipedia"),
                "DETAILS": roadster_data.get("details"),
                "VIDEO": roadster_data.get("video"),
                "FLICKR_IMAGES": json.dumps(roadster_data.get("flickr_images", [])),
                "CREATED_AT": current_time_str,
                "UPDATED_AT": current_time_str,
                "RAW_DATA": json.dumps(roadster_data),
            }

            # Write record with timezone-aware timestamp
            singer.write_record(
                stream_name=stream_name,
                record=transformed_roadster,
                time_extracted=current_time,
            )

            # Insert data into Snowflake
            self.insert_into_snowflake(stream_name, transformed_roadster)

            # Write state
            state = {"STG_SPACEX_DATA_ROADSTER": {"last_sync": current_time_str}}
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
