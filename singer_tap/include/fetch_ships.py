import json

import requests  # type: ignore
import singer  # type: ignore

from .spacex_tap_base import SpaceXTapBase


class ShipsTap(SpaceXTapBase):
    """shipsTap is a SpaceXTapBase sub class in charge of \
        SpaceX ships entity ingestion

    Args:
    - base_url (str) : The root url of v4 SpaceX API
    - config_path (str) : Config file that contains database credentials
    """

    def __init__(self, base_url: str, config_path: str):
        """Inherit base_url and config_path from SpaceXTapBase"""
        super().__init__(base_url, config_path)

    def fetch_ships(self) -> None:
        """Fetch and process ships data from SpaceX API with \
            Snowflake-compatible schema."""
        stream_name = "STG_SPACEX_DATA_SHIPS"

        try:
            # Fetch data from the ships endpoint
            response = requests.get(self.base_url + "ships")
            response.raise_for_status()
            ships_data = response.json()

            # Schema definition with Snowflake-compatible types
            schema = {
                "type": "object",
                "properties": {
                    "SHIP_ID": {
                        "type": ["string", "null"],
                        "description": "Unique identifier for the ship",
                    },
                    "NAME": {"type": ["string", "null"], "maxLength": 256},
                    "LEGACY_ID": {
                        "type": ["string", "null"],
                        "description": "Legacy identifier if exists",
                    },
                    "MODEL": {"type": ["string", "null"], "maxLength": 100},
                    "TYPE": {"type": ["string", "null"], "maxLength": 100},
                    "ACTIVE": {"type": ["boolean", "null"]},
                    "IMO": {
                        "type": ["integer", "null"],
                        "description": "International Maritime Organization number",
                    },
                    "MMSI": {
                        "type": ["integer", "null"],
                        "description": "Maritime Mobile Service Identity number",
                    },
                    "ABS": {
                        "type": ["integer", "null"],
                        "description": "American Bureau of Shipping identification",
                    },
                    "CLASS": {"type": ["integer", "null"]},
                    "MASS_KG": {"type": ["integer", "null"]},
                    "MASS_LBS": {"type": ["integer", "null"]},
                    "YEAR_BUILT": {"type": ["integer", "null"]},
                    "HOME_PORT": {"type": ["string", "null"], "maxLength": 100},
                    "STATUS": {"type": ["string", "null"], "maxLength": 100},
                    "SPEED_KN": {
                        "type": ["number", "null"],
                        "description": "Speed in knots",
                    },
                    "COURSE_DEG": {
                        "type": ["number", "null"],
                        "description": "Course in degrees",
                    },
                    "LATITUDE": {"type": ["number", "null"]},
                    "LONGITUDE": {"type": ["number", "null"]},
                    "LAST_AIS_UPDATE": {
                        "type": ["string", "null"],
                        "format": "date-time",
                        "description": "Last AIS update timestamp",
                    },
                    "LINK": {
                        "type": ["string", "null"],
                        "description": "URL to Marine Traffic page",
                    },
                    "IMAGE": {
                        "type": ["string", "null"],
                        "description": "URL to ship image",
                    },
                    "LAUNCHES": {
                        "type": ["string", "null"],
                        "description": "Array of launch IDs stored as JSON string",
                    },
                    "ROLES": {
                        "type": ["string", "null"],
                        "description": "Array of roles stored as JSON string",
                    },
                    "CREATED_AT": {"type": ["string", "null"], "format": "date-time"},
                    "UPDATED_AT": {"type": ["string", "null"], "format": "date-time"},
                    "RAW_DATA": {"type": ["string", "null"]},
                },
            }

            # Write schema
            singer.write_schema(
                stream_name=stream_name, schema=schema, key_properties=["SHIP_ID"]
            )

            # Get current time with timezone
            current_time = self.get_current_time()
            current_time_str = current_time.isoformat()

            # Skip processing if no ships data
            if not ships_data:
                # Write state even for empty response
                state = {"STG_SPACEX_DATA_SHIPS": {"last_sync": current_time_str}}
                singer.write_state(state)
                return

            # Process and write each ship record
            for ship in ships_data:
                try:
                    # Transform data for Snowflake compatibility
                    transformed_ship = {
                        "SHIP_ID": ship.get("id"),
                        "NAME": ship.get("name"),
                        "LEGACY_ID": ship.get("legacy_id"),
                        "MODEL": ship.get("model"),
                        "TYPE": ship.get("type"),
                        "ACTIVE": ship.get("active"),
                        "IMO": ship.get("imo"),
                        "MMSI": ship.get("mmsi"),
                        "ABS": ship.get("abs"),
                        "CLASS": ship.get("class"),
                        "MASS_KG": self._prepare_value_for_snowflake(
                            ship.get("mass_kg"), is_numeric=True
                        ),
                        "MASS_LBS": self._prepare_value_for_snowflake(
                            ship.get("mass_lbs"), is_numeric=True
                        ),
                        "YEAR_BUILT": self._prepare_value_for_snowflake(
                            ship.get("year_built"), is_numeric=True
                        ),
                        "HOME_PORT": ship.get("home_port"),
                        "STATUS": ship.get("status"),
                        "SPEED_KN": self._prepare_value_for_snowflake(
                            ship.get("speed_kn"), is_numeric=True
                        ),
                        "COURSE_DEG": self._prepare_value_for_snowflake(
                            ship.get("course_deg"), is_numeric=True
                        ),
                        "LATITUDE": self._prepare_value_for_snowflake(
                            ship.get("latitude"), is_numeric=True
                        ),
                        "LONGITUDE": self._prepare_value_for_snowflake(
                            ship.get("longitude"), is_numeric=True
                        ),
                        "LAST_AIS_UPDATE": ship.get("last_ais_update"),
                        "LINK": ship.get("link"),
                        "IMAGE": ship.get("image"),
                        "LAUNCHES": json.dumps(ship.get("launches", [])),
                        "ROLES": json.dumps(ship.get("roles", [])),
                        "CREATED_AT": current_time_str,
                        "UPDATED_AT": current_time_str,
                        "RAW_DATA": json.dumps(ship),
                    }

                    # Write record with timezone-aware timestamp
                    singer.write_record(
                        stream_name=stream_name,
                        record=transformed_ship,
                        time_extracted=current_time,
                    )

                    # Insert data into Snowflake
                    self.insert_into_snowflake(stream_name, transformed_ship)

                except Exception as transform_error:
                    self.log_error(
                        table_name=stream_name,
                        error_message=f"Data transformation error: \
                            {str(transform_error)}",
                        error_data=ship,
                    )
                    continue  # Continue processing other ship

            # Write state
            state = {"STG_SPACEX_DATA_SHIPS": {"last_sync": current_time_str}}
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
