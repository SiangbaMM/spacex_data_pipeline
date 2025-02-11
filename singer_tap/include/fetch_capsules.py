import json

import requests  # type: ignore
import singer  # type: ignore
from include.spacex_tap_base import SpaceXTapBase


class CapsulesTap(SpaceXTapBase):
    """CapsulesTap is a SpaceXTapBase sub class in charge of \
    SpaceX Capsule entity ingestion

    Args:
    - base_url (str) : The root url of v4 SpaceX API
    - config_path (str) : Config file that contains database credentials
    """

    def __init__(self, base_url: str, config_path: str):
        """Inherit base_url and config_path from SpaceXTapBase"""
        super().__init__(base_url, config_path)

    def fetch_capsules(self):
        """Fetch and process capsules data."""
        stream_name = "STG_SPACEX_DATA_CAPSULES"

        try:
            # Fetch data from API
            response = requests.get(self.base_url + "capsules")
            response.raise_for_status()
            capsules_data = response.json()

            # Schema definition for capsules data
            schema = {
                "type": "object",
                "properties": {
                    "CAPSULE_ID": {"type": ["string", "null"]},
                    "SERIAL": {"type": ["string", "null"]},
                    "STATUS": {"type": ["string", "null"]},
                    "DRAGON": {"type": ["string", "null"]},
                    "REUSE_COUNT": {"type": ["integer", "null"]},
                    "WATER_LANDINGS": {"type": ["integer", "null"]},
                    "LAND_LANDINGS": {"type": ["integer", "null"]},
                    "LAST_UPDATE": {"type": ["string", "null"]},
                    "LAUNCHES": {"type": ["array", "null"]},
                    "CREATED_AT": {"type": ["string", "null"]},
                    "UPDATED_AT": {"type": ["string", "null"]},
                    "RAW_DATA": {"type": ["string", "null"]},
                },
            }

            # Write schema
            singer.write_schema(
                stream_name=stream_name, schema=schema, key_properties=["CAPSULE_ID"]
            )

            # Process each capsule
            current_time = self.get_current_time()
            current_time_str = current_time.isoformat()

            # Process and write each capsule record
            for capsule in capsules_data:
                try:
                    transformed_capsule = {
                        "CAPSULE_ID": capsule.get("id"),
                        "SERIAL": capsule.get("serial"),
                        "STATUS": capsule.get("status"),
                        "DRAGON": capsule.get("dragon"),
                        "REUSE_COUNT": capsule.get("reuse_count"),
                        "WATER_LANDINGS": capsule.get("water_landings"),
                        "LAND_LANDINGS": capsule.get("land_landings"),
                        "LAST_UPDATE": capsule.get("last_update"),
                        "LAUNCHES": capsule.get("launches", []),
                        "CREATED_AT": current_time_str,
                        "UPDATED_AT": current_time_str,
                        "RAW_DATA": json.dumps(capsule),
                    }

                    # Write record
                    singer.write_record(
                        stream_name=stream_name,
                        record=transformed_capsule,
                        time_extracted=current_time,
                    )

                except Exception as transform_error:
                    self.log_error(
                        table_name=stream_name,
                        error_message=f"Data transformation error: \
                            {str(transform_error)}",
                        error_data=capsule,
                    )
                    continue  # Continue processing other capsules

            # Write state
            state = {"STG_SPACEX_DATA_CAPSULES": {"last_sync": current_time_str}}
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
