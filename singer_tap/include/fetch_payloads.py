import json

import requests  # type: ignore
import singer  # type: ignore

from .spacex_tap_base import SpaceXTapBase


class PayloadsTap(SpaceXTapBase):
    """PayloadsTap is a SpaceXTapBase sub class in charge of \
        SpaceX Payloads entity ingestion

    Args:
    - base_url (str) : The root url of v4 SpaceX API
    - config_path (str) : Config file that contains database credentials
    """

    def __init__(self, base_url: str, config_path: str):
        """Inherit base_url and config_path from SpaceXTapBase"""
        super().__init__(base_url, config_path)

    def fetch_payloads(self) -> None:
        """Fetch and process payloads data from SpaceX API with \
            Snowflake-compatible schema."""
        stream_name = "STG_SPACEX_DATA_PAYLOADS"

        try:
            # Fetch data from the payloads endpoint
            response = requests.get(self.base_url + "payloads")
            response.raise_for_status()
            payloads_data = response.json()

            # Schema definition with Snowflake-compatible types
            schema = {
                "type": "object",
                "properties": {
                    "PAYLOAD_ID": {
                        "type": ["string", "null"],
                        "description": "Unique identifier for the payload",
                    },
                    "NAME": {"type": ["string", "null"], "maxLength": 256},
                    "TYPE": {"type": ["string", "null"], "maxLength": 100},
                    "REUSED": {"type": ["boolean", "null"]},
                    "LAUNCH": {
                        "type": ["string", "null"],
                        "description": "Associated launch ID",
                    },
                    "CUSTOMERS": {
                        "type": ["string", "null"],
                        "description": "Array of customer names stored as JSON string",
                    },
                    "NORAD_IDS": {
                        "type": ["string", "null"],
                        "description": "Array of NORAD IDs stored as JSON string",
                    },
                    "NATIONALITIES": {
                        "type": ["string", "null"],
                        "description": "Array of nationality strings stored \
                            as JSON string",
                    },
                    "MANUFACTURERS": {
                        "type": ["string", "null"],
                        "description": "Array of manufacturer names stored \
                            as JSON string",
                    },
                    "MASS_KG": {"type": ["number", "null"]},
                    "MASS_LBS": {"type": ["number", "null"]},
                    "ORBIT": {"type": ["string", "null"], "maxLength": 50},
                    "REFERENCE_SYSTEM": {"type": ["string", "null"], "maxLength": 50},
                    "REGIME": {"type": ["string", "null"], "maxLength": 50},
                    "LONGITUDE": {"type": ["number", "null"]},
                    "SEMI_MAJOR_AXIS_KM": {"type": ["number", "null"]},
                    "ECCENTRICITY": {"type": ["number", "null"]},
                    "PERIAPSIS_KM": {"type": ["number", "null"]},
                    "APOAPSIS_KM": {"type": ["number", "null"]},
                    "INCLINATION_DEG": {"type": ["number", "null"]},
                    "PERIOD_MIN": {"type": ["number", "null"]},
                    "LIFESPAN_YEARS": {"type": ["number", "null"]},
                    "EPOCH": {"type": ["string", "null"], "format": "date-time"},
                    "MEAN_MOTION": {"type": ["number", "null"]},
                    "RAAN": {"type": ["number", "null"]},
                    "ARG_OF_PERICENTER": {"type": ["number", "null"]},
                    "MEAN_ANOMALY": {"type": ["number", "null"]},
                    "DRAGON": {
                        "type": ["string", "null"],
                        "description": "Dragon capsule details stored as JSON string",
                    },
                    "CREATED_AT": {"type": ["string", "null"]},
                    "UPDATED_AT": {"type": ["string", "null"]},
                    "RAW_DATA": {"type": ["string", "null"]},
                },
            }

            # Write schema
            singer.write_schema(
                stream_name=stream_name, schema=schema, key_properties=["PAYLOAD_ID"]
            )

            # Get current time with timezone
            current_time = self.get_current_time()
            current_time_str = current_time.isoformat()

            # Process and write each payload record
            for payload in payloads_data:
                try:
                    # Transform data for Snowflake compatibility
                    transformed_payload = {
                        "PAYLOAD_ID": payload.get("id"),
                        "NAME": payload.get("name"),
                        "TYPE": payload.get("type"),
                        "REUSED": payload.get("reused"),
                        "LAUNCH": payload.get("launch"),
                        "CUSTOMERS": json.dumps(payload.get("customers", [])),
                        "NORAD_IDS": json.dumps(payload.get("norad_ids", [])),
                        "NATIONALITIES": json.dumps(payload.get("nationalities", [])),
                        "MANUFACTURERS": json.dumps(payload.get("manufacturers", [])),
                        "MASS_KG": self._prepare_value_for_snowflake(
                            payload.get("mass_kg"), is_numeric=True
                        ),
                        "MASS_LBS": self._prepare_value_for_snowflake(
                            payload.get("mass_lbs"), is_numeric=True
                        ),
                        "ORBIT": payload.get("orbit"),
                        "REFERENCE_SYSTEM": payload.get("reference_system"),
                        "REGIME": payload.get("regime"),
                        "LONGITUDE": self._prepare_value_for_snowflake(
                            payload.get("longitude"), is_numeric=True
                        ),
                        "SEMI_MAJOR_AXIS_KM": self._prepare_value_for_snowflake(
                            payload.get("semi_major_axis_km"), is_numeric=True
                        ),
                        "ECCENTRICITY": self._prepare_value_for_snowflake(
                            payload.get("eccentricity"), is_numeric=True
                        ),
                        "PERIAPSIS_KM": self._prepare_value_for_snowflake(
                            payload.get("periapsis_km"), is_numeric=True
                        ),
                        "APOAPSIS_KM": self._prepare_value_for_snowflake(
                            payload.get("apoapsis_km"), is_numeric=True
                        ),
                        "INCLINATION_DEG": self._prepare_value_for_snowflake(
                            payload.get("inclination_deg"), is_numeric=True
                        ),
                        "PERIOD_MIN": self._prepare_value_for_snowflake(
                            payload.get("period_min"), is_numeric=True
                        ),
                        "LIFESPAN_YEARS": self._prepare_value_for_snowflake(
                            payload.get("lifespan_years"), is_numeric=True
                        ),
                        "EPOCH": payload.get("epoch"),
                        "MEAN_MOTION": self._prepare_value_for_snowflake(
                            payload.get("mean_motion"), is_numeric=True
                        ),
                        "RAAN": self._prepare_value_for_snowflake(
                            payload.get("raan"), is_numeric=True
                        ),
                        "ARG_OF_PERICENTER": self._prepare_value_for_snowflake(
                            payload.get("arg_of_pericenter"), is_numeric=True
                        ),
                        "MEAN_ANOMALY": self._prepare_value_for_snowflake(
                            payload.get("mean_anomaly"), is_numeric=True
                        ),
                        "DRAGON": json.dumps(payload.get("dragon", {}))
                        if payload.get("dragon")
                        else None,
                        "CREATED_AT": current_time_str,
                        "UPDATED_AT": current_time_str,
                        "RAW_DATA": json.dumps(payload),
                    }

                    # Write record with timezone-aware timestamp
                    singer.write_record(
                        stream_name=stream_name,
                        record=transformed_payload,
                    )

                    # Insert data into Snowflake
                    self.insert_into_snowflake(stream_name, transformed_payload)

                except Exception as transform_error:
                    self.log_error(
                        table_name=stream_name,
                        error_message=f"Data transformation error: \
                            {str(transform_error)}",
                        error_data=payload,
                    )
                    continue  # Continue processing other payload

            # Write state
            state = {"STG_SPACEX_DATA_PAYLOADS": {"last_sync": current_time_str}}
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
