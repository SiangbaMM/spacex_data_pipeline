import json

import requests  # type: ignore
import singer  # type: ignore

from .spacex_tap_base import SpaceXTapBase


class RocketsTap(SpaceXTapBase):
    """RocketsTap is a SpaceXTapBase sub class in charge of \
        SpaceX Rockets entity ingestion

    Args:
    - base_url (str) : The root url of v4 SpaceX API
    - config_path (str) : Config file that contains database credentials
    """

    def __init__(self, base_url: str, config_path: str):
        """Inherit base_url and config_path from SpaceXTapBase"""
        super().__init__(base_url, config_path)

    def fetch_rockets(self) -> None:
        """Fetch and process rockets data from SpaceX API with \
            Snowflake-compatible schema."""
        stream_name = "STG_SPACEX_DATA_ROCKETS"

        try:
            # Fetch data from the rockets endpoint
            response = requests.get(self.base_url + "rockets")
            response.raise_for_status()
            rockets_data = response.json()

            # Schema definition with Snowflake-compatible types
            schema = {
                "type": "object",
                "properties": {
                    "ROCKET_ID": {
                        "type": ["string", "null"],
                        "description": "Unique identifier for the rocket",
                    },
                    "NAME": {"type": ["string", "null"], "maxLength": 256},
                    "TYPE": {"type": ["string", "null"], "maxLength": 50},
                    "ACTIVE": {"type": ["boolean", "null"]},
                    "STAGES": {"type": ["integer", "null"]},
                    "BOOSTERS": {"type": ["integer", "null"]},
                    "COST_PER_LAUNCH": {"type": ["integer", "null"]},
                    "SUCCESS_RATE_PCT": {"type": ["integer", "null"]},
                    "FIRST_FLIGHT": {"type": ["string", "null"], "format": "date"},
                    "COUNTRY": {"type": ["string", "null"], "maxLength": 100},
                    "COMPANY": {"type": ["string", "null"], "maxLength": 100},
                    "HEIGHT_METERS": {"type": ["number", "null"]},
                    "HEIGHT_FEET": {"type": ["number", "null"]},
                    "DIAMETER_METERS": {"type": ["number", "null"]},
                    "DIAMETER_FEET": {"type": ["number", "null"]},
                    "MASS_KG": {"type": ["number", "null"]},
                    "MASS_LBS": {"type": ["number", "null"]},
                    "PAYLOAD_WEIGHTS": {
                        "type": ["string", "null"],
                        "description": "Array of payload weight info stored \
                            as JSON string",
                    },
                    "FIRST_STAGE": {
                        "type": ["string", "null"],
                        "description": "First stage details stored as JSON string",
                    },
                    "SECOND_STAGE": {
                        "type": ["string", "null"],
                        "description": "Second stage details stored as JSON string",
                    },
                    "ENGINES": {
                        "type": ["string", "null"],
                        "description": "Engine details stored as JSON string",
                    },
                    "LANDING_LEGS": {
                        "type": ["string", "null"],
                        "description": "Landing legs details stored as JSON string",
                    },
                    "FLICKR_IMAGES": {
                        "type": ["string", "null"],
                        "description": "Array of image URLs stored as JSON string",
                    },
                    "WIKIPEDIA": {"type": ["string", "null"]},
                    "DESCRIPTION": {"type": ["string", "null"]},
                    "CREATED_AT": {"type": ["string", "null"], "format": "date-time"},
                    "UPDATED_AT": {"type": ["string", "null"], "format": "date-time"},
                    "RAW_DATA": {"type": ["string", "null"]},
                },
            }

            # Write schema
            singer.write_schema(
                stream_name=stream_name, schema=schema, key_properties=["ROCKET_ID"]
            )

            # Get current time with timezone
            current_time = self.get_current_time()
            current_time_str = current_time.isoformat()

            # Process and write each rocket record
            for rocket in rockets_data:
                try:
                    # Extract height and diameter from nested objects
                    height = rocket.get("height", {})
                    diameter = rocket.get("diameter", {})
                    mass = rocket.get("mass", {})

                    # Transform data for Snowflake compatibility
                    transformed_rocket = {
                        "ROCKET_ID": rocket.get("id"),
                        "NAME": rocket.get("name"),
                        "TYPE": rocket.get("type"),
                        "ACTIVE": rocket.get("active"),
                        "STAGES": rocket.get("stages"),
                        "BOOSTERS": rocket.get("boosters"),
                        "COST_PER_LAUNCH": rocket.get("cost_per_launch"),
                        "SUCCESS_RATE_PCT": rocket.get("success_rate_pct"),
                        "FIRST_FLIGHT": rocket.get("first_flight"),
                        "COUNTRY": rocket.get("country"),
                        "COMPANY": rocket.get("company"),
                        "HEIGHT_METERS": height.get("meters"),
                        "HEIGHT_FEET": height.get("feet"),
                        "DIAMETER_METERS": diameter.get("meters"),
                        "DIAMETER_FEET": diameter.get("feet"),
                        "MASS_KG": mass.get("kg"),
                        "MASS_LBS": mass.get("lb"),
                        "PAYLOAD_WEIGHTS": json.dumps(
                            rocket.get("payload_weights", [])
                        ),
                        "FIRST_STAGE": json.dumps(rocket.get("first_stage", {})),
                        "SECOND_STAGE": json.dumps(rocket.get("second_stage", {})),
                        "ENGINES": json.dumps(rocket.get("engines", {})),
                        "LANDING_LEGS": json.dumps(rocket.get("landing_legs", {})),
                        "FLICKR_IMAGES": json.dumps(rocket.get("flickr_images", [])),
                        "WIKIPEDIA": rocket.get("wikipedia"),
                        "DESCRIPTION": rocket.get("description"),
                        "CREATED_AT": current_time_str,
                        "UPDATED_AT": current_time_str,
                        "RAW_DATA": json.dumps(rocket),
                    }

                    # Write record with timezone-aware timestamp
                    singer.write_record(
                        stream_name=stream_name,
                        record=transformed_rocket,
                        time_extracted=current_time,
                    )

                    # Insert data into Snowflake
                    self.insert_into_snowflake(stream_name, transformed_rocket)

                except Exception as transform_error:
                    self.log_error(
                        table_name=stream_name,
                        error_message=f"Data transformation error: \
                            {str(transform_error)}",
                        error_data=rocket,
                    )
                    continue  # Continue processing other rocket

            # Write state
            state = {"STG_SPACEX_DATA_ROCKETS": {"last_sync": current_time_str}}
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
