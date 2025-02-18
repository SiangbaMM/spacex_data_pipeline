import json

import requests  # type: ignore
import singer  # type: ignore

from .spacex_tap_base import SpaceXTapBase


class HistoryTap(SpaceXTapBase):
    """HistoryTap is a SpaceXTapBase sub class in charge of \
        SpaceX History entity ingestion

    Args:
    - base_url (str) : The root url of v4 SpaceX API
    - config_path (str) : Config file that contains database credentials
    """

    def __init__(self, base_url: str, config_path: str):
        """Inherit base_url and config_path from SpaceXTapBase"""
        super().__init__(base_url, config_path)

    def fetch_history(self) -> None:
        """Fetch and process history data from SpaceX API with \
            Snowflake-compatible schema."""
        stream_name = "STG_SPACEX_DATA_HISTORY"

        try:
            # Fetch data from the history endpoint
            response = requests.get(self.base_url + "history")
            response.raise_for_status()
            history_data = response.json()

            # Schema definition with Snowflake-compatible types
            schema = {
                "type": "object",
                "properties": {
                    "HISTORY_ID": {
                        "type": ["string", "null"],
                        "description": "Unique identifier for the historical event",
                    },
                    "TITLE": {"type": ["string", "null"], "maxLength": 512},
                    "EVENT_DATE_UTC": {
                        "type": ["string", "null"],
                        "format": "date-time",
                        "description": "Date of the historical event",
                    },
                    "EVENT_DATE_UNIX": {
                        "type": ["integer", "null"],
                        "description": "Unix timestamp of the event",
                    },
                    "DETAILS": {
                        "type": ["string", "null"],
                        "description": "Detailed description of the event",
                    },
                    "LINKS": {
                        "type": ["string", "null"],
                        "description": "Related links stored as JSON string",
                    },
                    "FLIGHT_NUMBER": {
                        "type": ["integer", "null"],
                        "description": "Associated flight number if applicable",
                    },
                    "CREATED_AT": {"type": ["string", "null"], "format": "date-time"},
                    "UPDATED_AT": {"type": ["string", "null"], "format": "date-time"},
                    "RAW_DATA": {"type": ["string", "null"]},
                },
            }

            # Write schema
            singer.write_schema(
                stream_name=stream_name, schema=schema, key_properties=["HISTORY_ID"]
            )

            # Get current time with timezone
            current_time = self.get_current_time()
            current_time_str = current_time.isoformat()

            # Process and write each history record
            for event in history_data:
                try:
                    # Transform links object to JSON string if it exists
                    links = (
                        json.dumps(event.get("links", {}))
                        if event.get("links")
                        else None
                    )

                    # Transform data for Snowflake compatibility
                    transformed_event = {
                        "HISTORY_ID": event.get("id"),
                        "TITLE": event.get("title"),
                        "EVENT_DATE_UTC": event.get("event_date_utc"),
                        "EVENT_DATE_UNIX": event.get("event_date_unix"),
                        "DETAILS": event.get("details"),
                        "LINKS": links,
                        "FLIGHT_NUMBER": event.get("flight_number"),
                        "CREATED_AT": current_time_str,
                        "UPDATED_AT": current_time_str,
                        "RAW_DATA": json.dumps(event),
                    }

                    # Write record with timezone-aware timestamp
                    singer.write_record(
                        stream_name=stream_name,
                        record=transformed_event,
                        time_extracted=current_time,
                    )

                    # Insert data into Snowflake
                    self.insert_into_snowflake(stream_name, transformed_event)
                except Exception as transform_error:
                    self.log_error(
                        table_name=stream_name,
                        error_message=f"Data transformation error: \
                            {str(transform_error)}",
                        error_data=event,
                    )
                    continue  # Continue processing other history

            # Write state
            state = {"STG_SPACEX_DATA_HISTORY": {"last_sync": current_time_str}}
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
