import singer                                           # type: ignore
import requests                                         # type: ignore
import json
from include.spacex_tap_base import SpaceXTapBase


class CrewTap(SpaceXTapBase):
    def __init__(self, base_url: str, config_path: str):
        super().__init__(base_url, config_path)
    
    def fetch_crew(self) -> None:
        """
        Fetch and process crew data from SpaceX API with Snowflake-compatible schema.
        
        Args:
            base_url (str): Base URL for the SpaceX API
        """
        stream_name = "STG_SPACEX_DATA_CREW"
        # Fetch data from the crew endpoint
        
        try:
            response = requests.get(self.base_url + "crew")
            response.raise_for_status()
            crew_data = response.json()
        
            # Schema definition with Snowflake-compatible types
            schema = {
                "type": "object",
                "properties": {
                    "CREW_ID": {
                        "type": ["string", "null"],
                        "description": "Unique identifier for the crew member"
                    },
                    "NAME": {
                        "type": ["string", "null"],
                        "maxLength": 256
                    },
                    "AGENCY": {
                        "type": ["string", "null"],
                        "maxLength": 256
                    },
                    "IMAGE": {
                        "type": ["string", "null"],
                        "description": "URL of crew member's image"
                    },
                    "WIKIPEDIA": {
                        "type": ["string", "null"],
                        "description": "URL of Wikipedia page"
                    },
                    "STATUS": {
                        "type": ["string", "null"],
                        "maxLength": 50
                    },
                    "LAUNCHES": {
                        "type": ["string", "null"],
                        "description": "Array of launch IDs stored as JSON string"
                    },
                    "CREATED_AT": {"type": ["string", "null"]},
                    "UPDATED_AT": {"type": ["string", "null"]},
                    "RAW_DATA": {"type": ["string", "null"]}
                }
            }
        
            # Write schema
            singer.write_schema(
                stream_name=stream_name,
                schema=schema,
                key_properties=["CREW_ID"]
            )
        
            # Get current time with timezone
            current_time = self.get_current_time()
            current_time_str = current_time.isoformat()
        
            # Process and write each crew record
            for crew_member in crew_data:
                try :
                    # Transform data for Snowflake compatibility
                    transformed_crew = {
                        "CREW_ID": crew_member.get("id"),
                        "NAME": crew_member.get("name"),
                        "AGENCY": crew_member.get("agency"),
                        "IMAGE": crew_member.get("image"),
                        "WIKIPEDIA": crew_member.get("wikipedia"),
                        "STATUS": crew_member.get("status"),
                        "LAUNCHES": json.dumps(crew_member.get("launches", [])),
                        "CREATED_AT": current_time_str,
                        "UPDATED_AT": current_time_str,
                        "RAW_DATA": json.dumps(crew_member)
                    }
            
                    # Write record with timezone-aware timestamp
                    singer.write_record(
                        stream_name=stream_name,
                        record=transformed_crew,
                    )
                
                except Exception as transform_error:
                    self.log_error(
                        table_name=stream_name,
                        error_message=f"Data transformation error: {str(transform_error)}",
                        error_data=crew_member
                    )
                    continue  # Continue processing other capsules
        
            # Write state
            state = {
                "STG_SPACEX_DATA_CREW": {
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
