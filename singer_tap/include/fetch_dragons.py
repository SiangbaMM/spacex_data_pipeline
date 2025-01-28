import singer                                           # type: ignore
import requests                                         # type: ignore
import json
from include.spacex_tap_base import SpaceXTapBase

class DragonsTap(SpaceXTapBase):
    def __init__(self, base_url: str, config_path: str):
        super().__init__(base_url, config_path)
    
    def fetch_dragons(self) -> None:
        """
        Fetch and process dragon data from SpaceX API with Snowflake-compatible schema.
        
        Args:
            base_url (str): Base URL for the SpaceX API
        """
        stream_name = "STG_SPACEX_DATA_DRAGONS"
        
        try :
            # Fetch data from the dragons endpoint
            response = requests.get(self.base_url + "dragons")
            response.raise_for_status()
            dragons_data = response.json()
        
            # Schema definition with Snowflake-compatible types
            schema = {
                "type": "object",
                "properties": {
                    "DRAGON_ID": {
                        "type": ["string", "null"],
                        "description": "Unique identifier for the dragon"
                    },
                    "NAME": {
                        "type": ["string", "null"],
                        "maxLength": 256
                    },
                    "TYPE": {
                        "type": ["string", "null"],
                        "maxLength": 50
                    },
                    "ACTIVE": {
                        "type": ["boolean", "null"]
                    },
                    "CREW_CAPACITY": {
                        "type": ["integer", "null"]
                    },
                    "SIDEWALL_ANGLE_DEG": {
                        "type": ["number", "null"]
                    },
                    "ORBIT_DURATION_YR": {
                        "type": ["integer", "null"]
                    },
                    "DRY_MASS_KG": {
                        "type": ["integer", "null"]
                    },
                    "DRY_MASS_LB": {
                        "type": ["integer", "null"]
                    },
                    "FIRST_FLIGHT": {
                        "type": ["string", "null"],
                        "format": "date"
                    },
                    "HEAT_SHIELD": {
                        "type": ["string", "null"],
                        "description": "Heat shield details stored as JSON string"
                    },
                    "THRUSTERS": {
                        "type": ["string", "null"],
                        "description": "Thrusters details stored as JSON string"
                    },
                    "LAUNCH_PAYLOAD_MASS": {
                        "type": ["string", "null"],
                        "description": "Launch payload mass details as JSON string"
                    },
                    "LAUNCH_PAYLOAD_VOL": {
                        "type": ["string", "null"],
                        "description": "Launch payload volume details as JSON string"
                    },
                    "RETURN_PAYLOAD_MASS": {
                        "type": ["string", "null"],
                        "description": "Return payload mass details as JSON string"
                    },
                    "RETURN_PAYLOAD_VOL": {
                        "type": ["string", "null"],
                        "description": "Return payload volume details as JSON string"
                    },
                    "PRESSURIZED_CAPSULE": {
                        "type": ["string", "null"],
                        "description": "Pressurized capsule details as JSON string"
                    },
                    "TRUNK": {
                        "type": ["string", "null"],
                        "description": "Trunk details as JSON string"
                    },
                    "HEIGHT_W_TRUNK": {
                        "type": ["string", "null"],
                        "description": "Height with trunk details as JSON string"
                    },
                    "DIAMETER": {
                        "type": ["string", "null"],
                        "description": "Diameter details as JSON string"
                    },
                    "WIKIPEDIA": {
                        "type": ["string", "null"]
                    },
                    "DESCRIPTION": {
                        "type": ["string", "null"]
                    },
                    "FLICKR_IMAGES": {
                        "type": ["string", "null"],
                        "description": "Array of image URLs stored as JSON string"
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
                key_properties=["DRAGON_ID"]
            )
        
            # Get current time with timezone
            current_time = self.get_current_time()
            current_time_str = current_time.isoformat()
        
            # Process and write each dragon record
            for dragon in dragons_data:
                try:
                    # Transform data for Snowflake compatibility
                    transformed_dragon = {
                        "DRAGON_ID": dragon.get("id"),
                        "NAME": dragon.get("name"),
                        "TYPE": dragon.get("type"),
                        "ACTIVE": dragon.get("active"),
                        "CREW_CAPACITY": dragon.get("crew_capacity"),
                        "SIDEWALL_ANGLE_DEG": dragon.get("sidewall_angle_deg"),
                        "ORBIT_DURATION_YR": dragon.get("orbit_duration_yr"),
                        "DRY_MASS_KG": dragon.get("dry_mass_kg"),
                        "DRY_MASS_LB": dragon.get("dry_mass_lb"),
                        "FIRST_FLIGHT": dragon.get("first_flight"),
                        "HEAT_SHIELD": json.dumps(dragon.get("heat_shield")),
                        "THRUSTERS": json.dumps(dragon.get("thrusters", [])),
                        "LAUNCH_PAYLOAD_MASS": json.dumps(dragon.get("launch_payload_mass")),
                        "LAUNCH_PAYLOAD_VOL": json.dumps(dragon.get("launch_payload_vol")),
                        "RETURN_PAYLOAD_MASS": json.dumps(dragon.get("return_payload_mass")),
                        "RETURN_PAYLOAD_VOL": json.dumps(dragon.get("return_payload_vol")),
                        "PRESSURIZED_CAPSULE": json.dumps(dragon.get("pressurized_capsule")),
                        "TRUNK": json.dumps(dragon.get("trunk")),
                        "HEIGHT_W_TRUNK": json.dumps(dragon.get("height_w_trunk")),
                        "DIAMETER": json.dumps(dragon.get("diameter")),
                        "WIKIPEDIA": dragon.get("wikipedia"),
                        "DESCRIPTION": dragon.get("description"),
                        "FLICKR_IMAGES": json.dumps(dragon.get("flickr_images", [])),
                        "CREATED_AT": current_time_str,
                        "UPDATED_AT": current_time_str,
                        "RAW_DATA": json.dumps(dragon)
                    }
            
                    # Write record with timezone-aware timestamp
                    singer.write_record(
                        stream_name=stream_name,
                        record=transformed_dragon,
                    )
                
                except Exception as transform_error:
                    self.log_error(
                        table_name=stream_name,
                        error_message=f"Data transformation error: {str(transform_error)}",
                        error_data=dragon
                    )
                    continue  # Continue processing other capsules 
                
        
            # Write state
            state = {
                "DRAGONS": {
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
