import singer                               # type: ignore
import pandas as pd                         # type: ignore
import numpy as np                          # type: ignore
import requests                             # type: ignore
import json                                 # type: ignore
import pytz                                 # type: ignore
from typing import Dict, Any
from datetime import datetime

LOGGER  = singer.get_logger()

def get_current_time():
    """Get current time with UTC timezone."""
    return datetime.now(pytz.UTC)


def fetch_capsules(base_url: str) -> None:
    """
    Fetch and process capsules data from SpaceX API with Snowflake-compatible schema.
    
    Args:
        base_url (str): Base URL for the SpaceX API
    """
    # Fetch data from the capsules endpoint
    response = requests.get(base_url + "capsules")
    response.raise_for_status()
    capsules_data = response.json()

    # Schema definition with Snowflake-compatible types
    schema = {
        "type": "object",
        "properties": {
            "CAPSULE_ID": {
                "type": ["string", "null"],
                "description": "Unique identifier for the capsule"
            },
            "SERIAL": {
                "type": ["string", "null"],
                "maxLength": 256
            },
            "STATUS": {
                "type": ["string", "null"],
                "maxLength": 50
            },
            "CAPSULE_TYPE": {
                "type": ["string", "null"],
                "maxLength": 50
            },
            "DRAGON_ID": {
                "type": ["string", "null"]
            },
            "REUSE_COUNT": {
                "type": ["integer", "null"]
            },
            "WATER_LANDINGS": {
                "type": ["integer", "null"]
            },
            "LAND_LANDINGS": {
                "type": ["integer", "null"]
            },
            "LAST_UPDATE": {
                "type": ["string", "null"]
            },
            "LAUNCHES": {
                "type": ["string", "null"]
            },
            "CREATED_AT": {
                "type": ["string", "null"],
                "format": "date-time"
            },
            "UPDATED_AT": {
                "type": ["string", "null"],
                "format": "date-time"
            }
        }
    }

    # Write schema
    singer.write_schema(
        stream_name="CAPSULES",
        schema=schema,
        key_properties=["CAPSULE_ID"]
    )

    # Get current time with timezone
    current_time = get_current_time()
    current_time_str = current_time.isoformat()

    # Process and write each capsule record
    for capsule in capsules_data:
        # Transform data for Snowflake compatibility
        transformed_capsule = {
            "CAPSULE_ID": capsule.get("id"),
            "SERIAL": capsule.get("serial"),
            "STATUS": capsule.get("status"),
            "CAPSULE_TYPE": capsule.get("type"),
            "DRAGON_ID": capsule.get("dragon"),
            "REUSE_COUNT": capsule.get("reuse_count"),
            "WATER_LANDINGS": capsule.get("water_landings"),
            "LAND_LANDINGS": capsule.get("land_landings"),
            "LAST_UPDATE": capsule.get("last_update"),
            "LAUNCHES": json.dumps(capsule.get("launches", [])),
            "CREATED_AT": current_time_str,
            "UPDATED_AT": current_time_str
        }

        # Write record with timezone-aware timestamp
        singer.write_record(
            stream_name="CAPSULES",
            record=transformed_capsule,
            time_extracted=current_time
        )

    # Write state
    state = {
        "CAPSULES": {
            "last_sync": current_time_str
        }
    }
    singer.write_state(state)
    

def fetch_company(based_url:str) -> None:
    
    # Fetch data directly using requests instead of pandas
    response = requests.get(based_url + "company")
    response.raise_for_status()
    company_data = response.json()

    schema = {
        "type": "object",  # Add this line to specify it's an object
        "properties": {
            "id": {
                "type": ["string", "null"]
            },
            "name": {
                "type": ["string", "null"]
            },
            "founder": {
                "type": ["string", "null"]
            },
            "founded": {
                "type": ["integer", "null"]
            },
            "employees": {
                "type": ["integer", "null"]
            },
            "vehicles": {
                "type": ["integer", "null"]
            },
            "launch_sites": {
                "type": ["integer", "null"]
            },
            "test_sites": {
                "type": ["integer", "null"]
            },
            "ceo": {
                "type": ["string", "null"]
            },
            "cto": {
                "type": ["string", "null"]
            },
            "coo": {
                "type": ["string", "null"]
            },
            "cto_propulsion": {
                "type": ["string", "null"]
            },
            "valuation": {
                "type": ["number", "null"]
            },
            "headquarters": {
                "type": "object",
                "properties": {
                    "address": {"type": ["string", "null"]},
                    "city": {"type": ["string", "null"]},
                    "state": {"type": ["string", "null"]}
                }
            },
            "links": {
                "type": "object",
                "properties": {
                    "website": {"type": ["string", "null"]},
                    "flickr": {"type": ["string", "null"]},
                    "twitter": {"type": ["string", "null"]},
                    "elon_twitter": {"type": ["string", "null"]}
                }
            },
            "summary": {
                "type": ["string", "null"]
            }
        }
    }

    # Write schema first
    singer.write_schema(
        stream_name="company",
        schema=schema,
        key_properties=["id"]  # If there's no id, you might need to adjust this
    )

    # Transform the data to handle any null values
    transformed_data = {k: (None if pd.isna(v) else v) for k, v in company_data.items()}

    # Write the single record
    singer.write_record(
        stream_name="company",
        record=transformed_data
    )
    

def fetch_cores(base_url: str) -> None:
    """
    Fetch and process cores data from SpaceX API with Snowflake-compatible schema.
    
    Args:
        base_url (str): Base URL for the SpaceX API
    """
    # Fetch data from the cores endpoint
    response = requests.get(base_url + "cores")
    response.raise_for_status()
    cores_data = response.json()

    # Schema definition with Snowflake-compatible types
    schema = {
        "type": "object",
        "properties": {
            "CORE_ID": {
                "type": ["string", "null"],
                "description": "Unique identifier for the core"
            },
            "SERIAL": {
                "type": ["string", "null"],
                "maxLength": 256
            },
            "BLOCK": {
                "type": ["integer", "null"]
            },
            "STATUS": {
                "type": ["string", "null"],
                "maxLength": 50
            },
            "REUSE_COUNT": {
                "type": ["integer", "null"]
            },
            "RTLS_ATTEMPTS": {
                "type": ["integer", "null"]
            },
            "RTLS_LANDINGS": {
                "type": ["integer", "null"]
            },
            "ASDS_ATTEMPTS": {
                "type": ["integer", "null"]
            },
            "ASDS_LANDINGS": {
                "type": ["integer", "null"]
            },
            "LAST_UPDATE": {
                "type": ["string", "null"]
            },
            "LAUNCHES": {
                "type": ["string", "null"],
                "description": "Array of launch IDs stored as JSON string"
            },
            "CREATED_AT": {
                "type": ["string", "null"],
                "format": "date-time"
            },
            "UPDATED_AT": {
                "type": ["string", "null"],
                "format": "date-time"
            }
        }
    }

    # Write schema
    singer.write_schema(
        stream_name="CORES",
        schema=schema,
        key_properties=["CORE_ID"]
    )

    # Get current time with timezone
    current_time = get_current_time()
    current_time_str = current_time.isoformat()

    # Process and write each core record
    for core in cores_data:
        # Transform data for Snowflake compatibility
        transformed_core = {
            "CORE_ID": core.get("id"),
            "SERIAL": core.get("serial"),
            "BLOCK": core.get("block"),
            "STATUS": core.get("status"),
            "REUSE_COUNT": core.get("reuse_count"),
            "RTLS_ATTEMPTS": core.get("rtls_attempts"),
            "RTLS_LANDINGS": core.get("rtls_landings"),
            "ASDS_ATTEMPTS": core.get("asds_attempts"),
            "ASDS_LANDINGS": core.get("asds_landings"),
            "LAST_UPDATE": core.get("last_update"),
            "LAUNCHES": json.dumps(core.get("launches", [])),
            "CREATED_AT": current_time_str,
            "UPDATED_AT": current_time_str
        }

        # Write record with timezone-aware timestamp
        singer.write_record(
            stream_name="CORES",
            record=transformed_core,
            time_extracted=current_time
        )

    # Write state
    state = {
        "CORES": {
            "last_sync": current_time_str
        }
    }
    singer.write_state(state)


def fetch_crew(base_url: str) -> None:
    """
    Fetch and process crew data from SpaceX API with Snowflake-compatible schema.
    
    Args:
        base_url (str): Base URL for the SpaceX API
    """
    # Fetch data from the crew endpoint
    response = requests.get(base_url + "crew")
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
            "CREATED_AT": {
                "type": ["string", "null"],
                "format": "date-time"
            },
            "UPDATED_AT": {
                "type": ["string", "null"],
                "format": "date-time"
            }
        }
    }

    # Write schema
    singer.write_schema(
        stream_name="CREW",
        schema=schema,
        key_properties=["CREW_ID"]
    )

    # Get current time with timezone
    current_time = get_current_time()
    current_time_str = current_time.isoformat()

    # Process and write each crew record
    for crew_member in crew_data:
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
            "UPDATED_AT": current_time_str
        }

        # Write record with timezone-aware timestamp
        singer.write_record(
            stream_name="CREW",
            record=transformed_crew,
            time_extracted=current_time
        )

    # Write state
    state = {
        "CREW": {
            "last_sync": current_time_str
        }
    }
    singer.write_state(state)
   
        
def fetch_dragons(base_url: str) -> None:
    """
    Fetch and process dragon data from SpaceX API with Snowflake-compatible schema.
    
    Args:
        base_url (str): Base URL for the SpaceX API
    """
    # Fetch data from the dragons endpoint
    response = requests.get(base_url + "dragons")
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
            "CREATED_AT": {
                "type": ["string", "null"],
                "format": "date-time"
            },
            "UPDATED_AT": {
                "type": ["string", "null"],
                "format": "date-time"
            }
        }
    }

    # Write schema
    singer.write_schema(
        stream_name="DRAGONS",
        schema=schema,
        key_properties=["DRAGON_ID"]
    )

    # Get current time with timezone
    current_time = get_current_time()
    current_time_str = current_time.isoformat()

    # Process and write each dragon record
    for dragon in dragons_data:
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
            "UPDATED_AT": current_time_str
        }

        # Write record with timezone-aware timestamp
        singer.write_record(
            stream_name="DRAGONS",
            record=transformed_dragon,
            time_extracted=current_time
        )

    # Write state
    state = {
        "DRAGONS": {
            "last_sync": current_time_str
        }
    }
    singer.write_state(state)
    
    
def fetch_history(base_url: str) -> None:
    """
    Fetch and process history data from SpaceX API with Snowflake-compatible schema.
    
    Args:
        base_url (str): Base URL for the SpaceX API
    """
    # Fetch data from the history endpoint
    response = requests.get(base_url + "history")
    response.raise_for_status()
    history_data = response.json()

    # Schema definition with Snowflake-compatible types
    schema = {
        "type": "object",
        "properties": {
            "HISTORY_ID": {
                "type": ["string", "null"],
                "description": "Unique identifier for the historical event"
            },
            "TITLE": {
                "type": ["string", "null"],
                "maxLength": 512
            },
            "EVENT_DATE_UTC": {
                "type": ["string", "null"],
                "format": "date-time",
                "description": "Date of the historical event"
            },
            "EVENT_DATE_UNIX": {
                "type": ["integer", "null"],
                "description": "Unix timestamp of the event"
            },
            "DETAILS": {
                "type": ["string", "null"],
                "description": "Detailed description of the event"
            },
            "LINKS": {
                "type": ["string", "null"],
                "description": "Related links stored as JSON string"
            },
            "FLIGHT_NUMBER": {
                "type": ["integer", "null"],
                "description": "Associated flight number if applicable"
            },
            "CREATED_AT": {
                "type": ["string", "null"],
                "format": "date-time"
            },
            "UPDATED_AT": {
                "type": ["string", "null"],
                "format": "date-time"
            }
        }
    }

    # Write schema
    singer.write_schema(
        stream_name="HISTORY",
        schema=schema,
        key_properties=["HISTORY_ID"]
    )

    # Get current time with timezone
    current_time = get_current_time()
    current_time_str = current_time.isoformat()

    # Process and write each history record
    for event in history_data:
        # Transform links object to JSON string if it exists
        links = json.dumps(event.get("links", {})) if event.get("links") else None
        
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
            "UPDATED_AT": current_time_str
        }

        # Write record with timezone-aware timestamp
        singer.write_record(
            stream_name="HISTORY",
            record=transformed_event,
            time_extracted=current_time
        )

    # Write state
    state = {
        "HISTORY": {
            "last_sync": current_time_str
        }
    }
    singer.write_state(state)


def fetch_landpads(base_url: str) -> None:
    """
    Fetch and process landpads data from SpaceX API with Snowflake-compatible schema.
    
    Args:
        base_url (str): Base URL for the SpaceX API
    """
    # Fetch data from the landpads endpoint
    response = requests.get(base_url + "landpads")
    response.raise_for_status()
    landpads_data = response.json()

    # Schema definition with Snowflake-compatible types
    schema = {
        "type": "object",
        "properties": {
            "LANDPAD_ID": {
                "type": ["string", "null"],
                "description": "Unique identifier for the landing pad"
            },
            "NAME": {
                "type": ["string", "null"],
                "maxLength": 256
            },
            "FULL_NAME": {
                "type": ["string", "null"],
                "maxLength": 512
            },
            "STATUS": {
                "type": ["string", "null"],
                "maxLength": 50
            },
            "TYPE": {
                "type": ["string", "null"],
                "maxLength": 50
            },
            "LOCALITY": {
                "type": ["string", "null"],
                "maxLength": 256
            },
            "REGION": {
                "type": ["string", "null"],
                "maxLength": 256
            },
            "LATITUDE": {
                "type": ["number", "null"]
            },
            "LONGITUDE": {
                "type": ["number", "null"]
            },
            "LANDING_ATTEMPTS": {
                "type": ["integer", "null"]
            },
            "LANDING_SUCCESSES": {
                "type": ["integer", "null"]
            },
            "WIKIPEDIA": {
                "type": ["string", "null"]
            },
            "DETAILS": {
                "type": ["string", "null"]
            },
            "LAUNCHES": {
                "type": ["string", "null"],
                "description": "Array of launch IDs stored as JSON string"
            },
            "IMAGES": {
                "type": ["string", "null"],
                "description": "Image URLs stored as JSON string"
            },
            "CREATED_AT": {
                "type": ["string", "null"],
                "format": "date-time"
            },
            "UPDATED_AT": {
                "type": ["string", "null"],
                "format": "date-time"
            }
        }
    }

    # Write schema
    singer.write_schema(
        stream_name="LANDPADS",
        schema=schema,
        key_properties=["LANDPAD_ID"]
    )

    # Get current time with timezone
    current_time = get_current_time()
    current_time_str = current_time.isoformat()

    # Process and write each landpad record
    for landpad in landpads_data:
        # Handle images object specifically
        images = {}
        if "images" in landpad:
            images = {
                "large": landpad["images"].get("large", []),
                "small": landpad["images"].get("small", [])
            }

        # Transform data for Snowflake compatibility
        transformed_landpad = {
            "LANDPAD_ID": landpad.get("id"),
            "NAME": landpad.get("name"),
            "FULL_NAME": landpad.get("full_name"),
            "STATUS": landpad.get("status"),
            "TYPE": landpad.get("type"),
            "LOCALITY": landpad.get("locality"),
            "REGION": landpad.get("region"),
            "LATITUDE": landpad.get("latitude"),
            "LONGITUDE": landpad.get("longitude"),
            "LANDING_ATTEMPTS": landpad.get("landing_attempts"),
            "LANDING_SUCCESSES": landpad.get("landing_successes"),
            "WIKIPEDIA": landpad.get("wikipedia"),
            "DETAILS": landpad.get("details"),
            "LAUNCHES": json.dumps(landpad.get("launches", [])),
            "IMAGES": json.dumps(images),
            "CREATED_AT": current_time_str,
            "UPDATED_AT": current_time_str
        }

        # Write record with timezone-aware timestamp
        singer.write_record(
            stream_name="LANDPADS",
            record=transformed_landpad,
            time_extracted=current_time
        )

    # Write state
    state = {
        "LANDPADS": {
            "last_sync": current_time_str
        }
    }
    singer.write_state(state)


def fetch_launches(base_url: str) -> None:
    """
    Fetch and process launches data from SpaceX API with Snowflake-compatible schema.
    
    Args:
        base_url (str): Base URL for the SpaceX API
    """
    # Fetch data from the launches endpoint
    response = requests.get(base_url + "launches")
    response.raise_for_status()
    launches_data = response.json()

    # Schema definition with Snowflake-compatible types
    schema = {
        "type": "object",
        "properties": {
            "LAUNCH_ID": {
                "type": ["string", "null"],
                "description": "Unique identifier for the launch"
            },
            "FLIGHT_NUMBER": {
                "type": ["integer", "null"]
            },
            "NAME": {
                "type": ["string", "null"],
                "maxLength": 256
            },
            "DATE_UTC": {
                "type": ["string", "null"],
                "format": "date-time"
            },
            "DATE_UNIX": {
                "type": ["integer", "null"]
            },
            "DATE_LOCAL": {
                "type": ["string", "null"]
            },
            "DATE_PRECISION": {
                "type": ["string", "null"]
            },
            "STATIC_FIRE_DATE_UTC": {
                "type": ["string", "null"],
                "format": "date-time"
            },
            "STATIC_FIRE_DATE_UNIX": {
                "type": ["integer", "null"]
            },
            "NET": {
                "type": ["boolean", "null"]
            },
            "WINDOW": {
                "type": ["integer", "null"]
            },
            "ROCKET": {
                "type": ["string", "null"]
            },
            "SUCCESS": {
                "type": ["boolean", "null"]
            },
            "FAILURES": {
                "type": ["string", "null"],
                "description": "Array of failure details stored as JSON string"
            },
            "UPCOMING": {
                "type": ["boolean", "null"]
            },
            "DETAILS": {
                "type": ["string", "null"]
            },
            "FAIRINGS": {
                "type": ["string", "null"],
                "description": "Fairings details stored as JSON string"
            },
            "CREW": {
                "type": ["string", "null"],
                "description": "Array of crew details stored as JSON string"
            },
            "SHIPS": {
                "type": ["string", "null"],
                "description": "Array of ship IDs stored as JSON string"
            },
            "CAPSULES": {
                "type": ["string", "null"],
                "description": "Array of capsule IDs stored as JSON string"
            },
            "PAYLOADS": {
                "type": ["string", "null"],
                "description": "Array of payload IDs stored as JSON string"
            },
            "LAUNCHPAD": {
                "type": ["string", "null"]
            },
            "CORES": {
                "type": ["string", "null"],
                "description": "Array of core details stored as JSON string"
            },
            "LINKS": {
                "type": ["string", "null"],
                "description": "Related links stored as JSON string"
            },
            "AUTO_UPDATE": {
                "type": ["boolean", "null"]
            },
            "LAUNCH_LIBRARY_ID": {
                "type": ["string", "null"]
            },
            "CREATED_AT": {
                "type": ["string", "null"],
                "format": "date-time"
            },
            "UPDATED_AT": {
                "type": ["string", "null"],
                "format": "date-time"
            }
        }
    }

    # Write schema
    singer.write_schema(
        stream_name="LAUNCHES",
        schema=schema,
        key_properties=["LAUNCH_ID"]
    )

    # Get current time with timezone
    current_time = get_current_time()
    current_time_str = current_time.isoformat()

    # Process and write each launch record
    for launch in launches_data:
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
            "UPDATED_AT": current_time_str
        }

        # Write record with timezone-aware timestamp
        singer.write_record(
            stream_name="LAUNCHES",
            record=transformed_launch,
            time_extracted=current_time
        )

    # Write state
    state = {
        "LAUNCHES": {
            "last_sync": current_time_str
        }
    }
    singer.write_state(state)
    

def fetch_launchpads(base_url: str) -> None:
    """
    Fetch and process launchpads data from SpaceX API with Snowflake-compatible schema.
    
    Args:
        base_url (str): Base URL for the SpaceX API
    """
    # Fetch data from the launchpads endpoint
    response = requests.get(base_url + "launchpads")
    response.raise_for_status()
    launchpads_data = response.json()

    # Schema definition with Snowflake-compatible types
    schema = {
        "type": "object",
        "properties": {
            "LAUNCHPAD_ID": {
                "type": ["string", "null"],
                "description": "Unique identifier for the launch pad"
            },
            "NAME": {
                "type": ["string", "null"],
                "maxLength": 256
            },
            "FULL_NAME": {
                "type": ["string", "null"],
                "maxLength": 512
            },
            "STATUS": {
                "type": ["string", "null"],
                "maxLength": 50
            },
            "LOCALITY": {
                "type": ["string", "null"],
                "maxLength": 256
            },
            "REGION": {
                "type": ["string", "null"],
                "maxLength": 256
            },
            "TIMEZONE": {
                "type": ["string", "null"],
                "maxLength": 50
            },
            "LATITUDE": {
                "type": ["number", "null"]
            },
            "LONGITUDE": {
                "type": ["number", "null"]
            },
            "LAUNCH_ATTEMPTS": {
                "type": ["integer", "null"]
            },
            "LAUNCH_SUCCESSES": {
                "type": ["integer", "null"]
            },
            "ROCKETS": {
                "type": ["string", "null"],
                "description": "Array of rocket IDs stored as JSON string"
            },
            "LAUNCHES": {
                "type": ["string", "null"],
                "description": "Array of launch IDs stored as JSON string"
            },
            "DETAILS": {
                "type": ["string", "null"]
            },
            "IMAGES": {
                "type": ["string", "null"],
                "description": "Image URLs stored as JSON string"
            },
            "CREATED_AT": {
                "type": ["string", "null"],
                "format": "date-time"
            },
            "UPDATED_AT": {
                "type": ["string", "null"],
                "format": "date-time"
            }
        }
    }

    # Write schema
    singer.write_schema(
        stream_name="LAUNCHPADS",
        schema=schema,
        key_properties=["LAUNCHPAD_ID"]
    )

    # Get current time with timezone
    current_time = get_current_time()
    current_time_str = current_time.isoformat()

    # Process and write each launchpad record
    for launchpad in launchpads_data:
        # Handle images object specifically
        images = {}
        if "images" in launchpad:
            images = {
                "large": launchpad["images"].get("large", []),
                "small": launchpad["images"].get("small", [])
            }

        # Transform data for Snowflake compatibility
        transformed_launchpad = {
            "LAUNCHPAD_ID": launchpad.get("id"),
            "NAME": launchpad.get("name"),
            "FULL_NAME": launchpad.get("full_name"),
            "STATUS": launchpad.get("status"),
            "LOCALITY": launchpad.get("locality"),
            "REGION": launchpad.get("region"),
            "TIMEZONE": launchpad.get("timezone"),
            "LATITUDE": launchpad.get("latitude"),
            "LONGITUDE": launchpad.get("longitude"),
            "LAUNCH_ATTEMPTS": launchpad.get("launch_attempts"),
            "LAUNCH_SUCCESSES": launchpad.get("launch_successes"),
            "ROCKETS": json.dumps(launchpad.get("rockets", [])),
            "LAUNCHES": json.dumps(launchpad.get("launches", [])),
            "DETAILS": launchpad.get("details"),
            "IMAGES": json.dumps(images),
            "CREATED_AT": current_time_str,
            "UPDATED_AT": current_time_str
        }

        # Write record with timezone-aware timestamp
        singer.write_record(
            stream_name="LAUNCHPADS",
            record=transformed_launchpad,
            time_extracted=current_time
        )

    # Write state
    state = {
        "LAUNCHPADS": {
            "last_sync": current_time_str
        }
    }
    singer.write_state(state)


def fetch_payloads(base_url: str) -> None:
    """
    Fetch and process payloads data from SpaceX API with Snowflake-compatible schema.
    
    Args:
        base_url (str): Base URL for the SpaceX API
    """
    # Fetch data from the payloads endpoint
    response = requests.get(base_url + "payloads")
    response.raise_for_status()
    payloads_data = response.json()

    # Schema definition with Snowflake-compatible types
    schema = {
        "type": "object",
        "properties": {
            "PAYLOAD_ID": {
                "type": ["string", "null"],
                "description": "Unique identifier for the payload"
            },
            "NAME": {
                "type": ["string", "null"],
                "maxLength": 256
            },
            "TYPE": {
                "type": ["string", "null"],
                "maxLength": 100
            },
            "REUSED": {
                "type": ["boolean", "null"]
            },
            "LAUNCH": {
                "type": ["string", "null"],
                "description": "Associated launch ID"
            },
            "CUSTOMERS": {
                "type": ["string", "null"],
                "description": "Array of customer names stored as JSON string"
            },
            "NORAD_IDS": {
                "type": ["string", "null"],
                "description": "Array of NORAD IDs stored as JSON string"
            },
            "NATIONALITIES": {
                "type": ["string", "null"],
                "description": "Array of nationality strings stored as JSON string"
            },
            "MANUFACTURERS": {
                "type": ["string", "null"],
                "description": "Array of manufacturer names stored as JSON string"
            },
            "MASS_KG": {
                "type": ["number", "null"]
            },
            "MASS_LBS": {
                "type": ["number", "null"]
            },
            "ORBIT": {
                "type": ["string", "null"],
                "maxLength": 50
            },
            "REFERENCE_SYSTEM": {
                "type": ["string", "null"],
                "maxLength": 50
            },
            "REGIME": {
                "type": ["string", "null"],
                "maxLength": 50
            },
            "LONGITUDE": {
                "type": ["number", "null"]
            },
            "SEMI_MAJOR_AXIS_KM": {
                "type": ["number", "null"]
            },
            "ECCENTRICITY": {
                "type": ["number", "null"]
            },
            "PERIAPSIS_KM": {
                "type": ["number", "null"]
            },
            "APOAPSIS_KM": {
                "type": ["number", "null"]
            },
            "INCLINATION_DEG": {
                "type": ["number", "null"]
            },
            "PERIOD_MIN": {
                "type": ["number", "null"]
            },
            "LIFESPAN_YEARS": {
                "type": ["number", "null"]
            },
            "EPOCH": {
                "type": ["string", "null"],
                "format": "date-time"
            },
            "MEAN_MOTION": {
                "type": ["number", "null"]
            },
            "RAAN": {
                "type": ["number", "null"]
            },
            "ARG_OF_PERICENTER": {
                "type": ["number", "null"]
            },
            "MEAN_ANOMALY": {
                "type": ["number", "null"]
            },
            "DRAGON": {
                "type": ["string", "null"],
                "description": "Dragon capsule details stored as JSON string"
            },
            "CREATED_AT": {
                "type": ["string", "null"],
                "format": "date-time"
            },
            "UPDATED_AT": {
                "type": ["string", "null"],
                "format": "date-time"
            }
        }
    }

    # Write schema
    singer.write_schema(
        stream_name="PAYLOADS",
        schema=schema,
        key_properties=["PAYLOAD_ID"]
    )

    # Get current time with timezone
    current_time = get_current_time()
    current_time_str = current_time.isoformat()

    # Process and write each payload record
    for payload in payloads_data:
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
            "MASS_KG": payload.get("mass_kg"),
            "MASS_LBS": payload.get("mass_lbs"),
            "ORBIT": payload.get("orbit"),
            "REFERENCE_SYSTEM": payload.get("reference_system"),
            "REGIME": payload.get("regime"),
            "LONGITUDE": payload.get("longitude"),
            "SEMI_MAJOR_AXIS_KM": payload.get("semi_major_axis_km"),
            "ECCENTRICITY": payload.get("eccentricity"),
            "PERIAPSIS_KM": payload.get("periapsis_km"),
            "APOAPSIS_KM": payload.get("apoapsis_km"),
            "INCLINATION_DEG": payload.get("inclination_deg"),
            "PERIOD_MIN": payload.get("period_min"),
            "LIFESPAN_YEARS": payload.get("lifespan_years"),
            "EPOCH": payload.get("epoch"),
            "MEAN_MOTION": payload.get("mean_motion"),
            "RAAN": payload.get("raan"),
            "ARG_OF_PERICENTER": payload.get("arg_of_pericenter"),
            "MEAN_ANOMALY": payload.get("mean_anomaly"),
            "DRAGON": json.dumps(payload.get("dragon", {})) if payload.get("dragon") else None,
            "CREATED_AT": current_time_str,
            "UPDATED_AT": current_time_str
        }

        # Write record with timezone-aware timestamp
        singer.write_record(
            stream_name="PAYLOADS",
            record=transformed_payload,
            time_extracted=current_time
        )

    # Write state
    state = {
        "PAYLOADS": {
            "last_sync": current_time_str
        }
    }
    singer.write_state(state)
    

def fetch_roadster(base_url: str) -> None:
    """
    Fetch and process roadster data from SpaceX API with Snowflake-compatible schema.
    
    Args:
        base_url (str): Base URL for the SpaceX API
    """
    # Fetch data from the roadster endpoint
    response = requests.get(base_url + "roadster")
    response.raise_for_status()
    roadster_data = response.json()

    # Schema definition with Snowflake-compatible types
    schema = {
        "type": "object",
        "properties": {
            "ROADSTER_ID": {
                "type": ["string", "null"],
                "description": "Unique identifier for the roadster"
            },
            "NAME": {
                "type": ["string", "null"],
                "maxLength": 256
            },
            "LAUNCH_DATE_UTC": {
                "type": ["string", "null"],
                "format": "date-time"
            },
            "LAUNCH_DATE_UNIX": {
                "type": ["integer", "null"]
            },
            "LAUNCH_MASS_KG": {
                "type": ["number", "null"]
            },
            "LAUNCH_MASS_LBS": {
                "type": ["number", "null"]
            },
            "NORAD_ID": {
                "type": ["integer", "null"]
            },
            "EPOCH_JD": {
                "type": ["number", "null"],
                "description": "Julian Date of epoch"
            },
            "ORBIT_TYPE": {
                "type": ["string", "null"]
            },
            "APOAPSIS_AU": {
                "type": ["number", "null"]
            },
            "PERIAPSIS_AU": {
                "type": ["number", "null"]
            },
            "SEMI_MAJOR_AXIS_AU": {
                "type": ["number", "null"]
            },
            "ECCENTRICITY": {
                "type": ["number", "null"]
            },
            "INCLINATION": {
                "type": ["number", "null"]
            },
            "LONGITUDE": {
                "type": ["number", "null"]
            },
            "PERIOD_DAYS": {
                "type": ["number", "null"]
            },
            "SPEED_KPH": {
                "type": ["number", "null"]
            },
            "SPEED_MPH": {
                "type": ["number", "null"]
            },
            "EARTH_DISTANCE_KM": {
                "type": ["number", "null"]
            },
            "EARTH_DISTANCE_MI": {
                "type": ["number", "null"]
            },
            "MARS_DISTANCE_KM": {
                "type": ["number", "null"]
            },
            "MARS_DISTANCE_MI": {
                "type": ["number", "null"]
            },
            "WIKIPEDIA": {
                "type": ["string", "null"]
            },
            "DETAILS": {
                "type": ["string", "null"]
            },
            "VIDEO": {
                "type": ["string", "null"],
                "description": "URL of video"
            },
            "FLICKR_IMAGES": {
                "type": ["string", "null"],
                "description": "Array of image URLs stored as JSON string"
            },
            "CREATED_AT": {
                "type": ["string", "null"],
                "format": "date-time"
            },
            "UPDATED_AT": {
                "type": ["string", "null"],
                "format": "date-time"
            }
        }
    }

    # Write schema
    singer.write_schema(
        stream_name="ROADSTER",
        schema=schema,
        key_properties=["ROADSTER_ID"]
    )

    # Get current time with timezone
    current_time = get_current_time()
    current_time_str = current_time.isoformat()

    # Transform data for Snowflake compatibility
    transformed_roadster = {
        "ROADSTER_ID": roadster_data.get("id"),
        "NAME": roadster_data.get("name"),
        "LAUNCH_DATE_UTC": roadster_data.get("launch_date_utc"),
        "LAUNCH_DATE_UNIX": roadster_data.get("launch_date_unix"),
        "LAUNCH_MASS_KG": roadster_data.get("launch_mass_kg"),
        "LAUNCH_MASS_LBS": roadster_data.get("launch_mass_lbs"),
        "NORAD_ID": roadster_data.get("norad_id"),
        "EPOCH_JD": roadster_data.get("epoch_jd"),
        "ORBIT_TYPE": roadster_data.get("orbit_type"),
        "APOAPSIS_AU": roadster_data.get("apoapsis_au"),
        "PERIAPSIS_AU": roadster_data.get("periapsis_au"),
        "SEMI_MAJOR_AXIS_AU": roadster_data.get("semi_major_axis_au"),
        "ECCENTRICITY": roadster_data.get("eccentricity"),
        "INCLINATION": roadster_data.get("inclination"),
        "LONGITUDE": roadster_data.get("longitude"),
        "PERIOD_DAYS": roadster_data.get("period_days"),
        "SPEED_KPH": roadster_data.get("speed_kph"),
        "SPEED_MPH": roadster_data.get("speed_mph"),
        "EARTH_DISTANCE_KM": roadster_data.get("earth_distance_km"),
        "EARTH_DISTANCE_MI": roadster_data.get("earth_distance_mi"),
        "MARS_DISTANCE_KM": roadster_data.get("mars_distance_km"),
        "MARS_DISTANCE_MI": roadster_data.get("mars_distance_mi"),
        "WIKIPEDIA": roadster_data.get("wikipedia"),
        "DETAILS": roadster_data.get("details"),
        "VIDEO": roadster_data.get("video"),
        "FLICKR_IMAGES": json.dumps(roadster_data.get("flickr_images", [])),
        "CREATED_AT": current_time_str,
        "UPDATED_AT": current_time_str
    }

    # Write record with timezone-aware timestamp
    singer.write_record(
        stream_name="ROADSTER",
        record=transformed_roadster,
        time_extracted=current_time
    )

    # Write state
    state = {
        "ROADSTER": {
            "last_sync": current_time_str
        }
    }
    singer.write_state(state)


def fetch_rockets(base_url: str) -> None:
    """
    Fetch and process rockets data from SpaceX API with Snowflake-compatible schema.
    
    Args:
        base_url (str): Base URL for the SpaceX API
    """
    # Fetch data from the rockets endpoint
    response = requests.get(base_url + "rockets")
    response.raise_for_status()
    rockets_data = response.json()

    # Schema definition with Snowflake-compatible types
    schema = {
        "type": "object",
        "properties": {
            "ROCKET_ID": {
                "type": ["string", "null"],
                "description": "Unique identifier for the rocket"
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
            "STAGES": {
                "type": ["integer", "null"]
            },
            "BOOSTERS": {
                "type": ["integer", "null"]
            },
            "COST_PER_LAUNCH": {
                "type": ["integer", "null"]
            },
            "SUCCESS_RATE_PCT": {
                "type": ["integer", "null"]
            },
            "FIRST_FLIGHT": {
                "type": ["string", "null"],
                "format": "date"
            },
            "COUNTRY": {
                "type": ["string", "null"],
                "maxLength": 100
            },
            "COMPANY": {
                "type": ["string", "null"],
                "maxLength": 100
            },
            "HEIGHT_METERS": {
                "type": ["number", "null"]
            },
            "HEIGHT_FEET": {
                "type": ["number", "null"]
            },
            "DIAMETER_METERS": {
                "type": ["number", "null"]
            },
            "DIAMETER_FEET": {
                "type": ["number", "null"]
            },
            "MASS_KG": {
                "type": ["number", "null"]
            },
            "MASS_LBS": {
                "type": ["number", "null"]
            },
            "PAYLOAD_WEIGHTS": {
                "type": ["string", "null"],
                "description": "Array of payload weight info stored as JSON string"
            },
            "FIRST_STAGE": {
                "type": ["string", "null"],
                "description": "First stage details stored as JSON string"
            },
            "SECOND_STAGE": {
                "type": ["string", "null"],
                "description": "Second stage details stored as JSON string"
            },
            "ENGINES": {
                "type": ["string", "null"],
                "description": "Engine details stored as JSON string"
            },
            "LANDING_LEGS": {
                "type": ["string", "null"],
                "description": "Landing legs details stored as JSON string"
            },
            "FLICKR_IMAGES": {
                "type": ["string", "null"],
                "description": "Array of image URLs stored as JSON string"
            },
            "WIKIPEDIA": {
                "type": ["string", "null"]
            },
            "DESCRIPTION": {
                "type": ["string", "null"]
            },
            "CREATED_AT": {
                "type": ["string", "null"],
                "format": "date-time"
            },
            "UPDATED_AT": {
                "type": ["string", "null"],
                "format": "date-time"
            }
        }
    }

    # Write schema
    singer.write_schema(
        stream_name="ROCKETS",
        schema=schema,
        key_properties=["ROCKET_ID"]
    )

    # Get current time with timezone
    current_time = get_current_time()
    current_time_str = current_time.isoformat()

    # Process and write each rocket record
    for rocket in rockets_data:
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
            "PAYLOAD_WEIGHTS": json.dumps(rocket.get("payload_weights", [])),
            "FIRST_STAGE": json.dumps(rocket.get("first_stage", {})),
            "SECOND_STAGE": json.dumps(rocket.get("second_stage", {})),
            "ENGINES": json.dumps(rocket.get("engines", {})),
            "LANDING_LEGS": json.dumps(rocket.get("landing_legs", {})),
            "FLICKR_IMAGES": json.dumps(rocket.get("flickr_images", [])),
            "WIKIPEDIA": rocket.get("wikipedia"),
            "DESCRIPTION": rocket.get("description"),
            "CREATED_AT": current_time_str,
            "UPDATED_AT": current_time_str
        }

        # Write record with timezone-aware timestamp
        singer.write_record(
            stream_name="ROCKETS",
            record=transformed_rocket,
            time_extracted=current_time
        )

    # Write state
    state = {
        "ROCKETS": {
            "last_sync": current_time_str
        }
    }
    singer.write_state(state)


def fetch_ships(base_url: str) -> None:
    """
    Fetch and process ships data from SpaceX API with Snowflake-compatible schema.
    
    Args:
        base_url (str): Base URL for the SpaceX API
    """
    # Fetch data from the ships endpoint
    response = requests.get(base_url + "ships")
    response.raise_for_status()
    ships_data = response.json()

    # Schema definition with Snowflake-compatible types
    schema = {
        "type": "object",
        "properties": {
            "SHIP_ID": {
                "type": ["string", "null"],
                "description": "Unique identifier for the ship"
            },
            "NAME": {
                "type": ["string", "null"],
                "maxLength": 256
            },
            "LEGACY_ID": {
                "type": ["string", "null"],
                "description": "Legacy identifier if exists"
            },
            "MODEL": {
                "type": ["string", "null"],
                "maxLength": 100
            },
            "TYPE": {
                "type": ["string", "null"],
                "maxLength": 100
            },
            "ACTIVE": {
                "type": ["boolean", "null"]
            },
            "IMO": {
                "type": ["integer", "null"],
                "description": "International Maritime Organization number"
            },
            "MMSI": {
                "type": ["integer", "null"],
                "description": "Maritime Mobile Service Identity number"
            },
            "ABS": {
                "type": ["integer", "null"],
                "description": "American Bureau of Shipping identification"
            },
            "CLASS": {
                "type": ["integer", "null"]
            },
            "MASS_KG": {
                "type": ["integer", "null"]
            },
            "MASS_LBS": {
                "type": ["integer", "null"]
            },
            "YEAR_BUILT": {
                "type": ["integer", "null"]
            },
            "HOME_PORT": {
                "type": ["string", "null"],
                "maxLength": 100
            },
            "STATUS": {
                "type": ["string", "null"],
                "maxLength": 100
            },
            "SPEED_KN": {
                "type": ["number", "null"],
                "description": "Speed in knots"
            },
            "COURSE_DEG": {
                "type": ["number", "null"],
                "description": "Course in degrees"
            },
            "LATITUDE": {
                "type": ["number", "null"]
            },
            "LONGITUDE": {
                "type": ["number", "null"]
            },
            "LAST_AIS_UPDATE": {
                "type": ["string", "null"],
                "format": "date-time",
                "description": "Last AIS update timestamp"
            },
            "LINK": {
                "type": ["string", "null"],
                "description": "URL to Marine Traffic page"
            },
            "IMAGE": {
                "type": ["string", "null"],
                "description": "URL to ship image"
            },
            "LAUNCHES": {
                "type": ["string", "null"],
                "description": "Array of launch IDs stored as JSON string"
            },
            "ROLES": {
                "type": ["string", "null"],
                "description": "Array of roles stored as JSON string"
            },
            "CREATED_AT": {
                "type": ["string", "null"],
                "format": "date-time"
            },
            "UPDATED_AT": {
                "type": ["string", "null"],
                "format": "date-time"
            }
        }
    }

    # Write schema
    singer.write_schema(
        stream_name="SHIPS",
        schema=schema,
        key_properties=["SHIP_ID"]
    )

    # Get current time with timezone
    current_time = get_current_time()
    current_time_str = current_time.isoformat()

    # Process and write each ship record
    for ship in ships_data:
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
            "MASS_KG": ship.get("mass_kg"),
            "MASS_LBS": ship.get("mass_lbs"),
            "YEAR_BUILT": ship.get("year_built"),
            "HOME_PORT": ship.get("home_port"),
            "STATUS": ship.get("status"),
            "SPEED_KN": ship.get("speed_kn"),
            "COURSE_DEG": ship.get("course_deg"),
            "LATITUDE": ship.get("latitude"),
            "LONGITUDE": ship.get("longitude"),
            "LAST_AIS_UPDATE": ship.get("last_ais_update"),
            "LINK": ship.get("link"),
            "IMAGE": ship.get("image"),
            "LAUNCHES": json.dumps(ship.get("launches", [])),
            "ROLES": json.dumps(ship.get("roles", [])),
            "CREATED_AT": current_time_str,
            "UPDATED_AT": current_time_str
        }

        # Write record with timezone-aware timestamp
        singer.write_record(
            stream_name="SHIPS",
            record=transformed_ship,
            time_extracted=current_time
        )

    # Write state
    state = {
        "SHIPS": {
            "last_sync": current_time_str
        }
    }
    singer.write_state(state)
    

def fetch_starlink(base_url: str) -> None:
    """
    Fetch and process Starlink satellites data from SpaceX API with Snowflake-compatible schema.
    
    Args:
        base_url (str): Base URL for the SpaceX API
    """
    # Fetch data from the starlink endpoint
    response = requests.get(base_url + "starlink")
    response.raise_for_status()
    starlink_data = response.json()

    # Schema definition with Snowflake-compatible types
    schema = {
        "type": "object",
        "properties": {
            "STARLINK_ID": {
                "type": ["string", "null"],
                "description": "Unique identifier for the Starlink satellite"
            },
            "VERSION": {
                "type": ["string", "null"],
                "maxLength": 50
            },
            "LAUNCH": {
                "type": ["string", "null"],
                "description": "Associated launch ID"
            },
            "LONGITUDE": {
                "type": ["number", "null"]
            },
            "LATITUDE": {
                "type": ["number", "null"]
            },
            "HEIGHT_KM": {
                "type": ["number", "null"]
            },
            "VELOCITY_KMS": {
                "type": ["number", "null"]
            },
            "SPACETRACK": {
                "type": ["string", "null"],
                "description": "Space-Track.org data stored as JSON string"
            },
            "LAUNCH_DATE": {
                "type": ["string", "null"],
                "format": "date-time"
            },
            "OBJECT_NAME": {
                "type": ["string", "null"],
                "maxLength": 100
            },
            "OBJECT_ID": {
                "type": ["string", "null"],
                "maxLength": 50
            },
            "EPOCH": {
                "type": ["string", "null"],
                "format": "date-time"
            },
            "PERIOD_MIN": {
                "type": ["number", "null"]
            },
            "INCLINATION_DEG": {
                "type": ["number", "null"]
            },
            "APOAPSIS_KM": {
                "type": ["number", "null"]
            },
            "PERIAPSIS_KM": {
                "type": ["number", "null"]
            },
            "ECCENTRICITY": {
                "type": ["number", "null"]
            },
            "MEAN_MOTION": {
                "type": ["number", "null"]
            },
            "MEAN_ANOMALY": {
                "type": ["number", "null"]
            },
            "ARG_OF_PERICENTER": {
                "type": ["number", "null"]
            },
            "RAAN": {
                "type": ["number", "null"],
                "description": "Right Ascension of the Ascending Node"
            },
            "SEMI_MAJOR_AXIS_KM": {
                "type": ["number", "null"]
            },
            "CREATED_AT": {
                "type": ["string", "null"],
                "format": "date-time"
            },
            "UPDATED_AT": {
                "type": ["string", "null"],
                "format": "date-time"
            }
        }
    }

    # Write schema
    singer.write_schema(
        stream_name="STARLINK",
        schema=schema,
        key_properties=["STARLINK_ID"]
    )

    # Get current time with timezone
    current_time = get_current_time()
    current_time_str = current_time.isoformat()

    # Process and write each Starlink satellite record
    for satellite in starlink_data:
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
            "UPDATED_AT": current_time_str
        }

        # Write record with timezone-aware timestamp
        singer.write_record(
            stream_name="STARLINK",
            record=transformed_satellite,
            time_extracted=current_time
        )

    # Write state
    state = {
        "STARLINK": {
            "last_sync": current_time_str
        }
    }
    singer.write_state(state)

