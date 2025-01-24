import singer                               # type: ignore
import pandas as pd                         # type: ignore
import numpy as np                          # type: ignore

LOGGER  = singer.get_logger()

def fetch_launches(based_url:str) -> None:
    url = based_url + "launches"
    df = pd.read_json(url)
    df = df.fillna('')
    df['success'] = df['success'].apply(lambda x: False if not isinstance(x, bool) else x)
    df['static_fire_date_unix'] = df['static_fire_date_unix'].apply(lambda x: 0 if not isinstance(x, float) else x)
    df['window'] = df['window'].apply(lambda x: 0 if not isinstance(x, float) else x)
    
    
    records= df.to_dict(orient="records")
    schema = {
        "properties": {
            "id": {
                "type": "string"
            },
            "flight_number": {
               "type": "number"
            },
            "name": {
              "type": "string"
            },
            "date_utc": {
              "type": "string"
            },
            "date_unix": {
              "type": "number"
            },
            "date_local": {
              "type": "string"
            },
            "date_precision": {
              "type": "string"
            },
            "static_fire_date_utc": {
              "type": "string"
            },
            "static_fire_date_unix": {
              "type": "number"
            },
            "tdb": {
              "type": "boolean"
            },
            "net": {
              "type": "boolean"
            },
            "window": {
              "type": "number"
            },
            "rocket": {
              "type": "string"
            },
            "success": {
              "type": "boolean"
            },
            "upcoming": {
              "type": "boolean"
            },
            "details": {
              "type": "string"
            },
            "launchpad": {
              "type": "string"
            },
            "presskit": {
              "type": "string"
            },
            "webcast": {
              "type": "string"
            },
            "youtube_id": {
              "type": "string"
            },
            "article": {
              "type": "string"
            },
            "wikipedia": {
              "type": "string"
            }
        }
    }
    
    singer.write_schema("launches", schema, "id")
    singer.write_records("launches", records)


def fetch_capsules(based_url:str) -> None:
    url = based_url + "capsules"
    df = pd.read_json(url)
    df = df.replace({np.nan: None})
    df['launches'] = df['launches'].apply(lambda x: str(x) if not isinstance(x, str) else x)
    df['last_update'] = df['last_update'].apply(lambda x: '' if not isinstance(x, str) else x)

    schema = {
        "properties": {
            "id": {
              "type": "string"
            },
            "serial": {
              "type": "string"
            },
            "status": {
              "type": "string"
            },
            "type": {
              "type": "string"
            },
            "dragon": {
              "type": "string"
            },
            "reuse_count": {
              "type": "number"
            },
            "water_landings": {
              "type": "number"
            },
            "land_landings": {
              "type": "number"
            },
            "last_update": {
              "type": "string"
            },
            "launches": {
              "type": "string"
            }
        }   
    }
    
    records = df.to_dict(orient="records")

    singer.write_schema("capsules", schema, "id")
    singer.write_records("capsules", records)

def fetch_company(based_url:str) -> None:
    df = pd.read_json(based_url + "company")
    # Replace NaN values with None
    df = df.replace({np.nan: None})
    
    schema = {
        "properties": {
            "id": {
              "type": "string"
            },
            "name": {
              "type": "string"
            },
            "founder": {
              "type": "string"
            },
            "founded": {
              "type": "number"
            },
            "employees": {
              "type": "number"
            },
            "vehicles": {
              "type": "number"
            },
            "launch_sites": {
              "type": "number"
            },
            "test_sites": {
              "type": "number"
            },
            "ceo": {
              "type": "string"
            },
            "cto": {
              "type": "string"
            },
            "coo": {
              "type": "string"
            },
            "cto_propulsion": {
              "type": "string"
            },
            "valuation": {
              "type": "number"
            },
            "summary": {
              "type": "string"
            }
        }   
    }
    
    records = df.to_dict(orient="records")

    singer.write_schema("company", schema, "id")
    singer.write_records("company", records)
  
def fetch_cores(based_url:str) -> None:
    df = pd.read_json(based_url + "cores")

    schema = {
        "properties": {
            "id": {
              "type": "string"
            },
            "serial": {
              "type": "string"
            },
            "block": {
              "type": "number"
            },
            "status": {
              "type": "string"
            },
            "reuse_count": {
              "type": "number"
            },
            "rtls_attempts": {
              "type": "number"
            },
            "rtls_landings": {
              "type": "number"
            },
            "asds_attempts": {
              "type": "number"
            },
            "asds_landings": {
              "type": "number"
            },
            "last_update": {
              "type": "string"
            },
            "launches": {
              "type": "string"
            }
        }   
    }
    
    df = df.dropna()
    df['block'] = df['block'].apply(lambda x: 0 if not isinstance(x, float) else x )
    df['launches'] = df['launches'].apply(lambda x: str(x))
    records = df.to_dict(orient="records")

    singer.write_schema("cores", schema, "id")
    singer.write_records("cores", records)


def fetch_crew(based_url:str) -> None:
    df = pd.read_json(based_url + "crew")

    schema = {
        "properties": {
            "id": {
              "type": "string"
            },
            "name": {
              "type": "string"
            },
            "status": {
              "type": "string"
            },
            "agency": {
              "type": "string"
            },
            "image": {
              "type": "string"
            },
            "wikipedia": {
              "type": "string"
            },
            "launches": {
              "type": "string"
            }
        }   
    }
    
    # Replace NaN values with None
    df = df.replace({np.nan: None})
    df['launches'] = df['launches'].apply(lambda x: str(x))
    records = df.to_dict(orient="records")

    singer.write_schema("crew", schema, "id")
    singer.write_records("crew", records)


def fetch_dragons(based_url:str) -> None:
    df = pd.read_json(based_url + "dragons")

    schema = {
        "properties": {
            "id": {
              "type": "string"
            },
            "name": {
              "type": "string"
            },
            "type": {
              "type": "string"
            },
            "active": {
              "type": "boolean"
            },
            "crew_capacity": {
              "type": "number"
            },
            "sidewall_angle_deg": {
              "type": "number"
            },
            "orbit_duration_yr": {
              "type": "number"
            },
            "dry_mass_kg": {
              "type": "number"
            },
            "dry_mass_lb": {
              "type": "number"
            },
            "first_flight": {
              "type": "string"
            },
            "wikipedia": {
              "type": "string"
            },
            "description": {
              "type": "string"
            }
        }   
    }
    
    # Replace NaN values with None
    df = df.replace({np.nan: None})
    records = df.to_dict(orient="records")

    singer.write_schema("dragons", schema, "id")
    singer.write_records("dragons", records)


def fetch_history(based_url:str) -> None:
    df = pd.read_json(based_url + "history")

    schema = {
        "properties": {
          "id": {
                "type": "string"
          },
          "title": {
            "type": "string"
          },
          "event_date_utc": {
            "type": "string"
          },
          "event_date_unix": {
            "type": "number"
          },
          "details": {
            "type": "string"
          }
        }   
    }
    
    # Replace NaN values with None
    df = df.replace({np.nan: None})
    records = df.to_dict(orient="records")

    singer.write_schema("history", schema, "id")
    singer.write_records("history", records)


def fetch_landpads(based_url:str) -> None:
    df = pd.read_json(based_url + "landpads")

    schema = {
        "properties": {
            "id": {
              "type": "string"
            },
            "name": {
              "type": "string"
            },
            "full_name": {
              "type": "string"
            },
            "status": {
              "type": "string"
            },
            "type": {
              "type": "string"
            },
            "locality": {
              "type": "string"
            },
            "region": {
              "type": "string"
            },
            "latitude": {
              "type": "number"
            },
            "longitude": {
              "type": "number"
            },
            "landing_attempts": {
              "type": "number"
            },
            "landing_successes": {
              "type": "number"
            },
            "wikipedia": {
              "type": "string"
            },
            "details": {
              "type": "string"
            },
            "launches": {
              "type": "string"
            }
        }   
    }
    
    # Replace NaN values with None
    df = df.replace({np.nan: None})
    df['launches'] = df['launches'].apply(lambda x: str(x))
    records = df.to_dict(orient="records")

    singer.write_schema("landpads", schema, "id")
    singer.write_records("landpads", records)

def fetch_launchpads(based_url:str) -> None:
    df = pd.read_json(based_url + "launchpads")

    schema = {
        "properties": {
            "id": {
                  "type": "string"
            },
            "name": {
              "type": "string",
            },
            "full_name": {
              "type": "string",
            },
            "status": {
              "type": "string"
            },
            "locality": {
              "type": "string"
            },
            "region": {
              "type": "string"
            },
            "timezone": {
              "type": "string"
            },
            "latitude": {
              "type": "number"
            },
            "longitude": {
              "type": "number"
            },
            "launch_attempts": {
              "type": "number"
            },
            "launch_successes": {
              "type": "number"
            },
            "rockets": {
              "type": "array"
            },
            "launches": {
              "type": "array"
            },
        }   
    }
    
    # Replace NaN values with None
    df = df.replace({np.nan: None})
    records = df.to_dict(orient="records")

    singer.write_schema("launchpads", schema, "id")
    singer.write_records("launchpads", records)


def fetch_payloads(based_url:str) -> None:
    df = pd.read_json(based_url + "payloads")
    
    schema = {
        "properties": {
            "id": {
              "type": "string"
            },
            "name": {
              "type": "string"
            },
            "type": {
              "type": "string"
            },
            "reused": {
              "type": "boolean"
            },
            "launch": {
              "type": "string"
            },
            "customers": {
              "type": "array"
            },
            "norad_ids": {
              "type": "array"
            },
            "nationalities": {
              "type": "array"
            },
            "manufacturers": {
              "type": "array"
            },
            "mass_kg": {
              "type": "number"
            },
            "mass_lbs": {
              "type": "number"
            },
            "orbit": {
              "type": "string"
            },
            "reference_system": {
              "type": "string"
            },
            "regime": {
              "type": "string"
            },
            "longitude": {
              "type": "string"
            },
            "semi_major_axis_km": {
              "type": "number"
            },
            "eccentricity": {
              "type": "number"
            },
            "periapsis_km": {
              "type": "number"
            },
            "apoapsis_km": {
              "type": "number"
            },
            "inclination_deg": {
              "type": "number"
            },
            "period_min": {
              "type": "number"
            },
            "lifespan_years": {
              "type": "number"
            },
            "epoch": {
              "type": "string"
            },
            "mean_motion": {
              "type": "number"
            },
            "raan": {
              "type": "number"
            },
            "arg_of_pericenter": {
              "type": "number"
            },
            "mean_anomaly": {
              "type": "number"
            }
        }   
    }
    
    # Replace NaN values with None
    df = df.dropna()
    df['launch'] = df['launch'].apply(lambda x: str(x))
    df['longitude'] = df['longitude'].apply(lambda x: str(x) if not isinstance(x, str) else x)
    df['semi_major_axis_km'] = df['semi_major_axis_km'].apply(lambda x: float(x) if not isinstance(x, float) else x)
    records = df.to_dict(orient="records")

    singer.write_schema("payloads", schema, "id")
    singer.write_records("payloads", records)


def fetch_roadster(based_url:str) -> None:
    df = pd.read_json(based_url + "roadster")

    schema = {
        "properties": {
          "id": {
                "type": "string"
            },
          "name": {
            "type": "string"
          },
          "launch_date_utc": {
            "type": "string"
          },
          "launch_date_unix": {
            "type": "number"
          },
          "launch_mass_kg": {
            "type": "number"
          },
          "launch_mass_lbs": {
            "type": "number"
          },
          "norad_id": {
            "type": "number"
          },
          "epoch_jd": {
            "type": "number"
          },
          "orbit_type": {
            "type": "string"
          },
          "apoapsis_au": {
            "type": "number"
          },
          "periapsis_au": {
            "type": "number"
          },
          "semi_major_axis_au": {
            "type": "number"
          },
          "eccentricity": {
            "type": "number"
          },
          "inclination": {
            "type": "number"
          },
          "longitude": {
            "type": "number"
          },
          "periapsis_arg": {
            "type": "number"
          },
          "period_days": {
            "type": "number"
          },
          "speed_kph": {
            "type": "number"
          },
          "speed_mph": {
            "type": "number"
          },
          "earth_distance_km": {
            "type": "number"
          },
          "earth_distance_mi": {
            "type": "number"
          },
          "mars_distance_km": {
            "type": "number"
          },
          "mars_distance_mi": {
            "type": "number"
          },
          "flickr_images": {
            "type": "string"
          },
          "wikipedia": {
            "type": "string"
          },
          "video": {
            "type": "string"
          },
          "details": {
            "type": "string"
          }
        }   
    }
    
    # Replace NaN values with None
    df = df.replace({np.nan: None})
    records = df.to_dict(orient="records")

    singer.write_schema("roadster", schema, "id")
    singer.write_records("roadster", records)


def fetch_rockets(based_url:str) -> None:
    url = based_url + "rockets"
    df = pd.read_json(url)

    schema = {
        "properties": {
            "id": {
                "type": "string"
            },
            "name": {
              "type": "string"
            },
            "type": {
              "type": "string"
            },
            "active": {
              "type": "boolean"
            },
            "stages": {
              "type": "number"
            },
            "boosters": {
              "type": "number"
            },
            "cost_per_launch": {
              "type": "number"
            },
            "success_rate_pct": {
              "type": "number"
            },
            "first_flight": {
              "type": "string"
            },
            "country": {
              "type": "string"
            },
            "company": {
              "type": "string"
            },
            "description": {
              "type": "string"
            }
        }
    }

    # Replace NaN values with None
    df = df.replace({np.nan: None})
    records = df.to_dict(orient="records")

    singer.write_schema("rockets", schema, "id")
    singer.write_records("rockets", records)


def fetch_ships(based_url:str) -> None:
    df = pd.read_json(based_url + "ships")
    df = df.replace({np.nan: None})
    df['model'] = df['model'].apply(lambda x: 'Null' if x is None else x)
    df['speed_kn'] = df['speed_kn'].apply(lambda x: 0 if x is None else x)
    df['course_deg'] = df['course_deg'].apply(lambda x: 0 if x is None else x)
    df['latitude'] = df['latitude'].apply(lambda x: 0 if x is None else x)
    df['longitude'] = df['longitude'].apply(lambda x: 0 if x is None else x)
    df['last_ais_update'] = df['last_ais_update'].apply(lambda x: '' if x is None else x)
    df['imo'] = df['imo'].apply(lambda x: '' if x is None else x)

    schema = {
        "properties": {
            "id": {
                "type": "string"
            },
            "name": {
              "type": "string"
            },
            "legacy_id": {
              "type": "string"
            },
            "model": {
              "type": "string"
            },
            "type": {
              "type": "string"
            },
            "roles": {
              "type": "array"
            },
            "active": {
              "type": "boolean"
            },
            "imo": {
              "type": "number"
            },
            "mmsi": {
              "type": "number"
            },
            "abs": {
              "type": "number"
            },
            "class": {
              "type": "number"
            },
            "mass_kg": {
              "type": "number"
            },
            "mass_lbs": {
              "type": "number"
            },
            "year_built": {
              "type": "number"
            },
            "home_port": {
              "type": "string"
            },
            "status": {
              "type": "string"
            },
            "speed_kn": {
              "type": "number"
            },
            "course_deg": {
              "type": "number"
            },
            "latitude": {
              "type": "number"
            },
            "longitude": {
              "type": "number"
            },
            "last_ais_update": {
              "type": "string"
            },
            "link": {
              "type": "string"
            },
            "image": {
              "type": "string"
            },
            "launches": {
              "type": "array"
            }
        }   
    }
    
    records = df.to_dict(orient="records")

    singer.write_schema("ships", schema, "id")
    singer.write_records("ships", records)
    

def fetch_starlink(based_url:str) -> None:
    df = pd.read_json(based_url + "history")

    schema = {
        "properties": {
          "id": {
                "type": "string"
          },
          "title": {
            "type": "string"
          },
          "event_date_utc": {
            "type": "string"
          },
          "event_date_unix": {
            "type": "number"
          },
          "details": {
            "type": "string"
          }
        }   
    }
    
    # Replace NaN values with None
    df = df.replace({np.nan: None})
    records = df.to_dict(orient="records")

    singer.write_schema("history", schema, "id")
    singer.write_records("history", records)

