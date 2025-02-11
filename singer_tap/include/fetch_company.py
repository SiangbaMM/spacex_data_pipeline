
import singer                                           # type: ignore
import requests                                         # type: ignore
import pytz                                             # type: ignore
import json
from datetime import datetime
from include.spacex_tap_base import SpaceXTapBase


class CompanyTap(SpaceXTapBase):
    
    def __init__(self, base_url: str, config_path: str):
        super().__init__(base_url, config_path)

    def fetch_company(self):
        """
        Fetch and process company data.
        """
        
        stream_name = "STG_SPACEX_DATA_COMPANY"
        
        try:
            # Fetch data from API
            response = requests.get(self.base_url + "company")
            response.raise_for_status()
            company_data = response.json()

            # Schema definition for company data
            schema = {
                "type": "object",
                "properties": {
                    "ID": {"type": ["string", "null"]},
                    "NAME": {"type": ["string", "null"]},
                    "FOUNDER": {"type": ["string", "null"]},
                    "FOUNDED": {"type": ["integer", "null"]},
                    "EMPLOYEES": {"type": ["integer", "null"]},
                    "VEHICLES": {"type": ["integer", "null"]},
                    "LAUNCH_SITES": {"type": ["integer", "null"]},
                    "TEST_SITES": {"type": ["integer", "null"]},
                    "CEO": {"type": ["string", "null"]},
                    "CTO": {"type": ["string", "null"]},
                    "COO": {"type": ["string", "null"]},
                    "CTO_PROPULSION": {"type": ["string", "null"]},
                    "VALUATION": {"type": ["number", "null"]},
                    "HEADQUARTERS": {"type": ["object", "null"]},
                    "LINKS": {"type": ["object", "null"]},
                    "SUMMARY": {"type": ["string", "null"]},
                    "CREATED_AT": {"type": ["string", "null"]},
                    "UPDATED_AT": {"type": ["string", "null"]},
                    "RAW_DATA": {"type": ["string", "null"]}
                }
            }

            # Write schema
            singer.write_schema(
                stream_name=stream_name,
                schema=schema,
                key_properties=["ID"]
            )
            
            # Process each capsule
            current_time = self.get_current_time()
            current_time_str = current_time.isoformat()

            
            try:
                # Transform and write record
                transformed_company = {
                    "ID": company_data.get("id"),
                    "NAME": company_data.get("name"),
                    "FOUNDER": company_data.get("founder"),
                    "FOUNDED": company_data.get("founded"),
                    "EMPLOYEES": company_data.get("employees"),
                    "VEHICLES": company_data.get("vehicles"),
                    "LAUNCH_SITES": company_data.get("launch_sites"),
                    "TEST_SITES": company_data.get("test_sites"),
                    "CEO": company_data.get("ceo"),
                    "CTO": company_data.get("cto"),
                    "COO": company_data.get("coo"),
                    "CTO_PROPULSION": company_data.get("cto_propulsion"),
                    "VALUATION": company_data.get("valuation"),
                    "HEADQUARTERS": company_data.get("headquarters", {}),
                    "LINKS": company_data.get("links", {}),
                    "SUMMARY": company_data.get("summary"),
                    "CREATED_AT": current_time_str,
                    "UPDATED_AT": current_time_str,
                    "RAW_DATA": json.dumps(company_data)
                }

                # Write record
                singer.write_record(
                    stream_name=stream_name,
                    record=transformed_company
                )

            except Exception as transform_error:
                self.log_error(
                    table_name=stream_name,
                    error_message=f"Data transformation error: {str(transform_error)}",
                    error_data=company_data
                )
                raise
            
            # Write state
            state = {
                "STG_SPACEX_DATA_CAPSULES": {
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
