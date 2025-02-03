import pytest
from unittest.mock import patch, MagicMock
import json
from datetime import datetime
from include.fetch_company import CompanyTap

@pytest.fixture
def company_tap():
    return CompanyTap(base_url="https://api.spacexdata.com/v4/", config_path="config_snowflake.json")

# Sample API response data
SAMPLE_COMPANY_DATA = {
    "id": "5eb75edc42fea42237d7f3ed",
    "name": "SpaceX",
    "founder": "Elon Musk",
    "founded": 2002,
    "employees": 10000,
    "vehicles": 3,
    "launch_sites": 3,
    "test_sites": 3,
    "ceo": "Elon Musk",
    "cto": "Unknown",
    "coo": "Gwynne Shotwell",
    "cto_propulsion": "Tom Mueller",
    "valuation": 74000000000,
    "headquarters": {
        "address": "Rocket Road",
        "city": "Hawthorne",
        "state": "California"
    },
    "links": {
        "website": "https://www.spacex.com/",
        "flickr": "https://www.flickr.com/photos/spacex/",
        "twitter": "https://twitter.com/SpaceX",
        "elon_twitter": "https://twitter.com/elonmusk"
    },
    "summary": "SpaceX designs, manufactures and launches advanced rockets and spacecraft."
}

def test_fetch_company_successful():
    """Test successful company data fetching and transformation"""
    with patch('requests.get') as mock_get, \
        patch('singer.write_schema') as mock_write_schema, \
        patch('singer.write_record') as mock_write_record:
        
        # Mock API response
        mock_response = MagicMock()
        mock_response.json.return_value = SAMPLE_COMPANY_DATA
        mock_get.return_value = mock_response

        # Execute fetch_company
        company_tap.fetch_company()

        # Verify API call
        mock_get.assert_called_once_with("https://api.spacexdata.com/v4/company")

        # Verify schema write
        mock_write_schema.assert_called_once()
        schema_call = mock_write_schema.call_args[1]
        assert schema_call['stream_name'] == "company"
        assert schema_call['key_properties'] == ["id"]

        # Verify record write
        mock_write_record.assert_called_once()
        record_call = mock_write_record.call_args[1]
        assert record_call['stream_name'] == "company"
        assert record_call['record']['name'] == "SpaceX"
        assert record_call['record']['founder'] == "Elon Musk"
        assert record_call['record']['founded'] == 2002
        assert record_call['record']['employees'] == 10000

def test_fetch_company_api_error():
    """Test API error handling"""
    with patch('requests.get') as mock_get:
        # Mock API error
        mock_get.side_effect = Exception("API Error")

        # Execute and verify error handling
        with pytest.raises(Exception):
            company_tap.fetch_company()

def test_fetch_company_schema_validation():
    """Test schema structure"""
    with patch('requests.get') as mock_get, \
        patch('singer.write_schema') as mock_write_schema:
        
        # Mock API response
        mock_response = MagicMock()
        mock_response.json.return_value = SAMPLE_COMPANY_DATA
        mock_get.return_value = mock_response

        # Execute fetch_company
        company_tap.fetch_company()

        # Verify schema structure
        schema_call = mock_write_schema.call_args[1]
        schema = schema_call['schema']
        
        # Check required fields
        assert 'id' in schema['properties']
        assert 'name' in schema['properties']
        assert 'founder' in schema['properties']
        assert 'founded' in schema['properties']
        assert 'employees' in schema['properties']
        assert 'headquarters' in schema['properties']
        assert 'links' in schema['properties']

        # Check data types
        assert schema['properties']['id']['type'] == ["string", "null"]
        assert schema['properties']['founded']['type'] == ["integer", "null"]
        assert schema['properties']['headquarters']['type'] == "object"
        assert schema['properties']['links']['type'] == "object"

def test_fetch_company_null_handling():
    """Test handling of null values in company data"""
    test_data = SAMPLE_COMPANY_DATA.copy()
    test_data['employees'] = None
    test_data['valuation'] = None

    with patch('requests.get') as mock_get, \
        patch('singer.write_schema') as mock_write_schema, \
        patch('singer.write_record') as mock_write_record:
        
        # Mock API response
        mock_response = MagicMock()
        mock_response.json.return_value = test_data
        mock_get.return_value = mock_response

        # Execute fetch_company
        company_tap.fetch_company()

        # Verify null values are handled correctly
        record_call = mock_write_record.call_args[1]
        assert record_call['record']['employees'] is None
        assert record_call['record']['valuation'] is None

def test_fetch_company_rate_limit_handling():
    """Test rate limit handling for company endpoint"""
    with patch('requests.get') as mock_get, \
        patch('time.sleep') as mock_sleep, \
        patch('singer.write_schema'), \
        patch('singer.write_record'):
        
        # Mock rate limit response followed by success
        rate_limit_response = MagicMock()
        rate_limit_response.status_code = 429
        rate_limit_response.headers = {'Retry-After': '2'}
        
        success_response = MagicMock()
        success_response.status_code = 200
        success_response.json.return_value = SAMPLE_COMPANY_DATA
        
        mock_get.side_effect = [rate_limit_response, success_response]
        
        # Execute fetch_company
        company_tap.fetch_company()
        
        # Verify retry behavior
        assert mock_get.call_count == 2
        assert mock_sleep.called
        mock_sleep.assert_called_once_with(2)

def test_fetch_company_malformed_response():
    """Test handling of malformed company data responses"""
    with patch('requests.get') as mock_get:
        # Test invalid JSON response
        invalid_json_response = MagicMock()
        invalid_json_response.status_code = 200
        invalid_json_response.json.side_effect = json.JSONDecodeError('Invalid JSON', '', 0)
        mock_get.return_value = invalid_json_response
        
        with pytest.raises(ValueError) as exc_info:
            company_tap.fetch_company()
            assert "Invalid JSON response" in str(exc_info.value)
        
        # Test missing required fields
        incomplete_response = MagicMock()
        incomplete_response.status_code = 200
        incomplete_response.json.return_value = {"name": "SpaceX"}  # Missing required 'id' field
        mock_get.return_value = incomplete_response
        
        with pytest.raises(ValueError) as exc_info:
            company_tap.fetch_company()
            assert "Missing required field" in str(exc_info.value)

def test_fetch_company_state_management():
    """Test state management for company endpoint"""
    with patch('requests.get') as mock_get, \
        patch('singer.write_schema'), \
        patch('singer.write_record'), \
        patch('singer.write_state') as mock_write_state:
        
        # Mock successful response
        mock_response = MagicMock()
        mock_response.json.return_value = SAMPLE_COMPANY_DATA
        mock_get.return_value = mock_response
        
        # Execute fetch_company
        company_tap.fetch_company()
        
        # Verify state was written with timestamp
        mock_write_state.assert_called_once()
        state_call = mock_write_state.call_args[1]
        assert 'company' in state_call['state']
        assert 'last_sync' in state_call['state']['company']
        
        # Verify timestamp format
        from datetime import datetime
        timestamp = state_call['state']['company']['last_sync']
        try:
            datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%SZ")
        except ValueError:
            pytest.fail("Invalid timestamp format in state")

def test_fetch_company_field_transformation():
    """Test transformation of company data fields"""
    test_data = SAMPLE_COMPANY_DATA.copy()
    test_data.update({
        "employees": "10000",  # String instead of integer
        "valuation": "74000000000",  # String instead of integer
        "headquarters": {
            "address": None,  # Test null nested field
            "city": "Hawthorne",
            "state": "California"
        }
    })

    with patch('requests.get') as mock_get, \
        patch('singer.write_schema'), \
        patch('singer.write_record') as mock_write_record:
        
        # Mock API response
        mock_response = MagicMock()
        mock_response.json.return_value = test_data
        mock_get.return_value = mock_response

        # Execute fetch_company
        company_tap.fetch_company()

        # Verify field transformations
        record_call = mock_write_record.call_args[1]
        record = record_call['record']
        
        # Check type conversions
        assert isinstance(record['employees'], int)
        assert isinstance(record['valuation'], int)
        
        # Check nested null handling
        headquarters = json.loads(record['headquarters'])
        assert headquarters['address'] is None
        assert headquarters['city'] == "Hawthorne"
