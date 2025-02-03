import pytest
from unittest.mock import patch, MagicMock
import json
from datetime import datetime
from include.fetch_rockets import RocketsTap

@pytest.fixture
def rockets_tap():
    return RocketsTap(base_url="https://api.spacexdata.com/v4/", config_path="config_snowflake.json")

# Sample API response data
SAMPLE_ROCKET_DATA = [
    {
        "id": "5e9d0d95eda69955f709d1eb",
        "name": "Falcon 1",
        "type": "rocket",
        "active": False,
        "stages": 2,
        "boosters": 0,
        "cost_per_launch": 6700000,
        "success_rate_pct": 40,
        "first_flight": "2006-03-24",
        "country": "Republic of the Marshall Islands",
        "company": "SpaceX",
        "height": {
            "meters": 22.25,
            "feet": 73
        },
        "diameter": {
            "meters": 1.68,
            "feet": 5.5
        },
        "mass": {
            "kg": 30146,
            "lb": 66460
        },
        "payload_weights": [
            {
                "id": "leo",
                "name": "Low Earth Orbit",
                "kg": 450,
                "lb": 992
            }
        ],
        "first_stage": {
            "reusable": False,
            "engines": 1,
            "fuel_amount_tons": 44.3,
            "burn_time_sec": 169,
            "thrust_sea_level": {
                "kN": 420,
                "lbf": 94000
            },
            "thrust_vacuum": {
                "kN": 480,
                "lbf": 108000
            }
        },
        "second_stage": {
            "reusable": False,
            "engines": 1,
            "fuel_amount_tons": 3.38,
            "burn_time_sec": 378,
            "thrust": {
                "kN": 31,
                "lbf": 7000
            }
        },
        "engines": {
            "number": 1,
            "type": "merlin",
            "version": "1C",
            "layout": "single",
            "engine_loss_max": 0,
            "propellant_1": "liquid oxygen",
            "propellant_2": "RP-1 kerosene",
            "thrust_sea_level": {
                "kN": 420,
                "lbf": 94000
            },
            "thrust_vacuum": {
                "kN": 480,
                "lbf": 108000
            }
        },
        "landing_legs": {
            "number": 0,
            "material": None
        },
        "flickr_images": [
            "https://imgur.com/DaCfMsj.jpg",
            "https://imgur.com/azYafd8.jpg"
        ],
        "wikipedia": "https://en.wikipedia.org/wiki/Falcon_1",
        "description": "The Falcon 1 was an expendable launch system privately developed and manufactured by SpaceX."
    }
]

def test_fetch_rockets_successful():
    """Test successful rockets data fetching and transformation"""
    with patch('requests.get') as mock_get, \
        patch('singer.write_schema') as mock_write_schema, \
        patch('singer.write_record') as mock_write_record, \
        patch('singer.write_state') as mock_write_state, \
        patch.object(rockets_tap, 'get_current_time') as mock_time:
        
        # Mock current time
        mock_time.return_value = datetime(2024, 1, 1, 12, 0, 0)
        
        # Mock API response
        mock_response = MagicMock()
        mock_response.json.return_value = SAMPLE_ROCKET_DATA
        mock_get.return_value = mock_response

        # Execute fetch_rockets
        rockets_tap.fetch_rockets()

        # Verify API call
        mock_get.assert_called_once_with("https://api.spacexdata.com/v4/rockets")

        # Verify schema write
        mock_write_schema.assert_called_once()
        schema_call = mock_write_schema.call_args[1]
        assert schema_call['stream_name'] == "ROCKETS"
        assert schema_call['key_properties'] == ["ROCKET_ID"]

        # Verify record write
        mock_write_record.assert_called_once()
        record_call = mock_write_record.call_args[1]
        record = record_call['record']
        
        # Verify basic fields
        assert record['ROCKET_ID'] == "5e9d0d95eda69955f709d1eb"
        assert record['NAME'] == "Falcon 1"
        assert record['TYPE'] == "rocket"
        assert record['ACTIVE'] is False
        assert record['STAGES'] == 2
        assert record['SUCCESS_RATE_PCT'] == 40
        
        # Verify nested numeric fields
        assert record['HEIGHT_METERS'] == 22.25
        assert record['DIAMETER_METERS'] == 1.68
        assert record['MASS_KG'] == 30146
        
        # Verify complex fields are properly JSON encoded
        payload_weights = json.loads(record['PAYLOAD_WEIGHTS'])
        assert payload_weights[0]['name'] == "Low Earth Orbit"
        assert payload_weights[0]['kg'] == 450
        
        first_stage = json.loads(record['FIRST_STAGE'])
        assert first_stage['engines'] == 1
        assert first_stage['fuel_amount_tons'] == 44.3
        
        engines = json.loads(record['ENGINES'])
        assert engines['type'] == "merlin"
        assert engines['version'] == "1C"
        
        # Verify state write
        mock_write_state.assert_called_once()
        state_call = mock_write_state.call_args[1]
        assert 'ROCKETS' in state_call['state']
        assert 'last_sync' in state_call['state']['ROCKETS']

def test_fetch_rockets_api_error():
    """Test API error handling"""
    with patch('requests.get') as mock_get:
        # Mock API error
        mock_get.side_effect = Exception("API Error")

        # Execute and verify error handling
        with pytest.raises(Exception):
            rockets_tap.fetch_rockets()

def test_fetch_rockets_schema_validation():
    """Test schema structure"""
    with patch('requests.get') as mock_get, \
        patch('singer.write_schema') as mock_write_schema:
        
        # Mock API response
        mock_response = MagicMock()
        mock_response.json.return_value = SAMPLE_ROCKET_DATA
        mock_get.return_value = mock_response

        # Execute fetch_rockets
        rockets_tap.fetch_rockets()

        # Verify schema structure
        schema_call = mock_write_schema.call_args[1]
        schema = schema_call['schema']
        
        # Check required fields
        assert 'ROCKET_ID' in schema['properties']
        assert 'NAME' in schema['properties']
        assert 'TYPE' in schema['properties']
        assert 'ACTIVE' in schema['properties']
        assert 'STAGES' in schema['properties']
        assert 'COST_PER_LAUNCH' in schema['properties']
        assert 'FIRST_STAGE' in schema['properties']
        assert 'SECOND_STAGE' in schema['properties']
        assert 'ENGINES' in schema['properties']
        
        # Check data types
        assert schema['properties']['ROCKET_ID']['type'] == ["string", "null"]
        assert schema['properties']['STAGES']['type'] == ["integer", "null"]
        assert schema['properties']['ACTIVE']['type'] == ["boolean", "null"]
        assert schema['properties']['HEIGHT_METERS']['type'] == ["number", "null"]

def test_fetch_rockets_empty_response():
    """Test handling of empty rockets data"""
    with patch('requests.get') as mock_get, \
        patch('singer.write_schema') as mock_write_schema, \
        patch('singer.write_record') as mock_write_record, \
        patch('singer.write_state') as mock_write_state:
        
        # Mock empty API response
        mock_response = MagicMock()
        mock_response.json.return_value = []
        mock_get.return_value = mock_response

        # Execute fetch_rockets
        rockets_tap.fetch_rockets()

        # Verify schema was still written
        mock_write_schema.assert_called_once()
        
        # Verify no records were written
        mock_write_record.assert_not_called()
        
        # Verify state was still written
        mock_write_state.assert_called_once()

def test_fetch_rockets_missing_nested_data():
    """Test handling of rockets with missing nested data"""
    incomplete_rocket_data = [{
        "id": "test_id",
        "name": "Test Rocket",
        "type": "rocket",
        # Missing nested objects
    }]

    with patch('requests.get') as mock_get, \
        patch('singer.write_schema') as mock_write_schema, \
        patch('singer.write_record') as mock_write_record:
        
        # Mock API response with incomplete data
        mock_response = MagicMock()
        mock_response.json.return_value = incomplete_rocket_data
        mock_get.return_value = mock_response

        # Execute fetch_rockets
        rockets_tap.fetch_rockets()

        # Verify record was written with None for missing nested data
        record_call = mock_write_record.call_args[1]
        record = record_call['record']
        assert record['ROCKET_ID'] == "test_id"
        assert record['NAME'] == "Test Rocket"
        assert record['TYPE'] == "rocket"
        assert record['HEIGHT_METERS'] is None
        assert record['DIAMETER_METERS'] is None
        assert record['MASS_KG'] is None
        assert record['PAYLOAD_WEIGHTS'] == "[]"
        assert json.loads(record['FIRST_STAGE']) == {}
        assert json.loads(record['SECOND_STAGE']) == {}
        assert json.loads(record['ENGINES']) == {}
        assert json.loads(record['LANDING_LEGS']) == {}

def test_fetch_rockets_numeric_handling():
    """Test handling of numeric values in rocket data"""
    rocket_with_numbers = [{
        "id": "test_id",
        "name": "Test Rocket",
        "height": {
            "meters": 0,  # Test zero
            "feet": None  # Test null
        },
        "mass": {
            "kg": 1000.5,  # Test decimal
            "lb": "invalid"  # Test invalid number
        }
    }]

    with patch('requests.get') as mock_get, \
        patch('singer.write_schema') as mock_write_schema, \
        patch('singer.write_record') as mock_write_record:
        
        # Mock API response
        mock_response = MagicMock()
        mock_response.json.return_value = rocket_with_numbers
        mock_get.return_value = mock_response

        # Execute fetch_rockets
        rockets_tap.fetch_rockets()

        # Verify numeric handling
        record_call = mock_write_record.call_args[1]
        record = record_call['record']
        assert record['HEIGHT_METERS'] == 0
        assert record['HEIGHT_FEET'] is None
        assert record['MASS_KG'] == 1000.5
        assert record['MASS_LBS'] is None  # Invalid number should be None

def test_fetch_rockets_rate_limit_handling():
    """Test rate limit handling for rockets endpoint"""
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
        success_response.json.return_value = SAMPLE_ROCKET_DATA
        
        mock_get.side_effect = [rate_limit_response, success_response]
        
        # Execute fetch_rockets
        rockets_tap.fetch_rockets()
        
        # Verify retry behavior
        assert mock_get.call_count == 2
        assert mock_sleep.called
        mock_sleep.assert_called_once_with(2)

def test_fetch_rockets_engine_data():
    """Test handling of detailed engine data"""
    rocket_with_engines = [{
        "id": "test_id",
        "engines": {
            "number": 9,
            "type": "merlin",
            "version": "1D+",
            "layout": "octaweb",
            "engine_loss_max": 2,
            "propellant_1": "liquid oxygen",
            "propellant_2": "RP-1 kerosene",
            "thrust_sea_level": {
                "kN": 7607,
                "lbf": 1710000
            },
            "thrust_vacuum": {
                "kN": 8227,
                "lbf": 1849500
            },
            "thrust_to_weight": 180.1
        }
    }]

    with patch('requests.get') as mock_get, \
        patch('singer.write_schema'), \
        patch('singer.write_record') as mock_write_record:
        
        # Mock API response
        mock_response = MagicMock()
        mock_response.json.return_value = rocket_with_engines
        mock_get.return_value = mock_response

        # Execute fetch_rockets
        rockets_tap.fetch_rockets()

        # Verify engine data handling
        record_call = mock_write_record.call_args[1]
        record = record_call['record']
        engines = json.loads(record['ENGINES'])
        assert engines['number'] == 9
        assert engines['type'] == "merlin"
        assert engines['version'] == "1D+"
        assert engines['layout'] == "octaweb"
        assert engines['engine_loss_max'] == 2
        assert engines['thrust_sea_level']['kN'] == 7607
        assert engines['thrust_vacuum']['kN'] == 8227
        assert engines['thrust_to_weight'] == 180.1

def test_fetch_rockets_state_management():
    """Test state management for rockets endpoint"""
    with patch('requests.get') as mock_get, \
        patch('singer.write_schema'), \
        patch('singer.write_record'), \
        patch('singer.write_state') as mock_write_state, \
        patch.object(rockets_tap, 'get_state') as mock_get_state:
        
        # Mock existing state with bookmark
        mock_get_state.return_value = {
            'bookmarks': {
                'rockets': {
                    'last_record': '2024-01-01T00:00:00Z'
                }
            }
        }
        
        # Mock API response
        mock_response = MagicMock()
        mock_response.json.return_value = SAMPLE_ROCKET_DATA
        mock_get.return_value = mock_response
        
        # Execute fetch_rockets
        rockets_tap.fetch_rockets()
        
        # Verify state was updated
        mock_write_state.assert_called()
        new_state = mock_write_state.call_args[1]['state']
        assert new_state['bookmarks']['rockets']['last_record'] > '2024-01-01T00:00:00Z'

def test_fetch_rockets_malformed_response():
    """Test handling of malformed rocket data responses"""
    with patch('requests.get') as mock_get:
        # Test invalid JSON response
        invalid_json_response = MagicMock()
        invalid_json_response.status_code = 200
        invalid_json_response.json.side_effect = json.JSONDecodeError('Invalid JSON', '', 0)
        mock_get.return_value = invalid_json_response
        
        with pytest.raises(ValueError) as exc_info:
            rockets_tap.fetch_rockets()
            assert "Invalid JSON response" in str(exc_info.value)
        
        # Test missing required fields
        incomplete_response = MagicMock()
        incomplete_response.status_code = 200
        incomplete_response.json.return_value = [{"name": "Test Rocket"}]  # Missing required 'id' field
        mock_get.return_value = incomplete_response
        
        with pytest.raises(ValueError) as exc_info:
            rockets_tap.fetch_rockets()
            assert "Missing required field" in str(exc_info.value)
