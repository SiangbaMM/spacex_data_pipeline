import pytest
from unittest.mock import patch, MagicMock
import json
from datetime import datetime
from include.fetch_dragons import DragonsTap

@pytest.fixture
def dragons_tap():
    return DragonsTap(base_url="https://api.spacexdata.com/v4/", config_path="config_snowflake.json")

# Sample API response data
SAMPLE_DRAGON_DATA = [
    {
        "id": "5e9d058759b1ff74a7ad5f8f",
        "name": "Dragon 1",
        "type": "capsule",
        "active": False,
        "crew_capacity": 0,
        "sidewall_angle_deg": 15,
        "orbit_duration_yr": 2,
        "dry_mass_kg": 4200,
        "dry_mass_lb": 9300,
        "first_flight": "2010-12-08",
        "heat_shield": {
            "material": "PICA-X",
            "size_meters": 3.6,
            "temp_degrees": 3000,
            "dev_partner": "NASA"
        },
        "thrusters": [
            {
                "type": "Draco",
                "amount": 18,
                "pods": 4,
                "fuel_1": "nitrogen tetroxide",
                "fuel_2": "monomethylhydrazine",
                "thrust": {
                    "kN": 0.4,
                    "lbf": 90
                }
            }
        ],
        "launch_payload_mass": {
            "kg": 6000,
            "lb": 13228
        },
        "launch_payload_vol": {
            "cubic_meters": 25,
            "cubic_feet": 883
        },
        "return_payload_mass": {
            "kg": 3000,
            "lb": 6614
        },
        "return_payload_vol": {
            "cubic_meters": 11,
            "cubic_feet": 388
        },
        "pressurized_capsule": {
            "payload_volume": {
                "cubic_meters": 11,
                "cubic_feet": 388
            }
        },
        "trunk": {
            "trunk_volume": {
                "cubic_meters": 14,
                "cubic_feet": 494
            },
            "cargo": {
                "solar_array": 2,
                "unpressurized_cargo": True
            }
        },
        "height_w_trunk": {
            "meters": 7.2,
            "feet": 23.6
        },
        "diameter": {
            "meters": 3.7,
            "feet": 12
        },
        "flickr_images": [
            "https://i.imgur.com/9fWdwNv.jpg",
            "https://i.imgur.com/NtY154T.jpg"
        ],
        "wikipedia": "https://en.wikipedia.org/wiki/SpaceX_Dragon",
        "description": "Dragon is a reusable spacecraft developed by SpaceX"
    }
]

def test_fetch_dragons_successful():
    """Test successful dragons data fetching and transformation"""
    with patch('requests.get') as mock_get, \
        patch('singer.write_schema') as mock_write_schema, \
        patch('singer.write_record') as mock_write_record, \
        patch('singer.write_state') as mock_write_state, \
        patch.object(dragons_tap, 'get_current_time') as mock_time:
        
        # Mock current time
        mock_time.return_value = datetime(2024, 1, 1, 12, 0, 0)
        
        # Mock API response
        mock_response = MagicMock()
        mock_response.json.return_value = SAMPLE_DRAGON_DATA
        mock_get.return_value = mock_response

        # Execute fetch_dragons
        dragons_tap.fetch_dragons()

        # Verify API call
        mock_get.assert_called_once_with("https://api.spacexdata.com/v4/dragons")

        # Verify schema write
        mock_write_schema.assert_called_once()
        schema_call = mock_write_schema.call_args[1]
        assert schema_call['stream_name'] == "DRAGONS"
        assert schema_call['key_properties'] == ["DRAGON_ID"]

        # Verify record write
        mock_write_record.assert_called_once()
        record_call = mock_write_record.call_args[1]
        record = record_call['record']
        
        # Verify basic fields
        assert record['DRAGON_ID'] == "5e9d058759b1ff74a7ad5f8f"
        assert record['NAME'] == "Dragon 1"
        assert record['TYPE'] == "capsule"
        assert record['ACTIVE'] is False
        assert record['CREW_CAPACITY'] == 0
        assert record['DRY_MASS_KG'] == 4200
        
        # Verify complex nested fields are properly JSON encoded
        heat_shield = json.loads(record['HEAT_SHIELD'])
        assert heat_shield['material'] == "PICA-X"
        assert heat_shield['size_meters'] == 3.6
        
        thrusters = json.loads(record['THRUSTERS'])
        assert len(thrusters) == 1
        assert thrusters[0]['type'] == "Draco"
        assert thrusters[0]['amount'] == 18
        
        trunk = json.loads(record['TRUNK'])
        assert trunk['trunk_volume']['cubic_meters'] == 14
        assert trunk['cargo']['solar_array'] == 2
        
        # Verify state write
        mock_write_state.assert_called_once()
        state_call = mock_write_state.call_args[1]
        assert 'DRAGONS' in state_call['state']
        assert 'last_sync' in state_call['state']['DRAGONS']

def test_fetch_dragons_api_error():
    """Test API error handling"""
    with patch('requests.get') as mock_get:
        # Mock API error
        mock_get.side_effect = Exception("API Error")

        # Execute and verify error handling
        with pytest.raises(Exception):
            dragons_tap.fetch_dragons()

def test_fetch_dragons_schema_validation():
    """Test schema structure"""
    with patch('requests.get') as mock_get, \
        patch('singer.write_schema') as mock_write_schema:
        
        # Mock API response
        mock_response = MagicMock()
        mock_response.json.return_value = SAMPLE_DRAGON_DATA
        mock_get.return_value = mock_response

        # Execute fetch_dragons
        dragons_tap.fetch_dragons()

        # Verify schema structure
        schema_call = mock_write_schema.call_args[1]
        schema = schema_call['schema']
        
        # Check required fields
        assert 'DRAGON_ID' in schema['properties']
        assert 'NAME' in schema['properties']
        assert 'TYPE' in schema['properties']
        assert 'ACTIVE' in schema['properties']
        assert 'CREW_CAPACITY' in schema['properties']
        assert 'HEAT_SHIELD' in schema['properties']
        assert 'THRUSTERS' in schema['properties']
        
        # Check data types
        assert schema['properties']['DRAGON_ID']['type'] == ["string", "null"]
        assert schema['properties']['CREW_CAPACITY']['type'] == ["integer", "null"]
        assert schema['properties']['SIDEWALL_ANGLE_DEG']['type'] == ["number", "null"]
        assert schema['properties']['FIRST_FLIGHT']['format'] == "date"

def test_fetch_dragons_empty_response():
    """Test handling of empty dragons data"""
    with patch('requests.get') as mock_get, \
        patch('singer.write_schema') as mock_write_schema, \
        patch('singer.write_record') as mock_write_record, \
        patch('singer.write_state') as mock_write_state:
        
        # Mock empty API response
        mock_response = MagicMock()
        mock_response.json.return_value = []
        mock_get.return_value = mock_response

        # Execute fetch_dragons
        dragons_tap.fetch_dragons()

        # Verify schema was still written
        mock_write_schema.assert_called_once()
        
        # Verify no records were written
        mock_write_record.assert_not_called()
        
        # Verify state was still written
        mock_write_state.assert_called_once()

def test_fetch_dragons_missing_nested_data():
    """Test handling of dragons with missing nested data"""
    incomplete_dragon_data = [{
        "id": "test_id",
        "name": "Test Dragon",
        "type": "capsule",
        # Missing nested objects
    }]

    with patch('requests.get') as mock_get, \
        patch('singer.write_schema') as mock_write_schema, \
        patch('singer.write_record') as mock_write_record:
        
        # Mock API response with incomplete data
        mock_response = MagicMock()
        mock_response.json.return_value = incomplete_dragon_data
        mock_get.return_value = mock_response

        # Execute fetch_dragons
        dragons_tap.fetch_dragons()

        # Verify record was written with empty JSON objects for missing nested data
        record_call = mock_write_record.call_args[1]
        record = record_call['record']
        assert record['DRAGON_ID'] == "test_id"
        assert record['NAME'] == "Test Dragon"
        assert record['TYPE'] == "capsule"
        assert json.loads(record['HEAT_SHIELD']) is None
        assert record['THRUSTERS'] == "[]"
        assert json.loads(record['LAUNCH_PAYLOAD_MASS']) is None
        assert json.loads(record['PRESSURIZED_CAPSULE']) is None
        assert json.loads(record['TRUNK']) is None

def test_fetch_dragons_complex_measurements():
    """Test handling of complex measurement data"""
    dragon_with_measurements = [{
        "id": "test_id",
        "name": "Test Dragon",
        "launch_payload_mass": {
            "kg": 6000,
            "lb": 13228
        },
        "height_w_trunk": {
            "meters": 7.2,
            "feet": 23.6
        },
        "pressurized_capsule": {
            "payload_volume": {
                "cubic_meters": 11,
                "cubic_feet": 388
            }
        }
    }]

    with patch('requests.get') as mock_get, \
        patch('singer.write_schema') as mock_write_schema, \
        patch('singer.write_record') as mock_write_record:
        
        # Mock API response
        mock_response = MagicMock()
        mock_response.json.return_value = dragon_with_measurements
        mock_get.return_value = mock_response

        # Execute fetch_dragons
        dragons_tap.fetch_dragons()

        # Verify measurement data handling
        record_call = mock_write_record.call_args[1]
        record = record_call['record']
        
        # Verify nested measurements are properly JSON encoded
        launch_payload = json.loads(record['LAUNCH_PAYLOAD_MASS'])
        assert launch_payload['kg'] == 6000
        assert launch_payload['lb'] == 13228
        
        height = json.loads(record['HEIGHT_W_TRUNK'])
        assert height['meters'] == 7.2
        assert height['feet'] == 23.6
        
        capsule = json.loads(record['PRESSURIZED_CAPSULE'])
        assert capsule['payload_volume']['cubic_meters'] == 11
        assert capsule['payload_volume']['cubic_feet'] == 388

def test_fetch_dragons_rate_limit_handling():
    """Test rate limit handling for dragons endpoint"""
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
        success_response.json.return_value = SAMPLE_DRAGON_DATA
        
        mock_get.side_effect = [rate_limit_response, success_response]
        
        # Execute fetch_dragons
        dragons_tap.fetch_dragons()
        
        # Verify retry behavior
        assert mock_get.call_count == 2
        assert mock_sleep.called
        mock_sleep.assert_called_once_with(2)

def test_fetch_dragons_malformed_response():
    """Test handling of malformed dragon data responses"""
    with patch('requests.get') as mock_get:
        # Test invalid JSON response
        invalid_json_response = MagicMock()
        invalid_json_response.status_code = 200
        invalid_json_response.json.side_effect = json.JSONDecodeError('Invalid JSON', '', 0)
        mock_get.return_value = invalid_json_response
        
        with pytest.raises(ValueError) as exc_info:
            dragons_tap.fetch_dragons()
        assert "Invalid JSON response" in str(exc_info.value)
        
        # Test missing required fields
        incomplete_response = MagicMock()
        incomplete_response.status_code = 200
        incomplete_response.json.return_value = [{"name": "Test Dragon"}]  # Missing required 'id' field
        mock_get.return_value = incomplete_response
        
        with pytest.raises(ValueError) as exc_info:
            dragons_tap.fetch_dragons()
        assert "Missing required field" in str(exc_info.value)

def test_fetch_dragons_state_management():
    """Test state management for dragons endpoint"""
    with patch('requests.get') as mock_get, \
        patch('singer.write_schema'), \
        patch('singer.write_record'), \
        patch('singer.write_state') as mock_write_state, \
        patch.object(dragons_tap, 'get_state') as mock_get_state, \
        patch.object(dragons_tap, 'get_current_time') as mock_time:
        
        # Mock current time
        mock_time.return_value = datetime(2024, 1, 1, 12, 0, 0)
        
        # Mock existing state with bookmark
        mock_get_state.return_value = {
            'bookmarks': {
                'dragons': {
                    'last_record': '2024-01-01T00:00:00Z'
                }
            }
        }
        
        # Mock API response
        mock_response = MagicMock()
        mock_response.json.return_value = SAMPLE_DRAGON_DATA
        mock_get.return_value = mock_response
        
        # Execute fetch_dragons
        dragons_tap.fetch_dragons()
        
        # Verify state was updated
        mock_write_state.assert_called()
        new_state = mock_write_state.call_args[1]['state']
        assert new_state['DRAGONS']['last_sync'] > '2024-01-01T00:00:00Z'

def test_fetch_dragons_multiple_versions():
    """Test handling of multiple dragon versions with different configurations"""
    multiple_dragons_data = [
        {
            "id": "dragon1",
            "name": "Dragon 1",
            "type": "cargo",
            "active": False,
            "crew_capacity": 0,
            "thrusters": [
                {
                    "type": "Draco",
                    "amount": 18
                }
            ]
        },
        {
            "id": "dragon2",
            "name": "Dragon 2",
            "type": "crew",
            "active": True,
            "crew_capacity": 7,
            "thrusters": [
                {
                    "type": "Draco",
                    "amount": 16
                },
                {
                    "type": "SuperDraco",
                    "amount": 8
                }
            ]
        }
    ]

    with patch('requests.get') as mock_get, \
        patch('singer.write_schema'), \
        patch('singer.write_record') as mock_write_record:
        
        # Mock API response
        mock_response = MagicMock()
        mock_response.json.return_value = multiple_dragons_data
        mock_get.return_value = mock_response

        # Execute fetch_dragons
        dragons_tap.fetch_dragons()

        # Verify all dragons were processed
        assert mock_write_record.call_count == 2

        # Verify each dragon's data
        records = [call[1]['record'] for call in mock_write_record.call_args_list]
        
        # Check Dragon 1
        assert records[0]['DRAGON_ID'] == "dragon1"
        assert records[0]['NAME'] == "Dragon 1"
        assert records[0]['TYPE'] == "cargo"
        assert records[0]['ACTIVE'] is False
        assert records[0]['CREW_CAPACITY'] == 0
        thrusters1 = json.loads(records[0]['THRUSTERS'])
        assert len(thrusters1) == 1
        assert thrusters1[0]['type'] == "Draco"
        assert thrusters1[0]['amount'] == 18

        # Check Dragon 2
        assert records[1]['DRAGON_ID'] == "dragon2"
        assert records[1]['NAME'] == "Dragon 2"
        assert records[1]['TYPE'] == "crew"
        assert records[1]['ACTIVE'] is True
        assert records[1]['CREW_CAPACITY'] == 7
        thrusters2 = json.loads(records[1]['THRUSTERS'])
        assert len(thrusters2) == 2
        assert thrusters2[0]['type'] == "Draco"
        assert thrusters2[1]['type'] == "SuperDraco"
