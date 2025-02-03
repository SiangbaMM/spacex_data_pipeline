import pytest
from unittest.mock import patch, MagicMock
import json
from datetime import datetime
from include.fetch_crew import CrewTap

@pytest.fixture
def crew_tap():
    return CrewTap(base_url="https://api.spacexdata.com/v4/", config_path="config_snowflake.json")

# Sample API response data
SAMPLE_CREW_DATA = [
    {
        "id": "5ebf1a6e23a9a60006e03a7a",
        "name": "Robert Behnken",
        "agency": "NASA",
        "image": "https://imgur.com/0smMgMH.png",
        "wikipedia": "https://en.wikipedia.org/wiki/Robert_L._Behnken",
        "launches": ["5eb87d46ffd86e000604b388"],
        "status": "active"
    },
    {
        "id": "5ebf1b7323a9a60006e03a7b",
        "name": "Douglas Hurley",
        "agency": "NASA",
        "image": "https://imgur.com/0smMgMH.png",
        "wikipedia": "https://en.wikipedia.org/wiki/Douglas_G._Hurley",
        "launches": ["5eb87d46ffd86e000604b388"],
        "status": "retired"
    }
]

def test_fetch_crew_successful():
    """Test successful crew data fetching and transformation"""
    with patch('requests.get') as mock_get, \
        patch('singer.write_schema') as mock_write_schema, \
        patch('singer.write_record') as mock_write_record, \
        patch('singer.write_state') as mock_write_state, \
        patch('tap_spacex.get_current_time') as mock_time:
        
        # Mock current time
        mock_time.return_value = datetime(2024, 1, 1, 12, 0, 0)
        
        # Mock API response
        mock_response = MagicMock()
        mock_response.json.return_value = SAMPLE_CREW_DATA
        mock_get.return_value = mock_response

        # Execute fetch_crew
        crew_tap.fetch_crew()

        # Verify API call
        mock_get.assert_called_once_with("https://api.spacexdata.com/v4/crew")

        # Verify schema write
        mock_write_schema.assert_called_once()
        schema_call = mock_write_schema.call_args[1]
        assert schema_call['stream_name'] == "CREW"
        assert schema_call['key_properties'] == ["CREW_ID"]

        # Verify record writes (should be called twice for two crew members)
        assert mock_write_record.call_count == 2
        
        # Verify first crew member record
        first_record = mock_write_record.call_args_list[0][1]
        assert first_record['stream_name'] == "CREW"
        assert first_record['record']['CREW_ID'] == "5ebf1a6e23a9a60006e03a7a"
        assert first_record['record']['NAME'] == "Robert Behnken"
        assert first_record['record']['AGENCY'] == "NASA"
        assert first_record['record']['STATUS'] == "active"
        assert json.loads(first_record['record']['LAUNCHES']) == ["5eb87d46ffd86e000604b388"]

        # Verify second crew member record
        second_record = mock_write_record.call_args_list[1][1]
        assert second_record['stream_name'] == "CREW"
        assert second_record['record']['CREW_ID'] == "5ebf1b7323a9a60006e03a7b"
        assert second_record['record']['NAME'] == "Douglas Hurley"
        assert second_record['record']['STATUS'] == "retired"

        # Verify state write
        mock_write_state.assert_called_once()
        state_call = mock_write_state.call_args[1]
        assert 'CREW' in state_call['state']
        assert 'last_sync' in state_call['state']['CREW']

def test_fetch_crew_api_error():
    """Test API error handling"""
    with patch('requests.get') as mock_get:
        # Mock API error
        mock_get.side_effect = Exception("API Error")

        # Execute and verify error handling
        with pytest.raises(Exception):
            crew_tap.fetch_crew()

def test_fetch_crew_schema_validation():
    """Test schema structure"""
    with patch('requests.get') as mock_get, \
        patch('singer.write_schema') as mock_write_schema:
        
        # Mock API response
        mock_response = MagicMock()
        mock_response.json.return_value = SAMPLE_CREW_DATA
        mock_get.return_value = mock_response

        # Execute fetch_crew
        crew_tap.fetch_crew()

        # Verify schema structure
        schema_call = mock_write_schema.call_args[1]
        schema = schema_call['schema']
        
        # Check required fields
        assert 'CREW_ID' in schema['properties']
        assert 'NAME' in schema['properties']
        assert 'AGENCY' in schema['properties']
        assert 'IMAGE' in schema['properties']
        assert 'WIKIPEDIA' in schema['properties']
        assert 'STATUS' in schema['properties']
        assert 'LAUNCHES' in schema['properties']

        # Check data types
        assert schema['properties']['CREW_ID']['type'] == ["string", "null"]
        assert schema['properties']['NAME']['type'] == ["string", "null"]
        assert schema['properties']['LAUNCHES']['type'] == ["string", "null"]
        assert schema['properties']['CREATED_AT']['format'] == "date-time"

def test_fetch_crew_empty_response():
    """Test handling of empty crew data"""
    with patch('requests.get') as mock_get, \
        patch('singer.write_schema') as mock_write_schema, \
        patch('singer.write_record') as mock_write_record, \
        patch('singer.write_state') as mock_write_state:
        
        # Mock empty API response
        mock_response = MagicMock()
        mock_response.json.return_value = []
        mock_get.return_value = mock_response

        # Execute fetch_crew
        crew_tap.fetch_crew()

        # Verify schema was still written
        mock_write_schema.assert_called_once()
        
        # Verify no records were written
        mock_write_record.assert_not_called()
        
        # Verify state was still written
        mock_write_state.assert_called_once()

def test_fetch_crew_missing_fields():
    """Test handling of crew data with missing fields"""
    incomplete_crew_data = [{
        "id": "test_id",
        "name": "Test Astronaut"
        # Missing other fields
    }]

    with patch('requests.get') as mock_get, \
        patch('singer.write_schema') as mock_write_schema, \
        patch('singer.write_record') as mock_write_record:
        
        # Mock API response with incomplete data
        mock_response = MagicMock()
        mock_response.json.return_value = incomplete_crew_data
        mock_get.return_value = mock_response

        # Execute fetch_crew
        crew_tap.fetch_crew()

        # Verify record was written with None for missing fields
        record_call = mock_write_record.call_args[1]
        assert record_call['record']['CREW_ID'] == "test_id"
        assert record_call['record']['NAME'] == "Test Astronaut"
        assert record_call['record']['AGENCY'] is None
        assert record_call['record']['IMAGE'] is None
        assert record_call['record']['WIKIPEDIA'] is None
        assert record_call['record']['STATUS'] is None
        assert record_call['record']['LAUNCHES'] == "[]"  # Empty array for missing launches

def test_fetch_crew_rate_limit_handling():
    """Test rate limit handling for crew endpoint"""
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
        success_response.json.return_value = SAMPLE_CREW_DATA
        
        mock_get.side_effect = [rate_limit_response, success_response]
        
        # Execute fetch_crew
        crew_tap.fetch_crew()
        
        # Verify retry behavior
        assert mock_get.call_count == 2
        assert mock_sleep.called
        mock_sleep.assert_called_once_with(2)

def test_fetch_crew_malformed_response():
    """Test handling of malformed crew data responses"""
    with patch('requests.get') as mock_get:
        # Test invalid JSON response
        invalid_json_response = MagicMock()
        invalid_json_response.status_code = 200
        invalid_json_response.json.side_effect = json.JSONDecodeError('Invalid JSON', '', 0)
        mock_get.return_value = invalid_json_response
        
        with pytest.raises(ValueError) as exc_info:
            crew_tap.fetch_crew()
            assert "Invalid JSON response" in str(exc_info.value)
        
        # Test missing required fields
        incomplete_response = MagicMock()
        incomplete_response.status_code = 200
        incomplete_response.json.return_value = [{"name": "Test Astronaut"}]  # Missing required 'id' field
        mock_get.return_value = incomplete_response
        
        with pytest.raises(ValueError) as exc_info:
            crew_tap.fetch_crew()
            assert "Missing required field" in str(exc_info.value)

def test_fetch_crew_state_management():
    """Test state management for crew endpoint"""
    with patch('requests.get') as mock_get, \
        patch('singer.write_schema'), \
        patch('singer.write_record'), \
        patch('singer.write_state') as mock_write_state, \
        patch.object(crew_tap, 'get_state') as mock_get_state, \
        patch.object(crew_tap, 'get_current_time') as mock_time:
        
        # Mock current time
        mock_time.return_value = datetime(2024, 1, 1, 12, 0, 0)
        
        # Mock existing state with bookmark
        mock_get_state.return_value = {
            'bookmarks': {
                'crew': {
                    'last_record': '2024-01-01T00:00:00Z'
                }
            }
        }
        
        # Mock API response
        mock_response = MagicMock()
        mock_response.json.return_value = SAMPLE_CREW_DATA
        mock_get.return_value = mock_response
        
        # Execute fetch_crew
        crew_tap.fetch_crew()
        
        # Verify state was updated
        mock_write_state.assert_called()
        new_state = mock_write_state.call_args[1]['state']
        assert new_state['CREW']['last_sync'] > '2024-01-01T00:00:00Z'

def test_fetch_crew_multiple_members():
    """Test handling of multiple crew members with different statuses"""
    multiple_crew_data = [
        {
            "id": "crew1",
            "name": "Active Astronaut",
            "agency": "NASA",
            "status": "active",
            "launches": ["launch1", "launch2"]
        },
        {
            "id": "crew2",
            "name": "Retired Astronaut",
            "agency": "ESA",
            "status": "retired",
            "launches": ["launch3"]
        },
        {
            "id": "crew3",
            "name": "Training Astronaut",
            "agency": "JAXA",
            "status": "training",
            "launches": []
        }
    ]

    with patch('requests.get') as mock_get, \
        patch('singer.write_schema'), \
        patch('singer.write_record') as mock_write_record:
        
        # Mock API response
        mock_response = MagicMock()
        mock_response.json.return_value = multiple_crew_data
        mock_get.return_value = mock_response

        # Execute fetch_crew
        crew_tap.fetch_crew()

        # Verify all crew members were processed
        assert mock_write_record.call_count == 3

        # Verify each crew member's data
        records = [call[1]['record'] for call in mock_write_record.call_args_list]
        
        # Check first crew member
        assert records[0]['CREW_ID'] == "crew1"
        assert records[0]['NAME'] == "Active Astronaut"
        assert records[0]['AGENCY'] == "NASA"
        assert records[0]['STATUS'] == "active"
        assert json.loads(records[0]['LAUNCHES']) == ["launch1", "launch2"]

        # Check second crew member
        assert records[1]['CREW_ID'] == "crew2"
        assert records[1]['NAME'] == "Retired Astronaut"
        assert records[1]['AGENCY'] == "ESA"
        assert records[1]['STATUS'] == "retired"
        assert json.loads(records[1]['LAUNCHES']) == ["launch3"]

        # Check third crew member
        assert records[2]['CREW_ID'] == "crew3"
        assert records[2]['NAME'] == "Training Astronaut"
        assert records[2]['AGENCY'] == "JAXA"
        assert records[2]['STATUS'] == "training"
        assert json.loads(records[2]['LAUNCHES']) == []
