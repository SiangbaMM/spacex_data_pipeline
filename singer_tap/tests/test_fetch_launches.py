import pytest
from unittest.mock import patch, MagicMock
import json
from datetime import datetime
from include.fetch_launches import LaunchesTap

@pytest.fixture
def launches_tap():
    return LaunchesTap(base_url="https://api.spacexdata.com/v4/", config_path="config_snowflake.json")

# Sample API response data
SAMPLE_LAUNCH_DATA = [
    {
        "id": "5eb87cd9ffd86e000604b32a",
        "flight_number": 1,
        "name": "FalconSat",
        "date_utc": "2006-03-24T22:30:00.000Z",
        "date_unix": 1143239400,
        "date_local": "2006-03-24T14:30:00-08:00",
        "date_precision": "hour",
        "static_fire_date_utc": "2006-03-17T00:00:00.000Z",
        "static_fire_date_unix": 1142553600,
        "net": False,
        "window": 0,
        "rocket": "5e9d0d95eda69955f709d1eb",
        "success": False,
        "failures": [
            {
                "time": 33,
                "altitude": None,
                "reason": "merlin engine failure"
            }
        ],
        "upcoming": False,
        "details": "Engine failure at 33 seconds and loss of vehicle",
        "fairings": {
            "reused": False,
            "recovery_attempt": False,
            "recovered": False,
            "ships": []
        },
        "crew": [],
        "ships": [],
        "capsules": [],
        "payloads": ["5eb0e4b5b6c3bb0006eeb1e1"],
        "launchpad": "5e9e4502f5090995de566f86",
        "cores": [
            {
                "core": "5e9e289df35918033d3b2623",
                "flight": 1,
                "gridfins": False,
                "legs": False,
                "reused": False,
                "landing_attempt": False,
                "landing_success": None,
                "landing_type": None,
                "landpad": None
            }
        ],
        "links": {
            "patch": {
                "small": "https://images2.imgbox.com/3c/0e/T8iJcSN3_o.png",
                "large": "https://images2.imgbox.com/40/e3/GypSkayF_o.png"
            },
            "reddit": {
                "campaign": None,
                "launch": None,
                "media": None,
                "recovery": None
            },
            "flickr": {
                "small": [],
                "original": []
            },
            "presskit": None,
            "webcast": "https://www.youtube.com/watch?v=0a_00nJ_Y88",
            "youtube_id": "0a_00nJ_Y88",
            "article": "https://www.space.com/2196-spacex-inaugural-falcon-1-rocket-lost-launch.html",
            "wikipedia": "https://en.wikipedia.org/wiki/DemoSat"
        },
        "auto_update": True,
        "launch_library_id": None
    }
]

def test_fetch_launches_successful():
    """Test successful launches data fetching and transformation"""
    with patch('requests.get') as mock_get, \
        patch('singer.write_schema') as mock_write_schema, \
        patch('singer.write_record') as mock_write_record, \
        patch('singer.write_state') as mock_write_state, \
        patch.object(launches_tap, 'get_current_time') as mock_time:
        
        # Mock current time
        mock_time.return_value = datetime(2024, 1, 1, 12, 0, 0)
        
        # Mock API response
        mock_response = MagicMock()
        mock_response.json.return_value = SAMPLE_LAUNCH_DATA
        mock_get.return_value = mock_response

        # Execute fetch_launches
        launches_tap.fetch_launches()

        # Verify API call
        mock_get.assert_called_once_with("https://api.spacexdata.com/v4/launches")

        # Verify schema write
        mock_write_schema.assert_called_once()
        schema_call = mock_write_schema.call_args[1]
        assert schema_call['stream_name'] == "LAUNCHES"
        assert schema_call['key_properties'] == ["LAUNCH_ID"]

        # Verify record write
        mock_write_record.assert_called_once()
        record_call = mock_write_record.call_args[1]
        record = record_call['record']
        
        # Verify basic fields
        assert record['LAUNCH_ID'] == "5eb87cd9ffd86e000604b32a"
        assert record['FLIGHT_NUMBER'] == 1
        assert record['NAME'] == "FalconSat"
        assert record['DATE_UTC'] == "2006-03-24T22:30:00.000Z"
        assert record['SUCCESS'] is False
        
        # Verify complex fields are properly JSON encoded
        assert json.loads(record['FAILURES'])[0]['reason'] == "merlin engine failure"
        assert json.loads(record['CORES'])[0]['flight'] == 1
        assert json.loads(record['PAYLOADS']) == ["5eb0e4b5b6c3bb0006eeb1e1"]
        
        # Verify state write
        mock_write_state.assert_called_once()
        state_call = mock_write_state.call_args[1]
        assert 'LAUNCHES' in state_call['state']
        assert 'last_sync' in state_call['state']['LAUNCHES']

def test_fetch_launches_api_error():
    """Test API error handling"""
    with patch('requests.get') as mock_get:
        # Mock API error
        mock_get.side_effect = Exception("API Error")

        # Execute and verify error handling
        with pytest.raises(Exception):
            launches_tap.fetch_launches()

def test_fetch_launches_schema_validation():
    """Test schema structure"""
    with patch('requests.get') as mock_get, \
        patch('singer.write_schema') as mock_write_schema:
        
        # Mock API response
        mock_response = MagicMock()
        mock_response.json.return_value = SAMPLE_LAUNCH_DATA
        mock_get.return_value = mock_response

        # Execute fetch_launches
        launches_tap.fetch_launches()

        # Verify schema structure
        schema_call = mock_write_schema.call_args[1]
        schema = schema_call['schema']
        
        # Check required fields
        assert 'LAUNCH_ID' in schema['properties']
        assert 'FLIGHT_NUMBER' in schema['properties']
        assert 'NAME' in schema['properties']
        assert 'DATE_UTC' in schema['properties']
        assert 'ROCKET' in schema['properties']
        assert 'SUCCESS' in schema['properties']
        assert 'FAILURES' in schema['properties']
        assert 'CORES' in schema['properties']
        assert 'LINKS' in schema['properties']
        
        # Check data types
        assert schema['properties']['LAUNCH_ID']['type'] == ["string", "null"]
        assert schema['properties']['FLIGHT_NUMBER']['type'] == ["integer", "null"]
        assert schema['properties']['SUCCESS']['type'] == ["boolean", "null"]
        assert schema['properties']['DATE_UTC']['format'] == "date-time"

def test_fetch_launches_empty_response():
    """Test handling of empty launches data"""
    with patch('requests.get') as mock_get, \
        patch('singer.write_schema') as mock_write_schema, \
        patch('singer.write_record') as mock_write_record, \
        patch('singer.write_state') as mock_write_state:
        
        # Mock empty API response
        mock_response = MagicMock()
        mock_response.json.return_value = []
        mock_get.return_value = mock_response

        # Execute fetch_launches
        launches_tap.fetch_launches()

        # Verify schema was still written
        mock_write_schema.assert_called_once()
        
        # Verify no records were written
        mock_write_record.assert_not_called()
        
        # Verify state was still written
        mock_write_state.assert_called_once()

def test_fetch_launches_missing_nested_data():
    """Test handling of launches with missing nested data"""
    incomplete_launch_data = [{
        "id": "test_id",
        "flight_number": 1,
        "name": "Test Launch",
        # Missing nested objects
    }]

    with patch('requests.get') as mock_get, \
        patch('singer.write_schema') as mock_write_schema, \
        patch('singer.write_record') as mock_write_record:
        
        # Mock API response with incomplete data
        mock_response = MagicMock()
        mock_response.json.return_value = incomplete_launch_data
        mock_get.return_value = mock_response

        # Execute fetch_launches
        launches_tap.fetch_launches()

        # Verify record was written with empty JSON arrays/objects for missing nested data
        record_call = mock_write_record.call_args[1]
        record = record_call['record']
        assert record['LAUNCH_ID'] == "test_id"
        assert record['FLIGHT_NUMBER'] == 1
        assert record['NAME'] == "Test Launch"
        assert record['FAILURES'] == "[]"
        assert record['CORES'] == "[]"
        assert record['CREW'] == "[]"
        assert record['SHIPS'] == "[]"
        assert record['CAPSULES'] == "[]"
        assert record['PAYLOADS'] == "[]"
        assert json.loads(record['LINKS']) == {}
        assert json.loads(record['FAIRINGS']) == {}

def test_fetch_launches_date_handling():
    """Test handling of various date formats in launch data"""
    launch_with_dates = [{
        "id": "test_id",
        "date_utc": "2024-01-01T00:00:00.000Z",
        "date_unix": 1704067200,
        "date_local": "2024-01-01T01:00:00+01:00",
        "static_fire_date_utc": None,  # Test null date
        "static_fire_date_unix": None
    }]

    with patch('requests.get') as mock_get, \
        patch('singer.write_schema') as mock_write_schema, \
        patch('singer.write_record') as mock_write_record:
        
        # Mock API response
        mock_response = MagicMock()
        mock_response.json.return_value = launch_with_dates
        mock_get.return_value = mock_response

        # Execute fetch_launches
        launches_tap.fetch_launches()

        # Verify date handling
        record_call = mock_write_record.call_args[1]
        record = record_call['record']
        assert record['DATE_UTC'] == "2024-01-01T00:00:00.000Z"
        assert record['DATE_UNIX'] == 1704067200
        assert record['DATE_LOCAL'] == "2024-01-01T01:00:00+01:00"
        assert record['STATIC_FIRE_DATE_UTC'] is None
        assert record['STATIC_FIRE_DATE_UNIX'] is None

def test_fetch_launches_rate_limit_handling():
    """Test rate limit handling for launches endpoint"""
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
        success_response.json.return_value = SAMPLE_LAUNCH_DATA
        
        mock_get.side_effect = [rate_limit_response, success_response]
        
        # Execute fetch_launches
        launches_tap.fetch_launches()
        
        # Verify retry behavior
        assert mock_get.call_count == 2
        assert mock_sleep.called
        mock_sleep.assert_called_once_with(2)

def test_fetch_launches_pagination():
    """Test handling of paginated launch data"""
    with patch('requests.get') as mock_get, \
        patch('singer.write_schema'), \
        patch('singer.write_record') as mock_write_record:
        
        # Mock paginated responses
        first_page = MagicMock()
        first_page.status_code = 200
        first_page.json.return_value = {
            'docs': [SAMPLE_LAUNCH_DATA[0]],
            'totalDocs': 2,
            'offset': 0,
            'limit': 1,
            'totalPages': 2,
            'page': 1,
            'pagingCounter': 1,
            'hasPrevPage': False,
            'hasNextPage': True,
            'prevPage': None,
            'nextPage': 2
        }
        
        second_page = MagicMock()
        second_page.status_code = 200
        second_page.json.return_value = {
            'docs': [{
                **SAMPLE_LAUNCH_DATA[0],
                'id': 'second_launch',
                'flight_number': 2
            }],
            'totalDocs': 2,
            'offset': 1,
            'limit': 1,
            'totalPages': 2,
            'page': 2,
            'pagingCounter': 2,
            'hasPrevPage': True,
            'hasNextPage': False,
            'prevPage': 1,
            'nextPage': None
        }
        
        mock_get.side_effect = [first_page, second_page]
        
        # Execute fetch_launches
        launches_tap.fetch_launches()
        
        # Verify both pages were processed
        assert mock_get.call_count == 2
        assert mock_write_record.call_count == 2
        
        # Verify records from both pages
        first_record = mock_write_record.call_args_list[0][1]['record']
        second_record = mock_write_record.call_args_list[1][1]['record']
        assert first_record['FLIGHT_NUMBER'] == 1
        assert second_record['FLIGHT_NUMBER'] == 2

def test_fetch_launches_incremental_sync():
    """Test incremental sync using bookmarks"""
    with patch('requests.get') as mock_get, \
        patch('singer.write_schema'), \
        patch('singer.write_record'), \
        patch('singer.write_state') as mock_write_state, \
        patch.object(launches_tap, 'get_state') as mock_get_state:
        
        # Mock existing state with bookmark
        mock_get_state.return_value = {
            'bookmarks': {
                'launches': {
                    'last_record': '2024-01-01T00:00:00Z'
                }
            }
        }
        
        # Mock API response
        mock_response = MagicMock()
        mock_response.json.return_value = SAMPLE_LAUNCH_DATA
        mock_get.return_value = mock_response
        
        # Execute fetch_launches
        launches_tap.fetch_launches()
        
        # Verify bookmark was used in API request
        query_params = mock_get.call_args[1].get('params', {})
        assert 'date_utc' in query_params
        assert query_params['date_utc']['$gt'] == '2024-01-01T00:00:00Z'
        
        # Verify state was updated
        mock_write_state.assert_called()
        new_state = mock_write_state.call_args[1]['state']
        assert new_state['bookmarks']['launches']['last_record'] > '2024-01-01T00:00:00Z'
