import json
from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest  # type: ignore
from include.fetch_cores import CoresTap

# Sample API response data
SAMPLE_CORE_DATA = [
    {
        "id": "5e9e289df35918033d3b2623",
        "serial": "B1001",
        "block": 1,
        "status": "retired",
        "reuse_count": 1,
        "rtls_attempts": 0,
        "rtls_landings": 0,
        "asds_attempts": 1,
        "asds_landings": 0,
        "last_update": "Engine failure at T+33 seconds and loss of vehicle",
        "launches": ["5eb87cd9ffd86e000604b32a"],
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
                "landpad": None,
            }
        ],
    }
]


@pytest.fixture
def cores_tap():
    """Instanciate cores_tap

    Args
        base_url (str) : The root url of v4 SpaceX API
        config_path (str) : Config file that contains database credentials

    Returns:
        CoresTap: Class instance
    """
    return CoresTap(
        base_url="https://api.spacexdata.com/v4/", config_path="config_snowflake.json"
    )


@pytest.fixture
def mock_current_time():
    """Mock datetime(2024, 1, 1, 12, 0, 0) as a current time"""
    return datetime(2024, 1, 1, 12, 0, 0)


def test_fetch_cores_successful(cores_tap, mock_current_time):
    """Test successful cores data fetching and transformation"""
    with (
        patch("requests.get") as mock_get,
        patch("singer.write_schema") as mock_write_schema,
        patch("singer.write_record") as mock_write_record,
        patch("singer.write_state") as mock_write_state,
        patch.object(cores_tap, "get_current_time", return_value=mock_current_time),
    ):
        # Mock API response
        mock_response = MagicMock()
        mock_response.json.return_value = SAMPLE_CORE_DATA
        mock_get.return_value = mock_response

        # Execute fetch_cores
        cores_tap.fetch_cores()

        # Verify API call
        mock_get.assert_called_once_with("https://api.spacexdata.com/v4/cores")

        # Verify schema write
        mock_write_schema.assert_called_once()
        schema_call = mock_write_schema.call_args[1]
        assert schema_call["stream_name"] == "CORES"
        assert schema_call["key_properties"] == ["CORE_ID"]

        # Verify record write
        mock_write_record.assert_called_once()
        record_call = mock_write_record.call_args[1]
        record = record_call["record"]

        # Verify basic fields
        assert record["CORE_ID"] == "5e9e289df35918033d3b2623"
        assert record["SERIAL"] == "B1001"
        assert record["BLOCK"] == 1
        assert record["STATUS"] == "retired"
        assert record["REUSE_COUNT"] == 1

        # Verify landing statistics
        assert record["RTLS_ATTEMPTS"] == 0
        assert record["RTLS_LANDINGS"] == 0
        assert record["ASDS_ATTEMPTS"] == 1
        assert record["ASDS_LANDINGS"] == 0

        # Verify array fields are properly JSON encoded
        assert json.loads(record["LAUNCHES"]) == ["5eb87cd9ffd86e000604b32a"]

        # Verify state write
        mock_write_state.assert_called_once()
        state_call = mock_write_state.call_args[1]
        assert "CORES" in state_call["state"]
        assert "last_sync" in state_call["state"]["CORES"]


def test_fetch_cores_api_error(cores_tap):
    """Test API error handling"""
    with patch("requests.get") as mock_get:
        # Mock API error
        mock_get.side_effect = Exception("API Error")

        # Execute and verify error handling
        with pytest.raises(Exception):
            cores_tap.fetch_cores()


def test_fetch_cores_schema_validation(cores_tap):
    """Test schema structure"""
    with (
        patch("requests.get") as mock_get,
        patch("singer.write_schema") as mock_write_schema,
    ):
        # Mock API response
        mock_response = MagicMock()
        mock_response.json.return_value = SAMPLE_CORE_DATA
        mock_get.return_value = mock_response

        # Execute fetch_cores
        cores_tap.fetch_cores()

        # Verify schema structure
        schema_call = mock_write_schema.call_args[1]
        schema = schema_call["schema"]

        # Check required fields
        assert "CORE_ID" in schema["properties"]
        assert "SERIAL" in schema["properties"]
        assert "BLOCK" in schema["properties"]
        assert "STATUS" in schema["properties"]
        assert "REUSE_COUNT" in schema["properties"]
        assert "RTLS_ATTEMPTS" in schema["properties"]
        assert "RTLS_LANDINGS" in schema["properties"]
        assert "ASDS_ATTEMPTS" in schema["properties"]
        assert "ASDS_LANDINGS" in schema["properties"]
        assert "LAUNCHES" in schema["properties"]

        # Check data types
        assert schema["properties"]["CORE_ID"]["type"] == ["string", "null"]
        assert schema["properties"]["BLOCK"]["type"] == ["integer", "null"]
        assert schema["properties"]["REUSE_COUNT"]["type"] == ["integer", "null"]
        assert schema["properties"]["LAUNCHES"]["type"] == ["string", "null"]


def test_fetch_cores_empty_response(cores_tap):
    """Test handling of empty cores data"""
    with (
        patch("requests.get") as mock_get,
        patch("singer.write_schema") as mock_write_schema,
        patch("singer.write_record") as mock_write_record,
        patch("singer.write_state") as mock_write_state,
    ):
        # Mock empty API response
        mock_response = MagicMock()
        mock_response.json.return_value = []
        mock_get.return_value = mock_response

        # Execute fetch_cores
        cores_tap.fetch_cores()

        # Verify schema was still written
        mock_write_schema.assert_called_once()

        # Verify no records were written
        mock_write_record.assert_not_called()

        # Verify state was still written
        mock_write_state.assert_called_once()


def test_fetch_cores_missing_fields(cores_tap):
    """Test handling of cores with missing fields"""
    incomplete_core_data = [
        {
            "id": "test_id",
            "serial": "B1001",
            # Missing most fields
        }
    ]

    with (
        patch("requests.get") as mock_get,
        # patch("singer.write_schema") as mock_write_schema,
        patch("singer.write_record") as mock_write_record,
    ):
        # Mock API response with incomplete data
        mock_response = MagicMock()
        mock_response.json.return_value = incomplete_core_data
        mock_get.return_value = mock_response

        # Execute fetch_cores
        cores_tap.fetch_cores()

        # Verify record was written with None for missing fields
        record_call = mock_write_record.call_args[1]
        record = record_call["record"]
        assert record["CORE_ID"] == "test_id"
        assert record["SERIAL"] == "B1001"
        assert record["BLOCK"] is None
        assert record["STATUS"] is None
        assert record["REUSE_COUNT"] is None
        assert record["RTLS_ATTEMPTS"] is None
        assert record["RTLS_LANDINGS"] is None
        assert record["ASDS_ATTEMPTS"] is None
        assert record["ASDS_LANDINGS"] is None
        assert record["LAUNCHES"] == "[]"


def test_fetch_cores_landing_statistics(cores_tap):
    """Test handling of core landing statistics"""
    core_with_landings = [
        {
            "id": "test_id",
            "serial": "B1001",
            "reuse_count": 5,
            "rtls_attempts": 3,
            "rtls_landings": 3,
            "asds_attempts": 2,
            "asds_landings": 2,
            "last_update": "Successfully landed on ASDS",
        }
    ]

    with (
        patch("requests.get") as mock_get,
        # patch("singer.write_schema") as mock_write_schema,
        patch("singer.write_record") as mock_write_record,
    ):
        # Mock API response
        mock_response = MagicMock()
        mock_response.json.return_value = core_with_landings
        mock_get.return_value = mock_response

        # Execute fetch_cores
        cores_tap.fetch_cores()

        # Verify landing statistics handling
        record_call = mock_write_record.call_args[1]
        record = record_call["record"]
        assert record["REUSE_COUNT"] == 5
        assert record["RTLS_ATTEMPTS"] == 3
        assert record["RTLS_LANDINGS"] == 3
        assert record["ASDS_ATTEMPTS"] == 2
        assert record["ASDS_LANDINGS"] == 2
        assert record["LAST_UPDATE"] == "Successfully landed on ASDS"


def test_fetch_cores_rate_limit_handling(cores_tap):
    """Test rate limit handling for cores endpoint"""
    with (
        patch("requests.get") as mock_get,
        patch("time.sleep") as mock_sleep,
        patch("singer.write_schema"),
        patch("singer.write_record"),
    ):
        # Mock rate limit response followed by success
        rate_limit_response = MagicMock()
        rate_limit_response.status_code = 429
        rate_limit_response.headers = {"Retry-After": "2"}

        success_response = MagicMock()
        success_response.status_code = 200
        success_response.json.return_value = SAMPLE_CORE_DATA

        mock_get.side_effect = [rate_limit_response, success_response]

        # Execute fetch_cores
        cores_tap.fetch_cores()

        # Verify retry behavior
        assert mock_get.call_count == 2
        assert mock_sleep.called
        mock_sleep.assert_called_once_with(2)


def test_fetch_cores_malformed_response(cores_tap):
    """Test handling of malformed core data responses"""
    with patch("requests.get") as mock_get:
        # Test invalid JSON response
        invalid_json_response = MagicMock()
        invalid_json_response.status_code = 200
        invalid_json_response.json.side_effect = json.JSONDecodeError(
            "Invalid JSON", "", 0
        )
        mock_get.return_value = invalid_json_response

        with pytest.raises(ValueError) as exc_info:
            cores_tap.fetch_cores()
        assert "Invalid JSON response" in str(exc_info.value)

        # Test missing required fields
        incomplete_response = MagicMock()
        incomplete_response.status_code = 200
        incomplete_response.json.return_value = [
            {"serial": "B1001"}
        ]  # Missing required 'id' field
        mock_get.return_value = incomplete_response

        with pytest.raises(ValueError) as exc_info:
            cores_tap.fetch_cores()
        assert "Missing required field" in str(exc_info.value)


def test_fetch_cores_state_management(cores_tap, mock_current_time):
    """Test state management for cores endpoint"""
    with (
        patch("requests.get") as mock_get,
        patch("singer.write_schema"),
        patch("singer.write_record"),
        patch("singer.write_state") as mock_write_state,
        patch.object(cores_tap, "get_state") as mock_get_state,
        patch.object(cores_tap, "get_current_time", return_value=mock_current_time),
    ):
        # Mock existing state with bookmark
        mock_get_state.return_value = {
            "bookmarks": {"cores": {"last_record": "2024-01-01T00:00:00Z"}}
        }

        # Mock API response
        mock_response = MagicMock()
        mock_response.json.return_value = SAMPLE_CORE_DATA
        mock_get.return_value = mock_response

        # Execute fetch_cores
        cores_tap.fetch_cores()

        # Verify state was updated
        mock_write_state.assert_called()
        new_state = mock_write_state.call_args[1]["state"]
        assert new_state["CORES"]["last_sync"] > "2024-01-01T00:00:00Z"
