import json
from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest  # type: ignore
from include.fetch_capsules import CapsulesTap

# Sample API response data
SAMPLE_CAPSULE_DATA = [
    {
        "id": "C101",
        "serial": "C201",
        "status": "active",
        "dragon": "dragon1",
        "reuse_count": 2,
        "water_landings": 1,
        "land_landings": 1,
        "last_update": "Successfully landed",
        "launches": ["mission1", "mission2"],
    }
]


@pytest.fixture
def capsules_tap():
    """Instanciate capsules_tap

    Args
        base_url (str) : The root url of v4 SpaceX API
        config_path (str) : Config file that contains database credentials

    Returns:
        CapsulesTap: Class instance
    """
    return CapsulesTap(
        base_url="https://api.spacexdata.com/v4/", config_path="config_snowflake.json"
    )


@pytest.fixture
def mock_current_time():
    """Mock datetime(2024, 1, 1, 12, 0, 0) as a current time"""
    return datetime(2024, 1, 1, 12, 0, 0)


def test_fetch_capsules_successful(capsules_tap, mock_current_time):
    """Test successful capsules data fetching and transformation"""
    with (
        patch("requests.get") as mock_get,
        patch("singer.write_schema") as mock_write_schema,
        patch("singer.write_record") as mock_write_record,
        patch("singer.write_state") as mock_write_state,
        patch.object(capsules_tap, "get_current_time", return_value=mock_current_time),
    ):
        # Mock API response
        mock_response = MagicMock()
        mock_response.json.return_value = SAMPLE_CAPSULE_DATA
        mock_get.return_value = mock_response

        # Execute fetch_capsules
        capsules_tap.fetch_capsules()

        # Verify API call
        mock_get.assert_called_once_with("https://api.spacexdata.com/v4/capsules")

        # Verify schema write
        mock_write_schema.assert_called_once()
        schema_call = mock_write_schema.call_args[1]
        assert schema_call["stream_name"] == "STG_SPACEX_DATA_CAPSULES"
        assert schema_call["key_properties"] == ["CAPSULE_ID"]

        # Verify record write
        mock_write_record.assert_called_once()
        record_call = mock_write_record.call_args[1]
        assert record_call["stream_name"] == "STG_SPACEX_DATA_CAPSULES"
        assert record_call["record"]["CAPSULE_ID"] == "C101"
        assert record_call["record"]["SERIAL"] == "C201"
        assert record_call["record"]["STATUS"] == "active"
        assert record_call["record"]["LAUNCHES"] == ["mission1", "mission2"]

        # Verify state write
        mock_write_state.assert_called_once()
        state_call = mock_write_state.call_args[1]
        assert "CAPSULES" in state_call["state"]
        assert "last_sync" in state_call["state"]["CAPSULES"]


def test_fetch_capsules_api_error(capsules_tap):
    """Test API error handling"""
    with (
        patch("requests.get") as mock_get,
        patch.object(capsules_tap, "log_error") as mock_log_error,
    ):
        # Mock API error
        mock_get.side_effect = Exception("API Error")

        # Execute and verify error handling
        with pytest.raises(Exception):
            capsules_tap.fetch_capsules()

        mock_log_error.assert_called_once()
        error_call = mock_log_error.call_args[1]
        assert error_call["table_name"] == "STG_SPACEX_DATA_CAPSULES"
        assert "API Error" in error_call["error_message"]


def test_fetch_capsules_transform_error(capsules_tap, mock_current_time):
    """Test data transformation error handling"""
    with (
        patch("requests.get") as mock_get,
        # patch("singer.write_schema") as mock_write_schema,
        patch.object(capsules_tap, "log_error") as mock_log_error,
        patch.object(capsules_tap, "get_current_time", return_value=mock_current_time),
    ):
        # Mock invalid API response
        mock_response = MagicMock()
        mock_response.json.return_value = [{"invalid": "data"}]
        mock_get.return_value = mock_response

        # Execute fetch_capsules
        capsules_tap.fetch_capsules()

        # Verify error logging
        mock_log_error.assert_called_once()
        error_call = mock_log_error.call_args[1]
        assert error_call["table_name"] == "STG_SPACEX_DATA_CAPSULES"
        assert "Data transformation error" in error_call["error_message"]


def test_fetch_capsules_schema_validation(capsules_tap):
    """Test schema structure"""
    with (
        patch("requests.get") as mock_get,
        patch("singer.write_schema") as mock_write_schema,
    ):
        # Mock API response
        mock_response = MagicMock()
        mock_response.json.return_value = SAMPLE_CAPSULE_DATA
        mock_get.return_value = mock_response

        # Execute fetch_capsules
        capsules_tap.fetch_capsules()

        # Verify schema structure
        schema_call = mock_write_schema.call_args[1]
        schema = schema_call["schema"]

        # Check required fields
        assert "CAPSULE_ID" in schema["properties"]
        assert "SERIAL" in schema["properties"]
        assert "STATUS" in schema["properties"]
        assert "DRAGON" in schema["properties"]
        assert "REUSE_COUNT" in schema["properties"]
        assert "WATER_LANDINGS" in schema["properties"]
        assert "LAND_LANDINGS" in schema["properties"]
        assert "LAUNCHES" in schema["properties"]
        assert "RAW_DATA" in schema["properties"]

        # Check data types
        assert schema["properties"]["CAPSULE_ID"]["type"] == ["string", "null"]
        assert schema["properties"]["REUSE_COUNT"]["type"] == ["integer", "null"]
        assert schema["properties"]["LAUNCHES"]["type"] == ["array", "null"]


def test_fetch_capsules_empty_response(capsules_tap):
    """Test handling of empty capsules data"""
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

        # Execute fetch_capsules
        capsules_tap.fetch_capsules()

        # Verify schema was still written
        mock_write_schema.assert_called_once()

        # Verify no records were written
        mock_write_record.assert_not_called()

        # Verify state was still written
        mock_write_state.assert_called_once()


def test_fetch_capsules_missing_data(capsules_tap):
    """Test handling of capsules with missing data"""
    incomplete_capsule_data = [
        {
            "id": "C101",
            "serial": "C201",
            # Missing other fields
        }
    ]

    with (
        patch("requests.get") as mock_get,
        # patch("singer.write_schema") as mock_write_schema,
        patch("singer.write_record") as mock_write_record,
    ):
        # Mock API response with incomplete data
        mock_response = MagicMock()
        mock_response.json.return_value = incomplete_capsule_data
        mock_get.return_value = mock_response

        # Execute fetch_capsules
        capsules_tap.fetch_capsules()

        # Verify record was written with None for missing data
        record_call = mock_write_record.call_args[1]
        record = record_call["record"]
        assert record["CAPSULE_ID"] == "C101"
        assert record["SERIAL"] == "C201"
        assert record["STATUS"] is None
        assert record["DRAGON"] is None
        assert record["REUSE_COUNT"] is None
        assert record["WATER_LANDINGS"] is None
        assert record["LAND_LANDINGS"] is None
        assert record["LAUNCHES"] == []


def test_fetch_capsules_rate_limit_handling(capsules_tap):
    """Test rate limit handling for capsules endpoint"""
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
        success_response.json.return_value = SAMPLE_CAPSULE_DATA

        mock_get.side_effect = [rate_limit_response, success_response]

        # Execute fetch_capsules
        capsules_tap.fetch_capsules()

        # Verify retry behavior
        assert mock_get.call_count == 2
        assert mock_sleep.called
        mock_sleep.assert_called_once_with(2)


def test_fetch_capsules_malformed_response(capsules_tap):
    """Test handling of malformed capsule data responses"""
    with (
        patch("requests.get") as mock_get,
        patch.object(capsules_tap, "log_error") as mock_log_error,
    ):
        # Test invalid JSON response
        invalid_json_response = MagicMock()
        invalid_json_response.status_code = 200
        invalid_json_response.json.side_effect = json.JSONDecodeError(
            "Invalid JSON", "", 0
        )
        mock_get.return_value = invalid_json_response

        capsules_tap.fetch_capsules()

        # Verify error was logged
        mock_log_error.assert_called_once()
        error_call = mock_log_error.call_args[1]
        assert error_call["table_name"] == "STG_SPACEX_DATA_CAPSULES"
        assert "Invalid JSON response" in error_call["error_message"]


def test_fetch_capsules_state_management(capsules_tap, mock_current_time):
    """Test state management for capsules endpoint"""
    with (
        patch("requests.get") as mock_get,
        patch("singer.write_schema"),
        patch("singer.write_record"),
        patch("singer.write_state") as mock_write_state,
        patch.object(capsules_tap, "get_state") as mock_get_state,
        patch.object(capsules_tap, "get_current_time", return_value=mock_current_time),
    ):
        # Mock existing state with bookmark
        mock_get_state.return_value = {
            "bookmarks": {"capsules": {"last_record": "2024-01-01T00:00:00Z"}}
        }

        # Mock API response
        mock_response = MagicMock()
        mock_response.json.return_value = SAMPLE_CAPSULE_DATA
        mock_get.return_value = mock_response

        # Execute fetch_capsules
        capsules_tap.fetch_capsules()

        # Verify state was updated
        mock_write_state.assert_called()
        new_state = mock_write_state.call_args[1]["state"]
        assert new_state["CAPSULES"]["last_sync"] > "2024-01-01T00:00:00Z"
