import json
from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest  # type:ignore

from singer_tap.include.spacex_tap_base import SpaceXTapBase


@pytest.fixture
def spacex_tap_base():
    """Instanciate spacex_tap_base

    Args:
        base_url (str) : The root url of v4 SpaceX API
        config_path (str) : Config file that contains database credentials

    Returns:
        SpaceXTapBase: Class instance
    """
    return SpaceXTapBase(
        base_url="https://api.spacexdata.com/v4/", config_path="config_snowflake.json"
    )


def test_get_current_time(spacex_tap_base):
    """Test get_current_time returns datetime object"""
    current_time = spacex_tap_base.get_current_time()
    assert isinstance(current_time, datetime)


def test_log_error(spacex_tap_base, caplog):
    """Test error logging functionality"""
    table_name = "TEST_TABLE"
    error_message = "Test error message"
    error_data = {"test": "data"}

    spacex_tap_base.log_error(
        table_name=table_name, error_message=error_message, error_data=error_data
    )

    # Verify log message contains required information
    assert table_name in caplog.text
    assert error_message in caplog.text
    assert str(error_data) in caplog.text


def test_base_url_initialization():
    """Test base URL initialization"""
    base_url = "https://test.api.com/"
    config_path = "test_config_snowflake.json"
    tap = SpaceXTapBase(base_url=base_url, config_path=config_path)

    assert tap.base_url == base_url
    assert tap.config_path == config_path


def test_base_url_trailing_slash():
    """Test base URL automatically adds trailing slash if missing"""
    base_url = "https://test.api.com"
    tap = SpaceXTapBase(base_url=base_url, config_path="config_snowflake.json")

    assert tap.base_url == base_url + "/"


def test_rate_limit_handling(spacex_tap_base):
    """Test rate limit handling with exponential backoff"""
    with patch("requests.get") as mock_get, patch("time.sleep") as mock_sleep:
        # Mock rate limit response followed by success
        rate_limit_response = MagicMock()
        rate_limit_response.status_code = 429
        rate_limit_response.headers = {"Retry-After": "2"}

        success_response = MagicMock()
        success_response.status_code = 200
        success_response.json.return_value = {"data": "test"}

        mock_get.side_effect = [rate_limit_response, success_response]

        # Make request
        response = spacex_tap_base.make_request("test_endpoint")

        # Verify retry behavior
        assert mock_get.call_count == 2
        assert mock_sleep.called
        assert response.json() == {"data": "test"}


def test_pagination_handling(spacex_tap_base):
    """Test handling of paginated responses"""
    with patch("requests.get") as mock_get:
        # Mock paginated responses
        first_page = MagicMock()
        first_page.status_code = 200
        first_page.json.return_value = {"data": ["item1", "item2"], "next": "/page/2"}

        second_page = MagicMock()
        second_page.status_code = 200
        second_page.json.return_value = {"data": ["item3"], "next": None}

        mock_get.side_effect = [first_page, second_page]

        # Get all pages
        all_data = spacex_tap_base.get_all_pages("test_endpoint")

        # Verify pagination handling
        assert mock_get.call_count == 2
        assert all_data == ["item1", "item2", "item3"]


def test_config_validation(spacex_tap_base):
    """Test configuration validation"""
    # Test missing required fields
    with pytest.raises(ValueError) as exc_info:
        SpaceXTapBase.validate_config({})
        assert "Missing required configuration" in str(exc_info.value)

    # Test invalid start date format
    with pytest.raises(ValueError) as exc_info:
        SpaceXTapBase.validate_config(
            {"start_date": "invalid-date", "api_token": "test_token"}
        )
        assert "Invalid start_date format" in str(exc_info.value)

    # Test valid config
    valid_config = {"start_date": "2024-01-01T00:00:00Z", "api_token": "test_token"}
    assert SpaceXTapBase.validate_config(valid_config) is None


def test_state_management(spacex_tap_base):
    """Test state management and bookmarking"""
    test_state = {"bookmarks": {"launches": {"last_record": "2024-01-01T00:00:00Z"}}}

    # Test state loading
    with patch("builtins.open", create=True) as mock_open:
        mock_open.return_value.__enter__.return_value.read.return_value = json.dumps(
            test_state
        )
        loaded_state = spacex_tap_base.load_state()
        assert loaded_state == test_state

    # Test state saving
    with patch("builtins.open", create=True) as mock_open:
        spacex_tap_base.save_state(test_state)
        mock_open.assert_called_once()
        written_data = json.loads(
            mock_open.return_value.__enter__.return_value.write.call_args[0][0]
        )
        assert written_data == test_state


def test_malformed_response_handling(spacex_tap_base):
    """Test handling of malformed API responses"""
    with patch("requests.get") as mock_get:
        # Test invalid JSON response
        invalid_json_response = MagicMock()
        invalid_json_response.status_code = 200
        invalid_json_response.json.side_effect = json.JSONDecodeError(
            "Invalid JSON", "", 0
        )
        mock_get.return_value = invalid_json_response

        with pytest.raises(ValueError) as exc_info:
            spacex_tap_base.make_request("test_endpoint")
            assert "Invalid JSON response" in str(exc_info.value)

        # Test unexpected response structure
        unexpected_response = MagicMock()
        unexpected_response.status_code = 200
        unexpected_response.json.return_value = None
        mock_get.return_value = unexpected_response

        with pytest.raises(ValueError) as exc_info:
            spacex_tap_base.make_request("test_endpoint")
            assert "Unexpected response format" in str(exc_info.value)


def test_bookmark_handling(spacex_tap_base):
    """Test bookmark handling for incremental syncs"""
    test_state = {"bookmarks": {"launches": {"last_record": "2024-01-01T00:00:00Z"}}}

    # Test getting bookmark
    with patch.object(spacex_tap_base, "load_state", return_value=test_state):
        bookmark = spacex_tap_base.get_bookmark("launches")
        assert bookmark == "2024-01-01T00:00:00Z"

        # Test non-existent bookmark
        assert spacex_tap_base.get_bookmark("non_existent") is None

    # Test setting bookmark
    new_bookmark = "2024-01-02T00:00:00Z"
    with patch.object(spacex_tap_base, "save_state") as mock_save:
        spacex_tap_base.set_bookmark("launches", new_bookmark)
        saved_state = mock_save.call_args[0][0]
        assert saved_state["bookmarks"]["launches"]["last_record"] == new_bookmark
