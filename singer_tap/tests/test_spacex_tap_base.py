import json
from datetime import datetime
from typing import Any, Dict, Generator, List, Union
from unittest.mock import MagicMock, mock_open, patch

import pytest
import pytz
from include.spacex_tap_base import SpaceXTapBase

ResponseData = Union[Dict[str, Any], List[Dict[str, Any]]]


@pytest.fixture
def mock_datetime() -> Generator[MagicMock, None, None]:
    """Fixture providing a mocked datetime"""
    mock_dt = MagicMock(wraps=datetime)
    mock_now = datetime(2024, 1, 1, 12, 0, 0, tzinfo=pytz.UTC)
    mock_dt.now = MagicMock(return_value=mock_now)
    with patch("include.spacex_tap_base.datetime", mock_dt), patch(
        "datetime.datetime", mock_dt
    ):
        yield mock_dt


@pytest.fixture
def mock_config() -> Dict[str, str]:
    """Fixture providing mock config data"""
    return {
        "user": "test_user",
        "password": "test_password",
        "account": "test_account",
        "warehouse": "test_warehouse",
        "database": "test_database",
        "schema": "test_schema",
    }


@pytest.fixture
def spacex_tap_base(
    mock_snowflake_connection: MagicMock,
    mock_datetime: MagicMock,
    mock_config: Dict[str, str],
) -> SpaceXTapBase:
    """Fixture providing a SpaceXTapBase instance"""
    mock_file = mock_open(read_data=json.dumps(mock_config))
    with patch("os.path.exists", return_value=True), patch("builtins.open", mock_file):
        tap = SpaceXTapBase(
            "https://api.spacexdata.com/v4/", "singer_tap/tests/config_test.json"
        )
        tap.conn = mock_snowflake_connection
        return tap


def test_get_current_time(
    spacex_tap_base: SpaceXTapBase, mock_datetime: MagicMock
) -> None:
    """Test get_current_time returns correct datetime"""
    current_time = spacex_tap_base.get_current_time()
    assert isinstance(current_time, datetime)
    assert current_time.tzinfo == pytz.UTC
    assert current_time == mock_datetime.now.return_value


def test_log_error(spacex_tap_base: SpaceXTapBase, mock_datetime: MagicMock) -> None:
    """Test error logging functionality"""
    test_table = "TEST_TABLE"
    test_message = "Test error message"
    test_data = {"key": "value"}

    # Mock cursor for error logging
    mock_cursor = MagicMock()
    spacex_tap_base.conn.cursor.return_value = mock_cursor

    spacex_tap_base.log_error(
        table_name=test_table, error_message=test_message, error_data=test_data
    )

    # Verify error was logged to Snowflake
    assert mock_cursor.execute.call_count == 1
    # Verify cursor was closed
    assert mock_cursor.close.call_count == 1
    # Verify the execute call arguments
    args = mock_cursor.execute.call_args[0]
    assert test_table in str(args)
    assert test_message in str(args)


def test_close_connection(spacex_tap_base: SpaceXTapBase) -> None:
    """Test close_connection method"""
    # Add some records to buffer to test flushing
    spacex_tap_base.insert_into_snowflake("TEST_STREAM", {"id": "1", "name": "test"})

    # Mock cursor for buffer flush
    mock_cursor = MagicMock()
    spacex_tap_base.conn.cursor.return_value = mock_cursor

    # Close connection
    spacex_tap_base.close_connection()

    # Verify buffer was flushed and connection was closed
    assert mock_cursor.execute.call_count >= 1
    assert spacex_tap_base.conn.close.call_count == 1


def test_base_url_initialization(spacex_tap_base: SpaceXTapBase) -> None:
    """Test base_url is properly initialized"""
    assert spacex_tap_base.base_url == "https://api.spacexdata.com/v4/"
    assert spacex_tap_base.base_url.endswith("/")

    # Test URL normalization
    mock_config = {
        "user": "test",
        "password": "test",
        "account": "test",
        "warehouse": "test",
        "database": "test",
        "schema": "test",
    }
    mock_file = mock_open(read_data=json.dumps(mock_config))
    with patch("os.path.exists", return_value=True), patch("builtins.open", mock_file):
        tap = SpaceXTapBase("https://api.example.com/v1///", "config.json")
        assert tap.base_url == "https://api.example.com/v1/"


def test_config_path_initialization(spacex_tap_base: SpaceXTapBase) -> None:
    """Test config_path is properly initialized"""
    assert spacex_tap_base.snowflake_config is not None
    assert isinstance(spacex_tap_base.snowflake_config, dict)
    assert "user" in spacex_tap_base.snowflake_config
    assert "password" in spacex_tap_base.snowflake_config
    assert "account" in spacex_tap_base.snowflake_config


def test_error_logging_without_data(
    spacex_tap_base: SpaceXTapBase, mock_datetime: MagicMock
) -> None:
    """Test error logging without error_data"""
    test_table = "TEST_TABLE"
    test_message = "Test error message"

    # Mock cursor for error logging
    mock_cursor = MagicMock()
    spacex_tap_base.conn.cursor.return_value = mock_cursor

    spacex_tap_base.log_error(table_name=test_table, error_message=test_message)

    # Verify error was logged to Snowflake
    assert mock_cursor.execute.call_count == 1
    # Verify cursor was closed
    assert mock_cursor.close.call_count == 1
    # Verify the execute call arguments
    args = mock_cursor.execute.call_args[0]
    assert test_table in str(args)
    assert test_message in str(args)


def test_initialization_with_invalid_url() -> None:
    """Test initialization with invalid URL"""
    mock_config = {
        "user": "test",
        "password": "test",
        "account": "test",
        "warehouse": "test",
        "database": "test",
        "schema": "test",
    }
    mock_file = mock_open(read_data=json.dumps(mock_config))
    with patch("os.path.exists", return_value=True), patch("builtins.open", mock_file):
        with pytest.raises(ValueError, match="base_url cannot be empty"):
            SpaceXTapBase("", "singer_tap/tests/config_test.json")


def test_initialization_with_invalid_config() -> None:
    """Test initialization with invalid config path"""
    with pytest.raises(ValueError, match="config_path cannot be empty"):
        SpaceXTapBase("https://api.spacexdata.com/v4/", "")


def test_initialization_with_missing_config_fields() -> None:
    """Test initialization with missing Snowflake config fields"""
    mock_config = {"user": "test"}  # Missing required fields
    mock_file = mock_open(read_data=json.dumps(mock_config))
    with patch("os.path.exists", return_value=True), patch("builtins.open", mock_file):
        with pytest.raises(
            ValueError, match="Missing required Snowflake configuration fields"
        ):
            SpaceXTapBase("https://api.spacexdata.com/v4/", "config.json")


def test_batch_processing(spacex_tap_base: SpaceXTapBase) -> None:
    """Test batch processing functionality"""
    stream_name = "TEST_STREAM"

    # Set small batch size for testing
    spacex_tap_base.BATCH_SIZE = 2

    # Mock cursor for batch inserts
    mock_cursor = MagicMock()
    spacex_tap_base.conn.cursor.return_value = mock_cursor

    # Insert records
    for i in range(3):  # This should cause one flush after 2 records
        record = {"id": str(i), "name": f"test_{i}"}
        spacex_tap_base.insert_into_snowflake(stream_name, record)

    # Verify one batch was flushed
    assert mock_cursor.execute.call_count >= 1
    # Close to flush remaining records
    spacex_tap_base.close_connection()
    # Verify final flush
    assert mock_cursor.execute.call_count >= 2
