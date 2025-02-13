import json
from datetime import datetime
from typing import Any, Callable, Dict, List, Union
from unittest.mock import MagicMock, mock_open

import pytest
from pytest import MonkeyPatch


@pytest.fixture
def mock_current_time() -> datetime:
    """Fixture providing a fixed datetime for testing"""
    return datetime(2024, 1, 1, 12, 0, 0)


@pytest.fixture
def sample_api_error() -> Exception:
    """Fixture providing a sample API error"""

    class MockApiError(Exception):
        def __init__(self) -> None:
            self.response = type(
                "Response", (), {"status_code": 500, "text": "Internal Server Error"}
            )

    return MockApiError()


@pytest.fixture
def sample_config() -> Dict[str, str]:
    """Fixture providing sample configuration"""
    return {
        "start_date": "2024-01-01T00:00:00Z",
        "api_token": "test_token",
        "user_agent": "SpaceX-Tap/1.0",
        "user": "test_user",
        "password": "test_password",
        "account": "test_account",
        "warehouse": "test_warehouse",
        "database": "test_database",
        "schema": "test_schema",
    }


@pytest.fixture
def mock_singer() -> MagicMock:
    """Fixture providing mocked singer functions with call tracking"""
    mock = MagicMock()
    mock.write_schema = MagicMock()
    mock.write_record = MagicMock()
    mock.write_state = MagicMock()
    return mock


@pytest.fixture
def mock_response() -> (
    Callable[[Union[Dict[str, Any], List[Dict[str, Any]]]], MagicMock]
):
    """Fixture providing a mock requests response"""

    def _create_response(
        json_data: Union[Dict[str, Any], List[Dict[str, Any]]], status_code: int = 200
    ) -> MagicMock:
        mock = MagicMock()
        mock.json.return_value = json_data
        mock.status_code = status_code
        return mock

    return _create_response


@pytest.fixture
def mock_snowflake_cursor() -> MagicMock:
    """Fixture providing a mock Snowflake cursor"""
    cursor = MagicMock()
    cursor.fetchone.return_value = ["test_version"]
    cursor.execute = MagicMock()
    return cursor


@pytest.fixture
def mock_snowflake_connection(mock_snowflake_cursor: MagicMock) -> MagicMock:
    """Fixture providing a mock Snowflake connection"""
    conn = MagicMock()
    conn.cursor.return_value = mock_snowflake_cursor
    return conn


@pytest.fixture(autouse=True)
def mock_snowflake(
    monkeypatch: MonkeyPatch, mock_snowflake_connection: MagicMock
) -> MagicMock:
    """Fixture to automatically mock Snowflake connector for all tests"""
    mock_connector = MagicMock()
    mock_connector.connect.return_value = mock_snowflake_connection
    monkeypatch.setattr("snowflake.connector", mock_connector)
    return mock_connector


@pytest.fixture(autouse=True)
def mock_config_file(sample_config: Dict[str, str], monkeypatch: MonkeyPatch) -> None:
    """Fixture to automatically mock config file operations for all tests"""
    mock_file = mock_open(read_data=json.dumps(sample_config))
    monkeypatch.setattr("builtins.open", mock_file)
    monkeypatch.setattr("os.path.exists", lambda x: True)
