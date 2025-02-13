import json
from datetime import datetime
from typing import Any, Callable, Dict, Generator, List
from unittest.mock import MagicMock, mock_open, patch

import pytest
import pytz
from include.fetch_ships import ShipsTap


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
def ships_tap(
    mock_snowflake_connection: MagicMock,
    mock_datetime: MagicMock,
    sample_config: Dict[str, str],
) -> ShipsTap:
    """Fixture providing a ShipsTap instance"""
    mock_file = mock_open(read_data=json.dumps(sample_config))
    with patch("os.path.exists", return_value=True), patch("builtins.open", mock_file):
        tap = ShipsTap(
            "https://api.spacexdata.com/v4/", "singer_tap/tests/config_test.json"
        )
        tap.conn = mock_snowflake_connection
        tap.BATCH_SIZE = 1
        return tap


@pytest.fixture
def sample_ships_data() -> List[Dict[str, Any]]:
    """Fixture providing sample ships data"""
    return [
        {
            "id": "5ea6ed2e080df4000697c908",
            "name": "GO Ms Tree",
            "type": "High Speed Craft",
            "active": True,
            "home_port": "Port Canaveral",
            "roles": ["Fairing Recovery"],
            "mass_kg": 449964,
            "year_built": 2015,
            "launches": ["5eb87d46ffd86e000604b388"],
            "image": "https://i.imgur.com/example.jpg",
            "last_ais_update": "2023-01-01T12:00:00.000Z",
        },
        {
            "id": "5ea6ed2f080df4000697c90b",
            "name": "GO Ms Chief",
            "type": "High Speed Craft",
            "active": False,
            "home_port": "Port Canaveral",
            "roles": ["Fairing Recovery"],
            "mass_kg": 449964,
            "year_built": 2014,
            "launches": ["5eb87d46ffd86e000604b389"],
            "image": "https://i.imgur.com/example2.jpg",
            "last_ais_update": "2023-01-02T12:00:00.000Z",
        },
    ]


@pytest.fixture
def mock_response() -> Callable[[List[Dict[str, Any]]], MagicMock]:
    """Fixture providing a mock response factory"""

    def _create_response(json_data: List[Dict[str, Any]]) -> MagicMock:
        mock = MagicMock()
        mock.json.return_value = json_data
        mock.status_code = 200
        return mock

    return _create_response


@pytest.fixture
def mock_singer() -> MagicMock:
    """Fixture providing mocked singer functions"""
    mock = MagicMock()
    mock.write_schema = MagicMock()
    mock.write_record = MagicMock()
    mock.write_state = MagicMock()
    return mock


@pytest.fixture
def sample_api_error() -> Exception:
    """Fixture providing a sample API error"""

    class MockApiError(Exception):
        def __init__(self) -> None:
            self.response = type(
                "Response", (), {"status_code": 500, "text": "Internal Server Error"}
            )

    return MockApiError()


def test_successful_ships_fetch(
    ships_tap: ShipsTap,
    sample_ships_data: List[Dict[str, Any]],
    mock_response: Callable[[List[Dict[str, Any]]], MagicMock],
    mock_singer: MagicMock,
    mock_datetime: MagicMock,
) -> None:
    """Test successful ships data fetching and processing"""
    with patch("requests.get") as mock_get:
        mock_get.return_value = mock_response(sample_ships_data)

        with patch("singer.write_schema", mock_singer.write_schema), patch(
            "singer.write_record", mock_singer.write_record
        ), patch("singer.write_state", mock_singer.write_state):
            ships_tap.fetch_ships()

            assert mock_singer.write_record.call_count == len(sample_ships_data)
            assert ships_tap.conn.cursor().execute.call_count >= len(sample_ships_data)


def test_api_error_handling(
    ships_tap: ShipsTap, sample_api_error: Exception, mock_singer: MagicMock
) -> None:
    """Test handling of API errors"""
    with patch("requests.get", side_effect=sample_api_error), patch(
        "singer.write_schema", mock_singer.write_schema
    ), patch("singer.write_record", mock_singer.write_record), patch(
        "singer.write_state", mock_singer.write_state
    ):
        with pytest.raises(Exception):
            ships_tap.fetch_ships()

        assert mock_singer.write_record.call_count == 0
        assert ships_tap.conn.cursor().execute.call_count > 0


def test_transformation_error(
    ships_tap: ShipsTap,
    sample_ships_data: List[Dict[str, Any]],
    mock_response: Callable[[List[Dict[str, Any]]], MagicMock],
    mock_singer: MagicMock,
) -> None:
    """Test handling of data transformation errors"""
    bad_data = sample_ships_data.copy()
    bad_data[0][
        "mass_kg"
    ] = "not a number"  # This should cause a type error during transformation

    def mock_prepare_value(value: Any) -> str:
        if value == "not a number":
            raise TypeError("Object of type 'str' is not JSON serializable")
        return str(value)

    with patch("requests.get") as mock_get, patch.object(
        ships_tap, "_prepare_value_for_snowflake", side_effect=mock_prepare_value
    ):
        mock_get.return_value = mock_response(bad_data)

        with patch("singer.write_schema", mock_singer.write_schema), patch(
            "singer.write_record", mock_singer.write_record
        ), patch("singer.write_state", mock_singer.write_state):
            # Execute fetch - should continue despite error
            ships_tap.fetch_ships()

            # Verify schema was written
            assert mock_singer.write_schema.call_count == 1
            # Verify only the second record was processed
            assert mock_singer.write_record.call_count == 1
            # Verify error was logged and successful record was inserted
            assert (
                ships_tap.conn.cursor().execute.call_count == 3
            )  # 1 for error log, 1 for successful record


def test_schema_validation(
    ships_tap: ShipsTap,
    sample_ships_data: List[Dict[str, Any]],
    mock_response: Callable[[List[Dict[str, Any]]], MagicMock],
    mock_singer: MagicMock,
) -> None:
    """Test that the schema matches the transformed data structure"""
    schema_spy = MagicMock()

    with patch("requests.get") as mock_get:
        mock_get.return_value = mock_response(sample_ships_data)

        with patch("singer.write_schema", schema_spy), patch(
            "singer.write_record", mock_singer.write_record
        ), patch("singer.write_state", mock_singer.write_state):
            ships_tap.fetch_ships()

            assert schema_spy.call_count == 1
            _, kwargs = schema_spy.call_args
            schema = kwargs["schema"]

            assert "SHIP_ID" in schema["properties"]
            assert "NAME" in schema["properties"]
            assert "TYPE" in schema["properties"]
            assert "ACTIVE" in schema["properties"]
            assert "HOME_PORT" in schema["properties"]
            assert "ROLES" in schema["properties"]
            assert "RAW_DATA" in schema["properties"]


def test_empty_response_handling(
    ships_tap: ShipsTap,
    mock_response: Callable[[List[Dict[str, Any]]], MagicMock],
    mock_singer: MagicMock,
) -> None:
    """Test handling of empty response from API"""
    with patch("requests.get") as mock_get:
        mock_get.return_value = mock_response([])

        with patch("singer.write_schema", mock_singer.write_schema), patch(
            "singer.write_record", mock_singer.write_record
        ), patch("singer.write_state", mock_singer.write_state):
            ships_tap.fetch_ships()

            # Verify schema was written even for empty response
            assert mock_singer.write_schema.call_count == 1
            # Verify no records were written
            assert mock_singer.write_record.call_count == 0
            # Verify state was written
            assert mock_singer.write_state.call_count == 1
            # Verify no database operations were performed
            assert ships_tap.conn.cursor().execute.call_count == 1
