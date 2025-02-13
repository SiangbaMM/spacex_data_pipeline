import json
from datetime import datetime
from typing import Any, Callable, Dict, Generator
from unittest.mock import MagicMock, mock_open, patch

import pytest
import pytz
from include.fetch_company import CompanyTap


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
def company_tap(
    mock_snowflake_connection: MagicMock,
    mock_datetime: MagicMock,
    sample_config: Dict[str, str],
) -> CompanyTap:
    """Fixture providing a CompanyTap instance"""
    mock_file = mock_open(read_data=json.dumps(sample_config))
    with patch("os.path.exists", return_value=True), patch("builtins.open", mock_file):
        tap = CompanyTap(
            "https://api.spacexdata.com/v4/", "singer_tap/tests/config_test.json"
        )
        tap.conn = mock_snowflake_connection
        return tap


@pytest.fixture
def sample_company_data() -> Dict[str, Any]:
    """Fixture providing sample company data"""
    return {
        "id": "5eb75edc42fea42237d7f3ed",
        "name": "SpaceX",
        "founder": "Elon Musk",
        "founded": 2002,
        "employees": 10000,
        "vehicles": 4,
        "launch_sites": 3,
        "test_sites": 2,
        "ceo": "Elon Musk",
        "cto": "Some CTO",
        "coo": "Some COO",
        "cto_propulsion": "Some CTO Propulsion",
        "valuation": 74000000000,
        "headquarters": {
            "address": "Rocket Road",
            "city": "Hawthorne",
            "state": "California",
        },
        "links": {
            "website": "https://www.spacex.com/",
            "twitter": "https://twitter.com/SpaceX",
        },
        "summary": "SpaceX company summary",
    }


@pytest.fixture
def mock_response() -> Callable[[Dict[str, Any]], MagicMock]:
    """Fixture providing a mock response factory"""

    def _create_response(json_data: Dict[str, Any]) -> MagicMock:
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


def test_successful_company_fetch(
    company_tap: CompanyTap,
    sample_company_data: Dict[str, Any],
    mock_response: Callable[[Dict[str, Any]], MagicMock],
    mock_singer: MagicMock,
    mock_datetime: MagicMock,
) -> None:
    """Test successful company data fetching and processing"""
    with patch("requests.get") as mock_get:
        mock_get.return_value = mock_response(sample_company_data)

        with patch("singer.write_schema", mock_singer.write_schema), patch(
            "singer.write_record", mock_singer.write_record
        ), patch("singer.write_state", mock_singer.write_state):
            company_tap.fetch_company()

            assert mock_singer.write_record.call_count == 1
            assert company_tap.conn.cursor().execute.call_count >= 1


def test_api_error_handling(
    company_tap: CompanyTap, sample_api_error: Exception, mock_singer: MagicMock
) -> None:
    """Test handling of API errors"""
    with patch("requests.get", side_effect=sample_api_error), patch(
        "singer.write_schema", mock_singer.write_schema
    ), patch("singer.write_record", mock_singer.write_record), patch(
        "singer.write_state", mock_singer.write_state
    ):
        with pytest.raises(Exception):
            company_tap.fetch_company()

        assert mock_singer.write_record.call_count == 0
        assert company_tap.conn.cursor().execute.call_count > 0


def test_transformation_error(
    company_tap: CompanyTap,
    sample_company_data: Dict[str, Any],
    mock_response: Callable[[Dict[str, Any]], MagicMock],
    mock_singer: MagicMock,
) -> None:
    """Test handling of data transformation errors"""
    bad_data = sample_company_data.copy()
    bad_data[
        "valuation"
    ] = "not a number"  # This should cause a type error during transformation

    def mock_prepare_value(value: Any) -> str:
        if value == "not a number":
            raise TypeError("Object of type 'str' is not JSON serializable")
        return str(value)

    with patch("requests.get") as mock_get, patch.object(
        company_tap, "_prepare_value_for_snowflake", side_effect=mock_prepare_value
    ):
        mock_get.return_value = mock_response(bad_data)

        with (
            patch("singer.write_schema", mock_singer.write_schema),
            patch("singer.write_record", mock_singer.write_record),
            patch("singer.write_state", mock_singer.write_state),
        ):
            # company tap re-raise transformation errors
            with pytest.raises(TypeError):
                #
                company_tap.fetch_company()

            # Verify schema was written before error
            assert mock_singer.write_schema.call_count == 1
            # Verify no records were written
            assert mock_singer.write_record.call_count == 0
            # Verify error was logged
            assert company_tap.conn.cursor().execute.call_count == 3


def test_schema_validation(
    company_tap: CompanyTap,
    sample_company_data: Dict[str, Any],
    mock_response: Callable[[Dict[str, Any]], MagicMock],
    mock_singer: MagicMock,
) -> None:
    """Test that the schema matches the transformed data structure"""
    schema_spy = MagicMock()

    with patch("requests.get") as mock_get:
        mock_get.return_value = mock_response(sample_company_data)

        with (
            patch("singer.write_schema", schema_spy),
            patch("singer.write_record", mock_singer.write_record),
            patch("singer.write_state", mock_singer.write_state),
        ):
            company_tap.fetch_company()

            assert schema_spy.call_count == 1
            _, kwargs = schema_spy.call_args
            schema = kwargs["schema"]

            assert "ID" in schema["properties"]
            assert "NAME" in schema["properties"]
            assert "FOUNDER" in schema["properties"]
            assert "FOUNDED" in schema["properties"]
            assert "EMPLOYEES" in schema["properties"]
            assert "VALUATION" in schema["properties"]
            assert "RAW_DATA" in schema["properties"]
