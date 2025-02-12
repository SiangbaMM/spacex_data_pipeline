import json
from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest  # type: ignore

from singer_tap.include.fetch_starlink import StarlinkTap


@pytest.fixture
def starlink_tap():
    """Instanciate starlink_tap

    Args
        base_url (str) : The root url of v4 SpaceX API
        config_path (str) : Config file that contains database credentials

    Returns:
        StarlinkTap: Class instance
    """
    return StarlinkTap(
        base_url="https://api.spacexdata.com/v4/", config_path="config_snowflake.json"
    )


# Sample API response data
SAMPLE_STARLINK_DATA = [
    {
        "id": "5eed770f096e59000698560d",
        "version": "v1.0",
        "launch": "5eb87d46ffd86e000604b388",
        "longitude": -55.0229,
        "latitude": 37.7749,
        "height_km": 550.52,
        "velocity_kms": 7.65,
        "spaceTrack": {
            "CCSDS_OMM_VERS": "2.0",
            "COMMENT": "GENERATED VIA SPACE-TRACK.ORG API",
            "CREATION_DATE": "2024-01-01 12:00:00",
            "ORIGINATOR": "18 SPCS",
            "OBJECT_NAME": "STARLINK-1234",
            "OBJECT_ID": "2020-001A",
            "CENTER_NAME": "EARTH",
            "REF_FRAME": "TEME",
            "TIME_SYSTEM": "UTC",
            "MEAN_ELEMENT_THEORY": "SGP4",
            "EPOCH": "2024-01-01T10:00:00.000Z",
            "MEAN_MOTION": 15.06,
            "ECCENTRICITY": 0.0001,
            "INCLINATION": 53.0,
            "RA_OF_ASC_NODE": 180.0,
            "ARG_OF_PERICENTER": 90.0,
            "MEAN_ANOMALY": 270.0,
            "EPHEMERIS_TYPE": "0",
            "CLASSIFICATION_TYPE": "U",
            "NORAD_CAT_ID": "44713",
            "ELEMENT_SET_NO": "999",
            "REV_AT_EPOCH": "1234",
            "BSTAR": "0.000001",
            "MEAN_MOTION_DOT": "0.00000001",
            "MEAN_MOTION_DDOT": "0.00000001",
            "SEMIMAJOR_AXIS": "6920",
            "PERIOD": "95",
            "APOAPSIS": "550",
            "PERIAPSIS": "550",
            "OBJECT_TYPE": "PAYLOAD",
            "RCS_SIZE": "LARGE",
            "COUNTRY_CODE": "US",
            "LAUNCH_DATE": "2020-01-01",
            "SITE": "AFETR",
            "DECAY_DATE": None,
            "DECAYED": 0,
            "FILE": "3456",
            "GP_ID": "12345",
            "TLE_LINE0": "0 STARLINK-1234",
            "TLE_LINE1": "1 44713U 20001A   24001.41666667  .00000001  00000-0  10000-3 0  9999",  # noqa E501
            "TLE_LINE2": "2 44713  53.0000 180.0000 0001000  90.0000 270.0000 15.06000000 12340",  # noqa E501
        },
    }
]


def test_fetch_starlink_successful():
    """Test successful Starlink data fetching and transformation"""
    with (
        patch("requests.get") as mock_get,
        patch("singer.write_schema") as mock_write_schema,
        patch("singer.write_record") as mock_write_record,
        patch("singer.write_state") as mock_write_state,
        patch.object(starlink_tap, "get_current_time") as mock_time,
    ):
        # Mock current time
        mock_time.return_value = datetime(2024, 1, 1, 12, 0, 0)

        # Mock API response
        mock_response = MagicMock()
        mock_response.json.return_value = SAMPLE_STARLINK_DATA
        mock_get.return_value = mock_response

        # Execute fetch_starlink
        starlink_tap.fetch_starlink()

        # Verify API call
        mock_get.assert_called_once_with("https://api.spacexdata.com/v4/starlink")

        # Verify schema write
        mock_write_schema.assert_called_once()
        schema_call = mock_write_schema.call_args[1]
        assert schema_call["stream_name"] == "STARLINK"
        assert schema_call["key_properties"] == ["STARLINK_ID"]

        # Verify record write
        mock_write_record.assert_called_once()
        record_call = mock_write_record.call_args[1]
        record = record_call["record"]

        # Verify basic fields
        assert record["STARLINK_ID"] == "5eed770f096e59000698560d"
        assert record["VERSION"] == "v1.0"
        assert record["LAUNCH"] == "5eb87d46ffd86e000604b388"
        assert record["LONGITUDE"] == -55.0229
        assert record["LATITUDE"] == 37.7749
        assert record["HEIGHT_KM"] == 550.52
        assert record["VELOCITY_KMS"] == 7.65

        # Verify spacetrack data
        spacetrack = json.loads(record["SPACETRACK"])
        assert spacetrack["OBJECT_NAME"] == "STARLINK-1234"
        assert spacetrack["OBJECT_ID"] == "2020-001A"
        assert spacetrack["NORAD_CAT_ID"] == "44713"

        # Verify orbital parameters
        assert record["EPOCH"] == "2024-01-01T10:00:00.000Z"
        assert record["PERIOD_MIN"] == "95"
        assert record["INCLINATION_DEG"] == "53.0"
        assert record["APOAPSIS_KM"] == "550"
        assert record["PERIAPSIS_KM"] == "550"

        # Verify state write
        mock_write_state.assert_called_once()
        state_call = mock_write_state.call_args[1]
        assert "STARLINK" in state_call["state"]
        assert "last_sync" in state_call["state"]["STARLINK"]


def test_fetch_starlink_api_error():
    """Test API error handling"""
    with patch("requests.get") as mock_get:
        # Mock API error
        mock_get.side_effect = Exception("API Error")

        # Execute and verify error handling
        with pytest.raises(Exception):
            starlink_tap.fetch_starlink()


def test_fetch_starlink_schema_validation():
    """Test schema structure"""
    with (
        patch("requests.get") as mock_get,
        patch("singer.write_schema") as mock_write_schema,
    ):
        # Mock API response
        mock_response = MagicMock()
        mock_response.json.return_value = SAMPLE_STARLINK_DATA
        mock_get.return_value = mock_response

        # Execute fetch_starlink
        starlink_tap.fetch_starlink()

        # Verify schema structure
        schema_call = mock_write_schema.call_args[1]
        schema = schema_call["schema"]

        # Check required fields
        assert "STARLINK_ID" in schema["properties"]
        assert "VERSION" in schema["properties"]
        assert "LAUNCH" in schema["properties"]
        assert "LONGITUDE" in schema["properties"]
        assert "LATITUDE" in schema["properties"]
        assert "HEIGHT_KM" in schema["properties"]
        assert "VELOCITY_KMS" in schema["properties"]
        assert "SPACETRACK" in schema["properties"]

        # Check data types
        assert schema["properties"]["STARLINK_ID"]["type"] == ["string", "null"]
        assert schema["properties"]["LONGITUDE"]["type"] == ["number", "null"]
        assert schema["properties"]["HEIGHT_KM"]["type"] == ["number", "null"]
        assert schema["properties"]["EPOCH"]["format"] == "date-time"


def test_fetch_starlink_empty_response():
    """Test handling of empty Starlink data"""
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

        # Execute fetch_starlink
        starlink_tap.fetch_starlink()

        # Verify schema was still written
        mock_write_schema.assert_called_once()

        # Verify no records were written
        mock_write_record.assert_not_called()

        # Verify state was still written
        mock_write_state.assert_called_once()


def test_fetch_starlink_missing_spacetrack():
    """Test handling of Starlink data with missing spacetrack info"""
    incomplete_starlink_data = [
        {
            "id": "test_id",
            "version": "v1.0",
            "launch": "test_launch",
            "longitude": -55.0229,
            "latitude": 37.7749,
            "height_km": 550.52,
            # Missing spacetrack data
        }
    ]

    with (
        patch("requests.get") as mock_get,
        # patch("singer.write_schema") as mock_write_schema,
        patch("singer.write_record") as mock_write_record,
    ):
        # Mock API response with incomplete data
        mock_response = MagicMock()
        mock_response.json.return_value = incomplete_starlink_data
        mock_get.return_value = mock_response

        # Execute fetch_starlink
        starlink_tap.fetch_starlink()

        # Verify record was written with None for missing spacetrack data
        record_call = mock_write_record.call_args[1]
        record = record_call["record"]
        assert record["STARLINK_ID"] == "test_id"
        assert record["VERSION"] == "v1.0"
        assert record["LAUNCH"] == "test_launch"
        assert record["LONGITUDE"] == -55.0229
        assert record["LATITUDE"] == 37.7749
        assert record["HEIGHT_KM"] == 550.52
        assert json.loads(record["SPACETRACK"]) == {}
        assert record["LAUNCH_DATE"] is None
        assert record["OBJECT_NAME"] is None
        assert record["EPOCH"] is None


def test_fetch_starlink_orbital_parameters():
    """Test handling of orbital parameters"""
    starlink_with_orbit = [
        {
            "id": "test_id",
            "spaceTrack": {
                "EPOCH": "2024-01-01T10:00:00.000Z",
                "MEAN_MOTION": 15.06,
                "ECCENTRICITY": 0.0001,
                "INCLINATION": 53.0,
                "PERIOD": "95",
                "APOAPSIS": "550",
                "PERIAPSIS": "550",
                "MEAN_ANOMALY": 270.0,
                "ARG_OF_PERICENTER": 90.0,
                "RAAN": 180.0,
                "SEMI_MAJOR_AXIS": "6920",
            },
        }
    ]

    with (
        patch("requests.get") as mock_get,
        # patch("singer.write_schema") as mock_write_schema,
        patch("singer.write_record") as mock_write_record,
    ):
        # Mock API response
        mock_response = MagicMock()
        mock_response.json.return_value = starlink_with_orbit
        mock_get.return_value = mock_response

        # Execute fetch_starlink
        starlink_tap.fetch_starlink()

        # Verify orbital parameters handling
        record_call = mock_write_record.call_args[1]
        record = record_call["record"]
        assert record["EPOCH"] == "2024-01-01T10:00:00.000Z"
        assert record["PERIOD_MIN"] == "95"
        assert record["INCLINATION_DEG"] == "53.0"
        assert record["APOAPSIS_KM"] == "550"
        assert record["PERIAPSIS_KM"] == "550"
        assert record["MEAN_MOTION"] == "15.06"
        assert record["MEAN_ANOMALY"] == "270.0"
        assert record["ARG_OF_PERICENTER"] == "90.0"
        assert record["RAAN"] == "180.0"
        assert record["SEMI_MAJOR_AXIS_KM"] == "6920"


def test_fetch_starlink_rate_limit_handling():
    """Test rate limit handling for Starlink endpoint"""
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
        success_response.json.return_value = SAMPLE_STARLINK_DATA

        mock_get.side_effect = [rate_limit_response, success_response]

        # Execute fetch_starlink
        starlink_tap.fetch_starlink()

        # Verify retry behavior
        assert mock_get.call_count == 2
        assert mock_sleep.called
        mock_sleep.assert_called_once_with(2)


def test_fetch_starlink_pagination():
    """Test handling of paginated Starlink data"""
    with (
        patch("requests.get") as mock_get,
        patch("singer.write_schema"),
        patch("singer.write_record") as mock_write_record,
    ):
        # Mock paginated responses
        first_page = MagicMock()
        first_page.status_code = 200
        first_page.json.return_value = {
            "docs": [SAMPLE_STARLINK_DATA[0]],
            "totalDocs": 2,
            "offset": 0,
            "limit": 1,
            "totalPages": 2,
            "page": 1,
            "pagingCounter": 1,
            "hasPrevPage": False,
            "hasNextPage": True,
            "prevPage": None,
            "nextPage": 2,
        }

        second_page = MagicMock()
        second_page.status_code = 200
        second_page.json.return_value = {
            "docs": [
                {
                    **SAMPLE_STARLINK_DATA[0],
                    "id": "second_starlink",
                    "spaceTrack": {
                        **SAMPLE_STARLINK_DATA[0]["spaceTrack"],
                        "OBJECT_NAME": "STARLINK-5678",
                    },
                }
            ],
            "totalDocs": 2,
            "offset": 1,
            "limit": 1,
            "totalPages": 2,
            "page": 2,
            "pagingCounter": 2,
            "hasPrevPage": True,
            "hasNextPage": False,
            "prevPage": 1,
            "nextPage": None,
        }

        mock_get.side_effect = [first_page, second_page]

        # Execute fetch_starlink
        starlink_tap.fetch_starlink()

        # Verify both pages were processed
        assert mock_get.call_count == 2
        assert mock_write_record.call_count == 2

        # Verify records from both pages
        first_record = mock_write_record.call_args_list[0][1]["record"]
        second_record = mock_write_record.call_args_list[1][1]["record"]
        first_spacetrack = json.loads(first_record["SPACETRACK"])
        second_spacetrack = json.loads(second_record["SPACETRACK"])
        assert first_spacetrack["OBJECT_NAME"] == "STARLINK-1234"
        assert second_spacetrack["OBJECT_NAME"] == "STARLINK-5678"


def test_fetch_starlink_malformed_tle():
    """Test handling of malformed TLE data in spaceTrack"""
    malformed_tle_data = [
        {
            "id": "test_id",
            "spaceTrack": {
                "TLE_LINE0": "Invalid TLE Line 0",
                "TLE_LINE1": "Invalid TLE Line 1",
                "TLE_LINE2": "Invalid TLE Line 2",
                "EPOCH": "2024-01-01T10:00:00.000Z",
            },
        }
    ]

    with (
        patch("requests.get") as mock_get,
        patch("singer.write_schema"),
        patch("singer.write_record") as mock_write_record,
    ):
        # Mock API response
        mock_response = MagicMock()
        mock_response.json.return_value = malformed_tle_data
        mock_get.return_value = mock_response

        # Execute fetch_starlink
        starlink_tap.fetch_starlink()

        # Verify TLE handling
        record_call = mock_write_record.call_args[1]
        record = record_call["record"]
        spacetrack = json.loads(record["SPACETRACK"])
        assert spacetrack["TLE_LINE0"] == "Invalid TLE Line 0"
        assert spacetrack["TLE_LINE1"] == "Invalid TLE Line 1"
        assert spacetrack["TLE_LINE2"] == "Invalid TLE Line 2"
        # Verify other fields are still processed
        assert record["EPOCH"] == "2024-01-01T10:00:00.000Z"


def test_fetch_starlink_state_management():
    """Test state management for Starlink endpoint"""
    with (
        patch("requests.get") as mock_get,
        patch("singer.write_schema"),
        patch("singer.write_record"),
        patch("singer.write_state") as mock_write_state,
        patch.object(starlink_tap, "get_state") as mock_get_state,
    ):
        # Mock existing state with bookmark
        mock_get_state.return_value = {
            "bookmarks": {"starlink": {"last_record": "2024-01-01T00:00:00Z"}}
        }

        # Mock API response
        mock_response = MagicMock()
        mock_response.json.return_value = SAMPLE_STARLINK_DATA
        mock_get.return_value = mock_response

        # Execute fetch_starlink
        starlink_tap.fetch_starlink()

        # Verify state was updated
        mock_write_state.assert_called()
        new_state = mock_write_state.call_args[1]["state"]
        assert (
            new_state["bookmarks"]["starlink"]["last_record"] > "2024-01-01T00:00:00Z"
        )
