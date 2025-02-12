import json
from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest  # type: ignore

from singer_tap.include.fetch_ships import ShipsTap


@pytest.fixture
def ships_tap():
    """Instanciate ships_tap

    Args
        base_url (str) : The root url of v4 SpaceX API
        config_path (str) : Config file that contains database credentials

    Returns:
        ShipsTap: Class instance
    """
    return ShipsTap(
        base_url="https://api.spacexdata.com/v4/", config_path="config_snowflake.json"
    )


# Sample API response data
SAMPLE_SHIP_DATA = [
    {
        "id": "5ea6ed2e080df4000697c908",
        "name": "GO Ms Tree",
        "legacy_id": "GOMSTREE",
        "model": "Crew Boat",
        "type": "High Speed Craft",
        "active": False,
        "imo": 9744465,
        "mmsi": 368099550,
        "abs": 1258198,
        "class": 7785938,
        "mass_kg": 449964,
        "mass_lbs": 992000,
        "year_built": 2015,
        "home_port": "Port Canaveral",
        "status": "Inactive",
        "speed_kn": 0,
        "course_deg": None,
        "latitude": 28.4104,
        "longitude": -80.5999,
        "last_ais_update": "2023-01-10T19:49:31Z",
        "link": "https://www.marinetraffic.com/en/ais/details/ships/shipid:3439091",
        "image": "https://i.imgur.com/MtEgYbY.jpg",
        "launches": ["5eb87d46ffd86e000604b388"],
        "roles": ["Fairing Recovery"],
    }
]


def test_fetch_ships_successful():
    """Test successful ships data fetching and transformation"""
    with (
        patch("requests.get") as mock_get,
        patch("singer.write_schema") as mock_write_schema,
        patch("singer.write_record") as mock_write_record,
        patch("singer.write_state") as mock_write_state,
        patch.object(ships_tap, "get_current_time") as mock_time,
    ):
        # Mock current time
        mock_time.return_value = datetime(2024, 1, 1, 12, 0, 0)

        # Mock API response
        mock_response = MagicMock()
        mock_response.json.return_value = SAMPLE_SHIP_DATA
        mock_get.return_value = mock_response

        # Execute fetch_ships
        ships_tap.fetch_ships()

        # Verify API call
        mock_get.assert_called_once_with("https://api.spacexdata.com/v4/ships")

        # Verify schema write
        mock_write_schema.assert_called_once()
        schema_call = mock_write_schema.call_args[1]
        assert schema_call["stream_name"] == "SHIPS"
        assert schema_call["key_properties"] == ["SHIP_ID"]

        # Verify record write
        mock_write_record.assert_called_once()
        record_call = mock_write_record.call_args[1]
        record = record_call["record"]

        # Verify basic fields
        assert record["SHIP_ID"] == "5ea6ed2e080df4000697c908"
        assert record["NAME"] == "GO Ms Tree"
        assert record["MODEL"] == "Crew Boat"
        assert record["TYPE"] == "High Speed Craft"
        assert record["ACTIVE"] is False

        # Verify numeric fields
        assert record["IMO"] == 9744465
        assert record["MMSI"] == 368099550
        assert record["MASS_KG"] == 449964
        assert record["YEAR_BUILT"] == 2015
        assert record["SPEED_KN"] == 0
        assert record["LATITUDE"] == 28.4104
        assert record["LONGITUDE"] == -80.5999

        # Verify array fields are properly JSON encoded
        assert json.loads(record["LAUNCHES"]) == ["5eb87d46ffd86e000604b388"]
        assert json.loads(record["ROLES"]) == ["Fairing Recovery"]

        # Verify state write
        mock_write_state.assert_called_once()
        state_call = mock_write_state.call_args[1]
        assert "SHIPS" in state_call["state"]
        assert "last_sync" in state_call["state"]["SHIPS"]


def test_fetch_ships_api_error():
    """Test API error handling"""
    with patch("requests.get") as mock_get:
        # Mock API error
        mock_get.side_effect = Exception("API Error")

        # Execute and verify error handling
        with pytest.raises(Exception):
            ships_tap.fetch_ships()


def test_fetch_ships_schema_validation():
    """Test schema structure"""
    with (
        patch("requests.get") as mock_get,
        patch("singer.write_schema") as mock_write_schema,
    ):
        # Mock API response
        mock_response = MagicMock()
        mock_response.json.return_value = SAMPLE_SHIP_DATA
        mock_get.return_value = mock_response

        # Execute fetch_ships
        ships_tap.fetch_ships()

        # Verify schema structure
        schema_call = mock_write_schema.call_args[1]
        schema = schema_call["schema"]

        # Check required fields
        assert "SHIP_ID" in schema["properties"]
        assert "NAME" in schema["properties"]
        assert "MODEL" in schema["properties"]
        assert "TYPE" in schema["properties"]
        assert "ACTIVE" in schema["properties"]
        assert "IMO" in schema["properties"]
        assert "MMSI" in schema["properties"]
        assert "HOME_PORT" in schema["properties"]
        assert "LAUNCHES" in schema["properties"]
        assert "ROLES" in schema["properties"]

        # Check data types
        assert schema["properties"]["SHIP_ID"]["type"] == ["string", "null"]
        assert schema["properties"]["IMO"]["type"] == ["integer", "null"]
        assert schema["properties"]["ACTIVE"]["type"] == ["boolean", "null"]
        assert schema["properties"]["SPEED_KN"]["type"] == ["number", "null"]
        assert schema["properties"]["LAST_AIS_UPDATE"]["format"] == "date-time"


def test_fetch_ships_empty_response():
    """Test handling of empty ships data"""
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

        # Execute fetch_ships
        ships_tap.fetch_ships()

        # Verify schema was still written
        mock_write_schema.assert_called_once()

        # Verify no records were written
        mock_write_record.assert_not_called()

        # Verify state was still written
        mock_write_state.assert_called_once()


def test_fetch_ships_missing_fields():
    """Test handling of ships with missing fields"""
    incomplete_ship_data = [
        {
            "id": "test_id",
            "name": "Test Ship",
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
        mock_response.json.return_value = incomplete_ship_data
        mock_get.return_value = mock_response

        # Execute fetch_ships
        ships_tap.fetch_ships()

        # Verify record was written with None for missing fields
        record_call = mock_write_record.call_args[1]
        record = record_call["record"]
        assert record["SHIP_ID"] == "test_id"
        assert record["NAME"] == "Test Ship"
        assert record["MODEL"] is None
        assert record["TYPE"] is None
        assert record["ACTIVE"] is None
        assert record["IMO"] is None
        assert record["MMSI"] is None
        assert record["MASS_KG"] is None
        assert record["LAUNCHES"] == "[]"
        assert record["ROLES"] == "[]"


def test_fetch_ships_location_data():
    """Test handling of ship location data"""
    ship_with_location = [
        {
            "id": "test_id",
            "name": "Test Ship",
            "latitude": 28.4104,
            "longitude": -80.5999,
            "speed_kn": 15.5,
            "course_deg": 180.0,
            "last_ais_update": "2024-01-01T12:00:00Z",
        }
    ]

    with (
        patch("requests.get") as mock_get,
        # patch("singer.write_schema") as mock_write_schema,
        patch("singer.write_record") as mock_write_record,
    ):
        # Mock API response
        mock_response = MagicMock()
        mock_response.json.return_value = ship_with_location
        mock_get.return_value = mock_response

        # Execute fetch_ships
        ships_tap.fetch_ships()

        # Verify location data handling
        record_call = mock_write_record.call_args[1]
        record = record_call["record"]
        assert record["LATITUDE"] == 28.4104
        assert record["LONGITUDE"] == -80.5999
        assert record["SPEED_KN"] == 15.5
        assert record["COURSE_DEG"] == 180.0
        assert record["LAST_AIS_UPDATE"] == "2024-01-01T12:00:00Z"


def test_fetch_ships_rate_limit_handling():
    """Test rate limit handling for ships endpoint"""
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
        success_response.json.return_value = SAMPLE_SHIP_DATA

        mock_get.side_effect = [rate_limit_response, success_response]

        # Execute fetch_ships
        ships_tap.fetch_ships()

        # Verify retry behavior
        assert mock_get.call_count == 2
        assert mock_sleep.called
        mock_sleep.assert_called_once_with(2)


def test_fetch_ships_malformed_response():
    """Test handling of malformed ship data responses"""
    with patch("requests.get") as mock_get:
        # Test invalid JSON response
        invalid_json_response = MagicMock()
        invalid_json_response.status_code = 200
        invalid_json_response.json.side_effect = json.JSONDecodeError(
            "Invalid JSON", "", 0
        )
        mock_get.return_value = invalid_json_response

        with pytest.raises(ValueError) as exc_info:
            ships_tap.fetch_ships()
            assert "Invalid JSON response" in str(exc_info.value)

        # Test missing required fields
        incomplete_response = MagicMock()
        incomplete_response.status_code = 200
        incomplete_response.json.return_value = [
            {"name": "Test Ship"}
        ]  # Missing required 'id' field
        mock_get.return_value = incomplete_response

        with pytest.raises(ValueError) as exc_info:
            ships_tap.fetch_ships()
            assert "Missing required field" in str(exc_info.value)


def test_fetch_ships_state_management():
    """Test state management for ships endpoint"""
    with (
        patch("requests.get") as mock_get,
        patch("singer.write_schema"),
        patch("singer.write_record"),
        patch("singer.write_state") as mock_write_state,
        patch.object(ships_tap, "get_state") as mock_get_state,
        patch.object(ships_tap, "get_current_time") as mock_time,
    ):
        # Mock current time
        mock_time.return_value = datetime(2024, 1, 1, 12, 0, 0)

        # Mock existing state with bookmark
        mock_get_state.return_value = {
            "bookmarks": {"ships": {"last_record": "2024-01-01T00:00:00Z"}}
        }

        # Mock API response
        mock_response = MagicMock()
        mock_response.json.return_value = SAMPLE_SHIP_DATA
        mock_get.return_value = mock_response

        # Execute fetch_ships
        ships_tap.fetch_ships()

        # Verify state was updated
        mock_write_state.assert_called()
        new_state = mock_write_state.call_args[1]["state"]
        assert new_state["SHIPS"]["last_sync"] > "2024-01-01T00:00:00Z"


def test_fetch_ships_multiple_types():
    """Test handling of multiple ship types with different roles"""
    multiple_ships_data = [
        {
            "id": "ship1",
            "name": "Recovery Ship",
            "type": "Cargo",
            "active": True,
            "roles": ["Fairing Recovery", "Dragon Recovery"],
            "launches": ["launch1", "launch2"],
        },
        {
            "id": "ship2",
            "name": "Support Ship",
            "type": "Tug",
            "active": True,
            "roles": ["Support Ship"],
            "launches": ["launch3"],
        },
        {
            "id": "ship3",
            "name": "Drone Ship",
            "type": "Barge",
            "active": True,
            "roles": ["Booster Recovery"],
            "launches": [],
        },
    ]

    with (
        patch("requests.get") as mock_get,
        patch("singer.write_schema"),
        patch("singer.write_record") as mock_write_record,
    ):
        # Mock API response
        mock_response = MagicMock()
        mock_response.json.return_value = multiple_ships_data
        mock_get.return_value = mock_response

        # Execute fetch_ships
        ships_tap.fetch_ships()

        # Verify all ships were processed
        assert mock_write_record.call_count == 3

        # Verify each ship's data
        records = [call[1]["record"] for call in mock_write_record.call_args_list]

        # Check Recovery Ship
        assert records[0]["SHIP_ID"] == "ship1"
        assert records[0]["NAME"] == "Recovery Ship"
        assert records[0]["TYPE"] == "Cargo"
        assert records[0]["ACTIVE"] is True
        assert json.loads(records[0]["ROLES"]) == [
            "Fairing Recovery",
            "Dragon Recovery",
        ]
        assert json.loads(records[0]["LAUNCHES"]) == ["launch1", "launch2"]

        # Check Support Ship
        assert records[1]["SHIP_ID"] == "ship2"
        assert records[1]["NAME"] == "Support Ship"
        assert records[1]["TYPE"] == "Tug"
        assert json.loads(records[1]["ROLES"]) == ["Support Ship"]
        assert json.loads(records[1]["LAUNCHES"]) == ["launch3"]

        # Check Drone Ship
        assert records[2]["SHIP_ID"] == "ship3"
        assert records[2]["NAME"] == "Drone Ship"
        assert records[2]["TYPE"] == "Barge"
        assert json.loads(records[2]["ROLES"]) == ["Booster Recovery"]
        assert json.loads(records[2]["LAUNCHES"]) == []
