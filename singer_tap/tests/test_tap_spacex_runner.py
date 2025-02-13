import json
import os
import sys
from datetime import datetime
from typing import Dict, Generator
from unittest.mock import MagicMock, mock_open, patch

import pytest
import pytz

# Add parent directory to path to allow imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from runner.tap_spacex_runner import SpaceXTapOrchestrator


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
def orchestrator(
    mock_snowflake_connection: MagicMock,
    mock_datetime: MagicMock,
    sample_config: Dict[str, str],
) -> SpaceXTapOrchestrator:
    """Fixture providing a SpaceXTapOrchestrator instance"""
    mock_file = mock_open(read_data=json.dumps(sample_config))
    with patch("os.path.exists", return_value=True), patch("builtins.open", mock_file):
        tap = SpaceXTapOrchestrator(
            "https://api.spacexdata.com/v4/", "singer_tap/tests/config_test.json"
        )
        tap.conn = mock_snowflake_connection
        return tap


def test_initialization(orchestrator: SpaceXTapOrchestrator) -> None:
    """Test orchestrator initialization"""
    assert orchestrator.base_url == "https://api.spacexdata.com/v4/"
    assert isinstance(orchestrator.snowflake_config, dict)
    assert "user" in orchestrator.snowflake_config


def test_run_first_set(orchestrator: SpaceXTapOrchestrator) -> None:
    """Test running first set of functions"""
    # Mock all the fetch methods in the first set
    with patch.object(
        orchestrator, "fetch_company", MagicMock(__name__="fetch_company")
    ) as mock_company, patch.object(
        orchestrator, "fetch_capsules", MagicMock(__name__="fetch_capsules")
    ) as mock_capsules, patch.object(
        orchestrator, "fetch_cores", MagicMock(__name__="fetch_cores")
    ) as mock_cores, patch.object(
        orchestrator, "fetch_crew", MagicMock(__name__="fetch_crew")
    ) as mock_crew, patch.object(
        orchestrator, "fetch_dragons", MagicMock(__name__="fetch_dragons")
    ) as mock_dragons:
        orchestrator.run_first_set()

        # Verify all methods were called
        mock_company.assert_called_once()
        mock_capsules.assert_called_once()
        mock_cores.assert_called_once()
        mock_crew.assert_called_once()
        mock_dragons.assert_called_once()


def test_run_second_set(orchestrator: SpaceXTapOrchestrator) -> None:
    """Test running second set of functions"""
    # Mock all the fetch methods in the second set
    with patch.object(
        orchestrator, "fetch_history", MagicMock(__name__="fetch_history")
    ) as mock_history, patch.object(
        orchestrator, "fetch_launches", MagicMock(__name__="fetch_launches")
    ) as mock_launches, patch.object(
        orchestrator, "fetch_launchpads", MagicMock(__name__="fetch_launchpads")
    ) as mock_launchpads, patch.object(
        orchestrator, "fetch_landpads", MagicMock(__name__="fetch_landpads")
    ) as mock_landpads, patch.object(
        orchestrator, "fetch_payloads", MagicMock(__name__="fetch_payloads")
    ) as mock_payloads:
        orchestrator.run_second_set()

        # Verify all methods were called
        mock_history.assert_called_once()
        mock_launches.assert_called_once()
        mock_launchpads.assert_called_once()
        mock_landpads.assert_called_once()
        mock_payloads.assert_called_once()


def test_run_third_set(orchestrator: SpaceXTapOrchestrator) -> None:
    """Test running third set of functions"""
    # Mock all the fetch methods in the third set
    with patch.object(
        orchestrator, "fetch_roadsters", MagicMock(__name__="fetch_roadsters")
    ) as mock_roadsters, patch.object(
        orchestrator, "fetch_rockets", MagicMock(__name__="fetch_rockets")
    ) as mock_rockets, patch.object(
        orchestrator, "fetch_starlink", MagicMock(__name__="fetch_starlink")
    ) as mock_starlink, patch.object(
        orchestrator, "fetch_ships", MagicMock(__name__="fetch_ships")
    ) as mock_ships:
        orchestrator.run_third_set()

        # Verify all methods were called
        mock_roadsters.assert_called_once()
        mock_rockets.assert_called_once()
        mock_starlink.assert_called_once()
        mock_ships.assert_called_once()


def test_error_handling_in_first_set(orchestrator: SpaceXTapOrchestrator) -> None:
    """Test error handling in first set"""
    # Make fetch_company raise an exception
    with patch.object(
        orchestrator, "fetch_company", side_effect=Exception("Test error")
    ):
        with pytest.raises(Exception):
            orchestrator.run_first_set()


def test_error_handling_in_second_set(orchestrator: SpaceXTapOrchestrator) -> None:
    """Test error handling in second set"""
    # Make fetch_launches raise an exception
    with patch.object(
        orchestrator, "fetch_launches", side_effect=Exception("Test error")
    ):
        with pytest.raises(Exception):
            orchestrator.run_second_set()


def test_error_handling_in_third_set(orchestrator: SpaceXTapOrchestrator) -> None:
    """Test error handling in third set"""
    # Make fetch_rockets raise an exception
    with patch.object(
        orchestrator, "fetch_rockets", side_effect=Exception("Test error")
    ):
        with pytest.raises(Exception):
            orchestrator.run_third_set()


def test_main_function() -> None:
    """Test the main function"""
    mock_orchestrator = MagicMock()
    with patch(
        "runner.tap_spacex_runner.SpaceXTapOrchestrator", return_value=mock_orchestrator
    ), patch("os.path.exists", return_value=True):
        # Import and run main
        from runner.tap_spacex_runner import main

        main()

        # Verify all sets were run
        mock_orchestrator.run_first_set.assert_called_once()
        mock_orchestrator.run_second_set.assert_called_once()
        mock_orchestrator.run_third_set.assert_called_once()
        mock_orchestrator.close_connection.assert_called_once()


def test_main_function_error_handling() -> None:
    """Test error handling in main function"""
    mock_orchestrator = MagicMock()
    mock_orchestrator.run_first_set.side_effect = Exception("Test error")

    with patch(
        "runner.tap_spacex_runner.SpaceXTapOrchestrator", return_value=mock_orchestrator
    ), patch("os.path.exists", return_value=True):
        # Import and run main
        from runner.tap_spacex_runner import main

        with pytest.raises(Exception):
            main()

        # Verify close_connection was still called
        mock_orchestrator.close_connection.assert_called_once()
