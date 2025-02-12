import logging
import sys
import time
from pathlib import Path

# Add the parent directory to sys.path
sys.path.append(str(Path(__file__).parent.parent))

from include.fetch_capsules import CapsulesTap  # type: ignore
from include.fetch_company import CompanyTap  # type: ignore
from include.fetch_cores import CoresTap  # type: ignore
from include.fetch_crew import CrewTap  # type: ignore
from include.fetch_dragons import DragonsTap  # type: ignore
from include.fetch_history import HistoryTap  # type: ignore
from include.fetch_landpads import LandpadsTap  # type: ignore
from include.fetch_launches import LaunchesTap  # type: ignore
from include.fetch_launchpads import LaunchpadsTap  # type: ignore
from include.fetch_payloads import PayloadsTap  # type: ignore
from include.fetch_roadster import RoadsterTap  # type: ignore
from include.fetch_rockets import RocketsTap  # type: ignore
from include.fetch_ships import ShipsTap  # type: ignore
from include.fetch_starlink import StarlinkTap  # type: ignore
from include.spacex_tap_base import SpaceXTapBase  # type: ignore

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class SpaceXTapOrchestrator(SpaceXTapBase):
    """SpaceX tap orchestrator

    Args:
        SpaceXTapBase (class): SpaceX tap base class
    """

    def __init__(self, base_url: str, config_path: str):
        """Spacex tap orchestrator constructor"""
        super().__init__(base_url, config_path)
        self.config_path = config_path

    # First set of functions - Core data
    def fetch_company(self):
        """Set fetch_company function from CompanyTap class"""
        logger.info("Fetching company data")
        company_tap = CompanyTap(self.base_url, self.config_path)
        try:
            company_tap.fetch_company()
        finally:
            company_tap.close_connection()

    def fetch_capsules(self):
        """Set fetch_capsules function from CapsulesTap class"""
        logger.info("Fetching capsules data")
        capsules_tap = CapsulesTap(self.base_url, self.config_path)
        try:
            capsules_tap.fetch_capsules()
        finally:
            capsules_tap.close_connection()

    def fetch_cores(self):
        """Set fetch_cores function from CoresTap class"""
        logger.info("Fetching cores data")
        cores_tap = CoresTap(self.base_url, self.config_path)
        try:
            cores_tap.fetch_cores()
        finally:
            cores_tap.close_connection()

    def fetch_crew(self):
        """Set fetch_crew function from CrewTap class"""
        logger.info("Fetching crew data")
        crew_tap = CrewTap(self.base_url, self.config_path)
        try:
            crew_tap.fetch_crew()
        finally:
            crew_tap.close_connection()

    def fetch_dragons(self):
        """Set fetch_dragons function from DragonsTap class"""
        logger.info("Fetching dragons data")
        dragons_tap = DragonsTap(self.base_url, self.config_path)
        try:
            dragons_tap.fetch_dragons()
        finally:
            dragons_tap.close_connection()

    # Second set of functions - Mission and location data
    def fetch_landpads(self):
        """Set fetch_landpads function from LandpadsTap class"""
        logger.info("Fetching landpads data")
        landpads_tap = LandpadsTap(self.base_url, self.config_path)
        try:
            landpads_tap.fetch_landpads()
        finally:
            landpads_tap.close_connection()

    def fetch_launches(self):
        """Set fetch_launches function from LaunchesTap class"""
        logger.info("Fetching launches data")
        launches_tap = LaunchesTap(self.base_url, self.config_path)
        try:
            launches_tap.fetch_launches()
        finally:
            launches_tap.close_connection()

    def fetch_launchpads(self):
        """Set fetch_launchpads function from LaunchpadsTap class"""
        logger.info("Fetching launchpads data")
        launchpads_tap = LaunchpadsTap(self.base_url, self.config_path)
        try:
            launchpads_tap.fetch_launchpads()
        finally:
            launchpads_tap.close_connection()

    def fetch_history(self):
        """Set fetch_history function from HistoryTap class"""
        logger.info("Fetching history data")
        history_tap = HistoryTap(self.base_url, self.config_path)
        try:
            history_tap.fetch_history()
        finally:
            history_tap.close_connection()

    def fetch_payloads(self):
        """Set fetch_payloads function from PayloadsTap class"""
        logger.info("Fetching payloads data")
        payloads_tap = PayloadsTap(self.base_url, self.config_path)
        try:
            payloads_tap.fetch_payloads()
        finally:
            payloads_tap.close_connection()

    def fetch_roadsters(self):
        """Set fetch_roadsters function from RoadsterTap class"""
        logger.info("Fetching payloads data")
        roadsters_tap = RoadsterTap(self.base_url, self.config_path)
        try:
            roadsters_tap.fetch_roadster()
        finally:
            roadsters_tap.close_connection()

    def fetch_rockets(self):
        """Set fetch_rockets function from RocketsTap class"""
        logger.info("Fetching payloads data")
        rockets_tap = RocketsTap(self.base_url, self.config_path)
        try:
            rockets_tap.fetch_rockets()
        finally:
            rockets_tap.close_connection()

    # Third set of functions - Mission and location data
    def fetch_ships(self):
        """Set fetch_ships function from ShipsTap class"""
        logger.info("Fetching ships data")
        ships_tap = ShipsTap(self.base_url, self.config_path)
        try:
            ships_tap.fetch_ships()
        finally:
            ships_tap.close_connection()

    def fetch_starlink(self):
        """Set fetch_starlink function from StarlinkTap class"""
        logger.info("Fetching starlink data")
        starlink_tap = StarlinkTap(self.base_url, self.config_path)
        try:
            starlink_tap.fetch_starlink()
        finally:
            starlink_tap.close_connection()

    def run_first_set(self):
        """Run first set of the following entity functions.

        Functions list :
        - company
        - capsules
        - cores
        - crew
        - dragons
        """
        logger.info("Starting first set of functions...")

        first_set_functions = [
            self.fetch_company,
            self.fetch_capsules,
            self.fetch_cores,
            self.fetch_crew,
            self.fetch_dragons,
        ]

        for func in first_set_functions:
            try:
                func()
                logger.info(f"Successfully completed {func.__name__}")
            except Exception as e:
                logger.error(f"Error in {func.__name__}: {str(e)}")
                raise

        logger.info("Completed first set of functions")

    def run_second_set(self):
        """Run second set of the following entity functions.

        Functions list :
        - history
        - launches
        - launchpads
        - landpads
        - payloads
        """
        logger.info("Starting second set of functions...")

        second_set_functions = [
            self.fetch_history,
            self.fetch_launches,
            self.fetch_launchpads,
            self.fetch_landpads,
            self.fetch_payloads,
        ]

        for func in second_set_functions:
            try:
                func()
                logger.info(f"Successfully completed {func.__name__}")
            except Exception as e:
                logger.error(f"Error in {func.__name__}: {str(e)}")
                raise

        logger.info("Completed second set of functions")

    def run_third_set(self):
        """Run third set of the following entity functions.

        Functions list
        - roadsters
        - rockets
        - startlink
        - ships
        """
        logger.info("Starting second set of functions...")

        third_set_functions = [
            self.fetch_roadsters,
            self.fetch_rockets,
            self.fetch_starlink,
            self.fetch_ships,
        ]

        for func in third_set_functions:
            try:
                func()
                logger.info(f"Successfully completed {func.__name__}")
            except Exception as e:
                logger.error(f"Error in {func.__name__}: {str(e)}")
                raise

        logger.info("Completed second set of functions")


def main():
    """Main function in charge of running SpaceX tap orchestrator."""  # noqa D401
    BASE_URL = "https://api.spacexdata.com/v4/"
    CONFIG_PATH = "config_snowflake.json"

    try:
        # Initialize orchestrator
        orchestrator = SpaceXTapOrchestrator(BASE_URL, CONFIG_PATH)

        # Run first set
        logger.info("\n\n=============================================\n\n")
        logger.info("Starting execution of first set...")
        orchestrator.run_first_set()
        logger.info("First set completed successfully")

        # Sleep
        logger.info("\n\n=============================================\n\n")
        time.sleep(5)

        # Run second set
        logger.info("Starting execution of second set...")
        orchestrator.run_second_set()
        logger.info("Second set completed successfully")

        # Sleep
        logger.info("\n\n=============================================\n\n")
        time.sleep(5)

        # Run third set
        logger.info("Starting execution of third set...")
        orchestrator.run_third_set()
        logger.info("Second set completed successfully")

    except Exception as e:
        logger.error(f"Error in main execution: {str(e)}")
        raise

    finally:
        if orchestrator:
            orchestrator.close_connection()


if __name__ == "__main__":
    main()
