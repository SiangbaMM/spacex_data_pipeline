from datetime import datetime

import pytest  # type: ignore


@pytest.fixture
def mock_current_time():
    """Fixture providing a fixed datetime for testing"""
    return datetime(2024, 1, 1, 12, 0, 0)


@pytest.fixture
def sample_api_error():
    """Fixture providing a sample API error"""

    class MockApiError(Exception):
        def __init__(self):
            self.response = type(
                "Response", (), {"status_code": 500, "text": "Internal Server Error"}
            )

    return MockApiError()


@pytest.fixture
def sample_config():
    """Fixture providing sample configuration"""
    return {
        "start_date": "2024-01-01T00:00:00Z",
        "api_token": "test_token",
        "user_agent": "SpaceX-Tap/1.0",
    }


@pytest.fixture
def mock_singer():
    """Fixture providing mocked singer functions"""

    class MockSinger:
        def write_schema(self, *args, **kwargs):
            pass

        def write_record(self, *args, **kwargs):
            pass

        def write_state(self, *args, **kwargs):
            pass

    return MockSinger()


@pytest.fixture
def mock_response():
    """Fixture providing a mock requests response"""

    class MockResponse:
        def __init__(self, json_data, status_code=200):
            self.json_data = json_data
            self.status_code = status_code

        def json(self):
            return self.json_data

        def raise_for_status(self):
            if self.status_code != 200:
                raise Exception(f"HTTP Status: {self.status_code}")

    return MockResponse
