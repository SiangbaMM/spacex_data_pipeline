# SpaceX Singer Tap Tests

This directory contains pytest tests for the SpaceX Singer tap components.

## Test Structure

- `conftest.py`: Shared pytest fixtures for all test files
- `test_spacex_tap_base.py`: Tests for the base tap functionality
- `test_fetch_capsules.py`: Tests for the capsules tap implementation
- Additional test files for other tap components (crew, launches, etc.)

## Setup

1. Install development dependencies:

```bash
pip install -r requirements.txt
```

2. Ensure you're in the singer_tap directory:

```bash
cd singer_tap
```

## Running Tests

### Run all tests:

```bash
pytest tests/
```

### Run with coverage report:

```bash
pytest --cov=include tests/ --cov-report=term-missing
```

### Run specific test file:

```bash
pytest tests/test_fetch_capsules.py
```

### Run tests in parallel:

```bash
pytest -n auto tests/
```

### Run with verbose output:

```bash
pytest -v tests/
```

## Test Categories

The test suite covers several key areas:

1. **Base Functionality Tests**

   - URL handling
   - Error logging
   - Time utilities
   - Configuration management

2. **API Integration Tests**

   - Request handling
   - Response parsing
   - Error handling
   - Rate limiting

3. **Data Transformation Tests**

   - Schema validation
   - Data type conversion
   - Field mapping
   - Null handling

4. **Singer Protocol Tests**
   - Schema writing
   - Record writing
   - State management
   - Bookmark handling

## Writing New Tests

When adding new tests:

1. Follow the existing pattern of using fixtures from `conftest.py`
2. Mock external dependencies (API calls, file operations)
3. Test both success and error cases
4. Include docstrings explaining test purpose
5. Verify schema compliance for new tap implementations

## Code Quality

The test suite includes configuration for:

- Black for code formatting
- Flake8 for style checking
- MyPy for type checking

Run quality checks:

```bash
black include tests
flake8 include tests
mypy include tests
```
