# Testing Best Practices

This directory contains all the tests for the Binance Data Services project. This document outlines the best practices and patterns to follow when creating or modifying tests.

## Test Directory Structure

The test directory is organized by module/component:

```tree
tests/
├── api_boundary/       # Tests for API boundary validation
├── cache_structure/    # Tests for cache system
├── data_sources/       # Tests for data source modules
├── intervals/          # Tests for time interval handling
├── market_constraints/ # Tests for market constraints
├── market_types/       # Tests for market types
├── network_utils/      # Tests for network utilities
├── time_boundary/      # Tests for time boundary logic
├── time_utils/         # Tests for time utilities
├── utils/              # Testing utilities and helpers
├── validation/         # Tests for validation modules
├── validation_utils/   # Legacy tests for validation_utils (being migrated)
└── vision_data_client/ # Tests for VisionDataClient
```

Each test directory should contain an `__init__.py` file with a docstring describing the tests in that directory.

## Running Tests

Always use the `scripts/op/run_tests_parallel.sh` script to run tests. This script provides several features:

- Parallel test execution (with `-n8` by default)
- Smart handling of tests marked with `@pytest.mark.serial`
- Proper event loop configuration for asyncio tests
- Error summaries and diagnostic tools

```bash
# Run all tests
scripts/op/run_tests_parallel.sh

# Run a specific test file or directory
scripts/op/run_tests_parallel.sh tests/utils/test_logging_parallel.py

# Run tests sequentially (for debugging)
scripts/op/run_tests_parallel.sh -s

# Run with increased log verbosity
scripts/op/run_tests_parallel.sh tests/ DEBUG
```

## Test Markers

We use the following pytest markers:

- `@pytest.mark.serial`: For tests that must run serially (not parallel with other tests)
- `@pytest.mark.asyncio`: For asynchronous tests (always use this for async functions)
- `@pytest.mark.real`: For tests that run against real data/resources
- `@pytest.mark.integration`: For tests that integrate with external services

Example:

```python
import pytest

# Regular synchronous test
def test_some_function():
    assert True

# Asynchronous test
@pytest.mark.asyncio
async def test_async_function():
    await some_async_function()
    assert True

# Test that should not run in parallel
@pytest.mark.serial
@pytest.mark.asyncio
async def test_intensive_operation():
    await resource_intensive_function()
    assert True
```

## Logging in Tests

For capturing and testing logs, use the `caplog_xdist_compatible` fixture included in `tests/utils/test_logging_parallel.py`. This fixture is compatible with parallel test execution and prevents KeyError issues with pytest-xdist.

```python
def test_with_logging(caplog_xdist_compatible):
    """Test with xdist-compatible logging capture."""
    caplog_xdist_compatible.set_level(logging.INFO)

    logger.info("Some log message")

    messages = [r.message for r in caplog_xdist_compatible.records]
    assert "Some log message" in messages
```

## Async Test Best Practices

1. **Always use function-scoped event loops**: Our configuration ensures each test gets a fresh event loop.
2. **Mark async tests with `@pytest.mark.asyncio`**: This ensures proper async test handling.

3. **Clean up resources**: Make sure to clean up any created resources in your tests, especially network connections.

4. **Use `@pytest.mark.serial` for resource-intensive async tests**: If a test needs exclusive access to resources, mark it as serial.

5. **Avoid creating global state**: Tests should be isolated and not depend on shared global state.

Example:

```python
import pytest
import asyncio
from utils.logger_setup import logger

@pytest.mark.asyncio
async def test_async_concurrent_tasks(caplog_xdist_compatible):
    """Test running concurrent async tasks."""
    caplog_xdist_compatible.set_level(logging.INFO)

    async def task(n):
        logger.info(f"Task {n} started")
        await asyncio.sleep(0.1)
        logger.info(f"Task {n} completed")
        return n

    # Run tasks concurrently
    results = await asyncio.gather(task(1), task(2), task(3))

    assert results == [1, 2, 3]
    assert len([msg for msg in caplog_xdist_compatible.records
                if "started" in msg.message]) == 3
```

## Network Requests in Tests

Use the `curl_cffi_client_with_cleanup` fixture for tests that make network requests:

```python
@pytest.mark.asyncio
async def test_api_client(curl_cffi_client_with_cleanup):
    client = curl_cffi_client_with_cleanup
    # Use the client for HTTP requests
    # The fixture will handle proper cleanup
```

## Additional Resources

For more details on testing with asyncio and pytest-xdist, see:

- Documentation in `docs/howto/pytest_asyncio_xdist_keyerror.md`
- Documentation in `docs/howto/pytest_logging_parallel.md`
- Example implementations in `playground/pytest_parallel_async_logging/`
