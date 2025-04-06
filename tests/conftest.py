#!/usr/bin/env python
"""Root conftest.py that provides fixtures for the test suite.

This file contains:
1. Fixtures for handling network clients with proper cleanup
2. Enhanced caplog fixture for pytest-xdist compatibility
3. Asyncio configuration for proper event loop management
4. Unified logging abstractions for parallel testing
"""

# Left empty for backwards compatibility
# The fixtures were previously imported here but are no longer used

import pytest
from curl_cffi.requests import AsyncSession
from utils.network_utils import safely_close_client

# Import our unified logging abstractions
from tests.utils.unified_logging import (
    UnifiedLogCapture,
    configure_root_logger_for_testing,
)

# Import fixtures to make them available to all tests
# These imports are needed for pytest to discover the fixtures
# Note: The imports must be explicit to ensure the fixtures are registered


# Set global asyncio configuration via pytest configuration
# This is the recommended approach for asyncio loop management
def pytest_configure(config):
    """Configure pytest with asyncio settings and custom markers."""
    # Add the serial marker for tests that should run sequentially
    config.addinivalue_line(
        "markers", "serial: mark test to run serially (non-parallel)"
    )

    # Register the existing markers properly
    config.addinivalue_line(
        "markers",
        "real: mark tests that run against real data/resources rather than mocks",
    )
    config.addinivalue_line(
        "markers", "integration: mark tests that integrate with external services"
    )

    # Set asyncio_default_fixture_loop_scope to function scope for all tests
    # This is critical for avoiding KeyError issues with pytest-xdist
    config.option.asyncio_default_fixture_loop_scope = "function"

    # Configure root logger to ensure all log levels pass through
    configure_root_logger_for_testing()


@pytest.fixture
async def curl_cffi_client_with_cleanup():
    """Create a curl_cffi client with proper cleanup of pending tasks.

    This fixture ensures that AsyncCurl's internal timeout tasks are properly handled
    by using the safely_close_client function which handles pending tasks properly.
    """
    client = AsyncSession()
    yield client
    # Use the enhanced safely_close_client function instead of directly closing
    await safely_close_client(client)


@pytest.fixture
def caplog():
    """Enhanced caplog fixture compatible with pytest-xdist.

    This fixture provides a drop-in replacement for pytest's standard caplog fixture
    that works reliably with pytest-xdist parallel testing.

    This implementation uses our unified log capture class which is designed to work
    in all testing environments, including pytest-xdist parallel execution.

    Yields:
        UnifiedLogCapture: A caplog-compatible object for capturing and inspecting logs
    """
    # Configure the root logger to ensure DEBUG logs are captured
    configure_root_logger_for_testing()

    # Create and yield our unified log capture implementation
    capture = UnifiedLogCapture()
    try:
        yield capture
    finally:
        capture.cleanup()


# Re-export the unified fixtures from unified_logging.py to make them globally available
# This ensures backward compatibility while encouraging the use of the new unified approach
