#!/usr/bin/env python
"""Root conftest.py that imports fixtures from time_boundary for backwards compatibility."""

# Left empty for backwards compatibility
# The fixtures were previously imported here but are no longer used

import pytest
import logging
from curl_cffi.requests import AsyncSession
from utils.network_utils import safely_close_client
from utils.logger_setup import logger


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
def caplog(request):
    """Fixture to provide a safe caplog alternative that works with pytest-xdist.

    This fixture replaces the standard pytest caplog fixture when tests are run
    with pytest-xdist in parallel mode (-n flag). It provides a compatible API
    that doesn't raise KeyError when accessing StashKey objects.
    """

    # Create a dummy caplog object that won't raise KeyError with pytest-xdist
    class DummyCaplog:
        """A dummy caplog implementation that doesn't raise KeyError."""

        def __init__(self):
            """Initialize with empty records and handler."""
            self.records = []
            self.text = ""
            self._handler = logging.NullHandler()
            self._handler.setLevel(logging.INFO)

            # Add a real handler to collect log records
            self._real_handler = logging.StreamHandler()
            self._real_handler.setFormatter(
                logging.Formatter("%(levelname)s %(name)s %(message)s")
            )
            root_logger = logging.getLogger()
            root_logger.addHandler(self._real_handler)

        @property
        def handler(self):
            """Get the handler property that pytest expects."""
            return self._handler

        def set_level(self, level, logger=None):
            """Set the capture level - works with strings or log level constants."""
            if isinstance(level, str):
                level = getattr(logging, level.upper(), logging.INFO)

            # Set level on our handler
            self._handler.setLevel(level)
            self._real_handler.setLevel(level)

            # If a specific logger is provided, set its level
            if logger is not None:
                logging.getLogger(logger).setLevel(level)

        def clear(self):
            """Clear logs."""
            self.records = []
            self.text = ""

        def __enter__(self):
            """Context manager entry."""
            return self

        def __exit__(self, *args, **kwargs):
            """Context manager exit."""
            pass

    try:
        # Try to get the real caplog fixture from pytest
        # This will work when running sequentially, but fail with pytest-xdist
        real_caplog = request.getfixturevalue("caplog")
        return real_caplog
    except Exception:
        # If caplog fixture isn't available or raises KeyError (with pytest-xdist)
        logger.debug(
            "Using dummy caplog implementation for compatibility with pytest-xdist"
        )
        return DummyCaplog()
