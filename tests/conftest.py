#!/usr/bin/env python
"""Root conftest.py that imports fixtures from time_boundary for backwards compatibility."""

# Left empty for backwards compatibility
# The fixtures were previously imported here but are no longer used

import pytest
from curl_cffi.requests import AsyncSession
from utils.network_utils import safely_close_client


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
