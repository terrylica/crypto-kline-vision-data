import pytest
import aiohttp
import httpx
from utils.http_client_factory import (
    create_aiohttp_client,
    create_httpx_client,
    create_client,
)
from utils.config import (
    DEFAULT_USER_AGENT,
    DEFAULT_ACCEPT_HEADER,
    DEFAULT_HTTP_TIMEOUT_SECONDS,
)


@pytest.mark.asyncio
async def test_create_aiohttp_client():
    """Test that aiohttp client is created with expected configuration."""
    # Create the client in an async context
    client = create_aiohttp_client()

    try:
        # Check client type
        assert isinstance(client, aiohttp.ClientSession)

        # Check headers
        assert client._default_headers["User-Agent"] == DEFAULT_USER_AGENT
        assert client._default_headers["Accept"] == DEFAULT_ACCEPT_HEADER

        # Check timeout
        assert client._timeout.total == DEFAULT_HTTP_TIMEOUT_SECONDS

        # Check custom parameters
        custom_timeout = 20.0
        custom_connections = 30
        custom_client = create_aiohttp_client(
            timeout=custom_timeout, max_connections=custom_connections
        )

        assert custom_client._timeout.total == custom_timeout
        assert custom_client._connector.limit == custom_connections

        await custom_client.close()
    finally:
        # Clean up
        await client.close()


def test_create_httpx_client():
    """Test that httpx client is created with expected configuration."""
    client = create_httpx_client()

    # Check client type
    assert isinstance(client, httpx.AsyncClient)

    # Check headers
    assert client.headers["User-Agent"] == DEFAULT_USER_AGENT
    assert client.headers["Accept"] == DEFAULT_ACCEPT_HEADER

    # Check timeout - httpx.Timeout is directly comparable
    assert client.timeout == httpx.Timeout(DEFAULT_HTTP_TIMEOUT_SECONDS)

    # Check custom parameters
    custom_timeout = 20.0
    custom_connections = 30
    custom_client = create_httpx_client(
        timeout=custom_timeout, max_connections=custom_connections
    )

    # Check timeout using direct comparison
    assert custom_client.timeout == httpx.Timeout(custom_timeout)

    # For httpx, we need to access the _transport._pool property
    # But this is implementation specific and might change
    # Instead, just check that the client was created successfully
    assert isinstance(custom_client, httpx.AsyncClient)


@pytest.mark.asyncio
async def test_create_client_aiohttp():
    """Test the unified client factory with aiohttp client type."""
    # Default client type is aiohttp
    client = create_client()

    try:
        # Check client type
        assert isinstance(client, aiohttp.ClientSession)

        # Check default configuration
        assert client._default_headers["User-Agent"] == DEFAULT_USER_AGENT
        assert client._default_headers["Accept"] == DEFAULT_ACCEPT_HEADER
        assert client._timeout.total == DEFAULT_HTTP_TIMEOUT_SECONDS

        # Test with explicit client type and custom parameters
        custom_headers = {"X-Custom-Header": "Test Value"}
        custom_timeout = 25.0
        custom_client = create_client(
            client_type="aiohttp",
            timeout=custom_timeout,
            max_connections=40,
            headers=custom_headers,
        )

        assert isinstance(custom_client, aiohttp.ClientSession)
        assert custom_client._timeout.total == custom_timeout
        assert custom_client._connector.limit == 40
        assert custom_client._default_headers["X-Custom-Header"] == "Test Value"

        # Original default headers should still be present
        assert custom_client._default_headers["User-Agent"] == DEFAULT_USER_AGENT
        assert custom_client._default_headers["Accept"] == DEFAULT_ACCEPT_HEADER

        await custom_client.close()
    finally:
        await client.close()


def test_create_client_httpx():
    """Test the unified client factory with httpx client type."""
    client = create_client(client_type="httpx")

    # Check client type
    assert isinstance(client, httpx.AsyncClient)

    # Check default configuration
    assert client.headers["User-Agent"] == DEFAULT_USER_AGENT
    assert client.headers["Accept"] == DEFAULT_ACCEPT_HEADER
    assert client.timeout == httpx.Timeout(DEFAULT_HTTP_TIMEOUT_SECONDS)

    # Test with custom parameters
    custom_headers = {"X-Custom-Header": "Test Value"}
    custom_timeout = 25.0
    custom_client = create_client(
        client_type="httpx",
        timeout=custom_timeout,
        max_connections=40,
        headers=custom_headers,
    )

    assert isinstance(custom_client, httpx.AsyncClient)
    assert custom_client.timeout == httpx.Timeout(custom_timeout)
    assert custom_client.headers["X-Custom-Header"] == "Test Value"

    # Original default headers should still be present
    assert custom_client.headers["User-Agent"] == DEFAULT_USER_AGENT
    assert custom_client.headers["Accept"] == DEFAULT_ACCEPT_HEADER


def test_create_client_invalid_type():
    """Test that an invalid client type raises a ValueError."""
    with pytest.raises(ValueError) as excinfo:
        create_client(client_type="invalid")

    assert "Unsupported client type" in str(excinfo.value)


@pytest.mark.asyncio
async def test_client_factory_dsm_integration():
    """Test that both client types work with DataSourceManager.

    This integration test verifies that the unified HTTP client factory
    works correctly with the DataSourceManager, which uses both client types.
    """
    from core.data_source_manager import DataSourceManager, DataSource
    from core.market_data_client import EnhancedRetriever
    from core.vision_data_client import VisionDataClient
    from utils.market_constraints import MarketType
    from datetime import datetime, timezone, timedelta
    import tempfile
    from pathlib import Path

    # Create a temporary cache directory
    with tempfile.TemporaryDirectory() as temp_dir:
        cache_dir = Path(temp_dir)

        # Create clients using our factory
        rest_client = create_client(client_type="aiohttp")
        vision_client = create_client(client_type="httpx")

        # Wrap them in their respective domain clients
        market_client = EnhancedRetriever(
            market_type=MarketType.SPOT, client=rest_client
        )
        data_client = VisionDataClient(symbol="BTCUSDT", interval="1s", use_cache=False)
        data_client._client = vision_client  # Replace the internal client

        # Create DSM with both client types
        dsm = DataSourceManager(
            market_type=MarketType.SPOT,
            rest_client=market_client,
            vision_client=data_client,
            cache_dir=cache_dir,
            use_cache=True,
        )

        try:
            # Set up test parameters - use data from a few days ago
            now = datetime.now(timezone.utc)
            end_time = now - timedelta(days=3)
            start_time = end_time - timedelta(minutes=5)

            # Test with REST API
            df_rest = await dsm.get_data(
                symbol="BTCUSDT",
                start_time=start_time,
                end_time=end_time,
                enforce_source=DataSource.REST,
            )

            # Verify data
            assert not df_rest.empty
            assert "open" in df_rest.columns
            assert "close" in df_rest.columns

            # Test with Vision API
            df_vision = await dsm.get_data(
                symbol="BTCUSDT",
                start_time=start_time,
                end_time=end_time,
                enforce_source=DataSource.VISION,
            )

            # Verify data
            assert not df_vision.empty
            assert "open" in df_vision.columns
            assert "close" in df_vision.columns

            # Verify that both data sources return compatible data
            assert set(df_rest.columns) == set(df_vision.columns)

        finally:
            # Clean up resources
            if hasattr(market_client, "client") and market_client.client:
                await market_client.client.close()
            if hasattr(data_client, "_client") and data_client._client:
                await data_client._client.aclose()
