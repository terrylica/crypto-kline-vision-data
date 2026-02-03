"""Unit tests for RestDataClient HTTP operations.

Tests cover:
- Successful fetch with mocked HTTP response
- 403 Forbidden handling
- 429 Rate Limit handling (RateLimitError raised)
- Timeout handling
- JSON decode error handling
- Empty response handling
"""

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from data_source_manager.core.providers.binance.rest_data_client import RestDataClient
from data_source_manager.utils.for_core.rest_exceptions import (
    HTTPError,
    JSONDecodeError,
    RateLimitError,
)
from data_source_manager.utils.market_constraints import Interval, MarketType


class TestRestDataClientInitialization:
    """Tests for RestDataClient initialization."""

    def test_init_spot_market(self):
        """Verify RestDataClient initializes correctly for spot market."""
        client = RestDataClient(market_type=MarketType.SPOT)
        assert client.market_type == MarketType.SPOT
        assert "api.binance.com" in client.base_url
        assert "/api/v3/klines" in client._endpoint

    def test_init_futures_usdt_market(self):
        """Verify RestDataClient initializes correctly for USDT futures."""
        client = RestDataClient(market_type=MarketType.FUTURES_USDT)
        assert client.market_type == MarketType.FUTURES_USDT
        assert "fapi.binance.com" in client.base_url
        assert "/fapi/v1/klines" in client._endpoint

    def test_init_futures_coin_market(self):
        """Verify RestDataClient initializes correctly for coin-margined futures."""
        client = RestDataClient(market_type=MarketType.FUTURES_COIN)
        assert client.market_type == MarketType.FUTURES_COIN
        assert "dapi.binance.com" in client.base_url
        assert "/dapi/v1/klines" in client._endpoint

    def test_init_with_custom_params(self):
        """Verify RestDataClient accepts custom parameters."""
        client = RestDataClient(
            market_type=MarketType.SPOT,
            retry_count=5,
            fetch_timeout=60.0,
            symbol="ETHUSDT",
            interval=Interval.HOUR_1,
        )
        assert client.retry_count == 5
        assert client.fetch_timeout == 60.0
        assert client._symbol == "ETHUSDT"
        assert client._interval == Interval.HOUR_1


class TestRestDataClientContextManager:
    """Tests for RestDataClient context manager protocol."""

    def test_context_manager_creates_client(self):
        """Verify context manager creates HTTP client on enter."""
        rest_client = RestDataClient(market_type=MarketType.SPOT)
        assert rest_client._client is None

        with rest_client as ctx:
            assert ctx._client is not None
            assert ctx is rest_client

    def test_context_manager_closes_client(self):
        """Verify context manager closes HTTP client on exit."""
        rest_client = RestDataClient(market_type=MarketType.SPOT)

        with rest_client:
            assert rest_client._client is not None

        # After exit, client should be None
        assert rest_client._client is None


class TestRestDataClientFetch:
    """Tests for RestDataClient.fetch() method."""

    @pytest.fixture
    def mock_http_client(self):
        """Create a mock HTTP client for testing."""
        return MagicMock()

    @pytest.fixture
    def sample_kline_response(self):
        """Sample kline data from Binance API.

        Format: [open_time, open, high, low, close, volume, close_time,
                 quote_asset_volume, number_of_trades, taker_buy_base,
                 taker_buy_quote, ignore]
        """
        base_time = 1704067200000  # 2024-01-01 00:00:00 UTC
        return [
            [
                base_time,
                "42000.00",
                "42500.00",
                "41800.00",
                "42200.00",
                "100.5",
                base_time + 3599999,
                "4220000.00",
                1500,
                "60.3",
                "2532600.00",
                "0",
            ],
            [
                base_time + 3600000,
                "42200.00",
                "42800.00",
                "42100.00",
                "42600.00",
                "150.2",
                base_time + 7199999,
                "6390000.00",
                2000,
                "80.1",
                "3412260.00",
                "0",
            ],
        ]

    @patch("data_source_manager.core.providers.binance.rest_data_client.fetch_chunk")
    @patch("data_source_manager.core.providers.binance.rest_data_client.create_optimized_client")
    def test_successful_fetch_returns_dataframe(
        self,
        mock_create_client,
        mock_fetch_chunk,
        sample_kline_response,
    ):
        """Verify successful fetch returns properly formatted DataFrame."""
        # Setup mocks
        mock_client = MagicMock()
        mock_create_client.return_value = mock_client
        mock_fetch_chunk.return_value = sample_kline_response

        # Create client and fetch data
        rest_client = RestDataClient(market_type=MarketType.SPOT)

        end_time = datetime(2024, 1, 1, 2, 0, 0, tzinfo=timezone.utc)
        start_time = end_time - timedelta(hours=2)

        with rest_client:
            df = rest_client.fetch(
                symbol="BTCUSDT",
                interval="1h",
                start_time=start_time,
                end_time=end_time,
            )

        # Verify DataFrame structure
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 2
        assert "open" in df.columns
        assert "high" in df.columns
        assert "low" in df.columns
        assert "close" in df.columns
        assert "volume" in df.columns

    @patch("data_source_manager.core.providers.binance.rest_data_client.fetch_chunk")
    @patch("data_source_manager.core.providers.binance.rest_data_client.create_optimized_client")
    def test_empty_response_returns_empty_dataframe(
        self,
        mock_create_client,
        mock_fetch_chunk,
    ):
        """Verify empty API response returns empty DataFrame."""
        mock_client = MagicMock()
        mock_create_client.return_value = mock_client
        mock_fetch_chunk.return_value = []

        rest_client = RestDataClient(market_type=MarketType.SPOT)

        end_time = datetime(2024, 1, 1, 2, 0, 0, tzinfo=timezone.utc)
        start_time = end_time - timedelta(hours=2)

        with rest_client:
            df = rest_client.fetch(
                symbol="BTCUSDT",
                interval="1h",
                start_time=start_time,
                end_time=end_time,
            )

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 0


class TestRestDataClientErrorHandling:
    """Tests for RestDataClient error handling."""

    @patch("data_source_manager.core.providers.binance.rest_data_client.fetch_chunk")
    @patch("data_source_manager.core.providers.binance.rest_data_client.create_optimized_client")
    def test_rate_limit_error_propagates(
        self,
        mock_create_client,
        mock_fetch_chunk,
    ):
        """Verify RateLimitError (429) propagates to caller."""
        mock_client = MagicMock()
        mock_create_client.return_value = mock_client
        mock_fetch_chunk.side_effect = RateLimitError("429 Too Many Requests")

        rest_client = RestDataClient(market_type=MarketType.SPOT)

        end_time = datetime(2024, 1, 1, 2, 0, 0, tzinfo=timezone.utc)
        start_time = end_time - timedelta(hours=2)

        with pytest.raises(RateLimitError):
            with rest_client:
                rest_client.fetch(
                    symbol="BTCUSDT",
                    interval="1h",
                    start_time=start_time,
                    end_time=end_time,
                )

    @patch("data_source_manager.core.providers.binance.rest_data_client.fetch_chunk")
    @patch("data_source_manager.core.providers.binance.rest_data_client.create_optimized_client")
    def test_http_403_returns_empty_dataframe(
        self,
        mock_create_client,
        mock_fetch_chunk,
    ):
        """Verify 403 Forbidden returns empty DataFrame (non-fatal error)."""
        mock_client = MagicMock()
        mock_create_client.return_value = mock_client
        mock_fetch_chunk.side_effect = HTTPError("403 Forbidden")

        rest_client = RestDataClient(market_type=MarketType.SPOT)

        end_time = datetime(2024, 1, 1, 2, 0, 0, tzinfo=timezone.utc)
        start_time = end_time - timedelta(hours=2)

        with rest_client:
            df = rest_client.fetch(
                symbol="BTCUSDT",
                interval="1h",
                start_time=start_time,
                end_time=end_time,
            )

        # HTTP errors are treated as transient - return empty DataFrame
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 0

    @patch("data_source_manager.core.providers.binance.rest_data_client.fetch_chunk")
    @patch("data_source_manager.core.providers.binance.rest_data_client.create_optimized_client")
    def test_json_decode_error_returns_empty_dataframe(
        self,
        mock_create_client,
        mock_fetch_chunk,
    ):
        """Verify JSON decode error returns empty DataFrame."""
        mock_client = MagicMock()
        mock_create_client.return_value = mock_client
        mock_fetch_chunk.side_effect = JSONDecodeError("Invalid JSON response")

        rest_client = RestDataClient(market_type=MarketType.SPOT)

        end_time = datetime(2024, 1, 1, 2, 0, 0, tzinfo=timezone.utc)
        start_time = end_time - timedelta(hours=2)

        with rest_client:
            df = rest_client.fetch(
                symbol="BTCUSDT",
                interval="1h",
                start_time=start_time,
                end_time=end_time,
            )

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 0

    @patch("data_source_manager.core.providers.binance.rest_data_client.fetch_chunk")
    @patch("data_source_manager.core.providers.binance.rest_data_client.create_optimized_client")
    def test_timeout_error_returns_empty_dataframe(
        self,
        mock_create_client,
        mock_fetch_chunk,
    ):
        """Verify timeout error returns empty DataFrame."""
        mock_client = MagicMock()
        mock_create_client.return_value = mock_client
        mock_fetch_chunk.side_effect = TimeoutError("Request timed out")

        rest_client = RestDataClient(market_type=MarketType.SPOT)

        end_time = datetime(2024, 1, 1, 2, 0, 0, tzinfo=timezone.utc)
        start_time = end_time - timedelta(hours=2)

        with rest_client:
            df = rest_client.fetch(
                symbol="BTCUSDT",
                interval="1h",
                start_time=start_time,
                end_time=end_time,
            )

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 0


class TestRestDataClientChunking:
    """Tests for RestDataClient chunk calculation."""

    def test_calculate_chunks_small_range(self):
        """Verify chunk calculation for small time ranges."""
        rest_client = RestDataClient(market_type=MarketType.SPOT)

        # 1 hour at 1-minute intervals = 60 data points (fits in one chunk)
        start_ms = 1704067200000  # 2024-01-01 00:00:00 UTC
        end_ms = start_ms + 3600000  # +1 hour

        chunks = rest_client._calculate_chunks(start_ms, end_ms, Interval.MINUTE_1)

        assert len(chunks) >= 1
        assert chunks[0][0] == start_ms

    def test_calculate_chunks_large_range(self):
        """Verify chunk calculation for large time ranges."""
        rest_client = RestDataClient(market_type=MarketType.SPOT)

        # 7 days at 1-minute intervals = 10080 data points (multiple chunks)
        start_ms = 1704067200000  # 2024-01-01 00:00:00 UTC
        end_ms = start_ms + (7 * 24 * 3600 * 1000)  # +7 days

        chunks = rest_client._calculate_chunks(start_ms, end_ms, Interval.MINUTE_1)

        # Should have multiple chunks
        assert len(chunks) > 1

        # First chunk should start at start_ms
        assert chunks[0][0] == start_ms

        # Last chunk should end at or after end_ms
        assert chunks[-1][1] >= end_ms


class TestRestDataClientInputValidation:
    """Tests for RestDataClient input validation."""

    def test_invalid_time_range_raises_error(self):
        """Verify invalid time range (start > end) raises ValueError."""
        rest_client = RestDataClient(market_type=MarketType.SPOT)

        end_time = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        start_time = end_time + timedelta(hours=1)  # Start is after end

        with pytest.raises(ValueError):
            with rest_client:
                rest_client.fetch(
                    symbol="BTCUSDT",
                    interval="1h",
                    start_time=start_time,
                    end_time=end_time,
                )

    @patch("data_source_manager.core.providers.binance.rest_data_client.fetch_chunk")
    @patch("data_source_manager.core.providers.binance.rest_data_client.create_optimized_client")
    def test_interval_string_parsed_correctly(
        self,
        mock_create_client,
        mock_fetch_chunk,
    ):
        """Verify interval string is parsed to Interval enum."""
        mock_client = MagicMock()
        mock_create_client.return_value = mock_client
        mock_fetch_chunk.return_value = []

        rest_client = RestDataClient(market_type=MarketType.SPOT)

        end_time = datetime(2024, 1, 1, 2, 0, 0, tzinfo=timezone.utc)
        start_time = end_time - timedelta(hours=2)

        with rest_client:
            # Should accept string interval
            df = rest_client.fetch(
                symbol="BTCUSDT",
                interval="1h",  # String
                start_time=start_time,
                end_time=end_time,
            )

        assert isinstance(df, pd.DataFrame)

    @patch("data_source_manager.core.providers.binance.rest_data_client.fetch_chunk")
    @patch("data_source_manager.core.providers.binance.rest_data_client.create_optimized_client")
    def test_interval_enum_accepted(
        self,
        mock_create_client,
        mock_fetch_chunk,
    ):
        """Verify Interval enum is accepted directly."""
        mock_client = MagicMock()
        mock_create_client.return_value = mock_client
        mock_fetch_chunk.return_value = []

        rest_client = RestDataClient(market_type=MarketType.SPOT)

        end_time = datetime(2024, 1, 1, 2, 0, 0, tzinfo=timezone.utc)
        start_time = end_time - timedelta(hours=2)

        with rest_client:
            # Should accept Interval enum directly
            df = rest_client.fetch(
                symbol="BTCUSDT",
                interval=Interval.HOUR_1,  # Enum
                start_time=start_time,
                end_time=end_time,
            )

        assert isinstance(df, pd.DataFrame)
