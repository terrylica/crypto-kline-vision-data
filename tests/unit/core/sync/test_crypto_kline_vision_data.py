"""Unit tests for CryptoKlineVisionData.get_data() core path.

Tests the Failover Control Protocol (FCP) orchestration logic:
1. Initialization tests
2. Input validation
3. Funding rate routing

ADR: docs/adr/2025-01-30-failover-control-protocol.md
"""

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from ckvd import DataProvider, CryptoKlineVisionData, Interval, MarketType


# =============================================================================
# Test Data Fixtures
# =============================================================================


@pytest.fixture
def sample_ohlcv_df():
    """Create a sample OHLCV DataFrame for testing with proper structure."""
    base_time = datetime(2024, 1, 15, 0, 0, 0, tzinfo=timezone.utc)
    timestamps = [base_time + timedelta(hours=i) for i in range(24)]
    return pd.DataFrame(
        {
            "open": [42000.0 + i * 10 for i in range(24)],
            "high": [42100.0 + i * 10 for i in range(24)],
            "low": [41900.0 + i * 10 for i in range(24)],
            "close": [42050.0 + i * 10 for i in range(24)],
            "volume": [1000.0 + i for i in range(24)],
        },
        index=pd.DatetimeIndex(timestamps, name="open_time", tz="UTC"),
    )


@pytest.fixture
def historical_time_range():
    """Historical time range for tests (8 days ago to 1 day ago, safe for Vision API)."""
    end = datetime(2024, 1, 15, 23, 0, 0, tzinfo=timezone.utc)
    start = datetime(2024, 1, 15, 0, 0, 0, tzinfo=timezone.utc)
    return start, end


# =============================================================================
# Initialization Tests (Constructor-level mocks)
# =============================================================================


@patch("ckvd.core.sync.crypto_kline_vision_data.get_provider_clients")
class TestCryptoKlineVisionDataInitialization:
    """Tests for CryptoKlineVisionData initialization."""

    def _create_mock_clients(self, provider=DataProvider.BINANCE, market_type=MarketType.SPOT):
        """Helper to create mock ProviderClients."""
        from ckvd.core.providers import ProviderClients

        return ProviderClients(
            vision=MagicMock(),
            rest=MagicMock(),
            cache=MagicMock(),
            provider=provider,
            market_type=market_type,
        )

    def test_create_manager_spot(self, mock_get_clients):
        """Verify manager creation for spot market."""
        mock_get_clients.return_value = self._create_mock_clients(DataProvider.BINANCE, MarketType.SPOT)

        manager = CryptoKlineVisionData.create(DataProvider.BINANCE, MarketType.SPOT)

        assert manager is not None
        assert manager.market_type == MarketType.SPOT
        assert manager.provider == DataProvider.BINANCE
        manager.close()

    def test_create_manager_futures_usdt(self, mock_get_clients):
        """Verify manager creation for USDT-margined futures."""
        mock_get_clients.return_value = self._create_mock_clients(DataProvider.BINANCE, MarketType.FUTURES_USDT)

        manager = CryptoKlineVisionData.create(DataProvider.BINANCE, MarketType.FUTURES_USDT)

        assert manager is not None
        assert manager.market_type == MarketType.FUTURES_USDT
        manager.close()

    def test_create_manager_futures_coin(self, mock_get_clients):
        """Verify manager creation for coin-margined futures."""
        mock_get_clients.return_value = self._create_mock_clients(DataProvider.BINANCE, MarketType.FUTURES_COIN)

        manager = CryptoKlineVisionData.create(DataProvider.BINANCE, MarketType.FUTURES_COIN)

        assert manager is not None
        assert manager.market_type == MarketType.FUTURES_COIN
        manager.close()

    def test_create_manager_with_cache_disabled(self, mock_get_clients):
        """Verify manager creation with cache disabled."""
        mock_get_clients.return_value = self._create_mock_clients()

        manager = CryptoKlineVisionData.create(
            DataProvider.BINANCE,
            MarketType.SPOT,
            use_cache=False,
        )

        assert manager is not None
        assert manager.use_cache is False
        manager.close()


class TestInputValidation:
    """Tests for input validation."""

    def test_invalid_time_range_raises_error(self):
        """start_time >= end_time should raise RuntimeError (wrapping ValueError)."""
        manager = CryptoKlineVisionData.create(DataProvider.BINANCE, MarketType.SPOT)
        now = datetime.now(timezone.utc)

        # ValueError is wrapped in RuntimeError by the error handler
        with pytest.raises(RuntimeError):
            manager.get_data(
                symbol="BTCUSDT",
                start_time=now,
                end_time=now - timedelta(days=1),
                interval=Interval.HOUR_1,
            )

        manager.close()

    def test_manager_creation_succeeds(self):
        """Manager creation should succeed with valid parameters."""
        manager = CryptoKlineVisionData.create(DataProvider.BINANCE, MarketType.SPOT)
        assert manager is not None
        assert manager.provider == DataProvider.BINANCE
        assert manager.market_type == MarketType.SPOT
        manager.close()


# =============================================================================
# Funding Rate Routing Tests
# =============================================================================


class TestFundingRateRouting:
    """Tests for ChartType.FUNDING_RATE routing to _fetch_funding_rate()."""

    @patch("ckvd.core.sync.crypto_kline_vision_data.BinanceFundingRateClient")
    def test_funding_rate_routes_to_dedicated_method(
        self,
        mock_funding_client,
        historical_time_range,
    ):
        """get_data with ChartType.FUNDING_RATE should route to _fetch_funding_rate."""
        from ckvd.utils.market_constraints import ChartType

        # Arrange
        start_time, end_time = historical_time_range

        # Mock funding rate client
        mock_client_instance = MagicMock()
        mock_client_instance.fetch.return_value = pd.DataFrame(
            {
                "symbol": ["BTCUSDT"] * 3,
                "funding_time": [start_time + timedelta(hours=i * 8) for i in range(3)],
                "funding_rate": [0.0001, 0.0002, 0.00015],
            }
        )
        mock_funding_client.return_value = mock_client_instance

        manager = CryptoKlineVisionData.create(
            DataProvider.BINANCE,
            MarketType.FUTURES_USDT,
        )

        # Act
        df = manager.get_data(
            symbol="BTCUSDT",
            start_time=start_time,
            end_time=end_time,
            interval=Interval.HOUR_8,
            chart_type=ChartType.FUNDING_RATE,
        )

        # Assert - BinanceFundingRateClient should have been called
        assert mock_funding_client.called
        assert mock_client_instance.fetch.called
        assert len(df) == 3
        manager.close()

    def test_funding_rate_invalid_market_type_raises_error(
        self,
        historical_time_range,
    ):
        """Funding rate with SPOT market should raise ValueError."""
        from ckvd.utils.market_constraints import ChartType

        # Arrange
        start_time, end_time = historical_time_range

        manager = CryptoKlineVisionData.create(
            DataProvider.BINANCE,
            MarketType.SPOT,  # Invalid for funding rate
        )

        # Act & Assert
        with pytest.raises(ValueError) as exc_info:
            manager.get_data(
                symbol="BTCUSDT",
                start_time=start_time,
                end_time=end_time,
                interval=Interval.HOUR_8,
                chart_type=ChartType.FUNDING_RATE,
            )

        assert "funding rate" in str(exc_info.value).lower()
        manager.close()
