"""Unit tests for DataSourceManager.get_data() core path.

Tests the Failover Control Protocol (FCP) orchestration logic:
1. Cache hit path
2. Vision API fallback path
3. REST API fallback path
4. Error propagation
5. auto_reindex behavior

ADR: docs/adr/2025-01-30-failover-control-protocol.md
"""

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pandas as pd
import polars as pl
import pytest

from data_source_manager import DataProvider, DataSourceManager, Interval, MarketType
from data_source_manager.utils.for_core.rest_exceptions import RateLimitError


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


@patch("data_source_manager.core.sync.data_source_manager.FSSpecVisionHandler")
@patch("data_source_manager.core.sync.data_source_manager.UnifiedCacheManager")
class TestDataSourceManagerInitialization:
    """Tests for DataSourceManager initialization."""

    def test_create_manager_spot(self, mock_cache, mock_vision):
        """Verify manager creation for spot market."""
        mock_cache.return_value = MagicMock()
        mock_vision.return_value = MagicMock()

        manager = DataSourceManager.create(DataProvider.BINANCE, MarketType.SPOT)

        assert manager is not None
        assert manager.market_type == MarketType.SPOT
        assert manager.provider == DataProvider.BINANCE
        manager.close()

    def test_create_manager_futures_usdt(self, mock_cache, mock_vision):
        """Verify manager creation for USDT-margined futures."""
        mock_cache.return_value = MagicMock()
        mock_vision.return_value = MagicMock()

        manager = DataSourceManager.create(DataProvider.BINANCE, MarketType.FUTURES_USDT)

        assert manager is not None
        assert manager.market_type == MarketType.FUTURES_USDT
        manager.close()

    def test_create_manager_futures_coin(self, mock_cache, mock_vision):
        """Verify manager creation for coin-margined futures."""
        mock_cache.return_value = MagicMock()
        mock_vision.return_value = MagicMock()

        manager = DataSourceManager.create(DataProvider.BINANCE, MarketType.FUTURES_COIN)

        assert manager is not None
        assert manager.market_type == MarketType.FUTURES_COIN
        manager.close()

    def test_create_manager_with_cache_disabled(self, mock_cache, mock_vision):
        """Verify manager creation with cache disabled."""
        manager = DataSourceManager.create(
            DataProvider.BINANCE,
            MarketType.SPOT,
            use_cache=False,
        )

        assert manager is not None
        assert manager.use_cache is False
        manager.close()


# =============================================================================
# FCP Logic Tests - Using FCP utility function patches
# =============================================================================


class TestFCPCacheHit:
    """Tests for FCP cache hit path (Step 1)."""

    @patch("data_source_manager.core.sync.data_source_manager.verify_final_data")
    @patch("data_source_manager.core.sync.data_source_manager.process_rest_step")
    @patch("data_source_manager.core.sync.data_source_manager.process_vision_step")
    @patch("data_source_manager.core.sync.data_source_manager.process_cache_step")
    @patch("data_source_manager.core.sync.data_source_manager.FSSpecVisionHandler")
    @patch("data_source_manager.core.sync.data_source_manager.UnifiedCacheManager")
    def test_cache_hit_returns_data_without_api_calls(
        self,
        mock_cache_mgr,
        mock_vision_handler,
        mock_cache_step,
        mock_vision_step,
        mock_rest_step,
        mock_verify,
        sample_ohlcv_df,
        historical_time_range,
    ):
        """Cache hit should return data without calling Vision or REST APIs."""
        # Arrange
        start_time, end_time = historical_time_range
        mock_cache_mgr.return_value = MagicMock()
        mock_vision_handler.return_value = MagicMock()

        # Cache returns complete data with no missing ranges
        mock_cache_step.return_value = (sample_ohlcv_df, [])
        mock_verify.return_value = sample_ohlcv_df  # verify_final_data returns DataFrame

        manager = DataSourceManager.create(DataProvider.BINANCE, MarketType.FUTURES_USDT)

        # Act
        df = manager.get_data(
            symbol="BTCUSDT",
            start_time=start_time,
            end_time=end_time,
            interval=Interval.HOUR_1,
        )

        # Assert
        assert df is not None
        assert len(df) > 0
        mock_cache_step.assert_called_once()
        mock_vision_step.assert_not_called()  # Vision not called on cache hit
        mock_rest_step.assert_not_called()  # REST not called on cache hit
        manager.close()


class TestFCPVisionFallback:
    """Tests for FCP Vision API fallback path (Step 2)."""

    @patch("data_source_manager.core.sync.data_source_manager.verify_final_data")
    @patch("data_source_manager.core.sync.data_source_manager.process_rest_step")
    @patch("data_source_manager.core.sync.data_source_manager.process_vision_step")
    @patch("data_source_manager.core.sync.data_source_manager.process_cache_step")
    @patch("data_source_manager.core.sync.data_source_manager.FSSpecVisionHandler")
    @patch("data_source_manager.core.sync.data_source_manager.UnifiedCacheManager")
    def test_vision_fallback_on_cache_miss(
        self,
        mock_cache_mgr,
        mock_vision_handler,
        mock_cache_step,
        mock_vision_step,
        mock_rest_step,
        mock_verify,
        sample_ohlcv_df,
        historical_time_range,
    ):
        """Cache miss should trigger Vision API call."""
        # Arrange
        start_time, end_time = historical_time_range
        mock_cache_mgr.return_value = MagicMock()
        mock_vision_handler.return_value = MagicMock()

        # Cache miss - returns empty df with missing ranges
        mock_cache_step.return_value = (pd.DataFrame(), [(start_time, end_time)])
        # Vision returns data and no remaining missing ranges
        mock_vision_step.return_value = (sample_ohlcv_df, [])
        mock_verify.return_value = None

        manager = DataSourceManager.create(DataProvider.BINANCE, MarketType.FUTURES_USDT)

        # Act
        df = manager.get_data(
            symbol="BTCUSDT",
            start_time=start_time,
            end_time=end_time,
            interval=Interval.HOUR_1,
        )

        # Assert
        assert df is not None
        mock_cache_step.assert_called_once()
        mock_vision_step.assert_called_once()  # Vision called on cache miss
        mock_rest_step.assert_not_called()  # REST not called if Vision succeeds
        manager.close()


class TestFCPRestFallback:
    """Tests for FCP REST API fallback path (Step 3)."""

    @patch("data_source_manager.core.sync.data_source_manager.verify_final_data")
    @patch("data_source_manager.core.sync.data_source_manager.process_rest_step")
    @patch("data_source_manager.core.sync.data_source_manager.process_vision_step")
    @patch("data_source_manager.core.sync.data_source_manager.process_cache_step")
    @patch("data_source_manager.core.sync.data_source_manager.FSSpecVisionHandler")
    @patch("data_source_manager.core.sync.data_source_manager.UnifiedCacheManager")
    def test_rest_fallback_on_vision_miss(
        self,
        mock_cache_mgr,
        mock_vision_handler,
        mock_cache_step,
        mock_vision_step,
        mock_rest_step,
        mock_verify,
        sample_ohlcv_df,
        historical_time_range,
    ):
        """Vision miss should trigger REST API call."""
        # Arrange
        start_time, end_time = historical_time_range
        mock_cache_mgr.return_value = MagicMock()
        mock_vision_handler.return_value = MagicMock()

        # Cache miss
        mock_cache_step.return_value = (pd.DataFrame(), [(start_time, end_time)])
        # Vision returns empty with remaining missing ranges
        mock_vision_step.return_value = (pd.DataFrame(), [(start_time, end_time)])
        # REST returns data
        mock_rest_step.return_value = sample_ohlcv_df
        mock_verify.return_value = None

        manager = DataSourceManager.create(DataProvider.BINANCE, MarketType.FUTURES_USDT)

        # Act
        df = manager.get_data(
            symbol="BTCUSDT",
            start_time=start_time,
            end_time=end_time,
            interval=Interval.HOUR_1,
        )

        # Assert
        assert df is not None
        mock_cache_step.assert_called_once()
        mock_vision_step.assert_called_once()
        mock_rest_step.assert_called_once()  # REST called as final fallback
        manager.close()

    @patch("data_source_manager.core.sync.data_source_manager.verify_final_data")
    @patch("data_source_manager.core.sync.data_source_manager.process_rest_step")
    @patch("data_source_manager.core.sync.data_source_manager.process_vision_step")
    @patch("data_source_manager.core.sync.data_source_manager.process_cache_step")
    @patch("data_source_manager.core.sync.data_source_manager.FSSpecVisionHandler")
    @patch("data_source_manager.core.sync.data_source_manager.UnifiedCacheManager")
    def test_complete_fcp_chain(
        self,
        mock_cache_mgr,
        mock_vision_handler,
        mock_cache_step,
        mock_vision_step,
        mock_rest_step,
        mock_verify,
        sample_ohlcv_df,
        historical_time_range,
    ):
        """Test complete FCP chain: Cache miss -> Vision miss -> REST success."""
        # Arrange
        start_time, end_time = historical_time_range
        mock_cache_mgr.return_value = MagicMock()
        mock_vision_handler.return_value = MagicMock()

        # All sources fail except REST
        mock_cache_step.return_value = (pd.DataFrame(), [(start_time, end_time)])
        mock_vision_step.return_value = (pd.DataFrame(), [(start_time, end_time)])
        mock_rest_step.return_value = sample_ohlcv_df
        mock_verify.return_value = None

        manager = DataSourceManager.create(DataProvider.BINANCE, MarketType.FUTURES_USDT)

        # Act
        df = manager.get_data(
            symbol="BTCUSDT",
            start_time=start_time,
            end_time=end_time,
            interval=Interval.HOUR_1,
        )

        # Assert - all three steps called in order
        assert mock_cache_step.called
        assert mock_vision_step.called
        assert mock_rest_step.called
        assert len(df) > 0
        manager.close()


class TestFCPErrorPropagation:
    """Tests for error propagation in FCP."""

    @patch("data_source_manager.core.sync.data_source_manager.process_rest_step")
    @patch("data_source_manager.core.sync.data_source_manager.process_vision_step")
    @patch("data_source_manager.core.sync.data_source_manager.process_cache_step")
    @patch("data_source_manager.core.sync.data_source_manager.FSSpecVisionHandler")
    @patch("data_source_manager.core.sync.data_source_manager.UnifiedCacheManager")
    def test_rate_limit_error_propagates(
        self,
        mock_cache_mgr,
        mock_vision_handler,
        mock_cache_step,
        mock_vision_step,
        mock_rest_step,
        historical_time_range,
    ):
        """RateLimitError from REST should propagate to caller (wrapped in RuntimeError)."""
        # Arrange
        start_time, end_time = historical_time_range
        mock_cache_mgr.return_value = MagicMock()
        mock_vision_handler.return_value = MagicMock()

        # Cache and Vision miss
        mock_cache_step.return_value = (pd.DataFrame(), [(start_time, end_time)])
        mock_vision_step.return_value = (pd.DataFrame(), [(start_time, end_time)])
        # REST raises rate limit error
        mock_rest_step.side_effect = RateLimitError("429 Too Many Requests")

        manager = DataSourceManager.create(DataProvider.BINANCE, MarketType.FUTURES_USDT)

        # Act & Assert - error should propagate (wrapped in RuntimeError by handle_error)
        with pytest.raises(RuntimeError) as exc_info:
            manager.get_data(
                symbol="BTCUSDT",
                start_time=start_time,
                end_time=end_time,
                interval=Interval.HOUR_1,
            )

        # Verify the original error is mentioned
        assert "RateLimitError" in str(exc_info.value) or "429" in str(exc_info.value)
        manager.close()


class TestAutoReindexBehavior:
    """Tests for auto_reindex parameter behavior."""

    @patch("data_source_manager.core.sync.data_source_manager.verify_final_data")
    @patch("data_source_manager.core.sync.data_source_manager.process_rest_step")
    @patch("data_source_manager.core.sync.data_source_manager.process_vision_step")
    @patch("data_source_manager.core.sync.data_source_manager.process_cache_step")
    @patch("data_source_manager.core.sync.data_source_manager.FSSpecVisionHandler")
    @patch("data_source_manager.core.sync.data_source_manager.UnifiedCacheManager")
    def test_auto_reindex_true_creates_complete_series(
        self,
        mock_cache_mgr,
        mock_vision_handler,
        mock_cache_step,
        mock_vision_step,
        mock_rest_step,
        mock_verify,
        sample_ohlcv_df,
        historical_time_range,
    ):
        """auto_reindex=True should create complete time series."""
        # Arrange
        start_time, end_time = historical_time_range
        mock_cache_mgr.return_value = MagicMock()
        mock_vision_handler.return_value = MagicMock()
        mock_cache_step.return_value = (sample_ohlcv_df, [])
        mock_verify.return_value = None

        manager = DataSourceManager.create(DataProvider.BINANCE, MarketType.FUTURES_USDT)

        # Act
        df = manager.get_data(
            symbol="BTCUSDT",
            start_time=start_time,
            end_time=end_time,
            interval=Interval.HOUR_1,
            auto_reindex=True,
        )

        # Assert - index should be monotonic
        assert df.index.is_monotonic_increasing
        assert not df.index.has_duplicates
        manager.close()

    @patch("data_source_manager.core.sync.data_source_manager.verify_final_data")
    @patch("data_source_manager.core.sync.data_source_manager.process_rest_step")
    @patch("data_source_manager.core.sync.data_source_manager.process_vision_step")
    @patch("data_source_manager.core.sync.data_source_manager.process_cache_step")
    @patch("data_source_manager.core.sync.data_source_manager.FSSpecVisionHandler")
    @patch("data_source_manager.core.sync.data_source_manager.UnifiedCacheManager")
    def test_auto_reindex_false_returns_only_available_data(
        self,
        mock_cache_mgr,
        mock_vision_handler,
        mock_cache_step,
        mock_vision_step,
        mock_rest_step,
        mock_verify,
        sample_ohlcv_df,
        historical_time_range,
    ):
        """auto_reindex=False should return only available data without NaN padding."""
        # Arrange
        start_time, end_time = historical_time_range
        mock_cache_mgr.return_value = MagicMock()
        mock_vision_handler.return_value = MagicMock()
        mock_cache_step.return_value = (sample_ohlcv_df, [])
        mock_verify.return_value = None

        manager = DataSourceManager.create(DataProvider.BINANCE, MarketType.FUTURES_USDT)

        # Act
        df = manager.get_data(
            symbol="BTCUSDT",
            start_time=start_time,
            end_time=end_time,
            interval=Interval.HOUR_1,
            auto_reindex=False,
        )

        # Assert - should not have NaN values in OHLCV columns
        assert not df["open"].isna().any()
        assert not df["close"].isna().any()
        manager.close()


class TestEmptyResultHandling:
    """Tests for empty result scenarios."""

    @patch("data_source_manager.core.sync.data_source_manager.process_rest_step")
    @patch("data_source_manager.core.sync.data_source_manager.process_vision_step")
    @patch("data_source_manager.core.sync.data_source_manager.process_cache_step")
    @patch("data_source_manager.core.sync.data_source_manager.FSSpecVisionHandler")
    @patch("data_source_manager.core.sync.data_source_manager.UnifiedCacheManager")
    def test_all_sources_empty_raises_error(
        self,
        mock_cache_mgr,
        mock_vision_handler,
        mock_cache_step,
        mock_vision_step,
        mock_rest_step,
        historical_time_range,
    ):
        """When all sources return empty, should raise RuntimeError."""
        # Arrange
        start_time, end_time = historical_time_range
        mock_cache_mgr.return_value = MagicMock()
        mock_vision_handler.return_value = MagicMock()

        # All sources return empty
        mock_cache_step.return_value = (pd.DataFrame(), [(start_time, end_time)])
        mock_vision_step.return_value = (pd.DataFrame(), [(start_time, end_time)])
        mock_rest_step.return_value = pd.DataFrame()

        manager = DataSourceManager.create(DataProvider.BINANCE, MarketType.FUTURES_USDT)

        # Act & Assert - should raise error, not return silently
        with pytest.raises(RuntimeError):
            manager.get_data(
                symbol="BTCUSDT",
                start_time=start_time,
                end_time=end_time,
                interval=Interval.HOUR_1,
            )

        manager.close()


class TestInputValidation:
    """Tests for input validation."""

    @patch("data_source_manager.core.sync.data_source_manager.FSSpecVisionHandler")
    @patch("data_source_manager.core.sync.data_source_manager.UnifiedCacheManager")
    def test_invalid_time_range_raises_error(self, mock_cache, mock_vision):
        """start_time >= end_time should raise RuntimeError (wrapping ValueError)."""
        mock_cache.return_value = MagicMock()
        mock_vision.return_value = MagicMock()

        manager = DataSourceManager.create(DataProvider.BINANCE, MarketType.SPOT)
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

    @patch("data_source_manager.core.sync.data_source_manager.FSSpecVisionHandler")
    @patch("data_source_manager.core.sync.data_source_manager.UnifiedCacheManager")
    def test_manager_creation_succeeds(self, mock_cache, mock_vision):
        """Manager creation should succeed with valid parameters."""
        mock_cache.return_value = MagicMock()
        mock_vision.return_value = MagicMock()

        manager = DataSourceManager.create(DataProvider.BINANCE, MarketType.SPOT)
        assert manager is not None
        assert manager.provider == DataProvider.BINANCE
        assert manager.market_type == MarketType.SPOT
        manager.close()


class TestReturnPolarsParameter:
    """Tests for return_polars parameter behavior."""

    @patch("data_source_manager.core.sync.data_source_manager.verify_final_data")
    @patch("data_source_manager.core.sync.data_source_manager.process_rest_step")
    @patch("data_source_manager.core.sync.data_source_manager.process_vision_step")
    @patch("data_source_manager.core.sync.data_source_manager.process_cache_step")
    @patch("data_source_manager.core.sync.data_source_manager.FSSpecVisionHandler")
    @patch("data_source_manager.core.sync.data_source_manager.UnifiedCacheManager")
    def test_return_polars_false_returns_pandas(
        self,
        mock_cache_mgr,
        mock_vision_handler,
        mock_cache_step,
        mock_vision_step,
        mock_rest_step,
        mock_verify,
        sample_ohlcv_df,
        historical_time_range,
    ):
        """return_polars=False (default) should return pandas DataFrame."""
        # Arrange
        start_time, end_time = historical_time_range
        mock_cache_mgr.return_value = MagicMock()
        mock_vision_handler.return_value = MagicMock()
        mock_cache_step.return_value = (sample_ohlcv_df, [])
        mock_verify.return_value = sample_ohlcv_df

        manager = DataSourceManager.create(DataProvider.BINANCE, MarketType.FUTURES_USDT)

        # Act
        df = manager.get_data(
            symbol="BTCUSDT",
            start_time=start_time,
            end_time=end_time,
            interval=Interval.HOUR_1,
            return_polars=False,
        )

        # Assert
        assert isinstance(df, pd.DataFrame)
        assert len(df) > 0
        manager.close()

    @patch("data_source_manager.core.sync.data_source_manager.verify_final_data")
    @patch("data_source_manager.core.sync.data_source_manager.process_rest_step")
    @patch("data_source_manager.core.sync.data_source_manager.process_vision_step")
    @patch("data_source_manager.core.sync.data_source_manager.process_cache_step")
    @patch("data_source_manager.core.sync.data_source_manager.FSSpecVisionHandler")
    @patch("data_source_manager.core.sync.data_source_manager.UnifiedCacheManager")
    def test_return_polars_true_returns_polars(
        self,
        mock_cache_mgr,
        mock_vision_handler,
        mock_cache_step,
        mock_vision_step,
        mock_rest_step,
        mock_verify,
        sample_ohlcv_df,
        historical_time_range,
    ):
        """return_polars=True should return polars DataFrame."""

        # Arrange
        start_time, end_time = historical_time_range
        mock_cache_mgr.return_value = MagicMock()
        mock_vision_handler.return_value = MagicMock()
        mock_cache_step.return_value = (sample_ohlcv_df, [])
        mock_verify.return_value = sample_ohlcv_df

        manager = DataSourceManager.create(DataProvider.BINANCE, MarketType.FUTURES_USDT)

        # Act
        df = manager.get_data(
            symbol="BTCUSDT",
            start_time=start_time,
            end_time=end_time,
            interval=Interval.HOUR_1,
            return_polars=True,
        )

        # Assert
        assert isinstance(df, pl.DataFrame)
        assert len(df) > 0
        manager.close()

    @patch("data_source_manager.core.sync.data_source_manager.verify_final_data")
    @patch("data_source_manager.core.sync.data_source_manager.process_rest_step")
    @patch("data_source_manager.core.sync.data_source_manager.process_vision_step")
    @patch("data_source_manager.core.sync.data_source_manager.process_cache_step")
    @patch("data_source_manager.core.sync.data_source_manager.FSSpecVisionHandler")
    @patch("data_source_manager.core.sync.data_source_manager.UnifiedCacheManager")
    def test_polars_df_contains_open_time_column(
        self,
        mock_cache_mgr,
        mock_vision_handler,
        mock_cache_step,
        mock_vision_step,
        mock_rest_step,
        mock_verify,
        sample_ohlcv_df,
        historical_time_range,
    ):
        """Polars DataFrame should have open_time as column (not index)."""

        # Arrange
        start_time, end_time = historical_time_range
        mock_cache_mgr.return_value = MagicMock()
        mock_vision_handler.return_value = MagicMock()
        mock_cache_step.return_value = (sample_ohlcv_df, [])
        mock_verify.return_value = sample_ohlcv_df

        manager = DataSourceManager.create(DataProvider.BINANCE, MarketType.FUTURES_USDT)

        # Act
        df = manager.get_data(
            symbol="BTCUSDT",
            start_time=start_time,
            end_time=end_time,
            interval=Interval.HOUR_1,
            return_polars=True,
        )

        # Assert - Polars doesn't have index, open_time should be a column
        assert isinstance(df, pl.DataFrame)
        assert "open_time" in df.columns
        expected_cols = ["open_time", "open", "high", "low", "close", "volume"]
        for col in expected_cols:
            assert col in df.columns
        manager.close()

    @patch("data_source_manager.core.sync.data_source_manager.verify_final_data")
    @patch("data_source_manager.core.sync.data_source_manager.process_rest_step")
    @patch("data_source_manager.core.sync.data_source_manager.process_vision_step")
    @patch("data_source_manager.core.sync.data_source_manager.process_cache_step")
    @patch("data_source_manager.core.sync.data_source_manager.FSSpecVisionHandler")
    @patch("data_source_manager.core.sync.data_source_manager.UnifiedCacheManager")
    def test_default_returns_pandas(
        self,
        mock_cache_mgr,
        mock_vision_handler,
        mock_cache_step,
        mock_vision_step,
        mock_rest_step,
        mock_verify,
        sample_ohlcv_df,
        historical_time_range,
    ):
        """Omitting return_polars should default to pandas DataFrame."""
        # Arrange
        start_time, end_time = historical_time_range
        mock_cache_mgr.return_value = MagicMock()
        mock_vision_handler.return_value = MagicMock()
        mock_cache_step.return_value = (sample_ohlcv_df, [])
        mock_verify.return_value = sample_ohlcv_df

        manager = DataSourceManager.create(DataProvider.BINANCE, MarketType.FUTURES_USDT)

        # Act - Do not pass return_polars parameter
        df = manager.get_data(
            symbol="BTCUSDT",
            start_time=start_time,
            end_time=end_time,
            interval=Interval.HOUR_1,
        )

        # Assert - Should be pandas by default
        assert isinstance(df, pd.DataFrame)
        manager.close()
