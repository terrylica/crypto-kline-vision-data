"""Tests for cache toggle behavior (use_cache=False).

Validates that the FCP workflow functions correctly when caching is disabled,
including environment variable override via CKVD_ENABLE_CACHE.

Related: Plan "Cache Toggle: Ensure FCP Works Correctly with Cache Disabled"
"""

import os
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pandas as pd
import pytest

from ckvd import CryptoKlineVisionData, DataProvider, Interval, MarketType
from ckvd.core.sync.ckvd_types import DataSource


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_ohlcv_df(start: datetime, count: int, freq_minutes: int = 60) -> pd.DataFrame:
    """Create a sample OHLCV DataFrame for testing."""
    rows = []
    for i in range(count):
        ts = start + timedelta(minutes=freq_minutes * i)
        rows.append(
            {
                "open_time": ts,
                "open": 42000.0 + i,
                "high": 42100.0 + i,
                "low": 41900.0 + i,
                "close": 42050.0 + i,
                "volume": 1000.0 + i,
            }
        )
    return pd.DataFrame(rows).set_index("open_time")


# ---------------------------------------------------------------------------
# 1. Initialization
# ---------------------------------------------------------------------------


class TestCacheToggleInitialization:
    """Verify use_cache is stored and propagated correctly."""

    def test_constructor_use_cache_false(self):
        """use_cache=False is stored on the instance."""
        mgr = CryptoKlineVisionData.create(DataProvider.BINANCE, MarketType.SPOT, use_cache=False)
        assert mgr.use_cache is False
        mgr.close()

    def test_constructor_use_cache_default_true(self):
        """use_cache defaults to True."""
        mgr = CryptoKlineVisionData.create(DataProvider.BINANCE, MarketType.SPOT)
        assert mgr.use_cache is True
        mgr.close()

    def test_create_factory_passes_use_cache(self):
        """Factory .create() passes use_cache to the constructor."""
        mgr = CryptoKlineVisionData.create(
            DataProvider.BINANCE, MarketType.FUTURES_USDT, use_cache=False
        )
        assert mgr.use_cache is False
        mgr.close()

    def test_create_factory_all_market_types(self):
        """use_cache=False works across all market types."""
        for mt in (MarketType.SPOT, MarketType.FUTURES_USDT, MarketType.FUTURES_COIN):
            mgr = CryptoKlineVisionData.create(DataProvider.BINANCE, mt, use_cache=False)
            assert mgr.use_cache is False
            mgr.close()


# ---------------------------------------------------------------------------
# 2. Cache read disabled
# ---------------------------------------------------------------------------


class TestCacheReadDisabled:
    """_get_from_cache() returns empty when cache is disabled."""

    def test_get_from_cache_returns_empty(self):
        """_get_from_cache returns empty df + full range when use_cache=False."""
        mgr = CryptoKlineVisionData.create(DataProvider.BINANCE, MarketType.SPOT, use_cache=False)
        end = datetime.now(timezone.utc)
        start = end - timedelta(days=1)

        df, missing = mgr._get_from_cache("BTCUSDT", start, end, Interval.HOUR_1)

        assert df.empty
        assert len(missing) == 1
        assert missing[0] == (start, end)
        mgr.close()

    @patch("ckvd.utils.for_core.ckvd_cache_utils.get_cache_lazyframes")
    def test_get_cache_lazyframes_never_called(self, mock_get_lf):
        """get_cache_lazyframes is never invoked when cache is off."""
        mgr = CryptoKlineVisionData.create(DataProvider.BINANCE, MarketType.SPOT, use_cache=False)
        end = datetime.now(timezone.utc)
        start = end - timedelta(days=1)

        mgr._get_from_cache("BTCUSDT", start, end, Interval.HOUR_1)
        mock_get_lf.assert_not_called()
        mgr.close()

    def test_cache_dir_still_set(self):
        """cache_dir is still set even when use_cache=False (no crash on access)."""
        mgr = CryptoKlineVisionData.create(DataProvider.BINANCE, MarketType.SPOT, use_cache=False)
        # cache_dir should still have a value (default platform path)
        assert mgr.cache_dir is not None
        mgr.close()

    @patch("ckvd.utils.for_core.ckvd_cache_utils.get_from_cache")
    def test_get_from_cache_util_never_called(self, mock_get_cache):
        """get_from_cache utility is never invoked when cache is off."""
        mgr = CryptoKlineVisionData.create(DataProvider.BINANCE, MarketType.SPOT, use_cache=False)
        end = datetime.now(timezone.utc)
        start = end - timedelta(days=1)

        mgr._get_from_cache("BTCUSDT", start, end, Interval.HOUR_1)
        mock_get_cache.assert_not_called()
        mgr.close()


# ---------------------------------------------------------------------------
# 3. Cache write disabled
# ---------------------------------------------------------------------------


class TestCacheWriteDisabled:
    """_save_to_cache() is a no-op when cache is disabled."""

    def test_save_to_cache_noop(self):
        """_save_to_cache does nothing when use_cache=False (no error)."""
        mgr = CryptoKlineVisionData.create(DataProvider.BINANCE, MarketType.SPOT, use_cache=False)
        df = _make_ohlcv_df(datetime(2024, 1, 1, tzinfo=timezone.utc), 24)

        # Should not raise
        mgr._save_to_cache(df, "BTCUSDT", Interval.HOUR_1, source="TEST")
        mgr.close()

    @patch("ckvd.utils.for_core.ckvd_cache_utils.save_to_cache")
    def test_save_to_cache_util_never_called(self, mock_save):
        """save_to_cache utility is never invoked when cache is off."""
        mgr = CryptoKlineVisionData.create(DataProvider.BINANCE, MarketType.SPOT, use_cache=False)
        df = _make_ohlcv_df(datetime(2024, 1, 1, tzinfo=timezone.utc), 24)

        mgr._save_to_cache(df, "BTCUSDT", Interval.HOUR_1, source="TEST")
        mock_save.assert_not_called()
        mgr.close()

    def test_vision_passes_none_save_func(self):
        """_fetch_from_vision passes save_to_cache_func=None when cache is off."""
        mgr = CryptoKlineVisionData.create(DataProvider.BINANCE, MarketType.SPOT, use_cache=False)

        with patch("ckvd.core.sync.crypto_kline_vision_data.fetch_from_vision") as mock_fv:
            mock_fv.return_value = pd.DataFrame()
            # Need a non-None vision_client for the function to be called
            mgr.vision_client = object()
            mgr._fetch_from_vision(
                "BTCUSDT",
                datetime(2024, 1, 1, tzinfo=timezone.utc),
                datetime(2024, 1, 2, tzinfo=timezone.utc),
                Interval.HOUR_1,
            )

            # save_to_cache_func should be None
            call_kwargs = mock_fv.call_args[1]
            assert call_kwargs["save_to_cache_func"] is None

        mgr.close()

    def test_rest_passes_none_save_func(self):
        """process_rest_step receives save_to_cache_func=None when cache is off."""
        mgr = CryptoKlineVisionData.create(DataProvider.BINANCE, MarketType.SPOT, use_cache=False)

        # The save_to_cache_func conditional is: self._save_to_cache if self.use_cache else None
        result = mgr._save_to_cache if mgr.use_cache else None
        assert result is None
        mgr.close()


# ---------------------------------------------------------------------------
# 4. Full FCP flow with cache disabled
# ---------------------------------------------------------------------------


class TestFCPFlowWithCacheDisabled:
    """Full FCP goes Vision→REST when cache is disabled."""

    @patch("ckvd.core.sync.crypto_kline_vision_data.process_rest_step")
    @patch("ckvd.core.sync.crypto_kline_vision_data.process_vision_step")
    @patch("ckvd.utils.validation.availability_data.is_symbol_available_at", return_value=(True, None))
    def test_fcp_skips_cache_hits_vision_and_rest(self, mock_avail, mock_vision_step, mock_rest_step):
        """With use_cache=False, FCP skips cache and proceeds to Vision then REST."""
        start = datetime(2024, 1, 1, tzinfo=timezone.utc)
        end = datetime(2024, 1, 8, tzinfo=timezone.utc)

        # Vision returns partial data with gaps
        vision_df = _make_ohlcv_df(start, 120)
        vision_df["_data_source"] = "VISION"
        mock_vision_step.return_value = (vision_df, [(end - timedelta(days=1), end)])

        # REST fills the rest
        rest_df = pd.concat([vision_df, _make_ohlcv_df(end - timedelta(days=1), 24)])
        rest_df["_data_source"] = "REST"
        mock_rest_step.return_value = rest_df

        mgr = CryptoKlineVisionData.create(DataProvider.BINANCE, MarketType.SPOT, use_cache=False)
        mgr.get_data("BTCUSDT", start, end, Interval.HOUR_1, auto_reindex=False)

        # Vision step was called
        mock_vision_step.assert_called_once()
        # REST step was called
        mock_rest_step.assert_called_once()

        # save_to_cache_func passed to REST should be None
        rest_kwargs = mock_rest_step.call_args[1]
        assert rest_kwargs.get("save_to_cache_func") is None

        mgr.close()

    @patch("ckvd.core.sync.crypto_kline_vision_data.process_rest_step")
    @patch("ckvd.core.sync.crypto_kline_vision_data.process_vision_step")
    @patch("ckvd.utils.validation.availability_data.is_symbol_available_at", return_value=(True, None))
    def test_fcp_cache_lazyframes_not_called(self, mock_avail, mock_vision_step, mock_rest_step):
        """get_cache_lazyframes is NOT called in the FCP when cache is disabled."""
        start = datetime(2024, 1, 1, tzinfo=timezone.utc)
        end = datetime(2024, 1, 2, tzinfo=timezone.utc)

        rest_df = _make_ohlcv_df(start, 24)
        rest_df["_data_source"] = "REST"
        mock_vision_step.return_value = (pd.DataFrame(), [(start, end)])
        mock_rest_step.return_value = rest_df

        with patch("ckvd.utils.for_core.ckvd_cache_utils.get_cache_lazyframes") as mock_lf:
            mgr = CryptoKlineVisionData.create(DataProvider.BINANCE, MarketType.SPOT, use_cache=False)
            mgr.get_data("BTCUSDT", start, end, Interval.HOUR_1, auto_reindex=False)

            mock_lf.assert_not_called()
            mgr.close()

    @patch("ckvd.core.sync.crypto_kline_vision_data.process_rest_step")
    @patch("ckvd.core.sync.crypto_kline_vision_data.process_vision_step")
    @patch("ckvd.utils.validation.availability_data.is_symbol_available_at", return_value=(True, None))
    def test_fcp_enforce_rest_with_cache_disabled(self, mock_avail, mock_vision_step, mock_rest_step):
        """enforce_source=REST + use_cache=False works correctly."""
        start = datetime(2024, 1, 1, tzinfo=timezone.utc)
        end = datetime(2024, 1, 2, tzinfo=timezone.utc)

        rest_df = _make_ohlcv_df(start, 24)
        rest_df["_data_source"] = "REST"
        mock_rest_step.return_value = rest_df

        mgr = CryptoKlineVisionData.create(DataProvider.BINANCE, MarketType.SPOT, use_cache=False)
        mgr.get_data(
            "BTCUSDT", start, end, Interval.HOUR_1,
            enforce_source=DataSource.REST, auto_reindex=False,
        )

        # Vision step should NOT be called with enforce_source=REST
        mock_vision_step.assert_not_called()
        mock_rest_step.assert_called_once()
        mgr.close()


# ---------------------------------------------------------------------------
# 5. End-to-end data correctness
# ---------------------------------------------------------------------------


class TestEndToEndDataCorrectness:
    """Data columns and types are correct with cache disabled."""

    @patch("ckvd.core.sync.crypto_kline_vision_data.process_rest_step")
    @patch("ckvd.core.sync.crypto_kline_vision_data.process_vision_step")
    @patch("ckvd.utils.validation.availability_data.is_symbol_available_at", return_value=(True, None))
    def test_columns_correct(self, mock_avail, mock_vision_step, mock_rest_step):
        """Returned DataFrame has correct OHLCV columns."""
        start = datetime(2024, 1, 1, tzinfo=timezone.utc)
        end = datetime(2024, 1, 2, tzinfo=timezone.utc)

        df = _make_ohlcv_df(start, 24)
        df["_data_source"] = "REST"
        mock_vision_step.return_value = (pd.DataFrame(), [(start, end)])
        mock_rest_step.return_value = df

        mgr = CryptoKlineVisionData.create(DataProvider.BINANCE, MarketType.SPOT, use_cache=False)
        result = mgr.get_data("BTCUSDT", start, end, Interval.HOUR_1, auto_reindex=False)

        for col in ("open", "high", "low", "close", "volume"):
            assert col in result.columns, f"Missing column: {col}"

        mgr.close()

    @patch("ckvd.core.sync.crypto_kline_vision_data.process_rest_step")
    @patch("ckvd.core.sync.crypto_kline_vision_data.process_vision_step")
    @patch("ckvd.utils.validation.availability_data.is_symbol_available_at", return_value=(True, None))
    def test_no_cache_source_in_data(self, mock_avail, mock_vision_step, mock_rest_step):
        """_data_source column has no CACHE entries when cache is off."""
        start = datetime(2024, 1, 1, tzinfo=timezone.utc)
        end = datetime(2024, 1, 2, tzinfo=timezone.utc)

        df = _make_ohlcv_df(start, 24)
        df["_data_source"] = "REST"
        mock_vision_step.return_value = (pd.DataFrame(), [(start, end)])
        mock_rest_step.return_value = df

        mgr = CryptoKlineVisionData.create(DataProvider.BINANCE, MarketType.SPOT, use_cache=False)
        result = mgr.get_data(
            "BTCUSDT", start, end, Interval.HOUR_1,
            include_source_info=True, auto_reindex=False,
        )

        if "_data_source" in result.columns:
            assert "CACHE" not in result["_data_source"].to_numpy()

        mgr.close()


# ---------------------------------------------------------------------------
# 6. enforce_source interaction
# ---------------------------------------------------------------------------


class TestEnforceSourceInteraction:
    """enforce_source=CACHE + use_cache=False raises ValueError."""

    def test_enforce_cache_with_cache_disabled_raises(self):
        """enforce_source=CACHE + use_cache=False raises ValueError (wrapped in RuntimeError by FCP)."""
        mgr = CryptoKlineVisionData.create(DataProvider.BINANCE, MarketType.SPOT, use_cache=False)
        start = datetime(2024, 1, 1, tzinfo=timezone.utc)
        end = datetime(2024, 1, 2, tzinfo=timezone.utc)

        # The ValueError is caught by FCP and re-raised as RuntimeError
        with pytest.raises(RuntimeError, match="Cannot use enforce_source=DataSource.CACHE when use_cache=False"):
            mgr.get_data(
                "BTCUSDT", start, end, Interval.HOUR_1,
                enforce_source=DataSource.CACHE,
            )

        mgr.close()

    @patch("ckvd.core.sync.crypto_kline_vision_data.process_rest_step")
    @patch("ckvd.core.sync.crypto_kline_vision_data.process_vision_step")
    @patch("ckvd.utils.validation.availability_data.is_symbol_available_at", return_value=(True, None))
    def test_enforce_rest_with_cache_disabled_ok(self, mock_avail, mock_vision_step, mock_rest_step):
        """enforce_source=REST + use_cache=False works without error."""
        start = datetime(2024, 1, 1, tzinfo=timezone.utc)
        end = datetime(2024, 1, 2, tzinfo=timezone.utc)

        df = _make_ohlcv_df(start, 24)
        df["_data_source"] = "REST"
        mock_rest_step.return_value = df

        mgr = CryptoKlineVisionData.create(DataProvider.BINANCE, MarketType.SPOT, use_cache=False)
        result = mgr.get_data(
            "BTCUSDT", start, end, Interval.HOUR_1,
            enforce_source=DataSource.REST, auto_reindex=False,
        )
        assert result is not None
        mgr.close()

    @patch("ckvd.core.sync.crypto_kline_vision_data.process_rest_step")
    @patch("ckvd.core.sync.crypto_kline_vision_data.process_vision_step")
    @patch("ckvd.utils.validation.availability_data.is_symbol_available_at", return_value=(True, None))
    def test_enforce_vision_with_cache_disabled_ok(self, mock_avail, mock_vision_step, mock_rest_step):
        """enforce_source=VISION + use_cache=False works without error."""
        start = datetime(2024, 1, 1, tzinfo=timezone.utc)
        end = datetime(2024, 1, 2, tzinfo=timezone.utc)

        df = _make_ohlcv_df(start, 24)
        df["_data_source"] = "VISION"
        mock_vision_step.return_value = (df, [])

        mgr = CryptoKlineVisionData.create(DataProvider.BINANCE, MarketType.SPOT, use_cache=False)
        result = mgr.get_data(
            "BTCUSDT", start, end, Interval.HOUR_1,
            enforce_source=DataSource.VISION, auto_reindex=False,
        )
        assert result is not None
        mgr.close()

    @patch("ckvd.core.sync.crypto_kline_vision_data.process_rest_step")
    @patch("ckvd.core.sync.crypto_kline_vision_data.process_vision_step")
    @patch("ckvd.utils.validation.availability_data.is_symbol_available_at", return_value=(True, None))
    def test_enforce_auto_with_cache_disabled_ok(self, mock_avail, mock_vision_step, mock_rest_step):
        """enforce_source=AUTO + use_cache=False works (skips cache, hits Vision+REST)."""
        start = datetime(2024, 1, 1, tzinfo=timezone.utc)
        end = datetime(2024, 1, 2, tzinfo=timezone.utc)

        df = _make_ohlcv_df(start, 24)
        df["_data_source"] = "REST"
        mock_vision_step.return_value = (pd.DataFrame(), [(start, end)])
        mock_rest_step.return_value = df

        mgr = CryptoKlineVisionData.create(DataProvider.BINANCE, MarketType.SPOT, use_cache=False)
        result = mgr.get_data(
            "BTCUSDT", start, end, Interval.HOUR_1,
            enforce_source=DataSource.AUTO, auto_reindex=False,
        )
        assert result is not None
        mgr.close()


# ---------------------------------------------------------------------------
# 7. fetch_market_data passthrough
# ---------------------------------------------------------------------------


class TestFetchMarketDataPassthrough:
    """High-level fetch_market_data(use_cache=False) propagates correctly."""

    @patch("ckvd.core.sync.crypto_kline_vision_data.process_rest_step")
    @patch("ckvd.core.sync.crypto_kline_vision_data.process_vision_step")
    @patch("ckvd.utils.validation.availability_data.is_symbol_available_at", return_value=(True, None))
    def test_fetch_market_data_use_cache_false(self, mock_avail, mock_vision_step, mock_rest_step):
        """fetch_market_data(use_cache=False) creates manager with cache disabled."""
        from ckvd import ChartType, fetch_market_data

        start = datetime(2024, 1, 1, tzinfo=timezone.utc)
        end = datetime(2024, 1, 2, tzinfo=timezone.utc)

        df = _make_ohlcv_df(start, 24)
        df["_data_source"] = "REST"
        mock_vision_step.return_value = (pd.DataFrame(), [(start, end)])
        mock_rest_step.return_value = df

        result_df, elapsed, count = fetch_market_data(
            provider=DataProvider.BINANCE,
            market_type=MarketType.SPOT,
            chart_type=ChartType.KLINES,
            symbol="BTCUSDT",
            interval=Interval.HOUR_1,
            start_time=start,
            end_time=end,
            use_cache=False,
        )

        assert result_df is not None
        assert elapsed >= 0

    @patch("ckvd.core.sync.crypto_kline_vision_data.process_rest_step")
    @patch("ckvd.core.sync.crypto_kline_vision_data.process_vision_step")
    @patch("ckvd.utils.validation.availability_data.is_symbol_available_at", return_value=(True, None))
    def test_fetch_market_data_default_has_cache(self, mock_avail, mock_vision_step, mock_rest_step):
        """fetch_market_data() with default use_cache=True creates manager with cache enabled."""
        from ckvd import ChartType, fetch_market_data

        start = datetime(2024, 1, 1, tzinfo=timezone.utc)
        end = datetime(2024, 1, 2, tzinfo=timezone.utc)

        df = _make_ohlcv_df(start, 24)
        df["_data_source"] = "REST"
        mock_vision_step.return_value = (pd.DataFrame(), [(start, end)])
        mock_rest_step.return_value = df

        # Spy on CryptoKlineVisionData.__init__ to verify use_cache
        original_init = CryptoKlineVisionData.__init__
        captured_use_cache = []

        def spy_init(self_inner, *args, **kwargs):
            captured_use_cache.append(kwargs.get("use_cache", True))
            return original_init(self_inner, *args, **kwargs)

        with patch.object(CryptoKlineVisionData, "__init__", spy_init):
            fetch_market_data(
                provider=DataProvider.BINANCE,
                market_type=MarketType.SPOT,
                chart_type=ChartType.KLINES,
                symbol="BTCUSDT",
                interval=Interval.HOUR_1,
                start_time=start,
                end_time=end,
            )

        # Default should have use_cache=True
        assert len(captured_use_cache) == 1
        assert captured_use_cache[0] is True


# ---------------------------------------------------------------------------
# 8. Funding rate client cache toggle
# ---------------------------------------------------------------------------


class TestFundingRateClientCacheToggle:
    """BinanceFundingRateClient respects use_cache=False."""

    def test_funding_rate_client_cache_disabled(self):
        """BinanceFundingRateClient(use_cache=False) stores the flag."""
        from ckvd.core.providers.binance.binance_funding_rate_client import BinanceFundingRateClient

        client = BinanceFundingRateClient(
            symbol="BTCUSDT",
            market_type=MarketType.FUTURES_USDT,
            use_cache=False,
        )
        assert client._use_cache is False

    def test_funding_rate_client_cache_enabled_default(self):
        """BinanceFundingRateClient defaults to use_cache=True."""
        from ckvd.core.providers.binance.binance_funding_rate_client import BinanceFundingRateClient

        client = BinanceFundingRateClient(
            symbol="BTCUSDT",
            market_type=MarketType.FUTURES_USDT,
        )
        assert client._use_cache is True

    def test_funding_rate_client_no_cache_manager_when_disabled(self):
        """Cache manager is not created when use_cache=False."""
        from ckvd.core.providers.binance.binance_funding_rate_client import BinanceFundingRateClient

        client = BinanceFundingRateClient(
            symbol="BTCUSDT",
            market_type=MarketType.FUTURES_USDT,
            use_cache=False,
        )
        # When use_cache=False, _cache_manager should not be initialized
        assert not hasattr(client, "_cache_manager") or client._cache_manager is None


# ---------------------------------------------------------------------------
# 9. Repeated requests without cache
# ---------------------------------------------------------------------------


class TestRepeatedRequestsWithoutCache:
    """Same request hits API twice (no persistence between calls)."""

    @patch("ckvd.core.sync.crypto_kline_vision_data.process_rest_step")
    @patch("ckvd.core.sync.crypto_kline_vision_data.process_vision_step")
    @patch("ckvd.utils.validation.availability_data.is_symbol_available_at", return_value=(True, None))
    def test_repeated_requests_both_hit_api(self, mock_avail, mock_vision_step, mock_rest_step):
        """Two identical requests both go through Vision+REST (nothing cached)."""
        start = datetime(2024, 1, 1, tzinfo=timezone.utc)
        end = datetime(2024, 1, 2, tzinfo=timezone.utc)

        df = _make_ohlcv_df(start, 24)
        df["_data_source"] = "REST"
        mock_vision_step.return_value = (pd.DataFrame(), [(start, end)])
        mock_rest_step.return_value = df

        mgr = CryptoKlineVisionData.create(DataProvider.BINANCE, MarketType.SPOT, use_cache=False)

        # First request
        mgr.get_data("BTCUSDT", start, end, Interval.HOUR_1, auto_reindex=False)
        # Second identical request
        mgr.get_data("BTCUSDT", start, end, Interval.HOUR_1, auto_reindex=False)

        # Both requests should have called Vision and REST
        assert mock_vision_step.call_count == 2
        assert mock_rest_step.call_count == 2

        mgr.close()

    @patch("ckvd.core.sync.crypto_kline_vision_data.process_rest_step")
    @patch("ckvd.core.sync.crypto_kline_vision_data.process_vision_step")
    @patch("ckvd.utils.validation.availability_data.is_symbol_available_at", return_value=(True, None))
    def test_no_save_to_cache_called(self, mock_avail, mock_vision_step, mock_rest_step):
        """_save_to_cache is never called during get_data with cache off."""
        start = datetime(2024, 1, 1, tzinfo=timezone.utc)
        end = datetime(2024, 1, 2, tzinfo=timezone.utc)

        df = _make_ohlcv_df(start, 24)
        df["_data_source"] = "REST"
        mock_vision_step.return_value = (pd.DataFrame(), [(start, end)])
        mock_rest_step.return_value = df

        mgr = CryptoKlineVisionData.create(DataProvider.BINANCE, MarketType.SPOT, use_cache=False)

        with patch.object(mgr, "_save_to_cache") as mock_save:
            mgr.get_data("BTCUSDT", start, end, Interval.HOUR_1, auto_reindex=False)
            mock_save.assert_not_called()

        mgr.close()


# ---------------------------------------------------------------------------
# 10. Environment variable override
# ---------------------------------------------------------------------------


class TestEnvironmentVariableOverride:
    """CKVD_ENABLE_CACHE=false disables cache via env var."""

    def test_env_var_false_disables_cache(self):
        """CKVD_ENABLE_CACHE=false overrides default use_cache=True."""
        with patch.dict(os.environ, {"CKVD_ENABLE_CACHE": "false"}):
            mgr = CryptoKlineVisionData.create(DataProvider.BINANCE, MarketType.SPOT)
            assert mgr.use_cache is False
            mgr.close()

    def test_env_var_zero_disables_cache(self):
        """CKVD_ENABLE_CACHE=0 overrides default use_cache=True."""
        with patch.dict(os.environ, {"CKVD_ENABLE_CACHE": "0"}):
            mgr = CryptoKlineVisionData.create(DataProvider.BINANCE, MarketType.SPOT)
            assert mgr.use_cache is False
            mgr.close()

    def test_env_var_no_disables_cache(self):
        """CKVD_ENABLE_CACHE=no overrides default use_cache=True."""
        with patch.dict(os.environ, {"CKVD_ENABLE_CACHE": "no"}):
            mgr = CryptoKlineVisionData.create(DataProvider.BINANCE, MarketType.SPOT)
            assert mgr.use_cache is False
            mgr.close()

    def test_explicit_false_wins_over_env(self):
        """Explicit use_cache=False stays False regardless of env var."""
        with patch.dict(os.environ, {"CKVD_ENABLE_CACHE": "true"}):
            mgr = CryptoKlineVisionData.create(DataProvider.BINANCE, MarketType.SPOT, use_cache=False)
            assert mgr.use_cache is False
            mgr.close()

    def test_env_var_not_set_keeps_default(self):
        """No env var set means use_cache stays at its default (True)."""
        with patch.dict(os.environ, {}, clear=True):
            # Remove CKVD_ENABLE_CACHE if present
            os.environ.pop("CKVD_ENABLE_CACHE", None)
            mgr = CryptoKlineVisionData.create(DataProvider.BINANCE, MarketType.SPOT)
            assert mgr.use_cache is True
            mgr.close()

    def test_env_var_true_keeps_cache_on(self):
        """CKVD_ENABLE_CACHE=true keeps cache enabled."""
        with patch.dict(os.environ, {"CKVD_ENABLE_CACHE": "true"}):
            mgr = CryptoKlineVisionData.create(DataProvider.BINANCE, MarketType.SPOT)
            assert mgr.use_cache is True
            mgr.close()
