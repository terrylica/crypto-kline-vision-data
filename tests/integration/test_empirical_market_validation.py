"""Comprehensive empirical validation across markets, symbols, and intervals.

This test suite performs real API calls to validate data integrity
across a broad spectrum of market conditions.

GitHub Issue #10: Data availability validation
GitHub Issue #11: Memory efficiency

Run with: uv run -p 3.13 pytest tests/integration/test_empirical_market_validation.py -v
"""

import gc
import tracemalloc
from datetime import datetime, timedelta, timezone

import pandas as pd
import pytest

from data_source_manager import DataProvider, DataSourceManager, Interval, MarketType


# =============================================================================
# Test Configuration
# =============================================================================

# Historical range safe for Vision API (>48h old)
EMPIRICAL_END = datetime.now(timezone.utc) - timedelta(days=3)
EMPIRICAL_START = EMPIRICAL_END - timedelta(days=7)

# Symbols to test per market type
SPOT_SYMBOLS = [
    "BTCUSDT",
    "ETHUSDT",
    "BNBUSDT",
    "SOLUSDT",
    "XRPUSDT",
    "ADAUSDT",
    "DOGEUSDT",
    "AVAXUSDT",
    "DOTUSDT",
    "LINKUSDT",
]

FUTURES_USDT_SYMBOLS = [
    "BTCUSDT",
    "ETHUSDT",
    "BNBUSDT",
    "SOLUSDT",
    "XRPUSDT",
    "ADAUSDT",
    "DOGEUSDT",
    "AVAXUSDT",
    "DOTUSDT",
    "LINKUSDT",
]

FUTURES_COIN_SYMBOLS = [
    "BTCUSD_PERP",
    "ETHUSD_PERP",
    "BNBUSD_PERP",
    "XRPUSD_PERP",
    "ADAUSD_PERP",
    "DOGEUSD_PERP",
    "DOTUSD_PERP",
    "LINKUSD_PERP",
]

# Intervals to validate
TEST_INTERVALS = [
    Interval.MINUTE_1,
    Interval.MINUTE_5,
    Interval.MINUTE_15,
    Interval.HOUR_1,
    Interval.HOUR_4,
    Interval.DAY_1,
]


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def spot_manager():
    """SPOT market manager."""
    manager = DataSourceManager.create(DataProvider.BINANCE, MarketType.SPOT)
    yield manager
    manager.close()


@pytest.fixture
def futures_usdt_manager():
    """FUTURES_USDT market manager."""
    manager = DataSourceManager.create(DataProvider.BINANCE, MarketType.FUTURES_USDT)
    yield manager
    manager.close()


@pytest.fixture
def futures_coin_manager():
    """FUTURES_COIN market manager."""
    manager = DataSourceManager.create(DataProvider.BINANCE, MarketType.FUTURES_COIN)
    yield manager
    manager.close()


# =============================================================================
# Data Validation Helpers
# =============================================================================


def validate_ohlcv_integrity(df: pd.DataFrame, symbol: str, market_type: str) -> dict:
    """Comprehensive OHLCV data validation.

    Returns dict with validation results and any issues found.
    """
    issues = []
    stats = {
        "symbol": symbol,
        "market_type": market_type,
        "row_count": len(df),
        "issues": issues,
    }

    if df.empty:
        issues.append("DataFrame is empty")
        return stats

    # 1. Index validation
    if df.index.name != "open_time":
        issues.append(f"Index name is '{df.index.name}', expected 'open_time'")

    if not df.index.is_monotonic_increasing:
        issues.append("Timestamps are not monotonically increasing")

    if df.index.has_duplicates:
        dup_count = df.index.duplicated().sum()
        issues.append(f"Found {dup_count} duplicate timestamps")

    # 2. Required columns
    required_cols = ["open", "high", "low", "close", "volume"]
    missing_cols = [c for c in required_cols if c not in df.columns]
    if missing_cols:
        issues.append(f"Missing columns: {missing_cols}")
        return stats  # Can't continue validation

    # 3. OHLCV logic validation
    # High >= Low
    high_low_violations = (df["high"] < df["low"]).sum()
    if high_low_violations > 0:
        issues.append(f"{high_low_violations} rows where high < low")

    # High >= Open and High >= Close
    high_open_violations = (df["high"] < df["open"]).sum()
    high_close_violations = (df["high"] < df["close"]).sum()
    if high_open_violations > 0:
        issues.append(f"{high_open_violations} rows where high < open")
    if high_close_violations > 0:
        issues.append(f"{high_close_violations} rows where high < close")

    # Low <= Open and Low <= Close
    low_open_violations = (df["low"] > df["open"]).sum()
    low_close_violations = (df["low"] > df["close"]).sum()
    if low_open_violations > 0:
        issues.append(f"{low_open_violations} rows where low > open")
    if low_close_violations > 0:
        issues.append(f"{low_close_violations} rows where low > close")

    # Positive prices
    negative_prices = (
        (df["open"] <= 0).sum()
        + (df["high"] <= 0).sum()
        + (df["low"] <= 0).sum()
        + (df["close"] <= 0).sum()
    )
    if negative_prices > 0:
        issues.append(f"{negative_prices} non-positive price values")

    # Non-negative volume
    negative_volume = (df["volume"] < 0).sum()
    if negative_volume > 0:
        issues.append(f"{negative_volume} negative volume values")

    # 4. Time range stats
    stats["start_time"] = df.index.min().isoformat()
    stats["end_time"] = df.index.max().isoformat()
    stats["time_span_hours"] = (df.index.max() - df.index.min()).total_seconds() / 3600

    # 5. Price stats
    stats["avg_close"] = float(df["close"].mean())
    stats["min_close"] = float(df["close"].min())
    stats["max_close"] = float(df["close"].max())
    stats["total_volume"] = float(df["volume"].sum())

    stats["valid"] = len(issues) == 0
    return stats


def validate_interval_spacing(df: pd.DataFrame, interval: Interval) -> dict:
    """Validate that candle spacing matches the interval."""
    if len(df) < 2:
        return {"valid": True, "checked": False, "reason": "Not enough rows"}

    expected_seconds = interval.to_seconds()
    actual_deltas = df.index.to_series().diff().dt.total_seconds().dropna()

    # Allow small tolerance for edge cases
    tolerance = expected_seconds * 0.01  # 1% tolerance

    correct_spacing = (
        (actual_deltas >= expected_seconds - tolerance)
        & (actual_deltas <= expected_seconds + tolerance)
    ).sum()

    total_gaps = len(actual_deltas)
    spacing_accuracy = correct_spacing / total_gaps if total_gaps > 0 else 0

    return {
        "valid": spacing_accuracy >= 0.95,  # 95% correct spacing
        "checked": True,
        "expected_seconds": expected_seconds,
        "spacing_accuracy": spacing_accuracy,
        "total_intervals": total_gaps,
        "correct_intervals": int(correct_spacing),
    }


# =============================================================================
# SPOT Market Tests
# =============================================================================


@pytest.mark.integration
class TestSpotMarketEmpirical:
    """Empirical validation of SPOT market data."""

    @pytest.mark.parametrize("symbol", SPOT_SYMBOLS)
    def test_spot_symbol_data_integrity(self, spot_manager, symbol):
        """Validate data integrity for each SPOT symbol."""
        df = spot_manager.get_data(
            symbol=symbol,
            start_time=EMPIRICAL_START,
            end_time=EMPIRICAL_END,
            interval=Interval.HOUR_1,
        )

        result = validate_ohlcv_integrity(df, symbol, "SPOT")

        assert result["valid"], f"SPOT {symbol} validation failed: {result['issues']}"
        assert result["row_count"] > 100, f"SPOT {symbol}: Expected >100 rows, got {result['row_count']}"

    @pytest.mark.parametrize("interval", TEST_INTERVALS)
    def test_spot_interval_coverage(self, spot_manager, interval):
        """Validate different intervals return correct spacing."""
        # Adjust time range based on interval to avoid too many rows
        if interval == Interval.MINUTE_1:
            start = EMPIRICAL_END - timedelta(hours=6)
        elif interval == Interval.MINUTE_5:
            start = EMPIRICAL_END - timedelta(days=1)
        elif interval == Interval.MINUTE_15:
            start = EMPIRICAL_END - timedelta(days=2)
        else:
            start = EMPIRICAL_START

        df = spot_manager.get_data(
            symbol="BTCUSDT",
            start_time=start,
            end_time=EMPIRICAL_END,
            interval=interval,
        )

        spacing_result = validate_interval_spacing(df, interval)

        assert spacing_result["valid"], (
            f"SPOT BTCUSDT {interval.value} spacing validation failed: "
            f"{spacing_result['spacing_accuracy']:.1%} correct"
        )


# =============================================================================
# FUTURES_USDT Market Tests
# =============================================================================


@pytest.mark.integration
class TestFuturesUsdtMarketEmpirical:
    """Empirical validation of FUTURES_USDT market data."""

    @pytest.mark.parametrize("symbol", FUTURES_USDT_SYMBOLS)
    def test_futures_usdt_symbol_data_integrity(self, futures_usdt_manager, symbol):
        """Validate data integrity for each FUTURES_USDT symbol."""
        df = futures_usdt_manager.get_data(
            symbol=symbol,
            start_time=EMPIRICAL_START,
            end_time=EMPIRICAL_END,
            interval=Interval.HOUR_1,
        )

        result = validate_ohlcv_integrity(df, symbol, "FUTURES_USDT")

        assert result["valid"], f"FUTURES_USDT {symbol} validation failed: {result['issues']}"
        assert result["row_count"] > 100, f"FUTURES_USDT {symbol}: Expected >100 rows, got {result['row_count']}"

    @pytest.mark.parametrize("interval", TEST_INTERVALS)
    def test_futures_usdt_interval_coverage(self, futures_usdt_manager, interval):
        """Validate different intervals return correct spacing."""
        if interval == Interval.MINUTE_1:
            start = EMPIRICAL_END - timedelta(hours=6)
        elif interval == Interval.MINUTE_5:
            start = EMPIRICAL_END - timedelta(days=1)
        elif interval == Interval.MINUTE_15:
            start = EMPIRICAL_END - timedelta(days=2)
        else:
            start = EMPIRICAL_START

        df = futures_usdt_manager.get_data(
            symbol="BTCUSDT",
            start_time=start,
            end_time=EMPIRICAL_END,
            interval=interval,
        )

        spacing_result = validate_interval_spacing(df, interval)

        assert spacing_result["valid"], (
            f"FUTURES_USDT BTCUSDT {interval.value} spacing validation failed: "
            f"{spacing_result['spacing_accuracy']:.1%} correct"
        )


# =============================================================================
# FUTURES_COIN Market Tests
# =============================================================================


@pytest.mark.integration
class TestFuturesCoinMarketEmpirical:
    """Empirical validation of FUTURES_COIN market data."""

    @pytest.mark.parametrize("symbol", FUTURES_COIN_SYMBOLS)
    def test_futures_coin_symbol_data_integrity(self, futures_coin_manager, symbol):
        """Validate data integrity for each FUTURES_COIN symbol."""
        df = futures_coin_manager.get_data(
            symbol=symbol,
            start_time=EMPIRICAL_START,
            end_time=EMPIRICAL_END,
            interval=Interval.HOUR_1,
        )

        result = validate_ohlcv_integrity(df, symbol, "FUTURES_COIN")

        assert result["valid"], f"FUTURES_COIN {symbol} validation failed: {result['issues']}"
        assert result["row_count"] > 100, f"FUTURES_COIN {symbol}: Expected >100 rows, got {result['row_count']}"

    @pytest.mark.parametrize("interval", TEST_INTERVALS)
    def test_futures_coin_interval_coverage(self, futures_coin_manager, interval):
        """Validate different intervals return correct spacing."""
        if interval == Interval.MINUTE_1:
            start = EMPIRICAL_END - timedelta(hours=6)
        elif interval == Interval.MINUTE_5:
            start = EMPIRICAL_END - timedelta(days=1)
        elif interval == Interval.MINUTE_15:
            start = EMPIRICAL_END - timedelta(days=2)
        else:
            start = EMPIRICAL_START

        df = futures_coin_manager.get_data(
            symbol="BTCUSD_PERP",
            start_time=start,
            end_time=EMPIRICAL_END,
            interval=interval,
        )

        spacing_result = validate_interval_spacing(df, interval)

        assert spacing_result["valid"], (
            f"FUTURES_COIN BTCUSD_PERP {interval.value} spacing validation failed: "
            f"{spacing_result['spacing_accuracy']:.1%} correct"
        )


# =============================================================================
# Cross-Market Consistency Tests
# =============================================================================


@pytest.mark.integration
class TestCrossMarketConsistency:
    """Validate data consistency across different markets for same underlying."""

    def test_btc_price_correlation_spot_vs_futures(
        self, spot_manager, futures_usdt_manager
    ):
        """BTC prices should be highly correlated across SPOT and FUTURES_USDT."""
        spot_df = spot_manager.get_data(
            symbol="BTCUSDT",
            start_time=EMPIRICAL_START,
            end_time=EMPIRICAL_END,
            interval=Interval.HOUR_1,
        )

        futures_df = futures_usdt_manager.get_data(
            symbol="BTCUSDT",
            start_time=EMPIRICAL_START,
            end_time=EMPIRICAL_END,
            interval=Interval.HOUR_1,
        )

        # Align on common timestamps
        common_index = spot_df.index.intersection(futures_df.index)
        assert len(common_index) > 50, f"Not enough common timestamps: {len(common_index)}"

        spot_aligned = spot_df.loc[common_index, "close"]
        futures_aligned = futures_df.loc[common_index, "close"]

        correlation = spot_aligned.corr(futures_aligned)
        assert correlation > 0.99, f"BTC SPOT vs FUTURES correlation too low: {correlation:.4f}"

    def test_eth_price_correlation_spot_vs_futures(
        self, spot_manager, futures_usdt_manager
    ):
        """ETH prices should be highly correlated across SPOT and FUTURES_USDT."""
        spot_df = spot_manager.get_data(
            symbol="ETHUSDT",
            start_time=EMPIRICAL_START,
            end_time=EMPIRICAL_END,
            interval=Interval.HOUR_1,
        )

        futures_df = futures_usdt_manager.get_data(
            symbol="ETHUSDT",
            start_time=EMPIRICAL_START,
            end_time=EMPIRICAL_END,
            interval=Interval.HOUR_1,
        )

        common_index = spot_df.index.intersection(futures_df.index)
        assert len(common_index) > 50, f"Not enough common timestamps: {len(common_index)}"

        spot_aligned = spot_df.loc[common_index, "close"]
        futures_aligned = futures_df.loc[common_index, "close"]

        correlation = spot_aligned.corr(futures_aligned)
        assert correlation > 0.99, f"ETH SPOT vs FUTURES correlation too low: {correlation:.4f}"

    def test_futures_usdt_vs_coin_same_underlying(
        self, futures_usdt_manager, futures_coin_manager
    ):
        """BTC USDT-margined and coin-margined futures should be correlated."""
        usdt_df = futures_usdt_manager.get_data(
            symbol="BTCUSDT",
            start_time=EMPIRICAL_START,
            end_time=EMPIRICAL_END,
            interval=Interval.HOUR_1,
        )

        coin_df = futures_coin_manager.get_data(
            symbol="BTCUSD_PERP",
            start_time=EMPIRICAL_START,
            end_time=EMPIRICAL_END,
            interval=Interval.HOUR_1,
        )

        common_index = usdt_df.index.intersection(coin_df.index)
        assert len(common_index) > 50, f"Not enough common timestamps: {len(common_index)}"

        usdt_aligned = usdt_df.loc[common_index, "close"]
        coin_aligned = coin_df.loc[common_index, "close"]

        correlation = usdt_aligned.corr(coin_aligned)
        assert correlation > 0.98, f"BTC USDT vs COIN futures correlation too low: {correlation:.4f}"


# =============================================================================
# Memory Efficiency Tests
# =============================================================================


@pytest.mark.integration
class TestMemoryEfficiencyEmpirical:
    """Validate memory efficiency during real data fetches."""

    def test_multi_symbol_fetch_memory_bounded(self, futures_usdt_manager):
        """Fetching multiple symbols should have bounded memory growth."""
        gc.collect()
        tracemalloc.start()
        baseline = tracemalloc.get_traced_memory()[0]

        symbols = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT"]
        dataframes = []

        for symbol in symbols:
            df = futures_usdt_manager.get_data(
                symbol=symbol,
                start_time=EMPIRICAL_START,
                end_time=EMPIRICAL_END,
                interval=Interval.HOUR_1,
            )
            dataframes.append(df)

        peak = tracemalloc.get_traced_memory()[1]
        tracemalloc.stop()

        # Clean up
        del dataframes
        gc.collect()

        peak_mb = (peak - baseline) / (1024 * 1024)
        # 5 symbols * 7 days * 24 hours = 840 rows each = 4200 total
        # Should be well under 50MB
        assert peak_mb < 50, f"Multi-symbol fetch used {peak_mb:.1f}MB (expected <50MB)"

    def test_large_interval_fetch_memory_efficient(self, spot_manager):
        """Large 1-minute fetch should be memory efficient."""
        # 3 days of 1-minute data = 4320 rows
        start = EMPIRICAL_END - timedelta(days=3)

        gc.collect()
        tracemalloc.start()
        baseline = tracemalloc.get_traced_memory()[0]

        df = spot_manager.get_data(
            symbol="BTCUSDT",
            start_time=start,
            end_time=EMPIRICAL_END,
            interval=Interval.MINUTE_1,
        )

        peak = tracemalloc.get_traced_memory()[1]
        tracemalloc.stop()

        peak_mb = (peak - baseline) / (1024 * 1024)
        rows = len(df)

        # Memory efficiency: should be < 1KB per row for OHLCV data
        bytes_per_row = (peak - baseline) / rows if rows > 0 else 0
        assert bytes_per_row < 1024, f"Memory per row: {bytes_per_row:.0f} bytes (expected <1024)"
        assert peak_mb < 30, f"3-day 1m fetch used {peak_mb:.1f}MB (expected <30MB)"


# =============================================================================
# FCP Source Tracking Tests
# =============================================================================


@pytest.mark.integration
class TestFCPSourceTracking:
    """Validate FCP source tracking across markets."""

    @pytest.mark.parametrize(
        "market_type,symbol",
        [
            (MarketType.SPOT, "BTCUSDT"),
            (MarketType.FUTURES_USDT, "BTCUSDT"),
            (MarketType.FUTURES_COIN, "BTCUSD_PERP"),
        ],
    )
    def test_source_tracking_enabled(self, market_type, symbol):
        """Verify _data_source column is present when requested."""
        manager = DataSourceManager.create(DataProvider.BINANCE, market_type)

        df = manager.get_data(
            symbol=symbol,
            start_time=EMPIRICAL_START,
            end_time=EMPIRICAL_END,
            interval=Interval.HOUR_1,
            include_source_info=True,
        )

        manager.close()

        assert "_data_source" in df.columns, f"Missing _data_source for {market_type.name}"
        sources = df["_data_source"].unique().tolist()
        valid_sources = ["CACHE", "VISION", "REST"]
        for src in sources:
            assert src in valid_sources, f"Invalid source '{src}' for {market_type.name}"

    def test_historical_data_uses_cache_or_vision(self, futures_usdt_manager):
        """Historical data (>7 days old) should primarily use CACHE or VISION."""
        # Use older data that's definitely in Vision
        old_end = datetime.now(timezone.utc) - timedelta(days=10)
        old_start = old_end - timedelta(days=3)

        # First fetch to populate cache
        futures_usdt_manager.get_data(
            symbol="BTCUSDT",
            start_time=old_start,
            end_time=old_end,
            interval=Interval.HOUR_1,
            include_source_info=True,
        )

        # Second fetch should hit cache
        df = futures_usdt_manager.get_data(
            symbol="BTCUSDT",
            start_time=old_start,
            end_time=old_end,
            interval=Interval.HOUR_1,
            include_source_info=True,
        )

        if "_data_source" in df.columns:
            sources = df["_data_source"].value_counts(normalize=True)
            cache_or_vision_pct = sources.get("CACHE", 0) + sources.get("VISION", 0)
            assert cache_or_vision_pct > 0.8, (
                f"Historical data should be >80% CACHE/VISION, got {cache_or_vision_pct:.1%}"
            )


# =============================================================================
# Summary Report Test
# =============================================================================


@pytest.mark.integration
class TestEmpiricalSummaryReport:
    """Generate a summary report of all market validations."""

    def test_generate_validation_summary(
        self, spot_manager, futures_usdt_manager, futures_coin_manager
    ):
        """Generate comprehensive validation summary across all markets."""
        results = []

        # Test each market type with multiple symbols
        test_cases = [
            (spot_manager, MarketType.SPOT, ["BTCUSDT", "ETHUSDT", "SOLUSDT"]),
            (futures_usdt_manager, MarketType.FUTURES_USDT, ["BTCUSDT", "ETHUSDT", "SOLUSDT"]),
            (futures_coin_manager, MarketType.FUTURES_COIN, ["BTCUSD_PERP", "ETHUSD_PERP"]),
        ]

        for manager, market_type, symbols in test_cases:
            for symbol in symbols:
                df = manager.get_data(
                    symbol=symbol,
                    start_time=EMPIRICAL_START,
                    end_time=EMPIRICAL_END,
                    interval=Interval.HOUR_1,
                )
                result = validate_ohlcv_integrity(df, symbol, market_type.name)
                results.append(result)

        # Print summary
        print("\n" + "=" * 60)
        print("EMPIRICAL VALIDATION SUMMARY")
        print("=" * 60)
        print(f"Time range: {EMPIRICAL_START.date()} to {EMPIRICAL_END.date()}")
        print("Interval: 1 hour")
        print("-" * 60)

        all_valid = True
        for r in results:
            status = "✓" if r["valid"] else "✗"
            print(f"{status} {r['market_type']:15} {r['symbol']:15} rows={r['row_count']:5}")
            if not r["valid"]:
                all_valid = False
                for issue in r["issues"]:
                    print(f"    └─ {issue}")

        print("-" * 60)
        valid_count = sum(1 for r in results if r["valid"])
        print(f"Total: {valid_count}/{len(results)} validations passed")
        print("=" * 60)

        assert all_valid, "Some validations failed - see report above"
