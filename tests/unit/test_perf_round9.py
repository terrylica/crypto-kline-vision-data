"""Performance benchmarks for Round 9: Algorithm Micro-Optimizations.

Validates that module-level constants and O(1) lookups outperform
per-call dict creation and O(n) enum scans.
"""

import timeit
from unittest.mock import patch

from ckvd.utils.for_core.rest_client_utils import (
    _INTERVAL_BY_VALUE,
    _INTERVAL_MS,
    get_interval_ms,
    parse_interval_string,
)
from ckvd.utils.market.enums import (
    Interval,
    _INTERVAL_SECONDS,
)


class TestGetIntervalMsPerformance:
    """Benchmark: module constant _INTERVAL_MS vs per-call dict creation."""

    def test_interval_ms_module_constant_exists(self):
        """_INTERVAL_MS should be a module-level dict with all intervals."""
        assert isinstance(_INTERVAL_MS, dict)
        assert len(_INTERVAL_MS) == len(Interval)
        for interval in Interval:
            assert interval in _INTERVAL_MS

    def test_interval_ms_values_correct(self):
        """Module constant values must match the original per-call dict values."""
        expected = {
            Interval.SECOND_1: 1_000,
            Interval.MINUTE_1: 60_000,
            Interval.MINUTE_5: 300_000,
            Interval.HOUR_1: 3_600_000,
            Interval.HOUR_4: 14_400_000,
            Interval.DAY_1: 86_400_000,
            Interval.WEEK_1: 604_800_000,
            Interval.MONTH_1: 2_592_000_000,
        }
        for interval, expected_ms in expected.items():
            assert get_interval_ms(interval) == expected_ms

    def test_interval_ms_benchmark(self):
        """Module constant lookup should be >=5x faster than per-call dict build."""
        iterations = 10_000

        # Simulate the old per-call pattern
        def old_get_interval_ms(interval):
            interval_map = {
                Interval.SECOND_1: 1_000,
                Interval.MINUTE_1: 60_000,
                Interval.MINUTE_3: 180_000,
                Interval.MINUTE_5: 300_000,
                Interval.MINUTE_15: 900_000,
                Interval.MINUTE_30: 1_800_000,
                Interval.HOUR_1: 3_600_000,
                Interval.HOUR_2: 7_200_000,
                Interval.HOUR_4: 14_400_000,
                Interval.HOUR_6: 21_600_000,
                Interval.HOUR_8: 28_800_000,
                Interval.HOUR_12: 43_200_000,
                Interval.DAY_1: 86_400_000,
                Interval.DAY_3: 259_200_000,
                Interval.WEEK_1: 604_800_000,
                Interval.MONTH_1: 2_592_000_000,
            }
            return interval_map.get(interval, 60_000)

        old_time = timeit.timeit(lambda: old_get_interval_ms(Interval.HOUR_1), number=iterations)
        new_time = timeit.timeit(lambda: get_interval_ms(Interval.HOUR_1), number=iterations)

        speedup = old_time / new_time
        assert speedup >= 5, f"Expected >=5x speedup, got {speedup:.1f}x (old={old_time:.4f}s, new={new_time:.4f}s)"


class TestParseIntervalStringPerformance:
    """Benchmark: O(1) _INTERVAL_BY_VALUE lookup vs O(n) enum scan."""

    def test_interval_by_value_exists(self):
        """_INTERVAL_BY_VALUE should map all interval string values to enum members."""
        assert isinstance(_INTERVAL_BY_VALUE, dict)
        for interval in Interval:
            assert interval.value in _INTERVAL_BY_VALUE
            assert _INTERVAL_BY_VALUE[interval.value] is interval

    def test_parse_correctness(self):
        """parse_interval_string should return correct enum for all valid strings."""
        for interval in Interval:
            result = parse_interval_string(interval.value)
            assert result is interval, f"Expected {interval}, got {result} for '{interval.value}'"

    def test_parse_interval_benchmark(self):
        """O(1) dict lookup should be >=5x faster than O(n) generator scan."""
        iterations = 10_000

        # Simulate the old O(n) pattern
        def old_parse(interval_str):
            return next((i for i in Interval if i.value == interval_str), None)

        # New O(1) pattern
        def new_parse(interval_str):
            return _INTERVAL_BY_VALUE.get(interval_str)

        old_time = timeit.timeit(lambda: old_parse("1h"), number=iterations)
        new_time = timeit.timeit(lambda: new_parse("1h"), number=iterations)

        speedup = old_time / new_time
        assert speedup >= 5, f"Expected >=5x speedup, got {speedup:.1f}x (old={old_time:.4f}s, new={new_time:.4f}s)"


class TestIntervalToSecondsPerformance:
    """Benchmark: pre-computed _INTERVAL_SECONDS lookup vs per-call regex + dict."""

    def test_interval_seconds_constant_exists(self):
        """_INTERVAL_SECONDS should contain all Interval members."""
        assert isinstance(_INTERVAL_SECONDS, dict)
        assert len(_INTERVAL_SECONDS) == len(Interval)

    def test_to_seconds_correctness(self):
        """to_seconds() results must match expected values."""
        expected = {
            Interval.SECOND_1: 1,
            Interval.MINUTE_1: 60,
            Interval.MINUTE_5: 300,
            Interval.HOUR_1: 3600,
            Interval.HOUR_4: 14400,
            Interval.DAY_1: 86400,
            Interval.WEEK_1: 604800,
            Interval.MONTH_1: 2592000,
        }
        for interval, expected_secs in expected.items():
            assert interval.to_seconds() == expected_secs

    def test_to_seconds_benchmark(self):
        """Pre-computed lookup should be >=2x faster than per-call regex + dict build."""
        import re
        iterations = 10_000

        # Simulate the original per-call pattern (rebuilds dict every call)
        def old_to_seconds(interval):
            value = interval.value
            match = re.compile(r"(\d+)([smhdwM])").match(value)
            num, unit = match.groups()
            multipliers = {"s": 1, "m": 60, "h": 3600, "d": 86400, "w": 604800, "M": 2592000}
            return int(num) * multipliers[unit]

        old_time = timeit.timeit(lambda: old_to_seconds(Interval.HOUR_1), number=iterations)
        new_time = timeit.timeit(lambda: Interval.HOUR_1.to_seconds(), number=iterations)

        speedup = old_time / new_time
        assert speedup >= 2, f"Expected >=2x speedup, got {speedup:.1f}x (old={old_time:.4f}s, new={new_time:.4f}s)"


class TestFilterDebugGuard:
    """Verify debug functions are skipped when DEBUG is disabled."""

    def test_debug_functions_skipped_when_not_debug(self):
        """When log level > DEBUG, trace and compare functions must not be called.

        analyze_filter_conditions is always called (fail-fast safety check).
        """
        import pandas as pd
        from datetime import datetime, timedelta, timezone

        from ckvd.utils.time.filtering import filter_dataframe_by_time

        # Create a small test DataFrame
        now = datetime.now(timezone.utc)
        dates = [now - timedelta(hours=i) for i in range(10)]
        df = pd.DataFrame({"open_time": dates, "close": range(10)})

        start = now - timedelta(hours=5)
        end = now

        # Patch logger to report DEBUG as disabled and track function calls
        with (
            patch("ckvd.utils.time.filtering.logger") as mock_logger,
            patch("ckvd.utils.time.timestamp_debug.trace_dataframe_timestamps") as mock_trace,
            patch("ckvd.utils.time.timestamp_debug.compare_filtered_results") as mock_compare,
        ):
            mock_logger.isEnabledFor.return_value = False

            result = filter_dataframe_by_time(df, start, end)

            # Debug-only functions should NOT be called
            mock_trace.assert_not_called()
            mock_compare.assert_not_called()

            assert len(result) > 0


class TestCapabilitiesFastPath:
    """Verify get_market_capabilities uses dict.get() fast path."""

    def test_fast_path_returns_correct_result(self):
        """Direct dict lookup should return same result as name-based scan."""
        from ckvd.utils.market.capabilities import get_market_capabilities
        from ckvd.utils.market.enums import DataProvider, MarketType

        for mt in [MarketType.SPOT, MarketType.FUTURES_USDT, MarketType.FUTURES_COIN]:
            result = get_market_capabilities(mt, DataProvider.BINANCE)
            assert result is not None
            assert result.default_symbol is not None


class TestIntervalSupportedFrozenset:
    """Verify is_interval_supported uses cached frozenset."""

    def test_is_interval_supported_correctness(self):
        """Cached frozenset lookup must return same results as list scan."""
        from ckvd.utils.market.enums import MarketType
        from ckvd.utils.market.validation import is_interval_supported

        # SPOT supports SECOND_1, futures don't
        assert is_interval_supported(MarketType.SPOT, Interval.SECOND_1) is True
        assert is_interval_supported(MarketType.FUTURES_USDT, Interval.SECOND_1) is False

        # All market types support HOUR_1
        for mt in [MarketType.SPOT, MarketType.FUTURES_USDT, MarketType.FUTURES_COIN]:
            assert is_interval_supported(mt, Interval.HOUR_1) is True

    def test_frozenset_cache_populated(self):
        """After calling is_interval_supported, cache should be populated."""
        from ckvd.utils.market.enums import MarketType
        from ckvd.utils.market.validation import _SUPPORTED_INTERVALS_CACHE, is_interval_supported

        is_interval_supported(MarketType.SPOT, Interval.HOUR_1)
        assert MarketType.SPOT in _SUPPORTED_INTERVALS_CACHE
        assert isinstance(_SUPPORTED_INTERVALS_CACHE[MarketType.SPOT], frozenset)
