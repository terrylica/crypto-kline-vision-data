"""Integration tests for data availability validation with fail-loud behavior.

GitHub Issue #10: Wire data availability validation into get_data() with fail-loud behavior.

Tests verify:
- DataNotAvailableError is raised for requests before symbol listing
- Cross-market futures warnings are emitted for SPOT requests
- Normal data retrieval works after validation passes
"""

from datetime import datetime, timedelta, timezone

import pytest

from data_source_manager import DataProvider, DataSourceManager, Interval, MarketType
from data_source_manager.utils.for_core.vision_exceptions import DataNotAvailableError


@pytest.mark.integration
class TestDataNotAvailableError:
    """Tests for fail-loud behavior when requesting data before symbol listing."""

    def test_requesting_data_before_listing_raises_error(self):
        """Requesting BTCUSDT futures data before 2019-12-31 should raise DataNotAvailableError."""
        manager = DataSourceManager.create(DataProvider.BINANCE, MarketType.FUTURES_USDT)

        # BTCUSDT futures listed 2019-12-31
        ancient_start = datetime(2015, 1, 1, tzinfo=timezone.utc)
        ancient_end = datetime(2015, 1, 2, tzinfo=timezone.utc)

        try:
            with pytest.raises(DataNotAvailableError) as exc_info:
                manager.get_data("BTCUSDT", ancient_start, ancient_end, Interval.HOUR_1)

            # Verify error message contains useful information
            error = exc_info.value
            assert "FAIL-LOUD" in str(error)
            assert "BTCUSDT" in str(error)
            assert error.symbol == "BTCUSDT"
            assert error.market_type == "FUTURES_USDT"
            assert error.requested_start == ancient_start
            assert error.earliest_available == datetime(2019, 12, 31, tzinfo=timezone.utc)
        finally:
            manager.close()

    def test_requesting_data_after_listing_succeeds(self):
        """Requesting data after listing date should succeed (not raise error)."""
        manager = DataSourceManager.create(DataProvider.BINANCE, MarketType.FUTURES_USDT)

        # Use a historical range that's after listing but old enough for Vision API
        end_time = datetime.now(timezone.utc) - timedelta(days=7)
        start_time = end_time - timedelta(days=1)

        try:
            # Should not raise DataNotAvailableError
            df = manager.get_data("BTCUSDT", start_time, end_time, Interval.HOUR_1)

            # Should return valid data
            assert df is not None
            assert len(df) > 0
        finally:
            manager.close()

    def test_error_attributes_accessible(self):
        """DataNotAvailableError should have accessible attributes for programmatic handling."""
        manager = DataSourceManager.create(DataProvider.BINANCE, MarketType.FUTURES_USDT)

        ancient_start = datetime(2018, 1, 1, tzinfo=timezone.utc)
        ancient_end = datetime(2018, 1, 2, tzinfo=timezone.utc)

        try:
            with pytest.raises(DataNotAvailableError) as exc_info:
                manager.get_data("BTCUSDT", ancient_start, ancient_end, Interval.HOUR_1)

            error = exc_info.value
            # All attributes should be accessible for programmatic error handling
            assert hasattr(error, "symbol")
            assert hasattr(error, "market_type")
            assert hasattr(error, "requested_start")
            assert hasattr(error, "earliest_available")

            # Verify isoformat works (used in error message)
            assert "2019-12-31" in error.earliest_available.isoformat()
        finally:
            manager.close()


@pytest.mark.integration
class TestCrossMarketFuturesWarning:
    """Tests for cross-market futures counterpart warnings on SPOT requests."""

    def test_spot_request_before_futures_listing_emits_warning(self, capsys):
        """SPOT request before futures listing should emit stderr warning."""
        manager = DataSourceManager.create(DataProvider.BINANCE, MarketType.SPOT)

        # Request 2018 spot data - before BTCUSDT futures was listed (2019-12-31)
        # Note: This may fail if spot data doesn't exist that far back,
        # but the warning should still be emitted before any data fetch attempt
        try:
            # Use a date range where spot data exists but futures didn't yet
            start_time = datetime(2018, 6, 1, tzinfo=timezone.utc)
            end_time = datetime(2018, 6, 2, tzinfo=timezone.utc)

            # Data fetch might fail for various reasons (data not available in Vision/REST)
            # but the warning should have been emitted before the fetch attempt
            data_fetch_succeeded = False
            try:
                _df = manager.get_data("BTCUSDT", start_time, end_time, Interval.HOUR_1)
                data_fetch_succeeded = True
            except (RuntimeError, ValueError, OSError) as e:
                # Expected - old data may not be available in Vision/REST
                # The warning should still have been emitted before this point
                _ = e  # Acknowledge the exception

            # Check stderr for the warning (emitted before data fetch attempt)
            captured = capsys.readouterr()
            assert "FUTURES COUNTERPART WARNING" in captured.err, (
                f"Expected futures warning in stderr. "
                f"Data fetch succeeded: {data_fetch_succeeded}. stderr: {captured.err[:200]}"
            )
            assert "BTCUSDT" in captured.err
        finally:
            manager.close()

    def test_spot_request_after_futures_listing_no_warning(self, capsys):
        """SPOT request after futures listing should NOT emit warning."""
        manager = DataSourceManager.create(DataProvider.BINANCE, MarketType.SPOT)

        # Use recent date range where both spot and futures exist
        end_time = datetime.now(timezone.utc) - timedelta(days=7)
        start_time = end_time - timedelta(days=1)

        try:
            _df = manager.get_data("BTCUSDT", start_time, end_time, Interval.HOUR_1)

            # Check stderr - should NOT have futures warning
            captured = capsys.readouterr()
            assert "FUTURES COUNTERPART WARNING" not in captured.err
        finally:
            manager.close()

    def test_futures_request_no_cross_market_warning(self, capsys):
        """FUTURES_USDT request should NOT emit cross-market warning (even for old dates)."""
        manager = DataSourceManager.create(DataProvider.BINANCE, MarketType.FUTURES_USDT)

        # Use a valid date range after futures listing
        end_time = datetime.now(timezone.utc) - timedelta(days=7)
        start_time = end_time - timedelta(days=1)

        try:
            _df = manager.get_data("BTCUSDT", start_time, end_time, Interval.HOUR_1)

            # Check stderr - should NOT have cross-market warning for futures requests
            captured = capsys.readouterr()
            assert "FUTURES COUNTERPART WARNING" not in captured.err
        finally:
            manager.close()


@pytest.mark.integration
class TestTraceIdLogging:
    """Tests for trace_id correlation in structured logging."""

    def test_trace_id_generation_works(self):
        """Verify trace_id generation method works correctly."""
        from data_source_manager.utils.loguru_setup import logger

        # Verify trace_id generation works
        trace_id = logger.generate_trace_id()

        # Trace ID should be 8-character hex string
        assert trace_id is not None
        assert len(trace_id) == 8
        assert all(c in "0123456789abcdef-" for c in trace_id)

    def test_get_data_completes_with_trace_id_binding(self):
        """get_data() should complete successfully with trace_id binding enabled."""
        manager = DataSourceManager.create(DataProvider.BINANCE, MarketType.FUTURES_USDT)

        end_time = datetime.now(timezone.utc) - timedelta(days=7)
        start_time = end_time - timedelta(days=1)

        try:
            # This should complete without error even with trace_id binding
            df = manager.get_data("BTCUSDT", start_time, end_time, Interval.HOUR_1)

            # Verify we got data back
            assert df is not None
            assert len(df) > 0
        finally:
            manager.close()


@pytest.mark.integration
class TestUnknownSymbolBehavior:
    """Tests for behavior with unknown symbols."""

    def test_unknown_symbol_allows_request(self, capsys):
        """Unknown symbols should be allowed (not blocked by availability check)."""
        import tenacity

        manager = DataSourceManager.create(DataProvider.BINANCE, MarketType.FUTURES_USDT)

        # Use a very recent date range to avoid Vision API issues
        end_time = datetime.now(timezone.utc) - timedelta(hours=1)
        start_time = end_time - timedelta(hours=2)

        try:
            # This symbol doesn't exist - should NOT raise DataNotAvailableError
            # (availability check allows unknown symbols)
            # It will likely fail with empty data or API error from Binance
            try:
                _df = manager.get_data(
                    "UNKNOWNSYMBOL123", start_time, end_time, Interval.HOUR_1
                )
                # If we get here, data was returned (unlikely but acceptable)
            except DataNotAvailableError:
                pytest.fail("Unknown symbols should not raise DataNotAvailableError")
            except (RuntimeError, ValueError, OSError, KeyError, tenacity.RetryError) as e:
                # Expected - Binance API will reject unknown symbols with various errors
                # RetryError occurs after tenacity exhausts retries for invalid symbol
                # The key assertion is that DataNotAvailableError was NOT raised
                _ = e  # Acknowledge the exception
        finally:
            manager.close()
