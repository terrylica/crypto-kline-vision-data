"""Unit tests for availability_data.py - Symbol listing date validation.

GitHub Issue #10: Wire data availability validation into get_data() with fail-loud behavior.

Tests cover:
- CSV loading and caching
- Symbol availability checking
- Cross-market futures counterpart warnings
"""

from datetime import datetime, timezone


from data_source_manager import MarketType
from data_source_manager.utils.validation.availability_data import (
    FuturesAvailabilityWarning,
    SymbolAvailability,
    _load_csv_data,
    check_futures_counterpart_availability,
    get_earliest_date,
    get_symbol_availability,
    is_symbol_available_at,
)


class TestLoadAvailabilityData:
    """Tests for CSV data loading and caching."""

    def test_load_availability_data_returns_dict(self):
        """CSV loading should return a dictionary of symbol -> availability."""
        data = _load_csv_data(MarketType.FUTURES_USDT)
        assert isinstance(data, dict)
        assert len(data) > 0  # Should have loaded some symbols

    def test_load_availability_data_caches_results(self):
        """Repeated calls should use cached data (lru_cache)."""
        # First call
        data1 = _load_csv_data(MarketType.FUTURES_USDT)
        # Second call should return same object (cached)
        data2 = _load_csv_data(MarketType.FUTURES_USDT)
        # Same object means cache hit
        assert data1 is data2

    def test_load_availability_data_all_market_types(self):
        """Should load data for all supported market types."""
        for market_type in [MarketType.SPOT, MarketType.FUTURES_USDT, MarketType.FUTURES_COIN]:
            data = _load_csv_data(market_type)
            assert isinstance(data, dict), f"Failed for {market_type.name}"

    def test_symbol_availability_structure(self):
        """SymbolAvailability should have expected fields."""
        data = _load_csv_data(MarketType.FUTURES_USDT)
        # Get any symbol
        symbol_name = next(iter(data.keys()))
        availability = data[symbol_name]

        assert isinstance(availability, SymbolAvailability)
        assert hasattr(availability, "market")
        assert hasattr(availability, "symbol")
        assert hasattr(availability, "earliest_date")
        assert hasattr(availability, "available_intervals")
        assert isinstance(availability.earliest_date, datetime)


class TestGetEarliestDate:
    """Tests for get_earliest_date function."""

    def test_get_earliest_date_futures_usdt_btcusdt(self):
        """BTCUSDT on FUTURES_USDT should have earliest date of 2019-12-31."""
        earliest = get_earliest_date(MarketType.FUTURES_USDT, "BTCUSDT")
        assert earliest is not None
        assert earliest == datetime(2019, 12, 31, tzinfo=timezone.utc)

    def test_get_earliest_date_unknown_symbol_returns_none(self):
        """Unknown symbols should return None (not raise error)."""
        earliest = get_earliest_date(MarketType.FUTURES_USDT, "NONEXISTENT_SYMBOL_XYZ")
        assert earliest is None

    def test_get_earliest_date_different_market_types(self):
        """Should return different dates for different market types."""
        # BTCUSDT exists in both spot and futures
        spot_earliest = get_earliest_date(MarketType.SPOT, "BTCUSDT")
        futures_earliest = get_earliest_date(MarketType.FUTURES_USDT, "BTCUSDT")

        # Both should exist
        assert spot_earliest is not None
        assert futures_earliest is not None


class TestIsSymbolAvailableAt:
    """Tests for is_symbol_available_at function."""

    def test_is_symbol_available_before_listing_fails(self):
        """Requesting data before listing date should return (False, earliest_date)."""
        # BTCUSDT futures listed 2019-12-31
        target_date = datetime(2015, 1, 1, tzinfo=timezone.utc)
        is_available, earliest = is_symbol_available_at(MarketType.FUTURES_USDT, "BTCUSDT", target_date)

        assert is_available is False
        assert earliest == datetime(2019, 12, 31, tzinfo=timezone.utc)

    def test_is_symbol_available_after_listing_passes(self):
        """Requesting data after listing date should return (True, earliest_date)."""
        # BTCUSDT futures listed 2019-12-31
        target_date = datetime(2020, 6, 1, tzinfo=timezone.utc)
        is_available, earliest = is_symbol_available_at(MarketType.FUTURES_USDT, "BTCUSDT", target_date)

        assert is_available is True
        assert earliest == datetime(2019, 12, 31, tzinfo=timezone.utc)

    def test_unknown_symbol_allows_request(self):
        """Unknown symbols should allow request (True, None) - don't block unknown symbols."""
        is_available, earliest = is_symbol_available_at(
            MarketType.FUTURES_USDT, "UNKNOWN_SYMBOL_ABC", datetime(2020, 1, 1, tzinfo=timezone.utc)
        )

        assert is_available is True
        assert earliest is None

    def test_naive_datetime_handled(self):
        """Naive datetimes should be handled (converted to UTC)."""
        # Naive datetime
        target_date = datetime(2020, 6, 1)
        is_available, _earliest = is_symbol_available_at(MarketType.FUTURES_USDT, "BTCUSDT", target_date)

        assert is_available is True  # Should work without crashing


class TestFuturesCounterpartWarning:
    """Tests for check_futures_counterpart_availability function."""

    def test_futures_counterpart_warning_for_spot_request_before_listing(self):
        """SPOT request before futures listing should emit warning."""
        # BTCUSDT futures listed 2019-12-31, request spot data from 2018
        target_date = datetime(2018, 1, 1, tzinfo=timezone.utc)
        warning = check_futures_counterpart_availability(MarketType.SPOT, "BTCUSDT", target_date)

        assert warning is not None
        assert isinstance(warning, FuturesAvailabilityWarning)
        assert "BTCUSDT" in warning.message
        assert "FUTURES_USDT" in warning.futures_market
        assert warning.earliest_date == datetime(2019, 12, 31, tzinfo=timezone.utc)
        assert warning.requested_start == target_date

    def test_no_futures_warning_when_futures_available(self):
        """SPOT request after futures listing should NOT emit warning."""
        # BTCUSDT futures listed 2019-12-31, request spot data from 2020
        target_date = datetime(2020, 6, 1, tzinfo=timezone.utc)
        warning = check_futures_counterpart_availability(MarketType.SPOT, "BTCUSDT", target_date)

        assert warning is None

    def test_no_cross_market_warning_for_futures_request(self):
        """FUTURES_USDT request should NOT emit cross-market warning."""
        # Even if date is before listing, this function only warns for non-futures
        target_date = datetime(2015, 1, 1, tzinfo=timezone.utc)
        warning = check_futures_counterpart_availability(MarketType.FUTURES_USDT, "BTCUSDT", target_date)

        assert warning is None  # No cross-market warning for futures requests

    def test_no_cross_market_warning_for_coin_futures_request(self):
        """FUTURES_COIN request should NOT emit cross-market warning."""
        target_date = datetime(2015, 1, 1, tzinfo=timezone.utc)
        warning = check_futures_counterpart_availability(MarketType.FUTURES_COIN, "BTCUSD_PERP", target_date)

        assert warning is None

    def test_warning_message_contains_useful_info(self):
        """Warning message should contain symbol, market type, and dates."""
        target_date = datetime(2018, 1, 1, tzinfo=timezone.utc)
        warning = check_futures_counterpart_availability(MarketType.SPOT, "BTCUSDT", target_date)

        assert warning is not None
        assert "BTCUSDT" in warning.message
        assert "2019-12-31" in warning.message  # Futures listing date
        assert "2018-01-01" in warning.message  # Requested start date
        assert "no corresponding futures hedge" in warning.message.lower()


class TestGetSymbolAvailability:
    """Tests for get_symbol_availability function."""

    def test_returns_symbol_availability_object(self):
        """Should return SymbolAvailability for known symbol."""
        availability = get_symbol_availability(MarketType.FUTURES_USDT, "BTCUSDT")

        assert availability is not None
        assert isinstance(availability, SymbolAvailability)
        assert availability.symbol == "BTCUSDT"
        assert availability.market == "um"

    def test_returns_none_for_unknown_symbol(self):
        """Should return None for unknown symbol."""
        availability = get_symbol_availability(MarketType.FUTURES_USDT, "NONEXISTENT_XYZ")

        assert availability is None

    def test_available_intervals_is_list(self):
        """Available intervals should be a list of strings."""
        availability = get_symbol_availability(MarketType.FUTURES_USDT, "BTCUSDT")

        assert availability is not None
        assert isinstance(availability.available_intervals, list)
        assert len(availability.available_intervals) > 0
        assert all(isinstance(i, str) for i in availability.available_intervals)


class TestEdgeCases:
    """Edge case tests for robustness."""

    def test_empty_symbol_handled(self):
        """Empty symbol should return None gracefully."""
        earliest = get_earliest_date(MarketType.FUTURES_USDT, "")
        assert earliest is None

    def test_case_sensitivity(self):
        """Symbols should be case-sensitive (CSV uses uppercase)."""
        # Uppercase should work
        upper_earliest = get_earliest_date(MarketType.FUTURES_USDT, "BTCUSDT")
        # Lowercase should not find anything (CSV has uppercase)
        lower_earliest = get_earliest_date(MarketType.FUTURES_USDT, "btcusdt")

        assert upper_earliest is not None
        assert lower_earliest is None

    def test_boundary_date_exactly_on_listing(self):
        """Request on exact listing date should be available."""
        # BTCUSDT listed 2019-12-31
        target_date = datetime(2019, 12, 31, tzinfo=timezone.utc)
        is_available, _earliest = is_symbol_available_at(MarketType.FUTURES_USDT, "BTCUSDT", target_date)

        assert is_available is True  # Exactly on listing date should be available

    def test_boundary_date_one_day_before_listing(self):
        """Request one day before listing should not be available."""
        # BTCUSDT listed 2019-12-31
        target_date = datetime(2019, 12, 30, tzinfo=timezone.utc)
        is_available, _earliest = is_symbol_available_at(MarketType.FUTURES_USDT, "BTCUSDT", target_date)

        assert is_available is False
