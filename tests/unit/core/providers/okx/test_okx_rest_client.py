#!/usr/bin/env python3
"""Unit tests for OKX REST client.

ADR: docs/adr/2025-01-30-failover-control-protocol.md

Tests the OKXRestClient implementation including:
- Symbol conversion (Binance → OKX format)
- Interval conversion (case-sensitive for hours+)
- Factory pattern integration
- Protocol compliance
"""

from datetime import datetime, timezone
from unittest.mock import patch

import pandas as pd
import pytest

from data_source_manager.core.providers import get_provider_clients, get_supported_providers
from data_source_manager.core.providers.okx.okx_rest_client import (
    OKX_INTERVAL_MAP,
    OKXRestClient,
    _convert_interval_to_okx,
    _convert_symbol_to_okx,
)
from data_source_manager.utils.market_constraints import (
    ChartType,
    DataProvider,
    Interval,
    MarketType,
)


class TestOKXProviderRegistration:
    """Tests for OKX provider factory registration."""

    def test_okx_is_supported_provider(self):
        """OKX should be in the supported providers list."""
        supported = get_supported_providers()
        assert DataProvider.OKX in supported

    def test_binance_still_supported(self):
        """Binance should still be supported after adding OKX."""
        supported = get_supported_providers()
        assert DataProvider.BINANCE in supported

    def test_get_provider_clients_returns_okx_rest_client(self):
        """get_provider_clients should return OKXRestClient for OKX provider."""
        clients = get_provider_clients(DataProvider.OKX, MarketType.SPOT)

        assert clients.provider == DataProvider.OKX
        assert clients.market_type == MarketType.SPOT
        assert isinstance(clients.rest, OKXRestClient)
        assert clients.vision is None  # OKX has no Vision API

    def test_okx_futures_creates_client(self):
        """OKX FUTURES_USDT should create valid clients."""
        clients = get_provider_clients(DataProvider.OKX, MarketType.FUTURES_USDT)

        assert clients.provider == DataProvider.OKX
        assert clients.market_type == MarketType.FUTURES_USDT


class TestSymbolConversion:
    """Tests for Binance → OKX symbol conversion."""

    @pytest.mark.parametrize(
        "input_symbol,market_type,expected",
        [
            # SPOT conversions
            ("BTCUSDT", MarketType.SPOT, "BTC-USDT"),
            ("ETHUSDT", MarketType.SPOT, "ETH-USDT"),
            ("SOLUSDT", MarketType.SPOT, "SOL-USDT"),
            ("BTCUSDC", MarketType.SPOT, "BTC-USDC"),
            # Already in OKX format
            ("BTC-USDT", MarketType.SPOT, "BTC-USDT"),
            # Futures conversions
            ("BTCUSDT", MarketType.FUTURES_USDT, "BTC-USD-SWAP"),
            ("ETHUSDT", MarketType.FUTURES_USDT, "ETH-USD-SWAP"),
            # Already in OKX format
            ("BTC-USD-SWAP", MarketType.FUTURES_USDT, "BTC-USD-SWAP"),
        ],
    )
    def test_convert_symbol_to_okx(self, input_symbol, market_type, expected):
        """Symbol conversion should handle various formats."""
        result = _convert_symbol_to_okx(input_symbol, market_type)
        assert result == expected


class TestIntervalConversion:
    """Tests for DSM Interval → OKX interval string conversion."""

    @pytest.mark.parametrize(
        "interval,expected",
        [
            (Interval.MINUTE_1, "1m"),
            (Interval.MINUTE_5, "5m"),
            (Interval.MINUTE_15, "15m"),
            (Interval.HOUR_1, "1H"),  # OKX requires uppercase
            (Interval.HOUR_4, "4H"),
            (Interval.DAY_1, "1D"),  # OKX requires uppercase
            (Interval.WEEK_1, "1W"),
        ],
    )
    def test_convert_interval_to_okx(self, interval, expected):
        """Interval conversion should use OKX-specific format (uppercase for hours+)."""
        result = _convert_interval_to_okx(interval)
        assert result == expected

    def test_interval_map_completeness(self):
        """All common intervals should be mapped."""
        # At minimum, these should be supported
        required_intervals = [
            Interval.MINUTE_1,
            Interval.MINUTE_5,
            Interval.HOUR_1,
            Interval.HOUR_4,
            Interval.DAY_1,
        ]
        for interval in required_intervals:
            assert interval in OKX_INTERVAL_MAP

    def test_unsupported_interval_raises(self):
        """Unsupported intervals should raise ValueError."""
        # SECOND_1 is not supported by OKX candles endpoint
        with pytest.raises(ValueError, match="not supported by OKX"):
            _convert_interval_to_okx(Interval.SECOND_1)


class TestOKXRestClientProperties:
    """Tests for OKXRestClient property methods."""

    def test_provider_property(self):
        """provider property should return OKX."""
        client = OKXRestClient(MarketType.SPOT)
        assert client.provider == DataProvider.OKX

    def test_chart_type_property(self):
        """chart_type property should return OKX_CANDLES."""
        client = OKXRestClient(MarketType.SPOT)
        assert client.chart_type == ChartType.OKX_CANDLES

    def test_symbol_property(self):
        """symbol property should return default symbol."""
        client = OKXRestClient(MarketType.SPOT, symbol="ETH-USDT")
        assert client.symbol == "ETH-USDT"

    def test_interval_property(self):
        """interval property should return interval value string."""
        client = OKXRestClient(MarketType.SPOT, interval=Interval.HOUR_1)
        assert client.interval == "1h"


class TestOKXRestClientDataValidation:
    """Tests for OKXRestClient data validation."""

    def test_validate_empty_dataframe(self):
        """Empty DataFrame should be valid."""
        client = OKXRestClient(MarketType.SPOT)
        df = client.create_empty_dataframe()

        is_valid, error = client.validate_data(df)
        assert is_valid is True
        assert error is None

    def test_validate_valid_data(self):
        """Valid OHLCV data should pass validation."""
        client = OKXRestClient(MarketType.SPOT)

        df = pd.DataFrame(
            {
                "open": [100.0, 101.0],
                "high": [102.0, 103.0],
                "low": [99.0, 100.0],
                "close": [101.0, 102.0],
                "volume": [1000.0, 1100.0],
            },
            index=pd.DatetimeIndex(
                [datetime(2024, 1, 1, tzinfo=timezone.utc), datetime(2024, 1, 1, 1, tzinfo=timezone.utc)],
                name="open_time",
            ),
        )

        is_valid, error = client.validate_data(df)
        assert is_valid is True
        assert error is None

    def test_validate_missing_columns(self):
        """Missing required columns should fail validation."""
        client = OKXRestClient(MarketType.SPOT)

        df = pd.DataFrame(
            {"open": [100.0], "high": [102.0]},  # Missing low, close, volume
            index=pd.DatetimeIndex([datetime(2024, 1, 1, tzinfo=timezone.utc)], name="open_time"),
        )

        is_valid, error = client.validate_data(df)
        assert is_valid is False
        assert "Missing columns" in error

    def test_validate_high_low_constraint(self):
        """High < Low should fail validation."""
        client = OKXRestClient(MarketType.SPOT)

        df = pd.DataFrame(
            {
                "open": [100.0],
                "high": [98.0],  # Invalid: high < low
                "low": [99.0],
                "close": [101.0],
                "volume": [1000.0],
            },
            index=pd.DatetimeIndex([datetime(2024, 1, 1, tzinfo=timezone.utc)], name="open_time"),
        )

        is_valid, error = client.validate_data(df)
        assert is_valid is False
        assert "High must be >= Low" in error


class TestOKXRestClientMocked:
    """Tests for OKXRestClient with mocked HTTP calls."""

    @patch.object(OKXRestClient, "_request_with_retry")
    def test_fetch_processes_okx_response(self, mock_request):
        """fetch should correctly process OKX candle response format."""
        # OKX returns: [timestamp, open, high, low, close, volume, volUSD, turnover, confirm]
        # 1704067200000 = 2024-01-01 00:00:00 UTC
        # 1704070800000 = 2024-01-01 01:00:00 UTC
        mock_request.return_value = {
            "code": "0",
            "msg": "",
            "data": [
                ["1704070800000", "42000", "42500", "41800", "42200", "100", "4200000", "4200000", "1"],
                ["1704067200000", "41500", "42100", "41400", "42000", "90", "3800000", "3800000", "1"],
            ],
        }

        client = OKXRestClient(MarketType.SPOT)
        start = datetime(2024, 1, 1, tzinfo=timezone.utc)
        end = datetime(2024, 1, 1, 2, tzinfo=timezone.utc)

        df = client.fetch("BTC-USDT", "1h", start, end)

        # Should have at least 1 record (2 if both are in range)
        assert len(df) >= 1
        assert "open" in df.columns
        assert "high" in df.columns
        assert "low" in df.columns
        assert "close" in df.columns
        assert "volume" in df.columns
        assert df.index.name == "open_time"

    @patch.object(OKXRestClient, "_request_with_retry")
    def test_fetch_empty_response(self, mock_request):
        """fetch should handle empty response gracefully."""
        mock_request.return_value = {"code": "0", "msg": "", "data": []}

        client = OKXRestClient(MarketType.SPOT)
        start = datetime(2024, 1, 1, tzinfo=timezone.utc)
        end = datetime(2024, 1, 1, 2, tzinfo=timezone.utc)

        df = client.fetch("BTC-USDT", "1h", start, end)

        assert df.empty
        assert df.index.name == "open_time"

    def test_context_manager(self):
        """OKXRestClient should work as context manager."""
        with OKXRestClient(MarketType.SPOT) as client:
            assert client._client is not None

        # After context exit, client should be closed
        assert client._client is None


class TestOKXRestClientIntervalParsing:
    """Tests for interval parsing in OKXRestClient."""

    def test_parse_interval_from_string(self):
        """_parse_interval should handle string intervals."""
        client = OKXRestClient(MarketType.SPOT)

        # Direct match
        assert client._parse_interval("1m") == Interval.MINUTE_1
        assert client._parse_interval("1h") == Interval.HOUR_1
        assert client._parse_interval("1d") == Interval.DAY_1

    def test_parse_interval_case_insensitive(self):
        """_parse_interval should be case-insensitive for input."""
        client = OKXRestClient(MarketType.SPOT)

        # OKX input format variations should all work
        assert client._parse_interval("1H") == Interval.HOUR_1
        assert client._parse_interval("1D") == Interval.DAY_1

    def test_parse_interval_from_enum(self):
        """_parse_interval should pass through Interval enums."""
        client = OKXRestClient(MarketType.SPOT)

        assert client._parse_interval(Interval.HOUR_4) == Interval.HOUR_4

    def test_parse_interval_invalid_raises(self):
        """_parse_interval should raise for invalid intervals."""
        client = OKXRestClient(MarketType.SPOT)

        with pytest.raises(ValueError, match="Invalid interval"):
            client._parse_interval("invalid")
