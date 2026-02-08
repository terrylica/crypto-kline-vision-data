#!/usr/bin/env python3
"""
OKX symbol formatting and validation tests.

These tests verify the symbol formatting, validation, and endpoint URL generation
for OKX data provider using the market_constraints utilities.
"""

import pytest

from ckvd.utils.market_constraints import (
    ChartType,
    DataProvider,
    MarketType,
    get_endpoint_url,
    get_market_symbol_format,
    validate_symbol_for_market_type,
)


@pytest.mark.okx
class TestSymbolFormatting:
    """Tests for OKX symbol formatting from generic to exchange-specific format."""

    @pytest.mark.parametrize(
        "original,market_type,expected",
        [
            ("BTCUSDT", MarketType.SPOT, "BTC-USDT"),
            ("ETHUSDT", MarketType.SPOT, "ETH-USDT"),
            ("BTC-USDT", MarketType.SPOT, "BTC-USDT"),  # Already formatted
            ("BTCUSDT", MarketType.FUTURES_USDT, "BTC-USD-SWAP"),
            ("ETHUSDT", MarketType.FUTURES_USDT, "ETH-USD-SWAP"),
            ("BTC-USD-SWAP", MarketType.FUTURES_USDT, "BTC-USD-SWAP"),  # Already formatted
        ],
    )
    def test_symbol_formatting_converts_correctly(self, original: str, market_type: MarketType, expected: str) -> None:
        """
        Verify symbol formatting converts generic symbols to OKX format.

        OKX uses hyphen-separated symbols:
        - Spot: BTC-USDT
        - Perpetual Swap: BTC-USD-SWAP

        Validates:
        - Function returns correctly formatted symbol
        """
        result = get_market_symbol_format(original, market_type, DataProvider.OKX)
        assert result == expected, f"Expected '{expected}', got '{result}'"


@pytest.mark.okx
class TestSymbolValidation:
    """Tests for OKX symbol validation."""

    @pytest.mark.parametrize(
        "symbol,market_type",
        [
            ("BTC-USDT", MarketType.SPOT),
            ("ETH-USDT", MarketType.SPOT),
            ("BTC-USD-SWAP", MarketType.FUTURES_USDT),
            ("ETH-USD-SWAP", MarketType.FUTURES_USDT),
        ],
    )
    def test_valid_symbols_pass_validation(self, symbol: str, market_type: MarketType) -> None:
        """
        Verify valid OKX symbols pass validation without raising exceptions.

        Validates:
        - No exception is raised for valid symbols
        """
        # Should not raise
        validate_symbol_for_market_type(symbol, market_type, DataProvider.OKX)

    @pytest.mark.parametrize(
        "symbol,market_type,description",
        [
            ("BTCUSDT", MarketType.SPOT, "Missing hyphen"),
            ("BTCUSDT", MarketType.FUTURES_USDT, "Missing hyphen and SWAP suffix"),
            ("BTC-USD", MarketType.FUTURES_USDT, "Missing SWAP suffix"),
        ],
    )
    def test_invalid_symbols_raise_error(self, symbol: str, market_type: MarketType, description: str) -> None:
        """
        Verify invalid OKX symbols raise ValueError.

        Validates:
        - ValueError is raised for invalid symbols
        """
        with pytest.raises(ValueError):
            validate_symbol_for_market_type(symbol, market_type, DataProvider.OKX)


@pytest.mark.okx
class TestEndpointUrls:
    """Tests for OKX endpoint URL generation."""

    @pytest.mark.parametrize(
        "market_type,chart_type,expected_url",
        [
            (
                MarketType.SPOT,
                ChartType.OKX_CANDLES,
                "https://www.okx.com/api/v5/market/candles",
            ),
            (
                MarketType.SPOT,
                ChartType.OKX_HISTORY_CANDLES,
                "https://www.okx.com/api/v5/market/history-candles",
            ),
            (
                MarketType.FUTURES_USDT,
                ChartType.OKX_CANDLES,
                "https://www.okx.com/api/v5/market/candles",
            ),
            (
                MarketType.FUTURES_USDT,
                ChartType.OKX_HISTORY_CANDLES,
                "https://www.okx.com/api/v5/market/history-candles",
            ),
        ],
    )
    def test_endpoint_url_generation(self, market_type: MarketType, chart_type: ChartType, expected_url: str) -> None:
        """
        Verify endpoint URL generation returns correct URLs for OKX.

        OKX uses the same endpoints for both spot and futures:
        - /market/candles for recent data
        - /market/history-candles for historical data

        Validates:
        - Function returns the expected URL
        """
        url = get_endpoint_url(market_type, chart_type, data_provider=DataProvider.OKX)
        assert url == expected_url, f"Expected '{expected_url}', got '{url}'"
