"""Tests for symbol security validation (CWE-22 path traversal prevention).

GitHub Issue #21: Symbol path traversal prevention.
Cross-repo analysis of gapless-crypto-data revealed CKVD had no path
traversal protection for user-supplied symbol strings used in file paths.
"""

import pytest

from ckvd.utils.market.enums import DataProvider, MarketType
from ckvd.utils.market.validation import validate_symbol_for_market_type


class TestSymbolPathTraversalPrevention:
    """Verify symbols with path traversal characters are rejected."""

    def test_path_traversal_dotdot(self):
        """Directory traversal via .. must be rejected."""
        with pytest.raises(ValueError, match="invalid characters"):
            validate_symbol_for_market_type("../../etc/passwd", MarketType.SPOT)

    def test_path_traversal_slash(self):
        """Forward slash in symbol must be rejected."""
        with pytest.raises(ValueError, match="invalid characters"):
            validate_symbol_for_market_type("BTCUSDT/../../", MarketType.SPOT)

    def test_path_traversal_backslash(self):
        """Backslash in symbol must be rejected."""
        with pytest.raises(ValueError, match="invalid characters"):
            validate_symbol_for_market_type("BTCUSDT\\..\\", MarketType.SPOT)

    def test_null_byte_injection(self):
        """Null byte in symbol must be rejected."""
        with pytest.raises(ValueError, match="invalid characters"):
            validate_symbol_for_market_type("BTCUSDT\x00", MarketType.SPOT)

    def test_empty_symbol(self):
        """Empty string must be rejected."""
        with pytest.raises(ValueError):
            validate_symbol_for_market_type("", MarketType.SPOT)

    def test_symbol_too_long(self):
        """Symbol exceeding 30 characters must be rejected."""
        with pytest.raises(ValueError, match="invalid characters"):
            validate_symbol_for_market_type("A" * 31, MarketType.SPOT)


class TestValidSymbolsPass:
    """Verify legitimate symbols pass security validation."""

    def test_valid_spot_symbol(self):
        """Standard spot symbol passes."""
        assert validate_symbol_for_market_type("BTCUSDT", MarketType.SPOT) is True

    def test_valid_coin_margined(self):
        """Coin-margined perpetual symbol with underscore passes."""
        assert validate_symbol_for_market_type("BTCUSD_PERP", MarketType.FUTURES_COIN) is True

    def test_valid_okx_symbol(self):
        """OKX hyphenated symbol passes security check."""
        # The security regex allows hyphens. OKX format validation accepts
        # hyphenated symbols for SPOT, so this passes both checks.
        assert validate_symbol_for_market_type("BTC-USDT", MarketType.SPOT, DataProvider.OKX) is True

    def test_valid_options_symbol(self):
        """Options symbol with hyphens and digits passes security check."""
        # Security regex allows this. Options format validation is separate.
        assert validate_symbol_for_market_type("BTC-240315-50000-C", MarketType.OPTIONS) is True


class TestDefenseInDepthGetData:
    """Verify get_data() has its own path traversal guard."""

    def test_get_data_rejects_traversal(self, mock_provider_clients):
        """get_data() must reject path traversal even if validate_symbol is bypassed."""
        from datetime import UTC, datetime, timedelta

        from ckvd import CryptoKlineVisionData, DataProvider, Interval, MarketType

        manager = CryptoKlineVisionData.create(DataProvider.BINANCE, MarketType.SPOT)
        end = datetime.now(UTC)
        start = end - timedelta(days=1)

        # FCP error handler wraps ValueError as RuntimeError (known behavior)
        with pytest.raises(RuntimeError, match="invalid characters"):
            manager.get_data(
                symbol="../../etc",
                start_time=start,
                end_time=end,
                interval=Interval.HOUR_1,
            )
        manager.close()


class TestEdgeCases:
    """Edge cases for symbol security validation."""

    def test_space_in_symbol(self):
        """Spaces in symbol must be rejected."""
        with pytest.raises(ValueError, match="invalid characters"):
            validate_symbol_for_market_type("BTC USDT", MarketType.SPOT)

    def test_newline_in_symbol(self):
        """Newline in symbol must be rejected."""
        with pytest.raises(ValueError, match="invalid characters"):
            validate_symbol_for_market_type("BTC\nUSDT", MarketType.SPOT)

    def test_url_encoded_traversal(self):
        """URL-encoded path traversal must be rejected (% is not in allowlist)."""
        with pytest.raises(ValueError, match="invalid characters"):
            validate_symbol_for_market_type("..%2F..%2Fetc", MarketType.SPOT)

    def test_single_char_symbol(self):
        """Single character symbol is allowed by regex (format validation is separate)."""
        # 'A' matches [A-Z0-9_-]{1,30} — security check passes.
        # Market-type format validation may reject it separately.
        # We only test that security check doesn't block it.
        try:
            validate_symbol_for_market_type("A", MarketType.SPOT)
        except ValueError as e:
            # If it raises, it should NOT be about "invalid characters"
            assert "invalid characters" not in str(e).lower()

    def test_lowercase_gets_uppercased(self):
        """Lowercase symbols should pass after uppercasing in the regex check."""
        # validate_symbol_for_market_type calls .upper() before regex check
        assert validate_symbol_for_market_type("btcusdt", MarketType.SPOT) is True
