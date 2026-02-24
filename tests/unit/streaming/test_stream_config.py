# ADR: docs/adr/2025-01-30-failover-control-protocol.md
"""Unit tests for StreamConfig (T15).

Covers:
- Defaults (confirmed_only, queue_maxsize, ping settings, compression)
- compression=None enforced — cannot set to other values
- confirmed_only / queue_maxsize overrides
- market_type required (no default)
- Immutability (frozen=True)
- StreamConfig is provider-aware (DataProvider default = BINANCE)
"""

import pytest

from ckvd.core.streaming.stream_config import StreamConfig
from ckvd.utils.market_constraints import DataProvider, MarketType


class TestStreamConfigDefaults:
    """Verify defaults match the SOTA streaming spec."""

    def test_confirmed_only_default_true(self, stream_config_futures):
        assert stream_config_futures.confirmed_only is True

    def test_queue_maxsize_default_1000(self, stream_config_futures):
        assert stream_config_futures.queue_maxsize == 1000

    def test_compression_default_none(self, stream_config_futures):
        assert stream_config_futures.compression is None

    def test_provider_default_binance(self, stream_config_futures):
        assert stream_config_futures.provider == DataProvider.BINANCE

    def test_max_reconnect_attempts_default_5(self, stream_config_futures):
        assert stream_config_futures.max_reconnect_attempts == 5

    def test_ping_interval_positive(self, stream_config_futures):
        assert stream_config_futures.ping_interval > 0

    def test_ping_timeout_positive(self, stream_config_futures):
        assert stream_config_futures.ping_timeout > 0

    def test_ping_timeout_less_than_interval(self, stream_config_futures):
        assert stream_config_futures.ping_timeout < stream_config_futures.ping_interval


class TestStreamConfigMarketType:
    """market_type is required; no default."""

    def test_spot_market_type(self, stream_config_spot):
        assert stream_config_spot.market_type == MarketType.SPOT

    def test_futures_usdt_market_type(self, stream_config_futures):
        assert stream_config_futures.market_type == MarketType.FUTURES_USDT

    def test_futures_coin_market_type(self):
        config = StreamConfig(market_type=MarketType.FUTURES_COIN)
        assert config.market_type == MarketType.FUTURES_COIN

    def test_missing_market_type_raises(self):
        with pytest.raises(TypeError):
            StreamConfig()  # type: ignore[call-arg]


class TestStreamConfigCompressionEnforcement:
    """compression=None must be enforced — Binance rejects deflate."""

    def test_compression_none_accepted(self):
        config = StreamConfig(market_type=MarketType.SPOT, compression=None)
        assert config.compression is None

    def test_compression_other_value_rejected(self):
        """Any non-None value must be rejected by the validator."""
        with pytest.raises((ValueError, TypeError)):
            StreamConfig(market_type=MarketType.SPOT, compression="deflate")  # type: ignore[arg-type]

    def test_compression_string_none_rejected(self):
        """String 'None' is not the same as None."""
        with pytest.raises((ValueError, TypeError)):
            StreamConfig(market_type=MarketType.SPOT, compression="None")  # type: ignore[arg-type]


class TestStreamConfigCustomValues:
    """Custom values override defaults correctly."""

    def test_confirmed_only_false(self):
        config = StreamConfig(market_type=MarketType.SPOT, confirmed_only=False)
        assert config.confirmed_only is False

    def test_queue_maxsize_custom(self):
        config = StreamConfig(market_type=MarketType.SPOT, queue_maxsize=500)
        assert config.queue_maxsize == 500

    def test_max_reconnect_attempts_custom(self):
        config = StreamConfig(market_type=MarketType.SPOT, max_reconnect_attempts=10)
        assert config.max_reconnect_attempts == 10

    def test_provider_custom(self):
        config = StreamConfig(market_type=MarketType.SPOT, provider=DataProvider.BINANCE)
        assert config.provider == DataProvider.BINANCE


class TestStreamConfigImmutability:
    """StreamConfig is frozen=True — no mutations allowed."""

    def test_cannot_set_confirmed_only(self, stream_config_spot):
        with pytest.raises((AttributeError, TypeError)):
            stream_config_spot.confirmed_only = False  # type: ignore[misc]

    def test_cannot_set_queue_maxsize(self, stream_config_spot):
        with pytest.raises((AttributeError, TypeError)):
            stream_config_spot.queue_maxsize = 9999  # type: ignore[misc]

    def test_cannot_set_market_type(self, stream_config_spot):
        with pytest.raises((AttributeError, TypeError)):
            stream_config_spot.market_type = MarketType.FUTURES_USDT  # type: ignore[misc]
