# ADR: docs/adr/2025-01-30-failover-control-protocol.md
"""Phase 4 tests: wire reconciliation into create_stream and sync_bridge.

Covers:
- create_stream(reconciliation_enabled=True) passes fetch_fn to KlineStream
- create_stream(reconciliation_enabled=False) does not pass fetch_fn
- stream_data_sync(reconciliation_enabled=True) forwards to async stream
- Default create_stream() has no reconciler (backward compat)

Note: create_stream() uses lazy imports inside the method, so we patch
at the source module paths rather than the crypto_kline_vision_data module.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from ckvd import CryptoKlineVisionData, DataProvider, MarketType


class TestCreateStreamWiring:
    """Tests for create_stream() reconciliation wiring."""

    @patch("ckvd.core.providers.binance.binance_stream_client.BinanceStreamClient")
    def test_reconciliation_enabled_passes_fetch_fn(self, mock_client_cls):
        """When reconciliation_enabled=True, KlineStream receives a fetch_fn."""
        mock_client_cls.return_value = MagicMock()

        manager = CryptoKlineVisionData.create(DataProvider.BINANCE, MarketType.FUTURES_USDT)
        stream = manager.create_stream(reconciliation_enabled=True)

        # The stream should have a reconciler because fetch_fn was passed
        assert stream._reconciler is not None
        manager.close()

    @patch("ckvd.core.providers.binance.binance_stream_client.BinanceStreamClient")
    def test_reconciliation_disabled_no_fetch_fn(self, mock_client_cls):
        """When reconciliation_enabled=False, KlineStream has no reconciler."""
        mock_client_cls.return_value = MagicMock()

        manager = CryptoKlineVisionData.create(DataProvider.BINANCE, MarketType.FUTURES_USDT)
        stream = manager.create_stream(reconciliation_enabled=False)

        assert stream._reconciler is None
        manager.close()

    @patch("ckvd.core.providers.binance.binance_stream_client.BinanceStreamClient")
    def test_default_no_reconciler(self, mock_client_cls):
        """Default create_stream() should not have a reconciler (backward compat)."""
        mock_client_cls.return_value = MagicMock()

        manager = CryptoKlineVisionData.create(DataProvider.BINANCE, MarketType.FUTURES_USDT)
        stream = manager.create_stream()

        assert stream._reconciler is None
        manager.close()

    @patch("ckvd.core.providers.binance.binance_stream_client.BinanceStreamClient")
    def test_reconciliation_config_forwarded(self, mock_client_cls):
        """reconciliation_enabled should be in StreamConfig."""
        mock_client_cls.return_value = MagicMock()

        manager = CryptoKlineVisionData.create(DataProvider.BINANCE, MarketType.FUTURES_USDT)
        stream = manager.create_stream(reconciliation_enabled=True)

        assert stream._config.reconciliation_enabled is True
        manager.close()


class TestStreamDataSyncWiring:
    """Tests for stream_data_sync() reconciliation forwarding."""

    @patch.object(CryptoKlineVisionData, "create_stream")
    def test_sync_forwards_reconciliation(self, mock_create):
        """stream_data_sync(reconciliation_enabled=True) should forward to create_stream."""
        mock_stream = MagicMock()
        mock_create.return_value = mock_stream

        # Patch the bridge to return an empty iterator
        with patch("ckvd.core.streaming.sync_bridge.stream_data_sync", return_value=iter([])):
            manager = CryptoKlineVisionData.create(DataProvider.BINANCE, MarketType.FUTURES_USDT)
            manager.stream_data_sync("BTCUSDT", "1h", reconciliation_enabled=True)

        mock_create.assert_called_once_with(
            confirmed_only=True,
            queue_maxsize=1000,
            reconciliation_enabled=True,
        )
        manager.close()

    @patch.object(CryptoKlineVisionData, "create_stream")
    def test_sync_default_no_reconciliation(self, mock_create):
        """Default stream_data_sync() should not enable reconciliation."""
        mock_stream = MagicMock()
        mock_create.return_value = mock_stream

        with patch("ckvd.core.streaming.sync_bridge.stream_data_sync", return_value=iter([])):
            manager = CryptoKlineVisionData.create(DataProvider.BINANCE, MarketType.FUTURES_USDT)
            manager.stream_data_sync("BTCUSDT", "1h")

        mock_create.assert_called_once_with(
            confirmed_only=True,
            queue_maxsize=1000,
            reconciliation_enabled=False,
        )
        manager.close()
