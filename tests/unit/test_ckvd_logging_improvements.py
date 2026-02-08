#!/usr/bin/env python3
"""Unit tests for CKVD logging improvements.

This test module verifies that the new logging control features work correctly:
1. HTTP debug logging is suppressed by default
2. Logging levels can be configured properly
3. Dynamic reconfiguration works
4. Context managers provide correct logging behavior
"""

import logging
import pytest
from unittest.mock import patch, MagicMock

from ckvd.core.sync.crypto_kline_vision_data import CryptoKlineVisionData, CKVDConfig
from ckvd.utils.loguru_setup import suppress_http_logging
from ckvd.utils.market_constraints import DataProvider, MarketType


class TestCKVDConfig:
    """Test the enhanced CKVDConfig with logging parameters."""

    def test_default_logging_config(self):
        """Test that default logging configuration is correct."""
        config = CKVDConfig(market_type=MarketType.SPOT, provider=DataProvider.BINANCE)

        assert config.log_level == "WARNING"
        assert config.suppress_http_debug is True
        assert config.quiet_mode is False

    def test_custom_logging_config(self):
        """Test custom logging configuration."""
        config = CKVDConfig(
            market_type=MarketType.SPOT, provider=DataProvider.BINANCE, log_level="DEBUG", suppress_http_debug=False, quiet_mode=True
        )

        assert config.log_level == "DEBUG"
        assert config.suppress_http_debug is False
        assert config.quiet_mode is True

    def test_log_level_validation(self):
        """Test that invalid log levels are rejected."""
        with pytest.raises(ValueError):
            CKVDConfig(market_type=MarketType.SPOT, provider=DataProvider.BINANCE, log_level="INVALID")

    def test_log_level_case_conversion(self):
        """Test that log levels are converted to uppercase."""
        config = CKVDConfig(market_type=MarketType.SPOT, provider=DataProvider.BINANCE, log_level="debug")

        assert config.log_level == "DEBUG"


class TestCryptoKlineVisionDataLogging:
    """Test CryptoKlineVisionData logging functionality."""

    @patch("ckvd.core.sync.crypto_kline_vision_data.get_provider_clients")
    def test_default_logging_configuration(self, mock_get_clients):
        """Test that CryptoKlineVisionData configures logging correctly by default."""
        # Mock the factory
        from ckvd.core.providers import ProviderClients

        mock_get_clients.return_value = ProviderClients(
            vision=MagicMock(),
            rest=MagicMock(),
            cache=MagicMock(),
            provider=DataProvider.BINANCE,
            market_type=MarketType.SPOT,
        )

        with patch("logging.getLogger") as mock_get_logger:
            mock_httpcore_logger = MagicMock()
            mock_httpx_logger = MagicMock()

            def get_logger_side_effect(name):
                if name == "httpcore":
                    return mock_httpcore_logger
                if name == "httpx":
                    return mock_httpx_logger
                return MagicMock()

            mock_get_logger.side_effect = get_logger_side_effect

            # Create CKVD with default settings
            ckvd = CryptoKlineVisionData(provider=DataProvider.BINANCE, market_type=MarketType.SPOT)

            # Verify HTTP loggers were configured to suppress debug
            mock_httpcore_logger.setLevel.assert_called_with(logging.WARNING)
            mock_httpx_logger.setLevel.assert_called_with(logging.WARNING)

            ckvd.close()

    @patch("ckvd.core.sync.crypto_kline_vision_data.get_provider_clients")
    def test_debug_logging_configuration(self, mock_get_clients):
        """Test that debug mode enables HTTP logging."""
        # Mock the factory
        from ckvd.core.providers import ProviderClients

        mock_get_clients.return_value = ProviderClients(
            vision=MagicMock(),
            rest=MagicMock(),
            cache=MagicMock(),
            provider=DataProvider.BINANCE,
            market_type=MarketType.SPOT,
        )

        with patch("logging.getLogger") as mock_get_logger:
            mock_httpcore_logger = MagicMock()
            mock_httpx_logger = MagicMock()

            def get_logger_side_effect(name):
                if name == "httpcore":
                    return mock_httpcore_logger
                if name == "httpx":
                    return mock_httpx_logger
                return MagicMock()

            mock_get_logger.side_effect = get_logger_side_effect

            # Create CKVD with debug logging
            ckvd = CryptoKlineVisionData(
                provider=DataProvider.BINANCE, market_type=MarketType.SPOT, log_level="DEBUG", suppress_http_debug=False
            )

            # Verify HTTP loggers were configured for debug
            mock_httpcore_logger.setLevel.assert_called_with(logging.DEBUG)
            mock_httpx_logger.setLevel.assert_called_with(logging.DEBUG)

            ckvd.close()

    @patch("ckvd.core.sync.crypto_kline_vision_data.get_provider_clients")
    def test_dynamic_reconfiguration(self, mock_get_clients):
        """Test dynamic logging reconfiguration."""
        # Mock the factory
        from ckvd.core.providers import ProviderClients

        mock_get_clients.return_value = ProviderClients(
            vision=MagicMock(),
            rest=MagicMock(),
            cache=MagicMock(),
            provider=DataProvider.BINANCE,
            market_type=MarketType.SPOT,
        )

        with patch("logging.getLogger") as mock_get_logger:
            mock_httpcore_logger = MagicMock()

            def get_logger_side_effect(name):
                if name == "httpcore":
                    return mock_httpcore_logger
                return MagicMock()

            mock_get_logger.side_effect = get_logger_side_effect

            # Create CKVD with default settings
            ckvd = CryptoKlineVisionData(provider=DataProvider.BINANCE, market_type=MarketType.SPOT)

            # Verify initial configuration
            assert ckvd.log_level == "WARNING"
            assert ckvd.suppress_http_debug is True

            # Reconfigure to debug mode
            ckvd.reconfigure_logging(log_level="DEBUG", suppress_http_debug=False)

            # Verify configuration changed
            assert ckvd.log_level == "DEBUG"
            assert ckvd.suppress_http_debug is False

            # Verify HTTP logger was reconfigured
            mock_httpcore_logger.setLevel.assert_called_with(logging.DEBUG)

            ckvd.close()


class TestCleanLoggingUtilities:
    """Test the clean logging utility functions."""

    def test_suppress_http_logging(self):
        """Test global HTTP logging suppression."""
        with patch("logging.getLogger") as mock_get_logger:
            mock_httpcore_logger = MagicMock()
            mock_httpx_logger = MagicMock()

            def get_logger_side_effect(name):
                if name == "httpcore":
                    return mock_httpcore_logger
                if name == "httpx":
                    return mock_httpx_logger
                return MagicMock()

            mock_get_logger.side_effect = get_logger_side_effect

            # Call the suppression function
            suppress_http_logging()

            # Verify loggers were configured
            mock_httpcore_logger.setLevel.assert_called_with(logging.WARNING)
            mock_httpx_logger.setLevel.assert_called_with(logging.WARNING)


class TestBackwardCompatibility:
    """Test that existing code continues to work."""

    def test_create_method_backward_compatibility(self):
        """Test that the create method still works with old parameters."""
        # This should work without any logging parameters
        config = CKVDConfig.create(DataProvider.BINANCE, MarketType.SPOT, use_cache=True, retry_count=3)

        # Should use default logging values
        assert config.log_level == "WARNING"
        assert config.suppress_http_debug is True
        assert config.quiet_mode is False

    @patch("ckvd.core.sync.crypto_kline_vision_data.get_provider_clients")
    def test_old_init_signature(self, mock_get_clients):
        """Test that old __init__ signature still works."""
        # Mock the factory
        from ckvd.core.providers import ProviderClients

        mock_get_clients.return_value = ProviderClients(
            vision=MagicMock(),
            rest=MagicMock(),
            cache=MagicMock(),
            provider=DataProvider.BINANCE,
            market_type=MarketType.SPOT,
        )

        with patch("logging.getLogger"):
            # This should work with old-style parameters
            ckvd = CryptoKlineVisionData(provider=DataProvider.BINANCE, market_type=MarketType.SPOT, use_cache=True, retry_count=3)

            # Should use default logging values
            assert ckvd.log_level == "WARNING"
            assert ckvd.suppress_http_debug is True
            assert ckvd.quiet_mode is False

            ckvd.close()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
