#!/usr/bin/env python
"""Unit tests for the updated CacheKeyManager.get_cache_path method."""

import os
from datetime import datetime, timezone
from pathlib import Path
import pytest
import tempfile

from utils.cache_validator import CacheKeyManager


class TestCacheKeyManager:
    """Tests for the CacheKeyManager class."""

    def setup_method(self):
        """Set up test environment with real paths."""
        # Use a real temp directory path instead of a fake path
        self.temp_dir = tempfile.mkdtemp()
        self.cache_dir = Path(self.temp_dir)
        self.symbol = "BTCUSDT"
        self.interval = "1m"
        self.date = datetime(2023, 1, 15, tzinfo=timezone.utc)

    def teardown_method(self):
        """Clean up resources."""
        # Clean up the temp directory
        import shutil

        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_default_path_structure(self, caplog):
        """Test the default path structure with all default parameters."""
        path = CacheKeyManager.get_cache_path(
            self.cache_dir, self.symbol, self.interval, self.date
        )
        expected_path = (
            self.cache_dir
            / "binance"
            / "spot"
            / "klines"
            / "daily"
            / self.symbol
            / self.interval
            / "20230115.arrow"
        )
        assert path == expected_path
        assert "binance" in str(path), "Exchange name not in path"
        assert "spot" in str(path), "Market type not in path"

    def test_custom_exchange(self, caplog):
        """Test with custom exchange parameter."""
        path = CacheKeyManager.get_cache_path(
            self.cache_dir,
            self.symbol,
            self.interval,
            self.date,
            exchange="coinbase",
        )
        expected_path = (
            self.cache_dir
            / "coinbase"
            / "spot"
            / "klines"
            / "daily"
            / self.symbol
            / self.interval
            / "20230115.arrow"
        )
        assert path == expected_path
        assert "coinbase" in str(path), "Custom exchange name not in path"

    def test_custom_market_type(self, caplog):
        """Test with custom market_type parameter."""
        path = CacheKeyManager.get_cache_path(
            self.cache_dir,
            self.symbol,
            self.interval,
            self.date,
            market_type="futures",
        )
        expected_path = (
            self.cache_dir
            / "binance"
            / "futures"
            / "klines"
            / "daily"
            / self.symbol
            / self.interval
            / "20230115.arrow"
        )
        assert path == expected_path
        assert "futures" in str(path), "Custom market type not in path"

    def test_custom_data_nature(self, caplog):
        """Test with custom data_nature parameter."""
        path = CacheKeyManager.get_cache_path(
            self.cache_dir,
            self.symbol,
            self.interval,
            self.date,
            data_nature="trades",
        )
        expected_path = (
            self.cache_dir
            / "binance"
            / "spot"
            / "trades"
            / "daily"
            / self.symbol
            / self.interval
            / "20230115.arrow"
        )
        assert path == expected_path
        assert "trades" in str(path), "Custom data nature not in path"

    def test_custom_packaging_frequency(self, caplog):
        """Test with custom packaging_frequency parameter."""
        path = CacheKeyManager.get_cache_path(
            self.cache_dir,
            self.symbol,
            self.interval,
            self.date,
            packaging_frequency="monthly",
        )
        expected_path = (
            self.cache_dir
            / "binance"
            / "spot"
            / "klines"
            / "monthly"
            / self.symbol
            / self.interval
            / "20230115.arrow"
        )
        assert path == expected_path
        assert "monthly" in str(path), "Custom packaging frequency not in path"

    def test_all_custom_parameters(self, caplog):
        """Test with all parameters customized."""
        path = CacheKeyManager.get_cache_path(
            self.cache_dir,
            self.symbol,
            self.interval,
            self.date,
            exchange="kraken",
            market_type="futures",
            data_nature="trades",
            packaging_frequency="hourly",
        )
        expected_path = (
            self.cache_dir
            / "kraken"
            / "futures"
            / "trades"
            / "hourly"
            / self.symbol
            / self.interval
            / "20230115.arrow"
        )
        assert path == expected_path
        assert "kraken" in str(path), "Custom exchange not in path"
        assert "futures" in str(path), "Custom market type not in path"
        assert "trades" in str(path), "Custom data nature not in path"
        assert "hourly" in str(path), "Custom packaging frequency not in path"

    def test_backward_compatibility(self, caplog):
        """Test for backward compatibility with existing code that might parse the path."""
        path = CacheKeyManager.get_cache_path(
            self.cache_dir, self.symbol, self.interval, self.date
        )
        # Verify that symbol and interval are at the correct position in the path
        path_parts = path.parts

        # Symbol should be at position -3 (third from end)
        assert path_parts[-3] == self.symbol

        # Interval should be at position -2 (second from end)
        assert path_parts[-2] == self.interval

        # Filename should be at position -1 (last)
        assert path_parts[-1] == f"{self.date.strftime('%Y%m%d')}.arrow"

    def test_different_dates(self, caplog):
        """Test that different dates produce the correct date format in the filename."""
        # Test with January
        jan_date = datetime(2023, 1, 1, tzinfo=timezone.utc)
        jan_path = CacheKeyManager.get_cache_path(
            self.cache_dir, self.symbol, self.interval, jan_date
        )
        assert jan_path.name == "20230101.arrow"

        # Test with December
        dec_date = datetime(2023, 12, 31, tzinfo=timezone.utc)
        dec_path = CacheKeyManager.get_cache_path(
            self.cache_dir, self.symbol, self.interval, dec_date
        )
        assert dec_path.name == "20231231.arrow"

        # Test with a date that has time components
        time_date = datetime(2023, 6, 15, 12, 30, 45, tzinfo=timezone.utc)
        time_path = CacheKeyManager.get_cache_path(
            self.cache_dir, self.symbol, self.interval, time_date
        )
        assert time_path.name == "20230615.arrow"

    def test_create_full_path(self, caplog):
        """Test that we can actually create the full path structure."""
        # Create the actual directories
        path = CacheKeyManager.get_cache_path(
            self.cache_dir, self.symbol, self.interval, self.date
        )
        # Create all parent directories
        path.parent.mkdir(parents=True, exist_ok=True)

        # Verify directories were created
        assert path.parent.exists(), "Failed to create directory structure"

        # Create an empty file
        with open(path, "w") as f:
            f.write("")

        # Verify file was created
        assert path.exists(), "Failed to create file"
        assert "20230115.arrow" in str(path), "File name incorrect"
