#!/usr/bin/env python
"""Unit tests for UnifiedCacheManager with the updated cache path structure."""

import os
from datetime import datetime, timezone
from pathlib import Path
import pytest
import tempfile
import json
import shutil

from core.cache_manager import UnifiedCacheManager
from utils.cache_validator import CacheKeyManager


class TestUnifiedCacheManager:
    """Tests for the UnifiedCacheManager class."""

    def setup_method(self):
        """Set up test environment with real paths."""
        # Use a real temp directory path
        self.temp_dir = tempfile.mkdtemp()
        self.cache_dir = Path(self.temp_dir)

        # Create necessary subdirectories
        (self.cache_dir / "data").mkdir(parents=True, exist_ok=True)
        (self.cache_dir / "metadata").mkdir(parents=True, exist_ok=True)

        # Set up test data
        self.symbol = "BTCUSDT"
        self.interval = "1m"
        self.date = datetime(2023, 1, 15, tzinfo=timezone.utc)

        # Create a metadata file
        metadata_file = self.cache_dir / "metadata" / "cache_index.json"
        with open(metadata_file, "w") as f:
            json.dump({}, f)

    def teardown_method(self):
        """Clean up resources."""
        # Clean up the temp directory
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_cache_manager_delegates_to_cache_key_manager(self, caplog):
        """Test that UnifiedCacheManager.get_cache_path correctly calls CacheKeyManager."""
        # Create a real cache manager
        cache_manager = UnifiedCacheManager(self.cache_dir)

        # Get the path
        path = cache_manager.get_cache_path(self.symbol, self.interval, self.date)

        # Verify the path is correct by comparing with direct CacheKeyManager call
        expected_data_dir = self.cache_dir / "data"
        expected_path = CacheKeyManager.get_cache_path(
            expected_data_dir, self.symbol, self.interval, self.date
        )

        assert path == expected_path
        assert "data" in str(path), "Data directory not in path"
        assert self.symbol in str(path), "Symbol not in path"
        assert self.interval in str(path), "Interval not in path"

    def test_cache_manager_cache_path_structure(self, caplog):
        """Test that the cache path structure from UnifiedCacheManager is correct."""
        # Create a real cache manager
        cache_manager = UnifiedCacheManager(self.cache_dir)

        # Get the path
        path = cache_manager.get_cache_path(self.symbol, self.interval, self.date)

        # Verify the path structure
        parts = path.parts
        data_dir = self.cache_dir / "data"
        data_dir_parts_count = len(data_dir.parts)
        relative_parts = parts[data_dir_parts_count:]

        assert relative_parts[0] == "binance", "Exchange directory incorrect"
        assert relative_parts[1] == "spot", "Market type directory incorrect"
        assert relative_parts[2] == "klines", "Data nature directory incorrect"
        assert relative_parts[3] == "daily", "Packaging frequency directory incorrect"
        assert relative_parts[4] == self.symbol, "Symbol directory incorrect"
        assert relative_parts[5] == self.interval, "Interval directory incorrect"
        assert (
            relative_parts[6] == f"{self.date.strftime('%Y%m')}.arrow"
        ), "File name incorrect"

    def test_cache_manager_creates_directory_structure(self, caplog):
        """Test that UnifiedCacheManager actually creates the directory structure."""
        # Create a real cache manager
        cache_manager = UnifiedCacheManager(self.cache_dir)

        # Get the path and create parent directories
        path = cache_manager.get_cache_path(self.symbol, self.interval, self.date)
        path.parent.mkdir(parents=True, exist_ok=True)

        # Verify the directories were created
        assert path.parent.exists(), "Failed to create directory structure"

        # The full path should be:
        # cache_dir/data/binance/spot/klines/daily/BTCUSDT/1m
        exchange_dir = self.cache_dir / "data" / "binance"
        assert exchange_dir.exists(), "Exchange directory not created"

        market_type_dir = exchange_dir / "spot"
        assert market_type_dir.exists(), "Market type directory not created"

        data_nature_dir = market_type_dir / "klines"
        assert data_nature_dir.exists(), "Data nature directory not created"

        packaging_dir = data_nature_dir / "daily"
        assert packaging_dir.exists(), "Packaging frequency directory not created"

        symbol_dir = packaging_dir / self.symbol
        assert symbol_dir.exists(), "Symbol directory not created"

        interval_dir = symbol_dir / self.interval
        assert interval_dir.exists(), "Interval directory not created"
