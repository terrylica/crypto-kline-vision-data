#!/usr/bin/env python3
"""Golden dataset regression tests.

Compare current CKVD output against pre-validated golden datasets
to detect regressions in data retrieval.

The golden datasets must be generated first using:
    uv run -p 3.13 python scripts/dev/generate_golden_datasets.py

ADR: docs/adr/2025-01-30-failover-control-protocol.md
"""

from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pytest

from ckvd import DataProvider, CryptoKlineVisionData, Interval, MarketType

# Golden fixtures directory
GOLDEN_DIR = Path(__file__).parent.parent / "fixtures" / "golden"


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def golden_btcusdt_futures_usdt():
    """Load BTCUSDT USDT futures golden dataset."""
    filepath = GOLDEN_DIR / "btcusdt_futures_usdt_1h_2024w01.parquet"
    if not filepath.exists():
        pytest.skip(f"Golden dataset not found: {filepath}")
    df = pd.read_parquet(filepath)
    # Set open_time as index to match CKVD output
    return df.set_index("open_time")


@pytest.fixture
def golden_btcusdt_spot():
    """Load BTCUSDT SPOT golden dataset."""
    filepath = GOLDEN_DIR / "btcusdt_spot_1h_2024w01.parquet"
    if not filepath.exists():
        pytest.skip(f"Golden dataset not found: {filepath}")
    df = pd.read_parquet(filepath)
    return df.set_index("open_time")


@pytest.fixture
def golden_btcusd_perp_coin():
    """Load BTCUSD_PERP coin-margined golden dataset."""
    filepath = GOLDEN_DIR / "btcusd_perp_coin_1h_2024w01.parquet"
    if not filepath.exists():
        pytest.skip(f"Golden dataset not found: {filepath}")
    df = pd.read_parquet(filepath)
    return df.set_index("open_time")


# =============================================================================
# Regression Tests
# =============================================================================


@pytest.mark.integration
class TestGoldenDatasetRegression:
    """Regression tests comparing current output against golden datasets."""

    # Standard date range for golden datasets
    START_TIME = datetime(2024, 1, 1, tzinfo=timezone.utc)
    END_TIME = datetime(2024, 1, 8, tzinfo=timezone.utc)

    def test_futures_usdt_matches_golden(self, golden_btcusdt_futures_usdt):
        """Current FUTURES_USDT output should match golden dataset."""
        manager = CryptoKlineVisionData.create(DataProvider.BINANCE, MarketType.FUTURES_USDT)

        df = manager.get_data(
            symbol="BTCUSDT",
            start_time=self.START_TIME,
            end_time=self.END_TIME,
            interval=Interval.HOUR_1,
        )

        manager.close()

        # Compare row counts
        assert len(df) == len(golden_btcusdt_futures_usdt), (
            f"Row count mismatch: current={len(df)}, golden={len(golden_btcusdt_futures_usdt)}"
        )

        # Compare OHLCV columns (exclude metadata columns like _data_source)
        ohlcv_cols = ["open", "high", "low", "close", "volume"]

        pd.testing.assert_frame_equal(
            df[ohlcv_cols].reset_index(drop=True),
            golden_btcusdt_futures_usdt[ohlcv_cols].reset_index(drop=True),
            check_exact=False,
            check_dtype=False,  # Allow float64 vs int64 for volume (Polars uses Float64)
            rtol=1e-10,
            obj="FUTURES_USDT golden comparison",
        )

    def test_spot_matches_golden(self, golden_btcusdt_spot):
        """Current SPOT output should match golden dataset."""
        manager = CryptoKlineVisionData.create(DataProvider.BINANCE, MarketType.SPOT)

        df = manager.get_data(
            symbol="BTCUSDT",
            start_time=self.START_TIME,
            end_time=self.END_TIME,
            interval=Interval.HOUR_1,
        )

        manager.close()

        assert len(df) == len(golden_btcusdt_spot), f"Row count mismatch: current={len(df)}, golden={len(golden_btcusdt_spot)}"

        ohlcv_cols = ["open", "high", "low", "close", "volume"]

        pd.testing.assert_frame_equal(
            df[ohlcv_cols].reset_index(drop=True),
            golden_btcusdt_spot[ohlcv_cols].reset_index(drop=True),
            check_exact=False,
            check_dtype=False,  # Allow float64 vs int64 for volume (Polars uses Float64)
            rtol=1e-10,
            obj="SPOT golden comparison",
        )

    def test_futures_coin_matches_golden(self, golden_btcusd_perp_coin):
        """Current FUTURES_COIN output should match golden dataset."""
        manager = CryptoKlineVisionData.create(DataProvider.BINANCE, MarketType.FUTURES_COIN)

        df = manager.get_data(
            symbol="BTCUSD_PERP",
            start_time=self.START_TIME,
            end_time=self.END_TIME,
            interval=Interval.HOUR_1,
        )

        manager.close()

        assert len(df) == len(golden_btcusd_perp_coin), f"Row count mismatch: current={len(df)}, golden={len(golden_btcusd_perp_coin)}"

        ohlcv_cols = ["open", "high", "low", "close", "volume"]

        pd.testing.assert_frame_equal(
            df[ohlcv_cols].reset_index(drop=True),
            golden_btcusd_perp_coin[ohlcv_cols].reset_index(drop=True),
            check_exact=False,
            check_dtype=False,  # Allow float64 vs int64 for volume (Polars uses Float64)
            rtol=1e-10,
            obj="FUTURES_COIN golden comparison",
        )


@pytest.mark.integration
class TestGoldenDatasetStructure:
    """Tests for golden dataset file structure and integrity."""

    def test_golden_directory_exists(self):
        """Golden fixtures directory should exist."""
        assert GOLDEN_DIR.exists(), f"Golden directory not found: {GOLDEN_DIR}"

    @pytest.mark.parametrize(
        "filename",
        [
            "btcusdt_futures_usdt_1h_2024w01.parquet",
            "btcusdt_spot_1h_2024w01.parquet",
            "btcusd_perp_coin_1h_2024w01.parquet",
        ],
    )
    def test_golden_file_readable(self, filename):
        """Each golden dataset file should be readable."""
        filepath = GOLDEN_DIR / filename
        if not filepath.exists():
            pytest.skip(f"Golden dataset not found: {filepath}")

        df = pd.read_parquet(filepath)

        # Basic structure checks
        assert len(df) > 0, f"Golden dataset {filename} is empty"
        assert "open_time" in df.columns, f"Golden dataset {filename} missing open_time"

        # OHLCV columns
        for col in ["open", "high", "low", "close", "volume"]:
            assert col in df.columns, f"Golden dataset {filename} missing {col}"
