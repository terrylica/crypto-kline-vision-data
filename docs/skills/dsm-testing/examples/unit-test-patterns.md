# Unit Test Patterns

Examples of well-structured unit tests for DSM.

## Basic Test Structure

```python
import pytest
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

from data_source_manager import DataSourceManager, DataProvider, MarketType, Interval


class TestDataSourceManager:
    """Unit tests for DataSourceManager."""

    def test_create_manager_spot(self):
        """Verify manager creation for spot market."""
        manager = DataSourceManager.create(
            DataProvider.BINANCE,
            MarketType.SPOT
        )

        assert manager is not None
        assert manager.market_type == MarketType.SPOT
        manager.close()

    def test_create_manager_futures_usdt(self):
        """Verify manager creation for USDT futures."""
        manager = DataSourceManager.create(
            DataProvider.BINANCE,
            MarketType.FUTURES_USDT
        )

        assert manager is not None
        assert manager.market_type == MarketType.FUTURES_USDT
        manager.close()
```

## Testing with Fixtures

```python
import pytest

@pytest.fixture
def manager():
    """Create a manager for testing, auto-cleanup."""
    mgr = DataSourceManager.create(
        DataProvider.BINANCE,
        MarketType.FUTURES_USDT
    )
    yield mgr
    mgr.close()

@pytest.fixture
def time_range():
    """Standard time range for tests."""
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=7)
    return start, end


class TestWithFixtures:
    """Tests using fixtures."""

    def test_manager_not_none(self, manager):
        """Manager should be created."""
        assert manager is not None

    def test_time_range_valid(self, time_range):
        """Time range should be valid."""
        start, end = time_range
        assert start < end
        assert end.tzinfo is not None  # Must be UTC
```

## Mocking HTTP Calls

```python
import pandas as pd

@patch("data_source_manager.core.sync.data_source_manager.FSSpecVisionHandler")
@patch("data_source_manager.core.sync.data_source_manager.UnifiedCacheManager")
class TestMockedDataSource:
    """Tests with mocked external dependencies."""

    def test_fetch_returns_dataframe(self, mock_cache, mock_vision):
        """Verify get_data returns a DataFrame."""
        # Arrange: Mock returns empty DataFrame
        # Note: DSM returns pd.DataFrame for API compatibility
        mock_cache.return_value.get_cached_data.return_value = None
        mock_vision.return_value.fetch_data.return_value = pd.DataFrame({
            "open_time": [],
            "open": [],
            "high": [],
            "low": [],
            "close": [],
            "volume": [],
        })

        manager = DataSourceManager.create(
            DataProvider.BINANCE,
            MarketType.FUTURES_USDT
        )

        # Act
        end = datetime.now(timezone.utc)
        start = end - timedelta(days=1)
        df = manager.get_data(
            symbol="BTCUSDT",
            interval=Interval.HOUR_1,
            start_time=start,
            end_time=end
        )

        # Assert
        assert df is not None
        manager.close()
```

## Testing Error Conditions

```python
import pytest
from data_source_manager.utils.for_core.rest_exceptions import RateLimitError


class TestErrorHandling:
    """Tests for error handling."""

    def test_invalid_symbol_raises(self, manager):
        """Invalid symbol should raise error."""
        with pytest.raises(ValueError):
            manager.get_data(
                symbol="",  # Empty symbol
                interval=Interval.HOUR_1,
                start_time=datetime.now(timezone.utc) - timedelta(days=1),
                end_time=datetime.now(timezone.utc)
            )

    @patch("data_source_manager.core.providers.binance.rest_data_client.RestDataClient")
    def test_rate_limit_propagates(self, mock_client, manager):
        """Rate limit errors should propagate."""
        mock_client.return_value.fetch_klines.side_effect = RateLimitError("429")

        with pytest.raises(RateLimitError):
            manager.get_data(
                symbol="BTCUSDT",
                interval=Interval.HOUR_1,
                start_time=datetime.now(timezone.utc) - timedelta(days=1),
                end_time=datetime.now(timezone.utc)
            )
```

## Related

- [Fixtures Reference](../references/fixtures.md)
- [Mocking Patterns](../references/mocking-patterns.md)
