# Integration Test Patterns

Examples of integration tests that call external APIs.

## Marking Integration Tests

```python
import pytest
from datetime import datetime, timedelta, timezone

from data_source_manager import DataSourceManager, DataProvider, MarketType, Interval


@pytest.mark.integration
class TestVisionAPIIntegration:
    """Integration tests for Vision API."""

    def test_fetch_historical_data(self):
        """Verify Vision API returns historical data."""
        manager = DataSourceManager.create(
            DataProvider.BINANCE,
            MarketType.FUTURES_USDT
        )

        # Use a date range we know has data
        end = datetime.now(timezone.utc) - timedelta(days=7)
        start = end - timedelta(days=1)

        df = manager.get_data(
            symbol="BTCUSDT",
            interval=Interval.H1,
            start_time=start,
            end_time=end
        )

        assert df is not None
        assert len(df) > 0
        assert "open" in df.columns
        assert "close" in df.columns
        manager.close()
```

## Running Integration Tests

```bash
# Run only integration tests
uv run -p 3.13 pytest tests/integration/ -v -m integration

# Skip integration tests
uv run -p 3.13 pytest tests/ -v -m "not integration"

# Run with verbose output for debugging
uv run -p 3.13 pytest tests/integration/ -v -s --log-cli-level=DEBUG
```

## Testing Different Market Types

```python
@pytest.mark.integration
class TestMarketTypeIntegration:
    """Test each market type works end-to-end."""

    @pytest.mark.parametrize("market_type,symbol", [
        (MarketType.SPOT, "BTCUSDT"),
        (MarketType.FUTURES_USDT, "BTCUSDT"),
        (MarketType.FUTURES_COIN, "BTCUSD_PERP"),  # Different format!
    ])
    def test_all_market_types(self, market_type, symbol):
        """Each market type should return valid data."""
        manager = DataSourceManager.create(
            DataProvider.BINANCE,
            market_type
        )

        end = datetime.now(timezone.utc) - timedelta(days=7)
        start = end - timedelta(days=1)

        df = manager.get_data(
            symbol=symbol,
            interval=Interval.H1,
            start_time=start,
            end_time=end
        )

        assert df is not None
        assert len(df) > 0
        manager.close()
```

## Testing FCP Fallback

```python
@pytest.mark.integration
class TestFCPFallback:
    """Test Failover Control Protocol behavior."""

    def test_recent_data_uses_rest(self):
        """Recent data (<48h) should fall back to REST API."""
        manager = DataSourceManager.create(
            DataProvider.BINANCE,
            MarketType.FUTURES_USDT
        )

        # Request recent data - Vision API won't have it
        end = datetime.now(timezone.utc) - timedelta(hours=1)
        start = end - timedelta(hours=2)

        df = manager.get_data(
            symbol="BTCUSDT",
            interval=Interval.M1,  # 1-minute for recent data
            start_time=start,
            end_time=end
        )

        # Should still get data via REST fallback
        assert df is not None
        assert len(df) > 0
        manager.close()
```

## Handling Rate Limits

```python
import time

@pytest.mark.integration
@pytest.mark.serial  # Don't run in parallel
class TestRateLimitHandling:
    """Tests for rate limit handling."""

    def test_many_requests_with_retry(self):
        """Multiple requests should handle rate limits."""
        manager = DataSourceManager.create(
            DataProvider.BINANCE,
            MarketType.SPOT
        )

        symbols = ["BTCUSDT", "ETHUSDT", "BNBUSDT"]
        end = datetime.now(timezone.utc) - timedelta(days=7)
        start = end - timedelta(days=1)

        for symbol in symbols:
            try:
                df = manager.get_data(
                    symbol=symbol,
                    interval=Interval.H1,
                    start_time=start,
                    end_time=end
                )
                assert df is not None
            except Exception:
                time.sleep(1)  # Wait and retry
                continue

        manager.close()
```

## Related

- [Test Markers](../SKILL.md#test-markers)
- [Coverage Reference](../references/coverage.md)
