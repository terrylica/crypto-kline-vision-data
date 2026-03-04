# Pytest Markers Reference

Comprehensive guide to pytest markers used in crypto-kline-vision-data.

## Built-in Markers

### @pytest.mark.integration

Tests that require external network access or API calls.

```python
@pytest.mark.integration
def test_live_api_fetch():
    """Test that requires network access to Binance."""
    manager = CryptoKlineVisionData.create(DataProvider.BINANCE, MarketType.SPOT)
    df = manager.get_data(symbol="BTCUSDT", ...)
    assert len(df) > 0
    manager.close()
```

**Skip with**: `pytest -m "not integration"`

### @pytest.mark.okx

OKX-specific integration tests.

```python
@pytest.mark.okx
def test_okx_spot_data():
    """OKX spot market data fetch."""
    manager = CryptoKlineVisionData.create(DataProvider.OKX, MarketType.SPOT)
    ...
```

**Run only OKX**: `pytest -m okx`

### @pytest.mark.serial

Tests that must run sequentially (not parallelizable).

```python
@pytest.mark.serial
def test_cache_population():
    """Test that modifies shared cache state."""
    # Clear cache first
    # Fetch data to populate
    # Verify cache contents
```

**Run serially**: `pytest -m serial --dist=no`

## Custom CKVD Markers

### @pytest.mark.fcp

Tests for Failover Control Protocol behavior.

```python
@pytest.mark.fcp
def test_vision_fallback_to_rest():
    """Vision 403 should fall back to REST."""
    ...
```

### @pytest.mark.cache

Cache-related tests.

```python
@pytest.mark.cache
def test_cache_hit():
    """Verify cache retrieval performance."""
    ...
```

### @pytest.mark.slow

Tests that take longer than 5 seconds.

```python
@pytest.mark.slow
def test_large_date_range():
    """Fetch 1 year of data - takes ~30s."""
    ...
```

**Skip slow**: `pytest -m "not slow"`

## Marker Configuration

In `pyproject.toml`:

```ini
[pytest]
markers =
    integration: marks tests as requiring network access
    okx: marks tests as OKX-specific
    serial: marks tests that cannot run in parallel
    fcp: marks tests for Failover Control Protocol
    cache: marks cache-related tests
    slow: marks tests that take >5 seconds
```

## Combining Markers

Run specific combinations:

```bash
# Integration tests except OKX
pytest -m "integration and not okx"

# FCP or cache tests
pytest -m "fcp or cache"

# Fast unit tests only
pytest -m "not integration and not slow"
```

## Marker Best Practices

1. **Always mark integration tests** - Distinguishes network-dependent tests
2. **Use slow marker** - Allows quick iteration during development
3. **Mark serial tests** - Prevents race conditions with pytest-xdist
4. **Combine markers** - Tests can have multiple markers
5. **Document markers** - Keep pyproject.toml markers list updated
