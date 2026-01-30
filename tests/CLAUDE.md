# Tests Directory

Context-specific instructions for working with DSM tests.

**Hub**: [Root CLAUDE.md](../CLAUDE.md) | **Siblings**: [src/](../src/CLAUDE.md) | [docs/](../docs/CLAUDE.md) | [examples/](../examples/CLAUDE.md)

---

## Quick Commands

```bash
# Unit tests only (fast, no network)
uv run -p 3.13 pytest tests/unit/ -v

# Integration tests (requires network)
uv run -p 3.13 pytest tests/integration/ -v

# OKX tests
uv run -p 3.13 pytest tests/okx/ -m okx -v

# All tests with coverage
uv run -p 3.13 pytest --cov=src/data_source_manager --cov-report=term-missing
```

---

## Directory Structure

| Directory      | Purpose                   | Network Required |
| -------------- | ------------------------- | ---------------- |
| `unit/`        | Fast, isolated tests      | No               |
| `integration/` | External API tests        | Yes              |
| `okx/`         | OKX-specific integration  | Yes              |
| `fcp_pm/`      | FCP protocol matrix tests | Yes              |

---

## Test Markers

```python
@pytest.mark.integration  # External service calls
@pytest.mark.okx          # OKX-specific tests
@pytest.mark.serial       # Must run sequentially
```

---

## Mocking Patterns

### Mock DataSourceManager

```python
from unittest.mock import patch, MagicMock

@patch("data_source_manager.core.sync.data_source_manager.FSSpecVisionHandler")
@patch("data_source_manager.core.sync.data_source_manager.UnifiedCacheManager")
def test_with_mocks(mock_cache, mock_vision):
    mock_cache.return_value = MagicMock()
    mock_vision.return_value = MagicMock()
    # Test logic here
```

### Mock HTTP Responses

```python
@patch("httpx.Client.get")
def test_rest_response(mock_get):
    mock_get.return_value = MagicMock(
        status_code=200,
        json=lambda: [{"open_time": 1234567890000, ...}]
    )
```

---

## Fixtures (conftest.py)

| Fixture             | Purpose                     |
| ------------------- | --------------------------- |
| `sample_ohlcv_data` | Standard OHLCV test data    |
| `mock_dsm`          | Pre-configured mock manager |
| `temp_cache_dir`    | Isolated cache for testing  |

---

## Writing New Tests

1. Use descriptive test names: `test_get_data_returns_empty_for_future_dates`
2. Follow Arrange-Act-Assert pattern
3. Always clean up resources (`manager.close()`)
4. Mark network-dependent tests with `@pytest.mark.integration`

---

## Related

- @docs/skills/dsm-testing/SKILL.md - Full testing guide
- @conftest.py - Shared fixtures
