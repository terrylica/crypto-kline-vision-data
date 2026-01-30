---
name: dsm-testing
description: Run tests for data-source-manager with proper markers and coverage. Use when writing tests, running tests, or checking test coverage for DSM.
argument-hint: "[test-pattern]"
---

# Testing Data Source Manager

## Test Organization

```
tests/
├── unit/                    # Fast, no network (~0.5s)
├── integration/             # External services
├── okx/                     # OKX API integration
└── fcp_pm/                  # FCP protocol tests
```

## Running Tests

### Unit Tests (Fast)

```bash
# Quick validation
uv run -p 3.13 pytest tests/unit/ -v

# With coverage
uv run -p 3.13 pytest tests/unit/ --cov=src/data_source_manager --cov-report=term-missing
```

### Integration Tests

```bash
# Requires network access
uv run -p 3.13 pytest tests/integration/ -v

# OKX-specific tests
uv run -p 3.13 pytest tests/okx/ -m okx -v
```

### All Tests

```bash
uv run -p 3.13 pytest tests/ -v
```

## Test Markers

| Marker                     | Purpose                       |
| -------------------------- | ----------------------------- |
| `@pytest.mark.integration` | Tests that call external APIs |
| `@pytest.mark.okx`         | OKX-specific tests            |
| `@pytest.mark.serial`      | Must run sequentially         |

## Writing New Tests

```python
import pytest
from data_source_manager import DataSourceManager, DataProvider, MarketType

class TestMyFeature:
    """Tests for MyFeature."""

    def test_basic_functionality(self):
        """Verify basic operation."""
        # Arrange
        manager = DataSourceManager.create(DataProvider.BINANCE, MarketType.SPOT)

        # Act
        result = manager.some_method()

        # Assert
        assert result is not None
        manager.close()

    @pytest.mark.integration
    def test_with_network(self):
        """Test requiring network access."""
        # Mark with @pytest.mark.integration for external calls
        pass
```

## Mocking HTTP Calls

```python
from unittest.mock import patch, MagicMock

@patch("data_source_manager.core.sync.data_source_manager.FSSpecVisionHandler")
@patch("data_source_manager.core.sync.data_source_manager.UnifiedCacheManager")
def test_with_mocks(self, mock_cache, mock_handler):
    mock_handler.return_value = MagicMock()
    mock_cache.return_value = MagicMock()
    # Test logic...
```

## Examples

Practical test examples:

- @examples/unit-test-patterns.md - Basic tests, fixtures, mocking
- @examples/integration-test-patterns.md - API tests, markers, FCP testing

## Helper Scripts

Quick test runner:

```bash
# Run all quick checks (lint + unit tests + import)
./docs/skills/dsm-testing/scripts/run_quick_tests.sh

# Run with test pattern filter
./docs/skills/dsm-testing/scripts/run_quick_tests.sh test_timestamp
```

## Detailed References

For deeper information, see:

- @references/fixtures.md - Pytest fixtures and auto-cleanup patterns
- @references/coverage.md - Coverage configuration and thresholds
- @references/mocking-patterns.md - DSM-specific mocking patterns
