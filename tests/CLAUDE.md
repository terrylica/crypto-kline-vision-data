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

# Unit tests with coverage
uv run -p 3.13 pytest tests/unit/ --cov=src/data_source_manager --cov-report=term-missing

# FCP edge case tests
uv run -p 3.13 pytest tests/fcp_pm/test_fcp_edge_cases.py -v
```

---

## Directory Structure

| Directory      | Purpose                   | Network Required |
| -------------- | ------------------------- | ---------------- |
| `unit/`        | Fast, isolated tests      | No               |
| `integration/` | External API tests        | Yes              |
| `okx/`         | OKX-specific integration  | Yes              |
| `fcp_pm/`      | FCP protocol matrix tests | Yes              |
| `stress/`      | Memory & fault tolerance  | Yes              |

### Unit Test Subdirectories

```
unit/
├── core/
│   ├── providers/binance/   # REST/Vision client tests
│   └── sync/                # DataSourceManager tests
├── utils/                   # Utility function tests
├── test_timestamp_semantics.py
└── test_dsm_logging_improvements.py
```

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

**IMPORTANT**: Use fixtures from `tests/conftest.py` - don't duplicate in test files.

### Time Fixtures

| Fixture            | Purpose                                           |
| ------------------ | ------------------------------------------------- |
| `utc_now`          | Current UTC time (use this, don't define locally) |
| `one_week_range`   | (start, end) tuple for 7 days                     |
| `one_day_range`    | (start, end) tuple for 1 day                      |
| `one_month_range`  | (start, end) tuple for 30 days                    |
| `historical_range` | Range ending 3 days ago (safe for Vision API)     |
| `recent_range`     | Last 2 hours (forces REST fallback)               |

### Mock Fixtures

| Fixture               | Purpose                      |
| --------------------- | ---------------------------- |
| `mock_vision_handler` | Mock FSSpec Vision API       |
| `mock_cache_manager`  | Mock cache manager           |
| `mock_all_sources`    | Combined mocks for isolation |

### Data Fixtures

| Fixture              | Purpose                  |
| -------------------- | ------------------------ |
| `sample_ohlcv_data`  | Standard OHLCV test data |
| `sample_symbol`      | "BTCUSDT"                |
| `sample_coin_symbol` | "BTCUSD_PERP"            |

### FCP Fixtures (fcp_pm/conftest.py)

| Fixture                | Purpose                                  |
| ---------------------- | ---------------------------------------- |
| `fcp_manager_spot`     | DSM for SPOT market with cache enabled   |
| `fcp_manager_futures`  | DSM for USDT futures with cache enabled  |
| `fcp_manager_coin`     | DSM for coin-margined with cache enabled |
| `fcp_manager_no_cache` | DSM with cache disabled for isolation    |

---

## Writing New Tests

1. Use descriptive test names: `test_get_data_returns_empty_for_future_dates`
2. Follow Arrange-Act-Assert pattern
3. Always clean up resources (`manager.close()`)
4. Mark network-dependent tests with `@pytest.mark.integration`

---

## FCP Edge Case Test Suite (tests/fcp_pm/test_fcp_edge_cases.py)

Comprehensive edge case tests for Failover Control Protocol. Run with `mise run test:fcp-edge`.

### Test Classes (10 edge case categories)

| Test Class               | Edge Case                   | Key Assertion                             |
| ------------------------ | --------------------------- | ----------------------------------------- |
| TestFCPCacheHit          | Cache fastest path          | CACHE >90%, timing <500ms                 |
| TestFCPVisionOnly        | Historical data (>7d old)   | VISION >50%, completeness >95%            |
| TestFCPRestOnly          | Recent data (<1h)           | REST >80%, data age <5min                 |
| TestFCPHybrid            | 48h boundary crossing       | Multiple sources, completeness >90%       |
| TestFCPFutureTimestamp   | Future end_time             | Graceful fail OR valid past data          |
| TestFCPSymbolValidation  | Wrong/correct symbol format | ValueError for wrong, success for correct |
| TestFCPRateLimitHandling | Rate limit error class      | RateLimitError importable (unit test)     |
| TestFCPIntervalCoverage  | 1m, 1h, 1d intervals        | Min bar count thresholds (parametrized)   |
| TestFCPEmptyResult       | Invalid symbol              | Non-silent failure (any exception)        |
| TestFCPCachePartialHit   | Gap filling                 | Monotonic, no duplicates                  |

### Audit Findings (2026-01-31)

**Issues Fixed:**

1. **Conditional assertions** - All source verification (CACHE, VISION, REST) now unconditional
2. **No-op test** - TestFCPRateLimitHandling now has actual assertions
3. **Missing completeness assertions** - TestFCPHybrid now asserts >90%
4. **Missing timing assertion** - TestFCPCacheHit now asserts <500ms

**Design Decisions:**

- TestFCPEmptyResult accepts multiple outcomes (intentional for edge case)
- TestFCPFutureTimestamp accepts data OR RuntimeError (documented DSM behavior)
- DAY_1 interval enforces REST to avoid Vision API CSV issues

### Source Verification Pattern

```python
# CORRECT: Unconditional assertion
assert "CACHE" in analysis["sources"], f"Expected CACHE, got: {list(analysis['sources'].keys())}"
cache_pct = analysis["sources"]["CACHE"]["percentage"]
assert cache_pct > 90, f"Expected >90%, got {cache_pct:.1f}%"

# WRONG: Conditional assertion (can pass without verification)
if "CACHE" in analysis["sources"]:
    assert ...  # Never runs if CACHE not present!
```

---

## Stress Test Suite (tests/stress/)

Memory efficiency and fault tolerance tests. Run with `mise run test:stress` or `pytest tests/stress/ -v -m stress`.

### Test Classes

| Test File              | Class                    | Focus                      |
| ---------------------- | ------------------------ | -------------------------- |
| `test_memory_pressure` | TestLargeHistoricalFetch | Large data fetch bounds    |
| `test_memory_pressure` | TestPolarsVsPandasMemory | Output format comparison   |
| `test_memory_pressure` | TestMixedSourceMerge     | FCP merge efficiency       |
| `test_object_churn`    | TestSequentialFetches    | Sequential fetch stability |
| `test_object_churn`    | TestResourceCleanup      | Resource release           |
| `test_fault_tolerance` | TestEmptyResults         | Empty/missing data         |
| `test_fault_tolerance` | TestErrorRecovery        | Error recovery patterns    |
| `test_fault_tolerance` | TestSymbolValidation     | Symbol format validation   |

### Stress Fixtures (stress/conftest.py)

| Fixture          | Purpose                                  |
| ---------------- | ---------------------------------------- |
| `memory_tracker` | tracemalloc context manager with peak_mb |
| `test_symbols`   | 10 common trading symbols                |
| `stress_range`   | 7-day historical range                   |
| `long_range`     | 30-day range for pressure tests          |

---

## Related

- @docs/skills/dsm-testing/SKILL.md - Full testing guide
- @conftest.py - Shared fixtures
- @tests/fcp_pm/test_fcp_edge_cases.py - FCP edge case tests
- @tests/stress/ - Memory and fault tolerance stress tests
