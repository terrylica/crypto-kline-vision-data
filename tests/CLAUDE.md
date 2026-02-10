# Tests Directory

Context-specific instructions for working with CKVD tests.

**Hub**: [Root CLAUDE.md](../CLAUDE.md) | **Siblings**: [src/](../src/CLAUDE.md) | [docs/](../docs/CLAUDE.md) | [examples/](../examples/CLAUDE.md) | [scripts/](../scripts/CLAUDE.md) | [playground/](../playground/CLAUDE.md)

---

## Quick Commands

```bash
# Unit tests only (fast, no network)
uv run -p 3.13 pytest tests/unit/ -v

# Single test file
uv run -p 3.13 pytest tests/unit/core/sync/test_fetch_market_data.py -v

# Integration tests (requires network)
uv run -p 3.13 pytest tests/integration/ -v

# OKX tests
uv run -p 3.13 pytest tests/okx/ -m okx -v

# Unit tests with coverage
uv run -p 3.13 pytest tests/unit/ --cov=src/ckvd --cov-report=term-missing

# FCP edge case tests
uv run -p 3.13 pytest tests/fcp_pm/test_fcp_edge_cases.py -v

# Stress tests
uv run -p 3.13 pytest tests/stress/ -v
```

---

## Directory Structure

| Directory               | Purpose                              | Network Required |
| ----------------------- | ------------------------------------ | ---------------- |
| `unit/`                 | Fast, isolated tests                 | No               |
| `integration/`          | External API tests                   | Yes              |
| [`okx/`](okx/CLAUDE.md) | OKX-specific integration             | Yes              |
| `fcp_pm/`               | FCP protocol matrix tests            | Yes              |
| `stress/`               | Memory & fault tolerance             | Yes              |
| `utils/`                | Test utilities (`data_integrity.py`) | No               |
| `fixtures/`             | Golden datasets (`.parquet`)         | No               |

### Unit Test Subdirectories

```
unit/
├── core/
│   ├── providers/
│   │   ├── binance/         # REST/Vision client tests
│   │   └── okx/             # OKX REST client tests
│   └── sync/                # CryptoKlineVisionData tests (has own conftest.py)
│       └── test_cache_toggle.py  # 34 cache toggle tests (CKVD_ENABLE_CACHE, use_cache, enforce_source)
├── utils/
│   ├── for_core/            # FCP utility tests
│   ├── internal/            # Polars pipeline tests
│   └── validation/          # Data validation tests
├── test_timestamp_semantics.py
└── test_ckvd_logging_improvements.py
```

### conftest.py Hierarchy

| File                               | Scope           | Key Role                                                      |
| ---------------------------------- | --------------- | ------------------------------------------------------------- |
| `tests/conftest.py`                | All tests       | Time, mock, data fixtures; `mock_provider_clients` factory    |
| `tests/unit/core/sync/conftest.py` | Unit sync tests | Autouse factory mock (old `@patch` decorators become no-ops)  |
| `tests/fcp_pm/conftest.py`         | FCP tests       | `fcp_manager_*` fixtures with real network                    |
| `tests/stress/conftest.py`         | Stress tests    | `memory_tracker`, `historical_time_range`, `large_time_range` |
| `tests/okx/conftest.py`            | OKX tests       | OKX-specific fixtures                                         |

---

## Test Markers

```python
@pytest.mark.integration  # External service calls
@pytest.mark.okx          # OKX-specific tests
@pytest.mark.serial       # Must run sequentially
```

---

## Mocking Patterns

### Mock via Provider Factory (preferred)

The codebase uses `get_provider_clients` factory pattern. Use the `mock_provider_clients` fixture from conftest.py:

```python
def test_with_factory_mock(mock_provider_clients):
    """mock_provider_clients patches get_provider_clients automatically."""
    manager = CryptoKlineVisionData.create(DataProvider.BINANCE, MarketType.SPOT)
    # Vision, REST, and cache clients are all mocked
    manager.close()
```

### Mock fetch_market_data (ckvd_lib)

```python
from unittest.mock import patch, MagicMock

@patch("ckvd.core.sync.ckvd_lib.CryptoKlineVisionData")
def test_fetch(mock_cls):
    mock_manager = MagicMock()
    mock_cls.return_value = mock_manager
    mock_manager.__enter__ = MagicMock(return_value=mock_manager)
    mock_manager.__exit__ = MagicMock(return_value=False)
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

| Fixture                 | Purpose                                         |
| ----------------------- | ----------------------------------------------- |
| `mock_provider_clients` | Mock `get_provider_clients` factory (preferred) |
| `mock_vision_handler`   | Mock Vision API (delegates to factory)          |
| `mock_cache_manager`    | Mock cache manager (delegates to factory)       |
| `mock_all_sources`      | Combined mocks for isolation                    |

### Data Fixtures

| Fixture              | Purpose                  |
| -------------------- | ------------------------ |
| `sample_ohlcv_data`  | Standard OHLCV test data |
| `sample_symbol`      | "BTCUSDT"                |
| `sample_coin_symbol` | "BTCUSD_PERP"            |

### FCP Fixtures (fcp_pm/conftest.py)

| Fixture                | Purpose                                   |
| ---------------------- | ----------------------------------------- |
| `fcp_manager_spot`     | CKVD for SPOT market with cache enabled   |
| `fcp_manager_futures`  | CKVD for USDT futures with cache enabled  |
| `fcp_manager_coin`     | CKVD for coin-margined with cache enabled |
| `fcp_manager_no_cache` | CKVD with cache disabled for isolation    |

---

## Writing New Tests

1. Use descriptive test names: `test_get_data_returns_empty_for_future_dates`
2. Follow Arrange-Act-Assert pattern
3. Always clean up resources (`manager.close()`)
4. Mark network-dependent tests with `@pytest.mark.integration`

---

## FCP Edge Case Test Suite (tests/fcp_pm/test_fcp_edge_cases.py)

Comprehensive edge case tests for Failover Control Protocol. Run with `mise run test:fcp-edge`.

### Test Classes (11 edge case categories)

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
| TestFCPPolarsIntegration | return_polars=True          | pl.DataFrame output, zero-copy path       |

### Audit Findings (2026-01-31)

**Issues Fixed:**

1. **Conditional assertions** - All source verification (CACHE, VISION, REST) now unconditional
2. **No-op test** - TestFCPRateLimitHandling now has actual assertions
3. **Missing completeness assertions** - TestFCPHybrid now asserts >90%
4. **Missing timing assertion** - TestFCPCacheHit now asserts <500ms

**Design Decisions:**

- TestFCPEmptyResult accepts multiple outcomes (intentional for edge case)
- TestFCPFutureTimestamp accepts data OR RuntimeError (documented CKVD behavior)
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

Memory efficiency and fault tolerance tests. Run with `uv run -p 3.13 pytest tests/stress/ -v`.

### Test Files (8 files)

| Test File                        | Focus                            |
| -------------------------------- | -------------------------------- |
| `test_memory_pressure.py`        | Large data fetch, memory bounds  |
| `test_object_churn.py`           | Sequential fetch stability       |
| `test_fault_tolerance.py`        | Error recovery, empty results    |
| `test_cache_stress.py`           | Cache read/write under load      |
| `test_concurrent_ckvd.py`        | Concurrent CKVD instances        |
| `test_extreme_volumes.py`        | High-volume data handling        |
| `test_performance_benchmarks.py` | Performance regression detection |
| `test_small_interval_stress.py`  | Sub-minute interval stress       |

### Stress Fixtures (stress/conftest.py)

| Fixture                 | Purpose                                  |
| ----------------------- | ---------------------------------------- |
| `memory_tracker`        | tracemalloc context manager with peak_mb |
| `test_symbols`          | 10 common trading symbols                |
| `historical_time_range` | 7-day historical range                   |
| `large_time_range`      | 30-day range for pressure tests          |

---

## Related

- @docs/skills/ckvd-testing/SKILL.md - Full testing guide
- @tests/conftest.py - Shared fixtures
- @tests/fcp_pm/test_fcp_edge_cases.py - FCP edge case tests
- @tests/stress/ - Memory and fault tolerance stress tests
