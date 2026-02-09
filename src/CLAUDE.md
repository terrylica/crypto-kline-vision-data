# Source Code Directory

Context-specific instructions for working with CKVD source code.

**Hub**: [Root CLAUDE.md](../CLAUDE.md) | **Siblings**: [tests/](../tests/CLAUDE.md) | [docs/](../docs/CLAUDE.md) | [examples/](../examples/CLAUDE.md) | [scripts/](../scripts/CLAUDE.md) | [playground/](../playground/CLAUDE.md)

---

## Package Structure

```
src/ckvd/
├── __init__.py              # Public API exports (lazy loading)
├── core/
│   ├── sync/
│   │   ├── crypto_kline_vision_data.py  # Main CKVD class with FCP
│   │   ├── ckvd_types.py            # DataSource, CKVDConfig
│   │   └── ckvd_lib.py              # High-level functions (fetch_market_data)
│   └── providers/
│       ├── __init__.py              # ProviderClients, get_provider_clients factory
│       ├── binance/
│       │   ├── vision_data_client.py    # Vision API (S3)
│       │   ├── rest_data_client.py      # REST API
│       │   ├── cache_manager.py         # Arrow cache
│       │   ├── vision_path_mapper.py    # Vision S3 path resolution
│       │   ├── data_client_interface.py # Provider interface contract
│       │   └── binance_funding_rate_client.py
│       └── okx/                     # OKX provider
└── utils/
    ├── market_constraints.py    # Enums and validation (re-export)
    ├── config.py                # Feature flags (USE_POLARS_OUTPUT, ENABLE_CACHE)
    ├── loguru_setup.py          # Logging configuration
    ├── market/                  # Enums and validation (source)
    │   ├── enums.py             # DataProvider, MarketType, Interval, ChartType
    │   ├── validation.py        # Symbol validation functions
    │   ├── capabilities.py      # Market capabilities
    │   └── endpoints.py         # API endpoint URLs
    ├── cache/                   # Cache subsystem
    │   ├── key_manager.py       # Cache key generation
    │   ├── memory_map.py        # Memory-mapped Arrow reads
    │   ├── vision_manager.py    # Vision cache coordination
    │   ├── validator.py         # Cache integrity checks
    │   ├── functions.py         # Cache utility functions
    │   ├── options.py           # Cache configuration
    │   └── errors.py            # Cache-specific exceptions
    ├── network/                 # Network utilities
    │   ├── client_factory.py    # HTTP client creation
    │   ├── api.py               # API request helpers
    │   ├── download.py          # File download utilities
    │   ├── vision_download.py   # Vision-specific downloads
    │   └── exceptions.py        # Network exceptions
    ├── time/                    # Time utilities
    │   ├── bars.py              # Bar count calculations
    │   ├── conversion.py        # Timestamp conversions
    │   ├── filtering.py         # Time range filtering
    │   ├── intervals.py         # Interval math
    │   ├── processor.py         # Time processing pipeline
    │   └── timestamp_debug.py   # Timestamp debugging helpers
    ├── validation/              # Data validation
    │   ├── dataframe_validation.py   # DataFrame integrity checks
    │   ├── file_validation.py        # File format validation
    │   ├── time_validation.py        # Time range validation
    │   ├── availability_data.py      # Data availability checks
    │   └── availability_validation.py
    ├── internal/
    │   └── polars_pipeline.py   # PolarsDataPipeline class
    └── for_core/                # FCP internal utilities
        ├── ckvd_fcp_utils.py    # FCP orchestration (local imports for circular deps)
        ├── ckvd_api_utils.py    # Vision/REST fetch helpers
        ├── ckvd_cache_utils.py  # Cache LazyFrame utilities
        ├── ckvd_date_range_utils.py  # Date range calculations
        ├── ckvd_time_range_utils.py  # Time range splitting
        ├── ckvd_utilities.py    # General CKVD helpers
        ├── rest_exceptions.py   # REST API exceptions
        ├── rest_client_utils.py # REST client helpers
        ├── rest_data_processing.py  # REST response parsing
        ├── rest_metrics.py      # REST performance metrics
        ├── rest_retry.py        # REST retry logic
        ├── vision_exceptions.py # Vision API exceptions
        ├── vision_checksum.py   # Checksum verification
        ├── vision_constraints.py    # Vision data constraints
        ├── vision_file_utils.py     # Vision file handling
        └── vision_timestamp.py      # Vision timestamp parsing
```

---

## Key Classes

| Class                   | Location                                | Purpose                    |
| ----------------------- | --------------------------------------- | -------------------------- |
| `CryptoKlineVisionData` | `core/sync/crypto_kline_vision_data.py` | Main entry point with FCP  |
| `CKVDConfig`            | `core/sync/ckvd_types.py`               | Configuration dataclass    |
| `DataSource`            | `core/sync/ckvd_types.py`               | Data source enum           |
| `DataProvider`          | `utils/market_constraints.py`           | Provider enum (BINANCE)    |
| `MarketType`            | `utils/market_constraints.py`           | Market type enum           |
| `Interval`              | `utils/market_constraints.py`           | Timeframe interval enum    |
| `PolarsDataPipeline`    | `utils/internal/polars_pipeline.py`     | Internal Polars processing |
| `FeatureFlags`          | `utils/config.py`                       | Feature flag configuration |

---

## FCP Implementation (core/sync/crypto_kline_vision_data.py)

The Failover Control Protocol orchestrates data retrieval:

```
1. Cache check (Arrow files) → Fast path (~1ms)
2. Vision API (S3) → Bulk historical (~1-5s)
3. REST API → Real-time fallback (~100-500ms)
```

Key methods:

- `get_data()` - Main entry point, implements FCP
- `_get_from_cache()` - Check local Arrow cache (no-op when `use_cache=False`)
- `_save_to_cache()` - Persist to Arrow cache (no-op when `use_cache=False`)
- `_fetch_from_vision()` - Fetch from Binance Vision
- `_fetch_from_rest()` - Fall back to REST API

**Cache toggle**: `use_cache=False` disables cache read/write. `CKVD_ENABLE_CACHE=false` env var also disables cache. `enforce_source=DataSource.CACHE` with `use_cache=False` raises `ValueError`.

---

## Data Flow Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     INTERNAL (Polars)                        │
│  Cache → pl.scan_ipc() → LazyFrame                          │
│  Vision → pl.LazyFrame                                       │
│  REST → pl.DataFrame → .lazy()                              │
│                    ↓                                         │
│  PolarsDataPipeline._merge_with_priority() → LazyFrame      │
│                    ↓                                         │
│  .collect(engine='streaming') → pl.DataFrame                │
└─────────────────────────────────────────────────────────────┘
                         ↓
┌─────────────────────────────────────────────────────────────┐
│                   API BOUNDARY                               │
│  return_polars=False → .to_pandas() → pd.DataFrame (default)│
│  return_polars=True  → pl.DataFrame (zero-copy)             │
└─────────────────────────────────────────────────────────────┘
```

---

## Code Patterns

### Exception Handling

```python
# CORRECT: Specific exceptions
from ckvd.utils.for_core.rest_exceptions import RateLimitError, RestAPIError
from ckvd.utils.for_core.vision_exceptions import VisionAPIError

try:
    df = manager.get_data(...)
except RateLimitError:
    logger.warning("Rate limited, retrying...")
except (RestAPIError, VisionAPIError) as e:
    logger.error(f"Data fetch failed: {e}")
    raise
```

### Timestamp Handling

```python
# CORRECT: Always UTC
from datetime import datetime, timezone
now = datetime.now(timezone.utc)
```

### HTTP Requests

```python
# CORRECT: Always with timeout
response = httpx.get(url, timeout=30)
```

---

## Modification Guidelines

1. **Never use bare `except:`** - Always catch specific exceptions
2. **Always use UTC datetimes** - `datetime.now(timezone.utc)`
3. **Always add HTTP timeouts** - Explicit `timeout=` parameter
4. **Match symbol format to market type** - BTCUSDT vs BTCUSD_PERP
5. **Close managers** - Always call `manager.close()`
6. **Local imports in `ckvd_fcp_utils.py`** - Avoids circular deps with `ckvd_api_utils.py`

---

## Related

- @.claude/rules/fcp-protocol.md - FCP decision logic
- @.claude/rules/error-handling.md - Exception patterns
- @docs/adr/2025-01-30-failover-control-protocol.md - FCP architecture
- @docs/benchmarks/README.md - Performance benchmarks
