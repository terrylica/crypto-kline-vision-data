# Source Code Directory

Context-specific instructions for working with CKVD source code.

**Hub**: [Root CLAUDE.md](../CLAUDE.md) | **Siblings**: [tests/](../tests/CLAUDE.md) | [docs/](../docs/CLAUDE.md) | [examples/](../examples/CLAUDE.md) | [scripts/](../scripts/CLAUDE.md) | [playground/](../playground/CLAUDE.md)

---

## Package Structure

```
src/ckvd/
├── __init__.py              # Public API exports (lazy loading)
├── __probe__.py             # AI agent API introspection (GitHub #22)
├── core/
│   ├── sync/
│   │   ├── crypto_kline_vision_data.py  # Main CKVD class with FCP + streaming methods
│   │   ├── ckvd_types.py            # DataSource (incl. STREAMING), CKVDConfig
│   │   └── ckvd_lib.py              # High-level functions (fetch_market_data)
│   ├── streaming/
│   │   ├── __init__.py              # Public streaming API
│   │   ├── kline_update.py          # KlineUpdate dataclass (frozen, slots)
│   │   ├── stream_config.py         # StreamConfig attrs definition
│   │   ├── stream_client.py         # StreamClient Protocol interface
│   │   ├── connection_manager.py    # 10-state FSM, TransitionNotAllowed
│   │   ├── kline_stream.py          # KlineStream async context manager
│   │   └── sync_bridge.py           # Threading bridge for sync callers
│   └── providers/
│       ├── __init__.py              # ProviderClients, get_provider_clients factory
│       ├── binance/
│       │   ├── vision_data_client.py    # Vision API (S3)
│       │   ├── rest_data_client.py      # REST API
│       │   ├── cache_manager.py         # Arrow cache
│       │   ├── vision_path_mapper.py    # Vision S3 path resolution
│       │   ├── data_client_interface.py # Provider interface contract
│       │   ├── binance_funding_rate_client.py
│       │   └── binance_stream_client.py # BinanceStreamClient (WebSocket)
│       └── okx/                     # OKX provider
└── utils/
    ├── market_constraints.py    # Enums and validation (re-export)
    ├── config.py                # Feature flags (USE_POLARS_OUTPUT)
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
    └── for_core/                # FCP + Streaming internal utilities
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
        ├── vision_timestamp.py      # Vision timestamp parsing
        └── streaming_exceptions.py  # WebSocket streaming exceptions
```

---

## Key Classes

**FCP + Historical Data:**

| Class                   | Location                                | Purpose                                                 |
| ----------------------- | --------------------------------------- | ------------------------------------------------------- |
| `CryptoKlineVisionData` | `core/sync/crypto_kline_vision_data.py` | Main entry point with FCP + streaming methods           |
| `CKVDConfig`            | `core/sync/ckvd_types.py`               | Configuration dataclass                                 |
| `DataSource`            | `core/sync/ckvd_types.py`               | Data source enum (AUTO, REST, VISION, CACHE, STREAMING) |
| `DataProvider`          | `utils/market_constraints.py`           | Provider enum (BINANCE)                                 |
| `MarketType`            | `utils/market_constraints.py`           | Market type enum                                        |
| `Interval`              | `utils/market_constraints.py`           | Timeframe interval enum                                 |
| `PolarsDataPipeline`    | `utils/internal/polars_pipeline.py`     | Internal Polars processing                              |
| `FeatureFlags`          | `utils/config.py`                       | Feature flag configuration                              |

**Streaming (Real-time WebSocket):**

| Class                 | Location                                     | Purpose                                                              |
| --------------------- | -------------------------------------------- | -------------------------------------------------------------------- |
| `KlineUpdate`         | `core/streaming/kline_update.py`             | Candlestick update model (frozen dataclass)                          |
| `StreamConfig`        | `core/streaming/stream_config.py`            | Streaming configuration (confirmed_only, queue_maxsize, compression) |
| `StreamClient`        | `core/streaming/stream_client.py`            | StreamClient Protocol interface                                      |
| `KlineStream`         | `core/streaming/kline_stream.py`             | Async context manager for streaming updates                          |
| `ConnectionManager`   | `core/streaming/connection_manager.py`       | 10-state FSM for connection lifecycle                                |
| `BinanceStreamClient` | `providers/binance/binance_stream_client.py` | Binance WebSocket implementation                                     |

---

## AI Agent Introspection (**probe**.py)

Stateless API discovery module for AI agents (GitHub #22). No file I/O, no network, JSON-serializable output.

```python
from ckvd.__probe__ import discover_api, get_capabilities
import json
print(json.dumps(discover_api(), indent=2))
```

| Function             | Returns                                                              |
| -------------------- | -------------------------------------------------------------------- |
| `discover_api()`     | Complete API surface: classes, methods, functions, enums, exceptions |
| `get_capabilities()` | Capability matrix: providers, market types, intervals, FCP, formats  |

**Lazy import**: `from ckvd import __probe__` works via `importlib.import_module("ckvd.__probe__")` in `__init__.py` (not `from . import __probe__` — that causes infinite recursion with `__getattr__`).

---

## FCP Implementation (core/sync/crypto_kline_vision_data.py)

The Failover Control Protocol orchestrates data retrieval:

| Source | When Used                             | Latency    |
| ------ | ------------------------------------- | ---------- |
| Cache  | Data exists locally (Arrow files)     | ~1ms       |
| Vision | Historical data (>48h old), bulk S3   | ~1-5s      |
| REST   | Recent data (<48h), live, or fallback | ~100-500ms |

Key methods:

- `get_data()` - Main entry point, implements FCP
- `_get_from_cache()` - Check local Arrow cache (no-op when `use_cache=False`)
- `_save_to_cache()` - Persist to Arrow cache (no-op when `use_cache=False`)
- `_fetch_from_vision()` - Fetch from Binance Vision
- `_fetch_from_rest()` - Fall back to REST API
- `reconfigure_logging(log_level=)` - Change log level at runtime

**Cache toggle**: `use_cache=False` disables cache read/write. `CKVD_ENABLE_CACHE=false` env var also disables cache (honored in `CKVDConfig.__attrs_post_init__`, `CryptoKlineVisionData.__init__`, and `BinanceFundingRateClient.__init__`). `enforce_source=DataSource.CACHE` with `use_cache=False` raises `ValueError`.

**Cache population rules**: Cache complete days from Vision/REST. Never cache partial days, future timestamps, error responses, or data <48h old. The `OPTIMIZE_CACHE_PARTIAL_DAYS` flag in `utils/config.py` controls partial-day optimization (always on).

**Cache storage**: Apache Arrow IPC (`.arrow`) files with atomic writes (temp-file-then-rename). One file per day. Cache location via `platformdirs.user_cache_path('crypto-kline-vision-data')`. Clear with `mise run cache:clear`.

**Tracking data sources**: `get_data(..., include_source_info=True)` adds a `_data_source` column (CACHE/VISION/REST) to the returned DataFrame.

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

**DataFrame conventions**: Index is `open_time` (UTC, monotonic, no duplicates). Standard OHLCV columns: `open`, `high`, `low`, `close`, `volume` (all float64).

---

## Exception Hierarchy

```
REST API Exceptions (for_core/rest_exceptions.py):
RestAPIError (base)              # All carry .details dict (GitHub #23)
├── RateLimitError        # 429 — has retry_after attribute
├── HTTPError             # HTTP errors with status code
├── APIError              # API-specific error codes
├── NetworkError          # Network connectivity issues
├── RestTimeoutError      # Request timeout
└── JSONDecodeError       # JSON parsing failures

Vision API Exceptions (for_core/vision_exceptions.py):
VisionAPIError (base)            # All carry .details dict (GitHub #23)
├── DataFreshnessError        # Data too recent for Vision API
├── ChecksumVerificationError # Checksum validation failed
├── DownloadFailedError       # File download failed
└── DataNotAvailableError     # Auto-populates .details from attributes

Streaming Exceptions (for_core/streaming_exceptions.py):
StreamingError (base, NOT ValueError)  # All carry .details dict (GitHub #23)
├── StreamConnectionError             # WebSocket connection/handshake fails
├── StreamSubscriptionError           # Subscribe/unsubscribe operation fails
├── StreamReconnectExhaustedError     # Max reconnection attempts exceeded
├── StreamTimeoutError                # WebSocket read or ping times out
├── StreamMessageParseError           # JSON decoding or schema validation fails
└── StreamBackpressureError           # Consumer too slow, queue limit exceeded

UnsupportedIntervalError (ValueError)  # Also carries .details dict
```

**Exception `.details` dict** (GitHub #23): All exception base classes accept `details: dict[str, Any] | None = None` keyword arg. Default is `{}` (never `None`). `DataNotAvailableError` auto-populates `.details` from its structured attributes (symbol, market_type, requested_start, earliest_available). Backward compatible — existing `raise RestAPIError("msg")` still works.

**CRITICAL — StreamingError inheritance**: Streaming exceptions inherit `Exception` directly (NOT `ValueError`). FCP's error handler wraps `ValueError → RuntimeError`, which would mangle streaming exceptions if they inherited `ValueError`.

**FCP error flow**: Cache miss → try Vision → `VisionAPIError` → try REST → `RestAPIError` → raise.

**Streaming architecture**: WebSocket streaming is PARALLEL to FCP, not part of it. Streaming updates are merged with historical FCP data via `DataSource.STREAMING` priority in the merge strategy.

---

## Streaming API (Real-time WebSocket)

Real-time kline updates via Binance WebSocket. Streaming is independent of FCP but can be merged with historical data.

### Async Streaming

```python
from ckvd import CryptoKlineVisionData, DataProvider, MarketType, StreamConfig
import asyncio

async def stream_live_data():
    manager = CryptoKlineVisionData.create(DataProvider.BINANCE, MarketType.FUTURES_USDT)

    config = StreamConfig(
        market_type=MarketType.FUTURES_USDT,
        confirmed_only=True,  # Only @klines with k.x (candle confirmed)
        queue_maxsize=1000,   # Drop oldest if queue fills
        compression=None      # No compression
    )

    async with manager.create_stream(config) as stream:
        async for kline_update in stream.messages():
            # KlineUpdate: open_time, close, is_closed, symbol, interval, open, high, low, volume
            print(f"{kline_update.symbol} {kline_update.interval}: {kline_update.close}")

    manager.close()

# Run
asyncio.run(stream_live_data())
```

### Sync Streaming (Threading Bridge)

```python
from ckvd import CryptoKlineVisionData, DataProvider, MarketType

manager = CryptoKlineVisionData.create(DataProvider.BINANCE, MarketType.FUTURES_USDT)

# Iterator-based sync API — internally uses threading
for kline_update in manager.stream_data_sync("BTCUSDT", Interval.HOUR_1, confirmed_only=True):
    print(f"Update: {kline_update.open_time} → {kline_update.close}")
    # Yields KlineUpdate objects one-by-one
    # Stop by breaking out of loop

manager.close()
```

### KlineUpdate Model

```python
from ckvd import KlineUpdate
from datetime import datetime, timezone

# Each streaming update is a frozen dataclass
# kline_update.open_time     → datetime (UTC)
# kline_update.close         → float
# kline_update.is_closed     → bool (True if k.x set)
# kline_update.symbol        → str (e.g. "BTCUSDT")
# kline_update.interval      → str (e.g. "1h")
# kline_update.open          → float
# kline_update.high          → float
# kline_update.low           → float
# kline_update.volume        → float

# Construction from raw Binance WebSocket message
update = KlineUpdate.from_binance_ws(raw_ws_message)

# Or from a historical DataFrame row
update = KlineUpdate.from_historical_row(df.iloc[0], interval="1h")
```

### StreamConfig Parameters

```python
from ckvd import StreamConfig, MarketType

config = StreamConfig(
    market_type=MarketType.FUTURES_USDT,     # Required: market type
    confirmed_only=True,                      # Only k.x candles (default: True)
    queue_maxsize=1000,                       # Message queue size (default: 1000)
    compression=None                          # Compression method (default: None, enforced)
)
```

### Error Handling

```python
from ckvd.utils.for_core.streaming_exceptions import (
    StreamConnectionError, StreamSubscriptionError, StreamReconnectExhaustedError
)

try:
    async with manager.create_stream(config) as stream:
        async for update in stream.messages():
            pass
except StreamConnectionError as e:
    print(f"Connection failed: {e}")
    print(f"Details: {e.details}")  # Machine-parseable context
except StreamReconnectExhaustedError as e:
    print(f"Reconnection attempts exhausted: {e}")
    print(f"Attempts: {e.details.get('attempts')}")
```

### Combining Streaming + Historical Data

```python
from datetime import datetime, timedelta, timezone
from ckvd import CryptoKlineVisionData, DataProvider, MarketType, Interval

# Fetch historical data
manager = CryptoKlineVisionData.create(DataProvider.BINANCE, MarketType.FUTURES_USDT)

end = datetime.now(timezone.utc) - timedelta(hours=1)
start = end - timedelta(days=7)

df_historical = manager.get_data("BTCUSDT", start, end, Interval.HOUR_1)
print(f"Historical: {len(df_historical)} candles")

# Stream future updates
for kline_update in manager.stream_data_sync("BTCUSDT", Interval.HOUR_1):
    # Each update is a KlineUpdate object
    print(f"Live: {kline_update.open_time} → {kline_update.close}")
    # Combine with historical_df as needed

manager.close()
```

---

## Binance API Reference

**Rate limits** (per minute, from Binance docs):

| Market       | Weight/min |
| ------------ | ---------- |
| Spot         | 6,000      |
| USDT Futures | 2,400      |
| Coin Futures | 2,400      |
| Vision API   | No limits  |

**HTTP status codes**: 403 = future timestamp requested, 429 = rate limited (wait 60s), 418 = IP banned.

**Timestamps**: All Binance timestamps are Unix milliseconds (UTC). `open_time` = **start** of candle period.

**Vision API delay**: ~48h from market close. Recent data not in Vision, falls through to REST.

---

## Symbol Formats

| Market Type  | Format           | Example     |
| ------------ | ---------------- | ----------- |
| SPOT         | `{BASE}{QUOTE}`  | BTCUSDT     |
| FUTURES_USDT | `{BASE}{QUOTE}`  | BTCUSDT     |
| FUTURES_COIN | `{BASE}USD_PERP` | BTCUSD_PERP |

**Validation**: `validate_symbol_for_market_type(symbol, market_type)` returns `bool` and raises `ValueError` with suggestion on invalid input. `get_market_symbol_format(symbol, market_type)` auto-converts.

**Security** (GitHub #21, CWE-22): Symbols are validated against allowlist regex `^[A-Z0-9_-]{1,30}$` before use in file paths or API URLs. Rejects path traversal (`../`), null bytes (`\x00`), slashes, empty strings, and symbols >30 chars. Defense-in-depth guard also in `get_data()` after `symbol.upper()`.

**Wrong format = empty DataFrame**: `BTCUSDT` on `FUTURES_COIN` returns empty. Use `BTCUSD_PERP`.

---

## Timestamp Patterns

```python
# Always UTC
from datetime import datetime, timezone
now = datetime.now(timezone.utc)

# Unix ms ↔ datetime
dt = datetime.fromtimestamp(ms / 1000, tz=timezone.utc)
ms = int(dt.timestamp() * 1000)

# open_time semantics: start of candle
# open_time=14:00 + interval=1h → covers 14:00:00–14:59:59
```

**Pitfalls**: Never mix UTC and local time. Never use `datetime.now()` (naive). Future timestamps (even 1ms ahead) cause 403 from Binance.

---

## Code Patterns

### Exception Handling

```python
from ckvd.utils.for_core.rest_exceptions import RateLimitError, RestAPIError
from ckvd.utils.for_core.vision_exceptions import VisionAPIError

try:
    df = manager.get_data(...)
except RateLimitError as e:
    time.sleep(e.retry_after or 60)
except (RestAPIError, VisionAPIError) as e:
    logger.error(f"Data fetch failed: {e}")
    logger.debug(f"Error details: {e.details}")  # Machine-parseable context
    raise
```

### HTTP Requests

```python
# CORRECT: Always with timeout
response = httpx.get(url, timeout=30)
```

### Debugging FCP

```python
os.environ["CKVD_LOG_LEVEL"] = "DEBUG"
manager = CryptoKlineVisionData.create(
    DataProvider.BINANCE, MarketType.FUTURES_USDT,
    log_level="DEBUG", suppress_http_debug=False,
)
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

- @docs/adr/2025-01-30-failover-control-protocol.md - FCP architecture
- @docs/api/ - Detailed Binance API documentation
- @docs/benchmarks/CLAUDE.md - Performance benchmarks
- @docs/TROUBLESHOOTING.md - Cache toggle and troubleshooting
