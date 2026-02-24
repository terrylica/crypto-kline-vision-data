---
status: accepted
date: 2026-02-24
decision-maker: terrylica
consulted: Production users, Binance WebSocket documentation, python-statemachine patterns
research-method: Production streaming implementation validated with 520+ unit tests
---

# WebSocket Streaming Subsystem for Real-Time Kline Updates

## Context and Problem Statement

Crypto Kline Vision Data provides historical market data via FCP (Failover Control Protocol). However, real-time trading and live analytics require **streaming** candlestick updates as they arrive on the WebSocket, not periodic polling.

The streaming subsystem (v4.4.0) must:

1. Establish persistent WebSocket connections to Binance per market type (Spot, USDT Futures, Coin Futures)
2. Subscribe to multiple kline streams (symbol + interval pairs)
3. Parse incoming JSON updates into structured `KlineUpdate` objects
4. Handle backpressure gracefully when consumers are slow
5. Survive network failures with automatic reconnection
6. Integrate seamlessly with CKVD's existing API without breaking historical data workflows
7. Avoid imposing streaming dependencies on non-streaming users

## Decision Drivers

- **Seamless integration** - Streaming should not change FCP or historical APIs
- **Production-grade reliability** - Automatic reconnection with exponential backoff
- **Backpressure handling** - Must not lose confirmed data when consumer is slow
- **Dependency isolation** - Base package unaffected by streaming library versions
- **Provider agnosticity** - Architecture should support future non-Binance providers
- **State correctness** - Connection state machine must eliminate hand-rolled boolean bugs

## Considered Options

### 1. Drop-Oldest Backpressure

**Drop-oldest queue (discard oldest messages when full)**

- **Pros**: Simple to implement
- **Cons**: **Loses confirmed historical data** in the queue. Consumer catches up to older position, missing completed candles.
- **Outcome**: Rejected

### 2. Blocking Put (Block Consumer on Full Queue)

**Synchronous `queue.put()` with block=True**

- **Pros**: Preserves all messages
- **Cons**: Blocks the WebSocket message loop → connection appears hung → network timeout triggers reconnect → cascading failures
- **Outcome**: Rejected

### 3. Drop-Newest Backpressure (Chosen)

**Drop newest messages when queue full; preserve already-queued data**

- **Pros**:
  - Consumer catches up to current position without losing confirmed data
  - WebSocket loop never blocks → stays responsive
  - Soft failure (delayed messages, not lost ones)
- **Cons**: Recent messages dropped if consumer is very slow
- **Outcome**: **Accepted** — `KlineStream` uses `asyncio.Queue.put_nowait()` with drop-newest on `asyncio.QueueFull`

### 4. Hand-Rolled Connection State Machine

**Track connection state with booleans: `is_connected`, `is_reconnecting`, `backoff_active`**

- **Pros**: Full control, no external dependency
- **Cons**: **Prone to state explosion bugs** — can enter impossible states (e.g., `is_connected=True` and `is_reconnecting=True` simultaneously) → causes unhandled exceptions or silent hangs
- **Outcome**: Rejected

### 5. python-statemachine FSM (Chosen)

**10-state ConnectionMachine with typed transitions**

- **Pros**:
  - Compiler-checked state graph (invalid transitions raise `TransitionNotAllowed`)
  - Type-safe state callbacks
  - Clear visualization of connection lifecycle
  - Eliminates 6+ classes of state bugs
- **Cons**: External dependency (`python-statemachine>=2.4`)
- **Outcome**: **Accepted** — ConnectionMachine replaces 6 separate boolean/event flags

### 6. GZIP Compression on WebSocket

**Per-message deflate compression (reduce bandwidth)**

- **Pros**: Lower bandwidth usage
- **Cons**: **Binance WebSocket rejects deflate** — empirically discovered when testing. Setting `compression='deflate'` results in connection rejection with protocol error.
- **Outcome**: Rejected; `compression` field in `StreamConfig` enforced as `None` with validator

### 7. ValueError Hierarchy for Streaming Errors

**Inherit streaming errors from ValueError for FCP compatibility**

- **Pros**: Reuses FCP error handling infrastructure
- **Cons**: **FCP's error handler wraps `ValueError → RuntimeError`**, silently mangling streaming exceptions. Makes debugging nearly impossible.
- **Outcome**: Rejected; `StreamingError` inherits `Exception` directly (not `ValueError`)

### 8. Intermediate Candle Updates vs. Confirmed Only

**Emit all kline updates (intermediate + confirmed)**

- **Pros**: Lowest latency
- **Cons**: ~1800 intermediate updates per 1-hour candle (high CPU/memory overhead)
- **Alternative**: Filter using `k.x=True` gate (confirmed only) — default in `StreamConfig.confirmed_only=True`
- **Outcome**: **Accepted** — `confirmed_only=True` by default; callers can opt-in to all updates with `StreamConfig(confirmed_only=False)`

### 9. TYPE_CHECKING Lazy Imports

**Guard all streaming type imports with `TYPE_CHECKING` to avoid import overhead for non-streaming users**

- **Pros**: Zero import overhead in `__init__.py` for base CKVD
- **Cons**: Must use `importlib.import_module()` at runtime for circular-import-safe lazy loading
- **Outcome**: **Accepted** — All streaming types in `__init__.py` use `if TYPE_CHECKING:` blocks; `__getattr__` uses `importlib.import_module("ckvd.core.streaming.*")`

### 10. Optional Extras `[streaming]`

**Make streaming dependencies optional via `pip install ckvd[streaming]`**

- **Pros**: Base install remains lightweight; users only install streaming if needed
- **Cons**: Requires clear documentation; missing extras result in `ImportError`
- **Outcome**: **Accepted** — `pyproject.toml` defines `extras = {streaming: [websockets>=16.0, orjson>=3.10, python-statemachine>=2.4, stamina>=24.3]}`

### 11. stamina Retry on Connection Failures

**Use `@stamina.retry` decorator for exponential backoff reconnection**

- **Pros**: Production-grade retry logic (exponential backoff, jitter, deadline support)
- **Cons**: External dependency
- **Outcome**: **Accepted** — BinanceStreamClient uses `@stamina.retry(on=StreamConnectionError, attempts=5)` for reconnection logic

### 12. Protocol Duck-Typing (StreamClient)

**Define `StreamClient` as `@runtime_checkable Protocol` instead of ABC**

- **Pros**: Provider implementations don't need to inherit ABC; testable with mock objects without coupling to ABC hierarchy
- **Cons**: Developers must remember to use `isinstance(client, StreamClient)` for duck-type checking
- **Outcome**: **Accepted** — Enables future OKX streaming client without base class coupling

## Decision Outcome

The streaming subsystem uses **drop-newest backpressure**, **python-statemachine FSM for connection lifecycle**, **Exception hierarchy (not ValueError)**, **optional extras for dependencies**, and **Protocol duck-typing for provider agnosticity**.

### Architecture

```
BinanceStreamClient (WebSocket client, implements StreamClient Protocol)
    │ Creates/manages
    ▼
KlineStream (asyncio.Queue drop-newest backpressure, k.x gate)
    │ Wraps
    ▼
sync_bridge (threading.Thread + queue.Queue for sync callers)
    │
    ▼
CryptoKlineVisionData.stream_data_sync() → Iterator[KlineUpdate]
```

### Key Decisions

| Decision                            | Rationale                                                 | Proof                                                         |
| ----------------------------------- | --------------------------------------------------------- | ------------------------------------------------------------- |
| Drop-newest backpressure            | Preserve confirmed data; keep WebSocket loop responsive   | `KlineStream.put()` uses `put_nowait()` + drop on `QueueFull` |
| python-statemachine FSM             | Type-safe state transitions; eliminate boolean state bugs | `ConnectionMachine` (10 states) in `connection_manager.py`    |
| StreamingError ≠ ValueError         | Prevent FCP error handler from swallowing streaming exns  | `StreamingError(Exception)` in `streaming_exceptions.py`      |
| `compression=None` (enforced)       | Binance rejects per-message deflate on WebSocket          | `StreamConfig.compression` has `validator=in_([None])`        |
| `confirmed_only=True` (default)     | Reduce ~1800 intermediate updates per 1h candle           | `StreamConfig.confirmed_only: bool` gates `k.x` checks        |
| Optional extras `[streaming]`       | Base CKVD unaffected by streaming dep versions            | `extras = {streaming: [websockets, orjson, stamina, ...]}`    |
| TYPE_CHECKING lazy imports          | Zero overhead for non-streaming users                     | `if TYPE_CHECKING: from ckvd.core.streaming import ...`       |
| Protocol duck-typing (StreamClient) | Support future providers without ABC coupling             | `@runtime_checkable` on `StreamClient` Protocol interface     |
| stamina @retry on connect           | Production-grade exponential backoff                      | `BinanceStreamClient.connect()` uses `@stamina.retry`         |

## Consequences

### Good

- **Transparent history + streaming**: Both historical and real-time data share the same DataFrame/Polars structure
- **Composable with FCP**: Streaming is a new `DataSource.STREAMING` option in FCP, doesn't change existing behavior
- **Zero overhead for non-streaming users**: Base CKVD package has no `websockets`, `orjson`, `stamina` dependencies
- **Reliable backpressure handling**: Drop-newest preserves confirmed data; async loop stays responsive
- **Type-safe connection lifecycle**: FSM eliminates impossible states and unhandled exceptions
- **Provider agnostic**: `StreamClient` Protocol allows Binance + OKX + future providers without base class coupling
- **Production-grade reliability**: `stamina` retry with exponential backoff handles transient network failures

### Bad

- **State machine learning curve**: Developers must understand the 10-state ConnectionMachine (vs simple booleans)
- **Optional dependency management**: Streaming requires `pip install ckvd[streaming]`; users get `ImportError` if extras not installed
- **Drop-newest semantics**: If consumer is much slower than WebSocket, recent messages are silently dropped (not all data is preserved)
- **Intermediate updates filtered by default**: `confirmed_only=True` by default — callers must opt-in with `StreamConfig(confirmed_only=False)` for all updates

### Additional Constraints

- **Per-market-type URLs**: Binance uses different endpoints for Spot, USDT Futures, Coin Futures — BinanceStreamClient must create market-type-aware URLs
- **k.x gate semantics**: The `k.x=True` (closed/confirmed) field from Binance determines whether a candle is finalized; intermediate updates have `k.x=False`
- **Subscription model**: Streams are subscribed/unsubscribed per symbol + interval pair; reconnection must re-subscribe to active symbols

## Implementation Details

### Directory Structure

```
src/ckvd/core/streaming/
├── __init__.py              # Public API exports (KlineStream, StreamConfig, KlineUpdate)
├── kline_update.py          # KlineUpdate frozen dataclass with slots
├── stream_config.py         # StreamConfig attrs with validators
├── stream_client.py         # StreamClient @runtime_checkable Protocol
├── connection_manager.py    # ConnectionMachine 10-state FSM
├── kline_stream.py          # KlineStream async context manager (drop-newest queue)
└── sync_bridge.py           # sync_bridge threading layer for sync callers

src/ckvd/core/providers/binance/
└── binance_stream_client.py # BinanceStreamClient WebSocket implementation

src/ckvd/utils/for_core/
└── streaming_exceptions.py  # StreamingError hierarchy (Exception, not ValueError)
```

### StreamConfig Fields

| Field                    | Type  | Default  | Purpose                                                       |
| ------------------------ | ----- | -------- | ------------------------------------------------------------- |
| `market_type`            | enum  | REQUIRED | Market type (SPOT, FUTURES_USDT, FUTURES_COIN)                |
| `provider`               | enum  | BINANCE  | Data provider (BINANCE)                                       |
| `max_reconnect_attempts` | int   | 5        | Max reconnect retries before StreamReconnectExhaustedError    |
| `reconnect_delay_base`   | float | 1.0s     | Base delay for exponential backoff                            |
| `reconnect_delay_max`    | float | 60.0s    | Max backoff ceiling                                           |
| `ping_interval`          | float | 20.0s    | WebSocket keepalive ping interval                             |
| `ping_timeout`           | float | 10.0s    | Seconds to wait for pong response                             |
| `queue_maxsize`          | int   | 1000     | asyncio.Queue size; drop-newest when full                     |
| `confirmed_only`         | bool  | True     | Emit only `k.x=True` (confirmed) updates; filter intermediate |
| `compression`            | None  | None     | MUST be None (Binance rejects deflate)                        |
| `log_level`              | str   | ERROR    | Logging verbosity (DEBUG, INFO, WARNING, ERROR, CRITICAL)     |

### Exception Hierarchy

```
StreamingError (Exception, not ValueError!)
├── StreamConnectionError        # WebSocket connect/handshake failed
├── StreamSubscriptionError      # subscribe/unsubscribe failed
├── StreamReconnectExhaustedError # Max reconnect attempts exceeded
├── StreamTimeoutError           # Read or ping timeout
├── StreamMessageParseError      # JSON decode or schema validation failed
└── StreamBackpressureError      # Queue full (soft error, msg dropped)
```

All exceptions carry `.details: dict[str, Any]` for machine-parseable error context.

### Usage Example

```python
from ckvd import CryptoKlineVisionData, DataProvider, MarketType
from ckvd.core.streaming import StreamConfig

manager = CryptoKlineVisionData.create(DataProvider.BINANCE, MarketType.FUTURES_USDT)

config = StreamConfig(
    market_type=MarketType.FUTURES_USDT,
    confirmed_only=True,  # Only finalized candles
    queue_maxsize=1000,
)

# Synchronous iterator (blocks in background thread)
for kline in manager.stream_data_sync("BTCUSDT", "1h", config=config):
    print(f"Symbol: {kline.symbol}, Close: {kline.close}, Confirmed: {kline.is_closed}")

manager.close()
```

### KlineUpdate Structure

```python
@dataclass(frozen=True, slots=True)
class KlineUpdate:
    symbol: str           # e.g., "BTCUSDT"
    interval: str         # e.g., "1h"
    open_time_ms: int     # Unix milliseconds (open_time)
    close_time_ms: int    # Unix milliseconds (close_time)
    open: float
    high: float
    low: float
    close: float
    volume: float
    quote_volume: float
    trade_count: int
    is_closed: bool       # k.x (whether candle is finalized)
    raw: dict             # Original Binance message for debugging
```

## Related ADRs

- [Failover Control Protocol (2025-01-30)](2025-01-30-failover-control-protocol.md) — FCP architecture that streaming integrates with
- [src-layout Package Structure (2025-01-30)](2025-01-30-src-layout-package-structure.md) — Core/providers layout

## More Information

- `src/ckvd/core/streaming/` — Streaming implementation
- `src/ckvd/core/providers/binance/binance_stream_client.py` — Binance WebSocket client
- `src/ckvd/utils/for_core/streaming_exceptions.py` — Exception definitions
- `pyproject.toml` — `extras = {streaming: [websockets, orjson, stamina, python-statemachine]}`
- `docs/skills/ckvd-usage/references/fcp-protocol.md` — High-level FCP/streaming integration
