# Glossary

Key terminology used in Crypto Kline Vision Data.

## Core Concepts

| Term       | Definition                                                                                     |
| ---------- | ---------------------------------------------------------------------------------------------- |
| **CKVD**   | Crypto Kline Vision Data - the main orchestrator class for market data retrieval               |
| **FCP**    | Failover Control Protocol - the priority-based data retrieval strategy (Cache → Vision → REST) |
| **Klines** | Candlestick/OHLCV data (Open, High, Low, Close, Volume)                                        |
| **OHLCV**  | Open, High, Low, Close, Volume - standard candlestick data columns                             |
| **NDJSON** | Newline-Delimited JSON - structured telemetry format emitted by all examples to `events.jsonl` |

## Data Sources

| Term           | Definition                                                                                   |
| -------------- | -------------------------------------------------------------------------------------------- |
| **Vision API** | Binance Vision - bulk historical data on AWS S3, ~48h delay, no rate limits                  |
| **REST API**   | Real-time Binance REST API, rate limited (Spot: 6,000 / Futures: 2,400 weight/min)           |
| **Cache**      | Local Apache Arrow files for fast repeated access (~1ms)                                     |
| **WebSocket**  | Real-time Binance WebSocket streams (RFC 6455) — continuous kline updates as k.x gate events |

## Storage Concepts

| Term             | Definition                                                            |
| ---------------- | --------------------------------------------------------------------- |
| **Apache Arrow** | Columnar data format used for high-performance cache storage          |
| **Arrow files**  | `.arrow` files storing cached market data, organized by date          |
| **MMAP**         | Memory-mapped file I/O for fast cache reads without full file loading |

## Market Types

| Term             | Definition                                                     |
| ---------------- | -------------------------------------------------------------- |
| **SPOT**         | Spot market - immediate delivery trading                       |
| **FUTURES_USDT** | USDT-margined perpetual futures (um)                           |
| **FUTURES_COIN** | Coin-margined perpetual futures (cm), uses `*USD_PERP` symbols |
| **FUTURES**      | Legacy/generic futures type for backward compatibility         |
| **OPTIONS**      | Options trading market type                                    |

## Time Concepts

| Term          | Definition                                               |
| ------------- | -------------------------------------------------------- |
| **open_time** | The **start** timestamp of a candle period (not the end) |
| **Interval**  | Candle timeframe (1m, 5m, 1h, 4h, 1d, etc.)              |
| **UTC**       | All timestamps are in Coordinated Universal Time         |

## Symbol Formats

| Market       | Format           | Example     |
| ------------ | ---------------- | ----------- |
| SPOT         | `{BASE}{QUOTE}`  | BTCUSDT     |
| FUTURES_USDT | `{BASE}{QUOTE}`  | BTCUSDT     |
| FUTURES_COIN | `{BASE}USD_PERP` | BTCUSD_PERP |

## API Concepts

| Term           | Definition                                                                |
| -------------- | ------------------------------------------------------------------------- |
| **Weight**     | API cost unit, REST API limited per minute (Spot: 6,000 / Futures: 2,400) |
| **Rate Limit** | Maximum allowed API requests per time period                              |
| **403**        | HTTP error indicating future timestamp requested                          |
| **429**        | HTTP error indicating rate limit exceeded                                 |

## Data Quality

| Term          | Definition                              |
| ------------- | --------------------------------------- |
| **Gap**       | Missing candles in a time series        |
| **Duplicate** | Repeated timestamps (invalid)           |
| **Monotonic** | Timestamps strictly increasing in order |

## Architecture

| Term                | Definition                                                                                    |
| ------------------- | --------------------------------------------------------------------------------------------- |
| **DataProvider**    | Enum for exchange sources (BINANCE, TRADESTATION, OKX)                                        |
| **MarketType**      | Enum for market categories (SPOT, FUTURES_USDT, FUTURES_COIN, FUTURES, OPTIONS)               |
| **ChartType**       | Enum for chart data types (KLINES, FUNDING_RATE, OKX_CANDLES, OKX_HISTORY_CANDLES)            |
| **Interval**        | Enum for candle timeframes (1s to 1M)                                                         |
| **DataSource**      | Enum for data source selection (AUTO, CACHE, VISION, REST, STREAMING)                         |
| **CKVDConfig**      | Configuration dataclass for CKVD instances                                                    |
| **ResilientLogger** | Logger wrapper in `_telemetry.py` that auto-heals sinks destroyed by CKVD's `logger.remove()` |
| **trace_id**        | 16-char hex correlation ID grouping all events from one example run                           |
| **span_id**         | 8-char hex ID linking a `timed_span`'s start/complete/fail events                             |

## Streaming Architecture

| Term                    | Definition                                                                                                                             |
| ----------------------- | -------------------------------------------------------------------------------------------------------------------------------------- |
| **KlineUpdate**         | Frozen dataclass (frozen=True, slots=True) representing a real-time kline event from WebSocket                                         |
| **StreamConfig**        | Immutable streaming configuration (attrs.define, frozen=True, slots=True) with market_type, confirmed_only, queue_maxsize, compression |
| **KlineStream**         | Async context manager wrapping StreamClient with asyncio.Queue backpressure and gap tracking                                           |
| **StreamClient**        | @runtime_checkable Protocol for WebSocket provider duck-typing                                                                         |
| **ConnectionMachine**   | 10-state FSM managing WebSocket connection lifecycle                                                                                   |
| **BinanceStreamClient** | Concrete StreamClient implementation for Binance WebSocket streams                                                                     |
| **sync_bridge**         | Thread-safe bridge between async streaming and synchronous callers                                                                     |

## Streaming Concepts

| Term               | Definition                                                                                                         |
| ------------------ | ------------------------------------------------------------------------------------------------------------------ |
| **k.x gate**       | Binance WebSocket field `k.x` (is_closed boolean) indicating a closed/finalized candle; filtered by confirmed_only |
| **Drop-newest**    | Queue backpressure strategy discarding incoming messages when full, preserving older queued data                   |
| **StreamingError** | Base exception hierarchy for streaming failures (NOT ValueError — critical for FCP compatibility)                  |
