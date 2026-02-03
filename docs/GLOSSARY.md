# Glossary

Key terminology used in Data Source Manager.

## Core Concepts

| Term       | Definition                                                                                     |
| ---------- | ---------------------------------------------------------------------------------------------- |
| **DSM**    | Data Source Manager - the main orchestrator class for market data retrieval                    |
| **FCP**    | Failover Control Protocol - the priority-based data retrieval strategy (Cache → Vision → REST) |
| **Klines** | Candlestick/OHLCV data (Open, High, Low, Close, Volume)                                        |
| **OHLCV**  | Open, High, Low, Close, Volume - standard candlestick data columns                             |

## Data Sources

| Term           | Definition                                                                  |
| -------------- | --------------------------------------------------------------------------- |
| **Vision API** | Binance Vision - bulk historical data on AWS S3, ~48h delay, no rate limits |
| **REST API**   | Real-time Binance REST API, rate limited (6000 weight/minute)               |
| **Cache**      | Local Apache Arrow files for fast repeated access (~1ms)                    |

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

| Term           | Definition                                            |
| -------------- | ----------------------------------------------------- |
| **Weight**     | API cost unit, REST API limited to 6000 weight/minute |
| **Rate Limit** | Maximum allowed API requests per time period          |
| **403**        | HTTP error indicating future timestamp requested      |
| **429**        | HTTP error indicating rate limit exceeded             |

## Data Quality

| Term          | Definition                              |
| ------------- | --------------------------------------- |
| **Gap**       | Missing candles in a time series        |
| **Duplicate** | Repeated timestamps (invalid)           |
| **Monotonic** | Timestamps strictly increasing in order |

## Architecture

| Term                 | Definition                                                                         |
| -------------------- | ---------------------------------------------------------------------------------- |
| **DataProvider**     | Enum for exchange sources (BINANCE, TRADESTATION, OKX)                             |
| **MarketType**       | Enum for market categories (SPOT, FUTURES_USDT, FUTURES_COIN, FUTURES, OPTIONS)    |
| **ChartType**        | Enum for chart data types (KLINES, FUNDING_RATE, OKX_CANDLES, OKX_HISTORY_CANDLES) |
| **Interval**         | Enum for candle timeframes (1s to 1M)                                              |
| **DataSource**       | Enum for FCP source selection (AUTO, CACHE, VISION, REST)                          |
| **DataSourceConfig** | Configuration dataclass for DSM instances                                          |
