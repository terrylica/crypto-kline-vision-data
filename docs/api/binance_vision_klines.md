# Binance Vision API Kline Data Documentation

This document provides information about the available kline (candlestick) data granularity intervals on the Binance Vision API for spot and futures markets.

## Available Kline Intervals

The following intervals are available for historical kline data on Binance Vision API:

| Interval | Description      | URL Path Component | Status    |
| -------- | ---------------- | ------------------ | --------- |
| 1s       | 1 second         | 1s                 | Available |
| 1m       | 1 minute         | 1m                 | Available |
| 3m       | 3 minutes        | 3m                 | Available |
| 5m       | 5 minutes        | 5m                 | Available |
| 15m      | 15 minutes       | 15m                | Available |
| 30m      | 30 minutes       | 30m                | Available |
| 1h       | 1 hour           | 1h                 | Available |
| 2h       | 2 hours          | 2h                 | Available |
| 4h       | 4 hours          | 4h                 | Available |
| 6h       | 6 hours          | 6h                 | Available |
| 8h       | 8 hours          | 8h                 | Available |
| 12h      | 12 hours         | 12h                | Available |
| 1d       | 1 day (24 hours) | 1d                 | Available |

## URL Structure

The Binance Vision API follows a consistent URL structure for accessing historical kline data, with different formats depending on the market type:

### Spot Market

```url
https://data.binance.vision/data/spot/daily/klines/{SYMBOL}/{INTERVAL}/{SYMBOL}-{INTERVAL}-{DATE}.zip
```

And the corresponding checksum file:

```url
https://data.binance.vision/data/spot/daily/klines/{SYMBOL}/{INTERVAL}/{SYMBOL}-{INTERVAL}-{DATE}.zip.CHECKSUM
```

### USDT-Margined Futures (UM)

```url
https://data.binance.vision/data/futures/um/daily/klines/{SYMBOL}/{INTERVAL}/{SYMBOL}-{INTERVAL}-{DATE}.zip
```

And the corresponding checksum file:

```url
https://data.binance.vision/data/futures/um/daily/klines/{SYMBOL}/{INTERVAL}/{SYMBOL}-{INTERVAL}-{DATE}.zip.CHECKSUM
```

### Coin-Margined Futures (CM)

```url
https://data.binance.vision/data/futures/cm/daily/klines/{SYMBOL}_PERP/{INTERVAL}/{SYMBOL}_PERP-{INTERVAL}-{DATE}.zip
```

And the corresponding checksum file:

```url
https://data.binance.vision/data/futures/cm/daily/klines/{SYMBOL}_PERP/{INTERVAL}/{SYMBOL}_PERP-{INTERVAL}-{DATE}.zip.CHECKSUM
```

Where:

- `{SYMBOL}`: The trading pair (e.g., BTCUSDT for spot and UM, BTCUSD for CM)
- `{INTERVAL}`: One of the supported intervals from the table above
- `{DATE}`: Date in YYYY-MM-DD format

Note that for Coin-Margined Futures (CM), the symbol includes a `_PERP` suffix for perpetual contracts.

## Example URLs

Here are example URLs for accessing kline data for different market types:

### Spot Market (BTCUSDT)

```url
https://data.binance.vision/data/spot/daily/klines/BTCUSDT/1m/BTCUSDT-1m-2023-12-01.zip
https://data.binance.vision/data/spot/daily/klines/BTCUSDT/1h/BTCUSDT-1h-2023-12-01.zip
```

### USDT-Margined Futures (BTCUSDT)

```url
https://data.binance.vision/data/futures/um/daily/klines/BTCUSDT/1m/BTCUSDT-1m-2023-12-01.zip
https://data.binance.vision/data/futures/um/daily/klines/BTCUSDT/1h/BTCUSDT-1h-2023-12-01.zip
```

### Coin-Margined Futures (BTCUSD_PERP)

```url
https://data.binance.vision/data/futures/cm/daily/klines/BTCUSD_PERP/1m/BTCUSD_PERP-1m-2023-12-01.zip
https://data.binance.vision/data/futures/cm/daily/klines/BTCUSD_PERP/1h/BTCUSD_PERP-1h-2023-12-01.zip
```

## Data Format

The downloaded ZIP files contain CSV data with the following columns:

1. Open time
2. Open price
3. High price
4. Low price
5. Close price
6. Volume
7. Close time
8. Quote asset volume
9. Number of trades
10. Taker buy base asset volume
11. Taker buy quote asset volume
12. Ignore

## Cache Management

The Data Source Manager includes utilities for cache management using the `CacheKeyManager` class. The following cache key format and path structure is used when caching data:

### Cache Key Format

```python
# Key format: {exchange}_{market_type}_{data_nature}_{packaging_frequency}_{symbol}_{interval}_{YYYY-MM-DD}
cache_key = f"{exchange}_{market_type}_{data_nature}_{packaging_frequency}_{symbol}_{interval}_{date.strftime('%Y-%m-%d')}"
```

### Cache Path Structure

```python
# Path structure: {cache_dir}/{exchange}/{market_type}/{data_nature}/{packaging_frequency}/{symbol}/{interval}/{YYYY-MM-DD}.arrow
cache_path = cache_dir / exchange / market_type / data_nature / packaging_frequency / symbol / interval / f"{date.strftime('%Y-%m-%d')}.arrow"
```

### File Format

Data is cached in Apache Arrow format (`.arrow` files) for efficient storage and retrieval. This format provides:

1. Faster read/write operations compared to CSV
2. Lower memory usage for large datasets
3. Column-oriented storage for optimized query performance
4. Preserved data types and schema

#### Arrow File Structure

The Arrow cache files maintain a standardized schema:

| Column Name            | Data Type     | Description                                        | Index |
| ---------------------- | ------------- | -------------------------------------------------- | ----- |
| open_time              | Timestamp[ns] | Candle open time (used as index with UTC timezone) | Yes   |
| open                   | Float64       | Opening price of the candle                        | No    |
| high                   | Float64       | Highest price during the candle period             | No    |
| low                    | Float64       | Lowest price during the candle period              | No    |
| close                  | Float64       | Closing price of the candle                        | No    |
| volume                 | Float64       | Trading volume during the candle period            | No    |
| close_time             | Timestamp[ns] | Candle close time (with UTC timezone)              | No    |
| quote_asset_volume     | Float64       | Volume in the quote currency                       | No    |
| number_of_trades       | Int64         | Number of trades executed during the period        | No    |
| taker_buy_base_volume  | Float64       | Base asset volume from taker buy orders            | No    |
| taker_buy_quote_volume | Float64       | Quote asset volume from taker buy orders           | No    |

#### Data Processing and Storage

When data is cached:

1. The DataFrame is indexed by `open_time` with UTC timezone
2. Duplicate records are removed to ensure data integrity
3. A SHA-256 checksum is generated and stored for data verification
4. Records are sorted by the `open_time` index to maintain temporal order
5. Each Arrow file corresponds to a single day's data for a specific symbol and interval

#### Reading Cached Data

The `VisionCacheManager` provides utilities for reading cached data with features such as:

- Memory mapping for efficient loading of large files
- Optional column filtering to reduce memory usage
- Automatic timezone handling (ensuring UTC timezone)
- Index validation and sorting

The `SafeMemoryMap` context manager ensures proper resource cleanup when accessing cached Arrow files.

The `VisionCacheManager` handles saving and loading data with features like:

- SHA-256 checksums for data integrity verification
- Duplicate record removal
- Memory-efficient loading using memory mapping
- Column filtering for reduced memory usage

## Integration with Data Source Manager

These kline intervals can be used with the Data Source Manager to fetch and cache historical candlestick data. When configuring data sources, use the interval values from the "URL Path Component" column in the table above.

## Verification

All intervals listed in this document were verified as available by testing API endpoint responses on the date of document creation.
