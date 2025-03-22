# Binance Vision API Kline Data Documentation

This document provides information about the available kline (candlestick) data granularity intervals on the Binance Vision API for spot markets.

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

The Binance Vision API follows a consistent URL structure for accessing historical kline data:

```url
https://data.binance.vision/data/spot/daily/klines/{SYMBOL}/{INTERVAL}/{SYMBOL}-{INTERVAL}-{DATE}.zip
```

And the corresponding checksum file:

```url
https://data.binance.vision/data/spot/daily/klines/{SYMBOL}/{INTERVAL}/{SYMBOL}-{INTERVAL}-{DATE}.zip.CHECKSUM
```

Where:

- `{SYMBOL}`: The trading pair (e.g., BTCUSDT)
- `{INTERVAL}`: One of the supported intervals from the table above
- `{DATE}`: Date in YYYY-MM-DD format

## Example URLs

Here are example URLs for accessing BTCUSDT kline data for December 1, 2023:

```url
https://data.binance.vision/data/spot/daily/klines/BTCUSDT/1m/BTCUSDT-1m-2023-12-01.zip
https://data.binance.vision/data/spot/daily/klines/BTCUSDT/1h/BTCUSDT-1h-2023-12-01.zip
https://data.binance.vision/data/spot/daily/klines/BTCUSDT/1d/BTCUSDT-1d-2023-12-01.zip
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
# Key format: {symbol}_{interval}_{YYYYMM}
cache_key = f"{symbol}_{interval}_{date.strftime('%Y%m')}"
```

### Cache Path Structure

```python
# Path structure: {cache_dir}/{symbol}/{interval}/{YYYYMM}.arrow
cache_path = cache_dir / symbol / interval / f"{year_month}.arrow"
```

### File Format

Data is cached in Apache Arrow format (`.arrow` files) for efficient storage and retrieval. This format provides:

1. Faster read/write operations compared to CSV
2. Lower memory usage for large datasets
3. Column-oriented storage for optimized query performance
4. Preserved data types and schema

The `VisionCacheManager` handles saving and loading data with features like:

- SHA-256 checksums for data integrity verification
- Duplicate record removal
- Memory-efficient loading using memory mapping
- Column filtering for reduced memory usage

## Integration with Data Source Manager

These kline intervals can be used with the Data Source Manager to fetch and cache historical candlestick data. When configuring data sources, use the interval values from the "URL Path Component" column in the table above.

## Verification

All intervals listed in this document were verified as available by testing API endpoint responses on the date of document creation.
