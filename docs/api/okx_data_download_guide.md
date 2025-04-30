# OKX Data Download Guide

## Overview

This document provides information about OKX's historical market data available through their Alibaba OSS-backed CDN and REST API. OKX offers various datasets for download through their data download page at [https://www.okx.com/data-download](https://www.okx.com/data-download) and through their public REST API endpoints.

Based on our investigation, OKX provides two primary types of historical market data, each accessed via a different mechanism:

1.  **CDN Downloads**: Provides raw trade and aggregate trade data in zipped CSV files.
2.  **REST API**: Provides processed candlestick (OHLCV) data.

Integrating OKX data into our data services stack requires understanding both of these sources and how they are prioritized within our data retrieval strategy, which is based on the Failover Control Protocol (FCP) mechanism.

## Data Retrieval Strategy and Failover Control Protocol (FCP)

Our data services stack utilizes the Failover Control Protocol (FCP) mechanism to retrieve historical market data. This protocol defines a prioritized sequence of data sources to ensure data completeness and efficiency. When retrieving data for OKX instruments, the FCP mechanism prioritizes sources in the following order:

1.  **Local Cache Retrieval**: Leverage local Apache Arrow files for the fastest access to previously cached data.
2.  **OKX CDN Download**: If data is not fully available in the cache, the next preferred source is the OKX CDN for raw trade or aggregate trade data. This data may require further processing (like resampling) to match required formats (e.g., OHLCV candles).
3.  **OKX REST API (History Candles)**: If data gaps remain after checking the cache and CDN, the `history-candles` endpoint of the OKX REST API is used to retrieve historical candlestick data.
4.  **OKX REST API (Candles)**: Finally, for the most recent data not yet available from other sources, the `candles` endpoint of the OKX REST API is queried.

This prioritized approach minimizes reliance on external APIs and leverages the fastest available sources first.

## Overall Data Provider Landscape

Our data services stack is designed to integrate data from multiple providers. Currently, we support or plan to support the following:

- **Binance**: Data sources include Vision API, REST API, and derived local cache (Arrow files).
- **OKX**: Data sources, prioritized for retrieval, include local cache (Arrow files), CDN downloads (trade/aggtrade), REST API (History Candles), and REST API (Candles).
- **TradeStation**: (Planned for future implementation)

The FCP mechanism is applied independently for each data provider, utilizing the specific data sources available from that provider in their defined priority order.

## OKX Data Sources in Detail

As outlined in the FCP strategy, OKX data is retrieved from two main external sources: the CDN for raw trade data and the REST API for processed candlestick data.

### 1. OKX CDN Downloads (Raw Trade Data)

The OKX CDN provides historical trade and aggregate trade data in compressed CSV files.

#### Data Structure and URL Pattern

Based on our investigation, OKX historical data follows this hierarchical structure on the CDN:

```
https://www.okx.com/cdn/okex/traderecords/
├── trades/
│   └── daily/
│       └── YYYYMMDD/
│           └── {symbol}-trades-{formatted-date}.zip
└── aggtrades/
    └── daily/
        └── YYYYMMDD/
            └── {symbol}-aggtrades-{formatted-date}.zip
```

The URL pattern for accessing specific files follows this format:

- Trades: `https://www.okx.com/cdn/okex/traderecords/trades/daily/{date}/{symbol}-trades-{formatted-date}.zip`
- Aggregate Trades: `https://www.okx.com/cdn/okex/traderecords/aggtrades/daily/{date}/{symbol}-aggtrades-{formatted-date}.zip`

Where:

- `{date}` is in format `YYYYMMDD` (e.g., 20230419)
- `{formatted-date}` is in format `YYYY-MM-DD` (e.g., 2023-04-19)
- `{symbol}` is the trading pair (e.g., BTC-USDT)

#### Available Data Types (CDN)

From our exploration, we've identified these main data types available via the CDN:

1.  **Trade Data (`trades`)**:

    - Individual trade records
    - Example: `BTC-USDT-trades-2023-04-19.zip`
    - Contains: trade_id, side, size, price, created_time

2.  **Aggregate Trade Data (`aggtrades`)**:
    - Aggregated trade data that might combine multiple trades
    - Example: `BTC-USDT-aggtrades-2023-04-23.zip`
    - Contains: trade_id, side, size, price, created_time

Note: Directory listings are disabled on the CDN, so direct browsing of available files is not possible.

#### Raw Data Format (CSV within Zip)

Both trade and aggregate trade data CSV files within zip archives share the same structure. Understanding this raw format is crucial for processing and potentially resampling this data into other formats like OHLCV candles.

```
trade_id/交易id,side/交易方向,size/数量,price/价格,created_time/成交时间
719977054,buy,0.00527188,84760.0,1682434103856
719977057,buy,0.0277,84760.0,1682434103856
...
```

Field descriptions:

- `trade_id`: Unique identifier for the trade
- `side`: Trade direction (buy/sell)
- `size`: Trade quantity
- `price`: Trade price
- `created_time`: Unix timestamp in milliseconds

#### Accessing Data from CDN

To download specific data files, use `curl` or any HTTP client:

```bash
curl -O https://www.okx.com/cdn/okex/traderecords/trades/daily/20230419/BTC-USDT-trades-2023-04-19.zip
```

Processing downloaded data typically involves unzipping the archive and reading the CSV file:

```bash
unzip BTC-USDT-trades-2023-04-19.zip -d extracted_data
```

```python
import pandas as pd

# Load the CSV file
df = pd.read_csv('extracted_data/BTC-USDT-trades-2023-04-19.csv')

# Display basic information
print(df.info())
print(df.head())
```

#### Batch Downloading Multiple Days (CDN)

For downloading multiple consecutive days from the CDN, you can use a script like this:

```bash
#!/bin/bash

# Define parameters
SYMBOL="BTC-USDT"
START_DATE="20230419"
DAYS=5

# Convert to date object for iteration
start_date=$(date -d "${START_DATE:0:4}-${START_DATE:4:2}-${START_DATE:6:2}" +%s)

for (( i=0; i<$DAYS; i++ )); do
  # Calculate current date
  current_date=$(date -d "@$((start_date + i*86400))" +%Y%m%d)
  formatted_date=$(date -d "@$((start_date + i*86400))" +%Y-%m-%d)

  # Build URL
  url="https://www.okx.com/cdn/okex/traderecords/trades/daily/${current_date}/${SYMBOL}-trades-${formatted_date}.zip"

  echo "Downloading $url"
  curl -O "$url"

  # Optional: Extract immediately
  # unzip "${SYMBOL}-trades-${formatted_date}.zip" -d "data/${formatted_date}"
done
```

### 2. OKX REST API (Candlestick Data)

OKX provides a comprehensive REST API for accessing processed candlestick (OHLCV) data. This data is available for both SPOT and SWAP instruments and supports multiple time intervals.

#### REST API Endpoints for Candlestick Data

OKX provides two primary endpoints for candlestick data:

1.  **Current Candles**: `/api/v5/market/candles` - Optimized for recent data.
2.  **Historical Candles**: `/api/v5/market/history-candles` - Optimized for historical data and provides deeper history.

#### Common Request Parameters (REST API)

Both the `/market/candles` and `/market/history-candles` endpoints share the following parameters:

Required parameter:

- `instId`: Instrument ID (e.g., `BTC-USDT` for SPOT, `BTC-USD-SWAP` for SWAP)

Optional parameters:

- `bar`: Time interval (e.g., `1m`, `1D`). Defaults to `1m` if omitted. See [Supported Time Intervals](#supported-time-intervals) for details.
- `limit`: Number of candles to return (max 300 for `/candles`, 100 for `/history-candles`).
- `before`: Pagination of data before a timestamp (Unix timestamp in milliseconds, exclusive)
- `after`: Pagination of data after a timestamp (Unix timestamp in milliseconds, exclusive)

#### Endpoint Comparison and Behavior Details (REST API)

The two candlestick endpoints have distinct characteristics and limitations:

| Feature                         | `/market/candles`                | `/market/history-candles`           |
| ------------------------------- | -------------------------------- | ----------------------------------- |
| **Use case**                    | Recent/current data              | Both recent and historical data     |
| **Data freshness**              | Most recent up to current minute | Delayed by ~1 minute from real-time |
| **Maximum records per request** | 300                              | 100                                 |
| **Overlap**                     | Overlaps with history-candles    | Shares some data with candles       |
| **Historical depth (1m)**       | Limited to ~24 hours             | From January 11, 2018               |
| **Historical depth (1D)**       | From May 21, 2021                | From October 10, 2017               |
| **Rate limit**                  | Same as other public endpoints   | Same as other public endpoints      |
| **1s Interval Support**         | No                               | Yes                                 |

\*Note: Tests confirm 1D data is available from October 10, 2017 through the history-candles endpoint.

**Notes on Behavior**:

Our tests show that the two endpoints have overlapping data, particularly for recent timestamps. This means you can retrieve the same candle from both endpoints during this overlapping period. The data values are identical between endpoints for the same timestamp.

- **Timestamp handling**: Both `before` and `after` parameters are exclusive (they don't include the exact timestamp). Both endpoints handle future timestamps without error (returns most recent data). Requesting very old timestamps (before available data) returns empty results without error. If `before` is older than `after`, the API ignores the invalid combination and still returns data.

- **Error handling**: Invalid instrument IDs return error code 51001: "Instrument ID doesn't exist". Invalid interval values return error code 51000: "Parameter bar error". Missing required parameters (`instId`) return HTTP 400 Bad Request. An empty instrument ID also returns API error code 51001.

#### Supported Time Intervals (REST API)

OKX supports various time intervals for both SPOT and SWAP instruments via the REST API. The following table outlines all supported intervals along with their availability by endpoint:

| Interval | Description | Parameter Value | Case Sensitivity                  | candles endpoint            | history-candles endpoint    |
| -------- | ----------- | --------------- | --------------------------------- | --------------------------- | --------------------------- |
| 1s       | 1 second    | `1s`            | Case-insensitive                  | ❌ Not available            | ✅ Available (~20 days)     |
| 1m       | 1 minute    | `1m`            | Case-insensitive                  | ✅ Available (~24 hours)    | ✅ Available (Jan 11, 2018) |
| 3m       | 3 minutes   | `3m`            | Case-insensitive                  | ✅ Available (~3 days)      | ✅ Available (Jan 11, 2018) |
| 5m       | 5 minutes   | `5m`            | Case-insensitive                  | ✅ Available (~5 days)      | ✅ Available (Jan 11, 2018) |
| 15m      | 15 minutes  | `15m`           | Case-insensitive                  | ✅ Available (~15 days)     | ✅ Available (Jan 11, 2018) |
| 30m      | 30 minutes  | `30m`           | Case-insensitive                  | ✅ Available (~30 days)     | ✅ Available (Jan 11, 2018) |
| 1H       | 1 hour      | `1H`            | Case-sensitive, must be uppercase | ✅ Available (~60 days)     | ✅ Available (Jan 11, 2018) |
| 2H       | 2 hours     | `2H`            | Case-sensitive, must be uppercase | ✅ Available (Dec 30, 2022) | ✅ Available (Jan 11, 2018) |
| 4H       | 4 hours     | `4H`            | Case-sensitive, must be uppercase | ✅ Available (Sep 02, 2022) | ✅ Available (Jan 11, 2018) |
| 6H       | 6 hours     | `6H`            | Case-sensitive, must be uppercase | ✅ Available (Aug 03, 2022) | ✅ Available (Jan 11, 2018) |
| 12H      | 12 hours    | `12H`           | Case-sensitive, must be uppercase | ✅ Available (Aug 03, 2022) | ✅ Available (Jan 11, 2018) |
| 1D       | 1 day       | `1D`            | Case-sensitive, must be uppercase | ✅ Available (May 21, 2021) | ✅ Available (Oct 10, 2017) |
| 1W       | 1 week      | `1W`            | Case-sensitive, must be uppercase | ✅ Available                | ✅ Available                |
| 1M       | 1 month     | `1M`            | Case-sensitive, must be uppercase | ✅ Available                | ✅ Available                |

**Important Notes on Intervals**:

- **Case sensitivity**: For minute intervals (1m, 3m, etc.), case doesn't matter. However, for hour, day, week, and month intervals, OKX requires uppercase letters (e.g., `1H` instead of `1h`). Using lowercase (e.g., `1h`, `1d`) will result in error code 51000: "Parameter bar error".

- **1-second interval**: The `history-candles` endpoint supports the `1s` parameter and returns data. Our testing confirms data availability of approximately 20 days. The `candles` endpoint rejects it with "Parameter bar error" and does not support `1s` at all. Historical 1-second data beyond this window is not available via the API and must be collected daily and stored locally.

- **Interval format**: Unlike some other exchanges, OKX strictly uses `1H` instead of `1h`, `1D` instead of `1d`, etc. for higher timeframes.

#### Candlestick Data Format (REST API)

The OKX REST API returns candlestick (Kline) data as an array of arrays in the `data` field. Each sub-array represents a single candlestick with the following structure:

| Index | Field     | Description                                                     | Type   |
| ----- | --------- | --------------------------------------------------------------- | ------ |
| 0     | timestamp | Unix timestamp in milliseconds                                  | string |
| 1     | open      | Opening price                                                   | string |
| 2     | high      | Highest price                                                   | string |
| 3     | low       | Lowest price                                                    | string |
| 4     | close     | Closing price                                                   | string |
| 5     | volume    | Trading volume                                                  | string |
| 6     | volumeUSD | Volume in USD                                                   | string |
| 7     | turnover  | Turnover (quote currency volume)                                | string |
| 8     | confirm   | Candle confirmation flag (1 = confirmed, 0 = not yet confirmed) | string |

Note: All fields are returned as strings and need to be converted to appropriate types for processing.

#### Historical Data Availability (REST API)

Through our systematic testing, we've determined that the earliest available data varies by interval and endpoint for the REST API:

1.  The earliest available data for BTC-USDT is:

    - For 1m (1-minute) interval data: From January 11, 2018 (timestamp: 1515669120000) in the history-candles endpoint
    - For 1D (daily) interval data: From May 21, 2021 in the candles endpoint and October 10, 2017 in the history-candles endpoint.

2.  Endpoint capabilities differ significantly:

    - **candles endpoint**: Primarily provides **very recent** data for 1m interval (~24 hours). Cannot retrieve any historical 1m data beyond this ~24-hour window. For intervals of 15m and larger, provides deeper historical data as noted in the "Supported Time Intervals" table.
    - **history-candles endpoint**: Provides comprehensive historical data access, including 1m data back to January 11, 2018 and 1D data back to October 10, 2017. Successfully returns data for all tested periods (1-730 days back from current date) for intervals it supports historically. Is the only option for obtaining any historical sub-daily data beyond the recent window offered by the `candles` endpoint.

3.  Data availability varies by instrument and may be more limited for newer or less popular trading pairs.

#### Best Practices for Data Retrieval (REST API)

Based on our findings and the FCP strategy, here are recommended practices for efficient data retrieval using the REST API sources (prioritized after Cache and CDN):

1.  **Historical Data (preferred)**: Use the `history-candles` endpoint to retrieve data from the past, as it offers deeper history for most intervals.
2.  **Recent Data (fallback)**: Use the `candles` endpoint to get the latest data up to the current minute, particularly for the most recent ~24 hours of 1m data not yet available elsewhere.
3.  **For large datasets**: Paginate through data using the `after` parameter, starting from the most recent date and working backward.
4.  **For performance**: Use the largest interval that meets your needs to minimize the number of requests.
5.  **For interval selection**:
    - Use 1D interval when possible for historical data, with availability back to May 21, 2021 for the `candles` endpoint and October 10, 2017 for the `history-candles` endpoint.
    - For sub-daily historical data (1H, 15m, 1m), use the `history-candles` endpoint unless the required depth is within the recent window provided by the `candles` endpoint (see "Supported Time Intervals" table for specifics).
    - For 1-second data, which is available only through the `history-candles` endpoint, our tests confirm availability for approximately 20 days.
6.  **For 1-minute historical data**:
    - For the most recent ~24 hours: You can use either the `candles` endpoint (up to 300 records per request) or the `history-candles` endpoint (up to 100 records per request).
    - For anything older than ~24 hours: You must use the `history-candles` endpoint.
    - Data is available back to January 11, 2018 for BTC-USDT, but only through the `history-candles` endpoint.

#### API Request Examples and Processing (REST API)

Below are examples for accessing both SPOT and SWAP data via the OKX REST API endpoints:

```bash
# Base URL for all API requests
BASE_URL="https://www.okx.com/api/v5/market"

# Fetching recent candles (same parameters for both SPOT and SWAP)
# Note: 'bar' parameter is optional and defaults to '1m'
curl -H "Accept: application/json" "${BASE_URL}/candles?instId=INSTRUMENT_ID&limit=LIMIT"

# Fetching historical candles (same parameters for both SPOT and SWAP)
curl -H "Accept: application/json" "${BASE_URL}/history-candles?instId=INSTRUMENT_ID&bar=INTERVAL&limit=LIMIT&after=TIMESTAMP"

# Using the 'before' parameter to get data before a specific timestamp
curl -H "Accept: application/json" "${BASE_URL}/history-candles?instId=INSTRUMENT_ID&bar=INTERVAL&limit=LIMIT&before=TIMESTAMP"

# Using both 'before' and 'after' parameters to specify a range
curl -H "Accept: application/json" "${BASE_URL}/history-candles?instId=INSTRUMENT_ID&bar=INTERVAL&limit=LIMIT&before=END_TIMESTAMP&after=START_TIMESTAMP"
```

Replace the placeholders with your specific values:

- `INSTRUMENT_ID`: BTC-USDT (SPOT) or BTC-USD-SWAP (SWAP)
- `INTERVAL`: Time interval (e.g., 1m, 1H, 1D)
- `LIMIT`: Number of records (max 300 for candles, 100 for history-candles)
- `TIMESTAMP`: Unix timestamp in milliseconds

#### Example for SPOT and SWAP (REST API)

```bash
# SPOT: Fetch 1-minute candles for BTC-USDT
curl -H "Accept: application/json" "${BASE_URL}/candles?instId=BTC-USDT&bar=1m&limit=100"

# SWAP: Fetch 1-minute candles for BTC-USD-SWAP
curl -H "Accept: application/json" "${BASE_URL}/candles?instId=BTC-USD-SWAP&bar=1m&limit=100"
```

#### Processing API Response in Python (REST API)

```python
import httpx
import pandas as pd
from datetime import datetime

# Function to fetch candlestick data
def fetch_okx_candles(instrument, interval, limit=100, endpoint="candles", after=None):
    """
    Fetch candlestick data from OKX API

    Args:
        instrument (str): Instrument ID (e.g., BTC-USDT or BTC-USD-SWAP)
        interval (str): Time interval (e.g., 1m, 1H, 1D)
        limit (int): Number of records to return (max 300 for candles, 100 for history-candles)
        endpoint (str): API endpoint to use ('candles' or 'history-candles')
        after (int, optional): Timestamp for pagination (Unix milliseconds)

    Returns:
        DataFrame: Processed candlestick data
    """
    base_url = "https://www.okx.com/api/v5/market"
    url = f"{base_url}/{endpoint}"

    params = {"instId": instrument, "bar": interval, "limit": limit}
    if after:
        params["after"] = after

    response = httpx.get(url, params=params)
    response.raise_for_status()
    data = response.json()

    if data.get("code") == "0":
        # Convert array data to DataFrame
        df = pd.DataFrame(data.get("data", []),
                          columns=["timestamp", "open", "high", "low", "close",
                                  "volume", "volumeUSD", "turnover", "confirm"])

        # Convert types
        df["timestamp"] = pd.to_datetime(df["timestamp"].astype(float), unit="ms")
        for col in ["open", "high", "low", "close", "volume", "volumeUSD", "turnover"]:
            df[col] = df[col].astype(float)

        return df
    else:
        raise Exception(f"API error: {data.get('msg')}")

# Example usage
# Using history-candles for potentially deeper historical data
btc_historical_df = fetch_okx_candles("BTC-USDT", "1m", 100, endpoint="history-candles", after=1674086400000)

# Using candles for the most recent data
btc_usdt_recent_df = fetch_okx_candles("BTC-USDT", "1D", 30, endpoint="candles")
btc_swap_recent_df = fetch_okx_candles("BTC-USD-SWAP", "1D", 30, endpoint="candles")


print(btc_historical_df.head())
print(btc_usdt_recent_df.head())
print(btc_swap_recent_df.head())
```

## Symbol Formatting

OKX uses a specific format for symbols that differs from some other exchanges. This is relevant for both CDN downloads and REST API requests.

### SPOT Markets

- Format: `BASE-QUOTE` (e.g., `BTC-USDT`)
- Examples: `BTC-USDT`, `ETH-USDT`, `SOL-USDT`
- Note: Binance-style symbols like `BTCUSDT` need to be converted to `BTC-USDT` format for OKX

### Perpetual Futures (SWAP) Markets

- Format: `BASE-USD-SWAP` (e.g., `BTC-USD-SWAP`)
- Examples: `BTC-USD-SWAP`, `ETH-USD-SWAP`, `SOL-USD-SWAP`
- Note: Binance-style symbols like `BTCUSDT` need to be converted to `BTC-USD-SWAP` format for OKX futures

### Delivery Futures

- Format: `BASE-QUOTE-YYMMDD` (e.g., `BTC-USD-230630`)
- Example: `BTC-USD-230630` for BTC futures expiring on June 30, 2023

## Limitations

Regardless of the source (CDN or REST API), there are general limitations to consider:

1.  **No Directory Listing (CDN)**: The OKX CDN has directory listings disabled, meaning you cannot browse available files directly.
2.  **No API for Data Discovery**: There is no documented API to programmatically discover available datasets on either the CDN or REST API beyond querying for specific instruments and intervals.
3.  **Authentication**: The CDN uses Alibaba OSS with authentication, but public access is granted to specific files. REST API requires no authentication for public endpoints.
4.  **Date Range Constraints**: Files on the CDN appear to be organized by date, but without documentation on what date ranges are available. REST API has documented historical limits by endpoint and interval.
5.  **API Request Limit (REST API)**: Confirmed through testing, the REST API has a maximum limit of 300 records per request for `/candles` and 100 for `/history-candles`.
6.  **Data Delay (REST API)**: Recent data may be delayed or unavailable through the `history-candles` endpoint compared to real-time.

## Instrument Analysis

OKX offers both SPOT and SWAP (perpetual futures) instruments for many cryptocurrencies. Our analysis shows that there are 30 cryptocurrencies that have both SPOT-USD and corresponding SWAP-USD-SWAP instruments.

### Key Pairs Available in Both SPOT and SWAP Markets

The following major cryptocurrencies have both SPOT and SWAP instruments available:

- BTC (Bitcoin): `BTC-USDT` and `BTC-USD-SWAP`
- ETH (Ethereum): `ETH-USDT` and `ETH-USD-SWAP`
- SOL (Solana): `SOL-USDT` and `SOL-USD-SWAP`
- TON (TON): `TON-USDT` and `TON-USD-SWAP`
- XRP (Ripple): `XRP-USDT` and `XRP-USD-SWAP`
- DOGE (Dogecoin): `DOGE-USDT` and `DOGE-USD-SWAP`
- ADA (Cardano): `ADA-USDT` and `ADA-USD-SWAP`
- AVAX (Avalanche): `AVAX-USDT` and `AVAX-USD-SWAP`

### Script for Analyzing Available Instruments

For a comprehensive analysis of available instruments, refer to our script (`playground/okx/analyze_instruments.py`) that identifies all cryptocurrencies available in both SPOT and SWAP markets.

## Historical Data Analysis Tools

### OKX Candles Depth Testing Script (REST API)

We've developed a specialized testing tool (`tests/okx/test_okx_candles_depth.py`) that uses binary search to efficiently determine the earliest available data for any instrument on OKX via the REST API. The tool focuses on the REST API endpoints (`candles` and `history-candles`) and their historical depth.

The tool:

1.  Performs a binary search between a known start date and the current date to find the earliest timestamp where data is available via the REST API.
    - Starting with a wide range (October 2017 to present)
    - Dividing the search space in half at each step
    - Converging on the earliest timestamp with precision down to the millisecond
2.  Verifies the exact earliest date by checking dates before and after the identified point.
3.  Compares the availability of data between the `candles` and `history-candles` endpoints across various intervals (1D, 1H, 15m, 1m, 1s).
4.  Tests recent-to-historical depth by checking data availability at specific time intervals from current date (1 day, 7 days, 30 days, 60 days, 90 days, 180 days, 365 days, and 730 days back).
5.  Uses a smart windowing algorithm to precisely determine the availability boundary for the `candles` endpoint:
    - First establishes a rough time window (e.g., between "now" and "7 days ago")
    - Uses binary search to narrow down to ~1-hour precision
    - Performs minute-by-minute testing around the boundary
    - Calculates exact time difference from current time

The binary search approach is highly efficient, allowing the script to:

- Find the earliest timestamp in just ~32 API calls (compared to thousands required for linear search)
- Achieve millisecond-level precision (the script identified May 21, 2021 16:00:00.001000 as the exact start timestamp for 1D data via the `candles` endpoint in one test)
- Complete the entire analysis in under 2 minutes

#### Interval-Specific Testing Results (REST API)

We've configured the tool to perform focused testing for specific intervals on the REST API. This testing precisely determined the historical depth for each interval as detailed in the "Supported Time Intervals" table.

1.  **1-minute (1m) data testing**:

    - Confirmed that the `history-candles` endpoint provides 1m data dating back to January 11, 2018.
    - Precisely determined that the `candles` endpoint only provides 1m data for the most recent ~24 hours (23 hours and 58 minutes in our tests).
    - Verified that the `candles` endpoint returns no data for any test points 1+ days ago.
    - Confirmed that the `history-candles` endpoint successfully returns 1m data for all tested time ranges (1-730 days back from current date).

2.  **Daily (1D) data testing**:
    - Found that the `history-candles` endpoint provides 1D data back to October 10, 2017 and the `candles` endpoint provides 1D data back to May 21, 2021.
    - Confirmed that both endpoints successfully return historical 1D data within their available ranges.

You can use this tool for your own analysis of REST API depth:

```bash
# Run the test with default settings (1m interval)
./tests/okx/test_okx_candles_depth.py
```

To modify the interval or instrument, edit the relevant constants in the script.

## Conclusion

OKX offers comprehensive market data access through both CDN downloads (raw trade data) and REST API endpoints (processed candlestick data). This guide provides information on both sources and how they are integrated into our FCP-based data retrieval strategy.

Key takeaways:

- OKX data is retrieved based on FCP priority: Cache -> CDN -> REST API (History Candles) -> REST API (Candles).
- The project integrates data from multiple providers, including Binance, OKX, and planned TradeStation.
- Use the proper OKX symbol format: `BASE-QUOTE` for SPOT and `BASE-USD-SWAP` for futures.
- CDN provides raw trade data in zipped CSV format, requiring potential resampling for OHLCV formats.
- REST API provides candlestick data with specific limits (300 for `/candles`, 100 for `/history-candles`).
- Multiple time intervals are supported via REST API, with historical depth varying by endpoint and interval (1s data only via `history-candles` for ~20 days).
- Historical data availability varies, with `history-candles` generally offering deeper history, especially for 1m data (back to Jan 11, 2018) and 1D data (back to Oct 10, 2017).
- Be aware of limitations such as no directory listing on CDN and API rate limits.

## Integration with Data Services Stack

When integrating OKX data with our existing data services stack, consider the following best practices:

### Symbol Conversion

Our existing systems typically use Binance-style concatenated symbols (e.g., `BTCUSDT`). When working with OKX, you need to convert these symbols to the OKX format depending on the market type (SPOT, Perpetual Futures SWAP, or Delivery Futures).

Refer to the **Symbol Formatting** section above for detailed examples and conversion rules for each market type.

### Data Processing (CDN)

Raw trade data downloaded from the CDN will need to be processed. If OHLCV data at specific intervals is required, the raw trade data will need to be resampled according to the desired timeframe. This involves:

1.  Loading the raw trade CSV data.
2.  Grouping trades by the desired time interval.
3.  Calculating the Open, High, Low, Close, and Volume for each interval.
4.  Ensuring data consistency and handling edge cases like intervals with no trades.

The specific implementation of the resampling logic will depend on the required intervals and the structure of the downstream data processing pipeline.
