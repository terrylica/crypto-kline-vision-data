# OKX Data Download Guide

## Overview

This document provides information about OKX's historical market data available through their Alibaba OSS-backed CDN. OKX offers various datasets for download through their data download page at [https://www.okx.com/data-download](https://www.okx.com/data-download).

## Data Structure

Based on our investigation, OKX historical data follows this hierarchical structure:

```url
https://www.okx.com/cdn/okex/traderecords/
├── trades/
│   └── daily/
│       └── YYYYMMDD/
│           └── SYMBOL-PAIR-trades-YYYY-MM-DD.zip
└── aggtrades/
    └── daily/
        └── YYYYMMDD/
            └── SYMBOL-PAIR-aggtrades-YYYY-MM-DD.zip
```

### URL Pattern

The URL pattern for accessing specific files follows this format:

- Trades: `https://www.okx.com/cdn/okex/traderecords/trades/daily/{date}/{symbol}-trades-{formatted-date}.zip`
- Aggregate Trades: `https://www.okx.com/cdn/okex/traderecords/aggtrades/daily/{date}/{symbol}-aggtrades-{formatted-date}.zip`

Where:

- `{date}` is in format `YYYYMMDD` (e.g., 20250419)
- `{formatted-date}` is in format `YYYY-MM-DD` (e.g., 2025-04-19)
- `{symbol}` is the trading pair (e.g., BTC-USDT)

## Available Data Types

From our exploration, we've identified these main data types:

1. **Trade Data (`trades`)**:

   - Individual trade records
   - Example: `BTC-USDT-trades-2025-04-19.zip`
   - Contains: trade_id, side, size, price, created_time

2. **Aggregate Trade Data (`aggtrades`)**:
   - Aggregated trade data that might combine multiple trades
   - Example: `BTC-USD-250926-aggtrades-2025-04-23.zip`
   - Contains: trade_id, side, size, price, created_time

Note: Directory listings are disabled on the CDN, so direct browsing of available files is not possible.

## Data Format

### Trade Data CSV Format

Based on our sample investigation, the CSV files within the zip archives have the following structure:

```
trade_id/交易id,side/交易方向,size/数量,price/价格,created_time/成交时间
719977054,buy,0.00527188,84760.0,1745034103856
719977057,buy,0.0277,84760.0,1745034103856
...
```

Field descriptions:

- `trade_id`: Unique identifier for the trade
- `side`: Trade direction (buy/sell)
- `size`: Trade quantity
- `price`: Trade price
- `created_time`: Unix timestamp in milliseconds

### Aggregate Trade Data CSV Format

Similar to trade data but may represent aggregated trades:

```
trade_id/交易id,side/交易方向,size/数量,price/价格,created_time/成交时间
1236992,sell,50.0,93314.4,1745339279777
1236999,sell,7.0,93315.9,1745339283134
...
```

## Accessing the Data

### Direct Download

To download specific data files, use `curl` or any HTTP client:

```bash
curl -O https://www.okx.com/cdn/okex/traderecords/trades/daily/20250419/BTC-USDT-trades-2025-04-19.zip
```

### Processing Downloaded Data

1. Unzip the downloaded file:

   ```bash
   unzip BTC-USDT-trades-2025-04-19.zip -d extracted_data
   ```

2. The CSV file can then be processed using standard data analysis tools such as pandas:

   ```python
   import pandas as pd

   # Load the CSV file
   df = pd.read_csv('extracted_data/BTC-USDT-trades-2025-04-19.csv')

   # Display basic information
   print(df.info())
   print(df.head())
   ```

## Limitations

1. **No Directory Listing**: The OKX CDN has directory listings disabled, meaning you cannot browse available files directly.
2. **No API Access**: There is no documented API to programmatically discover available datasets.
3. **Authentication**: The CDN uses Alibaba OSS with authentication, but public access is granted to specific files.
4. **Date Range Constraints**: Files appear to be organized by date, but without documentation on what date ranges are available.

## Example Use Cases

### Downloading Historical Trade Data for a Specific Day

```bash
# Download trade data for BTC-USDT on April 19, 2025
curl -O https://www.okx.com/cdn/okex/traderecords/trades/daily/20250419/BTC-USDT-trades-2025-04-19.zip

# Unzip the file
unzip BTC-USDT-trades-2025-04-19.zip

# Preview the data
head BTC-USDT-trades-2025-04-19.csv
```

### Batch Downloading Multiple Days

For downloading multiple consecutive days, you can use a script like this:

```bash
#!/bin/bash

# Define parameters
SYMBOL="BTC-USDT"
START_DATE="20250419"
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

## Fetching Candlestick Data (REST API)

In addition to historical data available via CDN, OKX also provides access to recent candlestick (Kline) data through its public REST API.

To fetch the latest 1-minute candlestick data for the `BTC-USDT-SWAP` perpetual futures contract, you can use the `/api/v5/market/candles` endpoint.

- Endpoint: `https://www.okx.com/api/v5/market/candles`
- Required Parameters: `instId` (instrument ID) and `bar` (candlestick granularity).

Here are examples using `curl` to fetch this data:

1.  **Output to Terminal:**

    ```bash
    curl -H "Accept: application/json" "https://www.okx.com/api/v5/market/candles?instId=BTC-USDT-SWAP&bar=1m"
    ```

2.  **Save to File (`btc_usdt_swap_1m.json`):**

    ```bash
    curl -H "Accept: application/json" "https://www.okx.com/api/v5/market/candles?instId=BTC-USDT-SWAP&bar=1m" -o btc_usdt_swap_1m.json
    ```

3.  **Pretty-print and Save to File (`btc_usdt_swap_1m_pretty.json`) using `jq`:**

    ```bash
    curl -H "Accept: application/json" "https://www.okx.com/api/v5/market/candles?instId=BTC-USDT-SWAP&bar=1m" | jq '.' > btc_usdt_swap_1m_pretty.json
    ```

Ensure you have `jq` installed on your system to use the pretty-printing option.

### Candlestick Data Format (REST API)

The OKX REST API returns candlestick (Kline) data as an array of arrays in the `data` field. Each sub-array represents a single candlestick with the following structure:

| Index | Field     | Description                      |
| ----- | --------- | -------------------------------- |
| 0     | timestamp | Unix timestamp in milliseconds   |
| 1     | open      | Opening price                    |
| 2     | high      | Highest price                    |
| 3     | low       | Lowest price                     |
| 4     | close     | Closing price                    |
| 5     | volume    | Trading volume                   |
| 6     | volumeUSD | Volume in USD                    |
| 7     | turnover  | Turnover (quote currency volume) |
| 8     | confirm   | Candle confirmation flag         |

#### Example jq Command to Inspect Format

To view the first candlestick entry with labeled fields:

```bash
jq '.data[0] | {timestamp: .[0], open: .[1], high: .[2], low: .[3], close: .[4], volume: .[5], volumeUSD: .[6], turnover: .[7], confirm: .[8]}' btc_usdt_swap_1m_pretty.json
```

#### Example Output

```json
{
  "timestamp": "1745885580000",
  "open": "94861.6",
  "high": "94861.7",
  "low": "94841.7",
  "close": "94841.7",
  "volume": "1530.2",
  "volumeUSD": "15.302",
  "turnover": "1451460.628",
  "confirm": "0"
}
```

## Instrument Analysis

In addition to accessing historical data, we've developed a script to analyze the relationship between different OKX instruments, particularly focusing on identifying which cryptocurrencies are available in both SPOT and SWAP (perpetual futures) markets.

### Analysis Script

We've created a script (`playground/okx/analyze_instruments.py`) that helps analyze the relationship between SPOT and SWAP instruments on the OKX exchange. The script identifies cryptocurrencies that have both spot and perpetual futures markets available with USD as the quote currency.

#### Features

- Identifies SPOT instruments with corresponding SWAP instruments
- Filters instruments to show only USD quote currency pairs
- Provides detailed listings of matching pairs
- Displays statistical summaries of the instrument counts
- Fetches data directly from OKX API in real-time
- Optionally saves API responses to local files for offline analysis
- Supports both online (API) and offline (local file) modes

#### Prerequisites

- Python 3.x
- Required Python packages:
  - typer
  - rich
  - httpx

#### Usage

The script can be run from the command line with the following options:

```bash
python3 playground/okx/analyze_instruments.py [OPTIONS]
```

**Options:**

- `-v, --verbose`: Show detailed instrument listings
- `-u, --usd-only`: Show only SPOT-USD instruments with SWAP-USD-SWAP counterparts
- `-l, --use-local`: Use local JSON files instead of fetching from API
- `-s, --save-files`: Save API response to local JSON files
- `-h, --help`: Show the help message and exit

#### Example Usage

1. Basic analysis (statistics only, fetching from API):

   ```bash
   python3 playground/okx/analyze_instruments.py
   ```

2. Show all matched instruments and fetch from API:

   ```bash
   python3 playground/okx/analyze_instruments.py -v
   ```

3. Show only USD quote currency matches (fetching from API):

   ```bash
   python3 playground/okx/analyze_instruments.py -u
   ```

4. Show detailed USD quote currency matches (fetching from API):

   ```bash
   python3 playground/okx/analyze_instruments.py -u -v
   ```

5. Fetch from API and save data to local files:

   ```bash
   python3 playground/okx/analyze_instruments.py -s
   ```

6. Use local files instead of API:

   ```bash
   python3 playground/okx/analyze_instruments.py -l
   ```

#### API Endpoints Used

The script uses the following OKX API endpoints to fetch instrument data:

- SPOT instruments: `https://www.okx.com/api/v5/public/instruments?instType=SPOT`
- SWAP instruments: `https://www.okx.com/api/v5/public/instruments?instType=SWAP`

#### Sample Output

```
Statistics Summary:
Total SPOT instruments: 783
Total SWAP instruments with -USD-SWAP: 30
SPOT-USD instruments with corresponding SWAP-USD-SWAP instruments: 30

SPOT-USD Instruments with SWAP-USD-SWAP Counterparts:
1. BTC-USD, Base: BTC, Quote: USD
   └─ Swap: BTC-USD-SWAP
2. ETH-USD, Base: ETH, Quote: USD
   └─ Swap: ETH-USD-SWAP
3. SOL-USD, Base: SOL, Quote: USD
   └─ Swap: SOL-USD-SWAP
...
```

### Findings and Insights

Our analysis of OKX instruments reveals that:

1. OKX has 783 total SPOT instruments across various quote currencies
2. There are 30 SWAP instruments with the `-USD-SWAP` suffix
3. All 30 of these SWAP instruments have corresponding SPOT-USD instruments with the same base currency

The matching pairs cover major cryptocurrencies including:

- BTC (Bitcoin)
- ETH (Ethereum)
- SOL (Solana)
- TON (TON)
- XRP (Ripple)
- DOGE (Dogecoin)
- ADA (Cardano)
- AVAX (Avalanche)
- And 22 other cryptocurrencies

### Script Implementation

The script works by:

1. Loading instrument data from JSON files
2. Filtering SWAP instruments to find those with `-USD-SWAP` suffix
3. Extracting base currencies from both SPOT and SWAP instruments
4. Matching SPOT instruments with corresponding SWAP instruments
5. Optionally filtering to show only USD quote currency pairs
6. Displaying statistics and detailed listings

### Future Enhancements

Potential improvements to the analysis script:

1. Add support for other instrument types (e.g., options, futures)
2. Include additional filters for market data (e.g., by volume, liquidity)
3. Implement data visualization features
4. Add historical data analysis capabilities
5. Support for other exchanges beyond OKX

## Conclusion

OKX offers valuable historical market data through their CDN, but without comprehensive documentation on available datasets or date ranges. This guide provides a starting point for accessing and using the data based on observed patterns and samples. For the most up-to-date information, check OKX's official documentation or contact their support.

## Further Investigation

Further investigation could involve:

1. Programmatically testing various date ranges to determine data availability
2. Checking for additional data types beyond trades and aggtrades
3. Exploring other potential hierarchical patterns
4. Contacting OKX support for official documentation on their historical data
