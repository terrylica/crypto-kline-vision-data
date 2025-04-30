# Comprehensive Binance REST API Endpoints Documentation

This document provides detailed information about Binance's REST API endpoints for retrieving market data, based on extensive empirical testing and analysis of response structures.

## 1. API Fundamentals

For general information on Binance API fundamentals, including rate limits, error handling, historical data availability, response times, and supported intervals across different market types, please refer to the following documents:

- [Binance API Rate Limits](binance_rate_limits.md): Detailed explanation of rate limit types, limits, headers, and errors.
- [Binance API Testing Results](binance_api_testing_results.md): Empirical findings on rate limits, historical data, intervals, response times, and common errors.
- [Binance API Best Practices](binance_best_practices.md): Strategies for efficient API consumption, including rate limit management and error handling.
- [`utils/market_constraints.py`](../../utils/market_constraints.py): Single source of truth for supported intervals and market constraints.

This section focuses on the specific API endpoint structure and available market data endpoints relevant to REST API usage.

### 1.1 Available Endpoints Overview

Binance offers several REST API endpoints for different market types:

| Market Type      | Base URL                        | Description                                     |
| ---------------- | ------------------------------- | ----------------------------------------------- |
| Spot             | <https://api.binance.com>       | Primary endpoint for spot trading               |
| Futures (USDT-M) | <https://fapi.binance.com>      | Perpetual futures contracts settled in USDT     |
| Futures (COIN-M) | <https://dapi.binance.com>      | Perpetual futures contracts settled in the coin |
| Options          | <https://eapi.binance.com>      | Options trading (limited functionality)         |
| Margin           | <https://api.binance.com/sapi/> | Margin trading with different paths             |

### 1.2 API URL Structure

The Binance REST API uses different URL patterns for different market types. Following these patterns is critical for ensuring proper connectivity to the Binance API.

#### 1.2.1 URL Patterns By Market Type

| Market Type      | Base URL                   | Path Pattern    | Example                                                                         |
| ---------------- | -------------------------- | --------------- | ------------------------------------------------------------------------------- |
| Spot             | <https://api.binance.com>  | /api/v3/klines  | <https://api.binance.com/api/v3/klines?symbol=BTCUSDT&interval=1m>              |
| Futures (USDT-M) | <https://fapi.binance.com> | /fapi/v1/klines | <https://fapi.binance.com/fapi/v1/klines?symbol=BTCUSDT&interval=1m>            |
| Futures (COIN-M) | <https://dapi.binance.com> | /dapi/v1/klines | <https://dapi.binance.com/dapi/v1/klines?symbol=BTCUSD_PERP&interval=1m>        |
| Options          | <https://eapi.binance.com> | /eapi/v1/klines | <https://eapi.binance.com/eapi/v1/klines?symbol=BTC-230531-60000-C&interval=1h> |

#### 1.2.2 Key Observations

- **Spot Market**: Includes `/api/` in the path
- **Futures (USDT-M)**: Uses `/fapi/` in the path (not `/api/`)
- **Futures (COIN-M)**: Uses `/dapi/` in the path (not `/api/`)
- **Options**: Uses `/eapi/` in the path (not `/api/`)

#### 1.2.3 Common Pitfalls

A common error is to assume all market types follow the same URL pattern as the Spot market (using `/api/` in the path). This can lead to connectivity failures when trying to access Futures or Options markets.

### 1.3 Rate Limiting and Weight System

Binance uses a weight-based rate limiting system. Each endpoint has an assigned weight, and users have a limit on the total weight they can use per minute.

#### 1.3.1 Rate Limit Headers

Headers returned in responses:

- `x-mbx-used-weight`: Current used weight
- `x-mbx-used-weight-1m`: Weight used in the last minute

#### 1.3.2 Default Weight Limits

Based on the exchange information response:

```json
[
  {
    "rateLimitType": "REQUEST_WEIGHT",
    "interval": "MINUTE",
    "intervalNum": 1,
    "limit": 6000
  }
]
```

- IP addresses have a default limit of 6000 weight per minute
- Weight costs vary by endpoint and parameters
- When reaching the limit, a 429 error is returned with a Retry-After header

#### 1.3.3 Empirical Weight Findings

Based on our empirical testing, the weight costs of different endpoints are:

| Endpoint                 | Weight (Single Symbol) | Weight (All Symbols) |
| ------------------------ | ---------------------- | -------------------- |
| /api/v3/klines           | 2                      | N/A                  |
| /api/v3/ticker/price     | 2                      | 4                    |
| /api/v3/trades           | 5                      | N/A                  |
| /api/v3/depth            | 5 (small limit)        | N/A                  |
| /api/v3/ticker/24hr      | 1                      | 40                   |
| /api/v3/historicalTrades | 5                      | N/A                  |

- Weight increments consistently with each request
- Weight costs are cumulative across all endpoints
- Weights are counted per minute, as shown in the headers
- The weight cost for the same endpoint may vary based on the parameters

#### 1.3.4 Weight Optimization Strategies

1. **Batch Requests**: Get all price tickers with weight 2 instead of individual requests at weight 1 each
2. **Use Lower Limit**: Request only the data you need to reduce weight
3. **Distribute Across Endpoints**: Use backup endpoints for better distribution
4. **Track Weights**: Monitor headers to avoid hitting limits
5. **Use Appropriate Endpoints**: Use lower-weight endpoints when possible (e.g., price ticker instead of 24hr ticker)

### 1.4 Response Limits

- The default number of records returned is 500
- The maximum number of records per request is 1000
- When requesting more than 1000 records (e.g., limit=1500), the API still returns only 1000 records (server-enforced limit)
- For historical data spanning more than 1000 candlesticks, multiple requests with different startTime/endTime parameters are required

### 1.5 Historical Data Limits

Empirical testing reveals important information about historical data limits:

- Spot market data is available back to at least March 2020 (5 years back)
- Some older data (early 2017/2018) may not be available for certain symbols
- Attempts to request data from before a symbol was listed return empty arrays (not errors)
- For very old data, Vision API is more reliable than REST API
- No specific "earliest date" limit was identified - availability seems to depend on when the symbol started trading
- Leveraged tokens like BTCUPUSDT have limited historical data compared to major pairs
- The data is typically available from when the symbol began trading on Binance

### 1.6 Error Responses

Common error responses:

#### Invalid Interval

```json
{
  "code": -1120,
  "msg": "Invalid interval."
}
```

#### Invalid Symbol

```json
{
  "code": -1121,
  "msg": "Invalid symbol."
}
```

#### Missing Required Parameter

```json
{
  "code": -1102,
  "msg": "Mandatory parameter 'symbol' was not sent, was empty/null, or malformed."
}
```

#### Invalid Parameter

```json
{
  "code": -1104,
  "msg": "Not all sent parameters were read; read '3' parameter(s) but was sent '4'."
}
```

#### Rate Limit Exceeded

```json
{
  "code": -1429,
  "msg": "Too many requests; current limit is 6000 request weight per 1 MINUTE. Please use the websocket for live updates to avoid polling the API."
}
```

#### Future Date Error (403 Forbidden)

When requesting data for timestamps in the future (even 1 millisecond ahead of the server's current time), the API returns a 403 Forbidden error. This is strictly enforced across all market types, but is especially critical for FUTURES_COIN markets.

**Error Response:**

```msg
HTTP/2 403 Forbidden
```

**Common Scenarios:**

- Current time is 2023-04-01 12:00:00.000 UTC
- Request for endTime=2023-04-01 12:00:00.001 (1ms in future)
- Result: 403 Forbidden

**Best Practices:**

1. Always validate timestamps against current UTC time before making API requests
2. Consider adding a small buffer (1-5 seconds) to account for clock drift
3. Use dynamic validation instead of hard-coded year checks
4. Implement proper error handling for 403 responses

**Example Implementation:**

```python
def validate_time_range(start_time, end_time):
    now = datetime.now(timezone.utc)
    if start_time > now or end_time > now:
        raise ValueError(f"Cannot request future date data. Current time: {now.isoformat()}")
    return start_time, end_time
```

**FUTURES_COIN Special Considerations:**
The FUTURES_COIN market (e.g., BTCUSD_PERP) is especially sensitive to future date requests and will consistently return 403 errors for future timestamps.

### 1.7 Response Time Analysis

Based on empirical testing, response times vary significantly between endpoints:

| Endpoint                  | Average Response Time |
| ------------------------- | --------------------- |
| Primary (api.binance.com) | ~0.14 seconds         |
| Backup (api3.binance.com) | ~0.44 seconds         |
| Data-only                 | ~0.53 seconds         |

- Primary endpoint is consistently faster than backup endpoints
- Data-only endpoint is typically the slowest
- Response time can vary significantly based on network conditions
- High-traffic periods may show increased response times
- Using multiple endpoints can help distribute load for better overall performance

## 2. Market Data Endpoints

### 2.1 Exchange Information

Get detailed information about available symbols and trading rules.

- **Endpoint:** `GET /api/v3/exchangeInfo`
- **Weight:** 10
- **Parameters:** None (or optional symbol/symbols)
- **Response Format:** Detailed JSON with exchange information
- **Example:** `https://api.binance.com/api/v3/exchangeInfo`

**Response Highlights:**

- Contains over 1000 trading pairs (symbols)
- Includes detailed filter information for each symbol
- Shows rate limits for the API
- Lists supported order types
- Provides information on which symbols support margin trading

### 2.2 Klines (Candlestick) Data

#### Spot Market Klines

- **Endpoint:** `GET /api/v3/klines`
- **Weight:** Varies (1-40 depending on parameters)
- **Parameters:**
  - `symbol` (STRING, required): Trading pair (e.g., BTCUSDT)
  - `interval` (ENUM, required): Candlestick interval
  - `startTime` (LONG, optional): Start time in milliseconds
  - `endTime` (LONG, optional): End time in milliseconds
  - `limit` (INT, optional): Default 500; max 1000
- **Example:** `https://api.binance.com/api/v3/klines?symbol=BTCUSDT&interval=1m&limit=5`

**Primary Endpoint:**

```url
https://api.binance.com/api/v3/klines
```

**Backup Endpoints (All Confirmed Working):**
All of these endpoints provide identical responses with the same data:

```url
https://api1.binance.com/api/v3/klines
https://api2.binance.com/api/v3/klines
https://api3.binance.com/api/v3/klines
https://api4.binance.com/api/v3/klines
https://api-gcp.binance.com/api/v3/klines
```

**Data-Only Endpoint:**
This endpoint also provides the same data, potentially with different rate limits:

```url
https://data-api.binance.vision/api/v3/klines
```

**Supported Intervals (Spot):**
All these intervals were empirically verified to be working:

| Interval | Description | Example URL Parameter |
| -------- | ----------- | --------------------- |
| 1s       | 1 second    | interval=1s           |
| 1m       | 1 minute    | interval=1m           |
| 3m       | 3 minutes   | interval=3m           |
| 5m       | 5 minutes   | interval=5m           |
| 15m      | 15 minutes  | interval=15m          |
| 30m      | 30 minutes  | interval=30m          |
| 1h       | 1 hour      | interval=1h           |
| 2h       | 2 hours     | interval=2h           |
| 4h       | 4 hours     | interval=4h           |
| 6h       | 6 hours     | interval=6h           |
| 8h       | 8 hours     | interval=8h           |
| 12h      | 12 hours    | interval=12h          |
| 1d       | 1 day       | interval=1d           |
| 3d       | 3 days      | interval=3d           |
| 1w       | 1 week      | interval=1w           |
| 1M       | 1 month     | interval=1M           |

**Response Format:**
The API returns an array of candlestick data with the following structure per element:

```json
[
  [
    1742634854000, // [0] Open time (milliseconds since epoch)
    "84299.20000000", // [1] Open price
    "84299.20000000", // [2] High price
    "84299.20000000", // [3] Low price
    "84299.20000000", // [4] Close price
    "0.00065000", // [5] Volume
    1742634854999, // [6] Close time (milliseconds since epoch)
    "54.79448000", // [7] Quote asset volume
    1, // [8] Number of trades
    "0.00000000", // [9] Taker buy base asset volume
    "0.00000000", // [10] Taker buy quote asset volume
    "0" // [11] Ignore field
  ]
]
```

**Timestamp Details:**
Based on empirical testing:

- For 1-second intervals:

  - Open time is in milliseconds (e.g., 1742634854000)
  - Close time is exactly 999 milliseconds later (e.g., 1742634854999)
  - The difference between open and close time is always 999ms

- For other intervals:
  - Open time is in milliseconds
  - Close time is calculated as (open time + interval - 1 millisecond)
  - For example, a 1-minute interval: close time = open time + 59999ms

**Empirical Findings:**

- Requesting limit=1500 will still return only 1000 records (server-enforced limit)
- The weight of a klines request is approximately 2 per request
- Timestamps are in milliseconds with precision to 999ms
- Parameter order in the URL does not affect the response (tested empirically)
- Historical data is available back to at least 2017 for major pairs
- Weight increments consistently by 2 for each kline request
- Response time varies by endpoint (primary is fastest, data-only is slowest)

#### Futures Market Klines

- **Primary Endpoint:** `GET /fapi/v1/klines`
- **Parameters:** Same as spot market
- **Example:** `https://fapi.binance.com/fapi/v1/klines?symbol=BTCUSDT&interval=1h&limit=10`

**Supported Intervals (Futures):**
All these intervals were empirically verified to be working:

| Interval | Description | Example URL Parameter |
| -------- | ----------- | --------------------- |
| 1m       | 1 minute    | interval=1m           |
| 3m       | 3 minutes   | interval=3m           |
| 5m       | 5 minutes   | interval=5m           |
| 15m      | 15 minutes  | interval=15m          |
| 30m      | 30 minutes  | interval=30m          |
| 1h       | 1 hour      | interval=1h           |
| 2h       | 2 hours     | interval=2h           |
| 4h       | 4 hours     | interval=4h           |
| 6h       | 6 hours     | interval=6h           |
| 8h       | 8 hours     | interval=8h           |
| 12h      | 12 hours    | interval=12h          |
| 1d       | 1 day       | interval=1d           |
| 3d       | 3 days      | interval=3d           |
| 1w       | 1 week      | interval=1w           |
| 1M       | 1 month     | interval=1M           |

**Key Difference:**

- Does NOT support 1-second (1s) intervals (confirmed empirically)

#### COIN-M Futures Klines

- **Primary Endpoint:** `GET /dapi/v1/klines`
- **Parameters:** Same as other market types
- **Example:** `https://dapi.binance.com/dapi/v1/klines?symbol=BTCUSD_PERP&interval=1m&limit=1`
- **Response Format:** Similar to spot market klines

**Key Differences:**

- Symbol format is different (e.g., `BTCUSD_PERP` instead of `BTCUSDT`)
- Does NOT support 1-second intervals (tested and confirmed)
- Price values may be displayed with fewer decimal places than spot

#### Continuous Futures Klines

For perpetual contracts that don't expire:

- **Endpoint:** `GET /fapi/v1/continuousKlines`
- **Parameters:**
  - `pair` (STRING, required): Base trading pair (e.g., BTCUSDT)
  - `contractType` (ENUM, required): PERPETUAL, CURRENT_MONTH, etc.
  - `interval` (ENUM, required): Time interval
  - Other parameters same as regular klines
- **Example:** `https://fapi.binance.com/fapi/v1/continuousKlines?pair=BTCUSDT&contractType=PERPETUAL&interval=1m&limit=1`

#### Options Market Klines

- **Endpoint:** `GET /eapi/v1/klines`
- **Parameters:**
  - `symbol` (STRING, required): Options contract (e.g., BTC-250627-55000-C)
  - `interval` (ENUM, required): Candlestick interval
  - `startTime` (LONG, optional): Start time in milliseconds
  - `endTime` (LONG, optional): End time in milliseconds
  - `limit` (INT, optional): Default 500; max 1000
- **Example:** `https://eapi.binance.com/eapi/v1/klines?symbol=BTC-250627-55000-C&interval=1m&limit=1`

**Response Format (Options Klines):**
The options API returns a different response structure than other markets:

```json
[
  {
    "open": "30800",
    "high": "30800",
    "low": "30800",
    "close": "30800",
    "volume": "0",
    "interval": "1m",
    "tradeCount": 0,
    "takerVolume": "0",
    "takerAmount": "0",
    "amount": "0",
    "openTime": 1743454200000,
    "closeTime": 1743454260000
  }
]
```

**Key Differences in Options Market:**

- Returns objects with named fields inside an array instead of positional arrays
- Includes named fields (e.g., "open", "high") instead of positional values
- Uses different field names for some values
- Does NOT support 1-second intervals

### 2.3 Trade Data Endpoints

#### Recent Trades

- **Endpoint:** `GET /api/v3/trades`
- **Weight:** 1
- **Parameters:**
  - `symbol` (STRING, required): Trading pair
  - `limit` (INT, optional): Default 500; max 1000
- **Example:** `https://api.binance.com/api/v3/trades?symbol=BTCUSDT&limit=5`

**Response Format:**

```json
[
  {
    "id": 4737522442, // Trade ID
    "price": "84378.45000000", // Price
    "qty": "0.00007000", // Quantity
    "quoteQty": "5.90649150", // Quote quantity (price * qty)
    "time": 1742635347154, // Trade time
    "isBuyerMaker": false, // If true, the buyer was the maker
    "isBestMatch": true // Best price match flag
  }
]
```

#### Historical Trades

- **Endpoint:** `GET /api/v3/historicalTrades`
- **Weight:** 5
- **Parameters:** Same as recent trades, plus optional `fromId`
- **Example:** `https://api.binance.com/api/v3/historicalTrades?symbol=BTCUSDT&limit=5`

#### Aggregate Trades

- **Endpoint:** `GET /api/v3/aggTrades`
- **Weight:** 1
- **Parameters:** Similar to trades, with additional time filtering options
- **Example:** `https://api.binance.com/api/v3/aggTrades?symbol=BTCUSDT&limit=5`

**Response Format:**

```json
[
  {
    "a": 3504251507, // Aggregate trade ID
    "p": "84379.59000000", // Price
    "q": "0.00015000", // Quantity
    "f": 4737522467, // First trade ID
    "l": 4737522467, // Last trade ID
    "T": 1742635356913, // Timestamp
    "m": true, // Was the buyer the maker?
    "M": true // Was the trade the best price match?
  }
]
```

### 2.4 Market Depth (Order Book)

- **Endpoint:** `GET /api/v3/depth`
- **Weight:** Varies (1-50 depending on limit)
- **Parameters:**
  - `symbol` (STRING, required): Trading pair
  - `limit` (INT, optional): Default 100; max 5000
- **Example:** `https://api.binance.com/api/v3/depth?symbol=BTCUSDT&limit=5`

**Response Format:**

```json
{
  "lastUpdateId": 64658426203,
  "bids": [
    [
      "84379.59000000", // Price level
      "6.52777000" // Quantity
    ]
    // More bid levels...
  ],
  "asks": [
    [
      "84379.60000000", // Price level
      "0.56063000" // Quantity
    ]
    // More ask levels...
  ]
}
```

### 2.5 Ticker Endpoints

#### 24hr Ticker

- **Endpoint:** `GET /api/v3/ticker/24hr`
- **Weight:** 1 for a single symbol, 40 for all symbols
- **Parameters:** Optional `symbol`
- **Example:** `https://api.binance.com/api/v3/ticker/24hr?symbol=BTCUSDT`

**Response Format:**

```json
{
  "symbol": "BTCUSDT",
  "priceChange": "355.67000000",
  "priceChangePercent": "0.423",
  "weightedAvgPrice": "84007.80445628",
  "prevClosePrice": "84019.31000000",
  "lastPrice": "84374.99000000",
  "lastQty": "0.00113000",
  "bidPrice": "84374.99000000",
  "bidQty": "7.97817000",
  "askPrice": "84375.00000000",
  "askQty": "0.03895000",
  "openPrice": "84019.32000000",
  "highPrice": "84584.00000000",
  "lowPrice": "83175.25000000",
  "volume": "9940.16569000",
  "quoteVolume": "835051495.54857270",
  "openTime": 1742548926167,
  "closeTime": 1742635326167,
  "firstId": 4735514853,
  "lastId": 4737522270,
  "count": 2007418
}
```

#### Price Ticker

- **Endpoint:** `GET /api/v3/ticker/price`
- **Weight:** 1 for a single symbol, 2 for all symbols
- **Parameters:** Optional `symbol`
- **Example:** `https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT`

**Response Format:**

```json
{
  "symbol": "BTCUSDT",
  "price": "84378.30000000"
}
```

#### Book Ticker

- **Endpoint:** `GET /api/v3/ticker/bookTicker`
- **Weight:** 1 for a single symbol, 2 for all symbols
- **Parameters:** Optional `symbol`
- **Example:** `https://api.binance.com/api/v3/ticker/bookTicker?symbol=BTCUSDT`

**Response Format:**

```json
{
  "symbol": "BTCUSDT",
  "bidPrice": "84443.58000000",
  "bidQty": "4.71443000",
  "askPrice": "84443.59000000",
  "askQty": "1.16154000"
}
```

**Empirical Findings on Ticker Comparison:**

- Price data is consistent between different ticker endpoints
- The `price` field in price ticker matches the `lastPrice` field in 24hr ticker
- Book ticker provides the current top of the order book (best bid and ask)
- Getting ALL price tickers at once has a significantly lower weight cost (2) than individual requests
- The 24hr ticker provides much more detailed information but has higher weight cost when requesting all symbols

## 3. Market Differences

Based on empirical testing, different market types have different capabilities:

| Feature              | Spot       | USDT-M Futures | COIN-M Futures | Options               |
| -------------------- | ---------- | -------------- | -------------- | --------------------- |
| 1s kline interval    | ✅ Yes     | ❌ No          | ❌ No          | ❌ No                 |
| Maximum klines limit | 1000       | 1000           | 1000           | 1000                  |
| Continuous klines    | N/A        | ✅ Yes         | ✅ Yes         | N/A                   |
| Base API path        | /api/v3/   | /fapi/v1/      | /dapi/v1/      | /eapi/v1/             |
| Symbol format        | BTCUSDT    | BTCUSDT        | BTCUSD_PERP    | BTC-YYMMDD-STRIKE-C/P |
| Response format      | Standard   | Standard       | Standard       | Named object format   |
| Price precision      | 8 decimals | 1-2 decimals   | 1 decimal      | Varies                |
| Historical data      | Extensive  | More limited   | More limited   | Very limited          |

### 3.1 Cross-Market Data Consistency

Empirical testing of prices across markets shows:

- Prices between spot and futures markets typically show small differences (basis)
- USDT-M futures prices typically track spot prices closely
- COIN-M futures (BTCUSD_PERP) show inverted price representation and slight variations
- Response formats differ between markets:
  - Spot price ticker returns simple objects
  - USDT-M futures includes a timestamp
  - COIN-M futures returns an array with additional fields

## 4. Key Differences Between REST API and Vision API

| Feature              | REST API                          | Vision API                          |
| -------------------- | --------------------------------- | ----------------------------------- |
| Data Freshness       | Real-time and historical          | Historical only (48+ hours delayed) |
| Access Method        | Direct HTTP requests              | ZIP file downloads                  |
| Rate Limits          | Weight-based (headers show usage) | None (static file downloads)        |
| Maximum Records      | 1000 per request                  | Full day/month of data per file     |
| 1s Data Availability | Available in spot markets only    | Available in spot markets only      |
| Data Format          | JSON array                        | CSV in ZIP files                    |
| Trading Pairs        | All currently active (3000+)      | May include delisted pairs          |
| Usage Scenario       | Real-time & recent historical     | Long-term historical research       |
| Historical Limits    | Varies by symbol                  | More comprehensive                  |
| Price Accuracy       | Standard precision                | Same as REST API                    |
| Bandwidth Usage      | Higher for multiple requests      | Lower (one-time download)           |

## 5. Practical Examples

### 5.1 Getting Historical Data Efficiently

```bash
# Start and end times in milliseconds (1 day apart)
curl "https://api.binance.com/api/v3/klines?symbol=BTCUSDT&interval=1h&startTime=1641081600000&endTime=1641167999999&limit=1000"
```

### 5.2 Getting Latest Data with Minimal Weight

```bash
# Get just the last 5 candles
curl "https://api.binance.com/api/v3/klines?symbol=BTCUSDT&interval=1m&limit=5"
```

### 5.3 Monitoring Multiple Symbols Efficiently

```bash
# Get all prices at once (weight: 2) instead of individually (weight: 1 each)
curl "https://api.binance.com/api/v3/ticker/price"
```

### 5.4 Basic Request (Spot Market)

```bash
curl "https://api.binance.com/api/v3/klines?symbol=BTCUSDT&interval=1m&limit=5"
```

### 5.5 Historical Data Request with Time Range

```bash
curl "https://api.binance.com/api/v3/klines?symbol=BTCUSDT&interval=1d&startTime=1640995200000&endTime=1641254400000"
```

### 5.6 Futures Market Request

```bash
curl "https://fapi.binance.com/fapi/v1/klines?symbol=BTCUSDT&interval=1h&limit=10"
```

### 5.7 COIN-M Futures Request

```bash
curl "https://dapi.binance.com/dapi/v1/klines?symbol=BTCUSD_PERP&interval=1h&limit=10"
```

### 5.8 Leveraged Token Data

```bash
curl "https://api.binance.com/api/v3/klines?symbol=BTCUPUSDT&interval=1d&limit=10"
```

## 6. Optimization Strategies

1. **Use Multiple Endpoints**: Distribute requests across available endpoints to avoid rate limiting.
2. **Batch Efficiently**: Request the maximum 1000 records per call to minimize API calls.
3. **Cache Results**: Implement local caching for frequently accessed data.
4. **Check Headers**: Monitor rate limit headers to avoid hitting limits.
5. **Combine with Vision API**: Use Vision API for older historical data and REST API for recent data.
6. **Parameter Order**: URL parameter order doesn't matter (confirmed through testing).
7. **Response Time Optimization**: Use the primary endpoint for time-sensitive operations.
8. **Cross-Market Data**: Be aware of format differences when using multiple market types.
9. **WebSocket for Real-time**: Use WebSockets instead of polling for real-time data.
10. **Batch Symbol Requests**: Request all ticker prices at once instead of individually.
11. **Use Domain-Specific URL Patterns**: Different market types (Spot, USDT-M Futures, COIN-M Futures, Options) use different URL patterns. Always use the correct pattern:

- Spot: `https://api.binance.com/api/v3/klines`
- USDT-M Futures: `https://fapi.binance.com/fapi/v1/klines`
- COIN-M Futures: `https://dapi.binance.com/dapi/v1/klines`
- Options: `https://eapi.binance.com/eapi/v1/klines`

### 6.1 Sample URL Construction Code

When working with multiple Binance market types, use a helper function to construct the correct URL:

```python
def construct_binance_url(market_type, symbol, interval):
    if market_type == "SPOT":
        return f"https://api.binance.com/api/v3/klines?symbol={symbol}&interval={interval}"
    elif market_type == "FUTURES_USDT":
        return f"https://fapi.binance.com/fapi/v1/klines?symbol={symbol}&interval={interval}"
    elif market_type == "FUTURES_COIN":
        return f"https://dapi.binance.com/dapi/v1/klines?symbol={symbol}&interval={interval}"
    elif market_type == "OPTIONS":
        return f"https://eapi.binance.com/eapi/v1/klines?symbol={symbol}&interval={interval}"
```
