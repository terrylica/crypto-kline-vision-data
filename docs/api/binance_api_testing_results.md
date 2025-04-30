# Binance API Testing Results

This document summarizes the empirical testing conducted on various Binance API endpoints to provide insights into the actual behavior, limits, and characteristics of the API.

## 1. Historical Data Availability Testing

### 1.1 Spot Market Klines

| Symbol    | Earliest Available Data | Notes                                    |
| --------- | ----------------------- | ---------------------------------------- |
| BTCUSDT   | August 2017             | Complete data from when Binance launched |
| ETHBTC    | August 2017             | Complete data from launch                |
| BTCUPUSDT | Limited history         | Leveraged tokens have shorter history    |

**Testing Notes:**

- Empty arrays are returned when requesting data from before a symbol started trading rather than errors
- Data availability correlates with when the symbol began trading on Binance
- Major pairs like BTCUSDT have comprehensive data going back 5+ years
- Attempting to request data from before Binance's launch returns empty arrays
- The REST API and Vision API provide the same historical coverage

### 1.2 Futures Market Klines

**USDT-M Futures:**

- Limited historical data compared to spot markets
- BTCUSDT futures data available from approximately September 2019

**COIN-M Futures:**

- BTCUSD_PERP has more limited historical data
- Format differences in the symbol representation (BTCUSD_PERP vs BTCUSDT)

## 2. Interval Testing

### 2.1 Supported Intervals by Market Type

| Interval | Spot Market  | USDT-M Futures   | COIN-M Futures   |
| -------- | ------------ | ---------------- | ---------------- |
| 1s       | ✅ Available | ❌ Not available | ❌ Not available |
| 1m       | ✅ Available | ✅ Available     | ✅ Available     |
| 3m       | ✅ Available | ✅ Available     | ✅ Available     |
| 5m       | ✅ Available | ✅ Available     | ✅ Available     |
| 15m      | ✅ Available | ✅ Available     | ✅ Available     |
| 30m      | ✅ Available | ✅ Available     | ✅ Available     |
| 1h       | ✅ Available | ✅ Available     | ✅ Available     |
| 2h       | ✅ Available | ✅ Available     | ✅ Available     |
| 4h       | ✅ Available | ✅ Available     | ✅ Available     |
| 6h       | ✅ Available | ✅ Available     | ✅ Available     |
| 8h       | ✅ Available | ✅ Available     | ✅ Available     |
| 12h      | ✅ Available | ✅ Available     | ✅ Available     |
| 1d       | ✅ Available | ✅ Available     | ✅ Available     |
| 3d       | ✅ Available | ✅ Available     | ✅ Available     |
| 1w       | ✅ Available | ✅ Available     | ✅ Available     |
| 1M       | ✅ Available | ✅ Available     | ✅ Available     |

**Testing Notes:**

- 1-second interval is exclusive to spot markets
- All other intervals are consistent across market types
- Invalid interval errors are returned with consistent error codes (-1120)

## 3. Rate Limit Testing

This section summarizes the observed weight consumption for various endpoints during testing. For a comprehensive guide on Binance API rate limits, including rate limit types, tracking headers, error responses, and optimization strategies, please refer to [Binance API Rate Limits](binance_rate_limits.md).

### 3.1 Weight Consumption by Endpoint

| Endpoint                 | Weight (Single Symbol) | Weight (All Symbols) | Notes                              |
| ------------------------ | ---------------------- | -------------------- | ---------------------------------- |
| /api/v3/klines           | 2                      | N/A                  | Consistent across all requests     |
| /api/v3/ticker/price     | 2                      | 4                    | Significant savings in batch       |
| /api/v3/ticker/24hr      | 1                      | 40                   | Higher weight for all symbols      |
| /api/v3/trades           | 5                      | N/A                  | Higher weight than basic endpoints |
| /api/v3/depth            | 5 (small limit)        | N/A                  | Varies based on depth requested    |
| /api/v3/historicalTrades | 5                      | N/A                  | Same as regular trades             |

### 3.2 Rate Limit Headers

Headers consistently returned in responses:

- `x-mbx-used-weight-1m`: Weight used in the last minute
- `x-mbx-used-weight`: Legacy header showing weight used

**Testing Notes:**

- Default limit appears to be 6000 weight per minute (confirmed via API responses)
- Weight costs are consistent across multiple test runs
- The 429 error is returned when rate limits are exceeded
- The Retry-After header in 429 responses indicates how long to wait

## 4. Response Time Analysis

### 4.1 Endpoint Performance Comparison

| Endpoint                           | Average Response Time | Notes                        |
| ---------------------------------- | --------------------- | ---------------------------- |
| api.binance.com                    | ~0.14 seconds         | Primary endpoint (fastest)   |
| api1.binance.com                   | ~0.31 seconds         | First backup endpoint        |
| api2.binance.com, api3.binance.com | ~0.44 seconds         | Other backup endpoints       |
| data-api.binance.vision            | ~0.53 seconds         | Data-only endpoint (slowest) |

**Testing Notes:**

- Response times are consistently faster on the primary endpoint
- Performance decreases slightly during high market volatility periods
- Backup endpoints show higher latency but similar reliability
- API performance is consistent across different symbols
- Data-only endpoint has higher latency but may have different rate limits

## 5. Error Handling Testing

### 5.1 Common Error Responses

| Error Code | Message                                                        | Trigger Condition                     |
| ---------- | -------------------------------------------------------------- | ------------------------------------- |
| -1120      | "Invalid interval."                                            | Using an unsupported interval         |
| -1121      | "Invalid symbol."                                              | Using a non-existent symbol           |
| -1102      | "Mandatory parameter 'symbol' was not sent, was empty/null..." | Missing required parameter            |
| -1104      | "Not all sent parameters were read; read '3' parameter(s)..."  | Using undocumented/invalid parameters |
| -1429      | "Too many requests; current limit is 6000 request weight..."   | Exceeding rate limits                 |

**Testing Notes:**

- Error responses are consistent and provide useful information
- Parameters that are not documented (e.g., 'fromId' and 'endId') are properly rejected
- Error messages include the weights and limits in rate limit errors
- Parameter order in requests does not affect the response or error handling

## 6. Cross-Market Data Consistency

### 6.1 Price Comparison Across Markets

| Market Type    | Symbol      | Example Price    | Format             | Precision        |
| -------------- | ----------- | ---------------- | ------------------ | ---------------- |
| Spot           | BTCUSDT     | "84392.30000000" | 8 decimal places   | Higher precision |
| USDT-M Futures | BTCUSDT     | "84343.40"       | 1-2 decimal places | Medium precision |
| COIN-M Futures | BTCUSD_PERP | "84313.7"        | 1 decimal place    | Lower precision  |

**Testing Notes:**

- Price differences are typically within 0.1-0.5% across markets
- Spot prices and USDT-M futures typically track closely
- COIN-M futures may show inverted price representation
- Response formats differ slightly between market types
- Decimal precision varies by market type

### 6.2 Leveraged Token Testing

| Token     | Example Price | Notes                                                            |
| --------- | ------------- | ---------------------------------------------------------------- |
| BTCUPUSDT | "16.60000000" | Leveraged token price is significantly different from underlying |

**Testing Notes:**

- Leveraged tokens follow the same API structure but have different price scales
- Data availability is more limited compared to major pairs
- APIs function identically for leveraged tokens

## 7. Ticker Comparison Testing

### 7.1 Different Ticker Endpoints

| Endpoint     | Example Response (BTCUSDT)                      | Notes                              |
| ------------ | ----------------------------------------------- | ---------------------------------- |
| Price Ticker | `{"symbol":"BTCUSDT","price":"84443.58000000"}` | Simple price only                  |
| 24hr Ticker  | Extensive stats including volume, high/low      | Much more detailed information     |
| Book Ticker  | Bid/ask prices and quantities                   | Order book top of book information |

**Testing Notes:**

- Price ticker returns minimal data with lower weight cost
- 24hr ticker provides extensive statistics but higher weight
- Book ticker shows current bid/ask spread
- Price consistency was observed across different ticker endpoints
- Getting all symbols at once is more efficient for price ticker

## 8. Parameter Testing

### 8.1 Parameter Behavior

| Test Case               | Result                    | Notes                                  |
| ----------------------- | ------------------------- | -------------------------------------- |
| Parameter ordering      | No impact on response     | Order of URL parameters doesn't matter |
| Undocumented parameters | Error code -1104          | Properly rejected with clear message   |
| Missing required params | Error code -1102          | Clear error message on missing params  |
| Exceeding limit (>1000) | Returns 1000 records only | Server enforces maximum limit          |

**Testing Notes:**

- URL parameter order has no effect on the response (confirmed through multiple tests)
- The API properly validates parameters and provides informative error messages
- When requesting more than 1000 records, the API silently caps the response at 1000
- The API maintains backward compatibility with older parameter formats

## 9. WebSocket Testing

WebSockets provide significantly lower latency for real-time data compared to REST API polling:

**WebSocket URLs:**

- `wss://stream.binance.com:9443/ws/<symbol>@kline_<interval>`
- Example: `wss://stream.binance.com:9443/ws/btcusdt@kline_1m`

**Advantages over REST polling:**

- Lower latency (real-time updates)
- No rate limits for public market data streams
- Reduced server load and bandwidth usage
- Updates only when data changes

## 10. Future Testing Considerations

Areas for additional testing:

1. Authentication and private API endpoints
2. WebSocket vs REST data consistency
3. More detailed futures market specifics
4. Options market API testing
5. Response delay during high volatility periods
6. Timestamp precision analysis
7. Public vs authenticated rate limit differences
8. Exact timeframe when rate limit counters reset

## 11. Testing Methodology

All tests were conducted using standard HTTP requests to the various Binance API endpoints with systematically varying parameters. Results were documented with timestamps, response times, and status codes to ensure reproducibility.
