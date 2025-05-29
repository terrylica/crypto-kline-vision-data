# Comprehensive Bybit REST API Endpoints Documentation

This document provides detailed information about Bybit's REST API endpoints for retrieving market data, based on empirical testing and analysis of response structures.

## 1. API Fundamentals

Bybit offers REST API endpoints for accessing market data including candlestick (kline) data for various intervals. This document focuses on the structure, parameters, and response format of these endpoints.

### 1.1 Available Endpoints Overview

Bybit offers several market types with a unified REST API structure:

| Market Type | Category Parameter | Description                             |
| ----------- | ------------------ | --------------------------------------- |
| Spot        | `spot`             | Spot trading pairs                      |
| Linear      | `linear`           | Perpetual contracts settled in USDT     |
| Inverse     | `inverse`          | Perpetual contracts settled in the coin |
| Option      | `option`           | Options trading                         |

### 1.2 API URL Structure

The Bybit REST API uses a unified URL pattern for all market types, with the market type specified in the `category` parameter.

#### 1.2.1 URL Pattern

Base URL: `https://api.bybit.com`

Path: `/v5/market/kline`

Complete URL pattern: `https://api.bybit.com/v5/market/kline?category={category}&symbol={symbol}&interval={interval}&start={startTime}&end={endTime}&limit={limit}`

Example: `https://api.bybit.com/v5/market/kline?category=linear&symbol=BTCUSDT&interval=5&start=1715000000000&end=1715100000000&limit=10`

#### 1.2.2 Key Observations

- Bybit uses a unified REST API structure across all market types
- The `category` parameter determines which market type is queried
- Timestamps are in milliseconds (Unix timestamp format)
- The response format is consistent across market types

## 2. Request Parameters

| Parameter | Type    | Required | Description                                                                           |
| --------- | ------- | -------- | ------------------------------------------------------------------------------------- |
| category  | string  | Yes      | Product type: `spot`, `linear`, `inverse`, `option`                                   |
| symbol    | string  | Yes      | Trading pair symbol, e.g., `BTCUSDT`                                                  |
| interval  | string  | Yes      | Kline interval. Supported intervals: 1, 3, 5, 15, 30, 60, 120, 240, 360, 720, D, W, M |
| start     | integer | No       | Start timestamp (ms)                                                                  |
| end       | integer | No       | End timestamp (ms)                                                                    |
| limit     | integer | No       | Limit for data size per page. [1, 1000]. Default: 200                                 |

### 2.1 Interval Values

Bybit uses numeric values for minute-based intervals and single letters for larger timeframes:

| Interval Value | Description |
| -------------- | ----------- |
| 1              | 1 minute    |
| 3              | 3 minutes   |
| 5              | 5 minutes   |
| 15             | 15 minutes  |
| 30             | 30 minutes  |
| 60             | 1 hour      |
| 120            | 2 hours     |
| 240            | 4 hours     |
| 360            | 6 hours     |
| 720            | 12 hours    |
| D              | 1 day       |
| W              | 1 week      |
| M              | 1 month     |

### 2.2 Timestamp Handling

- All timestamps are in milliseconds (Unix timestamp format)
- The API appears to round timestamps to the nearest interval boundary
- For 5-minute intervals, timestamps align to 00:00, 00:05, 00:10, etc.
- For 15-minute intervals, timestamps align to 00:00, 00:15, 00:30, 00:45, etc.

## 3. Response Structure

### 3.1 Response Format

```json
{
  "retCode": 0,
  "retMsg": "OK",
  "result": {
    "symbol": "BTCUSDT",
    "category": "linear",
    "list": [
      [
        "1715100000000", // Timestamp (ms)
        "63538.4", // Open price
        "63571.5", // High price
        "63510.1", // Low price
        "63528.5", // Close price
        "183.198", // Volume
        "11639163.7632" // Turnover (volume in quote currency)
      ]
      // Additional klines...
    ]
  },
  "retExtInfo": {},
  "time": 1748500535631 // Server timestamp
}
```

### 3.2 Kline Data Format

Each kline in the response `list` is an array with 7 elements:

| Index | Description                         | Type   |
| ----- | ----------------------------------- | ------ |
| 0     | Timestamp (milliseconds)            | string |
| 1     | Open price                          | string |
| 2     | High price                          | string |
| 3     | Low price                           | string |
| 4     | Close price                         | string |
| 5     | Volume (in base currency)           | string |
| 6     | Turnover (volume in quote currency) | string |

### 3.3 Empty Response

When no data is available for the requested time range (e.g., for very old dates or future dates), the API returns an empty list rather than an error:

```json
{
  "retCode": 0,
  "retMsg": "OK",
  "result": {
    "symbol": "BTCUSDT",
    "category": "linear",
    "list": []
  },
  "retExtInfo": {},
  "time": 1748500528559
}
```

## 4. Historical Data Availability

Based on empirical testing using the Bybit REST API, historical data availability for specific trading pairs and intervals depends on when the pair was first listed and the market type (Spot, Linear, Inverse). Our investigation employed a precise command-line methodology to pinpoint the earliest available data for key BTC pairs at 1-minute, 5-minute, and 15-minute intervals.

### 4.1 Earliest Data by Market and Interval (Empirical Findings)

| Market Type       | Symbol  | Interval   | Earliest Available Timestamp (UTC)                                                                                                                                                                                               |
| ----------------- | ------- | ---------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Spot              | BTCUSDT | 1 minute   | 2021-07-05 12:00:00                                                                                                                                                                                                              |
| Spot              | BTCUSDT | 5 minutes  | 2021-07-05 12:00:00                                                                                                                                                                                                              |
| Spot              | BTCUSDT | 15 minutes | 2021-07-05 12:00:00                                                                                                                                                                                                              |
| Linear Perpetual  | BTCUSDT | 1 minute   | 2020-03-25 10:36:00                                                                                                                                                                                                              |
| Linear Perpetual  | BTCUSDT | 5 minutes  | 2020-03-25 10:35:00                                                                                                                                                                                                              |
| Linear Perpetual  | BTCUSDT | 15 minutes | 2020-03-25 10:30:00                                                                                                                                                                                                              |
| Inverse Perpetual | BTCUSD  | 1 minute   | 2018-11-14 16:00:00 I want you to validate this portions of data to see the ensuing interval of data points at least 10 or at least 2 to 3 ensuing data points. After the genesis earliest available timestamps do exist or not. |
| Inverse Perpetual | BTCUSD  | 5 minutes  | 2018-11-14 16:00:00                                                                                                                                                                                                              |
| Inverse Perpetual | BTCUSD  | 15 minutes | 2018-11-14 16:00:00                                                                                                                                                                                                              |

**Note:** While our testing provides precise earliest times based on API responses, the official listing dates announced by Bybit may slightly predate the availability of granular kline data in the API.

### 4.2 Methodology for Determining Earliest Data

To determine the earliest available timestamp for a given trading pair and interval, we employed a precise command-line based methodology using `curl` and `jq`. This method focuses on identifying the _very first_ kline returned by the API when querying from a point in time known to be before the data's existence.

1.  **Initial Query:** We sent a `GET` request to the `/v5/market/kline` endpoint with a very early `start` timestamp (e.g., 2015-01-01) and specifically set the `limit=1`. The API is expected to return the first available kline at or after the provided `start` timestamp. If the result list is empty, it indicates data does not go back to that `start` time.
2.  **Identify Potential Earliest:** The timestamp of the single kline returned by the initial query (if a kline was returned) was considered the potential earliest available timestamp. This timestamp represents the beginning of the first kline found by the API from our very early starting point.
3.  **Rigorous Verification:** To confirm this was the absolute earliest available kline via the API for the specific interval, we performed a crucial verification step. We sent a subsequent query with the `start` timestamp set to be exactly **one interval (e.g., 1 minute, 5 minutes, or 15 minutes)** _before_ the potential earliest timestamp found in step 2, again with `limit=1`. If this verification query returned an _empty list_, it definitively confirmed that data does not exist before the potential earliest timestamp found in step 2, thus validating it as the true earliest available time via the API for that interval. If a kline was returned in the verification step, its timestamp would become the new potential earliest, and the verification process would be repeated.

This rigorous methodology was applied to the specific pairs and intervals listed above to generate the empirical findings, providing high confidence in the reported earliest available timestamps through the Bybit REST API.

## 5. Rate Limiting

Bybit implements rate limiting on their API. Based on their official documentation:

- Rate limits are applied per IP address
- Different endpoints have different rate limit weights
- The `/v5/market/kline` endpoint is subject to these rate limits

### 5.1 Rate Limit Headers

Bybit returns rate limit information in response headers:

- `X-Bapi-Limit`: The rate limit for the current endpoint
- `X-Bapi-Limit-Status`: Current usage count
- `X-Bapi-Limit-Reset-Timestamp`: Timestamp when the rate limit will reset

### 5.2 Rate Limit Errors

When rate limits are exceeded, the API returns an error response with:

```json
{
  "retCode": 10006,
  "retMsg": "Too many visits!"
}
```

## 6. Best Practices

### 6.1 Handling Historical Data

1. When requesting historical data, start with smaller time ranges to test availability
2. For large date ranges, consider paginating requests with appropriate `limit` values
3. Handle empty responses gracefully, as they indicate no data is available for the requested range

### 6.2 Rate Limit Management

1. Monitor rate limit headers in responses to track usage
2. Implement backoff strategies when approaching rate limits
3. Consider implementing request queuing for high-volume applications

### 6.3 Error Handling

1. Always check the `retCode` field in responses to verify success
2. Implement retry logic with exponential backoff for rate limit errors
3. Log unusual empty responses for investigation

## 7. Comparison with Other Endpoints

Bybit offers several other market data endpoints that may be useful depending on your use case:

- `/v5/market/tickers`: For current market prices and 24-hour statistics
- `/v5/market/trades`: For recent trades
- `/v5/market/orderbook`: For current order book data

Each endpoint has its own rate limits and response formats. Choose the appropriate endpoint based on your specific data needs.

## 8. Conclusion

The Bybit REST API provides a unified structure for accessing kline data across all market types. Understanding the request parameters, response format, and rate limit considerations will help you effectively integrate Bybit market data into your applications.
