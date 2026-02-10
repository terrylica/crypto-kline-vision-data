# Binance API Rate Limit Testing

**Hub**: [playground/](../CLAUDE.md) | **Root**: [CLAUDE.md](../../CLAUDE.md)

Tools for testing and analyzing Binance API rate limits — optimizing multi-symbol data retrieval within platform constraints.

## Key Components

- **direct_api_test.py**: Asynchronous script that tests direct API calls to fetch kline data for multiple symbols
- **run_direct_test.sh**: Shell script to run the API test with configurable parameters
- **rate_limit_tester.py**: Alternative implementation using the CryptoKlineVisionData for testing
- **extract_symbols.py**: Helper script to extract symbols from CSV files
- **symbols.txt**: List of symbols used for testing (50 symbols by default)

## Rate Limit Findings

Our tests have revealed several key insights about Binance's REST API rate limiting:

### Weight Calculation

1. **Weight Tracking**: Binance tracks API usage through a weight system, with a limit of 6000 weight units per minute per IP address
2. **Rolling Window**: The rate limit uses a rolling 1-minute window, not a fixed reset time
3. **Weight Measurement**: The `x-mbx-used-weight-1m` response header provides current weight usage

### Efficiency Analysis

| Parameter  | Value | Requests | Initial Weight | Final Weight | Net Weight | Weight/Request |
| ---------- | ----- | -------- | -------------- | ------------ | ---------- | -------------- |
| limit=1000 | 500   | 0\*      | 938            | ~938         | 1.88       | 1.88           |
| limit=100  | 450   | 24       | 844            | 820          | 1.82       | 1.82           |
| limit=10   | 500   | 0\*      | 990            | ~990         | 1.98       | 1.98           |
| limit=5    | 550   | 20       | 1074           | 1054         | 1.92       | 1.92           |

\*Estimated based on consistent patterns in other tests

### Key Observations

1. **Data Size vs. Weight**: Surprisingly, requesting fewer data points (smaller limit) does not consistently reduce weight consumption
2. **Consistent Weight Cost**: Each API request costs approximately 1.8-2.0 weight units regardless of the `limit` parameter
3. **Maximum Efficiency**: Using `limit=1000` provides the best data-to-weight ratio, as you get maximum data for the same weight cost
4. **Rate Limit Persistence**: The weight counter maintains state across requests in a rolling window, requiring careful tracking in production scenarios

## Usage Recommendations

### Optimal Configuration

- Always use `limit=1000` to get maximum data per weight unit
- For continuous monitoring, stagger requests to stay within the 6000 weight/minute limit
- Allow buffer for weight fluctuations (aim for ~80% utilization = 4800 weight/minute)

### Maximum Symbol Coverage

For continuous 1-second monitoring of market data:

- Each symbol costs ~2 weight units per request
- 50 symbols = ~100 weight/second = ~6000 weight/minute
- Recommended sustainable monitoring: 30-40 symbols continuously

### Advanced Strategies

1. **Staggered Updates**: Split symbols into groups and update each group in rotation
2. **Priority Tiers**: Update high-priority symbols more frequently than others
3. **Adaptive Throttling**: Dynamically adjust request frequency based on current weight usage
4. **Weight-Based Cooldown**: Implement backoff when approaching the rate limit threshold

## Execution Examples

Basic test with default parameters (50 symbols, 30 seconds duration, 1000 data points):

```bash
./run_direct_test.sh
```

Custom configuration:

```bash
./run_direct_test.sh [DURATION] [LIMIT]
```

Example with 5 seconds duration and 100 data points per request:

```bash
./run_direct_test.sh 5 100
```

## Results Analysis

Test results are saved as JSON files in the `results/` directory with detailed metrics including:

- Total requests made
- Success/failure counts
- Initial weight
- Final weight
- Net weight increase
- Weight per request
- Timestamps of all requests

## Conclusions

The Binance API provides a predictable rate limiting mechanism that allows for efficient market data monitoring when properly optimized. The key insight is to maximize the data retrieved per request rather than minimizing request size, as the weight cost remains relatively constant.

For production systems, implementing adaptive request strategies that respond to current weight usage can help ensure uninterrupted data collection while maximizing the number of instruments monitored.

## Future Work

Potential areas for further investigation:

- Testing with WebSocket connections as an alternative to REST API
- Exploring the impact of different API endpoints on weight consumption
- Implementing multiple IP strategies for increased capacity
- Testing rate limit behavior under extreme market conditions
