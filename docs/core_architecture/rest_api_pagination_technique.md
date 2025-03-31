# REST API Time-Based Chunking Pagination

This document explains the time-based chunking pagination technique implemented in the `RestDataClient` class to efficiently retrieve large datasets from the Binance REST API while respecting API limits and optimizing for different interval types.

## Overview

The Binance REST API imposes a limit of 1000 records per request for kline (candlestick) data. To retrieve larger datasets spanning extended time periods, the `RestDataClient` implements a sophisticated time-based chunking strategy that:

1. Divides large time ranges into optimally sized chunks based on interval type
2. Executes concurrent requests for all chunks with controlled parallelism
3. Handles rate limiting with endpoint rotation and exponential backoff
4. Aggregates results from all chunks into a single cohesive DataFrame

## Key Components

### 1. Time Boundary Alignment

The Binance REST API applies specific boundary handling to timestamps:

- **startTime**: Rounds UP to the next interval boundary if not exactly on a boundary
- **endTime**: Rounds DOWN to the previous interval boundary if not exactly on a boundary

To ensure accurate chunking, `RestDataClient` implements the `_align_interval_boundaries` method:

```python
def _align_interval_boundaries(
    self, start_time: datetime, end_time: datetime, interval: Interval
) -> Tuple[datetime, datetime]:
    """Align time boundaries according to Binance REST API behavior."""
    # Get interval in seconds
    interval_seconds = interval.to_seconds()

    # Extract seconds since epoch for calculations
    start_seconds = start_time.timestamp()
    end_seconds = end_time.timestamp()

    # Calculate floor of each timestamp to interval boundary
    start_floor = int(start_seconds) - (int(start_seconds) % interval_seconds)
    end_floor = int(end_seconds) - (int(end_seconds) % interval_seconds)

    # Apply Binance API boundary rules
    if start_seconds != start_floor:
        aligned_start = datetime.fromtimestamp(
            start_floor + interval_seconds, tz=timezone.utc
        )
    else:
        aligned_start = datetime.fromtimestamp(start_floor, tz=timezone.utc)

    aligned_end = datetime.fromtimestamp(end_floor, tz=timezone.utc)

    return aligned_start, aligned_end
```

### 2. Interval-Aware Chunk Calculation

The most critical component is the `_calculate_chunks` method, which divides time ranges into optimal chunks based on interval type:

```python
def _calculate_chunks(
    self, start_ms: int, end_ms: int, interval: Interval
) -> List[Tuple[int, int]]:
    """Calculate chunk ranges based on start and end times."""
    chunks = []
    current_start = start_ms

    # Get interval duration in milliseconds
    interval_ms = interval.to_seconds() * 1000

    # Calculate records per chunk - API max is 1000
    records_per_chunk = self.CHUNK_SIZE  # default 1000

    # Calculate optimal chunk duration based on interval type
    if interval == Interval.SECOND_1:
        # For 1s: max ~16 minutes per chunk (1000 records)
        chunk_ms = min(records_per_chunk * interval_ms, 1000 * 1000)

    elif interval == Interval.MINUTE_1:
        # For 1m: max ~16 hours per chunk (1000 records)
        chunk_ms = min(records_per_chunk * interval_ms, 1000 * 60 * 1000)

    elif interval in (Interval.MINUTE_3, Interval.MINUTE_5, Interval.MINUTE_15, Interval.MINUTE_30):
        # For other minute intervals: cap at 7 days per chunk
        chunk_ms = min(records_per_chunk * interval_ms, 7 * 24 * 60 * 60 * 1000)

    elif interval in (Interval.HOUR_1, Interval.HOUR_2, Interval.HOUR_4, Interval.HOUR_6, Interval.HOUR_8, Interval.HOUR_12):
        # For hour intervals: cap at 30 days per chunk
        chunk_ms = min(records_per_chunk * interval_ms, 30 * 24 * 60 * 60 * 1000)

    else:
        # For day/week/month intervals: use full chunk capacity
        chunk_ms = records_per_chunk * interval_ms

    # Process chunks with proper boundary alignment
    while current_start < end_ms:
        chunk_end = min(current_start + chunk_ms, end_ms)
        chunks.append((current_start, chunk_end))
        current_start = chunk_end + 1

    return chunks
```

This approach ensures:

- Smaller intervals (1s, 1m) use appropriately sized chunks to avoid overwhelming the API
- Larger intervals use larger chunks to minimize API calls
- Each chunk respects the 1000 record API limit

### 3. Concurrent Execution with Controlled Parallelism

The `fetch` method orchestrates concurrent requests for all chunks:

```python
# Get optimal concurrency value based on hardware
optimal_concurrency_result = self.hw_monitor.calculate_optimal_concurrency()
optimal_concurrency = optimal_concurrency_result["optimal_concurrency"]

# Limit semaphore to optimal concurrency
sem = asyncio.Semaphore(optimal_concurrency)

# Create tasks for all chunks
tasks = []
for chunk_start, chunk_end in chunks:
    task = asyncio.create_task(
        self._fetch_chunk_with_semaphore(
            symbol, interval, chunk_start, chunk_end, sem
        )
    )
    tasks.append(task)

# Wait for all tasks to complete
results = await asyncio.gather(*tasks, return_exceptions=True)
```

Key features:

- Uses `HardwareMonitor` to determine optimal concurrency based on system capabilities
- Uses a semaphore to control the maximum number of concurrent requests
- Runs all chunk requests concurrently using `asyncio`
- Handles exceptions gracefully with `return_exceptions=True`

### 4. Robust Error Handling and Endpoint Rotation

The `_fetch_chunk_with_semaphore` method implements robust error handling:

```python
async def _fetch_chunk_with_semaphore(
    self,
    symbol: str,
    interval: Interval,
    chunk_start: int,
    chunk_end: int,
    semaphore: asyncio.Semaphore,
    retry_count: int = 0,
) -> Tuple[List[List[Any]], str]:
    """Fetch a chunk of klines data with retry logic and semaphore control."""
    async with semaphore:
        try:
            # Make API request
            response = await self._client.get(endpoint, params=params)

            # Handle rate limiting
            if response.status_code in (418, 429):
                retry_after = int(response.headers.get("Retry-After", 1))
                await asyncio.sleep(retry_after)
                # Try with a different endpoint
                async with self._endpoint_lock:
                    self._endpoint_index = (self._endpoint_index + 1) % len(
                        self._endpoints
                    )
                return await self._fetch_chunk_with_semaphore(
                    symbol, interval, chunk_start, chunk_end, semaphore, retry_count
                )

            # Process and return data
            return data, endpoint

        except Exception as e:
            if retry_count >= self._retry_count:
                raise

            # Increment retry counter and wait with exponential backoff
            retry_count += 1
            wait_time = min(2**retry_count, 60)  # Cap at 60 seconds
            await asyncio.sleep(wait_time)

            # Try with a different endpoint
            async with self._endpoint_lock:
                self._endpoint_index = (self._endpoint_index + 1) % len(
                    self._endpoints
                )

            # Retry
            return await self._fetch_chunk_with_semaphore(
                symbol, interval, chunk_start, chunk_end, semaphore, retry_count
            )
```

Key features:

- Rotates between multiple API endpoints to distribute load and avoid rate limiting
- Implements exponential backoff for retries (wait time doubles with each retry)
- Honors `Retry-After` headers from the API for rate limit responses
- Caps maximum retries to avoid infinite loops

## Key Advantages of This Approach

1. **Interval-Optimized Chunking**

   - Different intervals use different chunking strategies optimized for their size
   - 1-second data uses small chunks (max ~16 minutes / 1000 records)
   - 1-minute data uses medium chunks (max ~16 hours / 1000 records)
   - Hour-based intervals use larger chunks (max 30 days)
   - Day/week/month intervals use full chunk capacity

2. **Parallel Processing with Resource Optimization**

   - Chunks are processed concurrently for maximum throughput
   - Concurrency is limited based on system capabilities (CPU, memory, network)
   - Parallelism is controlled via semaphores to prevent overwhelming the system
   - Endpoint rotation spreads load across multiple Binance endpoints

3. **Robust Error Handling**

   - Exponential backoff for retries with configurable maximum retry count
   - API-driven rate limit handling via `Retry-After` headers
   - Exceptions are captured and reported clearly
   - Failed chunks are tracked separately from successful ones

4. **Accurate Time Boundary Handling**
   - Implements the exact same boundary alignment behavior as the Binance API
   - Ensures consistent data retrieval regardless of timestamp precision
   - Provides clear logging of boundary adjustments for transparency

## Comparative Advantages Over Traditional Pagination

Unlike traditional offset/limit pagination used in many APIs, time-based chunking offers several advantages for time series data:

1. **Natural Time Boundaries**

   - Chunks align with natural time boundaries for the interval
   - No overlapping or missing data between chunks
   - Clean aggregation of results without duplicates

2. **Parallel Retrieval**

   - Independent time chunks can be retrieved concurrently
   - No need to wait for previous pages before requesting next ones
   - Dramatically reduces total time to retrieve large datasets

3. **Resilience to Data Changes**

   - Time-based chunks are stable even if data is added/removed during pagination
   - Traditional offset/limit pagination can miss or duplicate records if data changes

4. **Efficiency with Sparse Data**
   - Time chunks with no data return quickly rather than requiring many empty page requests
   - Naturally handles periods with varying data density

## Example Usage

```python
# Create client
client = RestDataClient(market_type=MarketType.SPOT)

# Define time range
start_time = datetime(2023, 1, 1, tzinfo=timezone.utc)
end_time = datetime(2023, 1, 31, tzinfo=timezone.utc)

# Fetch data with automatic chunking
async with client:
    df, stats = await client.fetch(
        symbol="BTCUSDT",
        interval=Interval.HOUR_1,
        start_time=start_time,
        end_time=end_time,
    )

print(f"Retrieved {len(df)} records in {stats['chunks_processed']} chunks")
```

The client automatically:

1. Aligns time boundaries to match Binance API behavior
2. Calculates optimal chunks based on the 1-hour interval
3. Executes concurrent requests for all chunks
4. Combines results into a single DataFrame

## Conclusion

The time-based chunking pagination technique implemented in `RestDataClient` provides an efficient, robust, and scalable approach to retrieving large time series datasets from the Binance REST API. By optimizing chunk sizes based on interval type, executing requests concurrently with controlled parallelism, and implementing robust error handling with endpoint rotation, the client can reliably retrieve data spanning extensive time periods while respecting API limits and maximizing performance.
