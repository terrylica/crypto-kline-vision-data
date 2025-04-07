# Timeout Handling Example: Practical Implementation

This document provides a practical example of the timeout handling architecture in action, demonstrating how it ensures reliable operation in real-world scenarios.

## Practical Example: REST Client Timeout Handling

Below is a simplified version of the timeout handling test that verifies the proper functioning of the timeout architecture:

```python
@pytest.mark.asyncio
async def test_timeout_handling():
    """Test that timeout is properly handled and logged."""
    # Setup temporary log file for timeout
    temp_log_file = os.path.join("logs", "timeout_incidents", "test_timeout.log")
    set_timeout_log_file(temp_log_file)

    # Clean up the log file if it exists
    if os.path.exists(temp_log_file):
        os.remove(temp_log_file)

    # Set up a very short timeout to trigger a timeout error
    async with RestDataClient(
        market_type=MarketType.SPOT,
        max_concurrent=1,  # Limit concurrency to make timeout more likely
        retry_count=0,     # No retries to make the test faster
        fetch_timeout=0.1  # Very short timeout to ensure it triggers
    ) as client:
        # Use a date range big enough to require multiple chunks
        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(days=30)  # 30 days of data should be too much to fetch in 0.1s

        # Fetch data that should timeout
        df, stats = await client.fetch(
            symbol="BTCUSDT",
            interval=Interval.MINUTE_1,
            start_time=start_time,
            end_time=end_time
        )

        # Verify we got an empty dataframe due to timeout
        assert df.empty, "DataFrame should be empty when timeout occurs"

    # Wait a moment for log to be written
    await asyncio.sleep(0.5)

    # Verify that the timeout was logged
    assert os.path.exists(temp_log_file), "Timeout log file should exist"

    # Read the log file to verify it contains timeout information
    with open(temp_log_file, "r") as f:
        log_content = f.read()

    assert "TIMEOUT" in log_content, "Log should contain TIMEOUT message"
    assert "REST API fetch" in log_content, "Log should mention the REST API fetch operation"
```

## Examining the Timeout Log

When a timeout occurs, the system generates a detailed log entry like this:

```
2025-04-06 21:15:28,342 [TIMEOUT] Operation 'REST API fetch for BTCUSDT 1m' timed out after 0.1s
Details: {
  "symbol": "BTCUSDT",
  "interval": "1m",
  "market_type": "SPOT",
  "start_time": "2025-03-07 21:15:28.342120+00:00",
  "end_time": "2025-04-06 21:15:28.342120+00:00",
  "chunks": 5,
  "elapsed": "0.10s",
  "completed_chunks": 0
}
```

This log entry provides:

1. **Timestamp**: When the timeout occurred
2. **Operation**: The specific operation that timed out
3. **Timeout Value**: The timeout setting that was triggered
4. **Detailed Context**:
   - Symbol and interval being fetched
   - Market type
   - Time range requested
   - Number of chunks involved
   - How much time elapsed before timeout
   - How many chunks were successfully completed

## Data Client Implementation

The actual timeout handling in the REST client is implemented through the following mechanism:

```python
async def fetch(self, symbol, interval, start_time, end_time):
    # Calculate chunks needed
    chunks = self._calculate_chunks(start_ms, end_ms, interval)

    # Track time for performance analysis
    t_start = time.time()

    # Set up timeout for the overall fetch operation
    effective_timeout = min(MAX_TIMEOUT, self.fetch_timeout * 2)

    # Create a task for the chunked fetch operation
    all_chunks_task = asyncio.create_task(
        self._fetch_all_chunks(symbol, interval, chunks, stats)
    )

    try:
        # Wait for the task with timeout
        results = await asyncio.wait_for(all_chunks_task, timeout=effective_timeout)

        # Calculate the actual time taken
        t_end = time.time()
        fetch_time = t_end - t_start

        # Process the results into a DataFrame
        if results and len(results) > 0:
            # Process data and return DataFrame
            return final_df, stats

        # Return empty DataFrame if no results
        return self.create_empty_dataframe(), stats

    except asyncio.TimeoutError:
        # Calculate elapsed time
        elapsed = time.time() - t_start

        # Log timeout to both console and dedicated log file
        logger.log_timeout(
            operation=f"REST API fetch for {symbol} {interval.value}",
            timeout_value=effective_timeout,
            details={
                "symbol": symbol,
                "interval": interval.value,
                "market_type": self.market_type.name,
                "start_time": str(start_time),
                "end_time": str(end_time),
                "chunks": len(chunks),
                "elapsed": f"{elapsed:.2f}s",
                "completed_chunks": stats.get("completed_chunks", 0)
            }
        )

        # Cancel the task and clean up resources
        if not all_chunks_task.done():
            all_chunks_task.cancel()
            await self._cleanup_force_timeout_tasks()

        # Return empty DataFrame as fallback
        return self.create_empty_dataframe(), stats
```

## Real-World Benefits

This timeout handling approach provides several important benefits in real-world scenarios:

1. **Preventing Hanging**: Without proper timeout handling, network operations could block indefinitely, causing the application to appear frozen.

2. **Resource Management**: The explicit task tracking and cleanup ensures no resources are leaked when timeouts occur.

3. **Fallback Behavior**: By returning an empty DataFrame with the correct structure, the system ensures downstream code can continue to function without having to implement special case handling for timeouts.

4. **Observability**: The detailed timeout logs provide crucial information for debugging and optimizing system performance.

5. **Consistency**: By applying the same timeout handling pattern to all data clients, the system ensures consistent behavior and makes the code more maintainable.

## Conclusion

The timeout handling implementation demonstrates a robust approach to managing network operations in an asynchronous environment. By combining centralized configuration, detailed logging, proper task management, and resource cleanup, the system ensures reliable operation even in challenging network conditions.
