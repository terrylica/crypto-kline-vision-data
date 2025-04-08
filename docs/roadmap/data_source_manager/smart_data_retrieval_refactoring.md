# Smart Data Retrieval Refactoring Plan

## Overview

This document outlines a plan to enhance the DataSourceManager to intelligently split data retrieval between multiple sources using a Failover Composition Priority (FCP) approach:

1. **Cache** (highest priority) - Local Arrow files
2. **Vision API** (for historical data, including 1s intervals in SPOT markets)
3. **REST API** (for recent data, or as fallback when Vision API fails)

The goal is to optimize data retrieval by automatically using the most appropriate source for each time segment without requiring explicit configuration by the user. Following the Liskov Substitution Principle (LSP), the final DataFrame output will maintain the same format and behavior regardless of which source(s) the data came from.

## Current Implementation Analysis

### Current Data Flow

1. `get_data()` calls `_get_data_impl()`
2. Cache is checked for the entire time range if enabled
3. A single source (Vision or REST) is selected for the entire time range via `_determine_data_source()`
4. Data is fetched from the selected source
5. If Vision API fails, it falls back to REST API for the entire range

### Current Source Selection Logic

The `_should_use_vision_api` method currently uses the following rules to decide whether to use Vision API:

1. Use REST API for small intervals like 1s that Vision doesn't support
   - Note: This is incorrect for SPOT markets, which DO support 1s intervals in Vision API
2. Use Vision API for large time ranges that would exceed REST API's chunk limits:

   ```python
   if data_points > self.REST_CHUNK_SIZE * self.REST_MAX_CHUNKS:
       # Use Vision API
   ```

3. Use Vision API for historical data older than the VISION_DATA_DELAY_HOURS threshold:

   ```python
   if end_time < vision_threshold:  # vision_threshold = now - VISION_DATA_DELAY_HOURS
       # Use Vision API
   ```

4. Default to REST API for recent data

The fallback mechanism (if Vision API fails, try REST API) is already implemented but only works after a full Vision API failure.

### Key Limitations

1. **Binary Source Selection**: Currently uses either Vision API or REST API for the entire range
2. **Fallback Only on Failure**: REST API is only used if Vision API fails
3. **No Time Range Splitting**: Doesn't split the query across sources for partial time ranges
4. **Redundant Vision API Threshold**: The historical data threshold rule isn't necessary since:
   - Vision API automatically falls back to REST API on failure
   - The chunk size constraint already handles large data ranges
   - The threshold creates an arbitrary boundary that can lead to inefficient data retrieval
5. **No Source Identification**: No metadata to track which data came from which source
6. **LSP Violation**: The current approach doesn't ensure that the final DataFrame maintains consistent behavior when data comes from multiple sources

## Refactoring Plan

### 1. Redesign Source Selection Logic

**Main Change**: Remove the historical data threshold rule and prioritize the Failover Composition Priority (FCP) concept.

1. New `_should_use_vision_api` implementation:

   ```python
   def _should_use_vision_api(
       self, start_time: datetime, end_time: datetime, interval: Interval
   ) -> bool:
       """Determine if Vision API should be used based on time range and interval."""
       # Use REST API for 1s intervals in non-SPOT markets (Vision API doesn't support it)
       if interval.name == Interval.SECOND_1.name and self.market_type != MarketType.SPOT:
           logger.debug("Using REST API for 1s data in non-SPOT markets (Vision API doesn't support it)")
           return False

       # Always prefer Vision API for all data retrievals regardless of size
       # REST API will only be used if Vision API fails or for recent data not yet in Vision
       logger.debug(
           f"Using Vision API as preferred source for data retrieval (FCP: Cache → Vision → REST)"
       )
       return True
   ```

2. This simplified logic:
   - Corrects the inaccurate handling of 1s intervals, only rejecting them for non-SPOT markets
   - Removes the arbitrary historical data threshold (`VISION_DATA_DELAY_HOURS`)
   - Always prefers Vision API as the primary source after cache
   - Maintains the fallback mechanism where REST API is used if Vision API fails
   - Supports the Failover Composition Priority (FCP) approach

### 2. Design Updated Data Flow with Failover Composition Priority (FCP)

**New Flow:**

1. Check cache first for any available data
2. For any missing data ranges:
   - Try Vision API first for all ranges regardless of size
   - If Vision API fails or data is too recent, use REST API
   - For REST API requests exceeding 1000 data points, automatically chunk them using the existing robust chunking logic
3. Merge all results from all sources into a unified DataFrame (maintaining LSP)

### 3. Core Function Modifications

#### A. Time Range Splitting Logic

Create a new method in `DataSourceManager`:

```python
def _split_time_range(
    self,
    start_time: datetime,
    end_time: datetime,
    interval: Interval
) -> Tuple[
    Optional[Tuple[datetime, datetime]],  # vision_range
    Optional[Tuple[datetime, datetime]]   # rest_range
]:
    """Split a time range into segments for Vision and REST APIs based on availability.

    Always tries Vision API first, falling back to REST API for recent data not in Vision.
    This approach follows the Failover Composition Priority (FCP) concept where
    Cache → Vision API → REST API in order of preference.
    """
    # For 1s intervals in non-SPOT markets, always use REST API only
    if interval.name == Interval.SECOND_1.name and self.market_type != MarketType.SPOT:
        return None, (start_time, end_time)

    # For all other cases, try Vision API first for the entire range
    # If Vision API fails for any portion, the fallback mechanism will use REST API
    return (start_time, end_time), None
```

#### B. Update `_get_data_impl()`

Refactor `_get_data_impl()` to implement the Failover Composition Priority (FCP) concept:

```python
# After cache retrieval but before current source selection
cached_df = ... # existing cache retrieval logic
missing_ranges = self._identify_missing_ranges(cached_df, start_time, end_time, interval)

all_results = []
if cached_df is not None and not cached_df.empty:
    # Add source metadata for tracking and diagnostics
    cached_df['_data_source'] = DataSource.CACHE.name
    all_results.append(cached_df)
    logger.debug(f"Using cached data for portions of the requested time range (FCP: Cache)")

for missing_start, missing_end in missing_ranges:
    # Only process if user hasn't enforced a specific source
    if enforce_source == DataSource.AUTO:
        vision_range, rest_range = self._split_time_range(missing_start, missing_end, interval)

        if vision_range:
            # Try Vision API first (FCP: Cache → Vision)
            try:
                logger.debug(f"Attempting Vision API for range {vision_range[0]} to {vision_range[1]} (FCP: Cache → Vision)")
                vision_df = await self._fetch_from_source(
                    symbol, vision_range[0], vision_range[1], interval, use_vision=True
                )
                if not vision_df.empty:
                    # Add source metadata
                    vision_df['_data_source'] = DataSource.VISION.name
                    all_results.append(vision_df)
                    logger.debug(f"Successfully retrieved data from Vision API (FCP: Cache → Vision)")
                else:
                    # If Vision API returned empty results, fall back to REST API for this range
                    logger.debug(f"Vision API returned empty results, falling back to REST API (FCP: Cache → Vision → REST)")
                    rest_range = vision_range
            except Exception as e:
                logger.warning(f"Vision API fetch failed, falling back to REST API: {e} (FCP: Cache → Vision → REST)")
                rest_range = vision_range

        if rest_range:
            # Use REST API as fallback (FCP: Cache → Vision → REST)
            try:
                logger.debug(f"Using REST API for range {rest_range[0]} to {rest_range[1]} (FCP: Cache → Vision → REST)")
                rest_df = await self._fetch_from_source(
                    symbol, rest_range[0], rest_range[1], interval, use_vision=False
                )
                if not rest_df.empty:
                    # Add source metadata
                    rest_df['_data_source'] = DataSource.REST.name
                    all_results.append(rest_df)
                    logger.debug(f"Successfully retrieved data from REST API (FCP: Cache → Vision → REST)")
            except Exception as e:
                logger.error(f"REST API fetch failed: {e}")
                # If REST API fails, we've exhausted all fallback options
    else:
        # User enforced a specific source, respect that choice
        use_vision = (enforce_source == DataSource.VISION)
        try:
            df = await self._fetch_from_source(
                symbol, missing_start, missing_end, interval, use_vision=use_vision
            )
            if not df.empty:
                # Add source metadata
                df['_data_source'] = enforce_source.name
                all_results.append(df)
        except Exception as e:
            logger.error(f"Data fetch with enforced source {enforce_source.name} failed: {e}")

# Merge results from all sources (maintaining LSP)
if all_results:
    logger.debug(f"Merging data from {len(all_results)} sources into a unified DataFrame (LSP compliant)")
    final_df = pd.concat(all_results)
    final_df = final_df.sort_index().drop_duplicates()

    # Log information about the composition of the final DataFrame
    source_counts = {}
    if '_data_source' in final_df.columns:
        for source in final_df['_data_source'].unique():
            count = len(final_df[final_df['_data_source'] == source])
            source_counts[source] = count
        logger.info(f"Final DataFrame composition: {source_counts}")

    return final_df
else:
    logger.warning("No data retrieved from any source (Cache, Vision API, or REST API)")
    return self.create_empty_dataframe()
```

#### C. Add Missing Ranges Identification

```python
def _identify_missing_ranges(
    self,
    df: Optional[pd.DataFrame],
    start_time: datetime,
    end_time: datetime,
    interval: Interval
) -> List[Tuple[datetime, datetime]]:
    """Identify missing time ranges in a DataFrame to support the FCP concept.

    This method identifies gaps in the cached data that need to be filled by
    Vision API or REST API, following the Failover Composition Priority.

    Args:
        df: Cached DataFrame (possibly None if cache is empty)
        start_time: Start time of the requested range
        end_time: End time of the requested range
        interval: Time interval

    Returns:
        List of (start, end) tuples representing missing ranges
    """
    if df is None or df.empty:
        return [(start_time, end_time)]

    # Create a time series with the expected frequency
    expected_range = pd.date_range(
        start=start_time,
        end=end_time,
        freq=self._interval_to_freq(interval)
    )

    # Find missing timestamps
    missing_times = expected_range.difference(df.index)

    if len(missing_times) == 0:
        return []

    # Convert missing timestamps to continuous ranges
    return self._timestamps_to_ranges(missing_times, expected_range.freq)
```

#### D. Source Data Identification

Enhance the source data identification to support the FCP concept and LSP compliance:

```python
async def get_data(
    self,
    symbol: str,
    start_time: datetime,
    end_time: datetime,
    interval: Interval = Interval.SECOND_1,
    use_cache: bool = True,
    enforce_source: DataSource = DataSource.AUTO,
    provider: Optional[DataProvider] = None,
    chart_type: Optional[ChartType] = None,
    include_source_metadata: bool = False,  # New parameter
) -> pd.DataFrame:
    """Get data for symbol within time range using the Failover Composition Priority approach.

    This method retrieves data following the Failover Composition Priority (FCP):
    1. Check Cache first
    2. Try Vision API for missing data
    3. Fall back to REST API if Vision API fails

    The final DataFrame maintains Liskov Substitution Principle (LSP) compliance,
    ensuring the same format and behavior regardless of which source(s) the data came from.
    """
    # ... existing code ...

    # Retain or remove source metadata based on user preference
    if not include_source_metadata and '_data_source' in df.columns:
        df = df.drop(columns=['_data_source'])

    return df
```

### 4. Helper Functions

Enhance helper functions to support the FCP concept:

```python
def _interval_to_freq(self, interval: Interval) -> str:
    """Convert Interval enum to pandas frequency string for FCP data composition."""
    # Mapping from Interval enum to pandas freq string
    interval_to_freq_map = {
        Interval.SECOND_1.name: 'S',
        Interval.MINUTE_1.name: 'T',
        Interval.MINUTE_3.name: '3T',
        Interval.MINUTE_5.name: '5T',
        Interval.MINUTE_15.name: '15T',
        Interval.MINUTE_30.name: '30T',
        Interval.HOUR_1.name: 'H',
        Interval.HOUR_2.name: '2H',
        Interval.HOUR_4.name: '4H',
        Interval.HOUR_6.name: '6H',
        Interval.HOUR_8.name: '8H',
        Interval.HOUR_12.name: '12H',
        Interval.DAY_1.name: 'D',
        Interval.DAY_3.name: '3D',
        Interval.WEEK_1.name: 'W',
        Interval.MONTH_1.name: 'M',
    }
    return interval_to_freq_map.get(interval.name, 'T')  # Default to 1-minute if unknown

def _timestamps_to_ranges(
    self,
    timestamps: pd.DatetimeIndex,
    freq: pd.Timedelta
) -> List[Tuple[datetime, datetime]]:
    """Convert a set of timestamps to continuous date ranges for FCP-based data retrieval.

    This method consolidates missing timestamps into continuous ranges to minimize
    the number of API calls needed when following the Failover Composition Priority.
    """
    if len(timestamps) == 0:
        return []

    # Sort timestamps to ensure proper range formation
    timestamps = timestamps.sort_values()

    ranges = []
    range_start = timestamps[0]
    prev_ts = timestamps[0]

    # Group consecutive timestamps into ranges
    for ts in timestamps[1:]:
        # Check if timestamp is consecutive (accounting for interval frequency)
        if (ts - prev_ts) > freq:
            # End the current range and start a new one
            ranges.append((range_start, prev_ts))
            range_start = ts
        prev_ts = ts

    # Add the last range
    ranges.append((range_start, prev_ts))

    return ranges
```

### 5. Caching Strategy Updates

Update caching to work with the Failover Composition Priority (FCP) concept:

1. Merge all data into a single cache entry regardless of source
2. Cache the final concatenated DataFrame from all sources
3. Maintain the existing cache validation logic for merged data
4. Add metadata tracking of source information in the cache

```python
async def _update_cache_with_merged_data(
    self,
    df: pd.DataFrame,
    symbol: str,
    interval: Interval,
    cache_date: datetime,
) -> bool:
    """Update cache with data from multiple sources following the FCP approach.

    This method merges data from all sources (Cache, Vision API, REST API) and
    stores it as a single cache entry, maintaining LSP compliance.
    """
    if df is None or df.empty:
        logger.debug("No data to cache")
        return False

    # Prepare the data for caching
    # (Ensure proper timezone, sort by index, remove duplicates)
    df = df.sort_index().drop_duplicates()

    # Store information about data sources as cache metadata
    source_info = {}
    if '_data_source' in df.columns:
        for source in df['_data_source'].unique():
            count = len(df[df['_data_source'] == source])
            source_info[source] = count

    # Remove source metadata column before caching (optional)
    if '_data_source' in df.columns:
        df = df.drop(columns=['_data_source'])

    # Save to cache with source composition metadata
    try:
        await self._cache_manager.save_to_cache(
            df,
            date=cache_date,
            metadata={"source_composition": source_info},
            **{
                "symbol": symbol,
                "interval": interval.value,
                "provider": self.provider.name,
                "chart_type": self.chart_type.name,
            },
        )
        logger.debug(f"Successfully cached merged data from multiple sources: {source_info}")
        return True
    except Exception as e:
        logger.error(f"Failed to cache merged data: {e}")
        return False
```

### 6. Testing Plan

1. Unit tests:

   - Test time range splitting logic for various scenarios, ensuring FCP implementation
   - Test missing ranges identification with different cache states
   - Test merging logic for data from multiple sources (Cache, Vision, REST)
   - Test LSP compliance - ensure the final DataFrame has the same format regardless of source composition
   - Test the updated source selection logic without the historical threshold
   - Test 1s interval handling for SPOT vs non-SPOT markets

2. Integration tests:

   - Test end-to-end data retrieval with various time ranges using the FCP approach
   - Test partial cache hits - verify that only missing portions are retrieved from API
   - Test Vision API failures and confirm fallback to REST API
   - Test the seamless composition of data from all three sources
   - Verify proper handling of 1s data in SPOT markets via Vision API
   - Test error handling in the FCP chain

3. Performance tests:
   - Measure overhead of the FCP approach vs. the current single-source approach
   - Verify the approach scales with large data ranges
   - Benchmark partial cache hits vs. full API retrievals
   - Compare performance with and without the historical threshold

### 7. Implementation Phases

1. **Phase 1: Update Source Selection Logic**

   - Correct the handling of 1s intervals for SPOT markets
   - Remove the historical threshold rule from `_should_use_vision_api`
   - Implement the FCP concept in the source selection logic
   - Update tests for this function
   - Review with team

2. **Phase 2: Core FCP Logic**

   - Implement `_split_time_range()` based on the FCP strategy
   - Implement `_identify_missing_ranges()` for partial cache hits
   - Add tests for these functions
   - Review with team

3. **Phase 3: Modified Retrieval Flow**

   - Update `_get_data_impl()` to use the FCP approach
   - Implement source tracking metadata
   - Ensure LSP compliance in the merged DataFrame
   - Keep backward compatibility

4. **Phase 4: Caching Refinements**

   - Update caching strategy for the FCP approach
   - Implement single cache entry for merged data from all sources
   - Add source composition metadata
   - Optimize cache utilization for partial hits

5. **Phase 5: Testing and Validation**

   - Add comprehensive tests for all FCP scenarios
   - Verify LSP compliance in all data retrieval paths
   - Benchmark performance of the FCP approach
   - Verify behavior in varied scenarios

6. **Phase 6: Documentation and Final Review**
   - Update documentation to explain the FCP concept
   - Add examples of multi-source data composition
   - Document LSP compliance guarantees
   - Final review with team

## Risks and Mitigations

1. **Complexity Increase**

   - Mitigation: Isolate FCP logic in clear, well-tested functions
   - Add comprehensive logging of source transitions
   - Ensure backward compatibility

2. **Performance Overhead**

   - Mitigation: Optimize cache checking to minimize overhead
   - Implement efficient range merging to reduce API calls
   - Benchmark before/after implementation

3. **Cache Coherence**

   - Mitigation: Develop clear rules for handling overlapping/conflicting data
   - Store source composition metadata to aid debugging
   - Ensure validation checks for merged data

4. **Backward Compatibility**

   - Mitigation: Keep existing interfaces working
   - Add feature flags to enable/disable FCP behavior
   - Maintain LSP compliance across all code paths

5. **Loss of Historical Data Threshold**
   - Risk: Removing the VISION_DATA_DELAY_HOURS threshold might impact some edge cases
   - Mitigation: Thorough testing to ensure all scenarios are covered
   - FCP mechanism ensures REST API is used if Vision API fails

## Conclusion

This refactoring will enhance the DataSourceManager to intelligently retrieve data from the most appropriate sources following the Failover Composition Priority (FCP) approach: Cache first, then Vision API, followed by REST API as needed. By implementing this approach while ensuring Liskov Substitution Principle (LSP) compliance, we create a robust and efficient data retrieval system that:

1. Minimizes API calls by using cache whenever possible
2. Prefers Vision API for historical data (including 1s intervals in SPOT markets)
3. Falls back to REST API with chunking when necessary
4. Seamlessly combines data from multiple sources into a consistent DataFrame
5. Maintains the same output format and behavior regardless of the data source composition

The result will be a more resilient system that optimizes data retrieval while providing a consistent interface to users.
