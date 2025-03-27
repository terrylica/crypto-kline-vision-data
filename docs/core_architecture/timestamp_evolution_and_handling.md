# Timestamp Evolution and Handling in Binance Data Services

## Binance Vision Timestamp Evolution

It has been discovered that Binance Vision's timestamp format has evolved:

- **Pre-2025:** Timestamps are provided in milliseconds (13 digits).
- **2025 Onwards:** Timestamps are expected to be in microseconds (16 digits).

This evolution necessitates a flexible timestamp handling strategy to ensure the data services remain compatible with both historical and future data formats from Binance Vision.

## Observed Timestamp Precision Patterns

Consistent precision patterns have been identified in Binance Vision timestamps:

1. **`open_time`**:

   - Always aligned to second boundaries.
   - Microseconds component is always zero (`.000000`).
   - Format: `YYYYMMDD HH:MM:SS.000000`
   - Serves as the canonical index for DataFrames in the system.

2. **`close_time`**:
   - Exhibits full microsecond precision.
   - Consistently ends in `.999999` microseconds.
   - Is always 0.999999 seconds after the corresponding `open_time`.
   - Format: `YYYYMMDD HH:MM:SS.999999`
   - Used as a key indicator to verify microsecond precision support in the data.

These patterns are crucial for validating the correctness of timestamp parsing and conversion within the data services.

## Code Mechanisms for Handling Timestamp Evolution

The Binance Data Services core implements specific mechanisms to handle the timestamp evolution and precision patterns:

### 1. Dynamic Timestamp Unit Detection (`vision_constraints.py`)

```python:core/vision_constraints.py
# ...
def detect_timestamp_unit(sample_ts: int | str) -> TimestampUnit:
    # ...
```

- The `detect_timestamp_unit` function in `vision_constraints.py` is designed to dynamically detect whether a timestamp is in milliseconds or microseconds.
- It analyzes the number of digits in a sample timestamp to differentiate between the two formats (13 digits for milliseconds, 16 digits for microseconds).
- This function is critical for adapting to the format change expected in 2025 and beyond.

### 2. Microsecond Precision Processing and `close_time` Adjustment (`market_data_client.py`)

```python:core/market_data_client.py
def process_kline_data(raw_data: List[List]) -> pd.DataFrame:
    # ...
    for col in ["open_time", "close_time"]:
        # Convert milliseconds to microseconds by multiplying by 1000
        df[col] = df[col].astype(np.int64) * 1000
        df[col] = pd.to_datetime(df[col], unit=TIMESTAMP_UNIT, utc=True)

        # For close_time, add microseconds to match REST API behavior
        if col == "close_time":
            df[col] = df[col] + pd.Timedelta(microseconds=CLOSE_TIME_ADJUSTMENT)
    # ...
```

- The `process_kline_data` function in `market_data_client.py` is responsible for processing raw kline data and converting timestamps to `datetime` objects.
- **Microsecond Conversion**: It explicitly converts all timestamps to microsecond precision by multiplying millisecond timestamps by 1000 before using `pd.to_datetime`. This ensures consistent microsecond resolution regardless of the input format.
- **`close_time` Adjustment**: It adds a `CLOSE_TIME_ADJUSTMENT` (likely 999999 microseconds) to the `close_time` values. This adjustment directly addresses the observed pattern of `close_time` ending in `.999999`, ensuring accurate representation of the intended timestamp precision.

## Benefits of this Approach

- **Forward Compatibility**: The dynamic timestamp unit detection ensures the system will seamlessly handle the transition to microsecond timestamps from Binance Vision in 2025 and beyond.
- **Data Consistency**: By consistently processing timestamps in microseconds and adjusting `close_time`, the system maintains internal data consistency and aligns with the observed precision patterns.
- **Robustness**: The design anticipates and addresses potential changes in external data sources, making the data services more robust and less prone to breaking due to external format evolutions.

By implementing these mechanisms, Binance Data Services is well-equipped to handle the current and future timestamp formats from Binance Vision, ensuring data accuracy and reliability for downstream applications like financial time series forecasting.
