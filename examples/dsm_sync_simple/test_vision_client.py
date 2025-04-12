#!/usr/bin/env python3
from datetime import datetime, timezone, timedelta
from core.sync.vision_data_client import VisionDataClient
from utils.market_constraints import MarketType, Interval
from utils.logger_setup import logger
from rich import print
import pandas as pd

# Configure logger
logger.use_rich(True)
logger.setLevel("INFO")

# Test parameters
symbol = "BTCUSDT"
interval = Interval.MINUTE_1
market_type = MarketType.SPOT

# Use a date range with a day boundary for testing
end_time = datetime(2025, 1, 10, 12, 0, 0, tzinfo=timezone.utc)
start_time = end_time - timedelta(days=2)

print(f"Fetching data for {symbol} from {start_time} to {end_time}")

# Create client and fetch data
client = VisionDataClient(
    symbol=symbol,
    interval=interval.value,
    market_type=market_type
)

df = client.fetch(start_time, end_time)

# Check if we got data
if df is not None and not df.empty:
    # Carefully handle the dataframe to work with both indexed and non-indexed formats
    try:
        # Create a safe copy without index complications
        safe_df = pd.DataFrame(df.copy())
        
        # Get column access
        if 'open_time' in safe_df.columns:
            min_time = safe_df['open_time'].min()
            max_time = safe_df['open_time'].max()
        else:
            # Try to find the time column
            time_cols = [col for col in safe_df.columns if 'time' in str(col).lower()]
            if time_cols:
                min_time = safe_df[time_cols[0]].min()
                max_time = safe_df[time_cols[0]].max()
            else:
                min_time = "unknown"
                max_time = "unknown"
        
        print(f"Retrieved {len(safe_df)} records")
        print(f"Data spans from {min_time} to {max_time}")
    except Exception as e:
        print(f"Error working with dataframe: {e}")
        print(f"Retrieved {len(df)} records but couldn't extract time range")
else:
    print("Failed to retrieve data from VisionDataClient")
