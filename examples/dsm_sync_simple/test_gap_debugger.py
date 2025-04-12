#!/usr/bin/env python3
from datetime import datetime, timezone, timedelta
import pandas as pd
from examples.dsm_sync_simple.gap_debugger import debug_vision_client
from utils.market_constraints import MarketType, Interval
from utils.logger_setup import logger
from rich import print

# Configure logger
logger.use_rich(True)
logger.setLevel("INFO")

# Test parameters
symbol = "BTCUSDT"
interval = Interval.MINUTE_1
market_type = MarketType.SPOT

# Use a date range with a day boundary for testing
end_time = datetime(2025, 1, 10, 12, 0, 0, tzinfo=timezone.utc)
start_time = end_time - timedelta(days=1)

print(f"Running vision client debug for {symbol} from {start_time} to {end_time}")

try:
    # Use the debug_vision_client function from gap_debugger.py
    df, gap_info = debug_vision_client(
        symbol=symbol,
        interval=interval,
        start_time=start_time,
        end_time=end_time,
        market_type=market_type
    )

    # Check results
    if df is not None and not df.empty:
        # Create a clean copy to avoid any index/column conflicts
        try:
            safe_df = pd.DataFrame({'row_count': [len(df)]})
            print(f"Retrieved {len(df)} records with {len(gap_info)} reported gaps")
            
            if gap_info:
                print("\nGap details:")
                for i, gap in enumerate(gap_info):
                    print(f"Gap {i+1}: {gap['previous_time']} → {gap['current_time']}, " 
                        f"missing: {gap['missing_points']}, day boundary: {gap['day_boundary']}")
            else:
                print("No gaps detected")
        except Exception as e:
            print(f"Error processing results: {e}")
    else:
        print("Failed to retrieve data from debug_vision_client")
except Exception as e:
    print(f"Error running debug_vision_client: {e}")
