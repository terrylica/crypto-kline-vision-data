#!/bin/bash
# Test script for the gap detection refactoring
# This script runs various tests to ensure the refactored code works correctly

set -e  # Exit immediately if any command fails

echo "===== Gap Detection Refactoring Tests ====="
echo ""

# Current directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# First test the direct gap detector
echo "Step 1: Testing the gap detector directly"
python test_gap_refactoring.py
echo ""

# Test the vision_data_client.py
echo "Step 2: Testing the vision_data_client with gap detection"
# Create a simple test script
cat > test_vision_client.py << 'EOL'
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
EOL

# Make the script executable
chmod +x test_vision_client.py

# Run the script
python test_vision_client.py
echo ""

# Test the gap_debugger.py
echo "Step 3: Testing the gap_debugger.py"
# Create a simple test script
cat > test_gap_debugger.py << 'EOL'
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
EOL

# Make the script executable
chmod +x test_gap_debugger.py

# Run the script
python test_gap_debugger.py
echo ""

# Test running the main demo script
echo "Step 4: Testing the main demo.sh script"
# Run demo.sh if it exists
if [ -f "./demo.sh" ]; then
    bash ./demo.sh spot 1 BTCUSDT
else
    echo "demo.sh not found in the current directory, skipping this test"
fi

echo ""
echo "===== All tests completed =====" 