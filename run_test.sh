#!/bin/bash
# Run the rate limit tester with the specified duration

# Default duration is 300 seconds (5 minutes)
DURATION=${1:-300}

echo "Running rate limit test for $DURATION seconds"
cd "$(dirname "$0")"
PYTHONPATH=/workspaces/binance-data-services python3 rate_limit_tester.py --duration $DURATION 