#!/bin/bash
# Script to run the direct API test

# Set default duration to 30 seconds if not provided
DURATION=${1:-30}
# Set default limit to 1000 data points if not provided
LIMIT=${2:-1000}

# Get the absolute path of the playground/rate_limit_test directory
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# Make the test script executable
chmod +x ${SCRIPT_DIR}/direct_api_test.py

# Create results directory
mkdir -p ${SCRIPT_DIR}/results

# Set PYTHONPATH and run the direct API test with the specified duration and limit
echo "Running direct API test for $DURATION seconds with $LIMIT data points per request..."
PYTHONPATH=/workspaces/data-source-manager python ${SCRIPT_DIR}/direct_api_test.py --duration $DURATION --limit $LIMIT 