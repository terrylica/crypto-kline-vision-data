#!/bin/bash
# Test script for validating market constraints and cache integration

# Set script directory
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
BASE_DIR="$( cd "$SCRIPT_DIR/../.." && pwd )"
SYMBOL="BTCUSDT"
INTERVAL="5m"
TEST_DATE="2023-04-01"
TEST_DATA_CREATED=false

# Set colors for output
BLUE='\033[0;34m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Function to print section header
print_header() {
    echo -e "\n${BLUE}==== $1 ====${NC}\n"
}

# Function to print info
print_info() {
    echo -e "${GREEN}$1${NC}"
}

# Function to print warning
print_warning() {
    echo -e "${YELLOW}$1${NC}"
}

# Function to print error
print_error() {
    echo -e "${RED}$1${NC}"
}

# Test if we have data to work with
check_data_availability() {
    # Check if we have at least one file in the spot cache
    if [ -d "${BASE_DIR}/cache/BINANCE/KLINES/spot/${SYMBOL}/${INTERVAL}" ]; then
        for file in "${BASE_DIR}/cache/BINANCE/KLINES/spot/${SYMBOL}/${INTERVAL}"/*.arrow; do
            if [ -f "$file" ]; then
                return 0  # Data found
            fi
        done
    fi
    return 1  # No data found
}

# Function to create test data for different market types
create_test_data() {
    print_header "Creating test data for different market types"
    
    # Create spot market data
    print_info "Creating spot market data"
    "$SCRIPT_DIR/cache_builder.sh" --symbols "$SYMBOL" --intervals "$INTERVAL" --start-date "$TEST_DATE" --end-date "$TEST_DATE" --market-type "spot" --data-provider "BINANCE" --chart-type "KLINES"
    
    # Create USDT futures market data
    print_info "Creating USDT futures market data"
    "$SCRIPT_DIR/cache_builder.sh" --symbols "$SYMBOL" --intervals "1h" --start-date "$TEST_DATE" --end-date "$TEST_DATE" --market-type "futures_usdt" --data-provider "BINANCE" --chart-type "KLINES"
    
    # Create COIN futures market data (if symbol is available)
    print_info "Creating COIN futures market data"
    "$SCRIPT_DIR/cache_builder.sh" --symbols "$SYMBOL" --intervals "4h" --start-date "$TEST_DATE" --end-date "$TEST_DATE" --market-type "futures_coin" --data-provider "BINANCE" --chart-type "KLINES"
    
    # Mark that we created test data
    TEST_DATA_CREATED=true
}

# Main test function
run_tests() {
    print_header "Running market constraints integration tests"
    
    # First, check if there's data to test with
    if ! check_data_availability; then
        print_warning "No data available for testing. Creating test data..."
        create_test_data
    else
        print_info "Test data found. Proceeding with tests."
    fi
    
    # Run the Python test script
    print_info "Running Python test script"
    python -m scripts.arrow_cache.test_market_constraints
}

# Clean up function
cleanup() {
    if [ "$TEST_DATA_CREATED" = true ]; then
        print_header "Cleaning up test data"
        read -p "Remove test data created during this run? (y/n) " -n 1 -r
        echo
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            print_info "Removing test data..."
            rm -rf "${BASE_DIR}/cache/BINANCE/KLINES/spot/${SYMBOL}/${INTERVAL}/${TEST_DATE}.arrow"
            rm -rf "${BASE_DIR}/cache/BINANCE/KLINES/futures_um/${SYMBOL}/1h/${TEST_DATE}.arrow"
            rm -rf "${BASE_DIR}/cache/BINANCE/KLINES/futures_cm/${SYMBOL}/4h/${TEST_DATE}.arrow"
            print_info "Test data removed."
        else
            print_info "Keeping test data for further inspection."
        fi
    fi
}

# Run the tests
run_tests

# Clean up if needed
cleanup

print_header "Tests completed" 