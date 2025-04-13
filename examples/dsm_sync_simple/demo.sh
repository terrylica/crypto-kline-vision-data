#!/bin/bash
# Script to demonstrate synchronous data retrieval using DataSourceManager
# with Failover Composition Priority (FCP) for data source merging

# Navigate to project root
if [[ -d "core" && -d "utils" && -d "examples" ]]; then
  # Already in project root
  echo "Running from project root directory"
else
  # Try to navigate to project root
  if [[ -d "../../core" && -d "../../utils" ]]; then
    cd ../..
    echo "Changed to project root directory: $(pwd)"
  else
    echo "Error: Unable to locate project root directory"
    echo "Please run this script from either the project root or the examples/dsm_sync_simple directory"
    exit 1
  fi
fi

# Set script to exit on error
set -e

# Define colors for output
GREEN='\033[1;32m'
BLUE='\033[1;34m'
CYAN='\033[1;36m'
YELLOW='\033[1;33m'
RED='\033[1;31m'
NC='\033[0m' # No Color

# Check dependencies
check_dependencies() {
  # Check if matplotlib is installed (for statistics visualization)
  if ! python -c "import matplotlib" &>/dev/null; then
    echo -e "${YELLOW}Warning: matplotlib is not installed. Statistics visualizations will not be available.${NC}"
    echo -e "You can install it using: pip install matplotlib"
  fi

  # Check if pandas is installed
  if ! python -c "import pandas" &>/dev/null; then
    echo -e "${RED}Error: pandas is not installed. This demo requires pandas.${NC}"
    echo -e "Please install it using: pip install pandas"
    exit 1
  fi

  # Check if rich is installed
  if ! python -c "import rich" &>/dev/null; then
    echo -e "${RED}Error: rich is not installed. This demo requires rich for formatting.${NC}"
    echo -e "Please install it using: pip install rich"
    exit 1
  fi
}

# Show help function
show_help() {
  echo -e "${GREEN}################################################${NC}"
  echo -e "${GREEN}# Binance Data Retrieval Demo with FCP Merging #${NC}"
  echo -e "${GREEN}################################################${NC}"
  echo -e "This script demonstrates data source merging across Cache, VISION API, and REST API using the Failover Composition Priority (FCP).\n"
  
  echo -e "${CYAN}USAGE:${NC}"
  echo -e "  ./examples/dsm_sync_simple/demo.sh [OPTIONS] [PARAMETERS]${NC}"
  echo -e "  ./examples/dsm_sync_simple/demo.sh [MARKET] [SYMBOL] [INTERVAL] [CHART_TYPE]${NC}\n"
  
  echo -e "${CYAN}OPTIONS:${NC}"
  echo -e "  -h, --help             Show this help message and exit"
  echo -e "  --cache-demo           Demonstrate cache behavior by running the data retrieval twice"
  echo -e "  --historical-test      Run historical test with specific dates (Dec 2024-Feb 2025)"
  echo -e "  --detailed-stats       Show detailed statistics after the run and save to JSON file${NC}\n"
  
  echo -e "${CYAN}PARAMETERS:${NC}"
  echo -e "  MARKET                 Market type: spot, um, or cm (default: spot)"
  echo -e "  SYMBOL                 Trading symbol (default: BTCUSDT)"
  echo -e "  INTERVAL               Time interval: 1m, 5m, etc. (default: 1m)"
  echo -e "  CHART_TYPE             Type of chart data: klines or fundingRate (default: klines)${NC}\n"
  
  echo -e "${CYAN}EXAMPLES:${NC}"
  echo -e "  ${YELLOW}./examples/dsm_sync_simple/demo.sh${NC}                            # Run default FCP merge demo for BTCUSDT in SPOT market"
  echo -e "  ${YELLOW}./examples/dsm_sync_simple/demo.sh spot ETHUSDT 5m klines${NC}     # Run merge demo for ETH with 5m interval"
  echo -e "  ${YELLOW}./examples/dsm_sync_simple/demo.sh um BTCUSDT 1m klines${NC}       # Run merge demo for BTC in UM futures market"
  echo -e "  ${YELLOW}./examples/dsm_sync_simple/demo.sh cm BTCUSD_PERP 1m klines${NC}   # Run merge demo for BTC in CM futures market"
  echo -e "  ${YELLOW}./examples/dsm_sync_simple/demo.sh --cache-demo spot BTCUSDT${NC}  # Demonstrate cache performance"
  echo -e "  ${YELLOW}./examples/dsm_sync_simple/demo.sh --historical-test spot${NC}     # Run historical test in SPOT market"
  echo -e "  ${YELLOW}./examples/dsm_sync_simple/demo.sh --detailed-stats spot${NC}      # Run with detailed statistics"
}

# Function to demonstrate data source merging (now the default)
function run_merge_demo() {
  local market_type=${1:-"spot"}
  local symbol=${2:-"BTCUSDT"}
  local interval=${3:-"1m"}
  local chart_type=${4:-"klines"}
  local enforce_source=${5:-"AUTO"}
  local detailed_stats=${6:-"false"}
  
  echo -e "\n${CYAN}=================================================${NC}"
  echo -e "${CYAN}Running Data Source Merge Demo with FCP${NC}"
  echo -e "${CYAN}Market: $market_type | Symbol: $symbol | Interval: $interval | Chart Type: $chart_type${NC}"
  echo -e "${CYAN}=================================================${NC}"
  
  # Build command
  cmd="python examples/dsm_sync_simple/demo.py --market \"$market_type\" --symbol \"$symbol\" --interval \"$interval\" --chart-type \"$chart_type\""
  
  if [ "$enforce_source" != "AUTO" ]; then
    cmd="$cmd --enforce-source=$enforce_source"
  fi
  
  # Run the command
  eval $cmd
}

# Function to run historical data test
function run_historical_test() {
  local market_type=${1:-"spot"}
  local symbol=${2:-"BTCUSDT"}
  local interval=${3:-"1m"}
  local provider=${4:-"binance"}
  local use_cache="true"
  local debug_mode=${5:-"false"}
  local retries=${6:-3}
  local chart_type=${7:-"klines"}
  
  echo -e "${GREEN}################################################${NC}"
  echo -e "${GREEN}# Long-Term Historical Data Test Mode #${NC}"
  echo -e "${GREEN}################################################${NC}"
  echo -e "Running historical test for $market_type market with $symbol\n"
  
  # Construct the Python command
  cmd="python examples/dsm_sync_simple/demo.py"
  cmd+=" --market=$market_type"
  cmd+=" --symbol=$symbol"
  cmd+=" --interval=$interval"
  cmd+=" --provider=$provider"
  cmd+=" --retries=$retries"
  cmd+=" --chart-type=$chart_type"
  cmd+=" --historical-test"
  
  # Add optional flags
  if [ "$use_cache" = "true" ]; then
    cmd+=" --use-cache"
  fi
  
  if [ "$debug_mode" = "true" ]; then
    cmd+=" --debug"
  fi
  
  # Execute the command
  echo -e "${BLUE}Executing: $cmd${NC}"
  eval $cmd
}

# Function to demonstrate cache performance
function demo_cache_performance() {
  local market_type=${1:-"spot"}
  local symbol=${2:-"BTCUSDT"}
  local interval=${3:-"1m"}
  local chart_type=${4:-"klines"}
  
  echo -e "${GREEN}################################################${NC}"
  echo -e "${GREEN}# Cache Performance Demonstration #${NC}"
  echo -e "${GREEN}################################################${NC}"
  echo -e "Demonstrating cache behavior for $market_type market with $symbol\n"
  
  # Construct the Python command
  cmd="python examples/dsm_sync_simple/demo.py"
  cmd+=" --market=$market_type"
  cmd+=" --symbol=$symbol"
  cmd+=" --interval=$interval"
  cmd+=" --chart-type=$chart_type"
  cmd+=" --demo-cache"
  cmd+=" --show-cache"
  
  # Execute the command
  echo -e "${BLUE}Executing: $cmd${NC}"
  eval $cmd
}

# Check dependencies before proceeding
check_dependencies

# Parse arguments
if [[ $1 == "-h" || $1 == "--help" ]]; then
  # Show help
  show_help
  exit 0
elif [[ $1 == "--cache-demo" ]]; then
  # Run cache performance demo
  shift
  market_type=${1:-"spot"}
  symbol=${2:-"BTCUSDT"}
  interval=${3:-"1m"}
  chart_type=${4:-"klines"}
  
  demo_cache_performance "$market_type" "$symbol" "$interval" "$chart_type"
  exit 0
elif [[ $1 == "--historical-test" ]]; then
  # Run historical data test
  shift
  market_type=${1:-"spot"}
  symbol=${2:-"BTCUSDT"}
  interval=${3:-"1m"}
  provider=${4:-"binance"}
  debug_mode=${5:-"false"}
  
  run_historical_test "$market_type" "$symbol" "$interval" "$provider" "$debug_mode"
  exit 0
elif [[ $1 == "--detailed-stats" ]]; then
  # Run with detailed statistics
  shift
  market_type=${1:-"spot"}
  symbol=${2:-"BTCUSDT"}
  interval=${3:-"1m"}
  chart_type=${4:-"klines"}
  
  run_merge_demo "$market_type" "$symbol" "$interval" "$chart_type" "AUTO" "true"
  exit 0
else
  # Run the default merge demo with provided or default parameters
  market_type=${1:-"spot"}
  symbol=${2:-"BTCUSDT"}
  interval=${3:-"1m"}
  chart_type=${4:-"klines"}
  
  run_merge_demo "$market_type" "$symbol" "$interval" "$chart_type"
  exit 0
fi
