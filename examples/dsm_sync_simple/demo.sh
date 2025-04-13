#!/bin/bash
# Script to demonstrate synchronous data retrieval using DataSourceManager
# for Bitcoin data across different market types (SPOT, UM, CM)

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

# Define function to run demo
run_market_demo() {
  local market_type=$1
  local days=$2
  local provider=${3:-"binance"}
  local cache_flag=$4
  local cache_demo_flag=$5
  local retries=${6:-3}
  local chart_type=${7:-"klines"}
  
  echo -e "\n${CYAN}===============================================${NC}"
  echo -e "${CYAN}Running Bitcoin data demo for $market_type market${NC}"
  echo -e "${CYAN}Provider: $provider${NC}"
  echo -e "${CYAN}Chart Type: $chart_type${NC}"
  if [[ "$cache_flag" == "--use-cache" ]]; then
    echo -e "${CYAN}Cache: enabled${NC}"
  fi
  if [[ "$cache_demo_flag" == "--demo-cache" ]]; then
    echo -e "${CYAN}Cache Demo: enabled (will run twice to show cache effect)${NC}"
  fi
  echo -e "${CYAN}Retry attempts: $retries${NC}"
  echo -e "${CYAN}===============================================${NC}"
  
  # Build command with appropriate flags
  cmd="python examples/dsm_sync_simple/demo.py --market \"$market_type\" --days \"$days\" --provider \"$provider\" --retries \"$retries\" --chart-type \"$chart_type\""
  
  if [[ "$cache_flag" == "--use-cache" ]]; then
    cmd="$cmd --use-cache"
  fi
  
  if [[ "$cache_demo_flag" == "--demo-cache" ]]; then
    cmd="$cmd --demo-cache"
  fi
  
  # Run the command
  eval $cmd
  
  # Add a pause between runs
  if [ "$market_type" != "cm" ]; then
    echo -e "\n${YELLOW}Pausing before next demo...${NC}"
    sleep 2
  fi
}

# Function to run the merge demo
run_merge_demo() {
  local market_type=$1
  local symbol=$2
  local interval=${3:-"1m"}
  local chart_type=${4:-"klines"}
  
  echo -e "\n${CYAN}=================================================${NC}"
  echo -e "${CYAN}Running Data Source Merge Demo${NC}"
  echo -e "${CYAN}Market: $market_type | Symbol: $symbol | Interval: $interval | Chart Type: $chart_type${NC}"
  echo -e "${CYAN}=================================================${NC}"
  
  # Build command with appropriate flags
  cmd="python examples/dsm_sync_simple/demo.py --market \"$market_type\" --symbol \"$symbol\" --interval \"$interval\" --chart-type \"$chart_type\" --demo-merge"
  
  # Run the command
  eval $cmd
}

# Function to run the long-term historical data test
run_historical_test() {
  local market_type=$1
  local symbol=${2:-"BTCUSDT"}
  local interval=${3:-"1m"}
  local provider=${4:-"binance"}
  local use_cache=${5:-"--use-cache"}
  local debug_mode=${6:-""}
  local retries=${7:-3}
  local chart_type=${8:-"klines"}
  
  echo -e "\n${CYAN}===============================================${NC}"
  echo -e "${CYAN}Running Long-Term Historical Data Test${NC}"
  echo -e "${CYAN}Market: $market_type | Symbol: $symbol | Interval: $interval | Chart Type: $chart_type${NC}" 
  echo -e "${CYAN}Provider: $provider | Retries: $retries${NC}"
  echo -e "${CYAN}Test uses data from Dec 24, 2024 12:09:03 to Feb 25, 2025 23:56:56${NC}"
  echo -e "${CYAN}(Today is April 11, 2025, so these dates are historical)${NC}"
  if [[ "$use_cache" == "--use-cache" ]]; then
    echo -e "${CYAN}Cache: enabled${NC}"
  else
    echo -e "${CYAN}Cache: disabled${NC}"
  fi
  if [[ "$debug_mode" == "--debug" ]]; then
    echo -e "${CYAN}Debug mode: enabled (fetching in chunks)${NC}"
  fi
  echo -e "${CYAN}===============================================${NC}"
  
  # Build command with appropriate flags
  cmd="python examples/dsm_sync_simple/demo.py --market \"$market_type\" --symbol \"$symbol\" --interval \"$interval\" --provider \"$provider\" --retries \"$retries\" --historical-test --show-cache --chart-type \"$chart_type\""
  
  if [[ "$use_cache" == "--use-cache" ]]; then
    cmd="$cmd --use-cache"
  fi
  
  if [[ "$debug_mode" == "--debug" ]]; then
    cmd="$cmd --debug"
  fi
  
  # Run the command
  eval $cmd
}

# Function to get data synchronously
function get_data_sync() {
    echo -e "${YELLOW}Running data retrieval synchronously using DataSourceManager...${NC}"
    
    # Default to spot market if not specified
    MARKET=${1:-spot}
    SYMBOL=${2:-BTCUSDT}
    INTERVAL=${3:-1m}
    DAYS=${4:-1}
    USE_CACHE=${5:-false}
    CHART_TYPE=${6:-klines}
    DEBUG=${7:-false}
    ENFORCE_SOURCE=${8:-AUTO}
    
    # Construct the Python command
    CMD="python examples/dsm_sync_simple/demo.py"
    CMD+=" --market=$MARKET"
    CMD+=" --symbol=$SYMBOL"
    CMD+=" --interval=$INTERVAL"
    CMD+=" --days=$DAYS"
    CMD+=" --chart-type=$CHART_TYPE"
    
    # Add optional flags
    if [ "$USE_CACHE" = "true" ]; then
        CMD+=" --use-cache"
    fi
    
    if [ "$DEBUG" = "true" ]; then
        CMD+=" --debug"
    fi
    
    if [ "$ENFORCE_SOURCE" != "AUTO" ]; then
        CMD+=" --enforce-source=$ENFORCE_SOURCE"
    fi
    
    # Execute the command
    echo -e "${BLUE}Executing: $CMD${NC}"
    eval $CMD
}

# Function to run historical data test
function run_historical_test() {
    echo -e "${YELLOW}Running historical data test using DataSourceManager...${NC}"
    
    # Default to spot market if not specified
    MARKET=${1:-spot}
    SYMBOL=${2:-BTCUSDT}
    INTERVAL=${3:-1m}
    USE_CACHE=${4:-true}
    CHART_TYPE=${5:-klines}
    DEBUG=${6:-false}
    ENFORCE_SOURCE=${7:-AUTO}
    
    # Construct the Python command
    CMD="python examples/dsm_sync_simple/demo.py"
    CMD+=" --market=$MARKET"
    CMD+=" --symbol=$SYMBOL"
    CMD+=" --interval=$INTERVAL"
    CMD+=" --chart-type=$CHART_TYPE"
    CMD+=" --historical-test"
    
    # Add optional flags
    if [ "$USE_CACHE" = "true" ]; then
        CMD+=" --use-cache"
    fi
    
    if [ "$DEBUG" = "true" ]; then
        CMD+=" --debug"
    fi
    
    if [ "$ENFORCE_SOURCE" != "AUTO" ]; then
        CMD+=" --enforce-source=$ENFORCE_SOURCE"
    fi
    
    # Execute the command
    echo -e "${BLUE}Executing: $CMD${NC}"
    eval $CMD
}

# Function to demonstrate data source merging
function demo_data_source_merging() {
    echo -e "${YELLOW}Demonstrating data source merging with DataSourceManager...${NC}"
    
    # Default to spot market if not specified
    MARKET=${1:-spot}
    SYMBOL=${2:-BTCUSDT}
    INTERVAL=${3:-1m}
    CHART_TYPE=${4:-klines}
    ENFORCE_SOURCE=${5:-AUTO}
    
    # Construct the Python command
    CMD="python examples/dsm_sync_simple/demo.py"
    CMD+=" --market=$MARKET"
    CMD+=" --symbol=$SYMBOL"
    CMD+=" --interval=$INTERVAL"
    CMD+=" --chart-type=$CHART_TYPE"
    CMD+=" --demo-merge"
    
    if [ "$ENFORCE_SOURCE" != "AUTO" ]; then
        CMD+=" --enforce-source=$ENFORCE_SOURCE"
    fi
    
    # Execute the command
    echo -e "${BLUE}Executing: $CMD${NC}"
    eval $CMD
}

# Function to demonstrate cache performance
function demo_cache_performance() {
    echo -e "${YELLOW}Demonstrating cache performance with DataSourceManager...${NC}"
    
    # Default to spot market if not specified
    MARKET=${1:-spot}
    SYMBOL=${2:-BTCUSDT}
    INTERVAL=${3:-1m}
    DAYS=${4:-1}
    CHART_TYPE=${5:-klines}
    
    # Construct the Python command
    CMD="python examples/dsm_sync_simple/demo.py"
    CMD+=" --market=$MARKET"
    CMD+=" --symbol=$SYMBOL"
    CMD+=" --interval=$INTERVAL"
    CMD+=" --days=$DAYS"
    CMD+=" --chart-type=$CHART_TYPE"
    CMD+=" --demo-cache"
    CMD+=" --show-cache"
    
    # Execute the command
    echo -e "${BLUE}Executing: $CMD${NC}"
    eval $CMD
}

# Parse arguments based on first parameter
if [[ $1 == "--demo-cache" ]]; then
  # Run cache demonstration for a specific market
  if [ $# -ge 3 ] && [ $# -le 6 ]; then
    market_type=$2
    days=$3
    provider=${4:-"binance"}
    retries=${5:-3}
    chart_type=${6:-"klines"}
    
    echo -e "${GREEN}################################################${NC}"
    echo -e "${GREEN}# Pure Synchronous Cache Demonstration Mode #${NC}"
    echo -e "${GREEN}################################################${NC}"
    echo -e "Demonstrating cache behavior for $market_type market\n"
    
    run_market_demo "$market_type" "$days" "$provider" "--use-cache" "--demo-cache" "$retries" "$chart_type"
    exit 0
  else
    echo -e "${RED}Error: Invalid number of arguments for cache demo mode${NC}"
    echo -e "Usage: $0 --demo-cache <market> <days> [provider] [retries] [chart-type]"
    echo -e "Example: $0 --demo-cache spot 1 binance 3 klines"
    exit 1
  fi
elif [[ $1 == "--historical-test" ]]; then
  # Run long-term historical data test
  if [ $# -ge 2 ]; then
    market_type=$2
    symbol=${3:-"BTCUSDT"}
    interval=${4:-"1m"}
    provider=${5:-"binance"}
    
    # Check for options
    use_cache="--use-cache"  # Default to using cache
    debug_mode=""            # Default to no debug mode
    retries=3                # Default retry count
    chart_type="klines"      # Default chart type
    
    # Parse remaining arguments if provided
    shift 5
    while [[ $# -gt 0 ]]; do
      case "$1" in
        --no-cache)
          use_cache=""
          shift
          ;;
        --debug)
          debug_mode="--debug"
          shift
          ;;
        --retries)
          retries="$2"
          shift 2
          ;;
        --chart-type)
          chart_type="$2"
          shift 2
          ;;
        *)
          echo -e "${RED}Error: Unknown option $1${NC}"
          exit 1
          ;;
      esac
    done
    
    echo -e "${GREEN}################################################${NC}"
    echo -e "${GREEN}# Long-Term Historical Data Test Mode #${NC}"
    echo -e "${GREEN}################################################${NC}"
    echo -e "Running historical test for $market_type market with $symbol\n"
    
    run_historical_test "$market_type" "$symbol" "$interval" "$provider" "$use_cache" "$debug_mode" "$retries" "$chart_type"
    exit 0
  else
    echo -e "${RED}Error: Invalid number of arguments for historical test mode${NC}"
    echo -e "Usage: $0 --historical-test <market> [symbol] [interval] [provider] [options]"
    echo -e "Options:"
    echo -e "  --no-cache          Disable cache usage"
    echo -e "  --debug             Enable debug mode (fetches data in chunks)"
    echo -e "  --retries <num>     Set number of retry attempts (default: 3)"
    echo -e "  --chart-type <type> Set chart type (klines, fundingRate) (default: klines)"
    echo -e ""
    echo -e "Example: $0 --historical-test spot BTCUSDT 1m binance --debug --chart-type fundingRate"
    exit 1
  fi
elif [[ $1 == "--demo-merge" ]]; then
  # Run data source merging demo
  if [ $# -ge 1 ] && [ $# -le 4 ]; then
    # Run with user-specified parameters
    market_type=$2
    symbol=${3:-"BTCUSDT"}
    interval=${4:-"1m"}
    chart_type=${5:-"klines"}
    
    echo -e "${GREEN}################################################${NC}"
    echo -e "${GREEN}# Data Source Merging Demonstration #${NC}"
    echo -e "${GREEN}################################################${NC}"
    echo -e "This demo shows how data from multiple sources is seamlessly merged:"
    echo -e "1. Cache (local Arrow files)"
    echo -e "2. Vision API (historical data)"
    echo -e "3. REST API (recent data)"
    echo -e ""
    
    run_merge_demo "$market_type" "$symbol" "$interval" "$chart_type"
    exit 0
  else
    echo -e "${RED}Error: Invalid number of arguments for merge demo mode${NC}"
    echo -e "Usage: $0 --demo-merge <market> [symbol] [interval] [chart-type]"
    echo -e "Example: $0 --demo-merge spot BTCUSDT 1m klines"
    exit 1
  fi
# Check if all parameters were provided for standard run
elif [ $# -ge 2 ] && [ $# -le 6 ]; then
  # Run for a single market type with specified parameters
  market_type=$1
  days=$2
  provider=${3:-"binance"}
  if [[ "$4" == "--use-cache" ]]; then
    cache_flag="--use-cache"
  else
    cache_flag=""
  fi
  retries=${5:-3}
  chart_type=${6:-"klines"}
  
  run_market_demo "$market_type" "$days" "$provider" "$cache_flag" "" "$retries" "$chart_type"
  exit 0
fi

# Default: run for all market types with 1 day of data
echo -e "${GREEN}################################################${NC}"
echo -e "${GREEN}# Pure Synchronous Binance Data Retrieval Demo #${NC}"
echo -e "${GREEN}################################################${NC}"
echo -e "Retrieving Bitcoin data for SPOT, UM, and CM markets\n"

# Run demo for all market types
run_market_demo "spot" 1 "binance" "" "" 3 "klines"
run_market_demo "um" 1 "binance" "" "" 3 "klines"
run_market_demo "cm" 1 "binance" "" "" 3 "klines"

echo -e "\n${GREEN}All demos completed successfully!${NC}"
echo -e "To run for a specific market with custom parameters:"
echo -e "  ${YELLOW}./examples/dsm_sync_simple/demo.sh spot 7${NC}                       # For 7 days of SPOT data"
echo -e "  ${YELLOW}./examples/dsm_sync_simple/demo.sh um 3${NC}                         # For 3 days of UM futures data"
echo -e "  ${YELLOW}./examples/dsm_sync_simple/demo.sh cm 5 binance${NC}                 # For 5 days of CM futures data with explicit provider"
echo -e "  ${YELLOW}./examples/dsm_sync_simple/demo.sh spot 1 binance --use-cache${NC}   # Enable caching for the request"
echo -e "  ${YELLOW}./examples/dsm_sync_simple/demo.sh spot 1 binance --use-cache 5${NC} # With 5 retry attempts"
echo -e "  ${YELLOW}./examples/dsm_sync_simple/demo.sh spot 1 binance --use-cache 3 klines${NC} # Specify chart type (klines, fundingRate)"
echo -e "  ${YELLOW}./examples/dsm_sync_simple/demo.sh --demo-cache spot 1${NC}          # Demonstrate cache behavior by running twice"
echo -e "  ${YELLOW}./examples/dsm_sync_simple/demo.sh --historical-test spot${NC}       # Run historical test (Dec 2024-Feb 2025)"
echo -e "  ${YELLOW}./examples/dsm_sync_simple/demo.sh --historical-test spot BTCUSDT 1m binance --chart-type fundingRate${NC} # Run historical test with funding rate data"
echo -e "  ${YELLOW}./examples/dsm_sync_simple/demo.sh --demo-merge spot${NC}            # Run data source merging demo"
echo -e "  ${YELLOW}./examples/dsm_sync_simple/demo.sh --demo-merge spot ETHUSDT 5m klines${NC} # Run merge demo with custom parameters"

exit 0
