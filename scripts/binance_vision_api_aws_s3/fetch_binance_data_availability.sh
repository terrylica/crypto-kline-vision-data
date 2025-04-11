#!/bin/bash

###########################################################################
# Binance Data Availability Fetcher
#
# Description:
#   This script efficiently retrieves all available trading symbols 
#   and their earliest available data date from Binance Vision data 
#   repository. It works with spot, um (USDT-M futures), and cm (COIN-M futures)
#   markets and creates filtered lists based on specified criteria.
#
# Features:
#   - Multi-market support (spot, USDT-M futures, COIN-M futures)
#   - Parallel processing for faster data retrieval
#   - Automatic generation of market-specific and combined reports
#   - Cross-market symbol filtering based on quote currencies
#   - Customizable output formats and directories
#   - Performance timing for execution analysis
#
# Usage:
#   ./fetch_binance_data_availability.sh [OPTIONS]
#
# See function 'usage()' for available options.
###########################################################################

###########################################
# Check Dependencies
###########################################

# Check and install required dependencies
check_dependencies() {
    # Check if bc is installed
    if ! command -v bc &> /dev/null; then
        echo "Required dependency 'bc' is not installed."
        
        # If auto install not enabled, prompt the user
        if [[ "$AUTO_INSTALL_DEPS" != "true" ]]; then
            read -p "Would you like to automatically install the missing dependencies? (y/n): " install_deps
            if [[ "$install_deps" =~ ^[Yy]$ ]]; then
                AUTO_INSTALL_DEPS=true
            fi
        fi
        
        # Try to install bc
        if [[ "$AUTO_INSTALL_DEPS" == "true" ]]; then
            echo "Attempting to install bc..."
            
            # Try apt (Debian/Ubuntu)
            if command -v apt &> /dev/null; then
                echo "Installing bc using apt..."
                sudo apt update && sudo apt install -y bc
            # Try yum (CentOS/RHEL)
            elif command -v yum &> /dev/null; then
                echo "Installing bc using yum..."
                sudo yum install -y bc
            # Try brew (macOS)
            elif command -v brew &> /dev/null; then
                echo "Installing bc using brew..."
                brew install bc
            # Try apk (Alpine)
            elif command -v apk &> /dev/null; then
                echo "Installing bc using apk..."
                apk add --no-cache bc
            else
                echo "ERROR: Could not install bc automatically."
                echo "Please install bc manually and run the script again."
                exit 1
            fi
            
            # Verify installation
            if ! command -v bc &> /dev/null; then
                echo "ERROR: Failed to install bc. Please install manually and try again."
                exit 1
            fi
            
            echo "Successfully installed bc."
        else
            echo "ERROR: The 'bc' command is required for performance calculations."
            echo "Please install bc manually or run with --auto-install-deps to attempt automatic installation."
            exit 1
        fi
    fi
    
    # Check for AWS CLI
    if ! command -v aws &> /dev/null; then
        echo "WARNING: AWS CLI not found. The script may not work correctly."
        
        # If auto install not enabled, prompt the user
        if [[ "$AUTO_INSTALL_DEPS" != "true" ]]; then
            read -p "Would you like to automatically install AWS CLI? (y/n): " install_aws
            if [[ "$install_aws" =~ ^[Yy]$ ]]; then
                AUTO_INSTALL_DEPS=true
            fi
        fi
        
        if [[ "$AUTO_INSTALL_DEPS" == "true" ]]; then
            echo "Attempting to install AWS CLI..."
            
            # Try apt (Debian/Ubuntu)
            if command -v apt &> /dev/null; then
                echo "Installing AWS CLI using apt..."
                sudo apt update && sudo apt install -y awscli
            # Try pip (cross-platform)
            elif command -v pip3 &> /dev/null; then
                echo "Installing AWS CLI using pip3..."
                pip3 install --user awscli
            elif command -v pip &> /dev/null; then
                echo "Installing AWS CLI using pip..."
                pip install --user awscli
            else
                echo "WARNING: Could not install AWS CLI automatically."
                echo "The script will try to continue, but may fail later."
            fi
            
            # Check if AWS CLI is now available
            if command -v aws &> /dev/null; then
                echo "Successfully installed AWS CLI."
            else
                echo "WARNING: AWS CLI installation failed. The script will try to continue, but may fail."
            fi
        else
            echo "Continuing without AWS CLI. Some features may not work correctly."
        fi
    fi
    
    return 0
}

# Default settings
OUTPUT_DIR="scripts/binance_vision_api_aws_s3/reports"
DATA_STORE_DIR="scripts/binance_vision_api_aws_s3/historical_data/earliest_dates"
PERF_LOG="${OUTPUT_DIR}/performance.log"  # Performance log file
MAX_PARALLEL=100
TIMEOUT=30
VERBOSE=true
DEBUG=false
TEST_MODE=false
USE_DATA_STORE=true  # Whether to use the historical data store
AUTO_INSTALL_DEPS=false  # Whether to automatically install missing dependencies
MARKETS=("spot" "um" "cm")
DEFAULT_INTERVAL="1d"
S3_BUCKET="s3://data.binance.vision"
SHOW_PERFORMANCE=true  # Whether to show performance stats

# ANSI color codes
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
RED='\033[0;31m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m' # No Color

# Performance tracking variables
declare -A time_markers start_times
declare -A operation_durations
total_start_time=$(date +%s.%N)

###########################################
# Performance Measurement Functions
###########################################

# Start timing an operation
# Args:
#   $1: Name of the operation to time
start_timing() {
    local operation="$1"
    start_times["$operation"]=$(date +%s.%N)
    if [[ "$DEBUG" == "true" ]]; then
        log_debug "Started timing for operation: $operation"
    fi
}

# End timing an operation and record the duration
# Args:
#   $1: Name of the operation that was timed
end_timing() {
    local operation="$1"
    local end_time=$(date +%s.%N)
    local start_time=${start_times["$operation"]}
    
    if [[ -z "$start_time" ]]; then
        log_warning "No start time found for operation: $operation"
        return 1
    fi
    
    local duration=$(echo "$end_time - $start_time" | bc)
    operation_durations["$operation"]=$duration
    
    if [[ "$DEBUG" == "true" ]]; then
        log_debug "Timing for $operation completed: $duration seconds"
    fi
    
    return 0
}

# Display timing information for a specific operation
# Args:
#   $1: Name of the operation
#   $2: (Optional) Description of the operation
display_operation_timing() {
    local operation="$1"
    local description="${2:-$operation}"
    
    if [[ -n "${operation_durations[$operation]}" ]]; then
        printf "${CYAN}  %-30s${NC} : ${BOLD}%7.2f${NC} seconds\n" "$description" "${operation_durations[$operation]}"
    fi
}

# Display all performance metrics in a formatted table
display_performance_summary() {
    if [[ "$SHOW_PERFORMANCE" != "true" ]]; then
        return 0
    fi
    
    local total_duration=$(echo "$(date +%s.%N) - $total_start_time" | bc)
    operation_durations["total"]=$total_duration
    
    # Create performance log directory if it doesn't exist
    local perf_dir=$(dirname "$PERF_LOG")
    mkdir -p "$perf_dir"
    
    # Write the header to the performance log file
    echo "====== PERFORMANCE STATISTICS ======" > "$PERF_LOG"
    echo "Date: $(date)" >> "$PERF_LOG"
    echo "Markets: ${MARKETS[*]}" >> "$PERF_LOG"
    echo "Data Store: $(if [[ "$USE_DATA_STORE" == "true" ]]; then echo "Enabled"; else echo "Disabled"; fi)" >> "$PERF_LOG"
    echo "Test mode: $(if [[ "$TEST_MODE" == "true" ]]; then echo "Enabled"; else echo "Disabled"; fi)" >> "$PERF_LOG"
    echo "" >> "$PERF_LOG"
    echo "Operation Durations:" >> "$PERF_LOG"
    
    # Display the performance summary
    echo ""
    print_colored "$CYAN" "====== PERFORMANCE SUMMARY ======"
    
    # Display timings for each market processed
    print_colored "$CYAN" "Market Processing Times:"
    for market in "${MARKETS[@]}"; do
        display_operation_timing "process_${market}" "Process ${market} market"
        echo "  process_${market}: ${operation_durations[process_${market}]:-N/A} seconds" >> "$PERF_LOG"
    done
    
    # Display timings for filtering operations
    print_colored "$CYAN" "Filtering Operations:"
    display_operation_timing "filtering" "Filter and combine markets"
    echo "  filtering: ${operation_durations[filtering]:-N/A} seconds" >> "$PERF_LOG"
    
    # Display other significant operations
    print_colored "$CYAN" "Other Operations:"
    display_operation_timing "setup" "Initial setup"
    display_operation_timing "combining" "Combine all markets"
    echo "  setup: ${operation_durations[setup]:-N/A} seconds" >> "$PERF_LOG"
    echo "  combining: ${operation_durations[combining]:-N/A} seconds" >> "$PERF_LOG"
    
    # Display cache statistics if available
    if [[ -n "${operation_durations[data_store_operations]}" ]]; then
        print_colored "$CYAN" "Data Store Performance:"
        display_operation_timing "data_store_operations" "Total data store operations"
        echo "  data_store_operations: ${operation_durations[data_store_operations]:-N/A} seconds" >> "$PERF_LOG"
    fi
    
    # Display overall execution time
    echo ""
    printf "${BOLD}${CYAN}  %-30s${NC} : ${BOLD}%7.2f${NC} seconds\n" "TOTAL EXECUTION TIME" "$total_duration"
    echo "  total: $total_duration seconds" >> "$PERF_LOG"
    
    # Add additional script information and recommendations
    if (( $(echo "$total_duration > 60" | bc -l) )); then
        local minutes=$(echo "$total_duration / 60" | bc)
        local seconds=$(echo "$total_duration - ($minutes * 60)" | bc)
        printf "  ${YELLOW}Total time: %d minutes and %.2f seconds${NC}\n" "$minutes" "$seconds"
        echo "  Total time: $minutes minutes and $seconds seconds" >> "$PERF_LOG"
    fi
    
    echo ""
    print_colored "$BLUE" "Performance log saved to: $PERF_LOG"
    
    return 0
}

###########################################
# Utility Functions
###########################################

# Print colored message if not redirected to a file
# Args:
#   $1: Color code to use
#   $2: Message to print
print_colored() {
    local color="$1"
    local message="$2"
    
    if [ -t 1 ]; then  # If stdout is a terminal
        echo -e "${color}${message}${NC}"
    else
        echo "${message}"
    fi
}

# Print debug message if DEBUG is enabled
# Args:
#   $1: Debug message to print
debug_log() {
    if [[ "$DEBUG" == "true" ]]; then
        echo "[DEBUG] $1" >&2
    fi
}

# Append to debug log file
# Args:
#   $1: Log message to append
log_debug() {
    [[ "$DEBUG" == "true" ]] && echo "[$(date +%T.%3N)] $1" >> "${LOG_FILE}"
}

# Display usage information and exit
usage() {
    echo "Usage: $0 [OPTIONS]"
    echo "Efficiently retrieves all available trading symbols and their earliest data date from Binance Vision."
    echo ""
    echo "Options:"
    echo "  -o, --output DIR       Output directory (default: ${OUTPUT_DIR})"
    echo "  -c, --data-store DIR   Historical data store directory (default: ${DATA_STORE_DIR})"
    echo "  -m, --markets MARKETS  Comma-separated list of markets to scan (default: spot,um,cm)"
    echo "  -p, --parallel N       Number of parallel processes (default: ${MAX_PARALLEL})"
    echo "  -i, --interval INTVL   Default interval to check for earliest date (default: ${DEFAULT_INTERVAL})"
    echo "  -d, --debug            Enable debug logging"
    echo "  -t, --test             Test mode: only process a few symbols per market"
    echo "  -s, --skip-data-store  Skip using historical data store and fetch all data fresh"
    echo "  -q, --quiet            Suppress progress information"
    echo "  --no-perf              Disable performance statistics"
    echo "  --auto-install-deps    Automatically install missing dependencies"
    echo "  -h, --help             Display this help message and exit"
    echo ""
    echo "Example: $0 --output results --markets spot,um --parallel 30"
    exit 1
}

# Parse command line arguments
parse_arguments() {
    while [[ $# -gt 0 ]]; do
        case $1 in
            -o|--output)
                OUTPUT_DIR="$2"
                shift 2
                ;;
            -c|--data-store)
                DATA_STORE_DIR="$2"
                shift 2
                ;;
            -m|--markets)
                IFS=',' read -r -a MARKETS <<< "$2"
                shift 2
                ;;
            -p|--parallel)
                MAX_PARALLEL="$2"
                shift 2
                ;;
            -i|--interval)
                DEFAULT_INTERVAL="$2"
                shift 2
                ;;
            -d|--debug)
                DEBUG=true
                shift
                ;;
            -t|--test)
                TEST_MODE=true
                shift
                ;;
            -s|--skip-data-store)
                USE_DATA_STORE=false
                shift
                ;;
            -q|--quiet)
                VERBOSE=false
                shift
                ;;
            --no-perf)
                SHOW_PERFORMANCE=false
                shift
                ;;
            --auto-install-deps)
                AUTO_INSTALL_DEPS=true
                shift
                ;;
            -h|--help)
                usage
                ;;
            *)
                echo "Unknown option: $1" >&2
                usage
                ;;
        esac
    done
}

# Set up directories for output and temporary files
# Creates necessary directory structure and log files
setup_directories() {
    # Start timing for setup operation
    start_timing "setup"
    
    mkdir -p "${OUTPUT_DIR}"
    mkdir -p "${OUTPUT_DIR}/temp"
    
    # Create a temp directory for parallel processing
    TEMP_DIR="${OUTPUT_DIR}/temp/$$"
    mkdir -p "${TEMP_DIR}"
    
    # Create data store directory if it doesn't exist
    mkdir -p "${DATA_STORE_DIR}"
    for market in "${MARKETS[@]}"; do
        mkdir -p "${DATA_STORE_DIR}/${market}"
    done
    
    if [[ "$VERBOSE" == "true" ]]; then
        print_colored "$BLUE" "Output will be saved to ${OUTPUT_DIR}"
        if [[ "$USE_DATA_STORE" != "true" ]]; then
            print_colored "$YELLOW" "Historical data store is disabled for this run"
        else
            print_colored "$BLUE" "Using historical data from ${DATA_STORE_DIR}"
        fi
    fi
    
    # Create a log file for debugging
    LOG_FILE="${OUTPUT_DIR}/debug_log_$$.txt"
    if [[ "$DEBUG" == "true" ]]; then
        echo "Debug log file: ${LOG_FILE}" >&2
        echo "====== DEBUG LOG ====== $(date) ======" > "${LOG_FILE}"
    fi
    
    # End timing for setup operation
    end_timing "setup"
}

# Get S3 prefix for a market
# Args:
#   $1: Market (spot, um, cm)
#   $2: Symbol (optional)
#   $3: Interval (optional)
# Returns:
#   S3 path prefix for the specified market/symbol/interval
get_s3_prefix() {
    local market="$1"
    local symbol="$2"
    local interval="$3"
    
    local base_prefix=""
    case $market in
        spot)
            base_prefix="data/spot/daily/klines"
            ;;
        um)
            base_prefix="data/futures/um/daily/klines"
            ;;
        cm)
            base_prefix="data/futures/cm/daily/klines"
            ;;
        *)
            log_debug "Error: Unknown market $market"
            return 1
            ;;
    esac
    
    if [[ -n "$symbol" ]]; then
        base_prefix="${base_prefix}/${symbol}"
        
        if [[ -n "$interval" ]]; then
            base_prefix="${base_prefix}/${interval}"
        fi
    fi
    
    echo "$base_prefix"
}

# Run aws s3 ls command and capture results
# Args:
#   $1: S3 prefix to list
#   $2: Output file to save results
# Returns:
#   0 on success, 1 on failure
run_s3_ls() {
    local prefix="$1"
    local output_file="$2"
    
    log_debug "Running aws s3 ls --no-sign-request ${S3_BUCKET}/${prefix}/"
    
    if ! aws s3 ls --no-sign-request "${S3_BUCKET}/${prefix}/" > "$output_file" 2>/dev/null; then
        log_debug "Error: AWS S3 LS command failed for ${S3_BUCKET}/${prefix}/"
        return 1
    fi
    
    if [[ ! -s "$output_file" ]]; then
        log_debug "No results found for ${S3_BUCKET}/${prefix}/"
        return 1
    fi
    
    return 0
}

# Extract directories from S3 listing
# Args:
#   $1: Input file containing S3 listing
#   $2: Output file to save extracted directory names
# Returns:
#   Result of the operation (0 for success)
extract_dirs_from_listing() {
    local input_file="$1"
    local output_file="$2"
    
    awk '{print $2}' "$input_file" | \
        sed 's/PRE //g' | \
        sed 's/\///g' > "$output_file"
    
    return $?
}

###########################################
# Data Processing Functions
###########################################

# Get symbols from AWS S3 bucket for a market
# Args:
#   $1: Market (spot, um, cm)
# Returns:
#   0 on success, 1 on failure
fetch_symbols_for_market() {
    local market="$1"
    local output_file="${TEMP_DIR}/${market}_symbols.txt"
    local raw_listing="${TEMP_DIR}/${market}_raw_listing.txt"
    local prefix=$(get_s3_prefix "$market")
    
    if [[ -z "$prefix" ]]; then
        print_colored "$RED" "Error: Unknown market $market" >&2
        return 1
    fi
    
    if [[ "$VERBOSE" == "true" ]]; then
        print_colored "$YELLOW" "Fetching symbols for $market market..."
    fi
    
    log_debug "Fetching symbols from ${S3_BUCKET}/${prefix}/"
    
    # Use aws s3 ls to get the symbol list
    if ! run_s3_ls "$prefix" "$raw_listing"; then
        print_colored "$RED" "Error: Failed to fetch symbols for $market market" >&2
        return 1
    fi
    
    # Process the raw listing to extract symbols
    if ! extract_dirs_from_listing "$raw_listing" "$output_file"; then
        print_colored "$RED" "Error: Failed to extract symbols for $market market" >&2
        return 1
    fi
    
    # Verify the symbols file was created
    if [[ ! -f "$output_file" || ! -s "$output_file" ]]; then
        print_colored "$RED" "Error: Failed to create symbols file: $output_file" >&2
        log_debug "Error: Symbols file not created: $output_file"
        return 1
    fi
    
    local count=$(wc -l < "$output_file")
    if [[ "$VERBOSE" == "true" ]]; then
        print_colored "$GREEN" "Found $count symbols for $market market"
    fi
    
    log_debug "Found $count symbols for $market market in $output_file"
    
    rm -f "$raw_listing"
    return 0
}

# Get available intervals for a symbol
# Args:
#   $1: Market (spot, um, cm)
#   $2: Symbol
# Outputs:
#   List of intervals to stdout
# Returns:
#   0 on success, 1 on failure
get_intervals() {
    local market="$1"
    local symbol="$2"
    local temp_file="${TEMP_DIR}/${market}_${symbol}_intervals.txt"
    local raw_file="${temp_file}.raw"
    local prefix=$(get_s3_prefix "$market" "$symbol")
    
    if [[ -z "$prefix" ]]; then
        log_debug "Error: Could not determine S3 prefix for $market/$symbol"
        return 1
    fi
    
    log_debug "Getting intervals for ${market}/${symbol} from ${S3_BUCKET}/${prefix}/"
    
    # Use aws s3 ls to get the intervals
    if ! run_s3_ls "$prefix" "$raw_file"; then
        log_debug "Error getting intervals for ${market}/${symbol}"
        return 1
    fi
    
    # Extract intervals
    if ! extract_dirs_from_listing "$raw_file" "$temp_file"; then
        log_debug "Error extracting intervals for ${market}/${symbol}"
        rm -f "$raw_file"
        return 1
    fi
    
    cat "$temp_file"
    rm -f "$temp_file" "$raw_file"
    return 0
}

# Find earliest date for a symbol and interval
# Args:
#   $1: Market (spot, um, cm)
#   $2: Symbol
#   $3: Interval
# Outputs:
#   Earliest date to stdout (YYYY-MM-DD)
# Returns:
#   0 on success, 1 on failure
find_earliest_date() {
    local market="$1"
    local symbol="$2"
    local interval="$3"
    local temp_file="${TEMP_DIR}/${market}_${symbol}_${interval}_dates.txt"
    local raw_file="${temp_file}.raw"
    local prefix=$(get_s3_prefix "$market" "$symbol" "$interval")
    
    if [[ -z "$prefix" ]]; then
        log_debug "Error: Could not determine S3 prefix for $market/$symbol/$interval"
        return 1
    fi
    
    log_debug "Finding earliest date for ${market}/${symbol}/${interval} from ${S3_BUCKET}/${prefix}/"
    
    # Check if directory exists
    if ! run_s3_ls "$prefix" "$raw_file"; then
        log_debug "No data found for ${market}/${symbol}/${interval}"
        return 1
    fi
    
    # Extract dates from the files and find the earliest one
    # We're looking for the actual zip files, not the checksums
    if ! grep -v "CHECKSUM" "$raw_file" | \
         grep -o "[0-9]\{4\}-[0-9]\{2\}-[0-9]\{2\}" > "$temp_file"; then
        log_debug "No dates found in listing for ${market}/${symbol}/${interval}"
        rm -f "$raw_file" "$temp_file"
        return 1
    fi
    
    if [[ ! -s "$temp_file" ]]; then
        log_debug "Empty dates file for ${market}/${symbol}/${interval}"
        rm -f "$raw_file" "$temp_file"
        return 1
    fi
    
    earliest_date=$(sort "$temp_file" | head -1)
    log_debug "Earliest date for ${market}/${symbol}/${interval}: ${earliest_date}"
    
    rm -f "$raw_file" "$temp_file"
    echo "$earliest_date"
    return 0
}

# Process a single symbol to find its earliest date
# Args:
#   $1: Market (spot, um, cm)
#   $2: Symbol
# Creates:
#   Result file with symbol info in CSV format
# Returns:
#   0 on success
process_symbol() {
    local market="$1"
    local symbol="$2"
    local result_file="${TEMP_DIR}/${market}_${symbol}_result.txt"
    
    log_debug "Processing symbol ${market}/${symbol}"
    
    # Check if we can use the data store
    if [[ "$USE_DATA_STORE" == "true" ]] && read_from_data_store "$market" "$symbol" "$result_file"; then
        log_debug "Used historical data for ${market}/${symbol}"
        return 0
    fi
    
    # If not in data store, proceed with regular processing
    if [[ "$USE_DATA_STORE" != "true" ]]; then
        log_debug "Data store disabled, fetching data for ${market}/${symbol}"
    else
        log_debug "Fetching data for ${market}/${symbol} (not in data store)"
    fi
    
    # Check if the default interval exists for this symbol
    local intervals_file="${TEMP_DIR}/${market}_${symbol}_all_intervals.txt"
    if ! get_intervals "$market" "$symbol" > "$intervals_file"; then
        log_debug "Failed to get intervals for ${market}/${symbol}"
        echo "$market,$symbol,NO_INTERVALS_AVAILABLE,\"\"" > "$result_file"
        # Still write to data store to avoid re-fetching failed symbols
        write_to_data_store "$market" "$symbol" "$result_file"
        return 0
    fi
    
    if [[ ! -s "$intervals_file" ]]; then
        log_debug "No intervals found for ${market}/${symbol}"
        echo "$market,$symbol,NO_INTERVALS_AVAILABLE,\"\"" > "$result_file"
        write_to_data_store "$market" "$symbol" "$result_file"
        rm -f "$intervals_file"
        return 0
    fi
    
    # Read intervals into an array
    mapfile -t intervals < "$intervals_file"
    local interval_to_use="${DEFAULT_INTERVAL}"
    
    # If the default interval isn't available, use the first available one
    if ! grep -q "^${DEFAULT_INTERVAL}$" "$intervals_file"; then
        if [[ ${#intervals[@]} -gt 0 ]]; then
            interval_to_use="${intervals[0]}"
            log_debug "Using alternative interval ${interval_to_use} for ${market}/${symbol}"
        else
            log_debug "No intervals available for ${market}/${symbol}"
            echo "$market,$symbol,NO_INTERVALS_AVAILABLE,\"\"" > "$result_file"
            write_to_data_store "$market" "$symbol" "$result_file"
            rm -f "$intervals_file"
            return 0
        fi
    fi
    
    # Find the earliest date
    local earliest_date=$(find_earliest_date "$market" "$symbol" "$interval_to_use")
    
    if [[ -z "$earliest_date" ]]; then
        log_debug "No earliest date found for ${market}/${symbol}/${interval_to_use}"
        echo "$market,$symbol,NO_DATA_FOUND,\"\"" > "$result_file"
    else
        # Process all intervals and ensure there's no leading comma
        # First sort the intervals for consistency
        sort "$intervals_file" > "${intervals_file}.sorted"
        
        # Convert to comma-separated list and remove any potential trailing comma
        local all_intervals=$(tr '\n' ',' < "${intervals_file}.sorted" | sed 's/,$//')
        
        # Ensure we don't have any leading comma that could result from processing
        all_intervals=$(echo "$all_intervals" | sed 's/^,//')
        
        log_debug "Successfully processed ${market}/${symbol}: ${interval_to_use}, ${earliest_date}, intervals: ${all_intervals}"
        
        # Properly quote the intervals field for CSV
        echo "$market,$symbol,$earliest_date,\"$all_intervals\"" > "$result_file"
    fi
    
    # Update the data store with the new result
    write_to_data_store "$market" "$symbol" "$result_file"
    
    # Clean up
    rm -f "$intervals_file" "${intervals_file}.sorted" 2>/dev/null
    return 0
}

# Process all symbols for a market
# Fetches symbols, processes them, and generates earliest dates
# Args:
#   $1: Market (spot, um, cm)
# Returns:
#   0 on success, 1 on failure
process_market() {
    local market="$1"
    local symbols_file="${TEMP_DIR}/${market}_symbols.txt"
    
    # Start timing for this market's processing
    start_timing "process_${market}"
    
    # First fetch all symbols for this market
    if ! fetch_symbols_for_market "$market"; then
        print_colored "$RED" "Error: Failed to fetch symbols for $market market" >&2
        end_timing "process_${market}"
        return 1
    fi
    
    if [[ ! -f "$symbols_file" ]]; then
        print_colored "$RED" "Error: Symbols file not found: $symbols_file" >&2
        log_debug "Symbols file not found: $symbols_file"
        end_timing "process_${market}"
        return 1
    fi
    
    local total_symbols=$(wc -l < "$symbols_file")
    
    if [[ "$total_symbols" -eq 0 ]]; then
        print_colored "$YELLOW" "No symbols found for $market market" >&2
        log_debug "No symbols found in $symbols_file"
        end_timing "process_${market}"
        return 1
    fi
    
    if [[ "$VERBOSE" == "true" ]]; then
        print_colored "$BLUE" "Processing $total_symbols symbols for $market market..."
    fi
    
    log_debug "Starting to process $total_symbols symbols for $market market from $symbols_file"
    
    # For debugging, write the first few symbols to the log
    if [[ "$DEBUG" == "true" ]]; then
        echo "First 5 symbols for $market:" >> "${LOG_FILE}"
        head -5 "$symbols_file" >> "${LOG_FILE}"
    fi
    
    # Process a subset of symbols for testing if in test mode
    local max_symbols=$total_symbols
    if [[ "$TEST_MODE" == "true" ]]; then
        # In test mode, limit to processing 5 symbols per market for faster testing
        if [[ $total_symbols -gt 5 ]]; then
            max_symbols=5
            log_debug "TEST MODE: Limiting to first 5 symbols for testing"
        fi
    fi
    
    local symbol_count=0
    local progress=0
    local data_store_hits=0
    local data_store_misses=0
    
    # Start timing data store operations
    start_timing "data_store_operations"
    
    # Process symbols in parallel with controlled concurrency
    while IFS= read -r symbol; do
        # Skip empty lines
        if [[ -z "$symbol" ]]; then
            continue
        fi
        
        # In test mode, only process the first few symbols
        ((symbol_count++))
        if [[ "$TEST_MODE" == "true" && $symbol_count -gt $max_symbols ]]; then
            break
        fi
        
        # Check if symbol is in data store
        if [[ "$USE_DATA_STORE" == "true" ]] && is_in_data_store "$market" "$symbol"; then
            # Process directly from data store (not in background)
            local result_file="${TEMP_DIR}/${market}_${symbol}_result.txt"
            read_from_data_store "$market" "$symbol" "$result_file"
            ((data_store_hits++))
            ((progress++))
            
            # Update progress
            if [[ "$VERBOSE" == "true" ]]; then
                if ((progress % 50 == 0)) || ((progress == total_symbols)); then
                    percent=$((progress * 100 / total_symbols))
                    printf "${YELLOW}Progress: %3d%% (%d/%d) [Data Store: %d]${NC}\r" "$percent" "$progress" "$total_symbols" "$data_store_hits"
                fi
            fi
        else
            # Wait if we've reached max parallel processes
            while [[ $(jobs -r | wc -l) -ge $MAX_PARALLEL ]]; do
                sleep 0.5
            done
            
            # Process the symbol in the background
            process_symbol "$market" "$symbol" &
            ((data_store_misses++))
            
            # Update progress
            if [[ "$VERBOSE" == "true" ]]; then
                ((progress++))
                if ((progress % 50 == 0)) || ((progress == total_symbols)); then
                    percent=$((progress * 100 / total_symbols))
                    printf "${YELLOW}Progress: %3d%% (%d/%d) [Data Store: %d]${NC}\r" "$percent" "$progress" "$total_symbols" "$data_store_hits"
                fi
            fi
        fi
    done < "$symbols_file"
    
    # Wait for all background jobs to complete
    wait
    
    # End timing data store operations
    end_timing "data_store_operations"
    
    if [[ "$VERBOSE" == "true" ]]; then
        printf "\n"  # Add a newline after progress output
        print_colored "$GREEN" "Completed processing all symbols for $market market"
        if [[ "$USE_DATA_STORE" == "true" ]]; then
            print_colored "$GREEN" "Data store summary: $data_store_hits from store, $data_store_misses newly fetched"
        fi
    fi
    
    log_debug "Completed processing all symbols for $market market"
    log_debug "Data store stats for $market: $data_store_hits hits, $data_store_misses misses"
    
    # End timing for this market's processing
    end_timing "process_${market}"
    
    return 0
}

# Create a CSV file with header
# Args:
#   $1: File path to create
#   $2: Header line to use
# Returns:
#   0 on success, 1 on failure
create_csv_with_header() {
    local file_path="$1"
    local header="$2"
    
    echo "$header" > "$file_path"
    
    if [[ ! -f "$file_path" ]]; then
        log_debug "Error creating CSV file: $file_path"
        return 1
    fi
    
    return 0
}

# Combine results for a market
# Args:
#   $1: Market (spot, um, cm)
# Creates:
#   CSV file with combined results for the market
# Returns:
#   0 on success, 1 on failure
combine_market_results() {
    local market="$1"
    local output_file="${OUTPUT_DIR}/${market}_earliest_dates.csv"
    local header="market,symbol,earliest_date,available_intervals"
    
    log_debug "Combining results for $market market to $output_file"
    
    # Create header
    if ! create_csv_with_header "$output_file" "$header"; then
        print_colored "$RED" "Error: Failed to create output file $output_file" >&2
        return 1
    fi
    
    # Check if any result files exist
    local result_count=$(ls -1 "${TEMP_DIR}/${market}_"*"_result.txt" 2>/dev/null | wc -l)
    
    if [[ $result_count -eq 0 ]]; then
        print_colored "$YELLOW" "Warning: No result files found for $market market" >&2
        log_debug "No result files found for $market market in ${TEMP_DIR}"
        return 1
    fi
    
    # Combine all result files
    cat "${TEMP_DIR}/${market}_"*"_result.txt" | sort >> "$output_file"
    
    local count=$(wc -l < "$output_file")
    ((count--)) # Subtract header line
    
    if [[ "$VERBOSE" == "true" ]]; then
        print_colored "$GREEN" "Saved $count results for $market market to $output_file"
    fi
    
    log_debug "Saved $count results for $market market to $output_file"
    return 0
}

###########################################
# Filtering Functions
###########################################

# Extract base symbol from a trading pair
# Args:
#   $1: Symbol (e.g., BTCUSDT)
#   $2: Quote currency (e.g., USDT)
# Outputs:
#   Base symbol to stdout (e.g., BTC)
# Returns:
#   0 on success, 1 if no match found
extract_base_symbol() {
    local symbol="$1"
    local quote="$2"
    
    # Remove the quote currency suffix from the symbol for spot and um markets
    if [[ "$quote" == "USDT" && "$symbol" == *"$quote" ]]; then
        local base="${symbol%$quote}"
        log_debug "Extracted base '$base' from $symbol with quote $quote"
        echo "$base"
        return 0
    # Special case for CM market (coin-m futures) which uses patterns like BTCUSD_PERP
    elif [[ "$quote" == "USD_PERP" && "$symbol" == *"_PERP" ]]; then
        # First remove _PERP suffix
        local base="${symbol%_PERP}"
        # Then remove USD suffix for patterns like BTCUSD_PERP -> BTC
        if [[ "$base" == *"USD" ]]; then
            base="${base%USD}"
            log_debug "Extracted base '$base' from $symbol with quote $quote"
            echo "$base"
            return 0
        fi
    fi
    
    # If not matched, return empty
    log_debug "Could not extract base from $symbol with quote $quote"
    echo ""
    return 1
}

# Extract pairs with a specific quote currency from a CSV file
# Args:
#   $1: input_file - Input CSV file
#   $2: quote - Quote currency to extract (e.g. "USDT", "USD_PERP")
#   $3: output_file - Output CSV file
extract_pairs_with_quote() {
    local input_file=$1
    local quote=$2
    local output_file=$3
    
    # Check if input file exists
    if [[ ! -f "$input_file" ]]; then
        log_error "Input file does not exist: $input_file"
        return 1
    fi
    
    # Create a header for the output file, adding base_symbol column
    head -n 1 "$input_file" | awk -F, '{print $0",base_symbol"}' > "$output_file"
    
    local pattern=""
    case "$quote" in
        "USDT")
            pattern="USDT"
            ;;
        "USD_PERP")
            pattern="_PERP"
            ;;
        *)
            log_error "Unsupported quote currency: $quote"
            return 1
            ;;
    esac
    
    if [[ "$DEBUG" == "true" ]]; then
        log_debug "Extracting pairs with quote '$quote' from $input_file (matching pattern '$pattern')"
        
        # Show the first 5 matching lines for debugging
        log_debug "First 5 matching lines with pattern '$pattern' in $input_file:"
        grep "$pattern" "$input_file" | head -n 5 >> "${LOG_FILE}"
    fi
    
    # Read each line, extract base symbol, and write to output if valid
    local count=0
    
    # Process line by line instead of using awk to call extract_base_symbol
    while IFS= read -r line; do
        # Skip header
        if [[ "$line" == market* ]]; then
            continue
        fi
        
        # Extract fields, handling commas within quoted values
        # First get the market, symbol and earliest_date
        market=$(echo "$line" | cut -d',' -f1)
        symbol=$(echo "$line" | cut -d',' -f2)
        earliest_date=$(echo "$line" | cut -d',' -f3)
        
        # Get the intervals field (everything after 3rd comma)
        intervals=$(echo "$line" | sed 's/[^,]*,[^,]*,[^,]*,//')
        
        # Skip lines not matching the pattern
        if ! echo "$symbol" | grep -q "$pattern"; then
            continue
        fi
        
        # Extract base symbol from the trading pair
        local base_symbol=""
        base_symbol=$(extract_base_symbol "$symbol" "$quote")
        
        if [[ -n "$base_symbol" ]]; then
            # Write to output file with base_symbol column
            echo "$market,$symbol,$earliest_date,$intervals,$base_symbol" >> "$output_file"
            count=$((count + 1))
            
            # Debug: log first few added pairs
            if [[ "$DEBUG" == "true" && $count -le 3 ]]; then
                log_debug "Added pair: $market,$symbol -> base=$base_symbol"
            fi
        fi
    done < "$input_file"
    
    if [[ "$DEBUG" == "true" ]]; then
        log_debug "Extracted $count pairs with quote '$quote' from $input_file"
    fi
    
    if [[ $count -eq 0 ]]; then
        log_warning "No pairs with quote '$quote' found in $input_file"
    fi
    
    return 0
}

# Find common base symbols between two CSV files with base symbols in the last column
find_common_bases() {
    local file1=$1
    local file2=$2
    local output_file=$3
    local temp_dir=$4
    
    if [[ ! -f "$file1" || ! -f "$file2" ]]; then
        log_error "One or both input files do not exist: $file1, $file2"
        return 1
    fi
    
    # Extract base symbols using awk to get the last field (base_symbol)
    local bases1_file="${temp_dir}/bases1.txt"
    local bases2_file="${temp_dir}/bases2.txt"
    
    # Use awk to extract the last column (base_symbol), skip header
    awk -F, 'NR>1 {print $NF}' "$file1" | sort > "$bases1_file"
    awk -F, 'NR>1 {print $NF}' "$file2" | sort > "$bases2_file"
    
    # Debug output
    if [[ "$DEBUG" == "true" ]]; then
        log_debug "Base symbols in file1 ($file1):"
        head -n 10 "$bases1_file" | while read -r base; do
            log_debug "$base"
        done
        
        log_debug "Base symbols in file2 ($file2):"
        head -n 10 "$bases2_file" | while read -r base; do
            log_debug "$base"
        done
    fi
    
    # Find common base symbols using comm command
    local common_bases_file="${temp_dir}/common_bases.txt"
    comm -12 "$bases1_file" "$bases2_file" > "$common_bases_file"
    
    local common_count=$(wc -l < "$common_bases_file")
    
    if [[ "$DEBUG" == "true" ]]; then
        log_debug "Found $common_count common base symbols between the files:"
        head -n 10 "$common_bases_file" | while read -r base; do
            log_debug "$base"
        done
    fi
    
    # Create output file with common bases
    cp "$common_bases_file" "$output_file"
    
    return 0
}

# Create filtered CSV with symbols from multiple sources
# Args:
#   $1: Output CSV file path
#   $2: File containing base symbols to include
#   $3+: Input CSV files to extract data from
# Returns:
#   0 on success, 1 on failure
create_filtered_csv() {
    local output_file="$1"
    local bases_file="$2"
    shift 2
    local input_files=("$@")
    
    local header="market,symbol,earliest_date,available_intervals,base_symbol"
    create_csv_with_header "$output_file" "$header"
    local temp_unsorted="${output_file}.unsorted"
    
    > "$temp_unsorted"
    
    while IFS= read -r base; do
        for file in "${input_files[@]}"; do
            grep ",$base\$" "$file" >> "$temp_unsorted"
        done
    done < "$bases_file"
    
    if [[ ! -s "$temp_unsorted" ]]; then
        log_debug "No matching records found for filtered CSV"
        rm -f "$temp_unsorted"
        return 1
    fi
    
    # Sort by earliest_date
    tail -n +1 "$temp_unsorted" | sort -t, -k4,4 >> "$output_file"
    
    rm -f "$temp_unsorted"
    return 0
}

# Create a filtered list of instruments matching base symbols from a file and sorted by earliest_date
create_filtered_list() {
    local csv_file=$1
    local base_symbols_file=$2
    local output_file=$3
    local temp_dir=$4
    
    if [[ ! -f "$csv_file" || ! -f "$base_symbols_file" ]]; then
        log_error "Input file(s) not found for filtering: $csv_file $base_symbols_file"
        return 1
    fi
    
    # Create a header for the output file
    head -n 1 "$csv_file" > "$output_file"
    
    # Debug output for troubleshooting
    if [[ "$DEBUG" == "true" ]]; then
        log_debug "Filtering using base symbols from: $base_symbols_file"
        log_debug "Base symbols used for filtering (first 10):"
        head -n 10 "$base_symbols_file" | while read -r base; do
            log_debug "  - $base"
        done
        
        log_debug "Source CSV file: $csv_file (first 3 data rows):"
        awk 'NR==1 || (NR>=2 && NR<=4)' "$csv_file" | while read -r line; do
            log_debug "  $line"
        done
    fi
    
    # Create a temporary file for filtered results
    local filtered_file="${temp_dir}/filtered_temp.csv"
    : > "$filtered_file"  # Empty the file
    
    # Use awk to process the CSV file, properly handling quoted fields
    awk -F, 'NR>1 {
        # Get the last field (base_symbol)
        base = $NF;
        
        # Store the entire line
        line = $0;
        
        # Check if base symbol is in the base_symbols_file
        cmd = "grep -q \"^" base "$\" \"'$base_symbols_file'\"";
        if (system(cmd) == 0) {
            print line;
        }
    }' "$csv_file" > "$filtered_file"
    
    # Sort by earliest_date column (third column) and append to output
    if [[ -f "$filtered_file" ]]; then
        if [[ -s "$filtered_file" ]]; then
            # Debug the matched records
            if [[ "$DEBUG" == "true" ]]; then
                log_debug "Matched records (first 5):"
                head -n 5 "$filtered_file" | while read -r line; do
                    log_debug "  $line"
                done
            fi
            
            # Sort and append to output - sort on the 3rd field
            sort -t, -k3,3 "$filtered_file" >> "$output_file"
            record_count=$(wc -l < "$filtered_file")
            log_info "Created filtered list with $record_count records saved to $output_file"
        else
            log_warning "No matching records found for filtered CSV"
            echo "# No matching records found" >> "$output_file"
        fi
    else
        log_warning "Failed to create filtered file"
        echo "# Error: Failed to create filtered file" >> "$output_file"
    fi
    
    # Count records (excluding header)
    local record_count=$(( $(wc -l < "$output_file") - 1 ))
    return 0
}

# Create a consolidated CSV with base symbols and their availability across markets
# Args:
#   $1: Output CSV file path
#   $2: Spot market filtered CSV
#   $3: Um market filtered CSV
#   $4: Cm market filtered CSV
# Returns:
#   0 on success, 1 on failure
create_consolidated_base_symbol_file() {
    local output_file="$1"
    local spot_file="$2"
    local um_file="$3"
    local cm_file="$4"
    local temp_dir="$5"
    
    log_debug "Creating consolidated base symbol file: $output_file"
    
    # Create temporary files for each market's data
    local spot_data="${temp_dir}/spot_data.csv"
    local um_data="${temp_dir}/um_data.csv"
    local cm_data="${temp_dir}/cm_data.csv"
    local base_symbols="${temp_dir}/all_base_symbols.txt"
    
    # Extract data for spot market
    if [[ -f "$spot_file" ]]; then
        # Manual extraction of columns to preserve quoted fields
        while IFS= read -r line; do
            if [[ "$line" == market* ]]; then  # Skip header
                continue
            fi
            
            # Extract the fields we need
            local market=$(echo "$line" | cut -d',' -f1)
            local symbol=$(echo "$line" | cut -d',' -f2)
            local earliest_date=$(echo "$line" | cut -d',' -f3)
            local base_symbol=$(echo "$line" | cut -d',' -f5)
            
            # Extract intervals (4th column, may contain quotes)
            local intervals_field=$(echo "$line" | cut -d',' -f4-)
            local intervals=$(echo "$intervals_field" | cut -d',' -f1)
            
            # Output the data
            echo "$base_symbol,$symbol,$earliest_date,$intervals" >> "$spot_data"
            
        done < "$spot_file"
        log_debug "Extracted data from spot file: $(wc -l < "$spot_data") records"
    else
        touch "$spot_data"  # Create empty file if input doesn't exist
        log_debug "No spot file found or file empty, created empty spot_data file"
    fi
    
    # Extract data for um market
    if [[ -f "$um_file" ]]; then
        # Manual extraction for um market data
        while IFS= read -r line; do
            if [[ "$line" == market* ]]; then  # Skip header
                continue
            fi
            
            # Skip if not um market
            if ! echo "$line" | grep -q "^um,"; then
                continue
            fi
            
            # Extract the fields we need
            local market=$(echo "$line" | cut -d',' -f1)
            local symbol=$(echo "$line" | cut -d',' -f2)
            local earliest_date=$(echo "$line" | cut -d',' -f3)
            local base_symbol=$(echo "$line" | cut -d',' -f5)
            
            # Extract intervals (4th column, may contain quotes)
            local intervals_field=$(echo "$line" | cut -d',' -f4-)
            local intervals=$(echo "$intervals_field" | cut -d',' -f1)
            
            # Output the data
            echo "$base_symbol,$symbol,$earliest_date,$intervals" >> "$um_data"
            
        done < "$um_file"
        log_debug "Extracted um market data: $(wc -l < "$um_data") records"
    else
        touch "$um_data"
        log_debug "No um file found or file empty, created empty um_data file"
    fi
    
    # Extract data for cm market
    if [[ -f "$cm_file" ]]; then
        # Manual extraction for cm market data
        while IFS= read -r line; do
            if [[ "$line" == market* ]]; then  # Skip header
                continue
            fi
            
            # Skip if not cm market
            if ! echo "$line" | grep -q "^cm,"; then
                continue
            fi
            
            # Extract the fields we need
            local market=$(echo "$line" | cut -d',' -f1)
            local symbol=$(echo "$line" | cut -d',' -f2)
            local earliest_date=$(echo "$line" | cut -d',' -f3)
            local base_symbol=$(echo "$line" | cut -d',' -f5)
            
            # Extract intervals (4th column, may contain quotes)
            local intervals_field=$(echo "$line" | cut -d',' -f4-)
            local intervals=$(echo "$intervals_field" | cut -d',' -f1)
            
            # Output the data
            echo "$base_symbol,$symbol,$earliest_date,$intervals" >> "$cm_data"
            
        done < "$cm_file"
        log_debug "Extracted cm market data: $(wc -l < "$cm_data") records"
    else
        touch "$cm_data"
        log_debug "No cm file found or file empty, created empty cm_data file"
    fi
    
    # Get unique list of all base symbols across all markets
    cut -d',' -f1 "$spot_data" "$um_data" "$cm_data" | grep -v '^$' | sort -u > "$base_symbols"
    log_debug "Found $(wc -l < "$base_symbols") unique base symbols across all markets"
    
    # Create header for the consolidated file
    echo "base_symbol,spot_symbol,spot_earliest_date,spot_available_intervals,um_symbol,um_earliest_date,um_available_intervals,cm_symbol,cm_earliest_date,cm_available_intervals" > "$output_file"
    
    # Process each base symbol and consolidate information
    while IFS= read -r base_symbol; do
        # Variables to store data for each market
        local spot_symbol="" spot_earliest_date="" spot_available_intervals=""
        local um_symbol="" um_earliest_date="" um_available_intervals=""
        local cm_symbol="" cm_earliest_date="" cm_available_intervals=""
        
        # Get data for spot market
        if grep -q "^${base_symbol}," "$spot_data"; then
            # Get the data for this base symbol
            local spot_line=$(grep "^${base_symbol}," "$spot_data" | head -1)
            spot_symbol=$(echo "$spot_line" | cut -d',' -f2)
            spot_earliest_date=$(echo "$spot_line" | cut -d',' -f3)
            
            # Get intervals field (may contain quotes)
            spot_available_intervals=$(echo "$spot_line" | cut -d',' -f4-)
            
            log_debug "Found spot data for $base_symbol: $spot_symbol, $spot_earliest_date"
        fi
        
        # Get data for um market
        if grep -q "^${base_symbol}," "$um_data"; then
            # Get the data for this base symbol
            local um_line=$(grep "^${base_symbol}," "$um_data" | head -1)
            um_symbol=$(echo "$um_line" | cut -d',' -f2)
            um_earliest_date=$(echo "$um_line" | cut -d',' -f3)
            
            # Get intervals field (may contain quotes)
            um_available_intervals=$(echo "$um_line" | cut -d',' -f4-)
            
            log_debug "Found um data for $base_symbol: $um_symbol, $um_earliest_date"
        fi
        
        # Get data for cm market
        if grep -q "^${base_symbol}," "$cm_data"; then
            # Get the data for this base symbol
            local cm_line=$(grep "^${base_symbol}," "$cm_data" | head -1)
            cm_symbol=$(echo "$cm_line" | cut -d',' -f2)
            cm_earliest_date=$(echo "$cm_line" | cut -d',' -f3)
            
            # Get intervals field (may contain quotes)
            cm_available_intervals=$(echo "$cm_line" | cut -d',' -f4-)
            
            log_debug "Found cm data for $base_symbol: $cm_symbol, $cm_earliest_date"
        fi
        
        # Write consolidated row with proper quoting
        echo "${base_symbol},${spot_symbol},${spot_earliest_date},${spot_available_intervals},${um_symbol},${um_earliest_date},${um_available_intervals},${cm_symbol},${cm_earliest_date},${cm_available_intervals}" >> "$output_file"
        
    done < "$base_symbols"
    
    local record_count=$(( $(wc -l < "$output_file") - 1 ))
    log_info "Created consolidated base symbol file with $record_count records saved to $output_file"
    
    # Clean up temporary files
    rm -f "$spot_data" "$um_data" "$cm_data" "$base_symbols"
    
    return 0
}

# Filter CSV files for spot+um and spot+um+cm combinations
filter_and_combine_markets() {
    # Start timing for filtering operation
    start_timing "filtering"
    
    local spot_file=$1
    local um_file=$2
    local cm_file=$3
    local output_dir=$4
    local temp_dir=$5
    
    # Files for storing base symbols
    local spot_usdt_file="${temp_dir}/spot_usdt.csv"
    local um_usdt_file="${temp_dir}/um_usdt.csv"
    local cm_usd_perp_file="${temp_dir}/cm_usd_perp.csv"
    
    # Files for common base symbols
    local common_spot_um_bases="${temp_dir}/spot_um_common_bases.txt"
    local common_all_bases="${temp_dir}/all_markets_common_bases.txt"
    
    # Output files for filtered lists
    local spot_um_filtered="${output_dir}/spot_um_usdt_filtered.csv"
    local spot_um_cm_filtered="${output_dir}/spot_um_cm_filtered.csv"
    
    # New output file for consolidated base symbol data
    local consolidated_base_file="${output_dir}/consolidated_base_symbols.csv"
    
    # Extract USDT pairs from spot market
    extract_pairs_with_quote "$spot_file" "USDT" "$spot_usdt_file"
    
    # Debug: View the first few lines of the spot USDT file
    log_debug "First 3 lines of spot_usdt_file:"
    head -n 3 "$spot_usdt_file" >> "${LOG_FILE}"
    
    # Extract USDT pairs from um market
    extract_pairs_with_quote "$um_file" "USDT" "$um_usdt_file"
    
    # Debug: View the first few lines of the um USDT file
    log_debug "First 3 lines of um_usdt_file:"
    head -n 3 "$um_usdt_file" >> "${LOG_FILE}"
    
    # Get unique base symbols that exist in both spot and um USDT
    find_common_bases "$spot_usdt_file" "$um_usdt_file" "$common_spot_um_bases" "$temp_dir"
    
    # Debug: View the common bases found
    log_debug "First few common bases between spot and um:"
    cat "$common_spot_um_bases" >> "${LOG_FILE}"
    
    # Create filtered list for spot+um
    create_filtered_list "$spot_usdt_file" "$common_spot_um_bases" "$spot_um_filtered" "$temp_dir"
    
    # If cm_file is provided, also create spot+um+cm filtered list
    if [[ -n "$cm_file" && -f "$cm_file" ]]; then
        # Extract USD_PERP pairs from cm market
        extract_pairs_with_quote "$cm_file" "USD_PERP" "$cm_usd_perp_file"
        
        # Get base symbols that exist in all three markets
        find_common_bases "$common_spot_um_bases" "$cm_usd_perp_file" "$common_all_bases" "$temp_dir"
        
        # Create filtered list for spot+um+cm
        create_filtered_list "$spot_usdt_file" "$common_all_bases" "$spot_um_cm_filtered" "$temp_dir"
    fi
    
    # Create a simple consolidated CSV for testing
    echo "base_symbol,spot_symbol,spot_earliest_date,spot_available_intervals,um_symbol,um_earliest_date,um_available_intervals,cm_symbol,cm_earliest_date,cm_available_intervals" > "$consolidated_base_file"
    
    # Extract base symbols from filtered files
    if [[ -f "$spot_um_filtered" ]]; then
        # Get base symbols
        log_debug "Processing filtered file for consolidated data:"
        tail -n +2 "$spot_um_filtered" | while IFS= read -r line; do
            log_debug "Processing line: $line"
            
            # Extract fields carefully
            market=$(echo "$line" | cut -d',' -f1)
            symbol=$(echo "$line" | cut -d',' -f2)
            earliest_date=$(echo "$line" | cut -d',' -f3)
            
            # Extract the last part (base_symbol) - handle commas in quoted sections
            base_symbol=$(echo "$line" | awk -F, '{print $NF}')
            
            log_debug "Extracted base_symbol: $base_symbol"
            
            # Extract intervals (4th column, preserve quotes)
            # Get from after 3rd comma to before the last comma
            intervals=$(echo "$line" | cut -d',' -f4 | sed 's/^"//' | sed 's/"$//')
            
            # Write to consolidated base file
            echo "$base_symbol,$symbol,$earliest_date,\"$intervals\",,,,,," >> "$consolidated_base_file"
            log_debug "Added to consolidated file: $base_symbol,$symbol,$earliest_date,\"$intervals\",,,,,"
        done
    else
        log_warning "Spot+UM filtered file not found: $spot_um_filtered"
    fi
    
    local spot_um_count=$(( $(wc -l < "$spot_um_filtered") - 1 ))
    local spot_um_cm_count=0
    if [[ -f "$spot_um_cm_filtered" ]]; then
        spot_um_cm_count=$(( $(wc -l < "$spot_um_cm_filtered") - 1 ))
    fi
    
    local consolidated_count=$(( $(wc -l < "$consolidated_base_file") - 1 ))
    
    log_info "Created filtered instrument lists: $spot_um_filtered with $spot_um_count records, $spot_um_cm_filtered with $spot_um_cm_count records"
    log_info "Created consolidated base symbol file: $consolidated_base_file with $consolidated_count records"
    
    # End timing for filtering operation
    end_timing "filtering"
    
    return 0
}

###########################################
# Main Function
###########################################

# Main function to coordinate the process
# Processes all markets and creates filtered lists
main() {
    parse_arguments "$@"
    
    # Debug output for USE_DATA_STORE
    echo "DEBUG: Using data store: ${USE_DATA_STORE}"
    
    # Check dependencies before proceeding
    check_dependencies
    
    setup_directories
    
    log_debug "Starting script with settings: OUTPUT_DIR=${OUTPUT_DIR}, MAX_PARALLEL=${MAX_PARALLEL}, MARKETS=${MARKETS[@]}, USE_DATA_STORE=${USE_DATA_STORE}"
    
    # Process each market
    for market in "${MARKETS[@]}"; do
        log_debug "Starting processing for $market market"
        process_market "$market"
        combine_market_results "$market"
    done
    
    # Start timing for combining markets
    start_timing "combining"
    
    # Combine all markets
    local all_markets_file="${OUTPUT_DIR}/all_markets_earliest_dates.csv"
    create_csv_with_header "$all_markets_file" "market,symbol,earliest_date,available_intervals"
    
    for market in "${MARKETS[@]}"; do
        if [[ -f "${OUTPUT_DIR}/${market}_earliest_dates.csv" ]]; then
            # Skip the header line when appending
            tail -n +2 "${OUTPUT_DIR}/${market}_earliest_dates.csv" >> "$all_markets_file"
        else
            log_debug "Warning: ${OUTPUT_DIR}/${market}_earliest_dates.csv does not exist"
        fi
    done
    
    # End timing for combining markets
    end_timing "combining"
    
    # Create filtered lists based on special criteria
    filter_and_combine_markets "${OUTPUT_DIR}/spot_earliest_dates.csv" "${OUTPUT_DIR}/um_earliest_dates.csv" "${OUTPUT_DIR}/cm_earliest_dates.csv" "${OUTPUT_DIR}" "${TEMP_DIR}"
    
    local count=$(wc -l < "$all_markets_file")
    ((count--)) # Subtract header
    
    # Show summary
    print_colored "$GREEN" "All done! Found earliest dates for $count symbols across all markets."
    print_colored "$GREEN" "Combined results saved to ${all_markets_file}"
    print_colored "$GREEN" "Filtered lists saved to ${OUTPUT_DIR}/spot_um_usdt_filtered.csv and ${OUTPUT_DIR}/spot_um_cm_filtered.csv"
    print_colored "$GREEN" "Consolidated base symbol file saved to ${OUTPUT_DIR}/consolidated_base_symbols.csv"
    
    if [[ "$USE_DATA_STORE" != "true" ]]; then
        print_colored "$YELLOW" "Historical data store was disabled for this run"
    else
        print_colored "$GREEN" "Historical data used from ${DATA_STORE_DIR}"
    fi
    
    # Display performance summary if enabled
    display_performance_summary
    
    # In debug mode, keep temporary files for examination
    if [[ "$DEBUG" == "true" ]]; then
        print_colored "$BLUE" "Debug mode: Temporary files retained in $TEMP_DIR"
    else
        # Remove the script-specific temp directory
        rm -rf "$TEMP_DIR"
        # Also remove the parent temp directory
        rm -rf "${OUTPUT_DIR}/temp"
    fi
    
    log_debug "Script completed successfully. Found $count symbols across all markets."
    
    return 0
}

# Log an error message to log file and stderr
# Args:
#   $1: Log message to append
log_error() {
    echo "[$(date +%H:%M:%S.%3N)] ERROR: $1" >> "${LOG_FILE}"
    print_colored "$RED" "ERROR: $1" >&2
}

# Log a warning message to log file and stderr if verbose
# Args:
#   $1: Log message to append
log_warning() {
    echo "[$(date +%H:%M:%S.%3N)] WARNING: $1" >> "${LOG_FILE}"
    [[ "$VERBOSE" == "true" ]] && print_colored "$YELLOW" "WARNING: $1" >&2
}

# Log an info message to log file and stdout if verbose
# Args:
#   $1: Log message to append
log_info() {
    echo "[$(date +%H:%M:%S.%3N)] INFO: $1" >> "${LOG_FILE}"
    [[ "$VERBOSE" == "true" ]] && print_colored "$BLUE" "$1"
}

# Check if a symbol exists in data store
# Args:
#   $1: Market (spot, um, cm)
#   $2: Symbol
# Returns:
#   0 if found in data store, 1 if not found
is_in_data_store() {
    local market="$1"
    local symbol="$2"
    local data_file="${DATA_STORE_DIR}/${market}/${symbol}.csv"
    
    if [[ "$USE_DATA_STORE" != "true" ]]; then
        if [[ "$DEBUG" == "true" ]]; then
            log_debug "Data store disabled: USE_DATA_STORE=${USE_DATA_STORE}"
        fi
        return 1  # Skip data store check if flag is set
    fi
    
    if [[ -f "$data_file" ]]; then
        log_debug "Found $market/$symbol in data store: $data_file"
        return 0
    else
        log_debug "$market/$symbol not found in data store"
        return 1
    fi
}

# Read a symbol's earliest date from data store
# Args:
#   $1: Market (spot, um, cm)
#   $2: Symbol
#   $3: Output file to write the stored result
# Returns:
#   0 if successfully read from data store, 1 otherwise
read_from_data_store() {
    local market="$1"
    local symbol="$2"
    local output_file="$3"
    local data_file="${DATA_STORE_DIR}/${market}/${symbol}.csv"
    
    if [[ ! -f "$data_file" ]]; then
        log_debug "Data store miss for $market/$symbol"
        return 1
    fi
    
    # Copy data file to output
    cp "$data_file" "$output_file"
    log_debug "Data store hit for $market/$symbol (from $data_file)"
    return 0
}

# Write a symbol's results to data store
# Args:
#   $1: Market (spot, um, cm)
#   $2: Symbol
#   $3: File containing the result
# Returns:
#   0 on success, 1 on failure
write_to_data_store() {
    local market="$1"
    local symbol="$2"
    local input_file="$3"
    local data_file="${DATA_STORE_DIR}/${market}/${symbol}.csv"
    
    # Skip writing to data store if disabled
    if [[ "$USE_DATA_STORE" != "true" ]]; then
        log_debug "Skipping data store update for $market/$symbol (data store disabled)"
        return 0
    fi
    
    if [[ ! -f "$input_file" ]]; then
        log_debug "Cannot write to data store: input file $input_file not found"
        return 1
    fi
    
    # Create data store directory if it doesn't exist
    mkdir -p "${DATA_STORE_DIR}/${market}"
    
    # Write to data store
    cp "$input_file" "$data_file"
    log_debug "Updated data store for $market/$symbol ($data_file)"
    return 0
}

# Execute main function
main "$@" 