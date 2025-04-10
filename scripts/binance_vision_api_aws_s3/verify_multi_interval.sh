#!/bin/bash

###########################################
# Dependency Checking and Installation
###########################################

# Function to check if a command is available
check_command() {
    command -v "$1" >/dev/null 2>&1
}

# Function to install package using the appropriate package manager
install_package() {
    local package="$1"
    local success=false

    echo "Attempting to install $package..."

    # Try apt-get (Debian/Ubuntu)
    if check_command apt-get; then
        echo "Installing $package using apt-get..."
        sudo apt-get update -qq && sudo apt-get install -y "$package" && success=true
    # Try yum (CentOS/RHEL)
    elif check_command yum; then
        echo "Installing $package using yum..."
        sudo yum install -y "$package" && success=true
    # Try dnf (Fedora)
    elif check_command dnf; then
        echo "Installing $package using dnf..."
        sudo dnf install -y "$package" && success=true
    # Try brew (macOS)
    elif check_command brew; then
        echo "Installing $package using brew..."
        brew install "$package" && success=true
    # Try apk (Alpine)
    elif check_command apk; then
        echo "Installing $package using apk..."
        apk add --no-cache "$package" && success=true
    fi

    # Return success/failure
    $success
}

# Check and install required dependencies
check_dependencies() {
    local missing_deps=()
    local auto_install=${1:-false}
    
    # Essential dependencies
    echo "Checking for required dependencies..."
    
    # Check for curl (required)
    if ! check_command curl; then
        echo "⚠️ Missing required dependency: curl"
        missing_deps+=("curl")
    fi
    
    # Check for aria2c (preferred but can fallback to curl)
    if ! check_command aria2c; then
        echo "⚠️ Missing preferred dependency: aria2c (will fallback to curl if not installed)"
        missing_deps+=("aria2")
    fi
    
    # Check for other utilities
    if ! check_command unzip; then
        echo "⚠️ Missing required dependency: unzip"
        missing_deps+=("unzip")
    fi
    
    if ! check_command sha256sum; then
        if ! check_command shasum; then
            echo "⚠️ Missing required dependency: sha256sum/shasum"
            missing_deps+=("coreutils") # Provides sha256sum on many systems
        fi
    fi
    
    # If missing dependencies and auto-install is enabled, try to install them
    if [ ${#missing_deps[@]} -gt 0 ]; then
        if [ "$auto_install" = true ]; then
            echo "Attempting to install missing dependencies..."
            
            for dep in "${missing_deps[@]}"; do
                install_package "$dep" || echo "Failed to install $dep automatically. Please install it manually."
            done
            
            # Re-check after installation
            check_dependencies false
        else
            echo "❌ Missing dependencies detected."
            echo "You can run the script with AUTO_INSTALL_DEPS=true to attempt automatic installation."
            echo "Alternatively, install the following packages manually:"
            for dep in "${missing_deps[@]}"; do
                echo "  - $dep"
            done
            
            # Ask user if they want to continue despite missing dependencies
            read -p "Do you want to continue anyway? (y/N): " continue_anyway
            if [[ ! "$continue_anyway" =~ ^[Yy]$ ]]; then
                echo "Aborting script execution due to missing dependencies."
                exit 1
            fi
            echo "Continuing with limited functionality..."
        fi
    else
        echo "✅ All required dependencies are installed."
    fi
}

# Replace sha256sum with a cross-platform function
get_sha256() {
    local file="$1"
    if check_command sha256sum; then
        sha256sum "$file" | cut -d' ' -f1
    elif check_command shasum; then
        shasum -a 256 "$file" | cut -d' ' -f1
    else
        echo "ERROR: No SHA256 utility available" >&2
        return 1
    fi
}

###########################################
# User Configuration - Edit values below
###########################################

#--------------------------------------
# DATA SOURCE CONFIGURATION
#--------------------------------------
# Market Type:
# - "spot" - Spot Market (all intervals available, 1s and all others)
# - "um"   - USDT-Margined Futures (all intervals except 1s, earliest ~2019-12-31 for BTCUSDT)
# - "cm"   - Coin-Margined Futures (all intervals except 1s, symbols have _PERP suffix, earliest ~2020-09-23 for BCHUSD_PERP)
MARKET_TYPE="spot"

# Trading pair to process
# - For SPOT and UM: typically like "BTCUSDT", "ETHUSDT", etc.
# - For CM: typically like "BTCUSD_PERP", "ETHUSD_PERP" or "BCHUSD_PERP", etc.
# For multiple symbols, use space-separated list: "BTCUSDT ETHUSDT XRPUSDT"
# SYMBOLS="BTCUSDT ETHUSDT BNBUSDT LTCUSDT ADAUSDT XRPUSDT EOSUSDT XLMUSDT TRXUSDT ETCUSDT ICXUSDT VETUSDT LINKUSDT ZILUSDT XMRUSDT THETAUSDT MATICUSDT ATOMUSDT FTMUSDT ALGOUSDT DOGEUSDT CHZUSDT XTZUSDT BCHUSDT KNCUSDT MANAUSDT SOLUSDT SANDUSDT CRVUSDT DOTUSDT LUNAUSDT EGLDUSDT RUNEUSDT UNIUSDT AVAXUSDT NEARUSDT AAVEUSDT FILUSDT AXSUSDT ROSEUSDT GALAUSDT ENSUSDT GMTUSDT APEUSDT OPUSDT APTUSDT SUIUSDT WLDUSDT WIFUSDT DOGSUSDT"
# SYMBOLS="XMRUSDT THETAUSDT MATICUSDT ATOMUSDT FTMUSDT ALGOUSDT DOGEUSDT CHZUSDT XTZUSDT BCHUSDT KNCUSDT MANAUSDT SOLUSDT SANDUSDT CRVUSDT DOTUSDT LUNAUSDT EGLDUSDT RUNEUSDT UNIUSDT AVAXUSDT NEARUSDT AAVEUSDT FILUSDT AXSUSDT ROSEUSDT GALAUSDT ENSUSDT GMTUSDT APEUSDT OPUSDT APTUSDT SUIUSDT WLDUSDT WIFUSDT DOGSUSDT"
# SYMBOLS="LUNAUSDT EGLDUSDT RUNEUSDT UNIUSDT AVAXUSDT NEARUSDT AAVEUSDT FILUSDT AXSUSDT ROSEUSDT GALAUSDT ENSUSDT GMTUSDT APEUSDT OPUSDT APTUSDT SUIUSDT WLDUSDT WIFUSDT DOGSUSDT"
SYMBOLS="LUNAUSDT"

# Time intervals to process (space-separated list)
# Available intervals:
# - "1s" (SPOT only)
# - "1m", "3m", "5m", "15m", "30m"
# - "1h", "2h", "4h", "6h", "8h", "12h"
# - "1d"
INTERVALS="1s 1m 3m 5m 15m 30m 1h 2h 4h 6h 8h 12h 1d"
INTERVALS="1s 1m"

# Test mode - process a shorter date range for testing
TEST_MODE=false

# Date range to process
if [ "$TEST_MODE" = true ]; then
    # Test mode - just process 3 days of data to be faster
    LATEST_DATE=$(date -d "3 days ago" +"%Y-%m-%d" 2>/dev/null || date -j -v-3d +"%Y-%m-%d" 2>/dev/null)
    EARLIEST_DATE=$(date -d "5 days ago" +"%Y-%m-%d" 2>/dev/null || date -j -v-5d +"%Y-%m-%d" 2>/dev/null)
    LATEST_DATE_AUTO=true     # Auto-detect latest available date
    EARLIEST_DATE_AUTO=false  # Use fixed earliest date for faster testing
else
    # Full mode - process all available data
    # Note: In this script, data is processed from newest to oldest (LATEST_DATE → EARLIEST_DATE)
    # LATEST_DATE is usually more recent than EARLIEST_DATE.
    LATEST_DATE="2025-03-27"  # Latest date to process from (YYYY-MM-DD) - Not used when LATEST_DATE_AUTO=true
    EARLIEST_DATE="2025-03-20"  # Earliest date to process until (YYYY-MM-DD) - Not used when EARLIEST_DATE_AUTO=true
    LATEST_DATE_AUTO=true     # Set to true to automatically find the latest available date
    EARLIEST_DATE_AUTO=true   # Set to true to automatically find the earliest available date
fi

#--------------------------------------
# RUN IDENTIFICATION
#--------------------------------------
RUN_LABEL="initial"     # User-defined label for this run (e.g., "initial", "retry1", "full")
RETRY_FROM=""           # Path to CSV file to retry from (empty for fresh run)

#--------------------------------------
# PERFORMANCE CONFIGURATION
#--------------------------------------
MAX_PARALLEL=50          # Number of parallel processes (lower for testing, increase for production)
ARIA_CONNECTIONS=1       # Number of connections per download
DOWNLOAD_TIMEOUT=30      # Download timeout in seconds
MAX_RETRIES=10           # Maximum number of download retries (increased from 3 to 10)

#--------------------------------------
# OUTPUT CONFIGURATION
#--------------------------------------
SAVE_FAILURES=true      # Whether to save failed downloads separately
SHOW_PROGRESS=true      # Whether to show progress dots
SHOW_DATES=false        # Whether to show the list of dates being processed

#--------------------------------------
# DEPENDENCY CONFIGURATION
#--------------------------------------
AUTO_INSTALL_DEPS=false  # Set to true to attempt automatic installation of missing dependencies

###########################################
# Script Configuration - Do not edit below
###########################################

# Common variables
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_DIR="${SCRIPT_DIR}/logs"
TEMP_DIR="${LOG_DIR}/temp_$(date +%Y%m%d_%H%M%S)"
RESULTS_DIR="${TEMP_DIR}/results"
TERMINATED_SYMBOLS_LOG="${LOG_DIR}/terminated_symbols.log"

# Create log directory if it doesn't exist
mkdir -p "${LOG_DIR}"

# Check for dependencies before proceeding
check_dependencies $AUTO_INSTALL_DEPS

# Validate intervals and set expected lines per interval
VALID_INTERVALS=("1s" "1m" "3m" "5m" "15m" "30m" "1h" "2h" "4h" "6h" "8h" "12h" "1d")
SPOT_VALID_INTERVALS=("1s" "1m" "3m" "5m" "15m" "30m" "1h" "2h" "4h" "6h" "8h" "12h" "1d")
UM_CM_VALID_INTERVALS=("1m" "3m" "5m" "15m" "30m" "1h" "2h" "4h" "6h" "8h" "12h" "1d")

# Function to get expected line count for an interval
get_expected_lines() {
    local interval=$1
    
    case "${interval}" in
        "1s")  echo 86400 ;;   # 60 * 60 * 24
        "1m")  echo 1440 ;;    # 60 * 24
        "3m")  echo 480 ;;     # 60 * 24 / 3
        "5m")  echo 288 ;;     # 60 * 24 / 5
        "15m") echo 96 ;;      # 60 * 24 / 15
        "30m") echo 48 ;;      # 60 * 24 / 30
        "1h")  echo 24 ;;      # 24
        "2h")  echo 12 ;;      # 24 / 2
        "4h")  echo 6 ;;       # 24 / 4
        "6h")  echo 4 ;;       # 24 / 6
        "8h")  echo 3 ;;       # 24 / 8
        "12h") echo 2 ;;       # 24 / 12
        "1d")  echo 1 ;;       # 1
        *)     echo 0 ;;       # Unknown interval
    esac
}

# Function to get base URL for a market type
get_base_url() {
    local market=$1
    local symbol=$2
    
    if [ "${market}" = "spot" ]; then
        echo "https://data.binance.vision/data/spot/daily/klines/${symbol}"
    elif [ "${market}" = "um" ]; then
        echo "https://data.binance.vision/data/futures/um/daily/klines/${symbol}"
    elif [ "${market}" = "cm" ]; then
        echo "https://data.binance.vision/data/futures/cm/daily/klines/${symbol}"
    else
        echo "Error: Invalid market type. Must be 'spot', 'um', or 'cm'." >&2
        return 1
    fi
}

# Function to get the URL for a specific date
get_file_url() {
    local base_url=$1
    local interval=$2
    local date=$3
    local suffix=${4:-}

    echo "${base_url}/${interval}/${SYMBOL}-${interval}-${date}.zip${suffix}"
}

# Function to check if a file exists via HTTP
check_file_exists() {
    local url=$1
    local status_code=$(curl -s -o /dev/null -w "%{http_code}" -I "${url}")
    
    if [ "${status_code}" = "200" ] || [ "${status_code}" = "302" ]; then
        return 0  # Success
    else
        return 1  # Failure
    fi
}

# Function to handle date manipulation (compatible with BSD and GNU date)
adjust_date() {
    local input_date=$1
    local days_offset=$2
    
    date -d "${input_date} ${days_offset} days" +"%Y-%m-%d" 2>/dev/null || \
    date -j -v"${days_offset}d" -f "%Y-%m-%d" "${input_date}" +"%Y-%m-%d" 2>/dev/null
}

# Validate intervals based on market type
validate_intervals() {
    local market=$1
    local intervals_to_check=$2
    
    local valid_list
    if [ "${market}" = "spot" ]; then
        valid_list=("${SPOT_VALID_INTERVALS[@]}")
    else
        valid_list=("${UM_CM_VALID_INTERVALS[@]}")
    fi
    
    local validated_intervals=()
    for interval in ${intervals_to_check}; do
        local is_valid=false
        for valid_interval in "${valid_list[@]}"; do
            if [ "${interval}" = "${valid_interval}" ]; then
                is_valid=true
                break
            fi
        done
        
        if [ "${is_valid}" = true ]; then
            validated_intervals+=("${interval}")
        else
            echo "Warning: Interval '${interval}' is not valid for ${market} market, skipping." >&2
        fi
    done
    
    # Return space-separated list of validated intervals
    echo "${validated_intervals[@]}"
}

# Auto-detect the latest date for LATEST_DATE if LATEST_DATE_AUTO is true
auto_detect_latest_date() {
    local market_type=$1
    local symbol=$2
    local interval=$3
    local default_date=$4
    
    echo "Auto-detecting the latest available date for ${market_type}/${symbol}/${interval}..." >&2
    
    # Get base URL
    local base_url=$(get_base_url "${market_type}" "${symbol}")
    
    # Start from today and search backward up to 10 days
    local current_date=$(date +"%Y-%m-%d")
    local max_days_back=10
    
    echo -n "Checking dates: " >&2
    
    for days_back in $(seq 0 ${max_days_back}); do
        # Calculate the date to check
        local check_date=""
        if [ ${days_back} -eq 0 ]; then
            check_date="${current_date}"
        else
            # Use direct date command with the specific format needed
            check_date=$(date -d "${current_date} -${days_back} days" +"%Y-%m-%d" 2>/dev/null || date -j -v-${days_back}d -f "%Y-%m-%d" "${current_date}" +"%Y-%m-%d" 2>/dev/null)
        fi
        
        echo -n "${check_date} " >&2
        
        # Use direct curl command
        local filename="${symbol}-${interval}-${check_date}.zip"
        local url="${base_url}/${interval}/${filename}"
        local status=$(curl -s -o /dev/null -w "%{http_code}" -I "${url}")
        
        echo -n "[$status] " >&2
        
        if [ "${status}" = "200" ] || [ "${status}" = "302" ]; then
            echo "" >&2
            echo "✅ Found latest available date: ${check_date}" >&2
            echo "${check_date}"
            return 0
        fi
        
        echo -n "→ " >&2
    done
    
    echo "" >&2
    echo "❌ Could not find recent data within last ${max_days_back} days. Symbol appears to be TERMINATED." >&2
    
    # Return special code to indicate termination
    echo "TERMINATED"
    return 2
}

# Auto-detect the earliest date if EARLIEST_DATE_AUTO is true
find_earliest_date() {
    local market_type=$1
    local symbol=$2
    local interval=$3
    
    echo "Auto-detecting the earliest available date for ${market_type}/${symbol}/${interval}..." >&2
    
    # Check if the earliest date finder script exists
    if [ ! -f "${SCRIPT_DIR}/find_earliest_data_on_bn_vision.sh" ]; then
        echo "Error: find_earliest_data_on_bn_vision.sh script not found!" >&2
        return 1
    fi
    
    # Run the finder script to get the earliest date with S3 optimization
    # Show dots for progress but keep output clean
    local earliest_date=$(bash "${SCRIPT_DIR}/find_earliest_data_on_bn_vision.sh" "${market_type}" "${symbol}" "${interval}" "--s3" "--silent" | tail -n 1)
    
    # Check if earliest date was found successfully
    if [ -z "${earliest_date}" ] || [[ "${earliest_date}" == *"Error"* ]]; then
        echo "Error: Could not determine the earliest date for ${market_type}/${symbol}/${interval}" >&2
        return 1
    fi
    
    # Return the found earliest date
    echo "${earliest_date}"
}

# Function to generate standardized file names
get_file_names() {
    local symbol=$1
    local interval=$2
    local latest_date=$3
    local earliest_date=$4
    local timestamp=$(date +%Y%m%d_%H%M%S)
    
    # Ensure chronological ordering (earliest_to_latest) for filename clarity
    local filename_earliest_date=""
    local filename_latest_date=""
    
    # Compare dates to determine which is earlier (for filename only)
    if [ "$(date -d "$latest_date" +%s 2>/dev/null || date -j -f "%Y-%m-%d" "$latest_date" +%s 2>/dev/null)" -le "$(date -d "$earliest_date" +%s 2>/dev/null || date -j -f "%Y-%m-%d" "$earliest_date" +%s 2>/dev/null)" ]; then
        filename_earliest_date="$latest_date"
        filename_latest_date="$earliest_date"
    else
        filename_earliest_date="$earliest_date"
        filename_latest_date="$latest_date"
    fi
    
    local base_name="${MARKET_TYPE}_${symbol}_${interval}_${filename_earliest_date}_to_${filename_latest_date}"
    local retry_info=""
    
    # If this is a retry run, add information about which file we're retrying from
    if [ -n "${RETRY_FROM}" ]; then
        # Extract the run label from the original file
        local original_label=$(basename "${RETRY_FROM}" | sed -E 's/.*_([^_]+)_[0-9]{8}_[0-9]{6}(_.*)?.csv/\1/')
        retry_info="_retry_from_${original_label}"
    fi
    
    echo "${LOG_DIR}/${base_name}_${RUN_LABEL}${retry_info}_${timestamp}.csv"
}

# Function to get HTTP headers
get_http_headers() {
    local url=$1
    local headers_file=$2
    
    curl -sI "${url}" > "${headers_file}"
    
    # Extract header information
    local last_modified=$(grep -i "last-modified:" "${headers_file}" | cut -d' ' -f2- | tr -d '\r\n' || echo "NA")
    local etag=$(grep -i "etag:" "${headers_file}" | cut -d' ' -f2- | tr -d '\r\n"' || echo "NA")
    local server=$(grep -i "server:" "${headers_file}" | cut -d' ' -f2- | tr -d '\r\n' || echo "NA")
    local content_type=$(grep -i "content-type:" "${headers_file}" | cut -d' ' -f2- | tr -d '\r\n' || echo "NA")
    local content_length=$(grep -i "content-length:" "${headers_file}" | cut -d' ' -f2- | tr -d '\r\n' || echo "NA")
    local x_cache=$(grep -i "x-cache:" "${headers_file}" | cut -d' ' -f2- | tr -d '\r\n' || echo "NA")
    local x_amz_cf_id=$(grep -i "x-amz-cf-id:" "${headers_file}" | cut -d' ' -f2- | tr -d '\r\n' || echo "NA")
    
    echo "$last_modified,$etag,$server,$content_type,$content_length,$x_cache,$x_amz_cf_id"
}

# Function to download file with aria2 or curl as fallback
download_file() {
    local url=$1
    local output_file=$2
    local connections=$3
    local timeout=$4
    local max_retries=$5
    
    # First check if the file exists (using HTTP HEAD request)
    local status_code=$(curl -s -o /dev/null -w "%{http_code}" -I "${url}")
    
    # If file doesn't exist (404), don't attempt to download and return special code
    if [ "${status_code}" = "404" ]; then
        echo "DEBUG: File does not exist (404): ${url}" >&2
        return 404
    fi
    
    # If server error or other issue, we may want to retry
    if [ "${status_code}" != "200" ] && [ "${status_code}" != "302" ]; then
        echo "DEBUG: Unexpected status code ${status_code} for ${url}" >&2
    fi
    
    local retry=0
    local wait_time=1  # Initial wait time in seconds
    
    while [ $retry -lt $max_retries ]; do
        # Attempt to download the file using aria2c if available, otherwise fallback to curl
        if check_command aria2c; then
            aria2c --quiet --max-connection-per-server="${connections}" \
                   --connect-timeout="${timeout}" --timeout="${timeout}" \
                   --allow-overwrite=true \
                   --auto-file-renaming=false \
                   --out="${output_file}" "${url}"
        else
            # Fallback to curl if aria2c is not available
            curl -s -L --connect-timeout "${timeout}" --max-time "${timeout}" \
                 -o "${output_file}" "${url}"
        fi
        
        local result=$?
        
        # If download was successful, return success
        if [ $result -eq 0 ]; then
            return 0
        fi
        
        # Increment retry counter
        ((retry++))
        
        # If we've reached max retries, exit with failure
        if [ $retry -ge $max_retries ]; then
            return 1
        fi
        
        # Calculate exponential backoff wait time (2^retry * base_wait_time)
        # with a small random factor to avoid thundering herd problem
        local jitter=$(( RANDOM % 1000 ))
        local wait_ms=$(( (2**retry) * 1000 + jitter ))
        local wait_sec=$(echo "scale=2; $wait_ms/1000" | bc)
        
        echo "Download failed for ${url}. Retry ${retry}/${max_retries} after ${wait_sec}s..." >&2
        
        # Sleep with millisecond precision if possible, otherwise round to seconds
        if command -v sleep.exe >/dev/null 2>&1; then
            # Windows Git Bash or similar
            sleep.exe $wait_sec
        elif command -v python3 >/dev/null 2>&1; then
            # Use Python for precise sleep
            python3 -c "import time; time.sleep($wait_sec)"
        elif command -v python >/dev/null 2>&1; then
            # Fallback to python if python3 is not available
            python -c "import time; time.sleep($wait_sec)"
        else
            # Fallback to standard sleep with rounded seconds
            sleep $(echo "$wait_sec" | awk '{printf "%d", $1+0.5}')
        fi
    done
    
    return 1  # Should not reach here, but return failure if it does
}

# Function to show progress indicator
show_progress() {
    local status=$1
    
    if [ "${SHOW_PROGRESS}" = true ]; then
        if [ "${status}" = "success" ]; then
            echo -n "."
        elif [ "${status}" = "failure" ]; then
            echo -n "!"
        elif [ "${status}" = "unzip_error" ]; then
            echo -n "x"
        fi
    fi
}

# Function to create result line
create_result_line() {
    local checksum_match=$1
    local date=$2
    local interval=$3
    local expected_checksum=$4
    local actual_checksum=$5
    local file_size=$6
    local line_count=$7
    local unique_timestamps=$8
    local first_timestamp=$9
    local last_timestamp=${10}
    local http_status=${11}
    local headers=${12}
    
    echo "${checksum_match},${date},${interval},${expected_checksum},${actual_checksum},${file_size},${line_count},${unique_timestamps},${first_timestamp},${last_timestamp},${http_status},${headers}"
}

# Function to efficiently remove directories (faster than rm -rf for large directories)
fast_remove_dir() {
    local dir_to_remove=$1
    
    # Check if directory exists
    if [ ! -d "${dir_to_remove}" ]; then
        return 0
    fi
    
    # Check if rsync is available
    if command -v rsync >/dev/null 2>&1; then
        # Create empty temp directory
        local empty_dir=$(mktemp -d)
        
        # Use rsync to efficiently delete by syncing with empty directory
        rsync -a --delete "${empty_dir}/" "${dir_to_remove}/"
        
        # Remove the now-empty directory and temp directory
        rmdir "${dir_to_remove}" "${empty_dir}"
    else
        # Fall back to rm if rsync is not available
        rm -rf "${dir_to_remove}"
    fi
}

# Function to analyze a file and write to temp file
analyze_file() {
    local date=$1
    local interval=$2
    local index=$3
    local symbol=$4
    local result_file="${RESULTS_DIR}/${symbol}_${interval}_${index}.csv"
    local url_date=$(date -d "${date}" +"%Y-%m-%d" 2>/dev/null || date -j -f "%Y-%m-%d" "${date}" +"%Y-%m-%d" 2>/dev/null)
    local filename="${symbol}-${interval}-${url_date}.zip"
    local checksum_file="${filename}.CHECKSUM"
    local base_url=$(get_base_url "${MARKET_TYPE}" "${symbol}")
    local url="${base_url}/${interval}/${filename}"
    local checksum_url="${url}.CHECKSUM"
    local work_dir="${TEMP_DIR}/work_${symbol}_${interval}_${date}"
    local expected_lines=$(get_expected_lines "${interval}")
    
    # Update global stats - increment processed files counter
    TOTAL_FILES_PROCESSED=$((TOTAL_FILES_PROCESSED+1))
    
    # Create work directory
    fast_remove_dir "${work_dir}"  # Clean up any existing directory
    mkdir -p "${work_dir}"
    chmod 777 "${work_dir}"
    
    # Change to work directory
    pushd "${work_dir}" > /dev/null || {
        echo "0,${date},${interval},DIR_ERROR,DIR_ERROR,0,0,0,,,500,NA,NA,NA,NA,NA,NA,NA" > "${result_file}"
        TOTAL_FILES_FAILED=$((TOTAL_FILES_FAILED+1))
        return 1
    }
    
    # First check if the file exists and get headers
    local headers_file="${work_dir}/headers.txt"
    local header_values="NA,NA,NA,NA,NA,NA,NA" # Default values
    
    if ! curl -sI "${url}" > "${headers_file}"; then
        echo "0,${date},${interval},NOT_FOUND,NOT_FOUND,0,0,0,,,404,${header_values}" > "${result_file}"
        TOTAL_FILES_NOTFOUND=$((TOTAL_FILES_NOTFOUND+1))
        popd > /dev/null
        fast_remove_dir "${work_dir}"
        show_progress "failure"
        return 0
    fi
    
    # Extract HTTP status code from headers
    local http_status=$(grep -i "^HTTP" "${headers_file}" | awk '{print $2}')
    
    # If file doesn't exist (404), record and exit early
    if [ "${http_status}" = "404" ]; then
        echo "DEBUG: File does not exist for ${symbol} ${interval} ${date} (HTTP 404)" >&2
        echo "0,${date},${interval},FILE_NOT_FOUND,FILE_NOT_FOUND,0,0,0,,,404,${header_values}" > "${result_file}"
        TOTAL_FILES_NOTFOUND=$((TOTAL_FILES_NOTFOUND+1))
        popd > /dev/null
        fast_remove_dir "${work_dir}"
        show_progress "failure"
        return 0
    fi
    
    # Extract HTTP headers
    header_values=$(get_http_headers "${url}" "${headers_file}")
    
    # Download files with aria2 - we already checked existence, so we can ignore 404 return code
    local dl_result=0
    download_file "${url}" "${filename}" "${ARIA_CONNECTIONS}" "${DOWNLOAD_TIMEOUT}" "${MAX_RETRIES}"
    dl_result=$?
    
    # Check download_file result code
    if [ $dl_result -eq 404 ]; then
        echo "0,${date},${interval},FILE_NOT_FOUND,FILE_NOT_FOUND,0,0,0,,,404,${header_values}" > "${result_file}"
        TOTAL_FILES_NOTFOUND=$((TOTAL_FILES_NOTFOUND+1))
        popd > /dev/null
        fast_remove_dir "${work_dir}"
        show_progress "failure"
        return 0
    elif [ $dl_result -ne 0 ]; then
        # Some other download error occurred
        echo "0,${date},${interval},DOWNLOAD_FAILED,DOWNLOAD_FAILED,0,0,0,,,${http_status},${header_values}" > "${result_file}"
        TOTAL_FILES_FAILED=$((TOTAL_FILES_FAILED+1))
        popd > /dev/null
        fast_remove_dir "${work_dir}"
        show_progress "failure"
        return 0
    fi
    
    # Download checksum file - no need for special handling as we can continue without it
    download_file "${checksum_url}" "${checksum_file}" "${ARIA_CONNECTIONS}" "${DOWNLOAD_TIMEOUT}" "1"
    local checksum_dl_result=$?
    
    # Check if main file exists
    if [ ! -f "${filename}" ]; then
        echo "0,${date},${interval},FILE_MISSING,FILE_MISSING,0,0,0,,,404,${header_values}" > "${result_file}"
        TOTAL_FILES_FAILED=$((TOTAL_FILES_FAILED+1))
        popd > /dev/null
        fast_remove_dir "${work_dir}"
        show_progress "failure"
        return 0
    fi
    
    # Calculate actual checksum
    local actual_checksum=$(get_sha256 "${filename}")
    
    # Read expected checksum
    local expected_checksum=""
    if [ -f "${checksum_file}" ]; then
        expected_checksum=$(tr -d ' \r' < "${checksum_file}" | sed "s/${filename}\$//")
    fi
    
    # Check if checksums match
    local checksum_match=0
    if [ -n "${expected_checksum}" ] && [ "${actual_checksum}" = "${expected_checksum}" ]; then
        checksum_match=1
    else
        TOTAL_FILES_CHECKSUMERROR=$((TOTAL_FILES_CHECKSUMERROR+1))
    fi
    
    # Get file size
    local file_size=0
    if [ -f "${filename}" ]; then
        file_size=$(ls -l "${filename}" | awk '{print $5}')
    fi
    
    # Extract and analyze data
    local temp_csv="${work_dir}/temp.csv"
    if ! unzip -p "${filename}" > "${temp_csv}" 2>/dev/null; then
        echo "0,${date},${interval},UNZIP_FAILED,${actual_checksum},${file_size},0,0,,,200,${header_values}" > "${result_file}"
        TOTAL_FILES_FAILED=$((TOTAL_FILES_FAILED+1))
        popd > /dev/null
        fast_remove_dir "${work_dir}"
        show_progress "unzip_error"
        return 0
    fi
    
    # Count lines and unique timestamps
    local line_count=0
    local unique_timestamps=0
    local first_timestamp=""
    local last_timestamp=""
    
    if [ -f "${temp_csv}" ]; then
        line_count=$(wc -l < "${temp_csv}")
        unique_timestamps=$(cut -d',' -f1 "${temp_csv}" | sort -u | wc -l)
        first_timestamp=$(head -n1 "${temp_csv}" | cut -d',' -f1)
        last_timestamp=$(tail -n1 "${temp_csv}" | cut -d',' -f1)
    fi
    
    # Write results to result file
    echo "${checksum_match},${date},${interval},${expected_checksum},${actual_checksum},${file_size},${line_count},${unique_timestamps},${first_timestamp},${last_timestamp},${http_status},${header_values}" > "${result_file}"
    
    # Update successful download counter
    TOTAL_FILES_DOWNLOADED=$((TOTAL_FILES_DOWNLOADED+1))
    
    # Clean up
    popd > /dev/null
    fast_remove_dir "${work_dir}"
    
    # Show progress
    if [ "${checksum_match}" = "1" ]; then
        show_progress "success"
    else
        show_progress "failure"
    fi
}

# Function to get all dates between latest and earliest
get_date_range() {
    local latest_date=$1
    local earliest_date=$2
    local current_date=""
    local target_date=""
    
    # Determine which date is later to ensure we always process from newest to oldest
    # (regardless of the order the dates are passed in)
    if [ "$(date -d "$latest_date" +%s 2>/dev/null || date -j -f "%Y-%m-%d" "$latest_date" +%s 2>/dev/null)" -ge "$(date -d "$earliest_date" +%s 2>/dev/null || date -j -f "%Y-%m-%d" "$earliest_date" +%s 2>/dev/null)" ]; then
        # latest_date is actually later than or equal to earliest_date (normal case)
        current_date="$latest_date"
        target_date="$earliest_date"
    else
        # earliest_date is actually later than latest_date (reverse case)
        echo "Note: The dates provided seem to be in reverse order (latest_date is earlier than earliest_date)." >&2
        echo "Processing from ${earliest_date} back to ${latest_date} to ensure consistent behavior." >&2
        current_date="$earliest_date"
        target_date="$latest_date"
    fi
    
    # Process dates from later date down to earlier date
    while [ "$(date -d "$current_date" +%s 2>/dev/null || date -j -f "%Y-%m-%d" "$current_date" +%s 2>/dev/null)" -ge "$(date -d "$target_date" +%s 2>/dev/null || date -j -f "%Y-%m-%d" "$target_date" +%s 2>/dev/null)" ]; do
        echo "$current_date"
        current_date=$(adjust_date "$current_date" "-1")
    done
}

# Function to extract failed dates from CSV
get_failed_dates() {
    local csv_file=$1
    local interval=$2
    
    # Skip header, get dates where checksum_match is 0 and interval matches
    if [ -n "${interval}" ]; then
        tail -n +2 "${csv_file}" | awk -F',' "\$1 == \"0\" && \$3 == \"${interval}\" {print \$2}"
    else
        tail -n +2 "${csv_file}" | awk -F',' '$1 == "0" {print $2","$3}'
    fi
}

# Function to log terminated symbols
log_terminated_symbol() {
    local market_type=$1
    local symbol=$2
    local interval=$3
    local timestamp=$(date +"%Y-%m-%d %H:%M:%S")
    
    # Create log file if it doesn't exist
    if [ ! -f "${TERMINATED_SYMBOLS_LOG}" ]; then
        echo "TIMESTAMP,MARKET,SYMBOL,INTERVAL,STATUS" > "${TERMINATED_SYMBOLS_LOG}"
        chmod 666 "${TERMINATED_SYMBOLS_LOG}"
    fi
    
    # Log the terminated symbol
    echo "${timestamp},${market_type},${symbol},${interval},TERMINATED" >> "${TERMINATED_SYMBOLS_LOG}"
    
    echo "⚠️ Symbol ${symbol}/${interval} appears to be TERMINATED by Binance. Logged to ${TERMINATED_SYMBOLS_LOG}" >&2
}

# Function to process dates for a specific interval
process_interval() {
    local interval=$1
    local latest_date=$2
    local earliest_date=$3
    local symbol=$4
    
    # Get expected line count for this interval
    local expected_line_count=$(get_expected_lines "${interval}")
    echo "Processing ${symbol} - ${interval} data (${expected_line_count} expected lines per file)"
    
    # If auto-detect is enabled for this interval, detect latest date
    if [ "${LATEST_DATE_AUTO}" = true ]; then
        local auto_latest=$(auto_detect_latest_date "${MARKET_TYPE}" "${symbol}" "${interval}" "${latest_date}" | tail -n 1)
        
        # Check if symbol is terminated
        if [ "${auto_latest}" = "TERMINATED" ]; then
            # Log the terminated symbol
            log_terminated_symbol "${MARKET_TYPE}" "${symbol}" "${interval}"
            
            # Skip further processing for this symbol and interval
            echo "Skipping further processing for ${symbol} - ${interval} (TERMINATED)"
            echo "----------------------------------------"
            return 0
        elif [ -n "${auto_latest}" ]; then
            latest_date="${auto_latest}"
        fi
    fi
    
    # If auto-detect for earliest date is enabled, detect earliest date
    if [ "${EARLIEST_DATE_AUTO}" = true ]; then
        local auto_earliest=$(find_earliest_date "${MARKET_TYPE}" "${symbol}" "${interval}")
        if [ -n "${auto_earliest}" ]; then
            earliest_date="${auto_earliest}"
        fi
    fi
    
    echo "Date range for ${symbol} - ${interval}: ${latest_date} (latest) to ${earliest_date} (earliest)"
    
    # Create main CSV file for this interval
    local main_csv=$(get_file_names "${symbol}" "${interval}" "${latest_date}" "${earliest_date}")
    local failed_csv="${main_csv%.csv}_failed.csv"
    
    # Create CSV header
    echo "checksum_match,date,interval,expected_checksum,actual_checksum,file_size,line_count,unique_timestamps,first_timestamp,last_timestamp,http_status,last_modified,etag,server,content_type,content_length,x_cache,x_amz_cf_id" > "${main_csv}"
    chmod 666 "${main_csv}"  # Ensure file is writable
    
    if [ "${SAVE_FAILURES}" = true ]; then
        echo "checksum_match,date,interval,expected_checksum,actual_checksum,file_size,line_count,unique_timestamps,first_timestamp,last_timestamp,http_status,last_modified,etag,server,content_type,content_length,x_cache,x_amz_cf_id" > "${failed_csv}"
    fi
    
    # Get all dates to process
    local dates=()
    if [ "${RETRY_MODE}" = true ]; then
        # Get failed dates for this interval from the retry file
        mapfile -t dates < <(get_failed_dates "${RETRY_FROM}" "${interval}")
        if [ ${#dates[@]} -eq 0 ]; then
            echo "No failed downloads found for ${interval} in ${RETRY_FROM}"
            return 0
        fi
    else
        # Get all dates in the specified range
        mapfile -t dates < <(get_date_range "${latest_date}" "${earliest_date}")
    fi
    
    if [ "${SHOW_DATES}" = true ]; then
        echo "Number of dates to process for ${symbol} - ${interval}: ${#dates[@]}"
        echo "Dates to process:"
        printf '%s\n' "${dates[@]}"
        echo "----------------------------------------"
    fi
    
    echo "Results will be saved to: $(basename "${main_csv}")"
    
    if [ "${SHOW_PROGRESS}" = true ]; then
        echo -n "Progress for ${symbol} - ${interval}: "
    fi
    
    # Process dates in parallel with controlled concurrency
    index=0
    for date in "${dates[@]}"; do
        # Wait if we've reached max parallel processes
        while [ $(jobs -r | wc -l) -ge ${MAX_PARALLEL} ]; do
            sleep 1
        done
        
        # Start a new background process
        analyze_file "$date" "${interval}" "$index" "${symbol}" &
        
        ((index++))
    done
    
    # Wait for all background jobs to complete
    wait
    
    # Combine results in order
    for i in $(seq 0 $((index-1))); do
        if [ -f "${RESULTS_DIR}/${symbol}_${interval}_${i}.csv" ]; then
            # Add to main results file
            cat "${RESULTS_DIR}/${symbol}_${interval}_${i}.csv" >> "${main_csv}"
            
            # If checksum_match is 0 and failures saving is enabled, add to failures file
            if [ "${SAVE_FAILURES}" = true ] && grep -q "^0," "${RESULTS_DIR}/${symbol}_${interval}_${i}.csv"; then
                cat "${RESULTS_DIR}/${symbol}_${interval}_${i}.csv" >> "${failed_csv}"
            fi
        fi
    done
    
    if [ "${SHOW_PROGRESS}" = true ]; then
        echo  # New line after progress dots
    fi
    
    echo "Completed processing ${symbol} - ${interval}. Results saved to: ${main_csv}"
    if [ "${SAVE_FAILURES}" = true ]; then
        local failure_count=$(wc -l < "${failed_csv}")
        if [ "${failure_count}" -gt 1 ]; then  # More than just the header line
            # Count specific error types
            local not_found_count=$(grep -c "FILE_NOT_FOUND" "${failed_csv}" || echo 0)
            local download_failed_count=$(grep -c "DOWNLOAD_FAILED" "${failed_csv}" || echo 0)
            local unzip_failed_count=$(grep -c "UNZIP_FAILED" "${failed_csv}" || echo 0)
            
            # Display breakdown of failure types
            echo "FAILURE SUMMARY:"
            echo "- Files not found (404): ${not_found_count}"
            echo "- Download failures: ${download_failed_count}"
            echo "- Unzip failures: ${unzip_failed_count}"
            
            # Rename failed file with the actual date range of the failed entries
            local failed_earliest=$(tail -n +2 "${failed_csv}" | sort -t',' -k2 | head -n1 | cut -d',' -f2)
            local failed_latest=$(tail -n +2 "${failed_csv}" | sort -t',' -k2 | tail -n1 | cut -d',' -f2)
            
            if [ -n "${failed_earliest}" ] && [ -n "${failed_latest}" ]; then
                local timestamp=$(date +%Y%m%d_%H%M%S)
                local new_failed_name="${LOG_DIR}/${MARKET_TYPE}_${symbol}_${interval}_${failed_earliest}_to_${failed_latest}_${RUN_LABEL}_failed_${timestamp}.csv"
                mv "${failed_csv}" "${new_failed_name}"
                echo "Failed downloads for ${symbol} - ${interval} saved to: $(basename "${new_failed_name}")"
            else
                echo "Failed downloads for ${symbol} - ${interval} saved to: $(basename "${failed_csv}")"
            fi
        else
            echo "No failures detected for ${symbol} - ${interval}! All files processed successfully."
            rm -f "${failed_csv}"  # Remove empty failures file
        fi
    fi
    
    echo "----------------------------------------"
}

# Create directories if they don't exist
mkdir -p "${LOG_DIR}" "${RESULTS_DIR}"
chmod 777 "${LOG_DIR}" "${RESULTS_DIR}"

# Global counters for tracking success/failure
TOTAL_FILES_PROCESSED=0
TOTAL_FILES_DOWNLOADED=0
TOTAL_FILES_NOTFOUND=0
TOTAL_FILES_FAILED=0
TOTAL_FILES_CHECKSUMERROR=0

# Set retry mode flag
RETRY_MODE=false
if [ -n "${RETRY_FROM}" ]; then
    RETRY_MODE=true
    if [ ! -f "${RETRY_FROM}" ]; then
        echo "Error: Retry file ${RETRY_FROM} not found."
        exit 1
    fi
    echo "Retry mode: Processing failed downloads from ${RETRY_FROM}"
fi

# Validate intervals against market type
VALIDATED_INTERVALS=$(validate_intervals "${MARKET_TYPE}" "${INTERVALS}")

if [ -z "${VALIDATED_INTERVALS}" ]; then
    echo "Error: No valid intervals to process for ${MARKET_TYPE} market."
    exit 1
fi

# Print initial script information
echo "=========================================="
echo " Multi-Symbol & Interval Verification Tool"
echo "=========================================="
echo "Market: ${MARKET_TYPE}"
echo "Symbols: ${SYMBOLS}"
echo "Valid intervals to process: ${VALIDATED_INTERVALS}"
if [ "$TEST_MODE" = true ]; then
    echo "MODE: TEST (short date range)"
else
    echo "MODE: PRODUCTION (full date range)"
fi
echo "Date Processing: From ${LATEST_DATE} (latest) to ${EARLIEST_DATE} (earliest)"
echo "Parallel processes: ${MAX_PARALLEL}"
echo "==========================================

"

# Track which symbols/intervals we processed
PROCESSED_SYMBOLS_INTERVALS=()

# Process each symbol and interval
for symbol in ${SYMBOLS}; do
    echo "Processing symbol: ${symbol}"
    echo "--------------------"
    
    # Variables for tracking termination status
    all_intervals_terminated=true
    interval_processed=false
    
    for interval in ${VALIDATED_INTERVALS}; do
        # Process this interval for the symbol
        process_interval "${interval}" "${LATEST_DATE}" "${EARLIEST_DATE}" "${symbol}"
        
        # Add to our list of processed symbols/intervals
        PROCESSED_SYMBOLS_INTERVALS+=("${symbol}/${interval}")
        
        # Check if the interval was terminated (by looking at the log file)
        if grep -q "${symbol},${interval},TERMINATED" "${TERMINATED_SYMBOLS_LOG}" 2>/dev/null; then
            # This interval was terminated
            interval_processed=true
        else
            # At least one interval for this symbol is not terminated
            all_intervals_terminated=false
            interval_processed=true
        fi
    done
    
    # If all intervals were terminated, log that entire symbol is terminated
    if [ "${all_intervals_terminated}" = true ] && [ "${interval_processed}" = true ]; then
        echo "⚠️ All intervals for symbol ${symbol} appear to be TERMINATED by Binance"
    fi
    
    echo "Completed processing symbol: ${symbol}"
    echo ""
done

# Clean up the main temporary directory
fast_remove_dir "${TEMP_DIR}"

echo ""
echo "All symbol and interval verification completed."
echo ""

# Show summary of terminated symbols if any were found
if [ -f "${TERMINATED_SYMBOLS_LOG}" ] && [ $(wc -l < "${TERMINATED_SYMBOLS_LOG}") -gt 1 ]; then
    echo "TERMINATED SYMBOLS SUMMARY:"
    echo "--------------------"
    echo "The following symbols or intervals appear to be TERMINATED by Binance Exchange:"
    tail -n +2 "${TERMINATED_SYMBOLS_LOG}" | awk -F',' '{print "- " $3 " (" $2 ", " $4 ")"}'
    echo ""
fi

# Generate and display final summary report
echo "DOWNLOAD SUMMARY:"
echo "--------------------"
echo "Total files processed: ${TOTAL_FILES_PROCESSED}"
echo "Files successfully downloaded: ${TOTAL_FILES_DOWNLOADED}"
echo "Files not found (404): ${TOTAL_FILES_NOTFOUND}"
echo "Download failures: ${TOTAL_FILES_FAILED}"
echo "Checksum errors: ${TOTAL_FILES_CHECKSUMERROR}"
echo ""

if [ ${TOTAL_FILES_NOTFOUND} -gt 0 ]; then
    echo "RECOMMENDATION:"
    echo "--------------------"
    echo "Some files were not found (404). This is expected for dates before the symbol"
    echo "started trading or dates with no data. These are not errors in the script."
    echo ""
fi

if [ ${TOTAL_FILES_FAILED} -gt 0 ]; then
    echo "RECOMMENDATION:"
    echo "--------------------"
    echo "Some downloads failed due to network or server issues. Consider:"
    echo "1. Check your network connection"
    echo "2. Reduce MAX_PARALLEL (currently ${MAX_PARALLEL})"
    echo "3. Increase DOWNLOAD_TIMEOUT (currently ${DOWNLOAD_TIMEOUT})"
    echo "4. Run again with a more focused symbol/interval list"
    echo ""
fi

echo "REFERENCE INFORMATION:"
echo "--------------------"
echo "Market Types and Data Availability:"
echo "- SPOT: All intervals available (including 1s). Earliest dates vary by symbol."
echo "- UM (USDT-Margined Futures): All intervals except 1s. Earliest ~2019-12-31 for BTCUSDT."
echo "- CM (Coin-Margined Futures): All intervals except 1s. Uses symbols like BTCUSD_PERP." 
echo "  Earliest ~2020-09-23 for BCHUSD_PERP."
echo "" 