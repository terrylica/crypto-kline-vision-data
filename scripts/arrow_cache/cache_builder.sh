#!/bin/bash
# Script to build cache from Binance Vision API

# Initialize variables
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
BASE_DIR="$( cd "$SCRIPT_DIR/../.." && pwd )"
CSV_FILE="${BASE_DIR}/scripts/binance_vision_api_aws_s3/reports/spot_synchronal.csv"
SYMBOLS=""
INTERVALS="5m"
START_DATE="2025-04-02"
END_DATE=$(date +%Y-%m-%d)
LIMIT=""
LOG_DIR="${BASE_DIR}/logs"
LOG_FILE="${LOG_DIR}/arrow_cache_builder_$(date +%Y%m%d_%H%M%S).log"
MODE="test"  # test or production
SKIP_CHECKSUM=false
PROCEED_ON_CHECKSUM_FAILURE=false
RETRY_FAILED_CHECKSUMS=false

# Create log directory
mkdir -p "$LOG_DIR"

# Function to display usage
usage() {
    echo "Usage: $0 [options]"
    echo "Options:"
    echo "  -s, --symbols SYMBOLS      Comma-separated list of symbols (e.g., BTCUSDT,ETHUSDT)"
    echo "  -i, --intervals INTERVALS  Comma-separated list of intervals (default: 5m)"
    echo "  -f, --csv-file FILE        Path to symbols CSV file"
    echo "  -d, --start-date DATE      Start date (YYYY-MM-DD)"
    echo "  -e, --end-date DATE        End date (YYYY-MM-DD)"
    echo "  -l, --limit N              Limit to N symbols"
    echo "  -m, --mode MODE            Mode (test or production)"
    echo "  --skip-checksum            Skip checksum verification entirely"
    echo "  --proceed-on-failure       Proceed with caching even when checksum verification fails"
    echo "  --retry-failed-checksums   Retry downloading files with previously failed checksums"
    echo "  -h, --help                 Display this help message"
    exit 1
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -s|--symbols)
            SYMBOLS="$2"
            shift 2
            ;;
        -i|--intervals)
            INTERVALS="$2"
            shift 2
            ;;
        -f|--csv-file)
            CSV_FILE="$2"
            shift 2
            ;;
        -d|--start-date)
            START_DATE="$2"
            shift 2
            ;;
        -e|--end-date)
            END_DATE="$2"
            shift 2
            ;;
        -l|--limit)
            LIMIT="$2"
            shift 2
            ;;
        -m|--mode)
            MODE="$2"
            shift 2
            ;;
        --skip-checksum)
            SKIP_CHECKSUM=true
            shift
            ;;
        --proceed-on-failure)
            PROCEED_ON_CHECKSUM_FAILURE=true
            shift
            ;;
        --retry-failed-checksums)
            RETRY_FAILED_CHECKSUMS=true
            shift
            ;;
        -h|--help)
            usage
            ;;
        *)
            echo "Unknown option: $1"
            usage
            ;;
    esac
done

# Set default mode-specific options
if [ "$MODE" = "test" ]; then
    # Default test mode options
    if [ -z "$SYMBOLS" ]; then
        SYMBOLS="BTCUSDT,ETHUSDT,BNBUSDT"
    fi
    INTERVALS="5m"
    if [ -z "$LIMIT" ]; then
        LIMIT="3"
    fi
elif [ "$MODE" = "production" ]; then
    # Default production mode options
    SYMBOLS=""  # Use all symbols from CSV
    INTERVALS=""  # Use all intervals from CSV
    LIMIT=""  # No limit
else
    echo "Invalid mode: $MODE. Must be 'test' or 'production'."
    exit 1
fi

# Only check for CSV file if we need to use it (no symbols provided or in production mode)
USE_CSV=false
if [ -z "$SYMBOLS" ] || [ "$MODE" = "production" ]; then
    USE_CSV=true
    # Check if CSV file exists
    if [ ! -f "$CSV_FILE" ]; then
        echo "CSV file not found: $CSV_FILE"
        exit 1
    fi
fi

# Log setup
echo "=== Arrow Cache Builder Started at $(date) ===" | tee -a "$LOG_FILE"
echo "Mode: $MODE" | tee -a "$LOG_FILE"
echo "Symbols: ${SYMBOLS:-'From CSV'}" | tee -a "$LOG_FILE"
echo "Intervals: ${INTERVALS:-'From CSV'}" | tee -a "$LOG_FILE"
echo "Date Range: $START_DATE to $END_DATE" | tee -a "$LOG_FILE"
if [ "$USE_CSV" = true ]; then
    echo "CSV File: $CSV_FILE" | tee -a "$LOG_FILE"
fi
echo "Limit: ${LIMIT:-'No limit'}" | tee -a "$LOG_FILE"
echo "Skip Checksum: $SKIP_CHECKSUM" | tee -a "$LOG_FILE"
echo "Proceed on Checksum Failure: $PROCEED_ON_CHECKSUM_FAILURE" | tee -a "$LOG_FILE"
echo "Retry Failed Checksums: $RETRY_FAILED_CHECKSUMS" | tee -a "$LOG_FILE"
echo "Log File: $LOG_FILE" | tee -a "$LOG_FILE"
echo "" | tee -a "$LOG_FILE"

# Build Python command - always use the synchronous version
PYTHON_CMD="python -m scripts.arrow_cache.cache_builder_sync"

# Add common arguments
if [ -n "$SYMBOLS" ]; then
    PYTHON_CMD="$PYTHON_CMD --symbols $SYMBOLS"
fi

if [ -n "$INTERVALS" ]; then
    PYTHON_CMD="$PYTHON_CMD --intervals $INTERVALS"
fi

PYTHON_CMD="$PYTHON_CMD --start-date $START_DATE --end-date $END_DATE"

# Only add CSV file if we're actually using it
if [ "$USE_CSV" = true ] && [ -n "$CSV_FILE" ]; then
    PYTHON_CMD="$PYTHON_CMD --csv-file $CSV_FILE"
fi

if [ -n "$LIMIT" ]; then
    PYTHON_CMD="$PYTHON_CMD --limit $LIMIT"
fi

# Add checksum options
if [ "$SKIP_CHECKSUM" = true ]; then
    PYTHON_CMD="$PYTHON_CMD --skip-checksum"
fi

if [ "$PROCEED_ON_CHECKSUM_FAILURE" = true ]; then
    PYTHON_CMD="$PYTHON_CMD --proceed-on-checksum-failure"
fi

if [ "$RETRY_FAILED_CHECKSUMS" = true ]; then
    PYTHON_CMD="$PYTHON_CMD --retry-failed-checksums"
fi

# Add debug flag
PYTHON_CMD="$PYTHON_CMD --debug"

# Log the command
echo "Executing: $PYTHON_CMD" | tee -a "$LOG_FILE"

# Execute Python script
cd "$BASE_DIR" && $PYTHON_CMD 2>&1 | tee -a "$LOG_FILE"

# Check execution status
STATUS=$?
if [ $STATUS -eq 0 ]; then
    echo "Arrow cache building completed successfully!" | tee -a "$LOG_FILE"
else
    echo "Arrow cache building failed with status $STATUS" | tee -a "$LOG_FILE"
fi

echo "=== Arrow Cache Builder Finished at $(date) ===" | tee -a "$LOG_FILE"

exit $STATUS 