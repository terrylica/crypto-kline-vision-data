#!/bin/bash
# Script to view and manage checksum failures

# Initialize variables
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
BASE_DIR="$( cd "$SCRIPT_DIR/../.." && pwd )"
FAILURES_DIR="${BASE_DIR}/logs/checksum_failures"
FAILURES_REGISTRY="${FAILURES_DIR}/registry.json"
FAILURES_LOG="${FAILURES_DIR}/checksum_failures.log"
OPTION=""

# Function to display usage
usage() {
    echo "Usage: $0 [options]"
    echo "Options:"
    echo "  -l, --list              List all checksum failures (default if no option provided)"
    echo "  -s, --summary           Show summary statistics of checksum failures"
    echo "  -r, --retry             Retry all failures by running the cache builder with retry option"
    echo "  -c, --clear             Clear the checksum failures registry"
    echo "  -d, --detail SYMBOL     Show detailed failures for specific symbol"
    echo "  -h, --help              Display this help message"
    exit 1
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -l|--list)
            OPTION="list"
            shift
            ;;
        -s|--summary)
            OPTION="summary"
            shift
            ;;
        -r|--retry)
            OPTION="retry"
            shift
            ;;
        -c|--clear)
            OPTION="clear"
            shift
            ;;
        -d|--detail)
            OPTION="detail"
            SYMBOL="$2"
            shift 2
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

# Default option if none provided
if [ -z "$OPTION" ]; then
    OPTION="list"
fi

# Check if registry exists
if [ ! -f "$FAILURES_REGISTRY" ] && [ "$OPTION" != "clear" ]; then
    echo "Checksum failures registry not found: $FAILURES_REGISTRY"
    echo "No failures have been recorded."
    exit 0
fi

# Function to count failures by symbol
count_failures_by_symbol() {
    # Requires jq
    if ! command -v jq &> /dev/null; then
        echo "jq is required for this operation. Please install jq."
        exit 1
    fi
    
    echo "Failures by symbol:"
    jq -r '.[].symbol' "$FAILURES_REGISTRY" | sort | uniq -c | sort -nr
}

# Function to count failures by interval
count_failures_by_interval() {
    # Requires jq
    if ! command -v jq &> /dev/null; then
        echo "jq is required for this operation. Please install jq."
        exit 1
    fi
    
    echo "Failures by interval:"
    jq -r '.[].interval' "$FAILURES_REGISTRY" | sort | uniq -c | sort -nr
}

# Function to count failures by action
count_failures_by_action() {
    # Requires jq
    if ! command -v jq &> /dev/null; then
        echo "jq is required for this operation. Please install jq."
        exit 1
    fi
    
    echo "Failures by action taken:"
    jq -r '.[].action_taken' "$FAILURES_REGISTRY" | sort | uniq -c | sort -nr
}

# Process the selected option
case $OPTION in
    list)
        echo "=== Checksum Failures Registry ==="
        echo "Registry file: $FAILURES_REGISTRY"
        echo ""
        
        if command -v jq &> /dev/null; then
            # Pretty print with jq if available
            jq '.' "$FAILURES_REGISTRY"
        else
            # Otherwise use cat
            cat "$FAILURES_REGISTRY"
        fi
        
        echo ""
        echo "Total failures: $(jq 'length' "$FAILURES_REGISTRY" 2>/dev/null || echo "unknown - install jq for exact count")"
        ;;
    
    summary)
        echo "=== Checksum Failures Summary ==="
        echo "Registry file: $FAILURES_REGISTRY"
        echo ""
        
        if command -v jq &> /dev/null; then
            # Get counts with jq
            echo "Total failures: $(jq 'length' "$FAILURES_REGISTRY")"
            echo ""
            count_failures_by_symbol
            echo ""
            count_failures_by_interval
            echo ""
            count_failures_by_action
        else
            echo "jq is required for summary statistics. Please install jq."
        fi
        ;;
    
    retry)
        echo "=== Retrying Failed Checksums ==="
        echo "Registry file: $FAILURES_REGISTRY"
        echo ""
        
        # Execute the cache builder with retry option
        echo "Executing: ${BASE_DIR}/scripts/arrow_cache/cache_builder.sh --retry-failed-checksums"
        ${BASE_DIR}/scripts/arrow_cache/cache_builder.sh --retry-failed-checksums
        ;;
    
    clear)
        echo "=== Clearing Checksum Failures Registry ==="
        
        # Ask for confirmation
        read -p "Are you sure you want to clear the checksum failures registry? (y/N) " confirm
        if [[ $confirm == [Yy]* ]]; then
            # Create a backup
            if [ -f "$FAILURES_REGISTRY" ]; then
                mkdir -p "${FAILURES_DIR}/backup"
                BACKUP_FILE="${FAILURES_DIR}/backup/registry_$(date +%Y%m%d_%H%M%S).json"
                cp "$FAILURES_REGISTRY" "$BACKUP_FILE"
                echo "Created backup at $BACKUP_FILE"
                
                # Clear the registry
                echo '[]' > "$FAILURES_REGISTRY"
                echo "Checksum failures registry cleared."
            else
                # Create an empty registry
                mkdir -p "$FAILURES_DIR"
                echo '[]' > "$FAILURES_REGISTRY"
                echo "Created empty checksum failures registry."
            fi
        else
            echo "Operation cancelled."
        fi
        ;;
    
    detail)
        echo "=== Detailed Failures for Symbol: $SYMBOL ==="
        echo "Registry file: $FAILURES_REGISTRY"
        echo ""
        
        if command -v jq &> /dev/null; then
            # Filter by symbol
            jq --arg symbol "$SYMBOL" '[.[] | select(.symbol == $symbol)]' "$FAILURES_REGISTRY"
            echo ""
            echo "Total failures for $SYMBOL: $(jq --arg symbol "$SYMBOL" '[.[] | select(.symbol == $symbol)] | length' "$FAILURES_REGISTRY")"
        else
            echo "jq is required for filtering. Please install jq."
        fi
        ;;
esac

exit 0 