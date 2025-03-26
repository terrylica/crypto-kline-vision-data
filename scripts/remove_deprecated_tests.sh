#!/bin/bash
# Script to remove deprecated test files after successful consolidation

set -e
echo "This script will remove the deprecated test files that have been consolidated."
echo "Make sure you have run and verified the consolidated tests first!"
echo

# List of files to remove
API_BOUNDARY_FILES=(
    "tests/api_boundary/test_api_boundary_validator.py"
    "tests/api_boundary/test_api_boundary_alignment.py"
    "tests/api_boundary/test_api_boundary_edge_cases.py"
)

MARKET_DATA_FILES=(
    "tests/interval_1s/test_market_api_integrity.py"
    "tests/interval_1s/test_market_data_structure_validation.py"
)

CACHE_FILES=(
    "tests/interval_1s/test_cache_core_functionality.py"
    "tests/interval_1s/test_cache_dsm_core_operations.py"
    "tests/interval_1s/test_dsm_vision_client_cache.py"
)

# Function to remove files with git
remove_files() {
    local files=("$@")
    local success=0
    
    for file in "${files[@]}"; do
        if [ -f "$file" ]; then
            echo "Removing: $file"
            git rm "$file"
            if [ $? -ne 0 ]; then
                echo "Error removing $file"
                success=1
            fi
        else
            echo "File not found: $file (already removed?)"
        fi
    done
    
    return $success
}

# Ask for confirmation
read -p "Are you sure you want to proceed with removal? (y/n): " confirm
if [[ "$confirm" != "y" && "$confirm" != "Y" ]]; then
    echo "Operation canceled."
    exit 0
fi

# Remove the files
echo "Removing API boundary test files..."
remove_files "${API_BOUNDARY_FILES[@]}"

echo "Removing market data test files..."
remove_files "${MARKET_DATA_FILES[@]}"

echo "Removing cache test files..."
remove_files "${CACHE_FILES[@]}"

echo
echo "Files have been removed. Please commit these changes with the message:"
echo "\"test: Remove deprecated test files after successful consolidation\""
echo
echo "Command: git commit -m \"test: Remove deprecated test files after successful consolidation\"" 