#!/bin/bash
# Shell tools benchmark for downloading from Binance Vision API
# This script compares performance of curl, wget, aws cli, and Python-based shell commands

set -e  # Exit on error

# Configuration
URL="https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/1m/BTCUSDT-1m-2023-01.zip"
FILENAME=$(basename "$URL")
ITERATIONS=3
OUTPUT_DIR="./temp_downloads"
DISCARD=true  # Set to true to discard output, false to save files

# Create output directory if needed
mkdir -p "$OUTPUT_DIR"

# Utility functions
run_benchmark() {
  local tool_name=$1
  local command=$2
  local iterations=$3
  
  echo "Running $tool_name benchmark..."
  local total_time=0
  local min_time=999999
  local max_time=0
  
  for i in $(seq 1 $iterations); do
    echo "  Iteration $i/$iterations..."
    start_time=$(date +%s.%N)
    
    eval "$command"
    
    end_time=$(date +%s.%N)
    elapsed=$(echo "$end_time - $start_time" | bc)
    total_time=$(echo "$total_time + $elapsed" | bc)
    
    # Update min/max
    min_time=$(echo "$elapsed < $min_time" | bc -l)
    if [ "$min_time" -eq 1 ]; then
      min_time=$elapsed
    fi
    
    max_time=$(echo "$elapsed > $max_time" | bc -l)
    if [ "$max_time" -eq 1 ]; then
      max_time=$elapsed
    fi
    
    echo "  Completed in ${elapsed}s"
    # Small delay between iterations
    sleep 0.5
  done
  
  # Calculate average
  avg_time=$(echo "scale=4; $total_time / $iterations" | bc)
  
  echo "  Results for $tool_name:"
  echo "    Average time: ${avg_time}s"
  echo "    Min time: ${min_time}s"
  echo "    Max time: ${max_time}s"
  echo
  
  # Save results for final comparison
  results+=("$tool_name:$avg_time:$min_time:$max_time")
}

# Cleanup function
cleanup() {
  echo "Cleaning up temporary files..."
  rm -f "$OUTPUT_DIR/$FILENAME"
  if [ "$DISCARD" = false ]; then
    echo "Downloaded files kept in $OUTPUT_DIR"
  fi
}

# Array to store results
declare -a results

# Benchmark curl
if [ "$DISCARD" = true ]; then
  curl_cmd="curl -s '$URL' > /dev/null"
else
  curl_cmd="curl -s '$URL' -o '$OUTPUT_DIR/$FILENAME'"
fi
run_benchmark "curl" "$curl_cmd" $ITERATIONS

# Benchmark curl with progress meter
if [ "$DISCARD" = true ]; then
  curl_progress_cmd="curl -# '$URL' > /dev/null"
else
  curl_progress_cmd="curl -# '$URL' -o '$OUTPUT_DIR/$FILENAME'"
fi
run_benchmark "curl (with progress)" "$curl_progress_cmd" $ITERATIONS

# Benchmark wget
if [ "$DISCARD" = true ]; then
  wget_cmd="wget -q '$URL' -O /dev/null"
else
  wget_cmd="wget -q '$URL' -O '$OUTPUT_DIR/$FILENAME'"
fi
run_benchmark "wget" "$wget_cmd" $ITERATIONS

# Check if AWS CLI is available
if command -v aws &> /dev/null; then
  # Use AWS CLI to download (this won't work for Binance Vision as it requires authentication)
  # But we include it for comparison of the command structure
  if [ "$DISCARD" = true ]; then
    aws_cmd="aws s3 cp 's3://bogus-bucket/$FILENAME' /dev/null --no-cli-pager 2>/dev/null || curl -s '$URL' > /dev/null"
  else
    aws_cmd="aws s3 cp 's3://bogus-bucket/$FILENAME' '$OUTPUT_DIR/$FILENAME' --no-cli-pager 2>/dev/null || curl -s '$URL' -o '$OUTPUT_DIR/$FILENAME'"
  fi
  run_benchmark "aws cli/curl" "$aws_cmd" $ITERATIONS
fi

# Make Python scripts executable
chmod +x python_requests_benchmark.py python_curl_cffi_benchmark.py python_daft_benchmark.py 2>/dev/null || true

# Benchmark Python with requests
if [ "$DISCARD" = true ]; then
  python_requests_cmd="./python_requests_benchmark.py"
else
  python_requests_cmd="./python_requests_benchmark.py save"
fi
run_benchmark "Python requests" "$python_requests_cmd" $ITERATIONS

# Benchmark Python with curl_cffi
if [ "$DISCARD" = true ]; then
  python_curl_cffi_cmd="./python_curl_cffi_benchmark.py"
else
  python_curl_cffi_cmd="./python_curl_cffi_benchmark.py save"
fi
run_benchmark "Python curl_cffi" "$python_curl_cffi_cmd" $ITERATIONS

# Benchmark Python with Daft
if [ "$DISCARD" = true ]; then
  python_daft_cmd="./python_daft_benchmark.py"
else
  python_daft_cmd="./python_daft_benchmark.py save"
fi
run_benchmark "Python with Daft" "$python_daft_cmd" $ITERATIONS

# Print final comparison
echo "========================================================"
echo "BENCHMARK RESULTS SUMMARY"
echo "========================================================"
echo "Tool                     Average Time    Min Time    Max Time"
echo "--------------------------------------------------------"

# Sort results by average time
IFS=$'\n' sorted=($(sort -t: -k2 -n <<< "${results[*]}"))
unset IFS

# Print sorted results
for result in "${sorted[@]}"; do
  IFS=':' read -r tool avg min max <<< "$result"
  printf "%-25s %-15s %-11s %-11s\n" "$tool" "${avg}s" "${min}s" "${max}s"
done

# Calculate speedup percentages
if [ ${#sorted[@]} -gt 1 ]; then
  echo
  echo "Performance comparison (speedup vs slowest):"
  IFS=':' read -r slowest_tool slowest_avg _ _ <<< "${sorted[${#sorted[@]}-1]}"
  
  for result in "${sorted[@]}"; do
    IFS=':' read -r tool avg _ _ <<< "$result"
    if [ "$tool" != "$slowest_tool" ]; then
      speedup=$(echo "scale=1; ($slowest_avg/$avg - 1) * 100" | bc)
      echo "$tool is ${speedup}% faster than $slowest_tool"
    fi
  done
fi

# Clean up
cleanup

echo
echo "Benchmark complete." 