#!/bin/bash
# Simplified shell tools benchmark for downloading from Binance Vision API

set -e  # Exit on error

# Configuration
URL="https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/1m/BTCUSDT-1m-2023-01.zip"
ITERATIONS=3

echo "==================== SHELL TOOL BENCHMARK ===================="
echo "Testing URL: $URL"
echo "Iterations: $ITERATIONS"
echo "=============================================================="

# Benchmark curl
echo -e "\n--- CURL BENCHMARK ---"
for i in $(seq 1 $ITERATIONS); do
  echo "Iteration $i:"
  time curl -s "$URL" > /dev/null
  sleep 0.5
done

# Benchmark curl with progress
echo -e "\n--- CURL WITH PROGRESS BENCHMARK ---"
for i in $(seq 1 $ITERATIONS); do
  echo "Iteration $i:"
  time curl -# "$URL" > /dev/null
  sleep 0.5
done

# Benchmark wget
echo -e "\n--- WGET BENCHMARK ---"
for i in $(seq 1 $ITERATIONS); do
  echo "Iteration $i:"
  time wget -q "$URL" -O /dev/null
  sleep 0.5
done

# Benchmark Python requests
echo -e "\n--- PYTHON REQUESTS BENCHMARK ---"
for i in $(seq 1 $ITERATIONS); do
  echo "Iteration $i:"
  time ./python_requests_benchmark.py
  sleep 0.5
done

# Benchmark Python curl_cffi
echo -e "\n--- PYTHON CURL_CFFI BENCHMARK ---"
for i in $(seq 1 $ITERATIONS); do
  echo "Iteration $i:"
  time ./python_curl_cffi_benchmark.py
  sleep 0.5
done

# Benchmark Python with Daft
echo -e "\n--- PYTHON DAFT BENCHMARK ---"
for i in $(seq 1 $ITERATIONS); do
  echo "Iteration $i:"
  time ./python_daft_benchmark.py
  sleep 0.5
done

echo -e "\nBenchmark complete." 