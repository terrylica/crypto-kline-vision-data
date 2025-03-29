#!/bin/bash
# Simple benchmark script to compare curl, wget, and Python tools

echo "===== CURL BENCHMARK ====="
time curl -s "https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/1m/BTCUSDT-1m-2023-01.zip" > /dev/null

echo -e "\n===== WGET BENCHMARK ====="
time wget -q "https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/1m/BTCUSDT-1m-2023-01.zip" -O /dev/null

echo -e "\n===== PYTHON REQUESTS BENCHMARK ====="
time python python_requests_benchmark.py

echo -e "\n===== PYTHON CURL_CFFI BENCHMARK ====="
time python python_curl_cffi_benchmark.py  

echo -e "\n===== PYTHON DAFT BENCHMARK ====="
time python python_daft_benchmark.py 