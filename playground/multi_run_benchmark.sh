#!/bin/bash
# Run the benchmark multiple times to get more reliable results

ITERATIONS=5

echo "Running benchmark $ITERATIONS times..."
echo

for i in $(seq 1 $ITERATIONS); do
  echo "=== RUN $i ==="
  ./compare_download_tools.sh
  echo "======================="
  echo
done

echo "All benchmark runs completed!" 