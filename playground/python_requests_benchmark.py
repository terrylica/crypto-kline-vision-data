#!/usr/bin/env python3
import requests
import sys
import os

url = "https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/1m/BTCUSDT-1m-2023-01.zip"
output_dir = "./temp_downloads"
filename = os.path.basename(url)
output_path = os.path.join(output_dir, filename)

# Check if we should discard output
discard = True
if len(sys.argv) > 1 and sys.argv[1] == "save":
    discard = False
    os.makedirs(output_dir, exist_ok=True)

if discard:
    response = requests.get(url, stream=True)
    # Read and discard content
    for chunk in response.iter_content(chunk_size=8192):
        pass
else:
    response = requests.get(url)
    with open(output_path, "wb") as f:
        f.write(response.content)
