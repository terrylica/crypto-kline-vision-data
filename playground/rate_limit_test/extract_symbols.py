#!/usr/bin/env python
"""Extract symbols from the CSV file and save them to a local file."""

import csv
import os
import sys
from pathlib import Path


def extract_symbols(csv_path, output_path):
    """Extract symbols from CSV and save to a file."""
    # Check if CSV exists
    if not os.path.exists(csv_path):
        print(f"CSV file not found: {csv_path}")
        return False

    symbols = []
    try:
        with open(csv_path, "r") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row["market"] == "spot":
                    symbols.append(row["symbol"])
    except Exception as e:
        print(f"Error reading CSV: {e!s}")
        return False

    # Write symbols to file
    try:
        with open(output_path, "w") as f:
            for symbol in symbols:
                f.write(f"{symbol}\n")
        print(f"Extracted {len(symbols)} symbols to {output_path}")
        return True
    except Exception as e:
        print(f"Error writing symbols file: {e!s}")
        return False


if __name__ == "__main__":
    # Set paths
    workspace_root = Path("/workspaces/raw-data-services")
    csv_path = (
        workspace_root / "scripts/binance_vision_api_aws_s3/reports/spot_synchronal.csv"
    )
    output_path = Path(__file__).parent / "symbols.txt"

    # Extract symbols
    success = extract_symbols(csv_path, output_path)
    if not success:
        sys.exit(1)
