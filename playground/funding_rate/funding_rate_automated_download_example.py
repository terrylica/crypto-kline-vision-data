#!/usr/bin/env python
"""Example demonstrating automated funding rate history download from Binance API."""

# Standard library imports
import os
import sys
from pathlib import Path

# Set up path to allow imports from parent directory
sys.path.append(str(Path(__file__).resolve().parent.parent.parent))

# Local imports - must come after path setup
from scripts.funding_rate_downloader.automate_funding_rate_download import (
    parse_arguments,
    process_all_symbols,
)
from utils.loguru_setup import logger


def run_automated_download_example():
    """Example of using the automated funding rate downloader."""

    # Override sys.argv to simulate command-line arguments
    sys.argv = [
        __file__,  # The script name
        "--symbols",
        "BTCUSDT",
        "ETHUSDT",  # Symbols to download
        "--run-once",  # Run once and exit
        "--output-dir",
        "tmp/funding_rate_automated_example",  # Custom output directory
    ]

    # Parse the simulated arguments
    args = parse_arguments()

    # Ensure the output directory exists
    os.makedirs(args.output_dir, exist_ok=True)

    # Log the configuration
    logger.info("Running automated funding rate downloader example")
    logger.info(f"Symbols: {', '.join(args.symbols)}")
    logger.info(f"Output directory: {args.output_dir}")

    # Download the data (run once)
    summary = process_all_symbols(args.symbols, args.output_dir)

    # Log summary
    success_count = sum(1 for result in summary.values() if result)
    logger.info(f"Download summary: {success_count}/{len(args.symbols)} successful")
    for symbol, success in summary.items():
        status = "Success" if success else "Failed"
        logger.info(f"  {symbol}: {status}")

    # Show example of how to run continuously
    logger.info("\nTo run the downloader continuously, you can use:")
    logger.info(
        "python -m scripts.funding_rate_downloader.automate_funding_rate_download --symbols BTCUSDT ETHUSDT --interval 60"
    )
    logger.info(
        "This would download funding rate data every 60 minutes for BTCUSDT and ETHUSDT"
    )


if __name__ == "__main__":
    run_automated_download_example()
