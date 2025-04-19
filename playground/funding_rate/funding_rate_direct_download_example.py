#!/usr/bin/env python
"""Example demonstrating direct funding rate history download from Binance API."""

import sys
from pathlib import Path

# Add project root to path for imports
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

from utils.logger_setup import logger

# Import the download function from the scripts
from scripts.funding_rate_downloader.download_funding_rate import (
    fetch_funding_rate_history,
    convert_to_csv,
    save_to_csv,
)

# No need to initialize logger with get_logger anymore


def download_funding_rate_example():
    """Example of downloading funding rate history directly from Binance API."""

    # Define symbols to download
    symbols = ["BTCUSDT", "ETHUSDT", "BNBUSDT"]

    # Create output directory
    output_dir = Path("tmp/funding_rate_example")
    output_dir.mkdir(exist_ok=True, parents=True)

    logger.info(f"Downloading funding rate history for {', '.join(symbols)}")

    results = {}

    # Process each symbol
    for symbol in symbols:
        logger.info(f"Processing {symbol}...")

        # Fetch the data from Binance API
        data = fetch_funding_rate_history(symbol=symbol, limit=100)

        if data:
            # Convert to dataframe
            df = convert_to_csv(data, symbol)

            if df is not None:
                # Save to CSV
                file_path = save_to_csv(df, symbol, output_dir=str(output_dir))

                if file_path:
                    logger.info(
                        f"Successfully downloaded and saved {symbol} funding rate to {file_path}"
                    )
                    results[symbol] = True
                else:
                    logger.error(f"Failed to save {symbol} funding rate to CSV")
                    results[symbol] = False
            else:
                logger.error(f"Failed to convert {symbol} funding rate data to CSV")
                results[symbol] = False
        else:
            logger.error(f"Failed to fetch {symbol} funding rate data")
            results[symbol] = False

    # Print summary
    logger.info("\nDownload Summary:")
    for symbol, success in results.items():
        status = "Success" if success else "Failed"
        logger.info(f"  {symbol}: {status}")

    logger.info(
        "\nCompare this direct API approach with the DataSourceManager example (funding_rate_example.py)"
    )
    logger.info(
        "The direct API method is simpler for one-off downloads but lacks caching and advanced features"
    )
    logger.info(
        "For production use, consider using the DataSourceManager approach for better robustness"
    )


if __name__ == "__main__":
    download_funding_rate_example()
