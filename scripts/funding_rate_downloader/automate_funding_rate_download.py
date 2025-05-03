#!/usr/bin/env python3

import argparse
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import httpx
import pandas as pd

from utils.config import HTTP_OK
from utils.logger_setup import logger

DEFAULT_SYMBOLS = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT"]
DEFAULT_INTERVAL_MINUTES = 60  # Default to download every hour
DEFAULT_OUTPUT_DIR = "tmp/funding_rate_history"


def fetch_funding_rate_history(
    symbol: str, limit: int = 1000
) -> Optional[List[Dict[str, Any]]]:
    """Fetch funding rate history from Binance API using httpx"""
    try:
        url = "https://fapi.binance.com/fapi/v1/fundingRate"
        params = {"symbol": symbol, "limit": limit}

        logger.info(f"Fetching funding rate history for {symbol} with limit {limit}")
        response = httpx.get(url, params=params)
        if response.status_code == HTTP_OK:
            data = response.json()
            logger.info(
                f"Successfully fetched {len(data)} funding rate records for {symbol}"
            )
            return data
        logger.error(
            f"Error fetching funding rate for {symbol}: {response.status_code} - {response.text}"
        )
        return None
    except Exception as e:
        logger.error(f"Exception fetching funding rate history for {symbol}: {e}")
        return None


def convert_to_csv(data: List[Dict[str, Any]], symbol: str) -> Optional[pd.DataFrame]:
    """Convert JSON funding rate data to CSV format"""
    if not data:
        logger.error(f"No data to convert to CSV for {symbol}")
        return None

    try:
        # Convert to DataFrame
        df = pd.DataFrame(data)

        # Convert funding time to datetime
        df["fundingTime"] = pd.to_datetime(df["fundingTime"], unit="ms")

        # Rename columns to match expected format
        df = df.rename(
            columns={
                "fundingTime": "Funding Time",
                "fundingRate": "Funding Rate",
                "markPrice": "Mark Price",
                "symbol": "Symbol",
            }
        )

        # Format the datetime as string
        df["Funding Time"] = df["Funding Time"].dt.strftime("%Y-%m-%d %H:%M:%S")

        logger.info(
            f"Successfully converted {len(df)} records to DataFrame for {symbol}"
        )
        return df
    except Exception as e:
        logger.error(f"Error converting data to CSV for {symbol}: {e}")
        return None


def save_to_csv(
    df: pd.DataFrame, symbol: str, output_dir: str = DEFAULT_OUTPUT_DIR
) -> Optional[str]:
    """Save DataFrame to CSV file with the required naming pattern"""
    try:
        output_path = Path(output_dir)
        if not output_path.exists():
            output_path.mkdir(parents=True, exist_ok=True)
            logger.info(f"Created output directory: {output_path}")

        # Format current date for filename
        today = datetime.now().strftime("%Y-%m-%d")
        filename = f"Funding Rate History_{symbol} Perpetual_{today}.csv"
        file_path = output_path / filename

        # Save to CSV
        df.to_csv(file_path, index=False)
        logger.info(f"Saved funding rate history to {file_path}")
        return str(file_path)
    except Exception as e:
        logger.error(f"Error saving CSV file for {symbol}: {e}")
        return None


def process_symbol(symbol: str, output_dir: str = DEFAULT_OUTPUT_DIR) -> bool:
    """Process a single symbol - fetch, convert and save funding rate history"""
    data = fetch_funding_rate_history(symbol=symbol, limit=1000)

    if data:
        # Convert to CSV format
        df = convert_to_csv(data, symbol)

        if df is not None:
            # Save to file
            file_path = save_to_csv(df, symbol, output_dir)
            if file_path:
                logger.info(
                    f"Successfully downloaded and saved funding rate history for {symbol} to {file_path}"
                )
                return True
            logger.error(f"Failed to save funding rate history for {symbol} to CSV")
            return False
        logger.error(f"Failed to convert funding rate data for {symbol} to CSV format")
        return False
    logger.error(f"Failed to fetch funding rate history for {symbol}")
    return False


def process_all_symbols(
    symbols: List[str], output_dir: str = DEFAULT_OUTPUT_DIR
) -> Dict[str, bool]:
    """Process multiple symbols in parallel"""
    results = [process_symbol(symbol, output_dir) for symbol in symbols]

    # Create results summary
    return {symbol: result for symbol, result in zip(symbols, results)}


def main_loop(symbols: List[str], interval_minutes: int, output_dir: str):
    """Main loop that runs indefinitely, downloading data at regular intervals"""
    while True:
        start_time = time.time()
        logger.info(
            f"Starting funding rate history download for symbols: {', '.join(symbols)}"
        )

        summary = process_all_symbols(symbols, output_dir)

        # Log summary
        success_count = sum(1 for result in summary.values() if result)
        logger.info(f"Download summary: {success_count}/{len(symbols)} successful")
        for symbol, success in summary.items():
            status = "Success" if success else "Failed"
            logger.info(f"  {symbol}: {status}")

        # Calculate time to next run
        elapsed = time.time() - start_time
        sleep_time = max(0, interval_minutes * 60 - elapsed)

        next_run = datetime.now().timestamp() + sleep_time
        next_run_str = datetime.fromtimestamp(next_run).strftime("%Y-%m-%d %H:%M:%S")

        logger.info(
            f"Next download scheduled at {next_run_str} (in {sleep_time / 60:.1f} minutes)"
        )
        time.sleep(sleep_time)


def parse_arguments():
    """Parse command-line arguments"""
    parser = argparse.ArgumentParser(
        description="Download Binance funding rate history at regular intervals"
    )
    parser.add_argument(
        "--symbols",
        nargs="+",
        default=DEFAULT_SYMBOLS,
        help=f"List of trading symbols to download (default: {DEFAULT_SYMBOLS})",
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=DEFAULT_INTERVAL_MINUTES,
        help=f"Download interval in minutes (default: {DEFAULT_INTERVAL_MINUTES})",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default=DEFAULT_OUTPUT_DIR,
        help=f"Output directory (default: {DEFAULT_OUTPUT_DIR})",
    )
    parser.add_argument(
        "--run-once",
        action="store_true",
        help="Run once and exit (default: run continuously)",
    )

    return parser.parse_args()


def main():
    args = parse_arguments()

    # Ensure output directory exists
    Path(args.output_dir).mkdir(parents=True, exist_ok=True)

    if args.run_once:
        # Run once and exit
        logger.info(
            f"Starting one-time funding rate history download for symbols: {', '.join(args.symbols)}"
        )
        summary = process_all_symbols(args.symbols, args.output_dir)

        # Log summary
        success_count = sum(1 for result in summary.values() if result)
        logger.info(f"Download summary: {success_count}/{len(args.symbols)} successful")
        for symbol, success in summary.items():
            status = "Success" if success else "Failed"
            logger.info(f"  {symbol}: {status}")
    else:
        # Run continuously
        logger.info("Starting automated funding rate history download")
        logger.info(f"Symbols: {', '.join(args.symbols)}")
        logger.info(f"Interval: {args.interval} minutes")
        logger.info(f"Output directory: {args.output_dir}")

        try:
            main_loop(args.symbols, args.interval, args.output_dir)
        except KeyboardInterrupt:
            logger.info("Keyboard interrupt received. Shutting down...")
        except Exception as e:
            logger.error(f"Unexpected error in main loop: {e}")
            raise


if __name__ == "__main__":
    main()
