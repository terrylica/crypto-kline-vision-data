#!/usr/bin/env python3

from datetime import datetime
from pathlib import Path

import httpx
import pandas as pd

from utils.config import HTTP_OK
from utils.loguru_setup import logger

# No need to initialize logger with get_logger anymore


def fetch_funding_rate_history(symbol="BTCUSDT", limit=1000):
    """Fetch funding rate history from Binance API using httpx"""
    try:
        url = "https://fapi.binance.com/fapi/v1/fundingRate"
        params = {"symbol": symbol, "limit": limit}

        logger.info(f"Fetching funding rate history for {symbol} with limit {limit}")
        response = httpx.get(url, params=params)
        if response.status_code == HTTP_OK:
            data = response.json()
            logger.info(f"Successfully fetched {len(data)} funding rate records")
            return data
        logger.error(f"Error fetching funding rate: {response.status_code} - {response.text}")
        return None
    except Exception as e:
        logger.error(f"Exception fetching funding rate history: {e}")
        return None


def convert_to_csv(data):
    """Convert JSON funding rate data to CSV format"""
    if not data:
        logger.error("No data to convert to CSV")
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

        logger.info(f"Successfully converted {len(df)} records to DataFrame")
        return df
    except Exception as e:
        logger.error(f"Error converting data to CSV: {e}")
        return None


def save_to_csv(df, symbol, output_dir="tmp"):
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
        return file_path
    except Exception as e:
        logger.error(f"Error saving CSV file: {e}")
        return None


def main():
    # Fetch funding rate history for BTC/USDT
    symbol = "BTCUSDT"
    logger.info(f"Starting funding rate history download for {symbol}")

    data = fetch_funding_rate_history(symbol=symbol, limit=1000)

    if data:
        # Convert to CSV format
        df = convert_to_csv(data)

        if df is not None:
            # Save to file
            file_path = save_to_csv(df, symbol)
            if file_path:
                logger.info(f"Successfully downloaded and saved funding rate history to {file_path}")
                # Display first few rows
                logger.info(f"Sample data:\n{df.head().to_string()}")
            else:
                logger.error("Failed to save funding rate history to CSV")
        else:
            logger.error("Failed to convert funding rate data to CSV format")
    else:
        logger.error("Failed to fetch funding rate history")


if __name__ == "__main__":
    main()
