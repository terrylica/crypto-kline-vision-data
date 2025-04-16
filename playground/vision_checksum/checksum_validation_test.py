#!/usr/bin/env python3
"""
Test script for checksum verification with Binance Vision API.

This script tests the checksum verification fix by downloading data for a known date
where checksums are available and verifying that the checksum validation works correctly.
"""

import argparse
import asyncio
import sys
import tempfile
from datetime import datetime
from pathlib import Path

import httpx
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from core.sync.vision_constraints import get_vision_url, FileType
from utils.logger_setup import logger
from utils.validation import DataValidation
from utils.for_core.vision_checksum import verify_file_checksum

# Console for rich output
console = Console()


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Test checksum verification fix")

    parser.add_argument(
        "--symbol",
        type=str,
        default="BTCUSDT",
        help="Trading symbol to download (default: BTCUSDT)",
    )
    parser.add_argument(
        "--interval",
        type=str,
        default="1m",
        help="Kline interval (default: 1m)",
    )
    parser.add_argument(
        "--date",
        type=str,
        default="2025-04-13",
        help="Date in YYYY-MM-DD format (default: 2025-04-13)",
    )
    parser.add_argument(
        "--market-type",
        type=str,
        default="spot",
        choices=["spot", "um", "cm"],
        help="Market type: spot, um (USDT-M futures), or cm (Coin-M futures) (default: spot)",
    )

    return parser.parse_args()


async def download_file(url: str, output_path: Path) -> bool:
    """
    Download a file from the given URL.

    Args:
        url: URL to download
        output_path: Path to save the downloaded file

    Returns:
        True if download succeeded, False otherwise
    """
    try:
        async with httpx.AsyncClient(timeout=60.0) as client:
            response = await client.get(url, follow_redirects=True)
            logger.info(
                f'HTTP Request: GET {url} "{response.status_code} {response.reason_phrase}"'
            )

            if response.status_code != 200:
                logger.warning(
                    f"Failed to download file: {response.status_code} {response.reason_phrase}"
                )
                return False

            # Save to file
            with open(output_path, "wb") as f:
                f.write(response.content)

            return True
    except Exception as e:
        logger.error(f"Error downloading {url}: {e}")
        return False


async def main():
    """Main function to test checksum verification."""
    args = parse_args()

    # Parse date
    try:
        date = datetime.strptime(args.date, "%Y-%m-%d")
    except ValueError:
        console.print(
            f"Invalid date format: {args.date}. Please use YYYY-MM-DD format."
        )
        sys.exit(1)

    # Print test setup
    console.print(
        Panel(
            f"Symbol: {args.symbol}\nInterval: {args.interval}\nDate: {args.date}\nMarket: {args.market_type}",
            title="Checksum Verification Test",
            expand=True,
        )
    )

    # Generate URLs using the updated function
    data_url = get_vision_url(
        symbol=args.symbol,
        interval=args.interval,
        date=date,
        file_type=FileType.DATA,
        market_type=args.market_type,
    )

    checksum_url = get_vision_url(
        symbol=args.symbol,
        interval=args.interval,
        date=date,
        file_type=FileType.CHECKSUM,
        market_type=args.market_type,
    )

    # Display URLs
    console.print(
        Panel(
            f"Data URL: {data_url}\nChecksum URL: {checksum_url}",
            title="Download URLs",
            expand=True,
        )
    )

    # Create temporary files
    data_file = Path(tempfile.mktemp(suffix=".zip"))
    checksum_file = Path(tempfile.mktemp(suffix=".CHECKSUM"))

    # Download files
    console.print("Downloading data file...")
    data_download_success = await download_file(data_url, data_file)
    if not data_download_success:
        console.print(f"[red]Failed to download data file from {data_url}[/red]")
        sys.exit(1)

    console.print("Downloading checksum file...")
    checksum_download_success = await download_file(checksum_url, checksum_file)
    if not checksum_download_success:
        console.print(
            f"[red]Failed to download checksum file from {checksum_url}[/red]"
        )
        sys.exit(1)

    # Verify checksum
    console.print("Verifying checksum...")
    try:
        success, error_message = verify_file_checksum(data_file, checksum_file)

        if success:
            console.print("[green]✓ Checksum verification passed![/green]")
        else:
            console.print(f"[red]✗ Checksum verification failed: {error_message}[/red]")
    except Exception as e:
        console.print(f"[red]Error during checksum verification: {e}[/red]")

    # Display detailed information
    table = Table(title="Verification Details")
    table.add_column("Item", style="cyan")
    table.add_column("Value", style="white")

    # Add file info
    if data_file.exists():
        table.add_row("Data File Size", f"{data_file.stat().st_size:,} bytes")
        actual_checksum = DataValidation.calculate_checksum(data_file)
        table.add_row("Actual SHA-256", actual_checksum)

    # Add checksum file info
    if checksum_file.exists():
        with open(checksum_file, "r") as f:
            content = f.read().strip()
            table.add_row("Checksum File Content", content)

    console.print(table)

    # Clean up
    if data_file.exists():
        data_file.unlink()
    if checksum_file.exists():
        checksum_file.unlink()


if __name__ == "__main__":
    asyncio.run(main())
