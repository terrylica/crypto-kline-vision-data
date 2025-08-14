#!/usr/bin/env python3
"""
Demo script for downloading and validating data files from Binance using checksums.

This script demonstrates how to:
1. Download data files from Binance Vision API
2. Download and parse checksum files
3. Verify file integrity using SHA-256 checksums
4. Implement robust error handling and logging
"""

import argparse
import sys
import tempfile
import time
from pathlib import Path
from typing import Any

import httpx
from rich import print as rprint
from rich.console import Console
from rich.panel import Panel
from rich.progress import (
    BarColumn,
    DownloadColumn,
    Progress,
    TextColumn,
    TimeRemainingColumn,
)
from rich.table import Table

from data_source_manager.utils.config import HTTP_OK
from data_source_manager.utils.for_core.vision_checksum import (
    extract_checksum_from_file,
    verify_file_checksum,
)
from data_source_manager.utils.loguru_setup import logger
from data_source_manager.utils.validation import DataValidation


def setup_argparse() -> argparse.ArgumentParser:
    """
    Set up command-line argument parsing.

    Returns:
        Configured argument parser object
    """
    parser = argparse.ArgumentParser(
        description="Download and validate Binance data files using checksums"
    )

    parser.add_argument(
        "symbols",
        nargs="+",
        help="Trading pair symbols to download (e.g., BTCUSDT ETHUSDT)",
    )

    parser.add_argument(
        "--date",
        default=time.strftime("%Y-%m-%d", time.localtime(time.time() - 86400)),
        help="Date in YYYY-MM-DD format (default: yesterday)",
    )

    parser.add_argument(
        "--interval",
        default="1m",
        choices=[
            "1m",
            "3m",
            "5m",
            "15m",
            "30m",
            "1h",
            "2h",
            "4h",
            "6h",
            "8h",
            "12h",
            "1d",
            "3d",
            "1w",
            "1mo",
        ],
        help="Kline interval (default: 1m)",
    )

    parser.add_argument(
        "--market",
        default="spot",
        choices=["spot", "futures", "um", "cm"],
        help="Market type (default: spot)",
    )

    parser.add_argument(
        "-o",
        "--output-dir",
        default=None,
        help="Output directory for downloaded files (default: temporary directory)",
    )

    parser.add_argument(
        "-v", "--verbose", action="store_true", help="Enable verbose output"
    )

    parser.add_argument(
        "--skip-download",
        action="store_true",
        help="Skip downloading files if they already exist",
    )

    parser.add_argument(
        "--skip-validation", action="store_true", help="Skip checksum validation"
    )

    return parser


def download_file(
    url: str, output_path: Path, verbose: bool = False, skip_if_exists: bool = False
) -> bool:
    """
    Download a file from a URL to a local path with progress tracking.

    Args:
        url: The URL to download from
        output_path: Local path to save the file
        verbose: Whether to show download progress
        skip_if_exists: Whether to skip download if file already exists

    Returns:
        True if download successful or file already exists, False otherwise
    """
    if output_path.exists():
        if skip_if_exists:
            logger.info(f"File already exists: {output_path}")
            return True
        logger.info(f"File exists but will be overwritten: {output_path}")

    try:
        # Create parent directories if they don't exist
        output_path.parent.mkdir(parents=True, exist_ok=True)

        with Progress(
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            DownloadColumn(),
            TimeRemainingColumn(),
            disable=not verbose,
        ) as progress:
            download_task = progress.add_task(
                f"Downloading {output_path.name}", total=None
            )

            # Use httpx for streaming download
            with httpx.stream("GET", url, follow_redirects=True) as response:
                if response.status_code != HTTP_OK:
                    logger.error(
                        f"Failed to download {url}: HTTP {response.status_code}"
                    )
                    return False

                response.raise_for_status()
                total_size = int(response.headers.get("Content-Length", 0))
                if total_size:
                    progress.update(download_task, total=total_size)

                with open(output_path, "wb") as f:
                    num_bytes = 0
                    for chunk in response.iter_bytes(chunk_size=8192):
                        f.write(chunk)
                        num_bytes += len(chunk)
                        if total_size:
                            progress.update(download_task, completed=num_bytes)

                logger.info(
                    f"Downloaded {output_path.name} ({output_path.stat().st_size} bytes)"
                )
                return True
    except Exception as e:
        logger.error(f"Error downloading {url}: {e}")
        # If file was partially downloaded, remove it
        if output_path.exists() and output_path.stat().st_size == 0:
            output_path.unlink()
        return False


def construct_file_url(
    symbol: str, interval: str, date: str, market: str = "spot"
) -> tuple[str, str, str]:
    """
    Construct the URL for a data file from Binance Vision API.

    Args:
        symbol: Trading pair symbol (e.g., "BTCUSDT")
        interval: Kline interval (e.g., "1m", "1h")
        date: Date in YYYY-MM-DD format
        market: Market type (spot, futures, um, cm)

    Returns:
        Tuple of (data_url, checksum_url, filename)
    """
    # Validate inputs
    symbol = symbol.upper()

    # Determine base URL based on market type
    if market == "spot":
        base_url = "https://data.binance.vision/data/spot/daily/klines"
    elif market in ["futures", "um"]:
        base_url = "https://data.binance.vision/data/futures/um/daily/klines"
    elif market == "cm":
        base_url = "https://data.binance.vision/data/futures/cm/daily/klines"
    else:
        raise ValueError(f"Unsupported market type: {market}")

    # Construct filename and URLs
    filename = f"{symbol}-{interval}-{date}.zip"
    data_url = f"{base_url}/{symbol}/{interval}/{filename}"
    checksum_url = f"{data_url}.CHECKSUM"

    return data_url, checksum_url, filename


def download_and_validate(
    symbol: str,
    interval: str,
    date: str,
    market: str,
    output_dir: Path,
    verbose: bool = False,
    skip_download: bool = False,
    skip_validation: bool = False,
) -> dict[str, Any]:
    """
    Download and validate a Binance data file.

    Args:
        symbol: Trading pair symbol (e.g., "BTCUSDT")
        interval: Kline interval (e.g., "1m", "1h")
        date: Date in YYYY-MM-DD format
        market: Market type (spot, futures, um, cm)
        output_dir: Directory to save downloaded files
        verbose: Whether to show verbose output
        skip_download: Whether to skip download if files already exist
        skip_validation: Whether to skip checksum validation

    Returns:
        Dictionary with download and validation results
    """
    result = {
        "symbol": symbol,
        "interval": interval,
        "date": date,
        "market": market,
        "data_file": None,
        "checksum_file": None,
        "data_download_success": False,
        "checksum_download_success": False,
        "validation_success": None,
        "validation_error": None,
        "expected_checksum": None,
        "actual_checksum": None,
    }

    try:
        # Construct URLs and filenames
        data_url, checksum_url, filename = construct_file_url(
            symbol, interval, date, market
        )

        # Set output paths
        data_path = output_dir / filename
        checksum_path = output_dir / f"{filename}.CHECKSUM"

        result["data_file"] = str(data_path)
        result["checksum_file"] = str(checksum_path)

        # Download checksum file
        if verbose:
            rprint(
                f"\n[bold]Downloading {symbol} {interval} {date} data and checksum...[/bold]"
            )

        checksum_success = download_file(
            checksum_url, checksum_path, verbose=verbose, skip_if_exists=skip_download
        )
        result["checksum_download_success"] = checksum_success

        if not checksum_success:
            logger.warning(f"Failed to download checksum file: {checksum_url}")
            return result

        # Extract expected checksum (even if we're not validating, we want to show it)
        expected_checksum = extract_checksum_from_file(checksum_path)
        result["expected_checksum"] = expected_checksum

        # Download data file
        data_success = download_file(
            data_url, data_path, verbose=verbose, skip_if_exists=skip_download
        )
        result["data_download_success"] = data_success

        if not data_success:
            logger.warning(f"Failed to download data file: {data_url}")
            return result

        # Skip validation if requested
        if skip_validation:
            logger.info(f"Skipping checksum validation for {filename}")
            return result

        # Calculate actual checksum
        actual_checksum = DataValidation.calculate_checksum(data_path)
        result["actual_checksum"] = actual_checksum

        # Verify checksum
        validation_success, validation_error = verify_file_checksum(
            data_path, checksum_path
        )
        result["validation_success"] = validation_success
        result["validation_error"] = validation_error

        if validation_success:
            logger.info(f"Checksum validation successful for {filename}")
        else:
            logger.warning(
                f"Checksum validation failed for {filename}: {validation_error}"
            )

        return result

    except Exception as e:
        logger.error(f"Error processing {symbol} {interval} {date}: {e}")
        result["validation_error"] = str(e)
        return result


def display_results(results: list[dict[str, Any]]) -> None:
    """
    Display download and validation results in a rich table.

    Args:
        results: List of download and validation results
    """
    console = Console()

    table = Table(title="Binance Data Download and Validation Results")
    table.add_column("Symbol", style="cyan")
    table.add_column("Interval", style="cyan")
    table.add_column("Date", style="cyan")
    table.add_column("Data", style="green")
    table.add_column("Checksum", style="green")
    table.add_column("Validation", style="yellow")

    for result in results:
        symbol = result["symbol"]
        interval = result["interval"]
        date = result["date"]

        data_status = (
            "[green]✓[/green]" if result["data_download_success"] else "[red]✗[/red]"
        )
        checksum_status = (
            "[green]✓[/green]"
            if result["checksum_download_success"]
            else "[red]✗[/red]"
        )

        if result["validation_success"] is None:
            validation_status = "[yellow]SKIPPED[/yellow]"
        elif result["validation_success"]:
            validation_status = "[green]✓ VALID[/green]"
        else:
            validation_status = "[red]✗ INVALID[/red]"

        table.add_row(
            symbol, interval, date, data_status, checksum_status, validation_status
        )

    console.print(table)

    # Display detailed results for each file
    for i, result in enumerate(results):
        if not (
            result["data_download_success"] and result["checksum_download_success"]
        ):
            continue

        if result["validation_success"] is None:
            continue

        validation_title = (
            "[green]✓ VALID CHECKSUM[/green]"
            if result["validation_success"]
            else "[red]✗ INVALID CHECKSUM[/red]"
        )

        detail_table = Table(
            title=f"{result['symbol']} {result['interval']} {result['date']} - {validation_title}",
            show_header=False,
            box=None,
        )
        detail_table.add_column("Key", style="bold cyan")
        detail_table.add_column("Value")

        detail_table.add_row("Data File", str(result["data_file"]))
        detail_table.add_row("Checksum File", str(result["checksum_file"]))
        detail_table.add_row("Expected Checksum", result["expected_checksum"])
        detail_table.add_row("Actual Checksum", result["actual_checksum"])

        # Add error message if validation failed
        if not result["validation_success"] and result["validation_error"]:
            detail_table.add_row("Error", f"[red]{result['validation_error']}[/red]")

        console.print(detail_table)
        console.print("")  # Add a blank line between details


def main() -> int:
    """
    Main entry point of the script.

    Returns:
        Exit code (0 for success, non-zero for failure)
    """
    parser = setup_argparse()
    args = parser.parse_args()

    # Set up output directory
    if args.output_dir:
        output_dir = Path(args.output_dir)
    else:
        # Create a temporary directory if output_dir not specified
        temp_dir = tempfile.mkdtemp(prefix="binance_data_")
        output_dir = Path(temp_dir)
        if args.verbose:
            rprint(f"[yellow]Using temporary directory: {output_dir}[/yellow]")

    # Create output directory if it doesn't exist
    output_dir.mkdir(parents=True, exist_ok=True)

    if args.verbose:
        rprint(
            Panel.fit(
                "[bold]Binance Data Download and Validation[/bold]",
                subtitle=f"Date: {args.date} | Interval: {args.interval} | Market: {args.market}",
            )
        )

    # Download and validate files for each symbol
    results = []
    for symbol in args.symbols:
        result = download_and_validate(
            symbol=symbol,
            interval=args.interval,
            date=args.date,
            market=args.market,
            output_dir=output_dir,
            verbose=args.verbose,
            skip_download=args.skip_download,
            skip_validation=args.skip_validation,
        )
        results.append(result)

    # Display results
    display_results(results)

    # Determine overall success
    success = all(
        result["data_download_success"]
        and result["checksum_download_success"]
        and (result["validation_success"] or args.skip_validation)
        for result in results
    )

    if args.verbose:
        if success:
            rprint(
                "\n[bold green]✓ All operations completed successfully![/bold green]"
            )
        else:
            rprint(
                "\n[bold red]✗ Some operations failed. See details above.[/bold red]"
            )

    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
