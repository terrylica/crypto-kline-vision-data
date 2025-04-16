#!/usr/bin/env python3
"""
Test script for validating the improved checksum verification in vision_data_client.py.

This script downloads data for a specific symbol and date range using the DataSourceManager
to verify that our improvements to checksum verification work correctly with real data,
allowing us to verify file integrity reliably.
"""

import argparse
import sys
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
import tempfile
from typing import Dict, Optional, Tuple, Any

import httpx
from rich.console import Console
from rich.progress import Progress, TextColumn, BarColumn, TimeRemainingColumn
from rich.table import Table
from rich.panel import Panel
from rich import print as rprint

from utils.logger_setup import logger
from utils.market_constraints import MarketType, Interval, DataProvider, ChartType
from core.sync.data_source_manager import DataSourceManager, DataSource
from utils.for_core.vision_checksum import (
    extract_checksum_from_file,
    verify_file_checksum,
    calculate_checksums_multiple_methods,
)
from utils.validation import DataValidation


def setup_argparse() -> argparse.ArgumentParser:
    """
    Set up command-line argument parsing.

    Returns:
        Configured argument parser object
    """
    parser = argparse.ArgumentParser(
        description="Test the improved checksum verification in vision_data_client.py"
    )

    parser.add_argument(
        "--symbol",
        default="BTCUSDT",
        help="Trading pair symbol to test (default: BTCUSDT)",
    )

    parser.add_argument(
        "--date",
        default=time.strftime("%Y-%m-%d", time.localtime(time.time() - 86400)),
        help="Date in YYYY-MM-DD format to test (default: yesterday)",
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
        choices=["spot", "um", "cm"],
        help="Market type: spot, um (USDT-M futures), cm (Coin-M futures) (default: spot)",
    )

    parser.add_argument(
        "-v", "--verbose", action="store_true", help="Enable verbose output"
    )

    parser.add_argument(
        "--skip-compare",
        action="store_true",
        help="Skip comparing DataSourceManager results with direct verification",
    )

    parser.add_argument(
        "--enforce-source",
        choices=["AUTO", "VISION", "REST"],
        default="VISION",
        help="Force specific data source (default: VISION)",
    )

    parser.add_argument(
        "--show-all-checksums",
        action="store_true",
        help="Show detailed SHA-256 checksum verification results",
    )

    return parser


def download_file(url: str, output_path: Path, verbose: bool = False) -> bool:
    """
    Download a file from a URL to a local path with progress tracking.

    Args:
        url: The URL to download from
        output_path: Local path to save the file
        verbose: Whether to show download progress

    Returns:
        True if download successful, False otherwise
    """
    if output_path.exists():
        logger.info(f"File already exists: {output_path}")
        return True

    try:
        output_path.parent.mkdir(parents=True, exist_ok=True)

        with Progress(
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TimeRemainingColumn(),
            disable=not verbose,
        ) as progress:
            download_task = progress.add_task(
                f"Downloading {output_path.name}", total=None
            )

            with httpx.stream("GET", url, follow_redirects=True) as response:
                if response.status_code != 200:
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
        return False


def construct_file_url(
    symbol: str, interval: str, date: str, market: str = "spot"
) -> Tuple[str, str]:
    """
    Construct the URLs for a data file and its checksum from Binance Vision API.

    Args:
        symbol: Trading pair symbol (e.g., "BTCUSDT")
        interval: Kline interval (e.g., "1m", "1h")
        date: Date in YYYY-MM-DD format
        market: Market type (spot, um, cm)

    Returns:
        Tuple of (data_url, checksum_url)
    """
    # Validate inputs
    symbol = symbol.upper()

    # Determine base URL based on market type
    if market == "spot":
        base_url = "https://data.binance.vision/data/spot/daily/klines"
    elif market == "um":
        base_url = "https://data.binance.vision/data/futures/um/daily/klines"
    elif market == "cm":
        base_url = "https://data.binance.vision/data/futures/cm/daily/klines"
    else:
        raise ValueError(f"Unsupported market type: {market}")

    # Construct and return URLs
    filename = f"{symbol}-{interval}-{date}.zip"
    data_url = f"{base_url}/{symbol}/{interval}/{filename}"
    checksum_url = f"{data_url}.CHECKSUM"

    return data_url, checksum_url


def verify_direct_download(
    symbol: str,
    interval: str,
    date: str,
    market: str,
    verbose: bool = False,
    show_all_checksums: bool = False,
) -> Dict[str, Any]:
    """
    Download and verify a data file directly using our checksum verification utilities.

    Args:
        symbol: Trading pair symbol (e.g., "BTCUSDT")
        interval: Kline interval (e.g., "1m", "1h")
        date: Date in YYYY-MM-DD format
        market: Market type (spot, um, cm)
        verbose: Whether to show verbose output
        show_all_checksums: Whether to show detailed SHA-256 checksum results

    Returns:
        Dictionary with verification results
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
        "all_checksums": None,
        "time_taken": 0,
    }

    try:
        # Create temporary directory for downloaded files
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_dir_path = Path(temp_dir)

            # Construct URLs
            data_url, checksum_url = construct_file_url(symbol, interval, date, market)

            # Set file paths
            data_path = temp_dir_path / f"{symbol}-{interval}-{date}.zip"
            checksum_path = temp_dir_path / f"{symbol}-{interval}-{date}.zip.CHECKSUM"

            result["data_file"] = str(data_path)
            result["checksum_file"] = str(checksum_path)

            # Download checksum file
            start_time = time.time()

            if verbose:
                rprint(
                    f"\n[bold]Directly downloading and verifying {symbol} {interval} {date}...[/bold]"
                )

            checksum_success = download_file(checksum_url, checksum_path, verbose)
            result["checksum_download_success"] = checksum_success

            if not checksum_success:
                logger.warning(f"Failed to download checksum file: {checksum_url}")
                return result

            # Print checksum file content for debugging
            try:
                with open(checksum_path, "rb") as f:
                    file_content = f.read()
                    content_length = len(file_content)
                    preview_length = min(30, content_length)

                    # Create a concise preview of the checksum content
                    if content_length > 0:
                        content_preview = file_content[:preview_length]
                        remaining = content_length - preview_length
                        logger.debug(
                            f"CHECKSUM FILE SUMMARY: {content_preview!r} ({remaining} more bytes, {content_length} total)"
                        )
                    else:
                        logger.warning(f"CHECKSUM FILE EMPTY (0 bytes)")
            except Exception as e:
                logger.error(f"Error reading checksum file: {e}")

            # Download data file
            data_success = download_file(data_url, data_path, verbose)
            result["data_download_success"] = data_success

            if not data_success:
                logger.warning(f"Failed to download data file: {data_url}")
                return result

            # Extract expected checksum
            expected_checksum = extract_checksum_from_file(checksum_path)
            result["expected_checksum"] = expected_checksum

            # Calculate checksums using all methods
            if show_all_checksums:
                all_checksums = calculate_checksums_multiple_methods(data_path)
                result["all_checksums"] = all_checksums
                # Use the SHA-256 result as the actual checksum for display
                if all_checksums:
                    result["actual_checksum"] = all_checksums.get("sha256", "")
            else:
                # Calculate only the standard checksum
                actual_checksum = DataValidation.calculate_checksum(data_path)
                result["actual_checksum"] = actual_checksum

            # Verify checksum
            validation_success, validation_error = verify_file_checksum(
                data_path, checksum_path
            )
            result["validation_success"] = validation_success
            result["validation_error"] = validation_error

            # Record time taken
            result["time_taken"] = time.time() - start_time

            if verbose:
                if validation_success:
                    rprint(f"[green]✓ Direct verification successful[/green]")
                else:
                    rprint(
                        f"[red]✗ Direct verification failed: {validation_error}[/red]"
                    )

            return result

    except Exception as e:
        logger.error(f"Error in direct verification: {e}")
        result["validation_error"] = str(e)
        return result


def verify_using_dsm(
    symbol: str,
    interval_str: str,
    date_str: str,
    market: str,
    enforce_source: str = "VISION",
    verbose: bool = False,
) -> Dict[str, Any]:
    """
    Verify data integrity using the DataSourceManager.

    Args:
        symbol: Trading pair symbol (e.g., "BTCUSDT")
        interval_str: Kline interval (e.g., "1m", "1h")
        date_str: Date in YYYY-MM-DD format
        market: Market type (spot, um, cm)
        enforce_source: Force specific data source (AUTO, VISION, REST)
        verbose: Whether to show verbose output

    Returns:
        Dictionary with verification results
    """
    result = {
        "symbol": symbol,
        "interval": interval_str,
        "date": date_str,
        "market": market,
        "data_rows": 0,
        "success": False,
        "error": None,
        "sources_used": [],
        "time_taken": 0,
    }

    try:
        # Convert interval string to enum
        interval = Interval(interval_str)

        # Convert market string to enum
        if market == "spot":
            market_type = MarketType.SPOT
        elif market == "um":
            market_type = MarketType.FUTURES_USDT
        elif market == "cm":
            market_type = MarketType.FUTURES_COIN
        else:
            raise ValueError(f"Unsupported market type: {market}")

        # Convert enforce_source string to enum
        if enforce_source == "AUTO":
            enforce_source_enum = DataSource.AUTO
        elif enforce_source == "VISION":
            enforce_source_enum = DataSource.VISION
        elif enforce_source == "REST":
            enforce_source_enum = DataSource.REST
        else:
            enforce_source_enum = DataSource.AUTO

        # Parse date string
        date = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)

        # Define time range (full day)
        start_time = date
        end_time = date + timedelta(days=1, microseconds=-1)

        if verbose:
            rprint(
                f"\n[bold]Testing DataSourceManager retrieval for {symbol} {interval.value} on {date_str}[/bold]"
            )
            rprint(f"Time range: {start_time.isoformat()} to {end_time.isoformat()}")
            rprint(f"Enforcing source: {enforce_source}")

        # Retrieve data using DataSourceManager
        start_time_retrieval = time.time()

        with DataSourceManager(
            market_type=market_type,
            provider=DataProvider.BINANCE,
            chart_type=ChartType.KLINES,
            use_cache=False,  # Disable cache to force download
            retry_count=3,
        ) as manager:
            # Get data with source info
            df = manager.get_data(
                symbol=symbol,
                start_time=start_time,
                end_time=end_time,
                interval=interval,
                chart_type=ChartType.KLINES,
                enforce_source=enforce_source_enum,
                include_source_info=True,
            )

        result["time_taken"] = time.time() - start_time_retrieval

        # Process results
        if df is None or df.empty:
            result["success"] = False
            result["error"] = "No data returned"
            if verbose:
                rprint(f"[red]✗ No data returned by DataSourceManager[/red]")
        else:
            result["success"] = True
            result["data_rows"] = len(df)

            # Analyze sources if source info is available
            if "_data_source" in df.columns:
                sources = df["_data_source"].unique().tolist()
                result["sources_used"] = sources

                if verbose:
                    source_counts = df["_data_source"].value_counts()
                    rprint(f"[green]✓ Successfully retrieved {len(df)} records[/green]")
                    rprint(f"Data sources used:")
                    for source, count in source_counts.items():
                        rprint(
                            f"  - {source}: {count} records ({count/len(df)*100:.1f}%)"
                        )

        return result

    except Exception as e:
        logger.error(f"Error in DSM verification: {e}")
        result["success"] = False
        result["error"] = str(e)
        return result


def display_results(
    direct_result: Dict[str, Any], dsm_result: Optional[Dict[str, Any]] = None
) -> None:
    """
    Display verification results in a rich table.

    Args:
        direct_result: Results from direct download and verification
        dsm_result: Results from DataSourceManager verification (optional)
    """
    console = Console()

    # Display summary table
    summary_table = Table(title="Checksum Verification Test Results")
    summary_table.add_column("Test", style="cyan")
    summary_table.add_column("Result", style="green")
    summary_table.add_column("Time (sec)", style="yellow")
    summary_table.add_column("Details", style="magenta")

    # Add direct verification result
    symbol = direct_result["symbol"]
    interval = direct_result["interval"]
    date = direct_result["date"]

    direct_status = (
        "[green]✓ PASSED[/green]"
        if direct_result["validation_success"]
        else f"[red]✗ FAILED[/red]"
    )

    direct_details = (
        f"{direct_result['expected_checksum'][:10]}...{direct_result['expected_checksum'][-10:]}"
        if direct_result["expected_checksum"]
        else "No checksum"
    )

    summary_table.add_row(
        "Direct Verification",
        direct_status,
        f"{direct_result['time_taken']:.2f}",
        direct_details,
    )

    # Add DSM verification result if available
    if dsm_result:
        dsm_status = (
            f"[green]✓ SUCCESS ({dsm_result['data_rows']} rows)[/green]"
            if dsm_result["success"]
            else f"[red]✗ FAILED[/red]"
        )

        dsm_details = (
            f"Sources: {', '.join(dsm_result['sources_used'])}"
            if dsm_result["sources_used"]
            else dsm_result["error"] or "No details"
        )

        summary_table.add_row(
            "DataSourceManager",
            dsm_status,
            f"{dsm_result['time_taken']:.2f}",
            dsm_details,
        )

    console.print(summary_table)

    # Display direct verification details
    details_table = Table(
        title=f"Verification Details: {symbol} {interval} {date}",
        show_header=False,
        box=None,
    )
    details_table.add_column("Property", style="bold cyan")
    details_table.add_column("Value")

    details_table.add_row("Symbol", symbol)
    details_table.add_row("Interval", interval)
    details_table.add_row("Date", date)
    details_table.add_row("Market", direct_result["market"])

    if direct_result["checksum_download_success"]:
        details_table.add_row("Checksum File", str(direct_result["checksum_file"]))
        details_table.add_row("Expected Checksum", direct_result["expected_checksum"])

    if direct_result["data_download_success"]:
        details_table.add_row("Data File", str(direct_result["data_file"]))
        details_table.add_row("Actual Checksum", direct_result["actual_checksum"])

    if direct_result["validation_error"]:
        details_table.add_row(
            "Error", f"[red]{direct_result['validation_error']}[/red]"
        )

    # Show all checksum methods if available
    if direct_result["all_checksums"]:
        checksum_table = Table(title="SHA-256 Checksum Result")
        checksum_table.add_column("Method", style="cyan")
        checksum_table.add_column("Checksum", style="green")
        checksum_table.add_column("Match with Expected", style="yellow")

        expected = direct_result["expected_checksum"]

        for method, checksum in direct_result["all_checksums"].items():
            match_status = ""
            if expected and checksum:
                match_status = (
                    "[green]✓ MATCH[/green]"
                    if expected.lower() == checksum.lower()
                    else "[red]✗ MISMATCH[/red]"
                )
            checksum_table.add_row(method, checksum, match_status)

        console.print(checksum_table)

    console.print(details_table)


def main() -> int:
    """
    Main entry point of the script.

    Returns:
        Exit code (0 for success, non-zero for failure)
    """
    parser = setup_argparse()
    args = parser.parse_args()

    # Set log level based on verbosity
    logger.setLevel("DEBUG" if args.verbose else "INFO")

    if args.verbose:
        rprint(
            Panel.fit(
                "[bold]Checksum Verification Test[/bold]",
                subtitle="Testing the improved checksum verification in vision_data_client.py",
            )
        )

    # Perform direct verification
    direct_result = verify_direct_download(
        symbol=args.symbol,
        interval=args.interval,
        date=args.date,
        market=args.market,
        verbose=args.verbose,
        show_all_checksums=args.show_all_checksums,
    )

    # Perform DSM verification if not skipped
    dsm_result = None
    if not args.skip_compare:
        dsm_result = verify_using_dsm(
            symbol=args.symbol,
            interval_str=args.interval,
            date_str=args.date,
            market=args.market,
            enforce_source=args.enforce_source,
            verbose=args.verbose,
        )

    # Display results
    display_results(direct_result, dsm_result)

    # Determine overall success
    direct_success = direct_result["validation_success"] is True
    dsm_success = dsm_result["success"] if dsm_result else True

    success = direct_success and dsm_success

    if args.verbose:
        if success:
            rprint(
                "\n[bold green]✓ All verification tests passed successfully![/bold green]"
            )
        else:
            rprint(
                "\n[bold red]✗ Some verification tests failed. See details above.[/bold red]"
            )

    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
