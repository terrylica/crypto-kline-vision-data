#!/usr/bin/env python3

import argparse
import hashlib
import os
import re
import tempfile
from pathlib import Path

import httpx
from rich.console import Console
from rich.progress import Progress
from rich.table import Table

from utils.config import HTTP_OK, TEXT_PREVIEW_LENGTH
from utils.for_core.vision_checksum import extract_checksum_from_file
from utils.validation import DataValidation

console = Console()


def setup_argparse() -> argparse.Namespace:
    """Set up command line argument parsing."""
    parser = argparse.ArgumentParser(
        description="Download and analyze CHECKSUM files from Binance Data"
    )
    parser.add_argument(
        "symbols", nargs="*", help="Symbols to check (e.g. BTCUSDT ETHUSDT)"
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true", help="Enable verbose output"
    )
    parser.add_argument(
        "--validate", metavar="FILE", help="Validate file against its checksum"
    )
    parser.add_argument(
        "--checksum-url", metavar="URL", help="Specify checksum file URL"
    )
    parser.add_argument(
        "--date", help="Date in YYYY-MM-DD format to check (defaults to today)"
    )
    parser.add_argument("--interval", default="1m", help="Kline interval (default: 1m)")
    parser.add_argument("--market", default="spot", help="Market type (spot, um, cm)")
    return parser.parse_args()


def download_file(url: str, output_path: Path) -> bool:
    """Download a file from URL to the specified path."""
    try:
        with Progress() as progress:
            task = progress.add_task(f"Downloading {url.split('/')[-1]}", total=1)

            with httpx.Client(timeout=30.0) as client:
                with client.stream("GET", url) as response:
                    if response.status_code != HTTP_OK:
                        console.print(
                            f"[red]Error downloading {url}: {response.status_code}"
                        )
                        return False

                    total_size = int(response.headers.get("content-length", 0))
                    if total_size:
                        progress.update(task, total=total_size)

                    with open(output_path, "wb") as f:
                        for chunk in response.iter_bytes(chunk_size=8192):
                            f.write(chunk)
                            if total_size:
                                progress.update(task, advance=len(chunk))

            progress.update(task, completed=1)
        return True
    except Exception as e:
        console.print(f"[red]Error downloading {url}: {e}")
        return False


def calculate_file_checksum(file_path: Path) -> str | None:
    """Calculate SHA256 checksum for a file."""
    try:
        sha256_hash = hashlib.sha256()
        with open(file_path, "rb") as f:
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)
        return sha256_hash.hexdigest()
    except Exception as e:
        console.print(f"[red]Error calculating checksum for {file_path}: {e}")
        return None


def validate_file_checksum(file_path: Path, checksum_url: str) -> tuple[bool, str, str]:
    """Validate a file against its checksum."""
    with tempfile.NamedTemporaryFile(delete=False) as temp_file:
        temp_path = Path(temp_file.name)

    try:
        # Download the checksum file
        if not download_file(checksum_url, temp_path):
            return False, "", "Failed to download checksum file"

        # Extract the expected checksum
        expected_checksum = extract_checksum_from_file(temp_path)
        if not expected_checksum:
            # Try direct parsing from URL content
            with open(temp_path, "rb") as f:
                content = f.read().decode("utf-8", errors="replace")
                content_length = len(content)
                preview_length = min(40, content_length)
                console.print(
                    f"[yellow]Raw checksum file content: {content[:preview_length]} (+ {content_length - preview_length} more chars, {content_length} total)"
                )
                # Look for a SHA-256 hash pattern
                matches = re.findall(r"([a-fA-F0-9]{64})", content)
                if matches:
                    expected_checksum = matches[0]

        if not expected_checksum:
            return False, "", "Could not extract checksum from file"

        # Calculate actual checksum
        actual_checksum = calculate_file_checksum(file_path)
        if not actual_checksum:
            return False, "", "Failed to calculate file checksum"

        # Compare checksums
        is_valid = expected_checksum.lower() == actual_checksum.lower()
        return is_valid, expected_checksum, actual_checksum
    finally:
        # Clean up temp file
        if temp_path.exists():
            os.unlink(temp_path)


def show_checksums_for_symbol(
    symbol: str, date: str, interval: str = "1m", market: str = "spot"
) -> None:
    """Download and display checksum for a specific symbol."""
    # Construct URL for the data file
    base_url = "https://data.binance.vision/data"
    zip_url = f"{base_url}/{market}/daily/klines/{symbol}/{interval}/{symbol}-{interval}-{date}.zip"
    checksum_url = f"{zip_url}.CHECKSUM"

    # Create a temporary directory to store downloaded files
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_dir_path = Path(temp_dir)

        # Download the checksum file
        checksum_path = temp_dir_path / f"{symbol}-{interval}-{date}.zip.CHECKSUM"
        if not download_file(checksum_url, checksum_path):
            return

        # Log the raw content of the checksum file
        try:
            with open(checksum_path, "rb") as f:
                content = f.read()
                content_length = len(content)
                preview_length = min(40, content_length)
                console.print("\n[bold]Raw checksum file content preview:[/bold]")
                console.print(
                    f"{content[:preview_length]} (+ {content_length - preview_length} more bytes, {content_length} total)"
                )

                # Try to decode as text
                console.print("\n[bold]Decoded content preview:[/bold]")
                try:
                    text_content = content.decode("utf-8", errors="replace")
                    text_preview = (
                        text_content[:TEXT_PREVIEW_LENGTH]
                        if len(text_content) > TEXT_PREVIEW_LENGTH
                        else text_content
                    )
                    console.print(
                        f"{text_preview}"
                        + ("..." if len(text_content) > TEXT_PREVIEW_LENGTH else "")
                    )
                except Exception as e:
                    console.print(f"[red]Error decoding content: {e}")
        except Exception as e:
            console.print(f"[red]Error reading checksum file: {e}")

        # Extract checksum using our improved function
        extracted_checksum = extract_checksum_from_file(checksum_path)

        # Now download the actual data file
        zip_path = temp_dir_path / f"{symbol}-{interval}-{date}.zip"
        if download_file(zip_url, zip_path):
            # Calculate the actual checksum
            calculated_checksum = calculate_file_checksum(zip_path)

            # Create a table to display results
            table = Table(title=f"Checksum Verification: {symbol} {interval} {date}")
            table.add_column("Source", style="cyan")
            table.add_column("Checksum", style="green")
            table.add_column("Status", style="yellow")

            # Add extracted checksum row
            if extracted_checksum:
                match_status = (
                    "[green]✓ MATCH"
                    if extracted_checksum.lower() == calculated_checksum.lower()
                    else "[red]✗ MISMATCH"
                )
                table.add_row(
                    "Extracted from .CHECKSUM file", extracted_checksum, match_status
                )
            else:
                table.add_row(
                    "Extracted from .CHECKSUM file",
                    "[red]Failed to extract",
                    "[red]✗ ERROR",
                )

            # Add calculated checksum row
            table.add_row("Calculated from .zip file", calculated_checksum, "")

            # Add additional rows for DataValidation implementation
            if calculated_checksum:
                validation_result = DataValidation.calculate_checksum(zip_path)
                validation_match = (
                    "[green]✓ MATCH"
                    if validation_result.lower() == calculated_checksum.lower()
                    else "[red]✗ MISMATCH"
                )
                table.add_row(
                    "DataValidation.calculate_checksum()",
                    validation_result,
                    validation_match,
                )

            console.print(table)


def main():
    args = setup_argparse()

    if args.validate and args.checksum_url:
        file_path = Path(args.validate)
        if not file_path.exists():
            console.print(f"[red]File not found: {file_path}")
            return

        console.print(
            f"[bold]Validating[/bold] {file_path} against {args.checksum_url}"
        )
        is_valid, expected, actual = validate_file_checksum(
            file_path, args.checksum_url
        )

        table = Table(title="Checksum Validation Results")
        table.add_column("Type", style="cyan")
        table.add_column("Value", style="green")

        table.add_row("File", str(file_path))
        table.add_row("Expected Checksum", expected)
        table.add_row("Actual Checksum", actual)
        table.add_row("Status", "[green]VALID" if is_valid else "[red]INVALID")

        console.print(table)
        return

    # Use default symbol if none provided
    symbols = args.symbols or ["BTCUSDT"]
    # Use specified date or default
    date = args.date or "2025-04-13"  # Use the date that's failing

    for symbol in symbols:
        show_checksums_for_symbol(symbol, date, args.interval, args.market)


if __name__ == "__main__":
    main()
