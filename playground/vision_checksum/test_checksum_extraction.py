#!/usr/bin/env python3
"""
Test script for verifying checksum extraction from Binance Vision API CHECKSUM files.

This script tests the extraction of SHA-256 checksums from various formats of CHECKSUM files
that might be returned by the Binance Vision API. It creates test files with different formats
and tests whether the extraction function correctly handles them.
"""

import argparse
import hashlib
import random
import string
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

from utils.config import HTTP_OK, TEXT_PREVIEW_LENGTH
from utils.for_core.vision_checksum import extract_checksum_from_file
from utils.loguru_setup import logger


def setup_argparse() -> argparse.ArgumentParser:
    """
    Set up command-line argument parsing.

    Returns:
        Configured argument parser object
    """
    parser = argparse.ArgumentParser(
        description="Test checksum extraction from various CHECKSUM file formats"
    )

    parser.add_argument(
        "-v", "--verbose", action="store_true", help="Enable verbose output"
    )

    parser.add_argument(
        "--test-types",
        nargs="+",
        choices=["standard", "header", "binary", "malformed", "real"],
        default=["standard", "header", "binary", "malformed", "real"],
        help="Specific test types to run (default: all)",
    )

    parser.add_argument(
        "--symbol",
        default="BTCUSDT",
        help="Symbol to use for real-world test (default: BTCUSDT)",
    )

    parser.add_argument(
        "--date",
        default=time.strftime("%Y-%m-%d", time.localtime(time.time() - 86400)),
        help="Date to use for real-world test (default: yesterday)",
    )

    return parser


def generate_random_sha256() -> str:
    """
    Generate a random SHA-256 hash string for testing.

    Returns:
        Random SHA-256 hash string (64 hex characters)
    """
    random_data = "".join(
        random.choice(string.ascii_letters + string.digits) for _ in range(64)
    )
    return hashlib.sha256(random_data.encode()).hexdigest()


def create_test_files() -> list[tuple[str, str, Path]]:
    """
    Create test files with various formats of CHECKSUM content for testing.

    Returns:
        List of tuples (description, expected_checksum, file_path)
    """
    test_files = []
    temp_dir = Path(tempfile.mkdtemp())

    # Test file 1: Standard format
    checksum1 = generate_random_sha256()
    filename1 = "BTCUSDT-1m-2023-01-01.zip"
    content1 = f"{checksum1}  {filename1}"
    path1 = temp_dir / "standard.CHECKSUM"
    with open(path1, "w") as f:
        f.write(content1)
    test_files.append(("Standard format", checksum1, path1))

    # Test file 2: With header
    checksum2 = generate_random_sha256()
    filename2 = "ETHUSDT-1m-2023-01-01.zip"
    content2 = f"</daily/klines/ETHUSDT/1m/ETHUSDT-1m-2023-01-01.zip.CHECKSUM\n{checksum2}  {filename2}"
    path2 = temp_dir / "with_header.CHECKSUM"
    with open(path2, "w") as f:
        f.write(content2)
    test_files.append(("With header", checksum2, path2))

    # Test file 3: Binary content with embedded checksum
    checksum3 = generate_random_sha256()
    binary_content = b"\x00\x01\x02\x03" + checksum3.encode() + b"\x04\x05\x06\x07"
    path3 = temp_dir / "binary.CHECKSUM"
    with open(path3, "wb") as f:
        f.write(binary_content)
    test_files.append(("Binary content", checksum3, path3))

    # Test file 4: Malformed with multiple checksums
    checksum4a = generate_random_sha256()
    checksum4b = generate_random_sha256()  # This is the one that should be extracted
    filename4 = "BNBUSDT-1m-2023-01-01.zip"
    content4 = (
        f"Invalid line with {checksum4a}\nAnother line\n{checksum4b}  {filename4}"
    )
    path4 = temp_dir / "multiple_checksums.CHECKSUM"
    with open(path4, "w") as f:
        f.write(content4)
    test_files.append(("Malformed with multiple checksums", checksum4b, path4))

    # Test file 5: Extra whitespace
    checksum5 = generate_random_sha256()
    filename5 = "ADAUSDT-1m-2023-01-01.zip"
    content5 = f"  \n  {checksum5}    {filename5}  \n  "
    path5 = temp_dir / "extra_whitespace.CHECKSUM"
    with open(path5, "w") as f:
        f.write(content5)
    test_files.append(("Extra whitespace", checksum5, path5))

    return test_files


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
            DownloadColumn(),
            TimeRemainingColumn(),
            disable=not verbose,
        ) as progress:
            download_task = progress.add_task(
                f"Downloading {output_path.name}", total=None
            )

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
        return False


def test_real_checksum_extraction(
    symbol: str, date: str, verbose: bool = False
) -> tuple[bool, str | None, str | None]:
    """
    Test checksum extraction with a real checksum file from Binance Vision API.

    Args:
        symbol: Trading pair symbol (e.g., "BTCUSDT")
        date: Date in YYYY-MM-DD format
        verbose: Whether to show verbose output

    Returns:
        Tuple of (success, extracted_checksum, error_message)
    """
    try:
        # Construct the URL for the checksum file
        interval = "1m"
        base_url = "https://data.binance.vision/data/spot/daily/klines"
        filename = f"{symbol}-{interval}-{date}.zip"
        checksum_url = f"{base_url}/{symbol}/{interval}/{filename}.CHECKSUM"

        # Create a temporary directory for downloaded files
        temp_dir = Path(tempfile.mkdtemp())
        checksum_path = temp_dir / f"{filename}.CHECKSUM"

        # Download the checksum file
        if not download_file(checksum_url, checksum_path, verbose):
            return False, None, f"Failed to download checksum file from {checksum_url}"

        # Display checksum file content
        if verbose:
            with open(checksum_path, "rb") as f:
                content = f.read()
                content_length = len(content)
                preview_length = min(40, content_length)
                rprint("\n[bold]Raw checksum file content preview:[/bold]")
                rprint(
                    f"{content[:preview_length]} (+ {content_length - preview_length} more bytes, {content_length} total)"
                )

                try:
                    decoded = content.decode("utf-8", errors="replace")
                    decoded_preview = (
                        decoded[:TEXT_PREVIEW_LENGTH]
                        if len(decoded) > TEXT_PREVIEW_LENGTH
                        else decoded
                    )
                    rprint("\n[bold]Decoded content preview:[/bold]")
                    rprint(
                        f"{decoded_preview}"
                        + ("..." if len(decoded) > TEXT_PREVIEW_LENGTH else "")
                    )
                except Exception:
                    pass

        # Extract checksum from the file
        extracted_checksum = extract_checksum_from_file(checksum_path)

        if extracted_checksum:
            return True, extracted_checksum, None
        return False, None, "Failed to extract checksum from file"

    except Exception as e:
        logger.error(f"Error testing real checksum extraction: {e}")
        return False, None, str(e)


def test_extract_checksum(
    test_files: list[tuple[str, str, Path]], verbose: bool = False
) -> dict[str, dict[str, Any]]:
    """
    Test the extraction of checksums from different file formats.

    Args:
        test_files: List of test files (description, expected_checksum, file_path)
        verbose: Whether to show verbose output

    Returns:
        Dictionary of test results
    """
    results = {}

    for description, expected_checksum, file_path in test_files:
        if verbose:
            rprint(f"\n[bold]Testing: {description}[/bold]")
            rprint(f"File: {file_path}")
            rprint(f"Expected checksum: {expected_checksum}")

        # Read raw content for verbose output
        if verbose:
            with open(file_path, "rb") as f:
                content = f.read()
                rprint(f"Raw content (first 100 bytes): {content[:100]}")

        # Extract checksum using the function
        extracted_checksum = extract_checksum_from_file(file_path)
        success = extracted_checksum == expected_checksum

        results[description] = {
            "expected": expected_checksum,
            "extracted": extracted_checksum,
            "success": success,
            "file_path": file_path,
        }

        if verbose:
            if success:
                rprint(f"[green]✓ Success:[/green] Extracted {extracted_checksum}")
            else:
                rprint(f"[red]✗ Failed:[/red] Extracted {extracted_checksum}")

    return results


def display_results(
    results: dict[str, dict[str, Any]],
    real_test_result: tuple[bool, str | None, str | None] | None = None,
) -> None:
    """
    Display the test results in a rich table.

    Args:
        results: Dictionary of test results
        real_test_result: Results from the real-world test (if available)
    """
    console = Console()

    table = Table(title="Checksum Extraction Test Results")
    table.add_column("Test Case", style="cyan")
    table.add_column("Expected Checksum", style="magenta")
    table.add_column("Extracted Checksum", style="yellow")
    table.add_column("Status", style="green")

    for description, result in results.items():
        expected = result["expected"]
        extracted = result["extracted"] or "None"
        status = "[green]✓ PASS[/green]" if result["success"] else "[red]✗ FAIL[/red]"

        table.add_row(
            description,
            expected[:10] + "..." + expected[-10:] if expected else "None",
            extracted[:10] + "..." + extracted[-10:] if extracted != "None" else "None",
            status,
        )

    # Add real-world test result if available
    if real_test_result:
        success, extracted, error = real_test_result
        status = "[green]✓ PASS[/green]" if success else f"[red]✗ FAIL: {error}[/red]"

        table.add_row(
            "Real-world Binance Vision API",
            "Unknown (live test)",
            extracted[:10] + "..." + extracted[-10:] if extracted else "None",
            status,
        )

    console.print(table)


def main() -> int:
    """
    Main entry point of the script.

    Returns:
        Exit code (0 for success, non-zero for failure)
    """
    parser = setup_argparse()
    args = parser.parse_args()

    if args.verbose:
        rprint(
            Panel.fit(
                "[bold]Checksum Extraction Test[/bold]",
                subtitle="Testing extraction of SHA-256 checksums from various file formats",
            )
        )

    all_results = {}

    # Test with synthetic test files
    if set(args.test_types) & {"standard", "header", "binary", "malformed"}:
        test_files = create_test_files()
        all_results = test_extract_checksum(test_files, args.verbose)

    # Test with real checksum file from Binance Vision API
    real_test_result = None
    if "real" in args.test_types:
        if args.verbose:
            rprint(
                f"\n[bold]Testing with real checksum file for {args.symbol} on {args.date}[/bold]"
            )

        real_test_result = test_real_checksum_extraction(
            args.symbol, args.date, args.verbose
        )

        if args.verbose:
            success, extracted, error = real_test_result
            if success:
                rprint(f"[green]✓ Success:[/green] Extracted {extracted}")
            else:
                rprint(f"[red]✗ Failed:[/red] {error}")

    # Display results
    display_results(all_results, real_test_result)

    # Return success if all tests passed
    test_success = all(result["success"] for result in all_results.values())

    if real_test_result:
        test_success = test_success and real_test_result[0]

    return 0 if test_success else 1


if __name__ == "__main__":
    sys.exit(main())
