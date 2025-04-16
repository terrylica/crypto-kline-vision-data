#!/usr/bin/env python3
"""
CLI tool for verifying checksums of files.

This tool provides a command-line interface for verifying the integrity of files
using SHA-256 checksums, with special handling for Binance Vision API files.
"""

import argparse
import sys
import time
from pathlib import Path

from rich.console import Console
from rich.panel import Panel
from rich import print as rprint

from utils.logger_setup import logger
from utils.for_core.vision_checksum import (
    verify_file_checksum,
    extract_checksum_from_file,
    calculate_checksums_multiple_methods,
)


def setup_argparse() -> argparse.ArgumentParser:
    """Set up command-line argument parsing."""
    parser = argparse.ArgumentParser(
        description="Verify file integrity using checksums"
    )

    parser.add_argument(
        "file_path",
        help="Path to the file to verify",
    )

    parser.add_argument(
        "--checksum-file",
        "-c",
        help="Path to the checksum file (default: <file_path>.CHECKSUM)",
    )

    parser.add_argument(
        "--calculate-only",
        "-o",
        action="store_true",
        help="Only calculate checksums without verification",
    )

    parser.add_argument(
        "-v", "--verbose", action="store_true", help="Enable verbose output"
    )

    return parser


def main() -> int:
    """Main entry point of the script."""
    console = Console()
    parser = setup_argparse()
    args = parser.parse_args()

    # Set log level based on verbosity
    logger.setLevel("DEBUG" if args.verbose else "INFO")

    # Convert file path to Path object
    file_path = Path(args.file_path)

    # Display header
    if args.verbose:
        rprint(
            Panel.fit(
                f"[bold]Checksum Verification[/bold]",
                subtitle=f"File: {file_path}",
            )
        )

    # Check if file exists
    if not file_path.exists():
        rprint(f"[red]✗ Error: File not found: {file_path}[/red]")
        return 1

    # If only calculating checksums
    if args.calculate_only:
        start_time = time.time()
        rprint(f"\n[bold]Calculating checksums for {file_path.name}...[/bold]")

        # Calculate checksums using all available methods
        results = calculate_checksums_multiple_methods(file_path)

        # Display results
        rprint(f"\n[bold]Checksums for {file_path.name}:[/bold]")
        for method, checksum in results.items():
            rprint(f"  [cyan]{method}:[/cyan] {checksum}")

        rprint(f"\nTime taken: {time.time() - start_time:.2f} seconds")
        return 0

    # Determine checksum file path
    if args.checksum_file:
        checksum_path = Path(args.checksum_file)
    else:
        # Try standard .CHECKSUM extension
        checksum_path = Path(f"{file_path}.CHECKSUM")

        # If not found, try without extension
        if not checksum_path.exists():
            checksum_path = Path(f"{file_path.parent / file_path.stem}.CHECKSUM")

    # Check if checksum file exists
    if not checksum_path.exists():
        rprint(f"[red]✗ Error: Checksum file not found: {checksum_path}[/red]")
        rprint(
            "[yellow]Hint: Use --checksum-file to specify the checksum file path[/yellow]"
        )
        return 1

    # Read expected checksum
    expected_checksum = extract_checksum_from_file(checksum_path)
    if not expected_checksum:
        rprint(f"[red]✗ Error: Could not extract checksum from {checksum_path}[/red]")
        return 1

    rprint(f"[bold]Expected checksum:[/bold] {expected_checksum}")

    # Verify checksum
    start_time = time.time()
    success, error = verify_file_checksum(file_path, checksum_path)
    verification_time = time.time() - start_time

    # Display results
    if success:
        rprint(
            f"[green]✓ Checksum verification successful for {file_path.name}[/green]"
        )
        rprint(f"Time taken: {verification_time:.2f} seconds")
        return 0
    else:
        rprint(f"[red]✗ Checksum verification failed:[/red] {error}")
        rprint(f"Time taken: {verification_time:.2f} seconds")
        return 1


if __name__ == "__main__":
    sys.exit(main())
