#!/usr/bin/env python3
"""
Command-line utility for testing file checksums against checksum files.

This utility allows you to test if a file's checksum matches the expected value
in a checksum file, supporting various checksum formats and robust error handling.
"""

import argparse
import sys
import time
from pathlib import Path
from typing import Dict, Any

from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich import print as rprint

from utils.logger_setup import logger
from utils.for_core.vision_checksum import (
    extract_checksum_from_file,
    verify_file_checksum,
    calculate_checksums_multiple_methods,
)


def setup_argparse() -> argparse.ArgumentParser:
    """Set up command-line argument parsing."""
    parser = argparse.ArgumentParser(
        description="Test file checksum against a checksum file"
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
        "--verbose", "-v", action="store_true", help="Enable verbose output"
    )

    parser.add_argument(
        "--all-methods",
        "-a",
        action="store_true",
        help="Show results from all checksum calculation methods",
    )

    return parser


def test_checksum(
    file_path: Path,
    checksum_path: Path,
    verbose: bool = False,
    all_methods: bool = False,
) -> Dict[str, Any]:
    """
    Test a file's checksum against an expected value from a checksum file.

    Args:
        file_path: Path to the file to verify
        checksum_path: Path to the checksum file
        verbose: Whether to show verbose output
        all_methods: Whether to show results from all checksum methods

    Returns:
        Dictionary with test results
    """
    result = {
        "file_path": str(file_path),
        "checksum_path": str(checksum_path),
        "expected_checksum": None,
        "actual_checksum": None,
        "all_checksums": None,
        "validation_success": None,
        "validation_error": None,
        "time_taken": 0,
    }

    start_time = time.time()

    try:
        # Check if files exist
        if not file_path.exists():
            result["validation_error"] = f"File not found: {file_path}"
            return result

        if not checksum_path.exists():
            result["validation_error"] = f"Checksum file not found: {checksum_path}"
            return result

        # Extract expected checksum
        expected_checksum = extract_checksum_from_file(checksum_path)
        result["expected_checksum"] = expected_checksum

        if not expected_checksum:
            result["validation_error"] = (
                f"Could not extract checksum from {checksum_path}"
            )
            return result

        # Calculate checksums
        if all_methods:
            all_checksums = calculate_checksums_multiple_methods(file_path)
            result["all_checksums"] = all_checksums
            # Use the first method's result as the actual checksum for display
            if all_checksums:
                result["actual_checksum"] = next(iter(all_checksums.values()))
        else:
            # Calculate actual checksum
            actual_checksums = calculate_checksums_multiple_methods(file_path)
            if "data_validation" in actual_checksums:
                result["actual_checksum"] = actual_checksums["data_validation"]
            elif actual_checksums:
                result["actual_checksum"] = next(iter(actual_checksums.values()))

        # Verify checksum
        validation_success, validation_error = verify_file_checksum(
            file_path, checksum_path
        )
        result["validation_success"] = validation_success
        result["validation_error"] = validation_error

    except Exception as e:
        result["validation_error"] = str(e)
        result["validation_success"] = False

    # Record time taken
    result["time_taken"] = time.time() - start_time

    return result


def display_results(result: Dict[str, Any], verbose: bool = False) -> None:
    """
    Display test results in a rich table.

    Args:
        result: Dictionary with test results
        verbose: Whether to show verbose output
    """
    console = Console()

    # Display header
    rprint(
        Panel.fit(
            f"[bold]Checksum Verification Result[/bold]",
            subtitle=f"File: {Path(result['file_path']).name}",
        )
    )

    # Display summary
    summary_table = Table()
    summary_table.add_column("Property", style="cyan")
    summary_table.add_column("Value", style="green")

    summary_table.add_row("File", result["file_path"])
    summary_table.add_row("Checksum File", result["checksum_path"])
    summary_table.add_row("Expected Checksum", result["expected_checksum"] or "N/A")
    summary_table.add_row("Actual Checksum", result["actual_checksum"] or "N/A")

    status = (
        "[green]✓ PASSED[/green]"
        if result["validation_success"]
        else f"[red]✗ FAILED[/red]"
    )
    summary_table.add_row("Status", status)

    if result["validation_error"]:
        summary_table.add_row("Error", f"[red]{result['validation_error']}[/red]")

    summary_table.add_row("Time Taken", f"{result['time_taken']:.2f} seconds")

    console.print(summary_table)

    # If showing all methods
    if result.get("all_checksums"):
        methods_table = Table(title="All Checksum Methods")
        methods_table.add_column("Method", style="cyan")
        methods_table.add_column("Checksum", style="green")
        methods_table.add_column("Match with Expected", style="yellow")

        expected = result["expected_checksum"]

        for method, checksum in result["all_checksums"].items():
            match_status = ""
            if expected and checksum:
                match_status = (
                    "[green]✓ MATCH[/green]"
                    if expected.lower() == checksum.lower()
                    else "[red]✗ MISMATCH[/red]"
                )
            methods_table.add_row(method, checksum, match_status)

        console.print(methods_table)


def main() -> int:
    """
    Main entry point of the script.

    Returns:
        Exit code (0 for success, non-zero for failure)
    """
    parser = setup_argparse()
    args = parser.parse_args()

    # Set log level based on verbosity
    logger.setLevel("DEBUG" if args.verbose else "WARNING")

    # Convert file path to Path object
    file_path = Path(args.file_path)

    # Determine checksum file path
    if args.checksum_file:
        checksum_path = Path(args.checksum_file)
    else:
        # Try standard .CHECKSUM extension
        checksum_path = Path(f"{file_path}.CHECKSUM")

        # If not found, try without extension
        if not checksum_path.exists():
            checksum_path = Path(f"{file_path.parent / file_path.stem}.CHECKSUM")

    # Test checksum
    result = test_checksum(
        file_path=file_path,
        checksum_path=checksum_path,
        verbose=args.verbose,
        all_methods=args.all_methods,
    )

    # Display results
    display_results(result, args.verbose)

    # Return success or failure
    return 0 if result["validation_success"] else 1


if __name__ == "__main__":
    sys.exit(main())
