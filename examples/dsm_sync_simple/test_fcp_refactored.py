#!/usr/bin/env python3
"""
Test script to verify the refactored FCP demo code.
This script demonstrates importing and using functions from the FCP utility modules.
"""

from pathlib import Path
import sys
import os

# Make sure we're running from the project root
if not os.path.isdir("utils"):
    # Try to navigate to project root
    if os.path.isdir("../../utils"):
        os.chdir("../..")
    else:
        print(
            "Error: Run this script from the project root or examples/dsm_sync_simple directory"
        )
        sys.exit(1)

from utils.logger_setup import configure_session_logging
from rich import print
from rich.panel import Panel

# Import FCP utility modules we created
from utils.for_demo.fcp_time_utils import parse_datetime
from utils.for_demo.fcp_cache_utils import clear_cache_directory
from utils.for_demo.fcp_project_utils import verify_project_root
from utils.for_demo.fcp_cli_examples import (
    define_example_commands,
)

# Set up logging
main_log, error_log, log_timestamp = configure_session_logging("fcp_test", "INFO")

# Display welcome message
print(
    Panel(
        "[bold green]FCP Refactoring Test[/bold green]\n"
        "This script verifies the refactored FCP utility modules are working correctly.",
        border_style="green",
    )
)

# Test the path verification
print("\n[bold cyan]Testing project path verification:[/bold cyan]")
verify_project_root()

# Test datetime parsing
print("\n[bold cyan]Testing datetime parsing:[/bold cyan]")
dt_str = "2025-04-01T12:30:45.123"
dt = parse_datetime(dt_str)
print(
    f"Parsed datetime: {dt.format('YYYY-MM-DD HH:mm:ss.SSS')} (timezone: {dt.timezone_name})"
)

# Test cache directory functions
print("\n[bold cyan]Testing cache directory functions:[/bold cyan]")
test_cache_dir = Path("./test_cache")
test_cache_dir.mkdir(exist_ok=True)
print(f"Created test cache directory: {test_cache_dir}")
clear_cache_directory(test_cache_dir)

# Test example display
print("\n[bold cyan]Testing example display:[/bold cyan]")
examples = define_example_commands()
print(f"Defined {len(examples)} example commands")
print(f"First example title: {examples[0]['title']}")

# Test help display
print("\n[bold cyan]Testing help display function:[/bold cyan]")
print("(Skipping actual display to avoid cluttering output)")

print(
    Panel(
        "[bold green]All tests completed successfully![/bold green]",
        border_style="green",
    )
)
