#!/usr/bin/env python3
"""
Runner script to easily execute gap analysis tools.
Provides a simple interface to run the gap_debugger.py which uses utils/gap_detector.py internally.
"""

import argparse
import subprocess
import sys
from pathlib import Path

from utils.logger_setup import logger
from rich import print
from rich.console import Console
from rich.panel import Panel

console = Console()


def main():
    """Main function to run the gap analysis tools."""
    parser = argparse.ArgumentParser(
        description="Runner for gap analysis tools",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run the gap debugger on a specific file with default settings
  python run_gap_analysis.py --file path/to/data.csv
  
  # Run with custom interval (5 minute data)
  python run_gap_analysis.py --file path/to/data.csv --interval 5
  
  # Run with custom threshold and output file
  python run_gap_analysis.py --file path/to/data.csv --interval 1m --threshold 0.5 --output analysis_results.json
  
  # Run with a specific time column name
  python run_gap_analysis.py --file path/to/data.csv --time-column open_time
""",
    )

    # File parameter (required)
    parser.add_argument(
        "--file",
        type=str,
        required=True,
        help="Run the gap debugger on the specified data file",
    )

    # Gap debugger parameters
    parser.add_argument(
        "--interval",
        type=str,
        default="1",
        help="Time interval in minutes or as string (e.g., '1m', '5m', '1h') (default: 1)",
    )

    parser.add_argument(
        "--threshold",
        type=float,
        default=0.3,
        help="Threshold for identifying gaps (default: 0.3, meaning 30% above expected interval)",
    )

    parser.add_argument(
        "--time-column",
        type=str,
        default="timestamp",
        help="Name of the timestamp column in the data file (default: timestamp)",
    )

    parser.add_argument(
        "--output",
        type=str,
        help="Output file path for gap analysis (default: auto-generated)",
    )

    # Parse arguments
    args = parser.parse_args()

    # Get the directory containing this script
    script_dir = Path(__file__).parent

    try:
        # Run the gap debugger
        cmd = [
            sys.executable,
            str(script_dir / "gap_debugger.py"),
            f"--input={args.file}",
            f"--interval={args.interval}",
            f"--threshold={args.threshold}",
            f"--time-column={args.time_column}",
        ]

        # Add output if specified
        if args.output:
            cmd.append(f"--output={args.output}")

        # Display info panel
        panel = Panel(
            f"Input File: [green]{args.file}[/green]\n"
            f"Interval: [green]{args.interval}[/green]\n"
            f"Threshold: [green]{args.threshold}[/green]\n"
            f"Time Column: [green]{args.time_column}[/green]\n"
            f"Output: [green]{args.output if args.output else 'Auto-generated'}[/green]",
            title="Running Gap Debugger",
            border_style="blue",
        )
        console.print(panel)

        print(f"[cyan]Executing command:[/cyan] {' '.join(cmd)}")

        # Execute the command
        process = subprocess.run(cmd)
        if process.returncode != 0:
            print(
                f"[bold red]Command failed with exit code {process.returncode}[/bold red]"
            )

    except Exception as e:
        logger.exception("Error running gap analysis")
        print(f"[bold red]Error: {str(e)}[/bold red]")
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
