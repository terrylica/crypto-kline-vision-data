#!/usr/bin/env python3
"""
Display utilities for the Failover Control Protocol (FCP) mechanism.
"""

import pendulum
from pathlib import Path
from utils.logger_setup import logger
from rich import print
from rich.table import Table
from utils_for_debug.dataframe_output import (
    format_dataframe_for_display,
)


def display_results(
    df,
    symbol,
    market_type,
    interval,
    chart_type,
    log_timestamp=None,
    session_name="dsm_demo",
):
    """
    Display the results of the FCP data retrieval.

    Args:
        df: Pandas DataFrame containing the retrieved data
        symbol: Trading symbol (e.g., "BTCUSDT")
        market_type: Market type (SPOT, FUTURES_USDT, FUTURES_COIN)
        interval: Time interval between data points
        chart_type: Type of chart data
        log_timestamp: Optional timestamp for log files
        session_name: Name of the session used for log files (default: "dsm_demo")

    Returns:
        Path to CSV file if data was saved, None otherwise
    """
    if df is None or df.empty:
        print("[bold red]No data to display[/bold red]")
        return

    print(f"\n[bold green]Successfully retrieved {len(df)} records[/bold green]")

    # Create a table for source breakdown
    if "_data_source" in df.columns:
        source_counts = df["_data_source"].value_counts()

        source_table = Table(title="Data Source Breakdown")
        source_table.add_column("Source", style="cyan")
        source_table.add_column("Records", style="green", justify="right")
        source_table.add_column("Percentage", style="yellow", justify="right")

        for source, count in source_counts.items():
            percentage = count / len(df) * 100
            source_table.add_row(source, f"{count:,}", f"{percentage:.1f}%")

        print(source_table)

        # Show timeline visualization of source distribution
        print("\n[bold cyan]Source Distribution Timeline:[/bold cyan]")

        # First, create a new column with the date part only
        df["date"] = df["open_time"].dt.date
        date_groups = (
            df.groupby("date")["_data_source"].value_counts().unstack(fill_value=0)
        )

        # Display timeline visualization
        timeline_table = Table(title="Sources by Date")
        timeline_table.add_column("Date", style="cyan")

        # Add columns for each source found
        for source in source_counts.index:
            timeline_table.add_column(source, style="green", justify="right")

        # Add rows for each date
        for date, row in date_groups.iterrows():
            values = [str(date)]
            for source in source_counts.index:
                if source in row:
                    values.append(f"{row[source]:,}")
                else:
                    values.append("0")
            timeline_table.add_row(*values)

        print(timeline_table)

        # Show sample data from each source
        print(f"\n[bold cyan]Sample Data by Source:[/bold cyan]")
        for source in source_counts.index:
            source_df = df[df["_data_source"] == source].head(2)
            if not source_df.empty:
                print(f"\n[bold green]Records from {source} source:[/bold green]")
                # Format the display for better readability
                display_df = format_dataframe_for_display(source_df)
                # Display in a clean format
                print(display_df)
    else:
        print(
            "[bold yellow]Warning: Source information not available in the data[/bold yellow]"
        )

    # Save data to CSV
    # Convert market_type to string if it's an enum
    market_str = (
        market_type.name.lower()
        if hasattr(market_type, "name")
        else market_type.lower()
    )

    # Generate timestamp with pendulum
    timestamp = log_timestamp or pendulum.now("UTC").format("YYYYMMDD_HHmmss")

    # Define the CSV path using pendulum timestamp - save to workspace logs directory
    csv_dir = Path("logs") / session_name
    csv_dir.mkdir(parents=True, exist_ok=True)
    csv_path = csv_dir / f"{market_str}_{symbol}_{interval}_{timestamp}.csv"

    try:
        df.to_csv(csv_path)
        print(f"\n[bold green]Data saved to: {csv_path}[/bold green]")

        # Display log file paths
        print("\n[bold cyan]Log Files:[/bold cyan]")

        # If log_timestamp is provided, use it for log paths
        if log_timestamp:
            main_log_path = (
                Path("logs")
                / f"{session_name}_logs"
                / f"{session_name}_{log_timestamp}.log"
            )
            error_log_path = (
                Path("logs/error_logs") / f"{session_name}_errors_{log_timestamp}.log"
            )
        else:
            # Fall back to timestamp from CSV file if log_timestamp not provided
            main_log_path = (
                Path("logs")
                / f"{session_name}_logs"
                / f"{session_name}_{timestamp}.log"
            )
            error_log_path = (
                Path("logs/error_logs") / f"{session_name}_errors_{timestamp}.log"
            )

        # Check detailed logs
        if main_log_path.exists():
            log_size = main_log_path.stat().st_size
            print(f"[green]Detailed logs: {main_log_path} ({log_size:,} bytes)[/green]")
        else:
            # Try looking for a log file with a similar timestamp (within the same minute)
            found_log = False
            log_dir = Path("logs") / f"{session_name}_logs"
            if log_dir.exists():
                # First try exact match
                if main_log_path.exists():
                    found_log = True
                    log_size = main_log_path.stat().st_size
                    print(
                        f"[green]Detailed logs: {main_log_path} ({log_size:,} bytes)[/green]"
                    )
                else:
                    # Try to parse the timestamp from the expected log path
                    try:
                        log_timestamp_str = main_log_path.stem.split("_")[-1]
                        base_name = "_".join(main_log_path.stem.split("_")[:-1])

                        # Look for files with similar base name
                        for log_file in log_dir.glob(f"{base_name}_*.log"):
                            # If we already found a match, skip
                            if found_log:
                                break

                            # Get the timestamp from this file
                            file_timestamp_str = log_file.stem.split("_")[-1]

                            try:
                                # Try to parse both timestamps
                                expected_timestamp = pendulum.from_format(
                                    log_timestamp_str, "YYYYMMDD_HHmmss"
                                )
                                file_timestamp = pendulum.from_format(
                                    file_timestamp_str, "YYYYMMDD_HHmmss"
                                )

                                # Check if they're close (within 30 seconds)
                                if (
                                    abs(
                                        (
                                            expected_timestamp - file_timestamp
                                        ).total_seconds()
                                    )
                                    < 30
                                ):
                                    found_log = True
                                    log_size = log_file.stat().st_size
                                    print(
                                        f"[green]Detailed logs: {log_file} ({log_size:,} bytes)[/green]"
                                    )
                                    # Log the timestamp difference for debugging
                                    logger.debug(
                                        f"Found log file with similar timestamp: {file_timestamp_str} (difference: {(expected_timestamp - file_timestamp).total_seconds()} seconds)"
                                    )
                                    break
                            except Exception as e:
                                logger.debug(
                                    f"Error parsing timestamp for file {log_file}: {str(e)}"
                                )
                                continue
                    except Exception as e:
                        logger.debug(f"Error in timestamp comparison: {str(e)}")

            # If still not found after all attempts, show diagnostic info and file not found message
            if not found_log:
                # Add debug log to help diagnose the issue
                logger.debug(f"Log file not found at: {main_log_path}")
                # Check if directory exists
                logger.debug(f"Log directory exists: {log_dir.exists()}")

                # OS-level verification
                try:
                    import subprocess

                    # Run ls -la on the directory
                    ls_cmd = ["ls", "-la", str(log_dir)]
                    ls_result = subprocess.run(ls_cmd, capture_output=True, text=True)
                    logger.debug(f"Directory listing:\n{ls_result.stdout}")

                    # Try to stat the file directly
                    stat_cmd = ["stat", str(main_log_path)]
                    stat_result = subprocess.run(
                        stat_cmd, capture_output=True, text=True
                    )
                    if stat_result.returncode == 0:
                        logger.debug(
                            f"File exists according to stat but not Path.exists()!\n{stat_result.stdout}"
                        )
                    else:
                        logger.debug(
                            f"File not found by stat command: {stat_result.stderr}"
                        )
                except Exception as e:
                    logger.debug(f"Error during OS-level verification: {str(e)}")

                if log_dir.exists():
                    log_files = list(log_dir.glob("*.log"))
                    logger.debug(f"Found {len(log_files)} log files in directory")
                    for log_file in log_files:
                        logger.debug(f"Found log file: {log_file}")
                print(
                    f"[yellow]Detailed logs: {main_log_path} (file not found)[/yellow]"
                )

        # Check error logs
        if error_log_path.exists():
            error_size = error_log_path.stat().st_size
            if error_size > 0:
                print(
                    f"[yellow]Error logs: {error_log_path} ({error_size:,} bytes - contains errors)[/yellow]"
                )
            else:
                print(
                    f"[green]Error logs: {error_log_path} (empty - no errors)[/green]"
                )
        else:
            # Try looking for an error log file with a similar timestamp (within the same minute)
            found_error_log = False
            error_log_dir = Path("logs/error_logs")
            if error_log_dir.exists():
                # First try exact match
                if error_log_path.exists():
                    found_error_log = True
                    error_size = error_log_path.stat().st_size
                    if error_size > 0:
                        print(
                            f"[yellow]Error logs: {error_log_path} ({error_size:,} bytes - contains errors)[/yellow]"
                        )
                    else:
                        print(
                            f"[green]Error logs: {error_log_path} (empty - no errors)[/green]"
                        )
                else:
                    # Try to parse the timestamp from the expected log path
                    try:
                        error_timestamp_str = error_log_path.stem.split("_")[-1]
                        error_base_name = "_".join(error_log_path.stem.split("_")[:-1])

                        # Look for files with similar base name
                        for error_file in error_log_dir.glob(
                            f"{error_base_name}_*.log"
                        ):
                            # If we already found a match, skip
                            if found_error_log:
                                break

                            # Get the timestamp from this file
                            file_timestamp_str = error_file.stem.split("_")[-1]

                            try:
                                # Try to parse both timestamps
                                expected_timestamp = pendulum.from_format(
                                    error_timestamp_str, "YYYYMMDD_HHmmss"
                                )
                                file_timestamp = pendulum.from_format(
                                    file_timestamp_str, "YYYYMMDD_HHmmss"
                                )

                                # Check if they're close (within 30 seconds)
                                if (
                                    abs(
                                        (
                                            expected_timestamp - file_timestamp
                                        ).total_seconds()
                                    )
                                    < 30
                                ):
                                    found_error_log = True
                                    error_size = error_file.stat().st_size
                                    if error_size > 0:
                                        print(
                                            f"[yellow]Error logs: {error_file} ({error_size:,} bytes - contains errors)[/yellow]"
                                        )
                                    else:
                                        print(
                                            f"[green]Error logs: {error_file} (empty - no errors)[/green]"
                                        )
                                    # Log the timestamp difference for debugging
                                    logger.debug(
                                        f"Found error log file with similar timestamp: {file_timestamp_str} (difference: {(expected_timestamp - file_timestamp).total_seconds()} seconds)"
                                    )
                                    break
                            except Exception as e:
                                logger.debug(
                                    f"Error parsing timestamp for error file {error_file}: {str(e)}"
                                )
                                continue
                    except Exception as e:
                        logger.debug(f"Error in error timestamp comparison: {str(e)}")

            # If still not found after all attempts, show diagnostic info and file not found message
            if not found_error_log:
                # Add debug log to help diagnose the issue
                logger.debug(f"Error log file not found at: {error_log_path}")
                # Check if directory exists
                logger.debug(f"Error log directory exists: {error_log_dir.exists()}")

                # OS-level verification
                try:
                    import subprocess

                    # Run ls -la on the directory
                    ls_cmd = ["ls", "-la", str(error_log_dir)]
                    ls_result = subprocess.run(ls_cmd, capture_output=True, text=True)
                    logger.debug(f"Error directory listing:\n{ls_result.stdout}")

                    # Try to stat the file directly
                    stat_cmd = ["stat", str(error_log_path)]
                    stat_result = subprocess.run(
                        stat_cmd, capture_output=True, text=True
                    )
                    if stat_result.returncode == 0:
                        logger.debug(
                            f"Error file exists according to stat but not Path.exists()!\n{stat_result.stdout}"
                        )
                    else:
                        logger.debug(
                            f"Error file not found by stat command: {stat_result.stderr}"
                        )
                except Exception as e:
                    logger.debug(f"Error during OS-level verification: {str(e)}")

                if error_log_dir.exists():
                    error_files = list(error_log_dir.glob("*.log"))
                    logger.debug(
                        f"Found {len(error_files)} error log files in directory"
                    )
                    for error_file in error_files:
                        logger.debug(f"Found error log file: {error_file}")
                print(f"[yellow]Error logs: {error_log_path} (file not found)[/yellow]")

        # Update command for viewing logs to use absolute path
        print(
            f"\n[dim]To view logs: cat {Path('logs') / f'{session_name}_logs' / f'{session_name}_*.log'}[/dim]"
        )

        return csv_path
    except Exception as e:
        print(f"[bold red]Error saving data to CSV: {e}[/bold red]")
        return None
