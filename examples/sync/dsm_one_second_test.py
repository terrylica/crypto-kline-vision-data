#!/usr/bin/env python3
"""One-second data retrieval test script.

This script tests the Data Source Manager's ability to handle one-second
data intervals without any deprecation warnings related to frequency strings.
"""

import sys
import warnings
from datetime import datetime, timedelta, timezone
from pathlib import Path

from rich.console import Console
from rich.panel import Panel

# Add project root to path if needed
project_root = Path(__file__).parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.append(str(project_root))

# Project imports (after path setup)
from core.sync.data_source_manager import DataSourceManager
from utils.dataframe_utils import verify_data_completeness
from utils.for_core.dsm_utilities import safely_reindex_dataframe
from utils.loguru_setup import configure_session_logging, logger
from utils.market_constraints import DataProvider, Interval, MarketType

# Console for rich output
console = Console()


def main():
    """Test one-second data retrieval and processing."""
    # Filter out all warnings to see if our fixes worked
    warnings.filterwarnings("error")

    # Configure logging
    main_log, error_log, _ = configure_session_logging("dsm_one_second_test", "INFO")
    logger.info(f"Logs: {main_log} and {error_log}")

    # Create DSM instance
    dsm = DataSourceManager.create(DataProvider.BINANCE, MarketType.SPOT)

    # Use one-second interval
    interval = Interval.SECOND_1

    # Define a small time window (just 2 minutes of data)
    end_time = datetime.now(timezone.utc).replace(microsecond=0)
    start_time = end_time - timedelta(minutes=2)

    console.print(Panel(f"Testing one-second data retrieval from {start_time} to {end_time}", style="blue"))

    try:
        # Retrieve data
        df = dsm.get_data(
            symbol="BTCUSDT",
            start_time=start_time,
            end_time=end_time,
            interval=interval,
        )

        console.print(f"[green]Successfully retrieved {len(df)} rows of one-second data[/green]")

        # Check data completeness
        is_complete, gaps = verify_data_completeness(df, start_time, end_time, interval="1s")
        if is_complete:
            console.print("[green]Data is complete - no gaps detected[/green]")
        else:
            console.print(f"[yellow]Found {len(gaps)} gaps in the data[/yellow]")

        # Test reindexing
        console.print("\n[blue]Testing reindexing with one-second data[/blue]")
        reindexed_df = safely_reindex_dataframe(
            df,
            start_time,
            end_time,
            interval="1s",
            fill_method="ffill",
        )
        console.print(f"[green]Successfully reindexed to {len(reindexed_df)} rows[/green]")

        # Print some sample data
        console.print("\n[blue]Sample of retrieved one-second data:[/blue]")
        if not df.empty:
            # Format the first 5 rows for display
            sample = df.head(5).copy()
            if "open_time" in sample.columns:
                sample["time"] = sample["open_time"].dt.strftime("%Y-%m-%d %H:%M:%S")
            else:
                sample["time"] = sample.index.strftime("%Y-%m-%d %H:%M:%S")

            for idx, row in enumerate(sample.itertuples()):
                console.print(f"Row {idx}: {row.time} - Open: {row.open:.2f}, Close: {row.close:.2f}")

        console.print("\n[green]✓ All tests completed successfully with no warnings![/green]")

    except Warning as w:
        console.print(f"[red]Warning occurred: {w}[/red]")
    except Exception as e:
        console.print(f"[red]Error occurred: {e}[/red]")


if __name__ == "__main__":
    main()
