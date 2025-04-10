#!/usr/bin/env python
"""
Rate Limit Tester for Binance Data

This script tests Binance's rate limits by fetching 1-second data for multiple symbols
simultaneously. It monitors the rate limit headers returned by the API to detect
if we're approaching rate limiting.

Usage:
    python rate_limit_tester.py --duration 300  # Run for 5 minutes
"""

import asyncio
import time
import argparse
import signal
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

from utils.logger_setup import logger
from rich import print
from rich.console import Console
from rich.progress import (
    Progress,
    TextColumn,
    BarColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)

# For API access
from core.data_source_manager import DataSourceManager, DataSource
from utils.market_constraints import MarketType, Interval, DataProvider


# Rate limit tracking
class RateLimitTracker:
    """Track rate limit usage from API responses."""

    def __init__(self):
        """Initialize the rate limit tracker."""
        self.current_weight = 0
        self.max_weight = 6000
        self.weight_per_request = 2
        self.last_reset = time.time()
        self.warning_threshold = 0.8  # 80% of limit
        self.weight_history = []
        self.warnings = 0

    def update(self, weight):
        """Update current weight usage."""
        # Check if we're in a new minute
        current_time = time.time()
        if current_time - self.last_reset >= 60:
            # Reset for the new minute
            self.weight_history.append(self.current_weight)
            self.current_weight = weight
            self.last_reset = current_time
        else:
            self.current_weight = weight

        # Check for warnings
        usage_percentage = self.current_weight / self.max_weight
        if usage_percentage >= self.warning_threshold:
            self.warnings += 1
            return True
        return False

    def get_stats(self):
        """Get current statistics."""
        usage_percentage = (self.current_weight / self.max_weight) * 100
        return {
            "current_weight": self.current_weight,
            "max_weight": self.max_weight,
            "usage_percentage": usage_percentage,
            "warnings": self.warnings,
            "time_in_current_window": time.time() - self.last_reset,
        }


# Main rate limit tester class
class RateLimitTester:
    """Test Binance API rate limits by requesting data for multiple symbols."""

    def __init__(self, symbols, duration=300):
        """Initialize the rate limit tester.

        Args:
            symbols: List of symbols to test
            duration: Test duration in seconds
        """
        self.symbols = symbols
        self.duration = duration
        self.console = Console()
        self.tracker = RateLimitTracker()
        self.running = False
        self.test_start_time = None
        self.total_requests = 0
        self.successful_requests = 0
        self.failed_requests = 0

        # Configure minimal logging
        logger.setup_root(level="WARNING", show_filename=True)

    async def setup(self):
        """Set up the data source manager."""
        self.manager = DataSourceManager(
            market_type=MarketType.SPOT,
            provider=DataProvider.BINANCE,
            use_cache=False,
            # Increase concurrent limit for better performance
            max_concurrent=50,
        )
        return self

    async def fetch_data(self, symbol):
        """Fetch 1-second data for a single symbol.

        Args:
            symbol: Trading pair symbol

        Returns:
            Data frame with the fetched data
        """
        try:
            # Calculate time range for last 1000 seconds
            end_time = datetime.now(timezone.utc)
            start_time = end_time - timedelta(seconds=1000)

            # Fetch data
            df = await self.manager.get_data(
                symbol=symbol,
                start_time=start_time,
                end_time=end_time,
                interval=Interval.SECOND_1,
                enforce_source=DataSource.REST,  # Ensure we use REST API
            )

            # Extract rate limit info from manager internals
            # This is a bit hacky but necessary to get the weight headers
            rest_client = self.manager._rest_client
            if hasattr(rest_client, "_client") and rest_client._client:
                client = rest_client._client
                if hasattr(client, "headers"):
                    weight = int(client.headers.get("x-mbx-used-weight-1m", "0"))
                    self.tracker.update(weight)

            self.total_requests += 1
            self.successful_requests += 1
            return df
        except Exception as e:
            self.total_requests += 1
            self.failed_requests += 1
            logger.error(f"Error fetching {symbol}: {str(e)}")
            return None

    async def run_test(self):
        """Run the rate limit test."""
        self.running = True
        self.test_start_time = time.time()

        # Register signal handlers
        signal.signal(signal.SIGINT, self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)

        # Print test parameters
        self.console.print(
            f"[bold green]Starting rate limit test with {len(self.symbols)} symbols[/bold green]"
        )
        self.console.print(f"Test duration: {self.duration} seconds")
        self.console.print(
            f"Expected weight per request: {self.tracker.weight_per_request}"
        )
        self.console.print(f"Maximum weight per minute: {self.tracker.max_weight}")

        # Display progress bar
        with Progress(
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TimeElapsedColumn(),
            TimeRemainingColumn(),
            console=self.console,
        ) as progress:
            task = progress.add_task("[cyan]Running test...", total=self.duration)

            # Main test loop
            while self.running and (time.time() - self.test_start_time) < self.duration:
                # Update progress
                elapsed = time.time() - self.test_start_time
                progress.update(task, completed=min(elapsed, self.duration))

                # Create tasks for all symbols
                tasks = []
                for symbol in self.symbols:
                    tasks.append(self.fetch_data(symbol))

                # Run all tasks concurrently
                await asyncio.gather(*tasks)

                # Display stats
                stats = self.tracker.get_stats()
                self.console.print(
                    f"Requests: {self.total_requests} | "
                    f"Weight: {stats['current_weight']}/{stats['max_weight']} "
                    f"({stats['usage_percentage']:.1f}%) | "
                    f"Warnings: {stats['warnings']}"
                )

                # Wait until 1 second has passed since the start of this iteration
                cycle_duration = time.time() - (self.test_start_time + elapsed)
                if cycle_duration < 1.0:
                    await asyncio.sleep(1.0 - cycle_duration)

        # Print final statistics
        await self.print_final_stats()

    def _handle_signal(self, signum, frame):
        """Handle termination signals."""
        self.console.print(
            "\n[bold red]Received termination signal. Shutting down...[/bold red]"
        )
        self.running = False

    async def print_final_stats(self):
        """Print final test statistics."""
        self.console.print("\n[bold green]Rate Limit Test Completed[/bold green]")
        self.console.print(
            f"Total test duration: {time.time() - self.test_start_time:.1f} seconds"
        )
        self.console.print(f"Total requests: {self.total_requests}")
        self.console.print(f"Successful requests: {self.successful_requests}")
        self.console.print(f"Failed requests: {self.failed_requests}")

        stats = self.tracker.get_stats()
        self.console.print(
            f"Final weight usage: {stats['current_weight']}/{stats['max_weight']} ({stats['usage_percentage']:.1f}%)"
        )
        self.console.print(f"Total warnings: {stats['warnings']}")

        if stats["warnings"] > 0:
            self.console.print(
                "[bold red]WARNING: Rate limit threshold was exceeded during the test![/bold red]"
            )
        else:
            self.console.print(
                "[bold green]SUCCESS: No rate limit warnings were triggered.[/bold green]"
            )

    async def cleanup(self):
        """Clean up resources."""
        if hasattr(self, "manager"):
            await self.manager.__aexit__(None, None, None)


# Helper functions
def read_symbols_from_file(file_path):
    """Read symbols from a text file.

    Args:
        file_path: Path to the text file with one symbol per line

    Returns:
        List of symbols
    """
    symbols = []
    try:
        with open(file_path, "r") as f:
            for line in f:
                symbol = line.strip()
                if symbol:
                    symbols.append(symbol)
        return symbols
    except Exception as e:
        print(f"Error reading symbols file: {str(e)}")
        return []


async def main():
    """Run the rate limit test."""
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Test Binance API rate limits")
    parser.add_argument(
        "--duration", type=int, default=300, help="Test duration in seconds"
    )
    parser.add_argument(
        "--symbols",
        type=str,
        default=str(Path(__file__).parent / "symbols.txt"),
        help="Path to symbols file (one per line)",
    )
    args = parser.parse_args()

    # Read symbols from file
    symbols = read_symbols_from_file(args.symbols)
    if not symbols:
        print("No symbols found. Run extract_symbols.py first.")
        return

    print(f"Loaded {len(symbols)} symbols from {args.symbols}")

    # Initialize and run the tester
    tester = await RateLimitTester(symbols, args.duration).setup()
    try:
        await tester.run_test()
    except Exception as e:
        print(f"Error during test: {str(e)}")
    finally:
        await tester.cleanup()


if __name__ == "__main__":
    asyncio.run(main())
