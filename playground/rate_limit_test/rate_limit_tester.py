#!/usr/bin/env python3
"""
Rate Limit Tester for Binance Data

This script tests Binance's rate limits by fetching 1-second data for multiple symbols
simultaneously. It monitors the rate limit headers returned by the API to detect
if we're approaching rate limiting.

Usage:
    python rate_limit_tester.py --duration 300  # Run for 5 minutes
"""

import argparse
import signal
import time
from pathlib import Path

import pandas as pd
from rich import print
from rich.console import Console
from rich.progress import (
    BarColumn,
    Progress,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)

from core.providers.binance.rest_data_client import RestDataClient

# For API access
from core.sync.data_source_manager import DataSourceManager
from utils.config import SECONDS_IN_MINUTE
from utils.logger_setup import logger
from utils.market_constraints import DataProvider, Interval, MarketType
from utils.network_utils import create_httpx_client


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
        if current_time - self.last_reset >= SECONDS_IN_MINUTE:
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
        self.http_client = None

        # Configure minimal logging
        logger.setup_root(level="WARNING")

    def setup(self):
        """Set up the data source manager and REST client."""
        self.manager = DataSourceManager(
            market_type=MarketType.SPOT,
            provider=DataProvider.BINANCE,
            use_cache=False,
        )

        # Create REST client directly
        self.rest_client = RestDataClient(market_type=MarketType.SPOT, retry_count=3)

        # Create httpx client
        self.http_client = create_httpx_client(timeout=30.0)

        return self

    def fetch_data(self, symbol):
        """Fetch 1-second data for a single symbol.

        Args:
            symbol: Trading pair symbol

        Returns:
            Data frame with the fetched data
        """
        try:
            # Build parameters without start/end time
            params = {
                "symbol": symbol,
                "interval": Interval.SECOND_1.value,
                "limit": 1000,
            }

            # Get endpoint URL
            endpoint_url = self.rest_client._get_klines_endpoint()

            # Call endpoint directly using the httpx client
            response = self.http_client.get(endpoint_url, params=params)

            # Extract rate limit info from response headers
            if hasattr(response, "headers"):
                weight = int(response.headers.get("x-mbx-used-weight-1m", "0"))
                self.tracker.update(weight)

            # Process response data
            data = response.json()
            if data and isinstance(data, list):
                # Convert to DataFrame manually since we're not using the async client
                columns = [
                    "open_time",
                    "open",
                    "high",
                    "low",
                    "close",
                    "volume",
                    "close_time",
                    "quote_volume",
                    "trades",
                    "taker_buy_base_volume",
                    "taker_buy_quote_volume",
                    "ignore",
                ]
                df = pd.DataFrame(data, columns=columns)

                # Convert timestamps to datetime
                df["open_time"] = pd.to_datetime(df["open_time"], unit="ms")
                df["close_time"] = pd.to_datetime(df["close_time"], unit="ms")

                # Convert numeric columns
                for col in [
                    "open",
                    "high",
                    "low",
                    "close",
                    "volume",
                    "quote_volume",
                    "trades",
                    "taker_buy_base_volume",
                    "taker_buy_quote_volume",
                ]:
                    df[col] = pd.to_numeric(df[col])
            else:
                df = pd.DataFrame()

            self.total_requests += 1
            self.successful_requests += 1
            return df
        except Exception as e:
            self.total_requests += 1
            self.failed_requests += 1
            logger.error(f"Error fetching {symbol}: {e!s}")
            return None

    def run_test(self):
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

                # Process symbols one at a time
                for symbol in self.symbols:
                    self.fetch_data(symbol)

                # Display stats
                stats = self.tracker.get_stats()
                self.console.print(
                    f"Requests: {self.total_requests} | "
                    f"Weight: {stats['current_weight']}/{stats['max_weight']} "
                    f"({stats['usage_percentage']:.1f}%) | "
                    f"Warnings: {stats['warnings']}"
                )

                # Sleep for a second before next iteration
                remaining_time = 1.0 - (time.time() - (self.test_start_time + elapsed))
                if remaining_time > 0:
                    time.sleep(remaining_time)

        # Print final statistics
        self.print_final_stats()

    def _handle_signal(self, _signum, _frame):
        """Handle termination signals.

        Args:
            _signum: Signal number (unused but required by signal handler interface)
            _frame: Current stack frame (unused but required by signal handler interface)
        """
        self.console.print(
            "\n[bold red]Received termination signal. Shutting down...[/bold red]"
        )
        self.running = False

    def print_final_stats(self):
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

    def cleanup(self):
        """Clean up resources."""
        # Clean up DataSourceManager
        if hasattr(self, "manager"):
            self.manager.__exit__(None, None, None)

        # Clean up RestDataClient
        if hasattr(self, "rest_client") and self.rest_client._client:
            self.rest_client.close()

        # Clean up httpx client
        if hasattr(self, "http_client"):
            self.http_client.close()


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
        print(f"Error reading symbols file: {e!s}")
        return []


def main():
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
        print(
            "No symbols found. Make sure symbols.txt exists with one symbol per line."
        )
        return

    print(f"Loaded {len(symbols)} symbols from {args.symbols}")

    # Initialize and run the tester
    tester = RateLimitTester(symbols, args.duration).setup()
    try:
        tester.run_test()
    except Exception as e:
        print(f"Error during test: {e!s}")
    finally:
        tester.cleanup()


if __name__ == "__main__":
    main()
