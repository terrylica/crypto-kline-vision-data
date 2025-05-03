#!/usr/bin/env python
"""
Direct API Test for Binance

This script tests making direct API calls to Binance to fetch the most recent
data points without specifying time ranges.
"""

import argparse
import asyncio
import json
import time
from datetime import datetime
from pathlib import Path
from typing import List, Optional

import httpx
import pandas as pd
from rich import print
from rich.console import Console

from utils.config import HTTP_OK, SECONDS_IN_MINUTE
from utils.logger_setup import logger

# Configure logging
logger.setup_root(level="INFO")
console = Console()


class DirectApiTester:
    """Test class for simulating direct API rate limiting."""

    def __init__(
        self,
        max_weight_per_minute: int = 1200,
        max_requests_per_minute: int = 20,
        initial_weight: int = 0,
    ):
        """Initialize the API tester with rate limit parameters.

        Args:
            max_weight_per_minute: Maximum request weight per minute
            max_requests_per_minute: Maximum number of requests per minute
            initial_weight: Initial request weight
        """
        self.max_weight_per_minute = max_weight_per_minute
        self.max_requests_per_minute = max_requests_per_minute
        self.current_weight = initial_weight
        self.current_requests = 0
        self.last_reset = time.time()
        self.weight_history: List[int] = []
        self.request_history: List[int] = []
        self.start_time = time.time()

    def record_request(self, weight: int = 1) -> None:
        """Record a new API request with a given weight.

        Args:
            weight: Weight of the request (default: 1)
        """
        # Check if we're in a new minute
        current_time = time.time()
        if current_time - self.last_reset >= SECONDS_IN_MINUTE:
            # Reset for the new minute
            self.weight_history.append(self.current_weight)
            self.request_history.append(self.current_requests)
            self.current_weight = 0
            self.current_requests = 0
            self.last_reset = current_time

        # Record the request
        self.current_weight += weight
        self.current_requests += 1

        # Check if we would exceed rate limits
        if self.current_weight > self.max_weight_per_minute:
            logger.warning(
                f"Weight limit exceeded: {self.current_weight}/{self.max_weight_per_minute}"
            )

        if self.current_requests > self.max_requests_per_minute:
            logger.warning(
                f"Request count limit exceeded: {self.current_requests}/{self.max_requests_per_minute}"
            )

    def get_average_weight_per_minute(self) -> float:
        """Calculate the average weight per minute.

        Returns:
            Average weight per minute
        """
        total_weight = sum(self.weight_history) + self.current_weight
        elapsed_minutes = (time.time() - self.start_time) / SECONDS_IN_MINUTE
        return total_weight / max(1, elapsed_minutes)

    def get_current_usage(self) -> dict:
        """Get current usage statistics.

        Returns:
            Dictionary with current usage statistics
        """
        return {
            "current_weight": self.current_weight,
            "current_requests": self.current_requests,
            "weight_history": self.weight_history,
            "request_history": self.request_history,
            "avg_weight_per_minute": self.get_average_weight_per_minute(),
        }

    def simulate_request(
        self, endpoint: str, params: Optional[dict] = None, weight: int = 1
    ) -> dict:
        """Simulate an API request and record it.

        Args:
            endpoint: API endpoint to simulate
            params: Request parameters
            weight: Request weight

        Returns:
            Dictionary with request details and current usage
        """
        self.record_request(weight)
        return {
            "endpoint": endpoint,
            "params": params or {},
            "weight": weight,
            "usage": self.get_current_usage(),
        }


async def fetch_klines(symbol, interval="1s", limit=1000):
    """Fetch klines data directly from Binance API without specifying time ranges."""
    async with httpx.AsyncClient() as client:
        # Build parameters without start/end time
        params = {
            "symbol": symbol.upper(),
            "interval": interval,
            "limit": limit,
        }

        # API endpoint
        endpoint_url = "https://api.binance.com/api/v3/klines"

        # Make the API request
        response = await client.get(endpoint_url, params=params)

        # Extract rate limit info
        weight = int(response.headers.get("x-mbx-used-weight-1m", "0"))

        # Process response data
        if response.status_code == HTTP_OK:
            data = response.json()
            return data, weight
        logger.error(f"Error fetching {symbol}: {response.text}")
        return None, weight


def process_kline_data(data):
    """Process klines data into a DataFrame."""
    if not data:
        return pd.DataFrame()

    # Create DataFrame
    df = pd.DataFrame(
        data,
        columns=[
            "open_time",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "close_time",
            "quote_volume",
            "count",
            "taker_buy_volume",
            "taker_buy_quote_volume",
            "ignore",
        ],
    )

    # Convert timestamp to datetime
    df["open_time"] = pd.to_datetime(df["open_time"], unit="ms")
    df["close_time"] = pd.to_datetime(df["close_time"], unit="ms")

    # Set index
    df.set_index("open_time", inplace=True)

    return df


async def run_test(symbols, duration=30, limit=1000):
    """Run the rate limit test using direct API calls."""
    tracker = DirectApiTester()
    total_requests = 0
    successful_requests = 0
    failed_requests = 0
    initial_weight = None

    console.print(
        f"[bold green]Starting direct API test with {len(symbols)} symbols[/bold green]"
    )
    console.print(f"Test duration: {duration} seconds")
    console.print(f"Data points per request: {limit}")
    console.print(f"Expected weight per request: {tracker.max_weight_per_minute}")
    console.print(f"Maximum weight per minute: {tracker.max_weight_per_minute}")

    # Save test output details to file
    output_dir = Path(__file__).parent / "results"
    output_dir.mkdir(exist_ok=True)
    output_file = (
        output_dir / f"direct_api_test_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    )

    test_results = {
        "start_time": datetime.now().isoformat(),
        "symbols": symbols,
        "duration": duration,
        "limit": limit,
        "requests": [],
    }

    start_time = time.time()
    running = True

    try:
        # Main test loop
        while running and (time.time() - start_time) < duration:
            # Create tasks for all symbols
            tasks = []
            for symbol in symbols:
                task = fetch_klines(symbol, limit=limit)
                tasks.append(task)

            # Run all tasks concurrently
            results = await asyncio.gather(*tasks, return_exceptions=True)

            # Process results
            for i, (symbol, result) in enumerate(zip(symbols, results)):
                total_requests += 1

                if isinstance(result, Exception):
                    failed_requests += 1
                    logger.error(f"Error fetching {symbol}: {str(result)}")
                    continue

                data, weight = result

                # Capture initial weight value
                if initial_weight is None and weight > 0:
                    initial_weight = weight
                    console.print(f"[cyan]Initial weight: {initial_weight}[/cyan]")

                if data:
                    successful_requests += 1

                    # For the first symbol, show some data
                    if i == 0:
                        df = process_kline_data(data)
                        if not df.empty:
                            console.print(f"[green]Sample data for {symbol}:[/green]")
                            console.print(df.head(3))

                    # Update rate limit
                    tracker.record_request(weight)

                    # Save request details
                    test_results["requests"].append(
                        tracker.simulate_request(
                            "https://api.binance.com/api/v3/klines",
                            params={
                                "symbol": symbol.upper(),
                                "interval": "1s",
                                "limit": limit,
                            },
                            weight=weight,
                        )
                    )
                else:
                    failed_requests += 1

            # Display stats
            stats = tracker.get_current_usage()
            console.print(
                f"Requests: {total_requests} | "
                f"Weight: {stats['current_weight']}/{tracker.max_weight_per_minute} "
                f"({stats['avg_weight_per_minute']:.1f}) | "
                f"Requests: {stats['current_requests']}/{tracker.max_requests_per_minute}"
            )

            # Wait until 1 second has passed since the start of this iteration
            cycle_duration = time.time() - (time.time() % 1)
            if cycle_duration < 1.0:
                await asyncio.sleep(1.0 - cycle_duration)
    except KeyboardInterrupt:
        console.print("[bold red]Test interrupted by user[/bold red]")
    finally:
        # Print final statistics
        test_duration = time.time() - start_time
        console.print("\n[bold green]Direct API Test Completed[/bold green]")
        console.print(f"Total test duration: {test_duration:.1f} seconds")
        console.print(f"Total requests: {total_requests}")
        console.print(f"Successful requests: {successful_requests}")
        console.print(f"Failed requests: {failed_requests}")

        stats = tracker.get_current_usage()
        net_weight = stats["current_weight"] - (initial_weight or 0)
        console.print(
            f"Final weight usage: {stats['current_weight']}/{tracker.max_weight_per_minute} ({stats['avg_weight_per_minute']:.1f})"
        )
        console.print(f"Initial weight: {initial_weight or 0}")
        console.print(f"Net weight increase: {net_weight}")
        console.print(
            f"Weight per request: {net_weight / total_requests if total_requests > 0 else 0:.2f}"
        )
        console.print(f"Total requests: {stats['current_requests']}")

        # Save final results to file
        test_results["end_time"] = datetime.now().isoformat()
        test_results["total_requests"] = total_requests
        test_results["successful_requests"] = successful_requests
        test_results["failed_requests"] = failed_requests
        test_results["initial_weight"] = initial_weight or 0
        test_results["final_weight"] = stats["current_weight"]
        test_results["net_weight"] = net_weight

        with open(output_file, "w") as f:
            json.dump(test_results, f, indent=2)

        console.print(f"Test results saved to {output_file}")


async def main():
    """Main entry point."""
    # Read symbols from file
    symbols_file = Path(__file__).parent / "symbols.txt"
    with open(symbols_file, "r") as f:
        symbols = [line.strip() for line in f.readlines() if line.strip()]

    # Default to 50 symbols for testing
    symbols = symbols[:50]

    # Get duration from arguments
    import sys

    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Test direct API calls to Binance")
    parser.add_argument(
        "--duration", type=int, default=30, help="Test duration in seconds"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=1000,
        help="Number of data points to fetch per symbol",
    )
    parser.add_argument(
        "--symbols", type=int, default=50, help="Number of symbols to test"
    )

    # If no args provided, use sys.argv
    if len(sys.argv) > 1:
        args = parser.parse_args()
        duration = args.duration
        limit = args.limit
        symbol_count = args.symbols
        symbols = symbols[:symbol_count]
    else:
        # Legacy mode - support old command line format
        duration = 30  # Default duration
        limit = 1000  # Default limit
        if len(sys.argv) > 1:
            try:
                duration = int(sys.argv[1])
            except ValueError:
                pass

    # Print test configuration
    print("Test configuration:")
    print(f"- Duration: {duration} seconds")
    print(f"- Limit: {limit} data points per symbol")
    print(f"- Symbols: {len(symbols)} symbols")

    await run_test(symbols, duration, limit=limit)


if __name__ == "__main__":
    asyncio.run(main())
