#!/usr/bin/env python3
import gc
import hashlib
import json
import math
import os
import platform
import resource
import statistics
import sys
import tempfile
import time
import traceback
import zipfile
from pathlib import Path
from typing import Any

import fsspec
import httpx
import pandas as pd
import psutil
import typer
from rich.console import Console
from rich.table import Table

# Ensure parent directory is in path
sys.path.append(str(Path(__file__).parent.parent.parent))

from rich import print

from utils.config import (
    HTTP_OK,
)
from utils.logger_setup import logger

app = typer.Typer(
    help="Benchmark ZIP file handling methods for Binance Vision API data",
    context_settings={"help_option_names": ["--help", "-h"]},
)


# Mock vision URL function similar to what's in vision_constraints.py
def get_vision_url(symbol, interval, date, market_type="spot", file_type="DATA"):
    """Generate a mock URL for Binance Vision API data."""
    date_str = date.strftime("%Y-%m-%d")
    base_url = "https://data.binance.vision/data"

    if market_type.lower() == "spot":
        url_path = f"{base_url}/spot/daily/klines/{symbol}/{interval}/{symbol}-{interval}-{date_str}"
    elif market_type.lower() == "futures_usdt" or market_type.lower() == "um":
        url_path = f"{base_url}/futures/um/daily/klines/{symbol}/{interval}/{symbol}-{interval}-{date_str}"
    elif market_type.lower() == "futures_coin" or market_type.lower() == "cm":
        perp_symbol = f"{symbol}_PERP" if "_PERP" not in symbol else symbol
        url_path = f"{base_url}/futures/cm/daily/klines/{perp_symbol}/{interval}/{perp_symbol}-{interval}-{date_str}"
    else:
        raise ValueError(f"Unsupported market type: {market_type}")

    if file_type.upper() == "DATA":
        return f"{url_path}.zip"
    if file_type.upper() == "CHECKSUM":
        return f"{url_path}.zip.CHECKSUM"
    raise ValueError(f"Unsupported file type: {file_type}")


def get_system_info() -> dict[str, Any]:
    """Get system information for benchmarking context."""
    memory = psutil.virtual_memory()
    try:
        cpu_freq = psutil.cpu_freq()
        cpu_freq_info = {
            "current": cpu_freq.current if cpu_freq else None,
            "min": cpu_freq.min if cpu_freq else None,
            "max": cpu_freq.max if cpu_freq else None,
        }
    except Exception:
        cpu_freq_info = {"error": "Could not determine CPU frequency"}

    return {
        "platform": platform.platform(),
        "python_version": platform.python_version(),
        "processor": platform.processor(),
        "cpu_count": psutil.cpu_count(logical=False),
        "cpu_count_logical": psutil.cpu_count(logical=True),
        "cpu_freq": cpu_freq_info,
        "memory_total_gb": round(memory.total / (1024**3), 2),
        "memory_available_gb": round(memory.available / (1024**3), 2),
    }


class BenchmarkRunner:
    def __init__(
        self, symbol: str, interval: str, date_str: str, market_type: str = "spot"
    ):
        """Initialize benchmark runner with parameters."""
        import pendulum

        self.symbol = symbol.upper()
        self.interval = interval
        self.date = pendulum.parse(date_str)
        self.market_type = market_type
        self.client = httpx.Client(
            timeout=30.0,
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept": "application/json, application/zip",
            },
            follow_redirects=True,
        )
        self.zip_path = None
        self.checksum_path = None
        self.file_size = 0

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, _exc_type, _exc_val, _exc_tb):
        """Context manager exit."""
        if self.client:
            self.client.close()
        if self.zip_path and Path(self.zip_path).exists():
            try:
                Path(self.zip_path).unlink()
            except Exception as e:
                logger.warning(f"Error cleaning up zip file: {e}")
        if self.checksum_path and Path(self.checksum_path).exists():
            try:
                Path(self.checksum_path).unlink()
            except Exception as e:
                logger.warning(f"Error cleaning up checksum file: {e}")

    def download_zip_file(self):
        """Download ZIP file from Binance Vision API."""
        url = get_vision_url(
            symbol=self.symbol,
            interval=self.interval,
            date=self.date,
            market_type=self.market_type,
            file_type="DATA",
        )

        logger.info(f"Downloading from URL: {url}")

        # Create temporary file with meaningful name
        filename = f"{self.symbol}-{self.interval}-{self.date.format('YYYY-MM-DD')}"
        temp_dir = tempfile.gettempdir()
        self.zip_path = str(Path(temp_dir) / f"{filename}.zip")

        # Download the file
        response = self.client.get(url)
        if response.status_code != HTTP_OK:
            raise Exception(f"HTTP error {response.status_code} while downloading file")

        # Save to temporary file
        with open(self.zip_path, "wb") as f:
            f.write(response.content)

        # Get file size
        self.file_size = Path(self.zip_path).stat().st_size

        logger.info(
            f"Downloaded file to {self.zip_path} ({self.file_size / 1024:.2f} KB)"
        )
        return self.zip_path

    def download_checksum(self) -> str:
        """Download checksum file from Binance Vision API."""
        url = get_vision_url(
            symbol=self.symbol,
            interval=self.interval,
            date=self.date,
            market_type=self.market_type,
            file_type="CHECKSUM",
        )

        logger.info(f"Downloading checksum from URL: {url}")

        # Create temporary file with meaningful name
        filename = f"{self.symbol}-{self.interval}-{self.date.format('YYYY-MM-DD')}"
        temp_dir = tempfile.gettempdir()
        self.checksum_path = str(Path(temp_dir) / f"{filename}.zip.CHECKSUM")

        # Download the file
        response = self.client.get(url)
        if response.status_code != HTTP_OK:
            raise Exception(
                f"HTTP error {response.status_code} while downloading checksum"
            )

        # Save to temporary file
        checksum_content = response.content.decode("utf-8").strip()
        with open(self.checksum_path, "w") as f:
            f.write(checksum_content)

        logger.info(f"Downloaded checksum to {self.checksum_path}")
        return checksum_content

    def compute_sha256(self, file_path: str) -> str:
        """Compute SHA-256 hash for a file."""
        sha256_hash = hashlib.sha256()
        with open(file_path, "rb") as f:
            # Read and update hash in chunks of 4K
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)
        return sha256_hash.hexdigest()

    def verify_checksum(self, file_path: str, expected_checksum: str) -> bool:
        """Verify the SHA-256 checksum of a file."""
        computed_hash = self.compute_sha256(file_path)
        # Handle case where checksum file might include filename
        if " " in expected_checksum:
            expected_checksum = expected_checksum.split(" ")[0]

        return computed_hash.lower() == expected_checksum.lower()

    def get_file_stats(self) -> dict[str, Any]:
        """Get statistics about the ZIP file and its contents."""
        if not self.zip_path or not Path(self.zip_path).exists():
            return {"error": "No ZIP file available"}

        try:
            stats = {
                "zip_file_size_bytes": self.file_size,
                "zip_file_size_kb": round(self.file_size / 1024, 2),
            }

            with zipfile.ZipFile(self.zip_path, "r") as zip_ref:
                csv_files = [f for f in zip_ref.namelist() if f.endswith(".csv")]
                if csv_files:
                    csv_file = csv_files[0]
                    info = zip_ref.getinfo(csv_file)
                    stats.update(
                        {
                            "csv_file_name": csv_file,
                            "csv_file_compressed_size_bytes": info.compress_size,
                            "csv_file_uncompressed_size_bytes": info.file_size,
                            "csv_file_compression_ratio": round(
                                (
                                    info.file_size / info.compress_size
                                    if info.compress_size
                                    else 1
                                ),
                                2,
                            ),
                        }
                    )

            return stats
        except Exception as e:
            return {"error": f"Error getting file stats: {e!s}"}

    def method_original(self, measure_memory=False, validate_checksum=False):
        """Original method using zipfile and tempfile."""
        # Force garbage collection before test
        gc.collect()

        start_memory = (
            resource.getrusage(resource.RUSAGE_SELF).ru_maxrss if measure_memory else 0
        )
        start_time = time.perf_counter()

        try:
            # Validate checksum if requested
            if validate_checksum:
                expected_checksum = self.download_checksum()
                checksum_valid = self.verify_checksum(self.zip_path, expected_checksum)
                if not checksum_valid:
                    raise ValueError("ZIP file checksum validation failed")

            with zipfile.ZipFile(self.zip_path, "r") as zip_ref:
                # Find the CSV file in the zip
                csv_files = [f for f in zip_ref.namelist() if f.endswith(".csv")]
                if not csv_files:
                    raise Exception("No CSV file found in ZIP")

                csv_file = csv_files[0]  # Take the first CSV file

                # Extract and process the CSV file
                with tempfile.TemporaryDirectory() as temp_dir:
                    zip_ref.extract(csv_file, temp_dir)
                    csv_path = os.path.join(temp_dir, csv_file)

                    # Read the CSV file (check for headers)
                    with open(csv_path) as f:
                        first_lines = []
                        for _ in range(3):
                            try:
                                line = next(f)
                                first_lines.append(line)
                            except StopIteration:
                                break
                        has_header = any(
                            "high" in line.lower() for line in first_lines[:1]
                        )

                    # Read CSV based on whether headers were detected
                    if has_header:
                        df = pd.read_csv(csv_path, header=0)
                    else:
                        # Standard Binance column names
                        columns = [
                            "open_time",
                            "open",
                            "high",
                            "low",
                            "close",
                            "volume",
                            "close_time",
                            "quote_asset_volume",
                            "count",
                            "taker_buy_base_volume",
                            "taker_buy_quote_volume",
                            "ignore",
                        ]
                        df = pd.read_csv(csv_path, header=None, names=columns)
        except Exception as e:
            logger.error(f"Error in original method: {e}\n{traceback.format_exc()}")
            raise

        elapsed = time.perf_counter() - start_time

        # Measure memory usage
        end_memory = (
            resource.getrusage(resource.RUSAGE_SELF).ru_maxrss if measure_memory else 0
        )
        memory_diff = end_memory - start_memory if measure_memory else 0

        return {
            "time": elapsed,
            "rows": len(df) if "df" in locals() else 0,
            "memory_kb": memory_diff,
        }

    def method_fsspec(self, measure_memory=False, validate_checksum=False):
        """Method using fsspec for accessing files in ZIP."""
        # Force garbage collection before test
        gc.collect()

        start_memory = (
            resource.getrusage(resource.RUSAGE_SELF).ru_maxrss if measure_memory else 0
        )
        start_time = time.perf_counter()

        try:
            # Validate checksum if requested
            if validate_checksum:
                expected_checksum = self.download_checksum()
                checksum_valid = self.verify_checksum(self.zip_path, expected_checksum)
                if not checksum_valid:
                    raise ValueError("ZIP file checksum validation failed")

            # Get list of files in the ZIP
            with zipfile.ZipFile(self.zip_path, "r") as zip_ref:
                # Find the CSV file in the zip
                csv_files = [f for f in zip_ref.namelist() if f.endswith(".csv")]
                if not csv_files:
                    raise Exception("No CSV file found in ZIP")

                csv_file = csv_files[0]  # Take the first CSV file

            # Directly open the file from ZIP and read to DataFrame
            with fsspec.open(f"zip://{csv_file}::{self.zip_path}", "rt") as f:
                # Check for headers by reading first few lines
                preview_lines = []
                for _ in range(3):
                    line = f.readline()
                    if not line:
                        break
                    preview_lines.append(line)

                # Reset file pointer
                f.seek(0)

                # Check if file has headers
                has_header = any("high" in line.lower() for line in preview_lines[:1])

                if has_header:
                    df = pd.read_csv(f, header=0)
                else:
                    # Standard Binance column names
                    columns = [
                        "open_time",
                        "open",
                        "high",
                        "low",
                        "close",
                        "volume",
                        "close_time",
                        "quote_asset_volume",
                        "count",
                        "taker_buy_base_volume",
                        "taker_buy_quote_volume",
                        "ignore",
                    ]
                    df = pd.read_csv(f, header=None, names=columns)
        except Exception as e:
            logger.error(f"Error in fsspec method: {e}")
            raise

        elapsed = time.perf_counter() - start_time

        # Measure memory usage
        end_memory = (
            resource.getrusage(resource.RUSAGE_SELF).ru_maxrss if measure_memory else 0
        )
        memory_diff = end_memory - start_memory if measure_memory else 0

        return {
            "time": elapsed,
            "rows": len(df) if "df" in locals() else 0,
            "memory_kb": memory_diff,
        }


def run_benchmark_case(
    symbol: str,
    interval: str,
    date: str,
    market_type: str,
    runs: int,
    warmup_runs: int,
    measure_memory: bool,
    validate_checksum: bool = False,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Run a single benchmark case with the given parameters."""
    print("[bold cyan]Running benchmark case[/bold cyan]")
    print(
        f"Symbol: {symbol}, Interval: {interval}, Date: {date}, Market: {market_type}"
    )
    print(
        f"Runs: {runs}, Warmup runs: {warmup_runs}, Validate checksum: {validate_checksum}"
    )

    # Initialize results list to store benchmark metrics
    results = []
    expected_checksum = None

    try:
        with BenchmarkRunner(symbol, interval, date, market_type) as runner:
            # Download the ZIP file and store path for later reference
            zip_path = runner.download_zip_file()
            logger.debug(f"Downloaded ZIP file to {zip_path}")

            # Get file stats
            file_stats = runner.get_file_stats()
            # Add file stats to results for reporting
            results.append({"file_stats": file_stats})

            # Get checksum once at the beginning if needed
            if validate_checksum:
                expected_checksum = runner.download_checksum()
                print(
                    "\n[yellow]Downloaded and verified checksum for all test runs[/yellow]"
                )
                checksum_valid = runner.verify_checksum(
                    runner.zip_path, expected_checksum
                )
                if not checksum_valid:
                    raise ValueError("ZIP file checksum validation failed")

            # Run warmup (results discarded)
            print("\n[yellow]Running warmup iterations...[/yellow]")
            for i in range(warmup_runs):
                print(f"  Warmup {i + 1}/{warmup_runs}")
                # Pass the pre-downloaded checksum to avoid network calls during timing
                if validate_checksum:
                    # Create monkey-patched methods that use the pre-downloaded checksum
                    def method_original_warmed(measure_memory=False):
                        # Force garbage collection before test
                        gc.collect()

                        start_memory = (
                            resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
                            if measure_memory
                            else 0
                        )
                        start_time = time.perf_counter()

                        try:
                            # Validate checksum (using pre-downloaded value)
                            checksum_valid = runner.verify_checksum(
                                runner.zip_path, expected_checksum
                            )
                            if not checksum_valid:
                                raise ValueError("ZIP file checksum validation failed")

                            with zipfile.ZipFile(runner.zip_path, "r") as zip_ref:
                                csv_files = [
                                    f for f in zip_ref.namelist() if f.endswith(".csv")
                                ]
                                if not csv_files:
                                    raise Exception("No CSV file found in ZIP")

                                csv_file = csv_files[0]

                                with tempfile.TemporaryDirectory() as temp_dir:
                                    zip_ref.extract(csv_file, temp_dir)
                                    csv_path = os.path.join(temp_dir, csv_file)

                                    with open(csv_path) as f:
                                        first_lines = []
                                        for _ in range(3):
                                            try:
                                                line = next(f)
                                                first_lines.append(line)
                                            except StopIteration:
                                                break
                                        has_header = any(
                                            "high" in line.lower()
                                            for line in first_lines[:1]
                                        )

                                    if has_header:
                                        df = pd.read_csv(csv_path, header=0)
                                    else:
                                        columns = [
                                            "open_time",
                                            "open",
                                            "high",
                                            "low",
                                            "close",
                                            "volume",
                                            "close_time",
                                            "quote_asset_volume",
                                            "count",
                                            "taker_buy_base_volume",
                                            "taker_buy_quote_volume",
                                            "ignore",
                                        ]
                                        df = pd.read_csv(
                                            csv_path, header=None, names=columns
                                        )
                        except Exception as e:
                            logger.error(f"Error in original method: {e}")
                            raise

                        elapsed = time.perf_counter() - start_time

                        end_memory = (
                            resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
                            if measure_memory
                            else 0
                        )
                        memory_diff = end_memory - start_memory if measure_memory else 0

                        return {
                            "time": elapsed,
                            "rows": len(df) if "df" in locals() else 0,
                            "memory_kb": memory_diff,
                        }

                    def method_fsspec_warmed(measure_memory=False):
                        # Force garbage collection before test
                        gc.collect()

                        start_memory = (
                            resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
                            if measure_memory
                            else 0
                        )
                        start_time = time.perf_counter()

                        try:
                            # Validate checksum (using pre-downloaded value)
                            checksum_valid = runner.verify_checksum(
                                runner.zip_path, expected_checksum
                            )
                            if not checksum_valid:
                                raise ValueError("ZIP file checksum validation failed")

                            with zipfile.ZipFile(runner.zip_path, "r") as zip_ref:
                                csv_files = [
                                    f for f in zip_ref.namelist() if f.endswith(".csv")
                                ]
                                if not csv_files:
                                    raise Exception("No CSV file found in ZIP")

                                csv_file = csv_files[0]

                            with fsspec.open(
                                f"zip://{csv_file}::{runner.zip_path}", "rt"
                            ) as f:
                                preview_lines = []
                                for _ in range(3):
                                    line = f.readline()
                                    if not line:
                                        break
                                    preview_lines.append(line)

                                f.seek(0)

                                has_header = any(
                                    "high" in line.lower() for line in preview_lines[:1]
                                )

                                if has_header:
                                    df = pd.read_csv(f, header=0)
                                else:
                                    columns = [
                                        "open_time",
                                        "open",
                                        "high",
                                        "low",
                                        "close",
                                        "volume",
                                        "close_time",
                                        "quote_asset_volume",
                                        "count",
                                        "taker_buy_base_volume",
                                        "taker_buy_quote_volume",
                                        "ignore",
                                    ]
                                    df = pd.read_csv(f, header=None, names=columns)
                        except Exception as e:
                            logger.error(f"Error in fsspec method: {e}")
                            raise

                        elapsed = time.perf_counter() - start_time

                        end_memory = (
                            resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
                            if measure_memory
                            else 0
                        )
                        memory_diff = end_memory - start_memory if measure_memory else 0

                        return {
                            "time": elapsed,
                            "rows": len(df) if "df" in locals() else 0,
                            "memory_kb": memory_diff,
                        }

                    # Use the warmed versions for the warmup runs
                    method_original_warmed()
                    method_fsspec_warmed()
                else:
                    runner.method_original(validate_checksum=False)
                    runner.method_fsspec(validate_checksum=False)

            # Test runs
            print("\n[yellow]Running benchmark iterations...[/yellow]")

            original_times = []
            fsspec_times = []
            original_memory = []
            fsspec_memory = []
            row_count = 0

            for i in range(runs):
                print(f"[cyan]Run {i + 1}/{runs}[/cyan]")

                # Original method
                try:
                    # Use the modified methods with pre-downloaded checksum if needed
                    if validate_checksum:
                        result = method_original_warmed(measure_memory=measure_memory)
                    else:
                        result = runner.method_original(
                            measure_memory=measure_memory, validate_checksum=False
                        )

                    original_times.append(result["time"])
                    if measure_memory:
                        original_memory.append(result["memory_kb"])
                    if i == 0:  # Only store row count once
                        row_count = result["rows"]
                    print(f"  Original method: {result['time']:.6f} seconds")
                    if measure_memory:
                        print(f"    Memory usage: {result['memory_kb']} KB")
                except Exception as e:
                    print(f"  [red]Original method failed: {e}[/red]")

                # fsspec method
                try:
                    # Use the modified methods with pre-downloaded checksum if needed
                    if validate_checksum:
                        result = method_fsspec_warmed(measure_memory=measure_memory)
                    else:
                        result = runner.method_fsspec(
                            measure_memory=measure_memory, validate_checksum=False
                        )

                    fsspec_times.append(result["time"])
                    if measure_memory:
                        fsspec_memory.append(result["memory_kb"])
                    print(f"  fsspec method:   {result['time']:.6f} seconds")
                    if measure_memory:
                        print(f"    Memory usage: {result['memory_kb']} KB")
                except Exception as e:
                    print(f"  [red]fsspec method failed: {e}[/red]")

            # Calculate statistics
            try:
                # Time stats
                original_stats = {
                    "avg": (
                        statistics.mean(original_times)
                        if original_times
                        else float("nan")
                    ),
                    "median": (
                        statistics.median(original_times)
                        if original_times
                        else float("nan")
                    ),
                    "stdev": (
                        statistics.stdev(original_times)
                        if len(original_times) > 1
                        else 0
                    ),
                    "min": min(original_times) if original_times else float("nan"),
                    "max": max(original_times) if original_times else float("nan"),
                }

                fsspec_stats = {
                    "avg": (
                        statistics.mean(fsspec_times) if fsspec_times else float("nan")
                    ),
                    "median": (
                        statistics.median(fsspec_times)
                        if fsspec_times
                        else float("nan")
                    ),
                    "stdev": (
                        statistics.stdev(fsspec_times) if len(fsspec_times) > 1 else 0
                    ),
                    "min": min(fsspec_times) if fsspec_times else float("nan"),
                    "max": max(fsspec_times) if fsspec_times else float("nan"),
                }

                # Memory stats if measured
                if measure_memory:
                    original_stats.update(
                        {
                            "memory_avg": (
                                statistics.mean(original_memory)
                                if original_memory
                                else float("nan")
                            ),
                            "memory_median": (
                                statistics.median(original_memory)
                                if original_memory
                                else float("nan")
                            ),
                            "memory_min": (
                                min(original_memory)
                                if original_memory
                                else float("nan")
                            ),
                            "memory_max": (
                                max(original_memory)
                                if original_memory
                                else float("nan")
                            ),
                        }
                    )

                    fsspec_stats.update(
                        {
                            "memory_avg": (
                                statistics.mean(fsspec_memory)
                                if fsspec_memory
                                else float("nan")
                            ),
                            "memory_median": (
                                statistics.median(fsspec_memory)
                                if fsspec_memory
                                else float("nan")
                            ),
                            "memory_min": (
                                min(fsspec_memory) if fsspec_memory else float("nan")
                            ),
                            "memory_max": (
                                max(fsspec_memory) if fsspec_memory else float("nan")
                            ),
                        }
                    )

                # Determine which method is faster
                if original_stats["median"] < fsspec_stats["median"]:
                    faster = "Original"
                    speedup = (
                        fsspec_stats["median"] / original_stats["median"]
                        if original_stats["median"] > 0
                        else float("nan")
                    )
                else:
                    faster = "fsspec"
                    speedup = (
                        original_stats["median"] / fsspec_stats["median"]
                        if fsspec_stats["median"] > 0
                        else float("nan")
                    )

                # Print detailed results
                console = Console()
                title = f"Benchmark Results: {symbol} {interval} {market_type} ({runs} runs)"
                if validate_checksum:
                    title += " with checksum validation"
                table = Table(title=title)

                table.add_column("Metric", style="cyan")
                table.add_column("Original Method", justify="right")
                table.add_column("fsspec Method", justify="right")

                table.add_row(
                    "Median Time (s)",
                    f"{original_stats['median']:.6f}",
                    f"{fsspec_stats['median']:.6f}",
                )
                table.add_row(
                    "Average Time (s)",
                    f"{original_stats['avg']:.6f}",
                    f"{fsspec_stats['avg']:.6f}",
                )
                table.add_row(
                    "Std Dev (s)",
                    f"{original_stats['stdev']:.6f}",
                    f"{fsspec_stats['stdev']:.6f}",
                )
                table.add_row(
                    "Min Time (s)",
                    f"{original_stats['min']:.6f}",
                    f"{fsspec_stats['min']:.6f}",
                )
                table.add_row(
                    "Max Time (s)",
                    f"{original_stats['max']:.6f}",
                    f"{fsspec_stats['max']:.6f}",
                )

                if measure_memory:
                    table.add_row("", "", "")
                    table.add_row(
                        "Median Memory (KB)",
                        f"{original_stats['memory_median']:.1f}",
                        f"{fsspec_stats['memory_median']:.1f}",
                    )
                    table.add_row(
                        "Average Memory (KB)",
                        f"{original_stats['memory_avg']:.1f}",
                        f"{fsspec_stats['memory_avg']:.1f}",
                    )

                table.add_row("", "", "")
                table.add_row("Rows Processed", str(row_count), str(row_count))

                console.print(table)

                # Print conclusion
                if not math.isnan(speedup):
                    color = "green" if faster == "fsspec" else "red"
                    print(
                        f"[{color}]{faster} method was {'faster' if speedup > 1 else 'slower'} by {speedup:.2f}x[/{color}]"
                    )
                else:
                    print("[yellow]Could not determine clear winner[/yellow]")

                # Prepare case results
                case_result = {
                    "config": {
                        "symbol": symbol,
                        "interval": interval,
                        "date": date,
                        "market_type": market_type,
                        "runs": runs,
                        "warmup_runs": warmup_runs,
                        "measure_memory": measure_memory,
                        "validate_checksum": validate_checksum,
                    },
                    "stats": {
                        "original": original_stats,
                        "fsspec": fsspec_stats,
                        "faster": faster,
                        "speedup": speedup,
                        "row_count": row_count,
                    },
                    "file_stats": file_stats,
                    "results": {
                        "original_times": original_times,
                        "fsspec_times": fsspec_times,
                    },
                }

                if measure_memory:
                    case_result["results"].update(
                        {
                            "original_memory": original_memory,
                            "fsspec_memory": fsspec_memory,
                        }
                    )

                return case_result, []

            except Exception as e:
                print(f"[red]Error calculating statistics: {e}[/red]")
                return {}, []

    except Exception as e:
        print(f"[red]Benchmark case failed: {e}[/red]")
        return {}, []


@app.command()
def benchmark_all(
    runs: int = typer.Option(
        15, "--runs", "-r", help="Number of benchmark runs to perform"
    ),
    warmup_runs: int = typer.Option(
        3, "--warmup", "-w", help="Number of warmup runs to perform"
    ),
    output: str | None = typer.Option(
        None, "--output", "-o", help="Output JSON file for detailed results"
    ),
    measure_memory: bool = typer.Option(
        False, "--memory", "-M", help="Measure memory usage (may affect timing results)"
    ),
    validate_checksum: bool = typer.Option(
        False, "--checksum", "-c", help="Validate checksum during benchmark"
    ),
):
    """Run comprehensive benchmarks across multiple configurations."""

    # Benchmark configurations to test
    test_cases = [
        # High-volume data
        {
            "symbol": "BTCUSDT",
            "interval": "1m",
            "date": "2023-12-01",
            "market_type": "spot",
        },
        {
            "symbol": "ETHUSDT",
            "interval": "1m",
            "date": "2023-12-01",
            "market_type": "spot",
        },
        {
            "symbol": "BTCUSDT",
            "interval": "1m",
            "date": "2023-12-01",
            "market_type": "um",
        },
        # Low-volume data
        {
            "symbol": "BTCUSDT",
            "interval": "1h",
            "date": "2023-12-01",
            "market_type": "spot",
        },
        {
            "symbol": "BTCUSDT",
            "interval": "1d",
            "date": "2023-12-01",
            "market_type": "spot",
        },
        # Different date
        {
            "symbol": "BTCUSDT",
            "interval": "1m",
            "date": "2023-06-01",
            "market_type": "spot",
        },
        # Less common symbol
        {
            "symbol": "ARBUSDT",
            "interval": "1m",
            "date": "2023-12-01",
            "market_type": "spot",
        },
    ]

    # Get system info for context
    sys_info = get_system_info()
    print("[bold]System Information:[/bold]")
    for key, value in sys_info.items():
        print(f"  {key}: {value}")

    all_results = []
    summary_data = []

    for i, case in enumerate(test_cases):
        print(f"\n[bold]Test Case {i + 1}/{len(test_cases)}[/bold]")

        result, errors = run_benchmark_case(
            symbol=case["symbol"],
            interval=case["interval"],
            date=case["date"],
            market_type=case["market_type"],
            runs=runs,
            warmup_runs=warmup_runs,
            measure_memory=measure_memory,
            validate_checksum=validate_checksum,
        )

        if result:
            all_results.append(result)

            # Extract data for summary
            config = result["config"]
            stats = result["stats"]
            file_stats = result["file_stats"]

            summary_data.append(
                {
                    "test_case": f"Case {i + 1}",
                    "symbol": config["symbol"],
                    "interval": config["interval"],
                    "market_type": config["market_type"],
                    "file_size_kb": file_stats.get("zip_file_size_kb", "N/A"),
                    "rows": stats["row_count"],
                    "original_median": round(
                        stats["original"]["median"] * 1000, 2
                    ),  # Convert to ms
                    "fsspec_median": round(
                        stats["fsspec"]["median"] * 1000, 2
                    ),  # Convert to ms
                    "speedup": (
                        round(stats["speedup"], 2)
                        if not math.isnan(stats["speedup"])
                        else "N/A"
                    ),
                    "winner": stats["faster"],
                }
            )

        # Add a separator between test cases
        print("\n" + "-" * 80)

    # Print summary table
    if summary_data:
        console = Console()
        title = f"Benchmark Summary ({runs} runs, {warmup_runs} warmup runs)"
        if validate_checksum:
            title += " with checksum validation"
        summary_table = Table(title=title)

        summary_table.add_column("Case", style="cyan")
        summary_table.add_column("Symbol")
        summary_table.add_column("Interval")
        summary_table.add_column("Market")
        summary_table.add_column("File Size (KB)", justify="right")
        summary_table.add_column("Rows", justify="right")
        summary_table.add_column("Original (ms)", justify="right")
        summary_table.add_column("fsspec (ms)", justify="right")
        summary_table.add_column("Speedup", justify="right")
        summary_table.add_column("Faster")

        for row in summary_data:
            color = "green" if row["winner"] == "fsspec" else "red"
            summary_table.add_row(
                row["test_case"],
                row["symbol"],
                row["interval"],
                row["market_type"],
                str(row["file_size_kb"]),
                str(row["rows"]),
                str(row["original_median"]),
                str(row["fsspec_median"]),
                (
                    f"[{color}]{row['speedup']}x[/{color}]"
                    if row["speedup"] != "N/A"
                    else "N/A"
                ),
                f"[{color}]{row['winner']}[/{color}]",
            )

        console.print(summary_table)

    # Save detailed results to JSON file
    if output and all_results:
        output_path = Path(output)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        output_data = {
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "system_info": sys_info,
            "test_cases": all_results,
            "summary": summary_data,
        }

        with open(output_path, "w") as f:
            json.dump(output_data, f, indent=2)

        print(f"\n[bold green]Detailed results saved to {output_path}[/bold green]")

    # Overall conclusion
    if summary_data:
        fsspec_wins = sum(1 for row in summary_data if row["winner"] == "fsspec")
        original_wins = sum(1 for row in summary_data if row["winner"] == "Original")

        print("\n[bold]Overall Conclusion:[/bold]")
        print(f"  fsspec was faster in {fsspec_wins} of {len(summary_data)} test cases")
        print(
            f"  Original method was faster in {original_wins} of {len(summary_data)} test cases"
        )

        # Calculate average speedup
        speedups = [row["speedup"] for row in summary_data if row["speedup"] != "N/A"]
        if speedups:
            avg_speedup = sum(speedups) / len(speedups)
            print(f"  Average speedup of the faster method: {avg_speedup:.2f}x")


@app.command()
def benchmark(
    symbol: str = typer.Option(
        "BTCUSDT", "--symbol", "-s", help="Symbol to download data for"
    ),
    interval: str = typer.Option("1m", "--interval", "-i", help="Kline interval"),
    date: str = typer.Option(
        "2023-12-01", "--date", "-d", help="Date in YYYY-MM-DD format"
    ),
    market_type: str = typer.Option(
        "spot", "--market", "-m", help="Market type (spot, um, cm)"
    ),
    runs: int = typer.Option(
        15, "--runs", "-r", help="Number of benchmark runs to perform"
    ),
    warmup_runs: int = typer.Option(
        3, "--warmup", "-w", help="Number of warmup runs to perform"
    ),
    measure_memory: bool = typer.Option(
        False, "--memory", "-M", help="Measure memory usage (may affect timing results)"
    ),
    validate_checksum: bool = typer.Option(
        False, "--checksum", "-c", help="Validate checksum during benchmark"
    ),
):
    """Run benchmark for a single configuration."""

    result, _ = run_benchmark_case(
        symbol=symbol,
        interval=interval,
        date=date,
        market_type=market_type,
        runs=runs,
        warmup_runs=warmup_runs,
        measure_memory=measure_memory,
        validate_checksum=validate_checksum,
    )

    if result:
        stats = result["stats"]
        print("\n[bold]Conclusion:[/bold]")
        if not math.isnan(stats["speedup"]):
            faster = stats["faster"]
            speedup = stats["speedup"]
            color = "green" if faster == "fsspec" else "red"
            print(
                f"  [{color}]{faster} method was {'faster' if speedup > 1 else 'slower'} by {speedup:.2f}x[/{color}]"
            )
        else:
            print("  [yellow]Could not determine clear winner[/yellow]")


@app.command()
def benchmark_checksum(
    symbol: str = typer.Option(
        "BTCUSDT", "--symbol", "-s", help="Symbol to download data for"
    ),
    interval: str = typer.Option("1m", "--interval", "-i", help="Kline interval"),
    date: str = typer.Option(
        "2023-12-01", "--date", "-d", help="Date in YYYY-MM-DD format"
    ),
    market_type: str = typer.Option(
        "spot", "--market", "-m", help="Market type (spot, um, cm)"
    ),
    runs: int = typer.Option(
        15, "--runs", "-r", help="Number of benchmark runs to perform"
    ),
    warmup_runs: int = typer.Option(
        3, "--warmup", "-w", help="Number of warmup runs to perform"
    ),
    measure_memory: bool = typer.Option(
        False, "--memory", "-M", help="Measure memory usage (may affect timing results)"
    ),
    output: str | None = typer.Option(
        None, "--output", "-o", help="Output JSON file for detailed results"
    ),
):
    """Run benchmark with and without checksum validation for direct comparison."""

    print("[bold]Benchmarking with and without checksum validation[/bold]")
    print(
        f"Symbol: {symbol}, Interval: {interval}, Date: {date}, Market: {market_type}"
    )

    # Run benchmark without checksum
    print("\n[bold cyan]Running benchmark WITHOUT checksum validation:[/bold cyan]")
    result_without_checksum, _ = run_benchmark_case(
        symbol=symbol,
        interval=interval,
        date=date,
        market_type=market_type,
        runs=runs,
        warmup_runs=warmup_runs,
        measure_memory=measure_memory,
        validate_checksum=False,
    )

    # Run benchmark with checksum
    print("\n[bold cyan]Running benchmark WITH checksum validation:[/bold cyan]")
    result_with_checksum, _ = run_benchmark_case(
        symbol=symbol,
        interval=interval,
        date=date,
        market_type=market_type,
        runs=runs,
        warmup_runs=warmup_runs,
        measure_memory=measure_memory,
        validate_checksum=True,
    )

    # Create comparison table
    if result_without_checksum and result_with_checksum:
        console = Console()
        title = f"Checksum Validation Impact: {symbol} {interval} {market_type} ({runs} runs)"
        comparison_table = Table(title=title)

        comparison_table.add_column("Metric", style="cyan")
        comparison_table.add_column("Without Checksum", justify="right")
        comparison_table.add_column("With Checksum", justify="right")
        comparison_table.add_column("Overhead", justify="right")

        # Original method comparison
        orig_without = (
            result_without_checksum["stats"]["original"]["median"] * 1000
        )  # ms
        orig_with = result_with_checksum["stats"]["original"]["median"] * 1000  # ms
        orig_overhead = (orig_with - orig_without) / orig_without * 100

        comparison_table.add_row(
            "Original Method (ms)",
            f"{orig_without:.2f}",
            f"{orig_with:.2f}",
            f"{orig_overhead:.1f}%",
        )

        # fsspec method comparison
        fsspec_without = (
            result_without_checksum["stats"]["fsspec"]["median"] * 1000
        )  # ms
        fsspec_with = result_with_checksum["stats"]["fsspec"]["median"] * 1000  # ms
        fsspec_overhead = (fsspec_with - fsspec_without) / fsspec_without * 100

        comparison_table.add_row(
            "fsspec Method (ms)",
            f"{fsspec_without:.2f}",
            f"{fsspec_with:.2f}",
            f"{fsspec_overhead:.1f}%",
        )

        # Winner comparison
        winner_without = result_without_checksum["stats"]["faster"]
        winner_with = result_with_checksum["stats"]["faster"]
        speedup_without = result_without_checksum["stats"]["speedup"]
        speedup_with = result_with_checksum["stats"]["speedup"]

        comparison_table.add_row("", "", "", "")
        comparison_table.add_row(
            "Faster Method",
            f"{winner_without} by {speedup_without:.2f}x",
            f"{winner_with} by {speedup_with:.2f}x",
            "N/A",
        )

        console.print(comparison_table)

        # Checksum overhead analysis
        print("\n[bold]Checksum Validation Analysis:[/bold]")
        print(f"  Original method overhead: {orig_overhead:.1f}%")
        print(f"  fsspec method overhead: {fsspec_overhead:.1f}%")

        if orig_overhead < fsspec_overhead:
            print(
                "  [green]Original method has less checksum validation overhead[/green]"
            )
        else:
            print(
                "  [green]fsspec method has less checksum validation overhead[/green]"
            )

        # Save results if requested
        if output:
            output_path = Path(output)
            output_path.parent.mkdir(parents=True, exist_ok=True)

            output_data = {
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
                "system_info": get_system_info(),
                "config": {
                    "symbol": symbol,
                    "interval": interval,
                    "date": date,
                    "market_type": market_type,
                    "runs": runs,
                    "warmup_runs": warmup_runs,
                },
                "without_checksum": result_without_checksum,
                "with_checksum": result_with_checksum,
                "comparison": {
                    "original_overhead_percent": orig_overhead,
                    "fsspec_overhead_percent": fsspec_overhead,
                    "better_for_checksums": (
                        "original" if orig_overhead < fsspec_overhead else "fsspec"
                    ),
                },
            }

            with open(output_path, "w") as f:
                json.dump(output_data, f, indent=2)

            print(
                f"\n[bold green]Comparison results saved to {output_path}[/bold green]"
            )


if __name__ == "__main__":
    app()
