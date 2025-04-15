#!/usr/bin/env python3
import sys
from pathlib import Path
import time
import tempfile
import zipfile
import os
import httpx
import pandas as pd
import fsspec
from concurrent.futures import ThreadPoolExecutor, as_completed
from rich.console import Console
from rich.table import Table
import typer
from typing import Optional, List, Dict, Any, Tuple
import json
import statistics
import gc
import platform
import psutil
import resource
import math

# Ensure parent directory is in path
sys.path.append(str(Path(__file__).parent.parent.parent))

from utils.logger_setup import logger
from rich import print

app = typer.Typer(
    help="Benchmark ZIP file handling methods for Binance Vision API data"
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
    elif file_type.upper() == "CHECKSUM":
        return f"{url_path}.zip.CHECKSUM"
    else:
        raise ValueError(f"Unsupported file type: {file_type}")


def get_system_info() -> Dict[str, Any]:
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
        self.file_size = 0

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        if self.client:
            self.client.close()
        if self.zip_path and Path(self.zip_path).exists():
            try:
                Path(self.zip_path).unlink()
            except Exception as e:
                logger.warning(f"Error cleaning up zip file: {e}")

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
        if response.status_code != 200:
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

    def get_file_stats(self) -> Dict[str, Any]:
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
            return {"error": f"Error getting file stats: {str(e)}"}

    def method_original(self, measure_memory=False):
        """Original method using zipfile and tempfile."""
        # Force garbage collection before test
        gc.collect()

        start_memory = (
            resource.getrusage(resource.RUSAGE_SELF).ru_maxrss if measure_memory else 0
        )
        start_time = time.perf_counter()

        try:
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
                    with open(csv_path, "r") as f:
                        first_lines = [next(f) for _ in range(3) if f]
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
            logger.error(f"Error in original method: {e}")
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

    def method_fsspec(self, measure_memory=False):
        """Method using fsspec for accessing files in ZIP."""
        # Force garbage collection before test
        gc.collect()

        start_memory = (
            resource.getrusage(resource.RUSAGE_SELF).ru_maxrss if measure_memory else 0
        )
        start_time = time.perf_counter()

        try:
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
) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    """Run a single benchmark case with the given parameters."""
    print(f"[bold cyan]Running benchmark case[/bold cyan]")
    print(
        f"Symbol: {symbol}, Interval: {interval}, Date: {date}, Market: {market_type}"
    )
    print(f"Runs: {runs}, Warmup runs: {warmup_runs}")

    results = []

    try:
        with BenchmarkRunner(symbol, interval, date, market_type) as runner:
            # Download the ZIP file
            zip_path = runner.download_zip_file()

            # Get file stats
            file_stats = runner.get_file_stats()

            # Run warmup (results discarded)
            print("\n[yellow]Running warmup iterations...[/yellow]")
            for i in range(warmup_runs):
                print(f"  Warmup {i+1}/{warmup_runs}")
                runner.method_original()
                runner.method_fsspec()

            # Test runs
            print("\n[yellow]Running benchmark iterations...[/yellow]")

            original_times = []
            fsspec_times = []
            original_memory = []
            fsspec_memory = []
            row_count = 0

            for i in range(runs):
                print(f"[cyan]Run {i+1}/{runs}[/cyan]")

                # Original method
                try:
                    result = runner.method_original(measure_memory=measure_memory)
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
                    result = runner.method_fsspec(measure_memory=measure_memory)
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
    output: Optional[str] = typer.Option(
        None, "--output", "-o", help="Output JSON file for detailed results"
    ),
    measure_memory: bool = typer.Option(
        False, "--memory", "-m", help="Measure memory usage (may affect timing results)"
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
        print(f"\n[bold]Test Case {i+1}/{len(test_cases)}[/bold]")

        result, errors = run_benchmark_case(
            symbol=case["symbol"],
            interval=case["interval"],
            date=case["date"],
            market_type=case["market_type"],
            runs=runs,
            warmup_runs=warmup_runs,
            measure_memory=measure_memory,
        )

        if result:
            all_results.append(result)

            # Extract data for summary
            config = result["config"]
            stats = result["stats"]
            file_stats = result["file_stats"]

            summary_data.append(
                {
                    "test_case": f"Case {i+1}",
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
        summary_table = Table(
            title=f"Benchmark Summary ({runs} runs, {warmup_runs} warmup runs)"
        )

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
    measure_memory: bool = typer.Option(False, "--memory", help="Measure memory usage"),
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


if __name__ == "__main__":
    app()
