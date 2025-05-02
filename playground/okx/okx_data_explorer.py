#!/usr/bin/env python3
"""
OKX Data Explorer

A tool to explore and download available data from the OKX CDN.
"""

import argparse
import concurrent.futures
import datetime
import zipfile
from pathlib import Path
from typing import Dict, List, Tuple

import httpx
import pandas as pd
from rich import print
from rich.console import Console
from rich.progress import Progress
from rich.table import Table

# Constants
BASE_URL = "https://www.okx.com/cdn/okex/traderecords"
DATA_TYPES = ["trades", "aggtrades"]
DEFAULT_SYMBOLS = ["BTC-USDT", "ETH-USDT", "BTC-USD", "ETH-USD"]
DEFAULT_TIMEOUT = 10.0  # seconds
MAX_WORKERS = 5

console = Console()


def format_date(date_obj: datetime.date) -> Tuple[str, str]:
    """
    Format a date object into two required formats:
    - Directory format: YYYYMMDD
    - Filename format: YYYY-MM-DD

    Args:
        date_obj: A datetime.date object

    Returns:
        Tuple of (directory_format, filename_format)
    """
    dir_format = date_obj.strftime("%Y%m%d")
    file_format = date_obj.strftime("%Y-%m-%d")
    return dir_format, file_format


def build_url(data_type: str, symbol: str, date_obj: datetime.date) -> str:
    """
    Build a URL for the given parameters

    Args:
        data_type: Type of data (trades, aggtrades)
        symbol: Trading pair symbol (e.g., BTC-USDT)
        date_obj: Date for the data

    Returns:
        Full URL to the data file
    """
    dir_date, file_date = format_date(date_obj)
    return (
        f"{BASE_URL}/{data_type}/daily/{dir_date}/{symbol}-{data_type}-{file_date}.zip"
    )


def check_url_exists(url: str, timeout: float = DEFAULT_TIMEOUT) -> bool:
    """
    Check if a URL exists by making a HEAD request

    Args:
        url: URL to check
        timeout: Request timeout in seconds

    Returns:
        True if the URL exists, False otherwise
    """
    try:
        with httpx.Client() as client:
            response = client.head(url, timeout=timeout, follow_redirects=True)
            return response.status_code == 200
    except Exception as e:
        print(f"[red]Error checking URL {url}: {str(e)}[/red]")
        return False


def download_file(
    url: str, output_path: Path, timeout: float = DEFAULT_TIMEOUT
) -> bool:
    """
    Download a file from the given URL

    Args:
        url: URL to download from
        output_path: Path to save the file to
        timeout: Request timeout in seconds

    Returns:
        True if download was successful, False otherwise
    """
    try:
        with httpx.Client() as client:
            with Progress() as progress:
                task = progress.add_task(
                    f"[cyan]Downloading {output_path.name}...", total=None
                )

                with client.stream(
                    "GET", url, timeout=timeout, follow_redirects=True
                ) as response:
                    response.raise_for_status()

                    # Get content length if available
                    total_size = int(response.headers.get("Content-Length", 0))
                    if total_size > 0:
                        progress.update(task, total=total_size)

                    # Download the file
                    with open(output_path, "wb") as f:
                        downloaded = 0
                        for chunk in response.iter_bytes():
                            f.write(chunk)
                            downloaded += len(chunk)
                            if total_size > 0:
                                progress.update(task, completed=downloaded)

                progress.update(
                    task,
                    completed=total_size if total_size > 0 else 1,
                    total=total_size if total_size > 0 else 1,
                )

        return True
    except Exception as e:
        print(f"[red]Error downloading {url}: {str(e)}[/red]")
        return False


def explore_date_range(
    start_date: datetime.date,
    end_date: datetime.date,
    data_types: List[str],
    symbols: List[str],
) -> Dict[str, List[Tuple[str, datetime.date]]]:
    """
    Explore available data in a date range

    Args:
        start_date: Start date for exploration
        end_date: End date for exploration
        data_types: List of data types to check
        symbols: List of symbols to check

    Returns:
        Dictionary of available files grouped by data type
    """
    available_files = {data_type: [] for data_type in data_types}

    # Create a list of dates to check
    dates = []
    current_date = start_date
    while current_date <= end_date:
        dates.append(current_date)
        current_date += datetime.timedelta(days=1)

    # Create all combinations to check
    combinations = []
    for data_type in data_types:
        for symbol in symbols:
            for date in dates:
                url = build_url(data_type, symbol, date)
                combinations.append((url, data_type, symbol, date))

    with Progress() as progress:
        task = progress.add_task(
            "[green]Checking available files...", total=len(combinations)
        )

        # Use ThreadPoolExecutor to check URLs in parallel
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_url = {
                executor.submit(check_url_exists, combo[0]): combo
                for combo in combinations
            }

            for future in concurrent.futures.as_completed(future_to_url):
                url, data_type, symbol, date = future_to_url[future]
                try:
                    exists = future.result()
                    if exists:
                        available_files[data_type].append((symbol, date))
                        print(
                            f"[green]Found: {data_type} data for {symbol} on {date.isoformat()}[/green]"
                        )
                except Exception as e:
                    print(f"[red]Error checking {url}: {str(e)}[/red]")

                progress.update(task, advance=1)

    return available_files


def preview_zip_content(zip_path: Path) -> None:
    """
    Preview the content of a zip file

    Args:
        zip_path: Path to the zip file
    """
    try:
        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            file_list = zip_ref.namelist()

            print(f"[bold]ZIP Contents[/bold]: {', '.join(file_list)}")

            # Preview each CSV file
            for file_name in file_list:
                if file_name.endswith(".csv"):
                    with zip_ref.open(file_name) as csv_file:
                        # Read CSV data
                        df = pd.read_csv(csv_file)

                        # Print information
                        console.print(f"[bold]File[/bold]: {file_name}")
                        console.print(
                            f"[bold]Shape[/bold]: {df.shape[0]} rows, {df.shape[1]} columns"
                        )
                        console.print(
                            "[bold]Columns[/bold]:", ", ".join(df.columns.tolist())
                        )

                        # Create a table for preview
                        table = Table(title=f"Preview of {file_name}")

                        # Add columns
                        for column in df.columns:
                            table.add_column(column)

                        # Add rows (first 5)
                        for _, row in df.head(5).iterrows():
                            table.add_row(*[str(val) for val in row.values])

                        console.print(table)

                        # Basic statistics
                        if "price" in df.columns:
                            console.print(
                                f"[bold]Price Range[/bold]: {df['price'].min()} - {df['price'].max()}"
                            )

                        if "created_time" in df.columns and len(df) > 0:
                            # Convert timestamps
                            try:
                                timestamps = pd.to_datetime(
                                    df["created_time"], unit="ms"
                                )
                                min_time = timestamps.min().strftime(
                                    "%Y-%m-%d %H:%M:%S"
                                )
                                max_time = timestamps.max().strftime(
                                    "%Y-%m-%d %H:%M:%S"
                                )
                                console.print(
                                    f"[bold]Time Range[/bold]: {min_time} - {max_time}"
                                )
                            except Exception:
                                pass

    except Exception as e:
        print(f"[red]Error previewing zip content: {str(e)}[/red]")


def main():
    parser = argparse.ArgumentParser(
        description="Explore and download OKX historical data"
    )

    # Date range arguments
    parser.add_argument("--start-date", type=str, help="Start date (YYYY-MM-DD)")
    parser.add_argument(
        "--end-date",
        type=str,
        help="End date (YYYY-MM-DD), defaults to start date if not provided",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=1,
        help="Number of days to check (used if start-date is provided)",
    )

    # Data type arguments
    parser.add_argument(
        "--data-types",
        type=str,
        nargs="+",
        choices=DATA_TYPES,
        default=DATA_TYPES,
        help=f"Data types to check (default: {' '.join(DATA_TYPES)})",
    )

    # Symbol arguments
    parser.add_argument(
        "--symbols",
        type=str,
        nargs="+",
        default=DEFAULT_SYMBOLS,
        help=f"Trading pair symbols to check (default: {' '.join(DEFAULT_SYMBOLS)})",
    )

    # Action arguments
    parser.add_argument("--explore", action="store_true", help="Explore available data")
    parser.add_argument("--download", action="store_true", help="Download found data")
    parser.add_argument(
        "--output-dir",
        type=str,
        default="./okx_data",
        help="Output directory for downloaded files (default: ./okx_data)",
    )

    # Download specific file
    parser.add_argument("--download-url", type=str, help="Download a specific URL")

    # Direct preview
    parser.add_argument("--preview", type=str, help="Preview a local zip file")

    args = parser.parse_args()

    # Preview local file
    if args.preview:
        preview_path = Path(args.preview)
        if preview_path.exists() and preview_path.is_file():
            preview_zip_content(preview_path)
            return
        else:
            print(f"[red]File not found: {args.preview}[/red]")
            return

    # Download specific URL
    if args.download_url:
        output_dir = Path(args.output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        file_name = args.download_url.split("/")[-1]
        output_path = output_dir / file_name

        if download_file(args.download_url, output_path):
            print(f"[green]Downloaded {file_name} to {output_path}[/green]")

            # Preview the downloaded file
            preview_zip_content(output_path)

        return

    # Parse dates
    today = datetime.date.today()

    if args.start_date:
        try:
            start_date = datetime.datetime.strptime(args.start_date, "%Y-%m-%d").date()
        except ValueError:
            print("[red]Invalid start date format. Use YYYY-MM-DD.[/red]")
            return
    else:
        # Default to yesterday
        start_date = today - datetime.timedelta(days=1)

    if args.end_date:
        try:
            end_date = datetime.datetime.strptime(args.end_date, "%Y-%m-%d").date()
        except ValueError:
            print("[red]Invalid end date format. Use YYYY-MM-DD.[/red]")
            return
    else:
        # Default to start_date + days - 1
        end_date = start_date + datetime.timedelta(days=args.days - 1)

    if end_date < start_date:
        print("[red]End date cannot be before start date.[/red]")
        return

    if end_date > today:
        print(
            f"[yellow]Warning: End date {end_date.isoformat()} is in the future, limiting to today.[/yellow]"
        )
        end_date = today

    print(
        f"[bold]Exploring data from {start_date.isoformat()} to {end_date.isoformat()}[/bold]"
    )
    print(f"[bold]Data types[/bold]: {', '.join(args.data_types)}")
    print(f"[bold]Symbols[/bold]: {', '.join(args.symbols)}")

    # Explore data
    available_files = explore_date_range(
        start_date, end_date, args.data_types, args.symbols
    )

    # Print summary
    console.print("\n[bold]Summary of Available Data:[/bold]")
    for data_type, files in available_files.items():
        if files:
            console.print(f"[green]{data_type}[/green]: {len(files)} files found")
        else:
            console.print(f"[yellow]{data_type}[/yellow]: No files found")

    # Download if requested
    if args.download and any(files for files in available_files.values()):
        output_dir = Path(args.output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        console.print(f"\n[bold]Downloading files to {output_dir}[/bold]")

        for data_type, files in available_files.items():
            for symbol, date in files:
                url = build_url(data_type, symbol, date)
                dir_date, file_date = format_date(date)
                file_name = f"{symbol}-{data_type}-{file_date}.zip"
                output_path = output_dir / file_name

                if download_file(url, output_path):
                    print(f"[green]Downloaded {file_name} to {output_path}[/green]")

                    # Preview the first downloaded file
                    if (
                        data_type == available_files[args.data_types[0]][0][0]
                        and date == available_files[args.data_types[0]][0][1]
                    ):
                        preview_zip_content(output_path)


if __name__ == "__main__":
    main()
