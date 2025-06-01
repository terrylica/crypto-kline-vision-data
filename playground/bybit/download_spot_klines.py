#!/usr/bin/env python3

import httpx
import datetime
import typer
from typing import Optional, List
import platformdirs
from pathlib import Path
from rich.progress import Progress, SpinnerColumn, TextColumn
import polars as pl
import time
from rich.console import Console
from rich.table import Table
from loguru import logger

# Determine log file path using platformdirs
log_dir = platformdirs.user_log_dir(
    appname="data_source_manager", appauthor="YourName"
)  # Replace "YourName" if you have a specific author name, otherwise its fine
log_file = Path(log_dir) / "bybit_download.log"
log_file.parent.mkdir(parents=True, exist_ok=True)  # Ensure log directory exists

# Configure loguru to write to a file with rotation
logger.remove()  # Remove default handlers
logger.add(
    str(log_file),
    rotation="10 MB",  # Rotate when the file reaches 10MB
    retention="1 week",  # Keep logs for 1 week
    compression="zip",  # Compress rotated logs
    level="INFO",  # Log info messages and above
    format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}",
)

# Uncomment to enable debug logging
# logger.add(
#     str(log_file),
#     level="DEBUG",  # Log debug messages and above
#     rotation="10 MB",
#     retention="1 week",
#     compression="zip",
#     format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}",
# )

app = typer.Typer()
console = Console()

BYBIT_API_URL = "https://api.bybit.com/v5/market/kline"
# Earliest known 5-minute spot BTCUSDT timestamp (UTC) from our empirical testing: 2021-07-05 12:00:00 UTC
# Let's use the genesis timestamp we found for Inverse BTCUSD 5m for testing this mode
# GENESIS_TIMESTAMP_MS = 1748509800000 # Example genesis timestamp for Inverse BTCUSD 5m
# INTERVAL_MINUTES = 5 # Redundant, use interval_to_ms
# INTERVAL_SECONDS = INTERVAL_MINUTES * 60 # Redundant
LIMIT = 5  # Set the API limit to 5 for testing overlap with a larger batch size # This seems to be set by the limit option now

# Symbol validation patterns for different market categories
SYMBOL_PATTERNS = {
    "spot": {"suffix": "", "examples": "BTCUSDT, ETHUSDC"},
    "linear": {"suffix": "USDT", "examples": "BTCUSDT, ETHUSDT"},
    "inverse": {"suffix": "USD", "examples": "BTCUSD, ETHUSD"}
}

def validate_symbol_for_category(symbol: str, category: str) -> bool:
    """
    Validates that the symbol follows the correct naming pattern for the specified category.
    
    Args:
        symbol: The trading pair symbol
        category: The market category (spot, linear, inverse)
        
    Returns:
        True if the symbol is valid for the category, False otherwise
    """
    logger.debug(f"Validating symbol '{symbol}' for category '{category}'")
    
    if category not in SYMBOL_PATTERNS:
        logger.error(f"Unknown category: {category}")
        return False
        
    pattern = SYMBOL_PATTERNS[category]
    
    # For spot market, we don't enforce specific suffix patterns
    if category == "spot":
        return True
        
    # For linear, symbols should end with USDT
    if category == "linear" and not symbol.endswith(pattern["suffix"]):
        logger.warning(f"Symbol '{symbol}' doesn't follow the naming convention for {category} market (should end with '{pattern['suffix']}')")
        return False
        
    # For inverse, symbols should end with USD
    if category == "inverse" and not symbol.endswith(pattern["suffix"]):
        logger.warning(f"Symbol '{symbol}' doesn't follow the naming convention for {category} market (should end with '{pattern['suffix']}')")
        return False
    
    return True

# KLINES_PER_BATCH = 3 # Define the number of klines to fetch per batch # This is not used anymore with limit
# NUM_BATCHES_TO_FETCH = 10 # Define the number of batches to download for testing # This is set by the num_batches option now


def check_for_duplicates(klines: List[List[str]]) -> tuple[bool, int]:
    """Checks a list of klines for duplicates based on timestamp."""
    logger.debug("Checking for duplicates.")
    if not klines:
        return False, 0
    # Ensure timestamps are integers for set comparison
    timestamps = [int(kline[0]) for kline in klines]
    # Convert list to set to easily find duplicates
    unique_timestamps = set(timestamps)
    has_duplicates = len(timestamps) != len(unique_timestamps)
    num_duplicates = len(timestamps) - len(unique_timestamps)
    logger.debug(f"Duplicate check results: has_duplicates={has_duplicates}, num_duplicates={num_duplicates}")
    return has_duplicates, num_duplicates


def interval_to_ms(interval: str) -> int:
    """Converts interval string (5, 15) to milliseconds."""
    logger.debug(f"Converting interval '{interval}' to milliseconds.")
    if interval == "5":
        return 5 * 60 * 1000
    if interval == "15":
        return 15 * 60 * 1000
    logger.error(f"Unsupported interval: {interval}")
    raise ValueError("Unsupported interval")


def round_down_timestamp_to_interval(timestamp_ms: int, interval_ms: int) -> int:
    """Rounds a timestamp down to the nearest interval boundary."""
    logger.debug(f"Rounding down timestamp {timestamp_ms} to interval {interval_ms}.")
    return (timestamp_ms // interval_ms) * interval_ms


# Removed @retry decorator for debugging
def fetch_klines(
    client: httpx.Client,
    category: str,
    symbol: str,
    interval: str,
    start_time_ms: Optional[int] = None,
    end_time_ms: Optional[int] = None,
    limit: int = 1000,
) -> List[List[str]]:
    """Fetches klines from Bybit API with logging (Synchronous)."""
    params = {
        "category": category,
        "symbol": symbol,
        "interval": interval,
        "limit": limit,
    }
    if start_time_ms is not None:
        params["start"] = start_time_ms
    if end_time_ms is not None:
        params["end"] = end_time_ms

    # Use logger.debug for debugging API call parameters
    logger.debug(f"API Call Params: {params}")

    # Use a small delay to avoid hitting rate limits quickly
    time.sleep(0.1)  # Use synchronous sleep

    try:
        response = client.get(BYBIT_API_URL, params=params)

        # Use logger for debugging response details
        logger.debug(f"Response Status Code: {response.status_code}")
        # Log response headers for rate limit info etc.
        logger.debug(f"Response Headers: {response.headers}")
        logger.debug(f"Response Body Snippet: {response.text[:500]}...")  # Log first 500 chars of body

        response.raise_for_status()

        data = response.json()

        if data["retCode"] != 0:
            console.print(f"\n[bold red]API Error:[/bold red] {data['retMsg']}")  # Keep console.print for user-facing error
            # Use logger.error for error response body
            logger.error(f"API returned error retCode={data['retCode']}, retMsg={data['retMsg']}")
            logger.error(f"Error Response Body: {response.text}")
            raise Exception(f"Bybit API returned error: {data['retMsg']}")

        klines = data["result"]["list"]
        logger.debug(f"Received {len(klines)} klines in this response.")
        
        # Validate that the response contains the expected category and symbol
        if "symbol" in data["result"] and "category" in data["result"]:
            response_symbol = data["result"]["symbol"]
            response_category = data["result"]["category"]
            
            # If response data doesn't match what we requested, log a warning
            if response_symbol != symbol or response_category != category:
                warning_msg = f"API response mismatch: Requested {category}/{symbol}, but received {response_category}/{response_symbol}"
                logger.warning(warning_msg)
                # Only print warning to console if the mismatch is severe (different category)
                if response_category != category:
                    console.print(f"[bold yellow]Warning:[/bold yellow] {warning_msg}")
                    console.print("[yellow]The data may not be from the expected market type. Use --force to continue.[/yellow]")
                    # Check if --force flag is set before raising an exception
                    if not getattr(typer.Context.get_current(), "params", {}).get("force", False):
                        raise Exception(f"API returned data from unexpected category: {response_category} (expected {category})")
        
        return klines

    except httpx.HTTPStatusError as e:
        logger.error(f"HTTP error occurred: {e}")
        raise  # Re-raise the exception
    except httpx.RequestError as e:
        logger.error(f"An error occurred while requesting {e.request.url!r}: {e}")
        raise  # Re-raise the exception
    except Exception as ex:
        logger.exception(f"An unexpected error occurred during fetch_klines: {ex}")
        raise  # Re-raise the exception


def find_true_genesis_timestamp_ms(client: httpx.Client, category: str, symbol: str, interval_ms: int) -> int:
    """
    Finds the true earliest available timestamp for a market using the rigorous method (Synchronous).
    """
    console.print(
        f"\n[bold blue]Discovering true genesis timestamp for {category.upper()} {symbol.upper()} "
        f"{interval_ms//60000}-minute data...[/bold blue]"
    ) # Keep console.print for user-facing message

    # Step 1: Initial query with a very early start and limit=1
    very_early_timestamp = 1500000000000  # Example: July 2017

    try:
        # Use logger.debug for debugging Genesis Step 1
        logger.debug(f"Genesis Step 1: Querying with start={very_early_timestamp} and limit=1 to find potential earliest.")
        # Use synchronous fetch_klines
        klines_initial = fetch_klines(client, category, symbol, str(interval_ms // 60000), start_time_ms=very_early_timestamp, limit=1)

        if not klines_initial:
            console.print(
                "[bold red]Error:[/bold red] Could not find any data even from a very early timestamp."
            )  # Keep console.print for user-facing error
            logger.error("Could not find any data from very early timestamp. Exiting.")
            raise typer.Exit(code=1)

        potential_earliest_ts = int(klines_initial[0][0])
        console.print(
            f"  Potential earliest timestamp found: {potential_earliest_ts} ("
            f"{datetime.datetime.fromtimestamp(potential_earliest_ts/1000, datetime.timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')})"
        ) # Keep console.print for user-facing message
        logger.debug(f"Potential earliest timestamp: {potential_earliest_ts}")

        # Step 2: Verify by querying one interval before
        verification_start_ts = potential_earliest_ts - interval_ms
        # Use logger.debug for debugging Genesis Step 2
        logger.debug(f"Genesis Step 2: Verifying by querying one interval before: start={verification_start_ts} and limit=1.")

        # Use synchronous fetch_klines
        klines_verification = fetch_klines(
            client, category, symbol, str(interval_ms // 60000), start_time_ms=verification_start_ts, limit=1
        )

        # Re-checking the doc's verification logic: "If this verification query returned an empty list..."
        if not klines_verification:
            console.print(
                "  Verification successful: Querying one interval before returned no klines."
            )  # Keep console.print for user-facing message
            logger.debug("Verification query returned empty list. Verification successful.")
            true_genesis_ts = potential_earliest_ts
        else:
            # Bybit API behavior: When querying before the earliest available data,
            # the API typically returns the earliest data point rather than an empty list
            
            # Check if the verification returned the same timestamp as our potential earliest
            if klines_verification and int(klines_verification[0][0]) == potential_earliest_ts:
                console.print(
                    f"  Verification matches expected Bybit API behavior: Querying before earliest data returns the earliest point."
                )  # Keep console.print for user-facing message
                logger.info("Verification confirmed earliest timestamp - Bybit API returns earliest point when querying before it.")
                true_genesis_ts = potential_earliest_ts
                console.print(
                    f"  Confirmed earliest timestamp: {true_genesis_ts}"
                )  # Keep console.print for user-facing message
            else:
                # This is an unexpected scenario where verification returned a different timestamp
                console.print(
                    "[bold yellow]Unexpected verification result:[/bold yellow] Querying one interval before returned different data."
                ) # Keep console.print for user-facing message
                # Log the timestamp returned in verification for debugging
                logger.warning(
                    f"Verification query returned unexpected data. First kline timestamp in verification: "
                    f"{int(klines_verification[0][0]) if klines_verification else 'N/A'}"
                )

                # Use console.print for debugging verification return
                console.print(
                    f"  Verification returned a different kline starting at: {int(klines_verification[0][0])}."
                )  # Keep console.print for user-facing message
                console.print(
                    "[bold yellow]Warning:[/bold yellow] Genesis timestamp verification returned unexpected data."
                )  # Keep console.print for user-facing error
                
                # For robustness, let's still use the potential_earliest_ts found in step 1 as a fallback.
                true_genesis_ts = potential_earliest_ts
                console.print(
                    f"  Falling back to potential earliest timestamp: {true_genesis_ts}"
                )  # Keep console.print for user-facing message

        console.print(
            f"[bold green]True genesis timestamp discovered:[/bold green] {true_genesis_ts} ("
            f"{datetime.datetime.fromtimestamp(true_genesis_ts/1000, datetime.timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')})"
        ) # Keep console.print for user-facing message
        logger.debug(f"True genesis timestamp determined: {true_genesis_ts}")
        return true_genesis_ts

    except Exception as e:
        console.print(f"\n[bold red]Error during genesis timestamp discovery:[/bold red] {e}")  # Keep console.print for user-facing error
        logger.exception(f"Exception during genesis timestamp discovery: {e}")  # Log the full exception traceback
        raise typer.Exit(code=1)


def validate_symbol_exists(client: httpx.Client, category: str, symbol: str) -> bool:
    """
    Validates that the symbol exists in the given category by making a test API call.
    
    Args:
        client: The httpx Client to use for API requests
        category: The market category (spot, linear, inverse)
        symbol: The trading pair symbol
        
    Returns:
        True if the symbol exists in the category, False otherwise
    """
    logger.debug(f"Validating that symbol '{symbol}' exists in category '{category}'")
    
    try:
        # Make a minimal request to check if the symbol exists
        params = {
            "category": category,
            "symbol": symbol,
            "interval": "5",  # Use a common interval
            "limit": 1,       # Just need one record to verify
        }
        
        response = client.get(BYBIT_API_URL, params=params)
        response.raise_for_status()
        
        data = response.json()
        
        # Check if the API returned a valid response
        if data["retCode"] != 0:
            logger.warning(f"API returned error when validating symbol: {data['retMsg']}")
            return False
            
        # Check if we got any data back
        klines = data["result"]["list"]
        if not klines:
            logger.warning(f"No data returned for symbol '{symbol}' in category '{category}'")
            return False
            
        # Check if the response symbol matches the requested symbol
        if data["result"]["symbol"] != symbol or data["result"]["category"] != category:
            logger.warning(f"API returned mismatched data: {data['result']['category']}/{data['result']['symbol']}")
            return False
            
        logger.debug(f"Symbol '{symbol}' exists in category '{category}'")
        return True
        
    except Exception as e:
        logger.warning(f"Error validating symbol: {e}")
        return False


@app.command()
def main(
    symbol: str = typer.Option("BTCUSDT", "-s", "--symbol", help="Trading pair symbol (e.g. BTCUSDT, ETHUSDT)."),
    interval: str = typer.Option("15", "-i", "--interval", help="Kline interval in minutes (5 or 15)."),
    category: str = typer.Option("spot", "-c", "--category", help="Market category (spot, linear, inverse)."),
    num_batches: int = typer.Option(10, "--num-batches", "-n", help="Number of batches to fetch when not using --fetch-all."),
    limit: int = typer.Option(1000, "--limit", "-l", help="Limit for data size per page [1, 1000]. Default is max (1000)."),
    fetch_all: bool = typer.Option(
        True,
        "--fetch-all/--no-fetch-all",
        "-a/-A",
        help="Fetch all data from genesis instead of recent batches backwards. Default is True.",
    ),
    gap_search_limit: int = typer.Option(
        100, "--gap-search-limit", "-g", help="Maximum number of intervals to search forward for data after a gap."
    ),
    force: bool = typer.Option(
        False,
        "--force",
        "-f",
        help="Force download even if symbol doesn't match the expected pattern for the category. WARNING: Using incorrect symbol formats may result in receiving data from a different market than requested (e.g., using BTCUSDT with inverse category returns linear market data)."
    ),
):
    """
    Download historical kline (candlestick) data from Bybit API.

    The script downloads OHLCV data for a specified trading pair from Bybit API,
    handles gaps in the data, and saves the results to a CSV file.

    Features:

    * Find the true genesis timestamp for the specified market
    * Automatically download all available data from genesis to current time
    * Detect and handle gaps in the data through adaptive binary search
    * Fill missing timestamps with NaN values
    * Perform data integrity checks including duplicate detection
    * Format data in standard OHLCV format
    * Advanced logging with loguru (including log rotation and compression)
    * Strict symbol validation to prevent data integrity issues

    Example usage:

    $ python download_spot_klines.py -s BTCUSDT -i 15 -c spot

    By default, the script fetches all available data (--fetch-all) with maximum batch size (--limit 1000).
    Data is saved to a directory in your user documents folder: ~/Documents/data_source_manager/data/bybit/{category}/{symbol}/{interval}m/

    The output file follows the format: bybit-{symbol}-{interval}m.csv
    
    Symbol naming conventions:
    * For inverse markets: Use USD suffix (e.g. BTCUSD, ETHUSD)
    * For linear markets: Use USDT suffix (e.g. BTCUSDT, ETHUSDT)
    
    ⚠️ Important API Behavior Warning:
    Our empirical testing has revealed that when using incorrect symbol formats with the Bybit API
    (e.g., requesting BTCUSDT with category=inverse), the API returns potentially misleading data:
    
    * When using inverse category with USDT-suffixed symbols, the API returns linear market data
      but incorrectly labels it as "inverse" in the response
    * The data values and timestamps are identical to what would be returned from linear market
    * This can lead to data integrity issues and incorrect analysis
    
    The script validates symbol formats to prevent this issue, but if you override with --force, 
    be aware that you may receive data from a different market than intended.
    """
    if interval not in ["5", "15"]:
        console.print("[bold red]Error:[/bold red] Only 5 and 15 minute intervals are supported for this test.")
        logger.error(f"Unsupported interval: {interval}")
        raise typer.Exit(code=1)

    if not 1 <= limit <= 1000:
        console.print("[bold red]Error:[/bold red] Limit must be between 1 and 1000.")
        logger.error(f"Invalid limit: {limit}")
        raise typer.Exit(code=1)

    if fetch_all and gap_search_limit < 0:
        console.print("[bold red]Error:[/bold red] Gap search limit cannot be negative when --fetch-all is used.")
        logger.error(f"Invalid gap_search_limit: {gap_search_limit}")
        raise typer.Exit(code=1)
        
    # Validate that the category is supported
    if category not in SYMBOL_PATTERNS:
        console.print(f"[bold red]Error:[/bold red] Unsupported market category: {category}. Supported categories are: {', '.join(SYMBOL_PATTERNS.keys())}")
        logger.error(f"Unsupported market category: {category}")
        raise typer.Exit(code=1)
    
    # Validate symbol format based on market category
    if not validate_symbol_for_category(symbol, category) and not force:
        pattern = SYMBOL_PATTERNS[category]
        console.print(f"[bold red]Error:[/bold red] Symbol '{symbol}' doesn't follow the expected naming convention for {category} market.")
        console.print(f"[yellow]Expected format for {category}:[/yellow] {pattern['examples']}")
        
        if category == "inverse" and symbol.endswith("USDT"):
            suggested_symbol = symbol.replace("USDT", "USD")
            console.print(f"[bold green]Suggestion:[/bold green] For inverse market, use '{suggested_symbol}' instead of '{symbol}'")
        elif category == "linear" and symbol.endswith("USD"):
            suggested_symbol = symbol + "T"
            console.print(f"[bold green]Suggestion:[/bold green] For linear market, use '{suggested_symbol}' instead of '{symbol}'")
            
        console.print("[yellow]You can override this check with the --force flag if you're sure about the symbol.[/yellow]")
        logger.error(f"Symbol '{symbol}' validation failed for category '{category}'")
        raise typer.Exit(code=1)
        
    if force:
        console.print(f"[bold yellow]Warning:[/bold yellow] Forcing download with symbol '{symbol}' for {category} market despite potential naming mismatch.")
        logger.warning(f"Forcing download with symbol '{symbol}' for {category} market despite potential naming mismatch")

    interval_ms = interval_to_ms(interval)

    # --- Directory and File Management ---
    data_dir = platformdirs.user_documents_dir()
    output_subdir = Path(data_dir) / "data_source_manager" / "data" / "bybit" / category / symbol / f"{interval}m"
    output_subdir.mkdir(parents=True, exist_ok=True)

    # Modify filename based on category
    if category == "spot":
        output_filename = f"bybit-{symbol}-{interval}m.csv"
    else:
        output_filename = f"bybit-{category}-{symbol}-{interval}m.csv"  # Include category in filename

    output_file = output_subdir / output_filename

    console.print(f"Saving test data to: {output_file}")  # Keep console.print for user-facing message
    logger.debug(f"Output file path: {output_file}")

    # --- Data Download ---
    all_klines_in_memory: List[List[str]] = []

    with httpx.Client() as client:
        # Verify that the symbol exists in the given category
        if not validate_symbol_exists(client, category, symbol) and not force:
            console.print(f"[bold red]Error:[/bold red] Symbol '{symbol}' does not appear to exist in the {category} market category.")
            console.print("[yellow]This could mean:[/yellow]")
            console.print("  1. The symbol name is incorrect")
            console.print("  2. The symbol exists but in a different market category")
            console.print("  3. The Bybit API is experiencing issues")
            console.print("[yellow]You can override this check with the --force flag if you're sure about the symbol.[/yellow]")
            logger.error(f"Symbol '{symbol}' validation failed - does not exist in category '{category}'")
            raise typer.Exit(code=1)
            
        if fetch_all:
            # --- Fetch All from Genesis ---
            start_time_ms = find_true_genesis_timestamp_ms(client, category, symbol, interval_ms)  # Call synchronous genesis finder
            console.print(
                f"Attempting to download {interval}-minute {category.upper()} {symbol.upper()} klines from genesis ("
                f"{datetime.datetime.fromtimestamp(start_time_ms/1000, datetime.timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')})..."
            ) # Keep console.print for user-facing message
            logger.info(
                f"Starting limited data download from genesis: "
                f"{datetime.datetime.fromtimestamp(start_time_ms / 1000, datetime.timezone.utc)}"
            )

            # Set a fixed end time based on current time when the script starts
            # This prevents the script from trying to fetch data beyond the current time
            # Subtract a small buffer (30 minutes) to ensure we don't request future data
            current_time_ms = int(datetime.datetime.now(datetime.timezone.utc).timestamp() * 1000)
            end_time_buffer_ms = 30 * 60 * 1000  # 30 minutes in milliseconds
            end_time_ms = current_time_ms - end_time_buffer_ms
            end_time_utc = datetime.datetime.fromtimestamp(end_time_ms / 1000, datetime.timezone.utc)
            console.print(f"Setting end time to: {end_time_utc.strftime('%Y-%m-%d %H:%M:%S UTC')} (current time minus 30-minute buffer)")
            logger.info(f"Fixed end time set to: {end_time_utc}")

            next_batch_start_time_ms = start_time_ms
            batch_num = 0
            total_klines_downloaded = 0

            # Set a limit on the total number of klines to fetch in "fetch all" mode

            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                TextColumn("•"),
                TextColumn("[bold blue]{task.fields[downloaded_count]}[/bold blue] klines downloaded"),
                TextColumn("•"),
                TextColumn("Current batch start: [cyan]{task.fields[current_batch_start_utc]}[/cyan]"),
                TextColumn("•"),
                TextColumn("Current batch newest: [cyan]{task.fields[current_batch_newest_utc]}[/cyan]"),
                auto_refresh=True,
                transient=True,
            ) as progress:
                download_task = progress.add_task(
                    f"Downloading from genesis until complete (limit per request={limit})",
                    total=None,  # Set total to None for indeterminate progress
                    downloaded_count=0,
                    current_batch_start_utc="N/A",
                    current_batch_newest_utc="N/A",
                )

                # Changed loop condition to always run until broken internally
                # while total_klines_downloaded < max_klines_to_fetch:  # Change condition to respect max_klines_to_fetch
                while True:
                    batch_num += 1
                    current_batch_request_start_time_ms = next_batch_start_time_ms

                    # Check if we've reached the end time
                    if current_batch_request_start_time_ms >= end_time_ms:
                        progress.stop()
                        console.print(
                            f"\n[green]Download complete: Reached end time {end_time_utc.strftime('%Y-%m-%d %H:%M:%S UTC')}[/green]"
                        )
                        progress.start()
                        break

                    logger.debug(f"--- Starting Batch {batch_num} ---")
                    # Use the API limit for each request
                    logger.debug(f"Attempting to fetch from start={current_batch_request_start_time_ms} with limit={limit}")

                    # Removed remaining_klines and batch_limit logic that capped total rows
                    # Calculate how many more klines we need
                    # remaining_klines = max_klines_to_fetch - total_klines_downloaded
                    # Use a smaller limit for the last batch if needed
                    # batch_limit = min(limit, remaining_klines)

                    # if batch_limit <= 0:
                    #     break  # We've already fetched enough klines

                    try:
                        # Use the user-provided limit (capped at 1000 by validation) for each batch request
                        klines = fetch_klines(
                            client,
                            category,
                            symbol,
                            str(interval_ms // 60000),
                            start_time_ms=current_batch_request_start_time_ms,
                            limit=limit,
                        )

                        logger.debug(f"Received {len(klines)} klines for Batch {batch_num}.")

                        if not klines:
                            logger.debug(f"Empty klines list received for start={current_batch_request_start_time_ms}.")

                            # Update progress task to indicate we're handling a gap
                            progress.update(
                                download_task,
                                description="Gap detected - Searching for data",
                                current_batch_start_utc=(
                                    datetime.datetime.fromtimestamp(current_batch_request_start_time_ms/1000, datetime.timezone.utc)
                                    .strftime('%Y-%m-%d %H:%M:%S UTC')
                                ),
                                current_batch_newest_utc="N/A"
                            )

                            # Temporarily hide the progress bar, print the message, then restore the progress bar
                            progress.stop()
                            console.print(
                                f"\n[yellow]No data received starting from "
                                f"{datetime.datetime.fromtimestamp(current_batch_request_start_time_ms/1000, datetime.timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}. Starting binary search for next data point...[/yellow]"
                            )
                            progress.start()

                            # Try to find the next available data point using adaptive binary search
                            next_timestamp = find_next_available_data(
                                client=client,
                                category=category,
                                symbol=symbol,
                                interval_ms=interval_ms,
                                start_timestamp_ms=current_batch_request_start_time_ms,
                                end_time_ms=end_time_ms,
                                progress=progress,
                                download_task=download_task,
                            )

                            if next_timestamp:
                                # Temporarily hide the progress bar, print the message, then restore the progress bar
                                progress.stop()
                                console.print(
                                    f"\n[green]Binary search successful! Next data found at {datetime.datetime.fromtimestamp(next_timestamp / 1000, datetime.timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}. Resuming download.[/green]"
                                )
                                progress.start()

                                next_batch_start_time_ms = next_timestamp
                                continue  # Continue with the main download loop
                            # If adaptive binary search failed, fall back to standard gap search for smaller gaps
                            # Temporarily hide the progress bar, print the message, then restore the progress bar
                            progress.stop()
                            console.print(
                                f"\n[yellow]Binary search could not find next data point. Falling back to standard gap search (up to {gap_search_limit} intervals)...[/yellow]"
                            )
                            progress.start()

                            # Standard gap search (preserved for small gaps)
                            found_data_after_gap = False
                            for i in range(1, gap_search_limit + 1):
                                search_timestamp_ms = current_batch_request_start_time_ms + (i * interval_ms)

                                # Skip if we're beyond the end time
                                if search_timestamp_ms >= end_time_ms:
                                    progress.stop()
                                    console.print("\n[yellow]Gap search reached end time limit. Ending search.[/yellow]")
                                    progress.start()
                                    break

                                logger.debug(f"Searching {i} intervals forward: trying start={search_timestamp_ms}")
                                try:
                                    # Fetch only one kline to see if data exists
                                    search_klines = fetch_klines(
                                        client, category, symbol, str(interval_ms // 60000), start_time_ms=search_timestamp_ms, limit=1
                                    )
                                    if search_klines:
                                        logger.debug(f"Found data at {search_timestamp_ms}")
                                        # Use progress.stop() and progress.start() to manage display
                                        progress.stop()
                                        console.print(
                                            f"\n[green]Data found {i} intervals after the gap, starting at {datetime.datetime.fromtimestamp(search_timestamp_ms / 1000, datetime.timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}. Resuming download.[/green]"
                                        )
                                        progress.start()
                                        next_batch_start_time_ms = search_timestamp_ms
                                        found_data_after_gap = True
                                        break  # Exit the gap search loop
                                except Exception as search_e:
                                    logger.debug(f"Error during gap search at {search_timestamp_ms}: {search_e}")
                                    # Continue searching even if there's an error on a search request

                            if found_data_after_gap:
                                # Update progress to indicate we're continuing after finding data
                                progress.update(
                                    download_task,
                                    description=f"Downloading from genesis (Batch {batch_num})",
                                    current_batch_start_utc=datetime.datetime.fromtimestamp(
                                        next_batch_start_time_ms / 1000, datetime.timezone.utc
                                    ).strftime("%Y-%m-%d %H:%M:%S UTC"),
                                    current_batch_newest_utc="Continuing after gap",
                                )
                                continue  # Continue the main download loop from the new start time
                            # No data found in standard gap search, go to next day
                            progress.stop()
                            console.print(
                                "\n[yellow]Standard gap search also failed. Data may be permanently unavailable in this period.[/yellow]"
                            )
                            progress.start()

                            # Jump ahead by 1 day
                            jump_ahead_ms = 24 * 60 * 60 * 1000  # 1 day in milliseconds
                            next_batch_start_time_ms = current_batch_request_start_time_ms + jump_ahead_ms

                            # Check if the jump would exceed the end time
                            if next_batch_start_time_ms >= end_time_ms:
                                progress.stop()
                                console.print("\n[green]Download complete: Reached end time after gap. Ending download.[/green]")
                                progress.start()
                                break
                            # Use progress.stop() and progress.start() to manage display
                            progress.stop()
                            console.print(
                                f"\n[yellow]Jumping ahead 24 hours to {datetime.datetime.fromtimestamp(next_batch_start_time_ms / 1000, datetime.timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')} to continue download.[/yellow]"
                            )
                            progress.start()
                            logger.info(
                                f"Jumping ahead 24 hours from {current_batch_request_start_time_ms} to {next_batch_start_time_ms} after finding no data in gap search"
                            )

                            # Update progress to indicate we're jumping ahead
                            progress.update(
                                download_task,
                                description=f"Downloading from genesis (Batch {batch_num})",
                                current_batch_start_utc=datetime.datetime.fromtimestamp(
                                    next_batch_start_time_ms / 1000, datetime.timezone.utc
                                ).strftime("%Y-%m-%d %H:%M:%S UTC"),
                                current_batch_newest_utc="Jumping ahead after gap",
                            )
                            continue  # Continue the main loop with the new start time

                        # Add all received klines
                        all_klines_in_memory.extend(klines)
                        total_klines_downloaded = len(all_klines_in_memory)

                        # klines[0][0] is the newest timestamp in the current batch (API returns descending)
                        newest_timestamp_in_batch = int(klines[0][0])
                        earliest_timestamp_in_batch = int(klines[-1][0])  # klines[-1][0] is the oldest

                        progress.update(
                            download_task,
                            description=f"Downloading from genesis (Batch {batch_num})",
                            completed=total_klines_downloaded,
                            downloaded_count=total_klines_downloaded,
                            current_batch_start_utc=datetime.datetime.fromtimestamp(
                                current_batch_request_start_time_ms / 1000, datetime.timezone.utc
                            ).strftime("%Y-%m-%d %H:%M:%S UTC"),
                            current_batch_newest_utc=datetime.datetime.fromtimestamp(
                                newest_timestamp_in_batch / 1000, datetime.timezone.utc
                            ).strftime("%Y-%m-%d %H:%M:%S UTC"),
                        )

                        # Set the start_time for the next request to be one interval AFTER the newest timestamp
                        # of the current batch to continue fetching forward.
                        next_batch_start_time_ms = newest_timestamp_in_batch + interval_ms
                        logger.debug(
                            f"Batch {batch_num} finished. Oldest TS: {earliest_timestamp_in_batch}, Newest TS: {newest_timestamp_in_batch}. Next batch start calculated as: {next_batch_start_time_ms}"
                        )

                    except Exception as e:
                        console.print(f"\n[bold red]Error during download:[/bold red] {e}")
                        logger.exception(f"An unexpected error occurred in Batch {batch_num}:")  # Log the full exception traceback
                        break  # Break loop on error

        else:  # Default behavior: fetch recent batches backwards
            # --- Fetch Recent Batches Backwards ---
            console.print(
                f"[bold blue]Starting download and data integrity test for {category.upper()} {symbol.upper()} {interval}-minute data (fetching {num_batches} recent batches backwards).[/bold blue]"
            )  # Keep console.print for user-facing message
            logger.info(f"Starting recent data download ({num_batches} batches backwards).")
            # Start fetching from near the current time, rounded down to the nearest interval
            current_time_ms = int(datetime.datetime.now(datetime.timezone.utc).timestamp() * 1000)
            start_time_ms = round_down_timestamp_to_interval(current_time_ms, interval_ms)  # This is the 'end' for the first batch

            batch_num = 0
            next_batch_end_time_ms = start_time_ms  # This is the 'end' for the current batch request
            total_klines_downloaded = 0  # Keep track of total klines downloaded

            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                TextColumn("•"),
                TextColumn("[bold blue]{task.fields[downloaded_count]}[/bold blue] klines downloaded"),
                TextColumn("•"),
                TextColumn("Current batch end: [cyan]{task.fields[current_batch_end_utc]}[/cyan]"),  # Changed text
                TextColumn("•"),
                TextColumn("Current batch earliest: [cyan]{task.fields[current_batch_earliest_utc]}[/cyan]"),
                auto_refresh=True,
                transient=True,
            ) as progress:
                download_task = progress.add_task(
                    f"Downloading {num_batches} batches of {interval}-minute {category.upper()} {symbol} klines backwards",
                    total=num_batches,
                    downloaded_count=0,
                    current_batch_end_utc="N/A",
                    current_batch_earliest_utc="N/A",
                )

                while batch_num < num_batches:
                    batch_num += 1
                    current_batch_request_end_time_ms = next_batch_end_time_ms

                    logger.debug(f"--- Starting Batch {batch_num} (Backward) ---")
                    logger.debug(f"Attempting to fetch up to end={current_batch_request_end_time_ms} with limit={limit}")

                    try:
                        # Use 'end' parameter to fetch data historically UP TO this point
                        klines = fetch_klines(
                            client, category, symbol, str(interval_ms // 60000), end_time_ms=current_batch_request_end_time_ms, limit=limit
                        )

                        logger.debug(f"Received {len(klines)} klines for Batch {batch_num}.")

                        if not klines:
                            console.print(
                                "\n[green]Download complete: No more klines received in batch.[/green]"
                            )  # Keep console.print for user-facing message
                            logger.info(f"Empty klines list received for end={current_batch_request_end_time_ms}. Breaking download loop.")
                            break  # Stop if no klines is received

                        all_klines_in_memory.extend(klines)
                        total_klines_downloaded = len(all_klines_in_memory)  # Update total klines downloaded

                        # klines[0][0] is the newest timestamp, klines[-1][0] is the oldest
                        newest_timestamp_in_batch = int(klines[0][0])
                        earliest_timestamp_in_batch = int(klines[-1][0])

                        progress.update(
                            download_task,
                            description=f"Downloading Batch {batch_num}/{num_batches}",
                            downloaded_count=total_klines_downloaded,  # Update with total downloaded
                            current_batch_end_utc=datetime.datetime.fromtimestamp(
                                current_batch_request_end_time_ms / 1000, datetime.timezone.utc
                            ).strftime("%Y-%m-%d %H:%M:%S UTC"),
                            current_batch_earliest_utc=datetime.datetime.fromtimestamp(
                                earliest_timestamp_in_batch / 1000, datetime.timezone.utc
                            ).strftime("%Y-%m-%d %H:%M:%S UTC"),
                        )
                        progress.advance(download_task)  # Advance task for batch count

                        # Set the end_time for the next request to be one interval BEFORE the earliest timestamp
                        # of the current batch to avoid duplication when fetching historically with 'end'.
                        next_batch_end_time_ms = earliest_timestamp_in_batch - interval_ms
                        logger.debug(
                            f"Batch {batch_num} finished. Newest TS: {newest_timestamp_in_batch}, Oldest TS: {earliest_timestamp_in_batch}. Next batch end calculated as: {next_batch_end_time_ms}"
                        )

                    except httpx.HTTPStatusError as e:
                        console.print(f"\n[bold red]HTTP error occurred:[/bold red] {e}")  # Keep console.print for user-facing error
                        logger.error(f"HTTP error occurred: {e}")  # Use logger for error
                        break
                    except httpx.RequestError as e:
                        console.print(
                            f"\n[bold red]An error occurred while requesting {e.request.url!r}:[/bold red] {e}"
                        )  # Keep console.print for user-facing error
                        logger.error(f"An error occurred while requesting {e.request.url!r}: {e}")  # Use logger for error
                        break
                    except Exception as e:
                        console.print(
                            f"\n[bold red]An unexpected error occurred:[/bold red] {e}"
                        )  # Keep console.print for user-facing error
                        logger.exception(f"An unexpected error occurred in Batch {batch_num}:")  # Log the full exception traceback
                        break  # Break loop on error

    # --- Data Processing and Saving to CSV ---
    console.print(
        f"\n[bold green]Download finished. Processing and saving data to {output_file}...[/bold green]"
    )  # Keep console.print for user-facing message
    logger.info("Processing and saving data.")

    # Define Polars schema based on the preferred output columns and order
    schema = {
        "low": pl.Float64,
        "open": pl.Float64,
        "volume": pl.Float64,
        "high": pl.Float64,
        "close": pl.Float64,
        "timeStamp": pl.Int64,  # Renamed from timestamp_ms to match preferred output
    }

    try:
        # Convert string values and order columns according to the new schema
        processed_data = []
        for kline in all_klines_in_memory:
            # Original API response order: [timestamp, open, high, low, close, volume, turnover]
            # Preferred output order: [low, open, volume, high, close, timeStamp]
            processed_kline = [
                float(kline[3]),  # low (index 3 in original)
                float(kline[1]),  # open (index 1 in original)
                float(kline[5]),  # volume (index 5 in original)
                float(kline[2]),  # high (index 2 in original)
                float(kline[4]),  # close (index 4 in original)
                int(kline[0]),  # timeStamp (index 0 in original, converted to int)
            ]
            processed_data.append(processed_kline)

        # Create DataFrame and sort by timeStamp in ascending order (oldest to newest)
        df = pl.DataFrame(processed_data, schema=list(schema.items()), orient="row").sort("timeStamp", descending=False)

        # If there are rows in the DataFrame, check for and fill gaps
        if df.shape[0] > 0:
            # Find and fill gaps in the time series
            df = fill_gaps_in_dataframe(df, interval_ms)

        console.print(
            f"[green]Successfully created Polars DataFrame with {df.shape[0]} rows and {df.shape[1]} columns.[/green]"
        )  # Keep console.print for user-facing message
        logger.info(f"Created Polars DataFrame with {df.shape[0]} rows.")

        # Write DataFrame to CSV with explicit column order
        df.select(["low", "open", "volume", "high", "close", "timeStamp"]).write_csv(output_file)
        console.print(f"[green]Data successfully saved to[/green] {output_file}")  # Keep console.print for user-facing message
        logger.info(f"Data saved to {output_file}")

    except Exception as e:
        console.print(f"[bold red]Error during Polars processing or saving:[/bold red] {e}")  # Keep console.print for user-facing error
        logger.exception("Error during Polars processing or saving:")  # Log the full exception

    # --- Data Validation (Duplicate and Gap Check) ---
    # Re-read the saved file for validation to ensure the saved data is correct
    console.print(
        f"\n[bold green]Performing data integrity checks on saved file:[/bold green] {output_file}"
    )  # Keep console.print for user-facing message
    logger.info("Performing data integrity checks.")

    try:
        # Read the CSV file with more options for reliability
        df_validation = pl.read_csv(
            output_file,
            has_header=True,
            infer_schema_length=10000,  # Increased inference length
            try_parse_dates=False,  # Don't attempt to parse dates
        )

        # After loading, attempt to cast columns with error handling
        try:
            # Now try casting columns with explicit error handling
            df_validation = df_validation.with_columns(
                [
                    pl.col("low").cast(pl.Float64, strict=False),
                    pl.col("open").cast(pl.Float64, strict=False),
                    pl.col("volume").cast(pl.Float64, strict=False),
                    pl.col("high").cast(pl.Float64, strict=False),
                    pl.col("close").cast(pl.Float64, strict=False),
                    pl.col("timeStamp").cast(pl.Int64, strict=False),
                ]
            )
        except Exception as cast_error:
            console.print(
                f"[yellow]Warning: Error during column type conversion: {cast_error}. Proceeding with validation using original column types.[/yellow]"
            )
            logger.warning(f"Error during column type conversion: {cast_error}")

        num_klines_in_file = df_validation.shape[0]
        console.print(f"Total klines found in file: {num_klines_in_file}")  # Keep console.print for user-facing message
        logger.info(f"Total klines in saved file: {num_klines_in_file}")

        if num_klines_in_file > 0:
            try:
                # 1. Duplicate Check on the saved DataFrame - all rows are real data
                # Need to convert to the list format check_for_duplicates expects
                klines_from_df = df_validation.rows()  # Get rows as lists
                # check_for_duplicates expects list of lists of strings, convert timestamp to string for check
                klines_for_duplicate_check = [[str(row[5])] + [str(col) for j, col in enumerate(row) if j != 5] for row in klines_from_df]

                # Pass the index of the timestamp column in the klines_for_duplicate_check list (0)
                # check_for_duplicates expects list of lists of strings, convert timestamp to string for check
                # The check_for_duplicates function should probably handle the correct column index internally
                # For now, let's assume the timestamp is the first element after conversion
                klines_for_duplicate_check = [[str(row[5])] + [str(col) for j, col in enumerate(row) if j != 5] for row in klines_from_df]
                has_duplicates, num_duplicates = check_for_duplicates(klines_for_duplicate_check)

                duplicate_table = Table(
                    title=f"Duplicate Check Results (from saved file) ({category.upper()} {symbol.upper()} {interval}-minute, {'All Data (with gap handling)' if fetch_all else f'{num_batches} batches'}, limit={limit})"
                )
                duplicate_table.add_column("Total Klines in File", style="cyan", no_wrap=True)
                duplicate_table.add_column("Has Duplicates", style="magenta")
                duplicate_table.add_column("Number of Duplicates", style="red")

                duplicate_table.add_row(
                    str(num_klines_in_file),
                    "[bold green]No[/bold green]" if not has_duplicates else "[bold red]Yes[/bold red]",
                    str(num_duplicates),
                )
                console.print(duplicate_table)  # Keep console.print for user-facing message
                logger.info(f"Duplicate check: Has Duplicates={has_duplicates}, Number of Duplicates={num_duplicates}")

                # 2. Gap Check using Polars on the saved DataFrame
                try:
                    check_timestamp_continuity(df_validation, interval_ms)
                except Exception as cont_error:
                    console.print(f"[yellow]Warning: Could not perform continuity check due to an error: {cont_error}[/yellow]")
                    logger.warning(f"Could not perform continuity check: {cont_error}")

                # Add this section to report the last timestamp
                try:
                    last_timestamp_ms = df_validation.tail(1)["timeStamp"].item()
                    last_timestamp_utc = datetime.datetime.fromtimestamp(last_timestamp_ms / 1000, datetime.timezone.utc)
                    console.print(
                        f"\n[bold green]Last kline timestamp in saved file:[/bold green] {last_timestamp_utc.strftime('%Y-%m-%d %H:%M:%S UTC')}"
                    )
                    logger.info(f"Last kline timestamp in saved file: {last_timestamp_utc.strftime('%Y-%m-%d %H:%M:%S UTC')}")
                except Exception as ts_error:
                    console.print(f"[yellow]Warning: Could not display last timestamp due to an error: {ts_error}[/yellow]")
                    logger.warning(f"Could not display last timestamp: {ts_error}")

            except Exception as validation_error:
                console.print(
                    f"[yellow]Warning: Error during data validation checks: {validation_error}. Skipping detailed validation.[/yellow]"
                )  # Keep console.print for user-facing error
                logger.warning(f"Error during data validation checks: {validation_error}")
        else:
            console.print("[red]Validation skipped: No data rows found in the file.[/red]")  # Keep console.print for user-facing message
            logger.warning("Validation skipped: No data rows found in file.")

    except FileNotFoundError:
        console.print(
            f"[bold red]Validation failed:[/bold red] File not found at {output_file}"
        )  # Keep console.print for user-facing error
        logger.error(f"Validation failed: File not found at {output_file}")
    except Exception as e:
        console.print(f"[bold red]Validation failed due to an error:[/bold red] {e}")  # Keep console.print for user-facing error
        logger.exception("Validation failed due to an error:")  # Log the full exception

    console.print(
        "\n[bold blue]Download, save, and data integrity test finished.[/bold blue]"
    )  # Keep console.print for user-facing message
    console.print(
        f"[bold green]Output CSV file saved to:[/bold green] {output_file.resolve()}"
    )  # Explicitly print absolute path # Keep console.print for user-facing message
    logger.info("Script finished.")

    console.print(f"[bold green]Logging output saved to:[/bold green] {log_file}")


def fill_gaps_in_dataframe(df: pl.DataFrame, interval_ms: int) -> pl.DataFrame:
    """
    Identifies and fills gaps in a time series DataFrame with NaN values.

    Args:
        df: A Polars DataFrame sorted by timeStamp in ascending order
        interval_ms: The expected interval between timestamps in milliseconds

    Returns:
        A DataFrame with gaps filled with NaN values
    """
    if df.shape[0] < 2:
        return df  # No gaps to fill with fewer than 2 rows

    # Get min and max timestamps from the dataset
    min_timestamp = df["timeStamp"].min()
    max_timestamp = df["timeStamp"].max()

    # Generate a complete sequence of timestamps at the specified interval
    expected_timestamps = list(range(min_timestamp, max_timestamp + interval_ms, interval_ms))

    # Create a DataFrame with just the expected timestamps
    expected_df = pl.DataFrame({"timeStamp": expected_timestamps})

    # Left join with the actual data to identify gaps
    merged_df = expected_df.join(df, on="timeStamp", how="left")

    # Count gaps (rows where we have a timestamp but no data)
    gap_count = merged_df.filter(pl.col("low").is_null()).shape[0]

    if gap_count > 0:
        console.print(f"\n[yellow]Found {gap_count} gaps in the time series. Filling with NaN values.[/yellow]")
        logger.info(f"Found {gap_count} gaps in the time series data. Filling with NaN values.")

    return merged_df


def check_timestamp_continuity(df: pl.DataFrame, interval_ms: int):
    """
    Checks for continuity in timestamps in a Polars DataFrame and also identifies rows with missing data.
    Prints a message if any gaps or inconsistencies are found.
    """
    console.print(
        "\n[bold yellow]Checking Timestamp Continuity and Data Quality:[/bold yellow]"
    )  # Keep console.print for user-facing message
    logger.debug("Starting timestamp continuity check.")
    if df.shape[0] < 2:
        console.print(
            "  [yellow]Skipping continuity check: Not enough data points (requires at least 2).[/yellow]"
        )  # Keep console.print for user-facing message
        logger.debug("Skipping continuity check: Not enough data points.")
        return True  # Consider it continuous if less than 2 points

    # Sort by timeStamp to ensure correct order for continuity check
    df_sorted = df.sort("timeStamp")
    logger.debug("DataFrame sorted by timeStamp for continuity check.")

    # Calculate the difference between consecutive timestamps
    time_diff = df_sorted["timeStamp"].diff()

    # Identify where the difference is not equal to the expected interval
    # We need to account for both zero differences (duplicates) and larger differences (gaps).
    # diff() adds a None at the first row, so we slice from 1.
    incorrect_diff_indices = time_diff.slice(1, time_diff.shape[0] - 1) != interval_ms
    issue_indices_relative = incorrect_diff_indices.arg_true()  # Indices within the sliced diff series

    # Check for rows with missing data (NaN values in price columns)
    rows_with_missing_data = df_sorted.filter(
        pl.col("low").is_null()
        | pl.col("high").is_null()
        | pl.col("open").is_null()
        | pl.col("close").is_null()
        | pl.col("volume").is_null()
    )

    missing_data_count = rows_with_missing_data.shape[0]

    if missing_data_count > 0:
        console.print(f"  [bold red]Found {missing_data_count} rows with missing price data (empty values).[/bold red]")
        logger.warning(f"Found {missing_data_count} rows with missing price data")

        # Show a sample of the missing data rows
        sample_size = min(5, missing_data_count)
        sample_missing = rows_with_missing_data.head(sample_size)

        console.print("  [yellow]Sample of rows with missing data:[/yellow]")
        for i in range(sample_size):
            ts = sample_missing[i, "timeStamp"]
            ts_str = datetime.datetime.fromtimestamp(ts / 1000, datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
            console.print(f"    - Timestamp {ts_str} (Unix: {ts}) has missing price data")

        # Show the distribution of missing data
        if missing_data_count > 10:
            # Get the first and last timestamp with missing data
            first_missing = rows_with_missing_data.select(pl.min("timeStamp")).item()
            last_missing = rows_with_missing_data.select(pl.max("timeStamp")).item()

            first_missing_str = datetime.datetime.fromtimestamp(first_missing / 1000, datetime.timezone.utc).strftime(
                "%Y-%m-%d %H:%M:%S UTC"
            )
            last_missing_str = datetime.datetime.fromtimestamp(last_missing / 1000, datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

            console.print(f"  [yellow]Missing data spans from {first_missing_str} to {last_missing_str}[/yellow]")

    if issue_indices_relative.shape[0] > 0:
        console.print(
            f"  [bold red]Found {issue_indices_relative.shape[0]} timestamp sequence issues.[/bold red]"
        )  # Keep console.print for user-facing message
        logger.warning(f"Found {issue_indices_relative.shape[0]} timestamp sequence issues.")
        is_continuous = False
        # Iterate through indices where the difference is not the expected interval
        for i_relative in issue_indices_relative:
            # The actual index in df_sorted is i_relative + 1 because diff() shifts
            actual_index_in_df_next = i_relative + 1
            actual_index_in_df_current = actual_index_in_df_next - 1  # The timestamp *before* the issue

            ts_current = df_sorted[actual_index_in_df_current, "timeStamp"]
            ts_next = df_sorted[actual_index_in_df_next, "timeStamp"]
            difference = ts_next - ts_current

            current_timestamp_str = datetime.datetime.fromtimestamp(ts_current / 1000, datetime.timezone.utc).strftime(
                "%Y-%m-%d %H:%M:%S UTC"
            )
            next_timestamp_str = datetime.datetime.fromtimestamp(ts_next / 1000, datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

            # Determine if it's a zero difference (duplicate) or a larger difference (gap)
            if difference == 0:
                console.print(
                    f"    [yellow]Warning:[/yellow] Zero difference found between timestamps {current_timestamp_str} and {next_timestamp_str} (potential duplicate/issue already flagged)."
                )  # Keep console.print for user-facing message
                logger.warning(f"Zero timestamp difference found at {current_timestamp_str}")
            elif difference > interval_ms:
                next_expected_timestamp_ms = ts_current + interval_ms
                console.print(
                    f"    [red]Gap found[/red] after timestamp: {current_timestamp_str}. Expected next timestamp at {datetime.datetime.fromtimestamp(next_expected_timestamp_ms / 1000, datetime.timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}, but found timestamp {next_timestamp_str} (difference {difference} ms)."
                )  # Keep console.print for user-facing message
                logger.error(
                    f"Gap found after {current_timestamp_str}. Expected {next_expected_timestamp_ms}, found {ts_next} (difference {difference} ms)."
                )

            # Note: difference < 0 should not happen with sorted data

    else:
        if missing_data_count > 0:
            console.print("  [green]Timestamp sequence is continuous, but data quality issues detected (see above).[/green]")
            is_continuous = False
        else:
            console.print(
                "  [bold green]Timestamp continuity and data quality check passed: No gaps or missing data found.[/bold green]"
            )  # Keep console.print for user-facing message
        logger.debug("Timestamp continuity check passed.")
        is_continuous = True

    return is_continuous  # Return True only if no gaps and no missing data


def find_next_available_data(
    client: httpx.Client,
    category: str,
    symbol: str,
    interval_ms: int,
    start_timestamp_ms: int,
    end_time_ms: Optional[int] = None,
    progress=None,
    download_task=None,
) -> Optional[int]:
    """
    Uses an adaptive binary search to find the next available data point after a gap.

    Args:
        client: The httpx Client to use for API requests
        category: Market category (spot, linear, inverse)
        symbol: Trading pair symbol
        interval_ms: Interval in milliseconds
        start_timestamp_ms: The timestamp after which no data was found (start of the gap)
        end_time_ms: Optional end time to limit the search (current time minus buffer)
        progress: Optional Progress object for updating the UI during search
        download_task: Optional task ID for the progress bar

    Returns:
        The timestamp of the next available data point, or None if no data is found
    """
    # Step 1: Exponential search to find an upper bound where data exists
    current_timestamp = start_timestamp_ms
    step_size = 24 * 60 * 60 * 1000  # Start with 1 day
    max_steps = 30  # Prevent infinite loops
    steps_taken = 0

    # Track the last timestamp where data was not found
    last_empty_timestamp = start_timestamp_ms

    logger.info(f"Starting exponential search from {start_timestamp_ms}")

    # First, we expand until we find data
    while steps_taken < max_steps:
        if progress and download_task:
            progress.update(
                download_task,
                description=f"Searching for data after gap (step {steps_taken + 1})",
                current_batch_start_utc=datetime.datetime.fromtimestamp(current_timestamp / 1000, datetime.timezone.utc).strftime(
                    "%Y-%m-%d %H:%M:%S UTC"
                ),
                current_batch_newest_utc=f"Step size: {step_size / (60 * 60 * 1000):.1f} hours",
            )

        # If we've exceeded the end time, return None
        if end_time_ms and current_timestamp >= end_time_ms:
            logger.info(f"Exponential search reached end time limit ({end_time_ms})")
            return None

        try:
            klines = fetch_klines(client, category, symbol, str(interval_ms // 60000), start_time_ms=current_timestamp, limit=1)

            if klines:
                # We found data, break out of the exponential search
                logger.info(f"Found data at {current_timestamp} after {steps_taken} steps")
                break

            # No data found, continue expanding
            last_empty_timestamp = current_timestamp
            current_timestamp += step_size
            step_size *= 2  # Double the step size
            steps_taken += 1

        except Exception as e:
            logger.error(f"Error during exponential search: {e}")
            steps_taken += 1
            # Still increase the step to avoid getting stuck
            current_timestamp += step_size
            step_size *= 2

    if steps_taken >= max_steps:
        logger.warning(f"Exponential search reached max steps ({max_steps}) without finding data")
        return None

    # If we get here, we found data at current_timestamp
    # Now perform binary search between last_empty_timestamp and current_timestamp
    lower_bound = last_empty_timestamp
    upper_bound = current_timestamp

    # Ensure we don't exceed the end time in our binary search
    if end_time_ms and upper_bound > end_time_ms:
        upper_bound = end_time_ms

    logger.info(f"Starting binary search between {lower_bound} and {upper_bound}")

    # Step 2: Binary search to find the exact first available timestamp
    binary_steps = 0
    max_binary_steps = 20  # Prevent too many API calls

    while binary_steps < max_binary_steps and (upper_bound - lower_bound) > interval_ms:
        mid = lower_bound + (upper_bound - lower_bound) // 2

        if progress and download_task:
            progress.update(
                download_task,
                description=f"Fine-tuning search (step {binary_steps + 1})",
                current_batch_start_utc=datetime.datetime.fromtimestamp(mid / 1000, datetime.timezone.utc).strftime(
                    "%Y-%m-%d %H:%M:%S UTC"
                ),
                current_batch_newest_utc=f"Range: {(upper_bound - lower_bound) / (60 * 60 * 1000):.1f} hours",
            )

        try:
            klines = fetch_klines(client, category, symbol, str(interval_ms // 60000), start_time_ms=mid, limit=1)

            if klines:
                # Data found at mid, look earlier
                upper_bound = mid
            else:
                # No data at mid, look later
                lower_bound = mid

            binary_steps += 1

        except Exception as e:
            logger.error(f"Error during binary search: {e}")
            # Assume no data in case of error
            lower_bound = mid
            binary_steps += 1

    # The upper_bound should now be the first timestamp with available data
    logger.info(f"Binary search completed. First available data at approximately {upper_bound}")

    # Verify the result with one final API call
    try:
        verification_klines = fetch_klines(client, category, symbol, str(interval_ms // 60000), start_time_ms=upper_bound, limit=1)

        if verification_klines:
            earliest_timestamp = int(verification_klines[0][0])
            logger.info(f"Confirmed data available at {earliest_timestamp}")

            if progress and download_task:
                progress.update(
                    download_task,
                    description="Found next data point",
                    current_batch_start_utc=datetime.datetime.fromtimestamp(earliest_timestamp / 1000, datetime.timezone.utc).strftime(
                        "%Y-%m-%d %H:%M:%S UTC"
                    ),
                    current_batch_newest_utc="Ready to resume download",
                )

            return earliest_timestamp
    except Exception as e:
        logger.error(f"Error during verification: {e}")

    return None


# Removed asyncio.run
if __name__ == "__main__":
    # The output_subdir creation within main handles directory creation before saving.
    app()
