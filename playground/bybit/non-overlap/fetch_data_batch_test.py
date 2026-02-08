# /usr/bin/env python3
import time
from pathlib import Path

import httpx
import typer
from rich.console import Console
from rich.table import Table

# Ensure attrs is available, although not strictly used in this simple script,
# keeping in mind the attrs instruction.
# import attr

app = typer.Typer()
console = Console()

BASE_URL = "https://api.bybit.com/v5/market/kline"


def check_for_duplicates(klines: list[list[str]]) -> tuple[bool, int]:
    """Checks a list of klines for duplicates based on timestamp."""
    if not klines:
        return False, 0
    timestamps = [kline[0] for kline in klines]
    # Convert list to set to easily find duplicates
    unique_timestamps = set(timestamps)
    has_duplicates = len(timestamps) != len(unique_timestamps)
    num_duplicates = len(timestamps) - len(unique_timestamps)
    return has_duplicates, num_duplicates


def interval_to_ms(interval: str) -> int:
    """Converts interval string (5, 15) to milliseconds."""
    if interval == "5":
        return 5 * 60 * 1000
    elif interval == "15":
        return 15 * 60 * 1000
    else:
        # This case should be caught by the interval validation in main
        raise ValueError("Unsupported interval")


@app.command()
def main(
    category: str = typer.Option(..., "--category", "-c", help="Market category (e.g., inverse, linear, spot)."),
    symbol: str = typer.Option(..., "--symbol", "-s", help="Trading pair symbol (e.g., BTCUSD, BTCUSDT)."),
    interval: str = typer.Option(..., "--interval", "-i", help="Kline interval (must be 5 or 15)."),
    num_batches: int = typer.Option(10, "--num-batches", "-n", help="Number of batches to fetch for each limit size."),
    limits: list[int] = typer.Option([3, 5, 7], "--limit", "-l", help="List of limit sizes to test."),
):
    """
    Fetches Bybit kline data in small batches and checks for duplicates
    across batches for different limit sizes.
    """
    if interval not in ["5", "15"]:
        console.print("[bold red]Error:[/bold red] Only 5 and 15 minute intervals are supported for this test.")
        raise typer.Exit(code=1)

    interval_ms = interval_to_ms(interval)

    console.print(f"[bold blue]Starting duplicate test for {category.upper()} {symbol.upper()} {interval}-minute data.[/bold blue]")

    for limit in limits:
        console.print(f"\n[bold yellow]Testing with limit={limit}[/bold yellow]")
        all_klines_for_limit: list[list[str]] = []
        end_time_ms: int | None = None  # Use the timestamp of the first kline in the previous batch as the end time

        for batch_num in range(num_batches):
            console.print(f"  Fetching batch {batch_num + 1}/{num_batches} with limit={limit}...")

            params = {
                "category": category,
                "symbol": symbol,
                "interval": interval,
                "limit": limit,
            }
            if end_time_ms is not None:
                params["end"] = end_time_ms

            try:
                # Use a small delay to avoid hitting rate limits quickly
                time.sleep(0.1)
                response = httpx.get(BASE_URL, params=params)
                response.raise_for_status()  # Raise an exception for bad status codes
                data = response.json()

                if data["retCode"] != 0:
                    console.print(f"[bold red]API Error:[/bold red] {data['retMsg']}")
                    # Stop fetching for this limit size if there's an API error
                    break

                klines = data["result"]["list"]

                if not klines:
                    console.print("  [bold magenta]Fetched empty batch. Stopping for this limit size.[/bold magenta]")
                    break

                # Extend the main list with fetched klines
                all_klines_for_limit.extend(klines)

                # Set the end_time for the next request to the timestamp of the oldest kline
                # in the current batch MINUS one interval duration.
                # The API returns data in reverse chronological order (newest first)
                end_time_ms = int(klines[-1][0]) - interval_ms

            except httpx.HTTPStatusError as e:
                console.print(f"[bold red]HTTP error occurred:[/bold red] {e}")
                break
            except httpx.RequestError as e:
                console.print(f"[bold red]An error occurred while requesting {e.request.url!r}:[/bold red] {e}")
                break
            except Exception as e:
                console.print(f"[bold red]An unexpected error occurred:[/bold red] {e}")
                break

        # After fetching all batches for this limit, check for duplicates
        has_duplicates, num_duplicates = check_for_duplicates(all_klines_for_limit)

        table = Table(title=f"Duplicate Check Results for limit={limit}")
        table.add_column("Total Klines Fetched", style="cyan", no_wrap=True)
        table.add_column("Has Duplicates", style="magenta")
        table.add_column("Number of Duplicates", style="red")

        table.add_row(
            str(len(all_klines_for_limit)),
            "[bold green]No[/bold green]" if not has_duplicates else "[bold red]Yes[/bold red]",
            str(num_duplicates),
        )

        console.print(table)

    console.print("\n[bold blue]Duplicate test finished.[/bold blue]")


if __name__ == "__main__":
    # Create the directory if it doesn't exist
    script_dir = Path(__file__).parent
    script_dir.mkdir(parents=True, exist_ok=True)
    # No need to manually chmod +x here, typer run handles this.
    # If running directly, remember to make it executable: chmod +x fetch_data_batch_test.py
    app()
