#!/usr/bin/env python3
import json
import sys  # For any additional path handling if needed
from pathlib import Path

import httpx
import typer
from rich import print as rprint  # Using rich for formatted output

from data_source_manager.utils.loguru_setup import logger  # For logging

# Ensure we're in the correct directory, but rely on the script being run from there
app = typer.Typer(
    help="Analyze OKX spot and swap instruments",
    context_settings={"help_option_names": ["-h", "--help"]},
)


def fetch_data(inst_type: str) -> list[dict]:
    """Fetch instrument data directly from OKX API"""
    url = f"https://www.okx.com/api/v5/public/instruments?instType={inst_type}"
    logger.debug(f"Fetching data from: {url}")

    try:
        with httpx.Client(timeout=30.0) as client:
            response = client.get(url, headers={"accept": "application/json"})
            response.raise_for_status()
            data = response.json()

            if "data" in data:
                if inst_type == "SPOT":
                    return data["data"]
                if inst_type == "SWAP":
                    return [
                        item
                        for item in data["data"]
                        if item.get("instId") and item["instId"].endswith("-USD-SWAP")
                    ]
                return data["data"]
            logger.error(f"Unexpected API response format: {data}")
            return []
    except httpx.HTTPStatusError as e:
        logger.error(f"HTTP error: {e.response.status_code} - {e.response.text}")
        sys.exit(1)
    except httpx.RequestError as e:
        logger.error(f"Request error: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Error fetching data: {e}")
        sys.exit(1)
    return []


def load_and_filter_data(
    file_path: str, filter_criteria: str | None = None
) -> list[dict]:
    """Load data from local file as fallback"""
    script_dir = Path(__file__).parent.absolute()
    full_path = script_dir / file_path
    logger.debug(f"Attempting to load file at: {full_path}")

    try:
        with open(full_path) as f:
            data = json.load(f)
            if "data" in data:
                if filter_criteria == "spot":
                    return data["data"]
                if filter_criteria == "swap":
                    return [
                        item
                        for item in data["data"]
                        if item.get("instId") and item["instId"].endswith("-USD-SWAP")
                    ]
                return data["data"]
    except FileNotFoundError as e:
        logger.error(f"File not found: {e}. Ensure files are in {script_dir}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Error loading file: {e}")
        sys.exit(1)
    return []


@app.command()
def analyze(
    verbose: bool = typer.Option(
        False, "-v", "--verbose", help="Show detailed instrument listings"
    ),
    usd_only: bool = typer.Option(
        False, "-u", "--usd-only", help="Only show SPOT-USD with SWAP-USD-SWAP matches"
    ),
    use_local: bool = typer.Option(
        False, "-l", "--use-local", help="Use local JSON files instead of API"
    ),
    save_files: bool = typer.Option(
        False, "-s", "--save-files", help="Save API response to local JSON files"
    ),
):
    """
    Analyze OKX spot and swap instruments to count spot instruments
    that have corresponding -USD-SWAP instruments with the same base currency.
    """
    # Get data either from API or local files
    if use_local:
        logger.info("Using local JSON files")
        spot_data = load_and_filter_data("spot_instruments.json", "spot")
        swap_data = load_and_filter_data("swap_instruments.json", "swap")
    else:
        logger.info("Fetching data from OKX API")
        spot_data = fetch_data("SPOT")
        swap_data = fetch_data("SWAP")

        # Save files if requested
        if save_files:
            script_dir = Path(__file__).parent.absolute()

            with open(script_dir / "spot_instruments.json", "w") as f:
                json.dump({"data": spot_data}, f, indent=2)
                logger.info(
                    f"Saved spot data to {script_dir / 'spot_instruments.json'}"
                )

            with open(script_dir / "swap_instruments.json", "w") as f:
                json.dump({"data": swap_data}, f, indent=2)
                logger.info(
                    f"Saved swap data to {script_dir / 'swap_instruments.json'}"
                )

    # Extract base currencies from swap instruments
    swap_base_currencies = {}
    for item in swap_data:
        # For SWAP instruments, extract base currency from the instId (e.g., BTC-USD-SWAP -> BTC)
        inst_id = item.get("instId", "")
        if inst_id and inst_id.endswith("-USD-SWAP"):
            base_currency = inst_id.split("-")[0]
            swap_base_currencies[base_currency] = item

    # Find spot instruments that have corresponding swap instruments
    matching_spot_instruments = []
    for spot_item in spot_data:
        base_ccy = spot_item.get("baseCcy")
        quote_ccy = spot_item.get("quoteCcy")

        # Apply USD filter if requested
        if usd_only and quote_ccy != "USD":
            continue

        if base_ccy and base_ccy in swap_base_currencies:
            matching_spot_instruments.append(
                {"spot": spot_item, "swap": swap_base_currencies[base_ccy]}
            )

    # Statistics for matching SPOT instruments
    matching_count = len(matching_spot_instruments)
    rprint("\n[bold green]Statistics Summary:[/bold green]")
    rprint(f"Total SPOT instruments: {len(spot_data)}")
    rprint(f"Total SWAP instruments with -USD-SWAP: {len(swap_data)}")
    if usd_only:
        rprint(
            f"SPOT-USD instruments with corresponding SWAP-USD-SWAP instruments: {matching_count}"
        )
    else:
        rprint(
            f"SPOT instruments with corresponding SWAP instruments: {matching_count}"
        )

    if verbose or matching_count > 0:
        if usd_only:
            rprint(
                "\n[bold cyan]SPOT-USD Instruments with SWAP-USD-SWAP Counterparts:[/bold cyan]"
            )
        else:
            rprint("\n[bold cyan]SPOT Instruments with SWAP Counterparts:[/bold cyan]")

        if matching_count > 0:
            for i, pair in enumerate(matching_spot_instruments):
                spot_item = pair["spot"]
                swap_item = pair["swap"]
                rprint(
                    f"{i + 1}. {spot_item.get('instId')}, Base: {spot_item.get('baseCcy')}, Quote: {spot_item.get('quoteCcy')}"
                )
                rprint(f"   └─ Swap: {swap_item.get('instId')}")
        else:
            rprint("No matching instruments found.")


if __name__ == "__main__":
    app()
