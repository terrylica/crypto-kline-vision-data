#!/usr/bin/env python3
"""Generate golden dataset fixtures for regression testing.

This script fetches real market data from Binance APIs and saves it as
parquet files for use in regression tests. Run this once to create
reference data files, then use them in tests to verify output stability.

Usage:
    uv run -p 3.13 python scripts/dev/generate_golden_datasets.py

The golden datasets are stored in tests/fixtures/golden/ and should be
committed to the repository.

ADR: docs/adr/2025-01-30-failover-control-protocol.md
"""

from datetime import datetime, timezone
from pathlib import Path

import typer
from rich import print
from rich.table import Table

from data_source_manager import DataProvider, DataSourceManager, Interval, MarketType

app = typer.Typer(help="Generate golden dataset fixtures for regression testing")

# Golden dataset configuration
GOLDEN_DATASETS = [
    {
        "name": "btcusdt_futures_usdt_1h_2024w01",
        "symbol": "BTCUSDT",
        "market_type": MarketType.FUTURES_USDT,
        "interval": Interval.HOUR_1,
        "start_time": datetime(2024, 1, 1, tzinfo=timezone.utc),
        "end_time": datetime(2024, 1, 8, tzinfo=timezone.utc),
        "description": "BTCUSDT USDT futures, 1h, first week of 2024",
    },
    {
        "name": "btcusdt_spot_1h_2024w01",
        "symbol": "BTCUSDT",
        "market_type": MarketType.SPOT,
        "interval": Interval.HOUR_1,
        "start_time": datetime(2024, 1, 1, tzinfo=timezone.utc),
        "end_time": datetime(2024, 1, 8, tzinfo=timezone.utc),
        "description": "BTCUSDT SPOT, 1h, first week of 2024",
    },
    {
        "name": "btcusd_perp_coin_1h_2024w01",
        "symbol": "BTCUSD_PERP",
        "market_type": MarketType.FUTURES_COIN,
        "interval": Interval.HOUR_1,
        "start_time": datetime(2024, 1, 1, tzinfo=timezone.utc),
        "end_time": datetime(2024, 1, 8, tzinfo=timezone.utc),
        "description": "BTCUSD_PERP coin-margined, 1h, first week of 2024",
    },
]

FIXTURES_DIR = Path(__file__).parent.parent.parent / "tests" / "fixtures" / "golden"


@app.command()
def generate(
    force: bool = typer.Option(False, "--force", "-f", help="Overwrite existing files"),
    dry_run: bool = typer.Option(
        False, "--dry-run", "-n", help="Show what would be generated without creating files"
    ),
):
    """Generate all golden dataset fixtures."""
    FIXTURES_DIR.mkdir(parents=True, exist_ok=True)

    table = Table(title="Golden Dataset Generation")
    table.add_column("Dataset", style="cyan")
    table.add_column("Symbol", style="green")
    table.add_column("Market", style="blue")
    table.add_column("Rows", style="magenta")
    table.add_column("Status", style="yellow")

    for config in GOLDEN_DATASETS:
        output_path = FIXTURES_DIR / f"{config['name']}.parquet"

        if output_path.exists() and not force:
            table.add_row(
                config["name"],
                config["symbol"],
                config["market_type"].name,
                "-",
                "[yellow]SKIPPED (exists)[/yellow]",
            )
            continue

        if dry_run:
            table.add_row(
                config["name"],
                config["symbol"],
                config["market_type"].name,
                "-",
                "[blue]WOULD CREATE[/blue]",
            )
            continue

        try:
            manager = DataSourceManager.create(DataProvider.BINANCE, config["market_type"])

            df = manager.get_data(
                symbol=config["symbol"],
                start_time=config["start_time"],
                end_time=config["end_time"],
                interval=config["interval"],
            )

            manager.close()

            if df is None or len(df) == 0:
                table.add_row(
                    config["name"],
                    config["symbol"],
                    config["market_type"].name,
                    "0",
                    "[red]FAILED (no data)[/red]",
                )
                continue

            # Reset index to include open_time as column for parquet
            df_to_save = df.reset_index()
            df_to_save.to_parquet(output_path, index=False)

            table.add_row(
                config["name"],
                config["symbol"],
                config["market_type"].name,
                str(len(df)),
                "[green]CREATED[/green]",
            )

        except (RuntimeError, ValueError, OSError) as e:
            table.add_row(
                config["name"],
                config["symbol"],
                config["market_type"].name,
                "-",
                f"[red]ERROR: {e}[/red]",
            )

    print(table)
    print(f"\nGolden datasets stored in: {FIXTURES_DIR}")


@app.command()
def list_datasets():
    """List all configured golden datasets."""
    table = Table(title="Configured Golden Datasets")
    table.add_column("Name", style="cyan")
    table.add_column("Symbol", style="green")
    table.add_column("Market", style="blue")
    table.add_column("Interval", style="magenta")
    table.add_column("Period", style="yellow")
    table.add_column("Description")

    for config in GOLDEN_DATASETS:
        table.add_row(
            config["name"],
            config["symbol"],
            config["market_type"].name,
            config["interval"].value,
            f"{config['start_time'].date()} to {config['end_time'].date()}",
            config["description"],
        )

    print(table)


@app.command()
def verify():
    """Verify all golden datasets exist and are readable."""
    table = Table(title="Golden Dataset Verification")
    table.add_column("Dataset", style="cyan")
    table.add_column("File", style="green")
    table.add_column("Rows", style="magenta")
    table.add_column("Status", style="yellow")

    import pandas as pd

    for config in GOLDEN_DATASETS:
        filepath = FIXTURES_DIR / f"{config['name']}.parquet"

        if not filepath.exists():
            table.add_row(
                config["name"],
                str(filepath.name),
                "-",
                "[red]MISSING[/red]",
            )
            continue

        try:
            df = pd.read_parquet(filepath)
            table.add_row(
                config["name"],
                str(filepath.name),
                str(len(df)),
                "[green]OK[/green]",
            )
        except (OSError, ValueError, RuntimeError) as e:
            table.add_row(
                config["name"],
                str(filepath.name),
                "-",
                f"[red]CORRUPT: {e}[/red]",
            )

    print(table)


if __name__ == "__main__":
    app()
