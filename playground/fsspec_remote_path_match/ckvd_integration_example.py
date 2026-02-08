#!/usr/bin/env python3

from datetime import datetime
from pathlib import Path

import pandas as pd
import pendulum
import typer
from rich.console import Console
from rich.table import Table
from vision_path_mapper import FSSpecVisionHandler, VisionPathMapper

from ckvd.utils.loguru_setup import logger
from ckvd.utils.market_constraints import ChartType, DataProvider, Interval, MarketType


class CryptoKlineVisionDataExample:
    """Simplified example showing how to integrate VisionPathMapper with CryptoKlineVisionData."""

    def __init__(
        self,
        market_type: MarketType,
        provider: DataProvider = DataProvider.BINANCE,
        chart_type: ChartType = ChartType.KLINES,
        cache_dir: Path = Path("cache"),
        use_cache: bool = True,
    ):
        """Initialize with market type and cache settings."""
        self.market_type = market_type
        self.provider = provider
        self.chart_type = chart_type
        self.cache_dir = Path(cache_dir)
        self.use_cache = use_cache

        # Initialize path handler components
        self.path_mapper = VisionPathMapper(base_cache_dir=self.cache_dir)
        self.fs_handler = FSSpecVisionHandler(base_cache_dir=self.cache_dir)

    def get_data(
        self,
        symbol: str,
        start_time: datetime,
        end_time: datetime,
        interval: Interval = Interval.MINUTE_1,
    ) -> pd.DataFrame:
        """Retrieve data for the given time range using path mapper."""
        # Convert to pendulum for easier date handling
        start_pendulum = pendulum.instance(start_time)
        end_pendulum = pendulum.instance(end_time)

        # Prepare tracking variables
        result_df = pd.DataFrame()
        cache_hits = []
        cache_misses = []

        # Iterate through days to retrieve data
        current_date = start_pendulum.start_of("day")
        end_date = end_pendulum.start_of("day")

        while current_date <= end_date:
            # Get paths for this date
            components = self.path_mapper.create_components_from_params(
                symbol=symbol,
                interval=str(interval.value),
                date=current_date,
                market_type=self.market_type,
                chart_type=self.chart_type,
            )

            local_path = self.path_mapper.get_local_path(components)
            remote_url = self.path_mapper.get_remote_url(components)

            # Log the path information for debugging
            logger.debug(f"Local path: {local_path}, Remote URL: {remote_url}")

            # Try cache first, then remote
            if self.use_cache and self.fs_handler.exists(local_path):
                logger.info(f"Cache hit for {current_date.format('YYYY-MM-DD')}")
                cache_hits.append(current_date.format("YYYY-MM-DD"))

                # In real implementation: df = pd.read_parquet(local_path)
                # For demo, create dummy data
                df = pd.DataFrame(
                    {
                        "open_time": pd.date_range(
                            start=current_date.start_of("day"),
                            end=current_date.end_of("day"),
                            freq="1min",
                        ),
                        "source": "cache",
                    }
                )
            else:
                logger.info(f"Cache miss for {current_date.format('YYYY-MM-DD')}")
                cache_misses.append(current_date.format("YYYY-MM-DD"))

                # In real implementation: download file, process, save to cache
                # For demo, create dummy data
                df = pd.DataFrame(
                    {
                        "open_time": pd.date_range(
                            start=current_date.start_of("day"),
                            end=current_date.end_of("day"),
                            freq="1min",
                        ),
                        "source": f"remote:{current_date.format('YYYY-MM-DD')}",
                    }
                )

            # Add to result set
            if not df.empty:
                result_df = pd.concat([result_df, df])

            current_date = current_date.add(days=1)

        # Filter to exact time range
        if not result_df.empty:
            try:
                # Ensure datetime type
                if not pd.api.types.is_datetime64_any_dtype(result_df["open_time"]):
                    result_df["open_time"] = pd.to_datetime(result_df["open_time"])

                # Apply time filter
                mask = (result_df["open_time"] >= start_time) & (result_df["open_time"] <= end_time)
                result_df = result_df[mask]
            except Exception as e:
                logger.error(f"Error filtering DataFrame: {e}")

        # Log summary
        logger.info(f"Retrieved data for {len(result_df)} time points")
        logger.info(f"Cache hits: {len(cache_hits)}, misses: {len(cache_misses)}")

        return result_df


def demo_all_market_types(
    days: int = 1,
    use_cache: bool = True,
    cache_dir: str = "cache",
):
    """Demonstrate data retrieval across all market types."""
    console = Console()
    console.print("[bold green]CryptoKlineVisionData - All Market Types Demo[/bold green]")
    console.print("")

    # Define parameters
    interval = Interval.MINUTE_1
    interval_str = "1m"
    end_time = pendulum.now().start_of("day")
    start_time = end_time.subtract(days=days)

    # Define market configs with appropriate symbols
    markets = [
        {"type": MarketType.SPOT, "name": "SPOT", "symbol": "BTCUSDT"},
        {"type": MarketType.FUTURES_USDT, "name": "UM", "symbol": "BTCUSDT"},
        {"type": MarketType.FUTURES_COIN, "name": "CM", "symbol": "BTCUSD_PERP"},
    ]

    # Run for each market type
    for market in markets:
        console.print(f"[bold magenta]Processing {market['name']} Market[/bold magenta]")

        # Create CKVD for this market
        ckvd = CryptoKlineVisionDataExample(
            market_type=market["type"],
            provider=DataProvider.BINANCE,
            chart_type=ChartType.KLINES,
            cache_dir=Path(cache_dir),
            use_cache=use_cache,
        )

        # Display config
        config_table = Table(title=f"{market['name']} Configuration")
        config_table.add_column("Setting", style="cyan")
        config_table.add_column("Value", style="green")

        config_table.add_row("Market Type", market["name"])
        config_table.add_row("Symbol", market["symbol"])
        config_table.add_row("Interval", interval_str)
        config_table.add_row("Start Time", start_time.format("YYYY-MM-DD HH:mm:ss"))
        config_table.add_row("End Time", end_time.format("YYYY-MM-DD HH:mm:ss"))

        console.print(config_table)

        # Get data
        console.print(f"[bold blue]Retrieving {market['name']} data...[/bold blue]")
        df = ckvd.get_data(
            symbol=market["symbol"],
            start_time=start_time.in_timezone("UTC"),
            end_time=end_time.in_timezone("UTC"),
            interval=interval,
        )

        # Show data statistics
        stats_table = Table(title=f"{market['name']} Data Statistics")
        stats_table.add_column("Statistic", style="cyan")
        stats_table.add_column("Value", style="green")

        stats_table.add_row("Number of Rows", str(len(df)))

        if not df.empty:
            df_start = df["open_time"].min()
            df_end = df["open_time"].max()

            if df_start is not None and df_end is not None:
                stats_table.add_row("First Timestamp", str(df_start))
                stats_table.add_row("Last Timestamp", str(df_end))

                # Count by source
                sources = df["source"].value_counts().to_dict()
                for source, count in sources.items():
                    stats_table.add_row(f"Source: {source}", str(count))

        console.print(stats_table)

        # Show path examples for this market
        paths_table = Table(title=f"{market['name']} Path Mapping")
        paths_table.add_column("Date", style="cyan")
        paths_table.add_column("Local Path", style="green")
        paths_table.add_column("Remote URL", style="blue")

        components = ckvd.path_mapper.create_components_from_params(
            symbol=market["symbol"],
            interval=interval_str,
            date=start_time,
            market_type=market["type"],
        )

        local_path = ckvd.path_mapper.get_local_path(components)
        remote_url = ckvd.path_mapper.get_remote_url(components)

        paths_table.add_row(
            start_time.format("YYYY-MM-DD"),
            str(local_path),
            remote_url,
        )

        console.print(paths_table)
        console.print("")


def main(
    market_type: str = typer.Option("spot", "--market-type", "-m", help="Market type (spot, um, cm)"),
    symbol: str = typer.Option("BTCUSDT", "--symbol", "-s", help="Trading symbol"),
    interval: str = typer.Option("1m", "--interval", "-i", help="Time interval"),
    days: int = typer.Option(3, "--days", "-d", help="Number of days to retrieve"),
    cache_dir: str = typer.Option("cache", "--cache-dir", "-c", help="Cache directory"),
    use_cache: bool = typer.Option(True, "--use-cache", help="Whether to use caching"),
    all_markets: bool = typer.Option(False, "--all-markets", "-a", help="Demo all market types"),
):
    """Demonstrates using the VisionPathMapper with a simplified CryptoKlineVisionData."""
    if all_markets:
        demo_all_market_types(days=days, use_cache=use_cache, cache_dir=cache_dir)
        return

    console = Console()
    console.print("[bold green]CryptoKlineVisionData Integration Example[/bold green]")
    console.print("")

    # Parse market type
    market_enum = None
    if market_type.lower() == "spot":
        market_enum = MarketType.SPOT
        if symbol == "BTCUSD":
            symbol = "BTCUSDT"
            console.print(f"[yellow]Auto-corrected symbol to {symbol} for Spot market[/yellow]")
    elif market_type.lower() in ["um", "futures_usdt"]:
        market_enum = MarketType.FUTURES_USDT
        if symbol == "BTCUSD":
            symbol = "BTCUSDT"
            console.print(f"[yellow]Auto-corrected symbol to {symbol} for UM market[/yellow]")
    elif market_type.lower() in ["cm", "futures_coin"]:
        market_enum = MarketType.FUTURES_COIN
        if symbol == "BTCUSDT":
            symbol = "BTCUSD_PERP"
            console.print(f"[yellow]Auto-corrected symbol to {symbol} for CM market[/yellow]")
    else:
        console.print(f"[bold red]Invalid market type: {market_type}[/bold red]")
        return

    # Parse interval
    interval_enum = None
    for interval_e in Interval:
        if interval_e.value == interval:
            interval_enum = interval_e
            break

    if interval_enum is None:
        console.print(f"[bold red]Invalid interval: {interval}[/bold red]")
        return

    # Create time range
    end_time = pendulum.now().start_of("day")
    start_time = end_time.subtract(days=days)

    # Create CryptoKlineVisionData
    ckvd = CryptoKlineVisionDataExample(
        market_type=market_enum,
        provider=DataProvider.BINANCE,
        chart_type=ChartType.KLINES,
        cache_dir=Path(cache_dir),
        use_cache=use_cache,
    )

    # Display configuration
    config_table = Table(title="Configuration")
    config_table.add_column("Setting", style="cyan")
    config_table.add_column("Value", style="green")

    config_table.add_row("Market Type", market_enum.name)
    config_table.add_row("Symbol", symbol)
    config_table.add_row("Interval", interval)
    config_table.add_row("Start Time", start_time.format("YYYY-MM-DD HH:mm:ss"))
    config_table.add_row("End Time", end_time.format("YYYY-MM-DD HH:mm:ss"))
    config_table.add_row("Cache Directory", str(ckvd.cache_dir))
    config_table.add_row("Use Cache", str(use_cache))

    console.print(config_table)
    console.print("")

    # Get data
    console.print("[bold blue]Retrieving data...[/bold blue]")
    df = ckvd.get_data(
        symbol=symbol,
        start_time=start_time.in_timezone("UTC"),
        end_time=end_time.in_timezone("UTC"),
        interval=interval_enum,
    )

    # Show data statistics
    console.print("[bold blue]Data Statistics[/bold blue]")

    stats_table = Table(title="Data Statistics")
    stats_table.add_column("Statistic", style="cyan")
    stats_table.add_column("Value", style="green")

    stats_table.add_row("Number of Rows", str(len(df)))

    if not df.empty:
        df_start = df["open_time"].min()
        df_end = df["open_time"].max()

        if df_start is not None and df_end is not None:
            stats_table.add_row("First Timestamp", str(df_start))
            stats_table.add_row("Last Timestamp", str(df_end))

            # Count by source
            sources = df["source"].value_counts().to_dict()
            for source, count in sources.items():
                stats_table.add_row(f"Source: {source}", str(count))

    console.print(stats_table)
    console.print("")

    # Show path mapping examples
    console.print("[bold blue]Path Mapping Examples[/bold blue]")

    paths_table = Table(title="Path Mapping Examples")
    paths_table.add_column("Date", style="cyan")
    paths_table.add_column("Local Path", style="green")
    paths_table.add_column("Remote URL", style="blue")

    # Show paths for a few dates
    demo_date = start_time
    for _ in range(min(3, days)):
        components = ckvd.path_mapper.create_components_from_params(
            symbol=symbol,
            interval=interval,
            date=demo_date,
            market_type=market_enum,
        )

        local_path = ckvd.path_mapper.get_local_path(components)
        remote_url = ckvd.path_mapper.get_remote_url(components)

        paths_table.add_row(
            demo_date.format("YYYY-MM-DD"),
            str(local_path),
            remote_url,
        )

        demo_date = demo_date.add(days=1)

    console.print(paths_table)


if __name__ == "__main__":
    typer.run(main)
