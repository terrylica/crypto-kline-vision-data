#!/usr/bin/env python
# ADR: docs/adr/2026-01-30-claude-code-infrastructure.md
# GitHub Issue #10: Wire data availability validation with fail-loud behavior
"""Symbol availability data loader from CSV files.

This module loads symbol listing dates from CSV files and provides functions
to validate data availability for requested time ranges. Used for fail-loud
validation in get_data() to prevent silent empty DataFrame returns.

CSV files are located in scripts/binance_vision_api_aws_s3/reports/:
- um_earliest_dates.csv: USDT-M futures listing dates
- cm_earliest_dates.csv: Coin-M futures listing dates
- spot_earliest_dates.csv: Spot listing dates
"""

from dataclasses import dataclass
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path

from data_source_manager.utils.loguru_setup import logger
from data_source_manager.utils.market_constraints import MarketType

__all__ = [
    "FuturesAvailabilityWarning",
    "SymbolAvailability",
    "check_futures_counterpart_availability",
    "get_earliest_date",
    "get_symbol_availability",
    "is_symbol_available_at",
]

# Path to CSV reports directory (relative to this file)
# From: src/data_source_manager/utils/validation/availability_data.py
# To:   scripts/binance_vision_api_aws_s3/reports/
# Need: .parent (validation) -> .parent (utils) -> .parent (data_source_manager)
#       -> .parent (src) -> .parent (data-source-manager repo root)
REPORTS_DIR = (
    Path(__file__).parent.parent.parent.parent.parent
    / "scripts"
    / "binance_vision_api_aws_s3"
    / "reports"
)

# CSV filenames by market type
CSV_FILES = {
    MarketType.FUTURES_USDT: "um_earliest_dates.csv",
    MarketType.FUTURES_COIN: "cm_earliest_dates.csv",
    MarketType.SPOT: "spot_earliest_dates.csv",
}


@dataclass
class SymbolAvailability:
    """Symbol availability information from CSV."""

    market: str
    symbol: str
    earliest_date: datetime
    available_intervals: list[str]


@dataclass
class FuturesAvailabilityWarning:
    """Warning when futures counterpart is not available for the requested period."""

    message: str
    futures_market: str  # "FUTURES_USDT" or "FUTURES_COIN"
    symbol: str
    earliest_date: datetime
    requested_start: datetime


@lru_cache(maxsize=3)
def _load_csv_data(market_type: MarketType) -> dict[str, SymbolAvailability]:
    """Load and cache CSV data for a market type.

    Args:
        market_type: The market type to load data for.

    Returns:
        Dictionary mapping symbol -> SymbolAvailability.
    """
    csv_file = CSV_FILES.get(market_type)
    if csv_file is None:
        logger.warning(f"No CSV file configured for market type {market_type.name}")
        return {}

    csv_path = REPORTS_DIR / csv_file
    if not csv_path.exists():
        logger.warning(f"CSV file not found: {csv_path}")
        return {}

    result: dict[str, SymbolAvailability] = {}

    with open(csv_path, encoding="utf-8") as f:
        # Skip header
        header = f.readline().strip()
        if not header.startswith("market,symbol"):
            logger.warning(f"Unexpected CSV header format: {header}")
            return {}

        for line in f:
            line = line.strip()
            if not line:
                continue

            # Parse CSV: market,symbol,earliest_date,"interval1,interval2,..."
            parts = line.split(",", 3)
            if len(parts) < 4:
                continue

            market = parts[0]
            symbol = parts[1]
            earliest_date_str = parts[2]

            # Parse date (format: YYYY-MM-DD)
            try:
                earliest_date = datetime.strptime(earliest_date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            except ValueError:
                logger.warning(f"Invalid date format for {symbol}: {earliest_date_str}")
                continue

            # Parse intervals (remove surrounding quotes)
            intervals_str = parts[3].strip('"')
            intervals = [i.strip() for i in intervals_str.split(",")]

            result[symbol] = SymbolAvailability(
                market=market,
                symbol=symbol,
                earliest_date=earliest_date,
                available_intervals=intervals,
            )

    logger.debug(f"Loaded {len(result)} symbols from {csv_path.name}")
    return result


def get_symbol_availability(market_type: MarketType, symbol: str) -> SymbolAvailability | None:
    """Get availability information for a symbol.

    Args:
        market_type: The market type (SPOT, FUTURES_USDT, FUTURES_COIN).
        symbol: The trading symbol (e.g., 'BTCUSDT').

    Returns:
        SymbolAvailability if found, None otherwise.
    """
    data = _load_csv_data(market_type)
    return data.get(symbol)


def get_earliest_date(market_type: MarketType, symbol: str) -> datetime | None:
    """Get the earliest available date for a symbol.

    Args:
        market_type: The market type (SPOT, FUTURES_USDT, FUTURES_COIN).
        symbol: The trading symbol (e.g., 'BTCUSDT').

    Returns:
        Earliest datetime if found, None if symbol not in CSV.
    """
    availability = get_symbol_availability(market_type, symbol)
    return availability.earliest_date if availability else None


def is_symbol_available_at(
    market_type: MarketType,
    symbol: str,
    target_date: datetime,
) -> tuple[bool, datetime | None]:
    """Check if a symbol is available at a target date.

    Args:
        market_type: The market type (SPOT, FUTURES_USDT, FUTURES_COIN).
        symbol: The trading symbol (e.g., 'BTCUSDT').
        target_date: The date to check availability for.

    Returns:
        Tuple of (is_available, earliest_date).
        - (True, earliest_date) if data is available at target_date
        - (False, earliest_date) if target_date is before earliest_date
        - (True, None) if symbol not in CSV (allow request, unknown symbol)
    """
    earliest_date = get_earliest_date(market_type, symbol)

    # If symbol not in CSV, allow the request (unknown symbols should not be blocked)
    if earliest_date is None:
        logger.debug(f"Symbol {symbol} not found in {market_type.name} CSV, allowing request")
        return (True, None)

    # Ensure target_date is timezone-aware
    if target_date.tzinfo is None:
        target_date = target_date.replace(tzinfo=timezone.utc)

    # Check if target_date is before earliest_date
    if target_date < earliest_date:
        logger.debug(
            f"Symbol {symbol} on {market_type.name} not available at {target_date.isoformat()}. "
            f"Earliest: {earliest_date.isoformat()}"
        )
        return (False, earliest_date)

    return (True, earliest_date)


def _convert_spot_symbol_to_futures(symbol: str) -> str:
    """Convert a SPOT symbol to potential futures symbol.

    Args:
        symbol: SPOT symbol (e.g., 'BTCUSDT').

    Returns:
        Futures symbol (same for USDT-M, transformed for coin-margined).
    """
    # For USDT-M futures, symbol is typically the same as SPOT
    return symbol


def check_futures_counterpart_availability(
    market_type: MarketType,
    symbol: str,
    target_date: datetime,
) -> FuturesAvailabilityWarning | None:
    """Check if futures counterpart is available for a non-futures request.

    This function checks if the corresponding USDT-M or Coin-M futures contract
    is available for the requested time period. If the user is requesting SPOT
    data from a period before the futures contract was listed, this emits a
    warning for quant research forensics.

    Args:
        market_type: The market type being requested.
        symbol: The trading symbol.
        target_date: The start date of the request.

    Returns:
        FuturesAvailabilityWarning if futures counterpart is unavailable, None otherwise.
    """
    # Only check for non-futures requests
    if market_type in (MarketType.FUTURES_USDT, MarketType.FUTURES_COIN):
        return None

    # Ensure target_date is timezone-aware
    if target_date.tzinfo is None:
        target_date = target_date.replace(tzinfo=timezone.utc)

    # Convert symbol to futures format
    futures_symbol = _convert_spot_symbol_to_futures(symbol)

    # Check USDT-M futures availability
    um_earliest = get_earliest_date(MarketType.FUTURES_USDT, futures_symbol)
    if um_earliest is not None and target_date < um_earliest:
        return FuturesAvailabilityWarning(
            message=(
                f"Futures counterpart {futures_symbol} on FUTURES_USDT not available "
                f"until {um_earliest.strftime('%Y-%m-%d')}. Requested data from "
                f"{target_date.strftime('%Y-%m-%d')} has no corresponding futures hedge."
            ),
            futures_market="FUTURES_USDT",
            symbol=futures_symbol,
            earliest_date=um_earliest,
            requested_start=target_date,
        )

    # If USDT-M not found, check Coin-M (with USD_PERP suffix)
    # Extract base asset from symbol (e.g., BTC from BTCUSDT)
    base_asset = None
    for quote in ["USDT", "USDC", "BUSD", "BTC", "ETH", "BNB"]:
        if symbol.endswith(quote):
            base_asset = symbol[: -len(quote)]
            break

    if base_asset:
        cm_symbol = f"{base_asset}USD_PERP"
        cm_earliest = get_earliest_date(MarketType.FUTURES_COIN, cm_symbol)
        # Only warn about coin-margined if USDT-M was not found
        if cm_earliest is not None and target_date < cm_earliest and um_earliest is None:
            return FuturesAvailabilityWarning(
                    message=(
                        f"Futures counterpart {cm_symbol} on FUTURES_COIN not available "
                        f"until {cm_earliest.strftime('%Y-%m-%d')}. Requested data from "
                        f"{target_date.strftime('%Y-%m-%d')} has no corresponding futures hedge."
                    ),
                    futures_market="FUTURES_COIN",
                    symbol=cm_symbol,
                    earliest_date=cm_earliest,
                    requested_start=target_date,
                )

    return None
