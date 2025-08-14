from rich import print

from data_source_manager.utils.market_constraints import MarketType


def get_market_type_str(market_type: MarketType) -> str:
    """Convert MarketType enum to standardized string format for cache keys.

    Args:
        market_type: Market type enum

    Returns:
        Standardized string format for the market type

    Raises:
        ValueError: If the market type is not supported
    """
    if market_type == MarketType.SPOT:
        return "spot"
    if market_type == MarketType.FUTURES_USDT:
        return "futures_usdt"
    if market_type == MarketType.FUTURES_COIN:
        return "futures_coin"
    if market_type == MarketType.FUTURES:
        return "futures_usdt"  # Default to USDT for legacy type
    raise ValueError(f"Unsupported market type: {market_type}")


# Unit tests
if __name__ == "__main__":
    # Test the get_market_type_str function with all market types
    market_types = [
        (MarketType.SPOT, "spot"),
        (MarketType.FUTURES_USDT, "futures_usdt"),
        (MarketType.FUTURES_COIN, "futures_coin"),
        (MarketType.FUTURES, "futures_usdt"),  # Legacy type defaults to USDT
    ]

    print("[bold cyan]Testing get_market_type_str function...[/bold cyan]")
    for market_type, expected in market_types:
        result = get_market_type_str(market_type)
        if result == expected:
            print(f"[green]✓[/green] {market_type.name} -> {result}")
        else:
            print(f"[red]✗[/red] {market_type.name} -> {result} (expected {expected})")

    # Test unsupported market type
    try:
        get_market_type_str(MarketType.OPTIONS)
        print("[red]✗[/red] OPTIONS did not raise ValueError")
    except ValueError as e:
        print(f"[green]✓[/green] OPTIONS raised ValueError: {e}")
