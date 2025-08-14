#!/usr/bin/env python3

from rich import print
from rich.console import Console
from rich.table import Table

from data_source_manager.utils.market_constraints import (
    ChartType,
    DataProvider,
    MarketType,
    get_endpoint_url,
    get_market_symbol_format,
    validate_symbol_for_market_type,
)


def test_okx_symbol_formatting():
    """Test OKX symbol formatting."""
    test_cases = [
        # Original symbol, Market type, Expected formatted symbol
        ("BTCUSDT", MarketType.SPOT, "BTC-USDT"),
        ("ETHUSDT", MarketType.SPOT, "ETH-USDT"),
        ("BTC-USDT", MarketType.SPOT, "BTC-USDT"),  # Already formatted
        ("BTCUSDT", MarketType.FUTURES_USDT, "BTC-USD-SWAP"),
        ("ETHUSDT", MarketType.FUTURES_USDT, "ETH-USD-SWAP"),
        ("BTC-USD-SWAP", MarketType.FUTURES_USDT, "BTC-USD-SWAP"),  # Already formatted
    ]

    results = []
    for original, market_type, expected in test_cases:
        try:
            result = get_market_symbol_format(original, market_type, DataProvider.OKX)
            status = "PASS" if result == expected else "FAIL"
        except Exception as e:
            result = str(e)
            status = "ERROR"

        results.append(
            {
                "original": original,
                "market_type": market_type.name,
                "expected": expected,
                "result": result,
                "status": status,
            }
        )

    return results


def test_okx_symbol_validation():
    """Test OKX symbol validation."""
    test_cases = [
        # Symbol, Market type, Expected valid?
        ("BTC-USDT", MarketType.SPOT, True),
        ("BTCUSDT", MarketType.SPOT, False),  # Missing hyphen
        ("BTC-USD-SWAP", MarketType.FUTURES_USDT, True),
        ("BTCUSDT", MarketType.FUTURES_USDT, False),  # Missing hyphen and SWAP
        ("BTC-USD", MarketType.FUTURES_USDT, False),  # Missing SWAP suffix
    ]

    results = []
    for symbol, market_type, expected_valid in test_cases:
        try:
            validate_symbol_for_market_type(symbol, market_type, DataProvider.OKX)
            is_valid = True
            message = "Valid"
            status = "PASS" if expected_valid else "FAIL"
        except ValueError as e:
            is_valid = False
            message = str(e)
            status = "PASS" if not expected_valid else "FAIL"
        except Exception as e:
            is_valid = False
            message = str(e)
            status = "ERROR"

        results.append(
            {
                "symbol": symbol,
                "market_type": market_type.name,
                "expected_valid": expected_valid,
                "is_valid": is_valid,
                "message": message if not is_valid else "Valid",
                "status": status,
            }
        )

    return results


def test_endpoint_urls():
    """Test OKX endpoint URLs."""
    test_cases = [
        # Market type, ChartType, Expected URL pattern
        (
            MarketType.SPOT,
            ChartType.OKX_CANDLES,
            "https://www.okx.com/api/v5/market/candles",
        ),
        (
            MarketType.SPOT,
            ChartType.OKX_HISTORY_CANDLES,
            "https://www.okx.com/api/v5/market/history-candles",
        ),
        (
            MarketType.FUTURES_USDT,
            ChartType.OKX_CANDLES,
            "https://www.okx.com/api/v5/market/candles",
        ),
        (
            MarketType.FUTURES_USDT,
            ChartType.OKX_HISTORY_CANDLES,
            "https://www.okx.com/api/v5/market/history-candles",
        ),
    ]

    results = []
    for market_type, chart_type, expected_url in test_cases:
        try:
            url = get_endpoint_url(
                market_type, chart_type, data_provider=DataProvider.OKX
            )
            status = "PASS" if url == expected_url else "FAIL"
        except Exception as e:
            url = str(e)
            status = "ERROR"

        results.append(
            {
                "market_type": market_type.name,
                "chart_type": chart_type.name,
                "expected_url": expected_url,
                "url": url,
                "status": status,
            }
        )

    return results


def print_results_table(title, results):
    """Print results in a formatted table."""
    console = Console()
    table = Table(title=title)

    # Add columns based on the first result's keys
    if results and len(results) > 0:
        for key in results[0].keys():
            table.add_column(key)

        # Add rows
        for result in results:
            row = []
            for key in results[0].keys():
                if key == "status":
                    value = (
                        f"[green]{result[key]}[/green]"
                        if result[key] == "PASS"
                        else f"[red]{result[key]}[/red]"
                    )
                else:
                    value = str(result.get(key, ""))
                row.append(value)
            table.add_row(*row)

    console.print(table)


def main():
    print("[bold green]OKX Symbol and Endpoint Tests[/bold green]")

    # Test 1: Symbol Formatting
    print("\n[bold blue]Testing OKX Symbol Formatting[/bold blue]")
    formatting_results = test_okx_symbol_formatting()
    print_results_table("Symbol Formatting Results", formatting_results)

    # Test 2: Symbol Validation
    print("\n[bold blue]Testing OKX Symbol Validation[/bold blue]")
    validation_results = test_okx_symbol_validation()
    print_results_table("Symbol Validation Results", validation_results)

    # Test 3: Endpoint URLs
    print("\n[bold blue]Testing OKX Endpoint URLs[/bold blue]")
    endpoint_results = test_endpoint_urls()
    print_results_table("Endpoint URL Results", endpoint_results)

    print("\n[bold green]Test Complete![/bold green]")


if __name__ == "__main__":
    main()
