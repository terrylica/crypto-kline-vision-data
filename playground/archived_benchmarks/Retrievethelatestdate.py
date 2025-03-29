import httpx
import asyncio
from datetime import datetime, timedelta
import time
import argparse

# Command-line arguments
parser = argparse.ArgumentParser(description="Test Binance Vision data availability")
parser.add_argument(
    "--market",
    default="spot",
    choices=["spot", "um", "cm"],
    help="Market type: spot, um (USDT-Margined Futures), or cm (Coin-Margined Futures)",
)
parser.add_argument("--symbol", default="BTCUSDT", help="Trading pair symbol")
parser.add_argument(
    "--interval",
    default="1m",
    choices=[
        "1s",
        "1m",
        "3m",
        "5m",
        "15m",
        "30m",
        "1h",
        "2h",
        "4h",
        "6h",
        "8h",
        "12h",
        "1d",
    ],
    help="Time interval",
)
parser.add_argument("--days", type=int, default=10, help="Maximum days to check back")
parser.add_argument("--debug", action="store_true", help="Enable debug output")
parser.add_argument(
    "--timeout", type=float, default=5.0, help="Request timeout in seconds"
)
parser.add_argument(
    "--max-retries", type=int, default=3, help="Maximum number of retries"
)
parser.add_argument("--direct-url", help="Check a specific URL directly")
args = parser.parse_args()

# Constants from verify_multi_interval.sh
SPOT_SYMBOLS = "BTCUSDT ETHUSDT BNBUSDT LTCUSDT ADAUSDT XRPUSDT EOSUSDT XLMUSDT TRXUSDT ETCUSDT ICXUSDT VETUSDT LINKUSDT ZILUSDT XMRUSDT THETAUSDT MATICUSDT ATOMUSDT FTMUSDT ALGOUSDT DOGEUSDT CHZUSDT XTZUSDT BCHUSDT KNCUSDT MANAUSDT SOLUSDT SANDUSDT CRVUSDT DOTUSDT LUNAUSDT EGLDUSDT RUNEUSDT UNIUSDT AVAXUSDT NEARUSDT AAVEUSDT FILUSDT AXSUSDT ROSEUSDT GALAUSDT ENSUSDT GMTUSDT APEUSDT OPUSDT APTUSDT SUIUSDT WLDUSDT WIFUSDT DOGSUSDT"
UM_SYMBOLS = SPOT_SYMBOLS  # Same symbols for USDT-Margined Futures
CM_SYMBOLS = (
    "BTCUSD_PERP ETHUSD_PERP BCHUSD_PERP"  # Example Coin-Margined Futures symbols
)

SPOT_INTERVALS = [
    "1s",
    "1m",
    "3m",
    "5m",
    "15m",
    "30m",
    "1h",
    "2h",
    "4h",
    "6h",
    "8h",
    "12h",
    "1d",
]
UM_CM_INTERVALS = [
    "1m",
    "3m",
    "5m",
    "15m",
    "30m",
    "1h",
    "2h",
    "4h",
    "6h",
    "8h",
    "12h",
    "1d",
]  # No 1s for futures


def get_base_url(market_type, symbol):
    """Get the base URL for data download based on market type."""
    if market_type == "spot":
        return f"https://data.binance.vision/data/spot/daily/klines/{symbol}"
    elif market_type == "um":
        return f"https://data.binance.vision/data/futures/um/daily/klines/{symbol}"
    elif market_type == "cm":
        return f"https://data.binance.vision/data/futures/cm/daily/klines/{symbol}"
    else:
        raise ValueError(f"Invalid market type: {market_type}")


async def check_url_availability(client, url, timeout, debug=False):
    """Check if a URL is available using GET request."""
    try:
        start_time = time.time()

        response = await client.head(url, timeout=timeout, follow_redirects=True)
        elapsed = time.time() - start_time

        if debug:
            print(f"Status code: {response.status_code}")
            print(f"Response time: {elapsed:.2f} seconds")

        return response.status_code == 200, response.status_code, elapsed

    except httpx.TimeoutException:
        elapsed = time.time() - start_time
        if debug:
            print(f"Timeout error")
            print(f"Timeout after: {elapsed:.2f} seconds")
        return False, 408, elapsed

    except Exception as e:
        elapsed = time.time() - start_time
        if debug:
            print(f"Error: {e}")
            print(f"Error time: {elapsed:.2f} seconds")
        return False, None, elapsed


async def check_latest_date(
    market_type,
    symbol,
    interval,
    max_days_back=10,
    debug=False,
    timeout=5.0,
    max_retries=3,
):
    """Check the latest available date for a given symbol and interval."""
    # Validate interval for market type
    if market_type != "spot" and interval == "1s":
        raise ValueError(
            f"1s interval is only available for spot market, not {market_type}"
        )

    base_url = get_base_url(market_type, symbol)
    current_date = datetime.utcnow()

    if debug:
        print(f"Checking latest date for {market_type}/{symbol}/{interval}...")
        print(f"Base URL: {base_url}")
        print(f"Max days back: {max_days_back}")
        print(f"Timeout: {timeout} seconds")
        print(f"Max retries: {max_retries}")

    async with httpx.AsyncClient(timeout=timeout) as client:
        for i in range(max_days_back + 1):
            check_date = (current_date - timedelta(days=i)).strftime("%Y-%m-%d")
            url = f"{base_url}/{interval}/{symbol}-{interval}-{check_date}.zip"

            if debug:
                print(f"\nChecking date: {check_date}")
                print(f"URL: {url}")

            # Try up to max_retries times with increasing backoff
            for retry in range(max_retries):
                if retry > 0 and debug:
                    print(f"Retry #{retry}...")

                is_available, status_code, elapsed = await check_url_availability(
                    client, url, timeout, debug=debug
                )

                if is_available:
                    if debug:
                        print(f"Found latest date: {check_date}")
                    return check_date

                # Only add delay if we're going to retry
                if retry < max_retries - 1:
                    await asyncio.sleep(0.5 * (retry + 1))  # Increasing backoff

    return f"No data found in the last {max_days_back} days"


async def check_specific_url(url, debug=False, timeout=5.0, max_retries=3):
    """Check if a specific URL is available."""
    if debug:
        print(f"Checking URL: {url}")
        print(f"Timeout: {timeout} seconds")
        print(f"Max retries: {max_retries}")

    async with httpx.AsyncClient(timeout=timeout) as client:
        # Try up to max_retries times with increasing backoff
        for retry in range(max_retries):
            if retry > 0 and debug:
                print(f"Retry #{retry}...")

            is_available, status_code, elapsed = await check_url_availability(
                client, url, timeout, debug=debug
            )

            if is_available:
                return f"URL is available (verified with HEAD request)"

            # If we get a 404 or timeout, try with GET instead
            if status_code == 404 or status_code == 408:
                try:
                    start_time = time.time()
                    response = await client.get(
                        url, timeout=timeout, follow_redirects=True
                    )
                    elapsed = time.time() - start_time

                    if debug:
                        print(f"GET status code: {response.status_code}")
                        print(f"GET response time: {elapsed:.2f} seconds")

                    if response.status_code == 200:
                        return f"URL is available (verified with GET request)"
                except Exception as e:
                    if debug:
                        print(f"GET error: {e}")

            # Only add delay if we're going to retry
            if retry < max_retries - 1:
                await asyncio.sleep(0.5 * (retry + 1))  # Increasing backoff

    return "URL is not available"


async def main():
    # If direct URL is provided, check that URL
    if args.direct_url:
        result = await check_specific_url(
            args.direct_url,
            debug=args.debug,
            timeout=args.timeout,
            max_retries=args.max_retries,
        )
        print(result)
        return

    market_type = args.market
    symbol = args.symbol
    interval = args.interval

    # Validate input parameters
    if market_type == "spot" and interval not in SPOT_INTERVALS:
        print(f"Error: Invalid interval '{interval}' for spot market.")
        return
    elif market_type in ["um", "cm"] and interval not in UM_CM_INTERVALS:
        print(f"Error: Invalid interval '{interval}' for {market_type} market.")
        return

    # Display available options if in debug mode
    if args.debug:
        print("Market types: spot, um, cm")
        print(f"Spot symbols: {SPOT_SYMBOLS}")
        print(f"UM symbols: {UM_SYMBOLS}")
        print(f"CM symbols: {CM_SYMBOLS}")
        print(f"Spot intervals: {', '.join(SPOT_INTERVALS)}")
        print(f"Futures intervals: {', '.join(UM_CM_INTERVALS)}")
        print("----------------------------------------")

    start_time = time.time()
    latest_date = await check_latest_date(
        market_type,
        symbol,
        interval,
        max_days_back=args.days,
        debug=args.debug,
        timeout=args.timeout,
        max_retries=args.max_retries,
    )
    total_time = time.time() - start_time

    print(f"{market_type}/{symbol}/{interval}: {latest_date}")
    print(f"Total execution time: {total_time:.2f} seconds")


if __name__ == "__main__":
    asyncio.run(main())
