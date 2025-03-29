#!/usr/bin/env python3
import asyncio
from datetime import datetime, timedelta
import time
import argparse
import warnings

# Suppress specific CURL warnings
warnings.filterwarnings("ignore", message=".*SSLKEYLOGFILE.*")

# Import curl_cffi for high-performance HTTP requests
try:
    import curl_cffi.requests as curl_requests
except ImportError:
    print("ERROR: curl_cffi is not installed. Run: pip install curl-cffi")
    exit(1)

# Command-line arguments
parser = argparse.ArgumentParser(
    description="Test Binance Vision data availability (curl_cffi no-retry)"
)
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
    "--timeout", type=float, default=3.0, help="Request timeout in seconds"
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


async def check_url_availability(session, url, timeout, debug=False):
    """Check if a URL is available using curl_cffi."""
    try:
        start_time = time.time()

        # Use head request first
        response = await session.head(url, timeout=timeout)
        status_code = response.status_code
        success = status_code == 200

        elapsed = time.time() - start_time

        if debug:
            print(f"Status code: {status_code}")
            print(f"Response time: {elapsed:.2f} seconds")

        return success, status_code, elapsed

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
    timeout=3.0,
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

    # Use a single session for all requests
    async with curl_requests.AsyncSession() as session:
        for i in range(max_days_back + 1):
            check_date = (current_date - timedelta(days=i)).strftime("%Y-%m-%d")
            url = f"{base_url}/{interval}/{symbol}-{interval}-{check_date}.zip"

            if debug:
                print(f"\nChecking date: {check_date}")
                print(f"URL: {url}")

            # Try HEAD request first
            is_available, status_code, elapsed = await check_url_availability(
                session, url, timeout, debug=debug
            )

            if is_available:
                if debug:
                    print(f"Found latest date: {check_date}")
                return check_date

            # If HEAD failed, try GET as fallback
            if status_code == 404 or status_code == 408 or status_code is None:
                try:
                    start_time = time.time()

                    response = await session.get(url, timeout=timeout)
                    status_code = response.status_code
                    success = status_code == 200

                    elapsed = time.time() - start_time

                    if debug:
                        print(f"GET status code: {status_code}")
                        print(f"GET response time: {elapsed:.2f} seconds")

                    if success:
                        if debug:
                            print(f"Found latest date with GET: {check_date}")
                        return check_date

                except Exception as e:
                    if debug:
                        print(f"GET error: {e}")

    return f"No data found in the last {max_days_back} days"


async def check_specific_url(url, debug=False, timeout=3.0):
    """Check if a specific URL is available."""
    if debug:
        print(f"Checking URL: {url}")
        print(f"Timeout: {timeout} seconds")

    async with curl_requests.AsyncSession() as session:
        # Try HEAD request first
        is_available, status_code, elapsed = await check_url_availability(
            session, url, timeout, debug=debug
        )

        if is_available:
            return f"URL is available (verified with HEAD request)"

        # If HEAD failed, try GET as fallback
        if status_code == 404 or status_code == 408 or status_code is None:
            try:
                start_time = time.time()

                response = await session.get(url, timeout=timeout)
                status_code = response.status_code
                success = status_code == 200

                elapsed = time.time() - start_time

                if debug:
                    print(f"GET status code: {status_code}")
                    print(f"GET response time: {elapsed:.2f} seconds")

                if success:
                    return f"URL is available (verified with GET request)"

            except Exception as e:
                if debug:
                    print(f"GET error: {e}")

    return "URL is not available"


async def main():
    # If direct URL is provided, check that URL
    if args.direct_url:
        result = await check_specific_url(
            args.direct_url,
            debug=args.debug,
            timeout=args.timeout,
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
    )
    total_time = time.time() - start_time

    print(f"{market_type}/{symbol}/{interval}: {latest_date}")
    print(f"Total execution time: {total_time:.2f} seconds")


if __name__ == "__main__":
    asyncio.run(main())
