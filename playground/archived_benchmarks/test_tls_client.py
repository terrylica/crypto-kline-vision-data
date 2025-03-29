#!/usr/bin/env python3
import asyncio
import tls_client
from functools import wraps, partial


# Wrap synchronous functions to make them async-compatible
def async_wrap(func):
    @wraps(func)
    async def run(*args, loop=None, executor=None, **kwargs):
        if loop is None:
            loop = asyncio.get_event_loop()
        pfunc = partial(func, *args, **kwargs)
        return await loop.run_in_executor(executor, pfunc)

    return run


async def test_tls_client():
    url = "https://data.binance.vision/data/spot/daily/klines/BTCUSDT/4h/BTCUSDT-4h-2025-03-28.zip"

    print("Testing tls_client with HEAD request...")

    # Test HEAD request
    try:
        session = tls_client.Session(client_identifier="chrome_110")
        head_async = async_wrap(session.head)
        response = await head_async(url, allow_redirects=True)
        print(f"HEAD Status code: {response.status_code}")
        print(f"HEAD Headers: {dict(response.headers)}")
    except Exception as e:
        print(f"HEAD Error: {e}")

    print("\nTesting tls_client with GET request...")

    # Test GET request
    try:
        session = tls_client.Session(client_identifier="chrome_110")
        get_async = async_wrap(session.get)
        response = await get_async(url, allow_redirects=True)
        print(f"GET Status code: {response.status_code}")
        print(f"GET Headers: {dict(response.headers)}")
        # Check if we got content
        print(f"Content length: {len(response.content) if response.content else 'N/A'}")
    except Exception as e:
        print(f"GET Error: {e}")


if __name__ == "__main__":
    asyncio.run(test_tls_client())
