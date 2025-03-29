#!/usr/bin/env python3
import asyncio
import curl_cffi.requests as curl_requests


async def test_curl_cffi():
    url = "https://data.binance.vision/data/spot/daily/klines/BTCUSDT/4h/BTCUSDT-4h-2025-03-28.zip"

    print("Testing curl_cffi with HEAD request...")

    # Test HEAD request
    async with curl_requests.AsyncSession() as session:
        try:
            response = await session.head(url, timeout=5.0)
            print(f"HEAD Status code: {response.status_code}")
            print(f"HEAD Headers: {response.headers}")
        except Exception as e:
            print(f"HEAD Error: {e}")

    print("\nTesting curl_cffi with GET request...")

    # Test GET request
    async with curl_requests.AsyncSession() as session:
        try:
            response = await session.get(url, timeout=5.0)
            print(f"GET Status code: {response.status_code}")
            print(f"GET Headers: {response.headers}")
            # Check if we got content
            print(
                f"Content length: {len(response.content) if response.content else 'N/A'}"
            )
        except Exception as e:
            print(f"GET Error: {e}")


if __name__ == "__main__":
    asyncio.run(test_curl_cffi())
