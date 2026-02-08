#!/usr/bin/env python3
# polars-exception: OKXRestClient returns pandas DataFrames for CKVD pipeline compatibility
# ADR: docs/adr/2026-01-30-claude-code-infrastructure.md
"""OKX REST API client for fetching market data.

OKX provides two candlestick endpoints:
- /api/v5/market/candles - Recent data (up to 300 records, ~24h for 1m)
- /api/v5/market/history-candles - Historical data (up to 100 records, back to Jan 2018)

Key differences from Binance:
- Symbol format: BTC-USDT (hyphenated), BTC-USD-SWAP (perpetual futures)
- Interval format: Case-sensitive for hours+ (1H, 1D not 1h, 1d)
- Response format: Array of arrays with 9 fields including confirm flag
- No Vision API - all historical data via REST endpoints
"""

import time
from datetime import datetime
from typing import Any

import httpx
import pandas as pd

from ckvd.core.providers.binance.data_client_interface import DataClientInterface
from ckvd.utils.config import DEFAULT_HTTP_TIMEOUT_SECONDS
from ckvd.utils.for_core.rest_exceptions import (
    APIError,
    HTTPError,
    JSONDecodeError,
    NetworkError,
    RateLimitError,
    RestAPIError,
)
from ckvd.utils.loguru_setup import logger
from ckvd.utils.market_constraints import (
    ChartType,
    DataProvider,
    Interval,
    MarketType,
    get_market_capabilities,
)
from ckvd.utils.time_utils import (
    datetime_to_milliseconds,
)

# OKX API constants
OKX_API_BASE_URL = "https://www.okx.com/api/v5"
OKX_CANDLES_ENDPOINT = f"{OKX_API_BASE_URL}/market/candles"
OKX_HISTORY_CANDLES_ENDPOINT = f"{OKX_API_BASE_URL}/market/history-candles"

# OKX-specific limits
OKX_CANDLES_MAX_LIMIT = 300
OKX_HISTORY_CANDLES_MAX_LIMIT = 100

# Retry configuration
MAX_RETRIES = 3
RETRY_DELAY = 1  # seconds

# OKX candlestick response field indices
OKX_TIMESTAMP_IDX = 0
OKX_OPEN_IDX = 1
OKX_HIGH_IDX = 2
OKX_LOW_IDX = 3
OKX_CLOSE_IDX = 4
OKX_VOLUME_IDX = 5
# OKX_VOLUME_USD_IDX = 6  # Not used in standard OHLCV
# OKX_TURNOVER_IDX = 7    # Not used in standard OHLCV
# OKX_CONFIRM_IDX = 8     # Not used in standard OHLCV

# OKX interval mapping (CKVD Interval → OKX API string)
# OKX requires uppercase for hour+ intervals (1H, 1D, etc.)
OKX_INTERVAL_MAP = {
    Interval.MINUTE_1: "1m",
    Interval.MINUTE_3: "3m",
    Interval.MINUTE_5: "5m",
    Interval.MINUTE_15: "15m",
    Interval.MINUTE_30: "30m",
    Interval.HOUR_1: "1H",  # OKX requires uppercase
    Interval.HOUR_2: "2H",
    Interval.HOUR_4: "4H",
    Interval.HOUR_6: "6H",
    Interval.HOUR_12: "12H",
    Interval.DAY_1: "1D",  # OKX requires uppercase
    Interval.WEEK_1: "1W",
    Interval.MONTH_1: "1M",
}


def _convert_symbol_to_okx(symbol: str, market_type: MarketType) -> str:
    """Convert Binance-style symbol to OKX format.

    Args:
        symbol: Symbol in Binance format (e.g., BTCUSDT) or OKX format (BTC-USDT)
        market_type: Market type to determine symbol format

    Returns:
        OKX-formatted symbol (e.g., BTC-USDT for spot, BTC-USD-SWAP for futures)
    """
    # If already in OKX format (contains hyphen), return as-is
    if "-" in symbol:
        return symbol

    # Convert Binance format to OKX format
    # Common quote currencies
    for quote in ["USDT", "USDC", "USD", "BTC", "ETH"]:
        if symbol.endswith(quote):
            base = symbol[: -len(quote)]
            if market_type == MarketType.SPOT:
                return f"{base}-{quote}"
            if market_type in (MarketType.FUTURES_USDT, MarketType.FUTURES_COIN, MarketType.FUTURES):
                return f"{base}-USD-SWAP"
            break

    # Default: assume USDT spot
    logger.warning(f"Could not parse symbol {symbol}, returning as-is with USDT suffix")
    return f"{symbol}-USDT"


def _convert_interval_to_okx(interval: Interval) -> str:
    """Convert CKVD Interval to OKX API interval string.

    Args:
        interval: CKVD Interval enum

    Returns:
        OKX interval string (e.g., "1H" not "1h")

    Raises:
        ValueError: If interval is not supported by OKX
    """
    if interval not in OKX_INTERVAL_MAP:
        raise ValueError(f"Interval {interval.value} is not supported by OKX. Supported intervals: {list(OKX_INTERVAL_MAP.keys())}")
    return OKX_INTERVAL_MAP[interval]


class OKXRestClient(DataClientInterface):
    """OKX REST API client for fetching candlestick data.

    This client implements the DataClientInterface for OKX's REST API.
    It handles both recent data (candles endpoint) and historical data
    (history-candles endpoint) with automatic pagination.
    """

    def __init__(
        self,
        market_type: MarketType,
        retry_count: int = MAX_RETRIES,
        fetch_timeout: float = DEFAULT_HTTP_TIMEOUT_SECONDS,
        client: httpx.Client | None = None,
        symbol: str = "BTC-USDT",
        interval: Interval = Interval.MINUTE_1,
    ) -> None:
        """Initialize the OKX REST client.

        Args:
            market_type: Market type (SPOT, FUTURES_USDT)
            retry_count: Number of retry attempts for failed requests
            fetch_timeout: Timeout in seconds for HTTP requests
            client: Optional pre-configured HTTP client
            symbol: Default symbol (OKX format: BTC-USDT)
            interval: Default interval
        """
        self.market_type = market_type
        self.retry_count = retry_count
        self.fetch_timeout = fetch_timeout
        self._client = client
        self._symbol = symbol
        self._interval = interval

        # Get capabilities for validation
        self._capabilities = get_market_capabilities(market_type, DataProvider.OKX)

        logger.debug(f"Initialized OKXRestClient with market_type={market_type.name}")

    def _ensure_client(self) -> httpx.Client:
        """Ensure HTTP client is initialized.

        Returns:
            httpx.Client instance
        """
        if self._client is None:
            self._client = httpx.Client(
                timeout=self.fetch_timeout,
                headers={"Accept": "application/json"},
            )
        return self._client

    def _request_with_retry(
        self,
        url: str,
        params: dict[str, Any],
    ) -> dict[str, Any]:
        """Make HTTP request with retry logic.

        Args:
            url: API endpoint URL
            params: Query parameters

        Returns:
            JSON response as dictionary

        Raises:
            RateLimitError: If rate limited after all retries
            HTTPError: If HTTP error after all retries
            NetworkError: If network error after all retries
        """
        client = self._ensure_client()

        for attempt in range(self.retry_count):
            try:
                response = client.get(url, params=params)
                response.raise_for_status()
                return response.json()

            except httpx.HTTPStatusError as e:
                if e.response.status_code == 429:
                    if attempt < self.retry_count - 1:
                        wait_time = RETRY_DELAY * (attempt + 1)
                        logger.warning(f"OKX rate limited, waiting {wait_time}s before retry")
                        time.sleep(wait_time)
                    else:
                        raise RateLimitError("OKX rate limit exceeded") from e
                elif attempt < self.retry_count - 1:
                    time.sleep(RETRY_DELAY * (attempt + 1))
                else:
                    raise HTTPError(f"HTTP {e.response.status_code}: {e}") from e

            except httpx.TimeoutException as e:
                if attempt < self.retry_count - 1:
                    time.sleep(RETRY_DELAY * (attempt + 1))
                else:
                    raise NetworkError(f"Timeout: {e}") from e

            except httpx.RequestError as e:
                if attempt < self.retry_count - 1:
                    time.sleep(RETRY_DELAY * (attempt + 1))
                else:
                    raise NetworkError(f"Network error: {e}") from e

        # Should not reach here, but return empty dict as safety
        return {}

    def _fetch_candles(
        self,
        okx_symbol: str,
        okx_interval: str,
        limit: int = OKX_CANDLES_MAX_LIMIT,
        after_ms: int | None = None,
        before_ms: int | None = None,
        use_history: bool = False,
    ) -> list[list[str]]:
        """Fetch candles from OKX API.

        Args:
            okx_symbol: OKX-formatted symbol (e.g., BTC-USDT)
            okx_interval: OKX interval string (e.g., 1H)
            limit: Maximum records to fetch
            after_ms: Fetch data after this timestamp (exclusive)
            before_ms: Fetch data before this timestamp (exclusive)
            use_history: Use history-candles endpoint instead of candles

        Returns:
            List of candle data arrays
        """
        endpoint = OKX_HISTORY_CANDLES_ENDPOINT if use_history else OKX_CANDLES_ENDPOINT
        max_limit = OKX_HISTORY_CANDLES_MAX_LIMIT if use_history else OKX_CANDLES_MAX_LIMIT

        params: dict[str, Any] = {
            "instId": okx_symbol,
            "bar": okx_interval,
            "limit": min(limit, max_limit),
        }

        if after_ms is not None:
            params["after"] = after_ms
        if before_ms is not None:
            params["before"] = before_ms

        try:
            response = self._request_with_retry(endpoint, params)

            # Check OKX response code
            code = response.get("code")
            if code != "0":
                msg = response.get("msg", "Unknown error")
                raise APIError(f"OKX API error {code}: {msg}")

            return response.get("data", [])

        except (RateLimitError, HTTPError, NetworkError):
            raise
        except (KeyError, TypeError, ValueError) as e:
            raise JSONDecodeError(f"Invalid OKX response: {e}") from e

    def _process_candles(self, raw_data: list[list[str]]) -> pd.DataFrame:
        """Process OKX candle data into DataFrame.

        Args:
            raw_data: List of candle arrays from OKX API

        Returns:
            DataFrame with open_time index and OHLCV columns
        """
        if not raw_data:
            return self.create_empty_dataframe()

        # Convert to DataFrame
        records = []
        for candle in raw_data:
            try:
                records.append(
                    {
                        "open_time": pd.Timestamp(int(candle[OKX_TIMESTAMP_IDX]), unit="ms", tz="UTC"),
                        "open": float(candle[OKX_OPEN_IDX]),
                        "high": float(candle[OKX_HIGH_IDX]),
                        "low": float(candle[OKX_LOW_IDX]),
                        "close": float(candle[OKX_CLOSE_IDX]),
                        "volume": float(candle[OKX_VOLUME_IDX]),
                    }
                )
            except (IndexError, ValueError, TypeError) as e:
                logger.warning(f"Skipping malformed candle: {candle}, error: {e}")
                continue

        if not records:
            return self.create_empty_dataframe()

        df = pd.DataFrame(records)
        df = df.set_index("open_time")
        return df.sort_index()

    def _parse_interval(self, interval: str | Interval) -> Interval:
        """Parse interval string or enum to Interval enum.

        Args:
            interval: Time interval string or Interval enum

        Returns:
            Interval enum

        Raises:
            ValueError: If interval is invalid
        """
        if isinstance(interval, Interval):
            return interval
        if isinstance(interval, str):
            try:
                return Interval(interval)
            except ValueError as e:
                # Try common variations (case-insensitive)
                interval_lower = interval.lower()
                for i in Interval:
                    if i.value.lower() == interval_lower:
                        return i
                raise ValueError(f"Invalid interval: {interval}") from e
        return self._interval

    def _fetch_paginated(
        self,
        okx_symbol: str,
        okx_interval: str,
        start_ms: int,
        end_ms: int,
    ) -> list[list[str]]:
        """Fetch all data with pagination.

        Args:
            okx_symbol: OKX-formatted symbol
            okx_interval: OKX interval string
            start_ms: Start timestamp in milliseconds
            end_ms: End timestamp in milliseconds

        Returns:
            List of all candle data arrays
        """
        all_data: list[list[str]] = []
        current_before = end_ms
        use_history = True
        max_iterations = 1000

        for _ in range(max_iterations):
            try:
                candles = self._fetch_candles(
                    okx_symbol,
                    okx_interval,
                    after_ms=start_ms,
                    before_ms=current_before,
                    use_history=use_history,
                )
                if not candles:
                    if use_history:
                        use_history = False
                        continue
                    break

                all_data.extend(candles)
                oldest_ts = min(int(c[OKX_TIMESTAMP_IDX]) for c in candles)
                if oldest_ts <= start_ms:
                    break
                current_before = oldest_ts

            except RateLimitError:
                raise
            except (HTTPError, APIError, NetworkError, JSONDecodeError) as e:
                if use_history:
                    use_history = False
                    continue
                raise RestAPIError(f"OKX fetch failed: {e}") from e

        return all_data

    def fetch(
        self,
        symbol: str,
        interval: str,
        start_time: datetime,
        end_time: datetime,
        **kwargs,
    ) -> pd.DataFrame:
        """Fetch candlestick data from OKX.

        Args:
            symbol: Trading pair symbol (Binance or OKX format)
            interval: Time interval string (e.g., "1m", "1h")
            start_time: Start time (timezone-aware UTC)
            end_time: End time (timezone-aware UTC)
            **kwargs: Additional parameters (unused)

        Returns:
            DataFrame with OHLCV data indexed by open_time

        Raises:
            ValueError: If parameters are invalid
            RateLimitError: If rate limited
            RestAPIError: If API error occurs
        """
        okx_symbol = _convert_symbol_to_okx(symbol, self.market_type)
        interval_enum = self._parse_interval(interval)
        okx_interval = _convert_interval_to_okx(interval_enum)

        start_ms = datetime_to_milliseconds(start_time)
        end_ms = datetime_to_milliseconds(end_time)

        logger.info(f"Fetching OKX {okx_interval} data for {okx_symbol} from {start_time.isoformat()} to {end_time.isoformat()}")

        all_data = self._fetch_paginated(okx_symbol, okx_interval, start_ms, end_ms)

        if not all_data:
            logger.warning(f"No data retrieved from OKX for {okx_symbol}")
            return self.create_empty_dataframe()

        df = self._process_candles(all_data)

        # Filter to requested time range
        df = df[(df.index >= start_time) & (df.index <= end_time)]

        # Remove duplicates (can occur at pagination boundaries)
        df = df[~df.index.duplicated(keep="first")]

        logger.info(f"Successfully retrieved {len(df)} records from OKX for {okx_symbol}")

        return df

    def create_empty_dataframe(self) -> pd.DataFrame:
        """Create an empty DataFrame with the correct OHLCV structure.

        Returns:
            Empty DataFrame with open_time index and OHLCV columns
        """
        df = pd.DataFrame(columns=["open", "high", "low", "close", "volume"])
        df.index.name = "open_time"
        return df

    def validate_data(self, df: pd.DataFrame) -> tuple[bool, str | None]:
        """Validate that DataFrame contains valid OHLCV data.

        Args:
            df: DataFrame to validate

        Returns:
            Tuple of (is_valid, error_message)
        """
        if df.empty:
            return True, None  # Empty is valid

        # Check required columns
        required = ["open", "high", "low", "close", "volume"]
        missing = [col for col in required if col not in df.columns]
        if missing:
            return False, f"Missing columns: {missing}"

        # Check index is datetime
        if not isinstance(df.index, pd.DatetimeIndex):
            return False, "Index must be DatetimeIndex"

        # Check OHLC constraints
        if not (df["high"] >= df["low"]).all():
            return False, "High must be >= Low"

        if not (df["volume"] >= 0).all():
            return False, "Volume must be non-negative"

        return True, None

    def close(self) -> None:
        """Close the HTTP client and release resources."""
        if self._client is not None:
            self._client.close()
            self._client = None
            logger.debug("Closed OKX HTTP client")

    @property
    def symbol(self) -> str:
        """Get the default symbol."""
        return self._symbol

    @property
    def interval(self) -> str:
        """Get the default interval."""
        return self._interval.value if isinstance(self._interval, Interval) else str(self._interval)

    @property
    def provider(self) -> DataProvider:
        """Get the data provider (always OKX)."""
        return DataProvider.OKX

    @property
    def chart_type(self) -> ChartType:
        """Get the chart type (OKX candles)."""
        return ChartType.OKX_CANDLES

    def __enter__(self) -> "OKXRestClient":
        """Context manager entry."""
        self._ensure_client()
        return self

    def __exit__(self, _exc_type, _exc_val, _exc_tb) -> None:
        """Context manager exit."""
        self.close()
