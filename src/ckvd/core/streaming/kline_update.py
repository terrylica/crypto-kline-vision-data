#!/usr/bin/env python3
# ADR: docs/adr/2025-01-30-failover-control-protocol.md
"""KlineUpdate: immutable dataclass for a single WebSocket kline event.

Wraps one Binance kline message from the WebSocket stream into a typed,
frozen dataclass. Supports both live stream parsing and historical backfill
(gap-filling via FCP REST).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pandas as pd


@dataclass(frozen=True, slots=True)
class KlineUpdate:
    """A single kline update from the WebSocket stream.

    Immutable (frozen=True) and memory-efficient (slots=True).

    Attributes:
        symbol: Trading pair symbol (e.g. "BTCUSDT").
        interval: Candle interval string (e.g. "1h", "1m").
        open_time: UTC datetime — start of the candle period.
        open: Opening price.
        high: Highest price in the period.
        low: Lowest price in the period.
        close: Closing price (latest price if candle not yet closed).
        volume: Base asset volume.
        close_time: UTC datetime — end of the candle period.
        is_closed: True when k.x=True — candle is finalized/closed.
            When False, the candle is still accumulating updates.
        data_source: Always "STREAMING" for live updates;
            "REST" or "CACHE" for historical backfill rows.

    Example:
        >>> raw = {"k": {"s": "BTCUSDT", "i": "1h", "t": 1000, "T": 3600000,
        ...              "o": "42000", "h": "42100", "l": "41900", "c": "42050",
        ...              "v": "100", "x": True}}
        >>> ku = KlineUpdate.from_binance_ws(raw)
        >>> assert ku.is_closed is True
        >>> assert ku.data_source == "STREAMING"
    """

    symbol: str
    interval: str
    open_time: datetime  # UTC, start of candle
    open: float
    high: float
    low: float
    close: float
    volume: float
    close_time: datetime  # UTC, end of candle
    is_closed: bool  # k.x flag
    data_source: str = "STREAMING"

    @classmethod
    def from_binance_ws(cls, raw: dict) -> KlineUpdate:
        """Parse a raw Binance WebSocket kline payload.

        Expects the message structure:
            {"e": "kline", "k": {"s": ..., "i": ..., "t": ..., ...}}

        Args:
            raw: Parsed JSON dict from Binance WebSocket.

        Returns:
            KlineUpdate with UTC timestamps and float prices.

        Raises:
            KeyError: If required fields are missing from the payload.
        """
        k = raw["k"]
        return cls(
            symbol=k["s"],
            interval=k["i"],
            open_time=datetime.fromtimestamp(k["t"] / 1000, tz=timezone.utc),
            open=float(k["o"]),
            high=float(k["h"]),
            low=float(k["l"]),
            close=float(k["c"]),
            volume=float(k["v"]),
            close_time=datetime.fromtimestamp(k["T"] / 1000, tz=timezone.utc),
            is_closed=bool(k["x"]),
            data_source="STREAMING",
        )

    @classmethod
    def from_historical_row(cls, row: pd.Series, symbol: str, interval: str) -> KlineUpdate:
        """Convert a historical FCP DataFrame row to KlineUpdate format.

        Used for gap-filling: when the stream reconnects, missed candles are
        fetched via FCP (REST/Cache) and converted to KlineUpdate for uniform
        downstream handling.

        Args:
            row: A pandas Series with open/high/low/close/volume fields.
                 Index is expected to be the open_time (UTC datetime).
            symbol: Trading pair symbol.
            interval: Candle interval string.

        Returns:
            KlineUpdate with is_closed=True (historical rows are always finalized)
            and data_source matching row's _data_source if present, else "REST".
        """
        data_source = str(row.get("_data_source", "REST")) if hasattr(row, "get") else "REST"
        open_time: datetime = row.name if isinstance(row.name, datetime) else datetime.fromtimestamp(row.name / 1000, tz=timezone.utc)
        if open_time.tzinfo is None:
            open_time = open_time.replace(tzinfo=timezone.utc)

        return cls(
            symbol=symbol,
            interval=interval,
            open_time=open_time,
            open=float(row["open"]),
            high=float(row["high"]),
            low=float(row["low"]),
            close=float(row["close"]),
            volume=float(row["volume"]),
            close_time=open_time,  # historical rows don't carry explicit close_time
            is_closed=True,  # historical rows are always finalized
            data_source=data_source,
        )
