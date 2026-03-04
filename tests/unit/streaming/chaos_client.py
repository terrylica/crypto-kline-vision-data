# ADR: docs/adr/2025-01-30-failover-control-protocol.md
"""ChaosStreamClient: fault-injection StreamClient for reconciliation testing.

Implements the StreamClient Protocol with configurable fault events:
- kline: yield a normal closed KlineUpdate
- gap: skip N intervals (no messages), then resume
- disconnect: raise StreamConnectionError mid-stream
- corrupt: yield KlineUpdate with NaN/zero/duplicate open_time
- delay: asyncio.sleep(seconds) before next yield
- burst: yield N messages without any delay (backpressure test)
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Literal

from ckvd.core.streaming.kline_update import KlineUpdate
from ckvd.utils.for_core.streaming_exceptions import StreamConnectionError


@dataclass
class ChaosEvent:
    """A single event in a chaos scenario.

    Attributes:
        type: Event type.
        count: Number of intervals to skip/corrupt/burst. Default 1.
        delay_seconds: Seconds to sleep for "delay" type. Default 0.
    """

    type: Literal["kline", "gap", "disconnect", "corrupt", "delay", "burst"]
    count: int = 1
    delay_seconds: float = 0.0


@dataclass
class ChaosScenario:
    """A sequence of ChaosEvents defining a test scenario."""

    events: list[ChaosEvent] = field(default_factory=list)


class ChaosStreamClient:
    """StreamClient that replays a scripted ChaosScenario.

    Generates KlineUpdate objects with incrementing open_time based on
    interval_td, then injects faults at scripted points.

    Args:
        scenario: Sequence of events to replay.
        symbol: Trading pair for generated updates. Default "BTCUSDT".
        interval: Candle interval string. Default "1h".
        interval_td: Timedelta per candle. Default 1 hour.
        base_time: Starting open_time. Default 2024-01-15T00:00:00Z.
        base_price: Starting close price. Default 42000.0.
    """

    def __init__(  # noqa: PLR0913
        self,
        scenario: ChaosScenario,
        *,
        symbol: str = "BTCUSDT",  # SSoT-OK: test fixture default
        interval: str = "1h",  # SSoT-OK: test fixture default
        interval_td: timedelta = timedelta(hours=1),
        base_time: datetime | None = None,
        base_price: float = 42000.0,
    ) -> None:
        self._scenario = scenario
        self._symbol = symbol
        self._interval = interval
        self._interval_td = interval_td
        self._current_time = base_time or datetime(2024, 1, 15, 0, 0, tzinfo=timezone.utc)
        self._base_price = base_price
        self._price_idx = 0
        self._connected = False

    async def connect(self) -> None:
        self._connected = True

    async def subscribe(self, symbol: str, interval: str) -> None:
        pass

    async def unsubscribe(self, symbol: str, interval: str) -> None:
        pass

    async def close(self) -> None:
        self._connected = False

    @property
    def is_connected(self) -> bool:
        return self._connected

    def messages(self) -> AsyncIterator[KlineUpdate]:
        return self._generate()

    async def _generate(self) -> AsyncIterator[KlineUpdate]:
        for event in self._scenario.events:
            match event.type:
                case "kline":
                    for _ in range(event.count):
                        yield self._make_update(is_closed=True)
                        self._advance_time()
                case "gap":
                    self._advance_time(event.count)
                case "disconnect":
                    raise StreamConnectionError(
                        "chaos: connection reset",
                        details={"code": 1006, "chaos": True},
                    )
                case "corrupt":
                    for _ in range(event.count):
                        yield self._make_corrupt_update()
                        self._advance_time()
                case "delay":
                    await asyncio.sleep(event.delay_seconds)
                case "burst":
                    for _ in range(event.count):
                        yield self._make_update(is_closed=True)
                        self._advance_time()

    def _make_update(self, *, is_closed: bool = True) -> KlineUpdate:
        price = self._base_price + self._price_idx * 10
        self._price_idx += 1
        return KlineUpdate(
            symbol=self._symbol,
            interval=self._interval,
            open_time=self._current_time,
            open=price,
            high=price + 100,
            low=price - 50,
            close=price + 50,
            volume=1500.0,
            close_time=self._current_time + self._interval_td - timedelta(milliseconds=1),
            is_closed=is_closed,
        )

    def _make_corrupt_update(self) -> KlineUpdate:
        """Create a corrupt update with NaN close price."""
        return KlineUpdate(
            symbol=self._symbol,
            interval=self._interval,
            open_time=self._current_time,
            open=float("nan"),
            high=float("nan"),
            low=float("nan"),
            close=float("nan"),
            volume=0.0,
            close_time=self._current_time + self._interval_td - timedelta(milliseconds=1),
            is_closed=True,
        )

    def _advance_time(self, intervals: int = 1) -> None:
        self._current_time += self._interval_td * intervals
