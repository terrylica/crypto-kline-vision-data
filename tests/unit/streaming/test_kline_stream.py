# ADR: docs/adr/2025-01-30-failover-control-protocol.md
"""Unit tests for KlineStream backpressure + filtering (T17).

Covers:
- Context manager: __aenter__ calls client.connect(), __aexit__ calls client.close()
- Context manager closes on exception
- subscribe() / unsubscribe() delegate to client
- confirmed_only=True: filters out is_closed=False updates
- confirmed_only=False: passes all updates through (mid-candle too)
- Drop-newest backpressure: when queue full, dropped_count increments
- Queue not modified for dropped messages (old data preserved)
- Gap tracking: last_confirmed_open_time updates on closed candles only
- Gap tracking: open candles do NOT update last_confirmed_open_time
- Gap tracking: out-of-order older timestamp does NOT update (monotonic)
- last_confirmed_open_time returns None before any closed candle
- dropped_count starts at 0
- queue_size reflects items waiting
- Multiple symbols tracked independently
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import datetime, timezone


from ckvd.core.streaming.kline_stream import KlineStream
from ckvd.core.streaming.kline_update import KlineUpdate
from ckvd.core.streaming.stream_config import StreamConfig
from ckvd.utils.market_constraints import MarketType


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_update(
    *,
    symbol: str = "BTCUSDT",
    interval: str = "1h",
    is_closed: bool = True,
    open_time_ms: int = 1_700_000_000_000,
    close: float = 36800.0,
) -> KlineUpdate:
    """Build a KlineUpdate for testing."""
    raw = {
        "e": "kline",
        "k": {
            "t": open_time_ms,
            "T": open_time_ms + 3_599_999,
            "s": symbol,
            "i": interval,
            "o": "36500.00",
            "c": str(close),
            "h": "37000.00",
            "l": "36400.00",
            "v": "1500.00",
            "x": is_closed,
        },
    }
    return KlineUpdate.from_binance_ws(raw)


async def _async_gen_from_list(updates: list[KlineUpdate]) -> AsyncIterator[KlineUpdate]:
    """Yield each update in a list as an async generator."""
    for update in updates:
        yield update


class _MockStreamClient:
    """Minimal mock implementation of the StreamClient Protocol."""

    def __init__(self, messages_to_yield: list[KlineUpdate] | None = None) -> None:
        self._messages = messages_to_yield or []
        self.connected = False
        self.closed = False
        self.subscribed_streams: list[tuple[str, str]] = []
        self.unsubscribed_streams: list[tuple[str, str]] = []

    async def connect(self) -> None:
        self.connected = True

    async def subscribe(self, symbol: str, interval: str) -> None:
        self.subscribed_streams.append((symbol, interval))

    async def unsubscribe(self, symbol: str, interval: str) -> None:
        self.unsubscribed_streams.append((symbol, interval))

    def messages(self) -> AsyncIterator[KlineUpdate]:
        return _async_gen_from_list(self._messages)

    async def close(self) -> None:
        self.closed = True

    @property
    def is_connected(self) -> bool:
        return self.connected


def _make_stream(
    messages: list[KlineUpdate] | None = None,
    *,
    confirmed_only: bool = True,
    queue_maxsize: int = 1000,
    market_type: MarketType = MarketType.FUTURES_USDT,
) -> tuple[KlineStream, _MockStreamClient]:
    """Create a KlineStream + mock client pair for testing."""
    config = StreamConfig(
        market_type=market_type,
        confirmed_only=confirmed_only,
        queue_maxsize=queue_maxsize,
    )
    client = _MockStreamClient(messages)
    stream = KlineStream(config, client)
    return stream, client


# ---------------------------------------------------------------------------
# Context Manager Tests
# ---------------------------------------------------------------------------

class TestKlineStreamContextManager:
    """KlineStream async context manager protocol."""

    async def test_aenter_calls_client_connect(self):
        stream, client = _make_stream()
        async with stream:
            assert client.connected is True

    async def test_aexit_calls_client_close(self):
        stream, client = _make_stream()
        async with stream:
            pass
        assert client.closed is True

    async def test_aexit_calls_close_on_exception(self):
        stream, client = _make_stream()
        try:
            async with stream:
                raise RuntimeError("test error")
        except RuntimeError:
            pass
        assert client.closed is True

    async def test_returns_self_from_aenter(self):
        stream, _ = _make_stream()
        async with stream as s:
            assert s is stream


# ---------------------------------------------------------------------------
# Subscribe / Unsubscribe Tests
# ---------------------------------------------------------------------------

class TestKlineStreamSubscribe:
    """subscribe() and unsubscribe() delegate to client."""

    async def test_subscribe_delegates_to_client(self):
        stream, client = _make_stream()
        async with stream:
            await stream.subscribe("BTCUSDT", "1h")
        assert ("BTCUSDT", "1h") in client.subscribed_streams

    async def test_unsubscribe_delegates_to_client(self):
        stream, client = _make_stream()
        async with stream:
            await stream.subscribe("BTCUSDT", "1h")
            await stream.unsubscribe("BTCUSDT", "1h")
        assert ("BTCUSDT", "1h") in client.unsubscribed_streams

    async def test_subscribe_multiple_symbols(self):
        stream, client = _make_stream()
        async with stream:
            await stream.subscribe("BTCUSDT", "1h")
            await stream.subscribe("ETHUSDT", "1m")
        assert ("BTCUSDT", "1h") in client.subscribed_streams
        assert ("ETHUSDT", "1m") in client.subscribed_streams


# ---------------------------------------------------------------------------
# Filtering Tests (confirmed_only gate)
# ---------------------------------------------------------------------------

class TestKlineStreamConfirmedOnlyFiltering:
    """confirmed_only=True filters mid-candle updates; False passes all."""

    async def test_confirmed_only_true_drops_open_candles(self):
        closed = _make_update(is_closed=True)
        open_candle = _make_update(is_closed=False, open_time_ms=1_700_003_600_000)
        stream, _ = _make_stream(
            [open_candle, open_candle, closed],
            confirmed_only=True,
        )
        received = []
        async for update in stream:
            received.append(update)
        assert len(received) == 1
        assert received[0].is_closed is True

    async def test_confirmed_only_true_empty_when_all_open(self):
        updates = [_make_update(is_closed=False) for _ in range(5)]
        stream, _ = _make_stream(updates, confirmed_only=True)
        received = [u async for u in stream]
        assert received == []

    async def test_confirmed_only_false_passes_open_candles(self):
        open_candle = _make_update(is_closed=False)
        closed = _make_update(is_closed=True)
        stream, _ = _make_stream([open_candle, closed], confirmed_only=False)
        received = [u async for u in stream]
        assert len(received) == 2

    async def test_confirmed_only_false_passes_all_types(self):
        updates = [_make_update(is_closed=(i % 2 == 0)) for i in range(6)]
        stream, _ = _make_stream(updates, confirmed_only=False)
        received = [u async for u in stream]
        assert len(received) == 6

    async def test_confirmed_only_true_passes_all_closed(self):
        updates = [_make_update(is_closed=True, open_time_ms=1_700_000_000_000 + i * 3_600_000) for i in range(4)]
        stream, _ = _make_stream(updates, confirmed_only=True)
        received = [u async for u in stream]
        assert len(received) == 4


# ---------------------------------------------------------------------------
# Backpressure Tests (drop-newest)
# ---------------------------------------------------------------------------

class TestKlineStreamBackpressure:
    """Drop-newest backpressure when queue is full."""

    async def test_dropped_count_starts_at_zero(self):
        stream, _ = _make_stream()
        assert stream.dropped_count == 0

    async def test_no_drops_when_queue_not_full(self):
        updates = [_make_update(is_closed=True, open_time_ms=1_700_000_000_000 + i * 3_600_000) for i in range(3)]
        stream, _ = _make_stream(updates, queue_maxsize=10)
        _ = [u async for u in stream]
        assert stream.dropped_count == 0

    async def test_drops_increment_when_queue_full(self):
        """Fill queue with maxsize=2, then send more: extras are dropped."""
        # Strategy: fill queue to capacity (2), then send 3 more messages
        # The consumer won't drain between messages — all 3 extras drop
        # We do this by making a queue that never drains via our own queue logic:
        # The implementation uses put_nowait → QueueFull → drop.
        # Simplest: use maxsize=0 (asyncio.Queue(0) = unbounded), but that's not drop-newest.
        # Instead: patch the queue to always raise QueueFull.

        updates = [_make_update(is_closed=True, open_time_ms=1_700_000_000_000 + i * 3_600_000) for i in range(5)]
        stream, _ = _make_stream(updates, queue_maxsize=2)

        # Force queue to appear full by filling it before iteration
        dummy = _make_update(is_closed=True)
        for _ in range(2):  # fill to maxsize=2
            await stream._queue.put(dummy)

        # Now consume: all 5 messages will hit a full queue → 5 drops
        [u async for u in stream]
        assert stream.dropped_count == 5

    async def test_queue_size_reflects_items(self):
        stream, _ = _make_stream()
        assert stream.queue_size == 0
        dummy = _make_update(is_closed=True)
        await stream._queue.put(dummy)
        assert stream.queue_size == 1
        stream._queue.get_nowait()
        assert stream.queue_size == 0


# ---------------------------------------------------------------------------
# Gap Tracking Tests
# ---------------------------------------------------------------------------

class TestKlineStreamGapTracking:
    """last_confirmed_open_time tracks closed candles for gap detection."""

    async def test_returns_none_before_any_closed_candle(self):
        stream, _ = _make_stream()
        assert stream.last_confirmed_open_time("BTCUSDT", "1h") is None

    async def test_updates_on_closed_candle(self):
        ms = 1_700_000_000_000
        update = _make_update(is_closed=True, open_time_ms=ms)
        stream, _ = _make_stream([update], confirmed_only=True)
        _ = [u async for u in stream]
        result = stream.last_confirmed_open_time("BTCUSDT", "1h")
        expected = datetime.fromtimestamp(ms / 1000, tz=timezone.utc)
        assert result == expected

    async def test_does_not_update_on_open_candle(self):
        open_candle = _make_update(is_closed=False)
        stream, _ = _make_stream([open_candle], confirmed_only=False)
        _ = [u async for u in stream]
        # Open candle: tracking should NOT update
        assert stream.last_confirmed_open_time("BTCUSDT", "1h") is None

    async def test_advances_monotonically(self):
        """Later timestamps replace earlier ones."""
        ms1 = 1_700_000_000_000
        ms2 = ms1 + 3_600_000  # 1h later
        u1 = _make_update(is_closed=True, open_time_ms=ms1)
        u2 = _make_update(is_closed=True, open_time_ms=ms2)
        stream, _ = _make_stream([u1, u2], confirmed_only=True)
        _ = [u async for u in stream]
        result = stream.last_confirmed_open_time("BTCUSDT", "1h")
        expected = datetime.fromtimestamp(ms2 / 1000, tz=timezone.utc)
        assert result == expected

    async def test_does_not_regress_on_older_timestamp(self):
        """An older closed candle arriving late doesn't replace a newer one."""
        ms_old = 1_700_000_000_000
        ms_new = ms_old + 7_200_000  # 2h later
        u_new = _make_update(is_closed=True, open_time_ms=ms_new)
        u_old = _make_update(is_closed=True, open_time_ms=ms_old)
        # Process new first, then old
        stream, _ = _make_stream([u_new, u_old], confirmed_only=True)
        _ = [u async for u in stream]
        result = stream.last_confirmed_open_time("BTCUSDT", "1h")
        expected = datetime.fromtimestamp(ms_new / 1000, tz=timezone.utc)
        assert result == expected

    async def test_tracks_multiple_symbol_interval_pairs_independently(self):
        """Each (symbol, interval) pair is tracked separately."""
        btc_ms = 1_700_000_000_000
        eth_ms = 1_700_000_000_000 + 60_000  # 1m offset
        btc = _make_update(symbol="BTCUSDT", interval="1h", is_closed=True, open_time_ms=btc_ms)
        eth = _make_update(symbol="ETHUSDT", interval="1m", is_closed=True, open_time_ms=eth_ms)
        stream, _ = _make_stream([btc, eth], confirmed_only=True)
        _ = [u async for u in stream]

        btc_time = stream.last_confirmed_open_time("BTCUSDT", "1h")
        eth_time = stream.last_confirmed_open_time("ETHUSDT", "1m")
        xrp_time = stream.last_confirmed_open_time("XRPUSDT", "1h")  # never seen

        assert btc_time == datetime.fromtimestamp(btc_ms / 1000, tz=timezone.utc)
        assert eth_time == datetime.fromtimestamp(eth_ms / 1000, tz=timezone.utc)
        assert xrp_time is None

    async def test_gap_tracking_updates_even_with_confirmed_only_false(self):
        """Closed candles update tracking regardless of confirmed_only setting."""
        ms = 1_700_000_000_000
        closed = _make_update(is_closed=True, open_time_ms=ms)
        stream, _ = _make_stream([closed], confirmed_only=False)
        _ = [u async for u in stream]
        result = stream.last_confirmed_open_time("BTCUSDT", "1h")
        assert result == datetime.fromtimestamp(ms / 1000, tz=timezone.utc)


# ---------------------------------------------------------------------------
# Observability Properties
# ---------------------------------------------------------------------------

class TestKlineStreamObservability:
    """dropped_count and queue_size properties."""

    def test_dropped_count_property_initial(self):
        stream, _ = _make_stream()
        assert stream.dropped_count == 0

    def test_queue_size_property_initial(self):
        stream, _ = _make_stream()
        assert stream.queue_size == 0

    async def test_empty_stream_yields_nothing(self):
        stream, _ = _make_stream([], confirmed_only=True)
        received = [u async for u in stream]
        assert received == []
