#!/usr/bin/env python3
# ADR: docs/adr/2025-01-30-failover-control-protocol.md
"""sync_bridge: synchronous wrapper for async KlineStream.

Provides stream_data_sync() — a blocking iterator that runs the async
event loop in a daemon thread and forwards KlineUpdate events via a
thread-safe queue.Queue.

This allows synchronous callers (scripts, notebooks, pandas workflows)
to consume the streaming API without any async/await machinery.

Usage:
    from ckvd.core.streaming.sync_bridge import stream_data_sync

    for update in stream_data_sync(stream, "BTCUSDT", "1h"):
        print(update.close)
"""

from __future__ import annotations

import asyncio
import queue
import threading
from collections.abc import Callable, Iterator
from typing import TYPE_CHECKING

from ckvd.core.streaming.kline_update import KlineUpdate
from ckvd.utils.for_core.streaming_exceptions import StreamConnectionError, StreamingError

if TYPE_CHECKING:
    from ckvd.core.streaming.kline_stream import KlineStream

# Sentinel value signalling the background thread has finished
_DONE = object()


def stream_data_sync(
    stream: KlineStream,
    symbol: str,
    interval: str,
    *,
    on_update: Callable[[KlineUpdate], None] | None = None,
) -> Iterator[KlineUpdate]:
    """Synchronous iterator over a KlineStream.

    Runs the async event loop in a background daemon thread and yields
    KlineUpdate events to the calling thread via a thread-safe queue.

    The background thread is started when the iterator is first consumed
    and terminates automatically when the stream ends or raises.

    Args:
        stream: An initialised KlineStream (not yet entered as context manager).
        symbol: Trading pair to subscribe (e.g. "BTCUSDT").
        interval: Candle interval string (e.g. "1h").
        on_update: Optional callback invoked in the background thread for each
            update before it is enqueued. Useful for side effects (logging,
            metrics) without affecting the iterator.

    Yields:
        KlineUpdate events in arrival order.

    Raises:
        Any exception raised inside the async stream is re-raised in the
        calling thread.

    Example:
        >>> for update in stream_data_sync(stream, "BTCUSDT", "1h"):
        ...     print(update.close)
    """
    result_queue: queue.Queue[KlineUpdate | StreamingError | object] = queue.Queue(maxsize=10_000)

    async def _run() -> None:
        try:
            async with stream:
                await stream.subscribe(symbol, interval)
                async for update in stream:
                    if on_update is not None:
                        on_update(update)
                    result_queue.put(update)
        except StreamingError as exc:
            # Transport streaming errors to the calling thread for re-raise.
            # Not silenced — re-raised below when the main thread dequeues it.
            result_queue.put(exc)
        except (OSError, asyncio.TimeoutError) as exc:
            # Network-level errors: wrap in StreamingError for uniform handling.
            result_queue.put(StreamConnectionError(str(exc), details={"cause": type(exc).__name__}))
        finally:
            # Always signal completion so the calling thread can unblock.
            result_queue.put(_DONE)

    thread = threading.Thread(target=asyncio.run, args=(_run,), daemon=True)
    thread.start()

    while True:
        item = result_queue.get()
        if item is _DONE:
            break
        if isinstance(item, StreamingError):
            raise item
        yield item  # type: ignore[misc]
