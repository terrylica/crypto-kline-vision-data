# ADR: docs/adr/2025-01-30-failover-control-protocol.md
"""Shared fixtures for streaming unit tests."""


import pytest

from ckvd.core.streaming.kline_update import KlineUpdate
from ckvd.utils.market_constraints import MarketType


# ---------------------------------------------------------------------------
# Raw Binance WebSocket payload fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def raw_kline_closed() -> dict:
    """Canonical closed kline payload from Binance WebSocket."""
    return {
        "e": "kline",
        "E": 1_700_000_060_000,
        "s": "BTCUSDT",
        "k": {
            "t": 1_700_000_000_000,   # open_time ms
            "T": 1_700_003_599_999,   # close_time ms
            "s": "BTCUSDT",
            "i": "1h",
            "f": 100,
            "L": 200,
            "o": "36500.00",
            "c": "36800.00",
            "h": "37000.00",
            "l": "36400.00",
            "v": "1500.00",
            "n": 500,
            "x": True,               # is_closed
            "q": "55000000.00",
            "V": "800.00",
            "Q": "29000000.00",
            "B": "0",
        },
    }


@pytest.fixture
def raw_kline_open() -> dict:
    """Open (mid-candle) kline payload from Binance WebSocket."""
    return {
        "e": "kline",
        "E": 1_700_000_030_000,
        "s": "ETHUSDT",
        "k": {
            "t": 1_700_000_000_000,
            "T": 1_700_000_059_999,
            "s": "ETHUSDT",
            "i": "1m",
            "f": 10,
            "L": 50,
            "o": "2000.00",
            "c": "2010.00",
            "h": "2015.00",
            "l": "1995.00",
            "v": "500.00",
            "n": 40,
            "x": False,              # NOT closed
            "q": "1000500.00",
            "V": "250.00",
            "Q": "500250.00",
            "B": "0",
        },
    }


@pytest.fixture
def closed_kline_update(raw_kline_closed) -> KlineUpdate:
    """Parsed KlineUpdate from a closed candle payload."""
    return KlineUpdate.from_binance_ws(raw_kline_closed)


@pytest.fixture
def open_kline_update(raw_kline_open) -> KlineUpdate:
    """Parsed KlineUpdate from an open (mid-candle) payload."""
    return KlineUpdate.from_binance_ws(raw_kline_open)


@pytest.fixture
def stream_config_futures():
    """StreamConfig for FUTURES_USDT market."""
    from ckvd.core.streaming.stream_config import StreamConfig

    return StreamConfig(market_type=MarketType.FUTURES_USDT)


@pytest.fixture
def stream_config_spot():
    """StreamConfig for SPOT market."""
    from ckvd.core.streaming.stream_config import StreamConfig

    return StreamConfig(market_type=MarketType.SPOT)
