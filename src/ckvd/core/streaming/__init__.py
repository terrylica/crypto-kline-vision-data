#!/usr/bin/env python3
# ADR: docs/adr/2025-01-30-failover-control-protocol.md
"""WebSocket streaming module for real-time kline data.

Provides async and sync iterators for Binance WebSocket kline streams
alongside the existing FCP (Cache → Vision → REST) pipeline.

Public API:
    KlineUpdate      — frozen dataclass for a single kline event
    KlineStream      — async context manager / async iterator
    StreamConfig     — configuration attrs dataclass
    stream_data_sync — synchronous bridge for blocking callers

Architecture:
    BinanceStreamClient (provider) → ConnectionMachine (FSM)
    → KlineStream (queue + filtering) → consumer
"""

__all__: list[str] = []
