#!/usr/bin/env python3
# ADR: docs/adr/2025-01-30-failover-control-protocol.md
"""StreamConfig: configuration dataclass for WebSocket kline streaming.

Follows the same attrs frozen dataclass pattern as CKVDConfig in
ckvd.core.sync.ckvd_types.
"""

from __future__ import annotations

import attr

from ckvd.utils.market_constraints import DataProvider, MarketType


@attr.define(slots=True, frozen=True)
class StreamConfig:
    """Configuration for WebSocket kline streaming.

    All fields are immutable after construction (frozen=True).

    Attributes:
        market_type: Market type (SPOT, FUTURES_USDT, FUTURES_COIN). Required.
        provider: Data provider. Default BINANCE.
        max_reconnect_attempts: Max reconnect retries before raising
            StreamReconnectExhaustedError. Default 5.
        reconnect_delay_base: Base delay in seconds for exponential backoff.
            Default 1.0s.
        reconnect_delay_max: Maximum backoff ceiling in seconds. Default 60.0s.
        ping_interval: WebSocket keepalive ping interval in seconds. Default 20.0.
        ping_timeout: Seconds to wait for pong response. Default 10.0.
        queue_maxsize: asyncio.Queue size. When full, newest messages are dropped
            (drop-newest backpressure). Default 1000.
        confirmed_only: When True (default), only emit klines where k.x=True
            (candle is finalized/closed). Filters ~1800 intermediate updates per
            1h candle.
        compression: WebSocket compression. MUST remain None — Binance does not
            support per-message deflate. Setting this non-None wastes CPU.
        log_level: Logging verbosity. Default 'ERROR'.

    Example:
        >>> from ckvd.utils.market_constraints import MarketType
        >>> config = StreamConfig(market_type=MarketType.FUTURES_USDT)
        >>> assert config.compression is None  # always
        >>> assert config.confirmed_only is True  # default
    """

    # Required
    market_type: MarketType = attr.field(validator=attr.validators.instance_of(MarketType))

    # Optional with sensible defaults
    provider: DataProvider = attr.field(
        default=DataProvider.BINANCE,
        validator=attr.validators.instance_of(DataProvider),
    )
    max_reconnect_attempts: int = attr.field(
        default=5,
        validator=[attr.validators.instance_of(int), lambda _, __, v: v >= 0],
    )
    reconnect_delay_base: float = attr.field(
        default=1.0,
        validator=[attr.validators.instance_of((int, float)), lambda _, __, v: v > 0],
    )
    reconnect_delay_max: float = attr.field(
        default=60.0,
        validator=[attr.validators.instance_of((int, float)), lambda _, __, v: v > 0],
    )
    ping_interval: float = attr.field(
        default=20.0,
        validator=[attr.validators.instance_of((int, float)), lambda _, __, v: v > 0],
    )
    ping_timeout: float = attr.field(
        default=10.0,
        validator=[attr.validators.instance_of((int, float)), lambda _, __, v: v > 0],
    )
    queue_maxsize: int = attr.field(
        default=1000,
        validator=[attr.validators.instance_of(int), lambda _, __, v: v > 0],
    )
    confirmed_only: bool = attr.field(
        default=True,
        validator=attr.validators.instance_of(bool),
    )
    # CRITICAL: Binance does not support per-message deflate compression.
    # This field is typed as None to make it impossible to set accidentally.
    compression: None = attr.field(
        default=None,
        validator=attr.validators.in_([None]),
    )
    log_level: str = attr.field(
        default="ERROR",
        validator=[
            attr.validators.instance_of(str),
            attr.validators.in_(["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]),
        ],
        converter=str.upper,
    )
