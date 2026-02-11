"""API introspection module for AI agent discovery.

Provides deterministic, JSON-serializable output for stateless agent probing:
- ``discover_api()`` — complete API surface with signatures
- ``get_capabilities()`` — capability matrix (providers, markets, intervals, etc.)

GitHub Issue #22. Pattern adopted from gapless-crypto-data cross-repo analysis.

Usage::

    from ckvd.__probe__ import discover_api, get_capabilities
    import json
    print(json.dumps(discover_api(), indent=2))
"""

from __future__ import annotations

import inspect
from typing import Any


def discover_api() -> dict[str, Any]:
    """Return complete API surface map for AI agent discovery.

    Returns a JSON-serializable dict describing all public classes, functions,
    and enums exposed by the ``ckvd`` package.  No file I/O or network calls.

    Returns:
        Dict with keys: ``metadata``, ``classes``, ``functions``, ``enums``,
        ``exceptions``.
    """
    return {
        "metadata": _metadata(),
        "classes": _discover_classes(),
        "functions": _discover_functions(),
        "enums": _discover_enums(),
        "exceptions": _discover_exceptions(),
    }


def get_capabilities() -> dict[str, Any]:
    """Return capability matrix for AI agent decision-making.

    Returns:
        Dict describing providers, market types, intervals, data sources,
        output formats, and FCP behaviour.
    """
    from .utils.market.enums import DataProvider, Interval, MarketType

    return {
        "providers": [m.name for m in DataProvider],
        "market_types": [m.name for m in MarketType],
        "intervals": [i.value for i in Interval],
        "data_sources": ["CACHE", "VISION", "REST"],
        "output_formats": {
            "default": "pandas.DataFrame",
            "opt_in": "polars.DataFrame (return_polars=True)",
        },
        "fcp": {
            "priority": ["CACHE", "VISION", "REST"],
            "cache_backend": "Apache Arrow IPC (.arrow)",
            "vision_delay": "~48h from market close",
            "rest_rate_limits": {
                "spot": "6,000 weight/min",
                "futures_usdt": "2,400 weight/min",
                "futures_coin": "2,400 weight/min",
            },
        },
        "exception_details": (
            "All exceptions carry a .details dict (dict[str, Any], default {}) "
            "for machine-parseable error context."
        ),
    }


# ---------------------------------------------------------------------------
# Internal helpers — all stateless, no I/O
# ---------------------------------------------------------------------------


def _metadata() -> dict[str, Any]:
    """Package metadata."""
    import ckvd

    return {
        "package": "crypto-kline-vision-data",
        "import_name": "ckvd",
        "version": ckvd.__version__,
        "probe_version": "1.0.0",
        "type": "market-data-integration",
    }


def _discover_classes() -> dict[str, Any]:
    """Discover CryptoKlineVisionData and CKVDConfig."""
    from .core.sync.ckvd_types import CKVDConfig
    from .core.sync.crypto_kline_vision_data import CryptoKlineVisionData

    return {
        "CryptoKlineVisionData": _class_info(CryptoKlineVisionData),
        "CKVDConfig": _class_info(CKVDConfig),
    }


def _discover_functions() -> dict[str, Any]:
    """Discover public functions (fetch_market_data)."""
    from .core.sync.ckvd_lib import fetch_market_data

    return {
        "fetch_market_data": _func_info(fetch_market_data),
    }


def _discover_enums() -> dict[str, Any]:
    """Discover all public enums with their members."""
    from .core.sync.ckvd_types import DataSource
    from .utils.market.enums import ChartType, DataProvider, Interval, MarketType

    result = {}
    for enum_cls in (DataProvider, MarketType, Interval, ChartType, DataSource):
        result[enum_cls.__name__] = {
            "members": [m.name for m in enum_cls],
            "module": enum_cls.__module__,
        }
    return result


def _discover_exceptions() -> dict[str, Any]:
    """Discover exception classes and their hierarchy."""
    from .utils.for_core.rest_exceptions import (
        APIError,
        HTTPError,
        JSONDecodeError,
        NetworkError,
        RateLimitError,
        RestAPIError,
        RestTimeoutError,
    )
    from .utils.for_core.vision_exceptions import (
        ChecksumVerificationError,
        DataFreshnessError,
        DataNotAvailableError,
        DownloadFailedError,
        UnsupportedIntervalError,
        VisionAPIError,
    )

    exceptions = {}
    for exc_cls in (
        RestAPIError,
        RateLimitError,
        HTTPError,
        APIError,
        NetworkError,
        RestTimeoutError,
        JSONDecodeError,
        VisionAPIError,
        DataFreshnessError,
        ChecksumVerificationError,
        DownloadFailedError,
        DataNotAvailableError,
        UnsupportedIntervalError,
    ):
        bases = [b.__name__ for b in exc_cls.__mro__[1:] if b is not object]
        exceptions[exc_cls.__name__] = {
            "module": exc_cls.__module__,
            "bases": bases,
            "has_details": True,
        }
    return exceptions


def _class_info(cls: type) -> dict[str, Any]:
    """Extract public methods and docstring from a class."""
    methods = {}
    for name in sorted(dir(cls)):
        if name.startswith("_"):
            continue
        obj = getattr(cls, name, None)
        if obj is None or not callable(obj):
            continue
        methods[name] = _func_info(obj)
    return {
        "module": cls.__module__,
        "docstring": (cls.__doc__ or "").strip().split("\n")[0],
        "methods": methods,
    }


def _func_info(func: Any) -> dict[str, Any]:
    """Extract parameter names and first-line docstring from a callable."""
    try:
        sig = inspect.signature(func)
        params = [p.name for p in sig.parameters.values() if p.name != "self"]
    except (ValueError, TypeError):
        params = []
    return {
        "parameters": params,
        "docstring": (func.__doc__ or "").strip().split("\n")[0],
    }


__all__ = [
    "discover_api",
    "get_capabilities",
]
