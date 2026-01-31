#!/usr/bin/env python
"""HTTP client factory functions.

# ADR: docs/adr/2026-01-30-claude-code-infrastructure.md
# Refactoring: Extract from network_utils.py for modularity
"""

from __future__ import annotations

import platform
from typing import Any

import httpx
from httpx import Limits, Timeout

from data_source_manager.utils.config import DEFAULT_HTTP_TIMEOUT_SECONDS
from data_source_manager.utils.loguru_setup import logger

__all__ = [
    "Client",
    "create_client",
    "create_httpx_client",
    "create_legacy_client",
    "safely_close_client",
]

# Define a generic Client type for HTTP clients
Client = httpx.Client


def create_httpx_client(
    timeout: float = DEFAULT_HTTP_TIMEOUT_SECONDS,
    max_connections: int = 50,
    headers: dict[str, str] | None = None,
    **kwargs: Any,
) -> Any:
    """Create an httpx Client for high-performance HTTP requests.

    Args:
        timeout: Request timeout in seconds
        max_connections: Maximum number of connections
        headers: Optional headers to include in all requests
        **kwargs: Additional keyword arguments to pass to Client

    Returns:
        httpx.Client: An initialized HTTP client
    """
    try:
        # Log the kwargs being passed to identify issues
        logger.debug(f"Creating httpx Client with kwargs: {kwargs}")

        # Remove known incompatible parameters
        if "impersonate" in kwargs:
            logger.warning("Removing unsupported 'impersonate' parameter from httpx client creation")
            kwargs.pop("impersonate")

        # Set up timeout with all required parameters defined
        # The error was "httpx.Timeout must either include a default, or set all four parameters explicitly"
        timeout_obj = Timeout(connect=min(timeout, 10.0), read=timeout, write=timeout, pool=timeout)

        # Set up connection limits
        limits = Limits(
            max_connections=max_connections,
            max_keepalive_connections=max_connections // 2,
        )

        # Create default headers if none provided
        if headers is None:
            headers = {
                "User-Agent": f"BinanceDataServices/0.1 Python/{platform.python_version()}",
                "Accept": "application/json",
            }

        # Create the client
        client = httpx.Client(
            timeout=timeout_obj,
            limits=limits,
            headers=headers,
            follow_redirects=True,
            **kwargs,
        )

        logger.debug(f"Created httpx Client with timeout={timeout}s, max_connections={max_connections}")
        return client

    except ImportError:
        logger.error("httpx is not installed. To use this function, install httpx: pip install httpx")
        raise


def create_client(
    timeout: float = DEFAULT_HTTP_TIMEOUT_SECONDS,
    max_connections: int | None = None,
    headers: dict[str, str] | None = None,
    **kwargs: Any,
) -> Any:
    """Create a client for making HTTP requests.

    This function provides a unified interface for creating HTTP clients
    using httpx, which provides better stability and compatibility.

    Args:
        timeout: Request timeout in seconds
        max_connections: Maximum number of connections
        headers: Optional headers to include in all requests
        **kwargs: Additional keyword arguments to pass to the client

    Returns:
        An initialized async HTTP client
    """
    if max_connections is None:
        max_connections = 50  # Default to 50 connections

    # Create httpx client
    try:
        logger.debug(f"Creating httpx client with {len(kwargs)} additional parameters")
        return create_httpx_client(timeout, max_connections, headers, **kwargs)
    except ImportError as e:
        logger.error("httpx is not available. Please install httpx: pip install httpx>=0.24.0")
        raise ImportError("httpx is required but not available. Install with: pip install httpx>=0.24.0") from e


def create_legacy_client(
    timeout: float = DEFAULT_HTTP_TIMEOUT_SECONDS,
    max_connections: int = 50,
    headers: dict[str, str] | None = None,
    **kwargs: Any,
) -> Any:
    """Deprecated function that now creates an httpx client instead.

    This function is maintained for backwards compatibility only. All code should
    use create_httpx_client or create_client directly.

    Args:
        timeout: Total timeout in seconds
        max_connections: Maximum number of connections
        headers: Optional custom headers
        **kwargs: Additional client configuration options

    Returns:
        httpx.Client configured with the provided settings
    """
    logger.warning(
        "create_legacy_client is deprecated. Please update your code to use create_httpx_client or create_client directly."
    )

    # Remove legacy-specific parameters
    if "impersonate" in kwargs:
        logger.warning("Parameter 'impersonate' is not supported by httpx and will be ignored")
        kwargs.pop("impersonate")

    return create_httpx_client(timeout=timeout, max_connections=max_connections, headers=headers, **kwargs)


def safely_close_client(client: Any) -> None:
    """Safely close an HTTP client, handling any exceptions.

    Args:
        client: HTTP client to close
    """
    if client is None:
        return

    try:
        if hasattr(client, "close") and callable(client.close):
            client.close()
            logger.debug("HTTP client closed successfully")
    except OSError as e:
        logger.warning(f"Error while closing HTTP client: {e}")
