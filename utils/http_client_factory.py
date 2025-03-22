#!/usr/bin/env python
"""HTTP client factory functions for creating standardized client instances.

This module centralizes the creation of HTTP clients, ensuring consistent configuration
across different parts of the application. It supports both aiohttp and httpx clients
with standardized headers, timeouts, and connection settings.

The module provides three main functions:
1. create_client() - A unified factory that can create either aiohttp or httpx clients
   with a consistent interface, serving as the recommended entry point
2. create_aiohttp_client() - Creates specifically an aiohttp ClientSession (maintained for
   backward compatibility)
3. create_httpx_client() - Creates specifically a httpx AsyncClient (maintained for
   backward compatibility)

Usage examples:
```python
# Create an aiohttp client (the default)
client = create_client()

# Create a httpx client with custom settings
httpx_client = create_client(
    client_type="httpx",
    timeout=30,
    max_connections=40,
    headers={"X-Custom-Header": "Value"}
)

# Use the specific factory functions (backward compatibility)
aiohttp_client = create_aiohttp_client(timeout=15)
```

This unified approach ensures consistent client configuration and behavior throughout
the application regardless of which HTTP client implementation is used.
"""

from typing import Dict, Any, Optional, Union, Literal
import aiohttp
import httpx
from utils.config import (
    DEFAULT_USER_AGENT,
    DEFAULT_ACCEPT_HEADER,
    DEFAULT_HTTP_TIMEOUT_SECONDS,
)


def create_client(
    client_type: Literal["aiohttp", "httpx"] = "aiohttp",
    timeout: float = DEFAULT_HTTP_TIMEOUT_SECONDS,
    max_connections: Optional[int] = None,
    headers: Optional[Dict[str, str]] = None,
    **kwargs: Any,
) -> Union[aiohttp.ClientSession, httpx.AsyncClient]:
    """Create a standardized HTTP client of the specified type.

    Provides a unified interface for creating both aiohttp and httpx clients
    with consistent configuration options.

    Args:
        client_type: The type of client to create ("aiohttp" or "httpx")
        timeout: Request timeout in seconds
        max_connections: Maximum number of connections
        headers: Optional custom headers to include in all requests
        **kwargs: Additional client-specific configuration options

    Returns:
        Configured HTTP client of the requested type with standard settings

    Raises:
        ValueError: If an unsupported client_type is specified
    """
    # Use default max connections based on client type if not specified
    if max_connections is None:
        max_connections = 20 if client_type == "aiohttp" else 13

    # Merge default headers with custom headers
    default_headers = {
        "Accept": DEFAULT_ACCEPT_HEADER,
        "User-Agent": DEFAULT_USER_AGENT,
    }

    if headers:
        default_headers.update(headers)

    # Create the appropriate client type
    if client_type == "aiohttp":
        return create_aiohttp_client(
            timeout=timeout,
            max_connections=max_connections,
            headers=default_headers,
            **kwargs,
        )
    elif client_type == "httpx":
        return create_httpx_client(
            timeout=timeout,
            max_connections=max_connections,
            headers=default_headers,
            **kwargs,
        )
    else:
        raise ValueError(f"Unsupported client type: {client_type}")


def create_aiohttp_client(
    timeout: float = DEFAULT_HTTP_TIMEOUT_SECONDS,
    max_connections: int = 20,
    headers: Optional[Dict[str, str]] = None,
    **kwargs: Any,
) -> aiohttp.ClientSession:
    """Factory function to create a pre-configured aiohttp ClientSession.

    Args:
        timeout: Total timeout in seconds
        max_connections: Maximum number of connections
        headers: Optional custom headers
        **kwargs: Additional client configuration options

    Returns:
        Configured aiohttp ClientSession with standardized settings
    """
    client_headers = {
        "Accept": DEFAULT_ACCEPT_HEADER,
        "User-Agent": DEFAULT_USER_AGENT,
    }

    if headers:
        client_headers.update(headers)

    client_timeout = aiohttp.ClientTimeout(
        total=timeout, connect=3, sock_connect=3, sock_read=5
    )
    connector = aiohttp.TCPConnector(limit=max_connections, force_close=False)

    client_kwargs = {
        "timeout": client_timeout,
        "connector": connector,
        "headers": client_headers,
    }

    # Add any additional kwargs
    client_kwargs.update(kwargs)

    return aiohttp.ClientSession(**client_kwargs)


def create_httpx_client(
    timeout: float = DEFAULT_HTTP_TIMEOUT_SECONDS,
    max_connections: int = 13,
    headers: Optional[Dict[str, str]] = None,
    **kwargs: Any,
) -> httpx.AsyncClient:
    """Factory function to create a pre-configured httpx AsyncClient.

    Args:
        timeout: Total timeout in seconds
        max_connections: Maximum number of connections
        headers: Optional custom headers
        **kwargs: Additional client configuration options

    Returns:
        Configured httpx AsyncClient with standardized settings
    """
    limits = httpx.Limits(
        max_connections=max_connections, max_keepalive_connections=max_connections
    )
    timeout_config = httpx.Timeout(timeout)

    client_headers = {
        "Accept": DEFAULT_ACCEPT_HEADER,
        "User-Agent": DEFAULT_USER_AGENT,
    }

    if headers:
        client_headers.update(headers)

    client_kwargs = {
        "limits": limits,
        "timeout": timeout_config,
        "headers": client_headers,
    }

    # Add any additional kwargs
    client_kwargs.update(kwargs)

    return httpx.AsyncClient(**client_kwargs)
