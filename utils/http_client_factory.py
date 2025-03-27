#!/usr/bin/env python
"""HTTP client factory functions for creating standardized client instances.

DEPRECATED: This module is deprecated in favor of utils.network_utils.
It will be removed in a future version.

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

import warnings
from typing import Dict, Any, Optional, Union, Literal
import aiohttp
import httpx
from utils.config import (
    DEFAULT_USER_AGENT,
    DEFAULT_ACCEPT_HEADER,
    DEFAULT_HTTP_TIMEOUT_SECONDS,
)
from utils.network_utils import (
    create_client as _create_client,
    create_aiohttp_client as _create_aiohttp_client,
    create_httpx_client as _create_httpx_client,
)

# Deprecation warning message template
DEPRECATION_WARNING = (
    "{} is deprecated and will be removed in a future version. "
    "Use utils.network_utils.{} instead."
)


def create_client(
    client_type: Literal["aiohttp", "httpx"] = "aiohttp",
    timeout: float = DEFAULT_HTTP_TIMEOUT_SECONDS,
    max_connections: Optional[int] = None,
    headers: Optional[Dict[str, str]] = None,
    **kwargs: Any,
) -> Union[aiohttp.ClientSession, httpx.AsyncClient]:
    """Create a standardized HTTP client of the specified type.

    DEPRECATED: Use utils.network_utils.create_client instead.

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
    warnings.warn(
        DEPRECATION_WARNING.format("create_client", "create_client"),
        DeprecationWarning,
        stacklevel=2,
    )
    return _create_client(
        client_type=client_type,
        timeout=timeout,
        max_connections=max_connections,
        headers=headers,
        **kwargs,
    )


def create_aiohttp_client(
    timeout: float = DEFAULT_HTTP_TIMEOUT_SECONDS,
    max_connections: int = 20,
    headers: Optional[Dict[str, str]] = None,
    **kwargs: Any,
) -> aiohttp.ClientSession:
    """Factory function to create a pre-configured aiohttp ClientSession.

    DEPRECATED: Use utils.network_utils.create_aiohttp_client instead.

    Args:
        timeout: Total timeout in seconds
        max_connections: Maximum number of connections
        headers: Optional custom headers
        **kwargs: Additional client configuration options

    Returns:
        Configured aiohttp ClientSession with standardized settings
    """
    warnings.warn(
        DEPRECATION_WARNING.format("create_aiohttp_client", "create_aiohttp_client"),
        DeprecationWarning,
        stacklevel=2,
    )
    return _create_aiohttp_client(
        timeout=timeout,
        max_connections=max_connections,
        headers=headers,
        **kwargs,
    )


def create_httpx_client(
    timeout: float = DEFAULT_HTTP_TIMEOUT_SECONDS,
    max_connections: int = 13,
    headers: Optional[Dict[str, str]] = None,
    **kwargs: Any,
) -> httpx.AsyncClient:
    """Factory function to create a pre-configured httpx AsyncClient.

    DEPRECATED: Use utils.network_utils.create_httpx_client instead.

    Args:
        timeout: Total timeout in seconds
        max_connections: Maximum number of connections
        headers: Optional custom headers
        **kwargs: Additional client configuration options

    Returns:
        Configured httpx AsyncClient with standardized settings
    """
    warnings.warn(
        DEPRECATION_WARNING.format("create_httpx_client", "create_httpx_client"),
        DeprecationWarning,
        stacklevel=2,
    )
    return _create_httpx_client(
        timeout=timeout,
        max_connections=max_connections,
        headers=headers,
        **kwargs,
    )
