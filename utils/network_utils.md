# Network Utilities

This module provides consolidated network-related utilities for HTTP client management, API requests, and file downloading. It serves as a single source of truth for all network operations within the application.

## Overview

The `network_utils.py` module consolidates functionality previously spread across multiple files:

- HTTP client factory (from `http_client_factory.py`)
- Download handling (from `download_handler.py`)
- API request management
- Vision data downloading

## Key Components

### HTTP Client Creation

The module provides standardized HTTP client creation with consistent configuration:

- `create_client(client_type, timeout, max_connections, headers, **kwargs)` - Unified factory for creating either aiohttp or httpx clients
- `create_aiohttp_client(timeout, max_connections, headers, **kwargs)` - Creates aiohttp.ClientSession
- `create_httpx_client(timeout, max_connections, headers, **kwargs)` - Creates httpx.AsyncClient

### Download Handling

Robust file download functionality with progress tracking, retry logic, and error handling:

- `DownloadProgressTracker` - Monitors download progress and detects stalls
- `DownloadHandler` - Manages downloads with retry logic
- `download_files_concurrently(client, urls, local_paths, max_concurrent)` - Downloads multiple files in parallel

### API Request Management

Standardized API request handling with retry logic and rate limit management:

- `make_api_request(client, url, params, headers, max_retries, retry_delay)` - Makes API requests with automatic retries

### Vision Data Management

Specialized download management for Binance Vision data:

- `VisionDownloadManager` - Handles downloading and processing Vision data files

## Exception Classes

- `DownloadException` - Base class for download-related exceptions
- `DownloadStalledException` - Raised when download progress stalls
- `RateLimitException` - Raised when rate limited by a server

## Usage Examples

### Creating HTTP Clients

```python
from utils.network_utils import create_client

# Create default aiohttp client
client = create_client()

# Create httpx client with custom settings
httpx_client = create_client(
    client_type="httpx",
    timeout=30,
    headers={"X-Custom-Header": "Value"}
)
```

### Downloading Files

```python
from utils.network_utils import DownloadHandler, create_client
from pathlib import Path

async def download_example():
    client = create_client(client_type="httpx")
    handler = DownloadHandler(client)

    success = await handler.download_file(
        url="https://example.com/file.zip",
        local_path=Path("./downloads/file.zip")
    )

    await client.aclose()
    return success
```

### Making API Requests

```python
from utils.network_utils import make_api_request, create_client

async def fetch_data():
    client = create_client(client_type="httpx")

    try:
        data = await make_api_request(
            client=client,
            url="https://api.example.com/data",
            params={"limit": 100},
            max_retries=3
        )
        return data
    finally:
        await client.aclose()
```

### Using VisionDownloadManager

```python
from utils.network_utils import VisionDownloadManager, create_client
from datetime import datetime, timezone

async def download_vision_data():
    client = create_client(client_type="httpx")
    manager = VisionDownloadManager(
        client=client,
        symbol="BTCUSDT",
        interval="1m"
    )

    date = datetime(2023, 1, 1, tzinfo=timezone.utc)
    data = await manager.download_date(date)

    await client.aclose()
    return data
```

## Migration Path

All network-related functionality has been consolidated into this module from:

- `http_client_factory.py` (HTTP client creation)
- `download_handler.py` (Download handling)

The original modules still exist with deprecated wrapper functions for backward compatibility, but new code should use `network_utils.py` directly.
