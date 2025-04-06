# Migration Guide: curl_cffi to httpx

## Overview

This guide explains how to migrate from `curl_cffi` to `httpx` to resolve hanging issues in the Binance Data Services library, particularly in Python 3.13.

## Background

The `curl_cffi` library can cause hanging issues during cleanup due to:

1. Circular references with the `_curlm` object
2. Lingering `_force_timeout` tasks that keep the event loop active
3. Python 3.13's stricter resource cleanup requirements

Switching to `httpx` provides a more reliable alternative that:

- Has better resource management
- More predictable cleanup behavior
- No issues with circular references

## How to Use httpx Instead of curl_cffi

### Option 1: Use the Built-in Parameter

The simplest way to switch is to use the `use_httpx` parameter when creating a `DataSourceManager`:

```python
from core.data_source_manager import DataSourceManager
from utils.market_constraints import MarketType

# Create a DataSourceManager with httpx
manager = DataSourceManager(
    market_type=MarketType.SPOT,
    use_httpx=True  # Use httpx instead of curl_cffi
)

# Use the manager as usual
async with manager:
    # Your data retrieval code here
    ...
```

This parameter is also available on the `RestDataClient` class:

```python
from core.rest_data_client import RestDataClient
from utils.market_constraints import MarketType

# Create a RestDataClient with httpx
client = RestDataClient(
    market_type=MarketType.SPOT,
    use_httpx=True  # Use httpx instead of curl_cffi
)

# Use the client as usual
async with client:
    # Your data retrieval code here
    ...
```

### Option 2: Create a Custom Client

For more control, you can create a custom `httpx` client:

```python
from utils.network_utils import create_httpx_client
from core.rest_data_client import RestDataClient
from utils.market_constraints import MarketType

# Create a custom httpx client
custom_client = create_httpx_client(
    timeout=10.0,
    max_connections=50,
    # Additional options...
)

# Use it with RestDataClient
client = RestDataClient(
    market_type=MarketType.SPOT,
    client=custom_client
)

# The client will use your custom httpx client
async with client:
    # Your data retrieval code here
    ...
```

## Testing Your Migration

We've provided a test script to verify your migration:

```bash
python scripts/test_httpx_client.py
```

This script:

1. Tests REST API data retrieval using httpx
2. Tests Vision API data retrieval using httpx
3. Verifies proper cleanup and resource management

## Installation Requirements

To use httpx, make sure it's installed:

```bash
pip install httpx
```

For HTTP/2 support (recommended for optimal performance):

```bash
pip install "httpx[http2]"
```

## Benefits of Using httpx

- **Reliability**: More consistent cleanup behavior, especially in Python 3.13
- **No Hanging**: Eliminates hanging issues caused by `curl_cffi`
- **Better Error Handling**: More detailed error information
- **HTTP/2 Support**: Improved performance with connection multiplexing
- **Broader Compatibility**: Works across more Python versions and platforms
- **Active Development**: Regular updates and improvements

## Note for Advanced Users

The library will automatically fall back to `curl_cffi` if `httpx` is not available, and vice versa. This ensures backward compatibility while providing a path to more reliable behavior.
