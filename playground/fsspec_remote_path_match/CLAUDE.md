# Binance Vision Path Mapper with fsspec

**Hub**: [playground/](../CLAUDE.md) | **Root**: [CLAUDE.md](../../CLAUDE.md)

Lightweight solution for mapping between Binance Vision API paths and local cache storage using `fsspec` filesystem abstraction.

## Overview

The Binance Vision Path Mapper solves a critical problem in the codebase: maintaining consistent path handling between remote Binance Vision API URLs and local cache paths. The solution ensures:

1. Simple and direct path mapping with minimal intervention
2. Support for all market types (Spot, UM, CM)
3. Bidirectional mapping between remote and local paths
4. Unified filesystem operations via `fsspec`

## Path Formats

### Remote URL Format

Remote URLs follow the standard Binance Vision API format:

- **Spot Market**:

  ```url
  https://data.binance.vision/data/spot/daily/klines/{SYMBOL}/{INTERVAL}/{SYMBOL}-{INTERVAL}-{DATE}.zip
  ```

- **USDT-Margined Futures (UM)**:

  ```url
  https://data.binance.vision/data/futures/um/daily/klines/{SYMBOL}/{INTERVAL}/{SYMBOL}-{INTERVAL}-{DATE}.zip
  ```

- **Coin-Margined Futures (CM)**:

  ```url
  https://data.binance.vision/data/futures/cm/daily/klines/{SYMBOL}_PERP/{INTERVAL}/{SYMBOL}_PERP-{INTERVAL}-{DATE}.zip
  ```

### Local Cache Path Format

Local cache paths mirror the remote structure but with a different base directory and file extension:

```tree
cache/data/{market_path}/daily/klines/{SYMBOL}/{INTERVAL}/{SYMBOL}-{INTERVAL}-{DATE}.arrow
```

Where:

- `{market_path}` is one of: `spot`, `futures/um`, or `futures/cm`
- `.arrow` is used instead of `.zip` for the cached data

This direct mirroring approach simplifies path handling and makes the mapping logic more maintainable.

## Key Components

### PathComponents

A dataclass that holds the essential components of a data path:

- `exchange`: The exchange name (e.g., "binance")
- `market_type`: Market type (SPOT, FUTURES_USDT, FUTURES_COIN)
- `chart_type`: Chart type (KLINES, etc.)
- `symbol`: Trading symbol (e.g., "BTCUSDT")
- `interval`: Time interval (e.g., "1m")
- `date`: Pendulum DateTime object
- `file_extension`: File extension (default: ".arrow")

### VisionPathMapper

The core class responsible for path mapping:

- `get_remote_url()`: Generates a remote URL from path components
- `get_local_path()`: Generates a local cache path from path components
- `map_remote_to_local()`: Converts a remote URL to a local cache path
- `map_local_to_remote()`: Converts a local cache path to a remote URL
- `create_components_from_params()`: Creates path components from parameters

### FSSpecVisionHandler

A higher-level handler that uses fsspec for file operations:

- `get_fs_and_path()`: Gets the appropriate filesystem and path using fsspec
- `exists()`: Checks if a file exists (works for both remote and local)
- `download_to_cache()`: Downloads a file from remote to local cache
- `find_all_available_dates()`: Finds all available dates in a range

## Usage Examples

### Basic Path Mapping

```python
from vision_path_mapper import VisionPathMapper
from ckvd.ckvd.utils.market_constraints import MarketType
import pendulum

# Create a mapper instance
mapper = VisionPathMapper(base_cache_dir="cache")

# Create path components
components = mapper.create_components_from_params(
    symbol="BTCUSDT",
    interval="1m",
    date="2025-04-16",
    market_type=MarketType.SPOT
)

# Get remote URL and local path
remote_url = mapper.get_remote_url(components)
local_path = mapper.get_local_path(components)

print(f"Remote URL: {remote_url}")
print(f"Local Path: {local_path}")
```

### Coin-Margined Futures Example

```python
# Create path components for Coin-Margined Futures
cm_components = mapper.create_components_from_params(
    symbol="BTCUSD_PERP",  # Note: CM symbols should include _PERP suffix
    interval="1m",
    date="2025-04-16",
    market_type=MarketType.FUTURES_COIN
)

# Get remote URL and local path
cm_remote_url = mapper.get_remote_url(cm_components)
cm_local_path = mapper.get_local_path(cm_components)

print(f"CM Remote URL: {cm_remote_url}")
print(f"CM Local Path: {cm_local_path}")

# The safe_symbol property ensures correct symbol format
# If you provide "BTCUSD" without the _PERP suffix, it will be added automatically
# for CM markets, but best practice is to include it explicitly
```

### Direct URL to Path Conversion

```python
# Convert a remote URL to a local path
remote_url = "https://data.binance.vision/data/spot/daily/klines/BTCUSDT/1m/BTCUSDT-1m-2025-04-16.zip"
local_path = mapper.map_remote_to_local(remote_url)
print(f"Local Path: {local_path}")

# Convert a local path to a remote URL
remote_url = mapper.map_local_to_remote(local_path)
print(f"Remote URL: {remote_url}")
```

### Filesystem Operations with FSSpec

```python
from vision_path_mapper import FSSpecVisionHandler
from ckvd.ckvd.utils.market_constraints import MarketType

# Create a handler instance
handler = FSSpecVisionHandler(base_cache_dir="cache")

# Check if a file exists
exists = handler.exists("https://data.binance.vision/data/spot/daily/klines/BTCUSDT/1m/BTCUSDT-1m-2025-04-16.zip")
print(f"File exists: {exists}")

# Download a file to cache
local_path = handler.download_to_cache(
    symbol="BTCUSDT",
    interval="1m",
    date="2025-04-16",
    market_type=MarketType.SPOT
)

# Find all available dates
dates = handler.find_all_available_dates(
    symbol="BTCUSDT",
    interval="1m",
    market_type=MarketType.SPOT,
    start_date="2025-04-01",
    end_date="2025-04-30"
)
```

## Testing All Market Types

The `test_all_market_types.py` script demonstrates path mapping for all market types:

```bash
# Run the test
python test_all_market_types.py
```

This will output tables showing the consistent path mapping across all market types.

## Integration with CryptoKlineVisionData

The `ckvd_integration_example.py` script demonstrates how to integrate the path mapper with CryptoKlineVisionData:

```bash
# Run the example with a specific market type
python ckvd_integration_example.py --market-type spot

# Run the example with USDT-margined futures
python ckvd_integration_example.py --market-type um

# Run the example with Coin-margined futures
python ckvd_integration_example.py --market-type cm

# Run the example with all market types
python ckvd_integration_example.py --all-markets
```

CryptoKlineVisionData uses the path mapper to:

1. Generate consistent paths for each market type
2. Handle local cache and remote API endpoints
3. Maintain proper symbol conventions for each market
4. Provide unified access to data regardless of source

## Key Benefits

- **Simplicity**: Direct mapping between remote and local paths
- **Consistency**: Same path structure across all market types
- **Flexibility**: Works with any symbol and interval
- **Efficiency**: Uses fsspec for unified file operations
- **Reliability**: Correctly handles special cases like CM markets requiring \_PERP suffix
