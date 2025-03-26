# Roadmap for Upgrading Cache Structure to Align with Binance Vision API

## 1. Overview

This document outlines the roadmap for upgrading the cache structure used in the Binance Data Services project. The current cache structure is based on symbol and interval, while the proposed new structure is designed to be more aligned with the Binance Vision API URL structure and to accommodate future expansion to multiple exchanges and data types.

**Current Cache Structure:**

```path
cache_dir / symbol / interval / YYYY-MM-DD.arrow
```

**Proposed New Cache Structure:**

```path
cache_dir / {exchange} / {market_type} / {data_nature} / {packaging_frequency} / {SYMBOL} / {INTERVAL} / YYYY-MM-DD.arrow
```

For Binance spot klines with daily granularity, this translates to:

```path
cache_dir / binance / spot / klines / daily / {SYMBOL} / {INTERVAL} / YYYY-MM-DD.arrow
```

**Motivation for Upgrade:**

- **Consistency:** Align the cache structure with the Binance Vision API URL structure for better organization and intuitiveness.
- **Scalability:** Prepare the cache system for future support of multiple exchanges, market types (spot, futures), and data natures (klines, trades, etc.).
- **Maintainability:** Improve the clarity and maintainability of the cache system by adopting a more structured and descriptive directory layout.

## 2. Goals

The primary goals of this cache structure upgrade are:

1. **Implement the new cache path structure** in `CacheKeyManager.get_cache_path` to include parameters for exchange, market type, data nature, and packaging_frequency.
2. **Update all usages of `CacheKeyManager.get_cache_path`** throughout the codebase to use the new parameters.
3. **Ensure backward compatibility** for existing code that relies on the cache.
4. **Provide a migration path** for existing cache files (optional but recommended).
5. **Thoroughly test** the new cache structure to ensure correct functionality and data integrity.

## 3. Detailed Steps

### Step 1: Modify `CacheKeyManager.get_cache_path`

- **Description:** Update the `get_cache_path` static method in `utils/cache_validator.py` to accept new parameters: `exchange`, `market_type`, `data_nature`, and `packaging_frequency`. Set default values for these parameters to maintain backward compatibility (e.g., `exchange="binance"`, `market_type="spot"`, `data_nature="klines"`, `packaging_frequency="daily"`). **Testing should include unit tests to verify correct path generation with various combinations of these parameters and default values.**
- **Code Change Location:** `utils/cache_validator.py`
- **Estimated Effort:** Low
- **Timeline:** 1 day

#### Sample Implementation

```python
@staticmethod
def get_cache_path(
    cache_dir: Path,
    symbol: str,
    interval: str,
    date: datetime,
    exchange: str = "binance",
    market_type: str = "spot",
    data_nature: str = "klines",
    packaging_frequency: str = "daily",
) -> Path:
    """Get cache file path for a specific date.

    Args:
        cache_dir: Base cache directory
        symbol: Trading pair symbol (e.g., BTCUSDT)
        interval: Time interval (e.g., 1m, 1h)
        date: Date for the cache file
        exchange: Exchange name (default: binance)
        market_type: Market type (default: spot)
        data_nature: Data nature (default: klines)
        packaging_frequency: Packaging frequency (default: daily)

    Returns:
        Path to the cache file
    """
    # Format date as YYYY-MM-DD for the filename
    year_month_day = date.strftime("%Y-%m-%d")

    # Create path with new structure
    cache_path = (
        cache_dir / exchange / market_type / data_nature /
        packaging_frequency / symbol / interval / f"{year_month_day}.arrow"
    )

    # Ensure parent directories exist
    cache_path.parent.mkdir(parents=True, exist_ok=True)

    return cache_path
```

### Step 2: Update Usages of `get_cache_path`

- **Description:** Identify all locations in the codebase where `CacheKeyManager.get_cache_path` is called and update them to include the new parameters. For initial integration with `VisionDataClient` and `DataSourceManager` for Binance spot klines, the default parameter values can be used. **This step has a high risk of breaking dependencies if usages are missed or incorrectly updated. Use code search tools and manual code review to ensure all instances are found and updated correctly. Pay close attention to passing the correct parameters in each context, and verify default parameters are used appropriately for backward compatibility.**
- **Code Change Locations:**
  - `core/vision_data_client.py`
  - `core/data_source_manager.py`
  - `core/cache_manager.py`
- **Estimated Effort:** Medium
- **Timeline:** 2 days

#### Backward Compatibility Considerations

For backward compatibility, the implementation will:

1. Use default parameter values that match the current configuration (binance/spot/klines/daily)
2. Support both old and new cache path formats during a transition period
3. Add a utility method for resolving paths that can check both formats:

```python
def resolve_cache_path(symbol: str, interval: str, date: datetime) -> Path:
    """Try to find cache file in either the old or new structure.

    First tries the new structure, then falls back to old structure if not found.
    """
    # Try new structure first
    new_path = CacheKeyManager.get_cache_path(
        self.data_dir, symbol, interval, date,
        exchange="binance", market_type="spot",
        data_nature="klines", packaging_frequency="daily"
    )

    if new_path.exists():
        return new_path

    # Fall back to old structure
    old_path = self.data_dir / symbol / interval / f"{date.strftime('%Y-%m-%d')}.arrow"

    return old_path
```

### Step 3: Update `UnifiedCacheManager` and `VisionCacheManager`

- **Description:** Ensure that `UnifiedCacheManager` and `VisionCacheManager` classes correctly use the updated `get_cache_path` method when saving and loading cache files. Verify that cache metadata registration and retrieval are also updated to work with the new path structure. **Note:** `UnifiedCacheManager` (in `core/cache_manager.py`) is the main cache manager, while `VisionCacheManager` (in `utils/cache_validator.py`) is a utility class for Arrow file operations used by `UnifiedCacheManager`. Focus updates on ensuring `UnifiedCacheManager` correctly utilizes `CacheKeyManager.get_cache_path`. **Specifically verify that `get_cache_path`, `save_to_cache`, `load_from_cache`, and `invalidate_cache` methods in `UnifiedCacheManager` and `save_to_cache`, `load_from_cache` in `VisionCacheManager` correctly use the updated path generation.**
- **Code Change Locations:**
  - `core/cache_manager.py` (UnifiedCacheManager)
  - `core/cache_manager.py`
  - `utils/cache_validator.py` (VisionCacheManager)
- **Estimated Effort:** Low
- **Timeline:** 1 day

### Step 4: Testing and Validation

- **Description:** Implement comprehensive tests to validate the new cache structure. This includes:
  - Unit tests for `CacheKeyManager.get_cache_path` to verify path generation with different parameters.
  - **Integration tests (Dependency Focus):** Design integration tests that specifically exercise the _entire_ data retrieval and caching workflow, starting from a data request in `DataSourceManager` or `VisionDataClient`, going through data fetching, caching via `UnifiedCacheManager` and `VisionCacheManager`, and loading from cache. These tests should verify that all components work seamlessly with the new cache structure. **Specifically test integration between `DataSourceManager`, `VisionDataClient`, `UnifiedCacheManager`, and `CacheKeyManager`.**
    - Data is correctly saved to the _new_ cache structure.
    - Data is correctly loaded from the _new_ cache structure.
    - Cache hits and misses work as expected with the _new_ structure.
    - Data fetching and caching work correctly for Binance spot klines using _default_ parameters (backward compatibility). **Explicitly test backward compatibility scenarios.**
  - Test cases to verify cache hits and misses work as expected in various scenarios.
  - Tests to ensure existing functionalities (data fetching, validation, etc.) are not broken by the cache structure change (**regression tests**). **Add new regression tests and run existing test suites to catch any dependency breaks.**
- **Test File Locations:**
  - `tests/utils/test_cache_validator.py`
- **Estimated Effort:** Medium
- **Timeline:** 3 days

### Step 5: Documentation Update

- **Description:** Update project documentation to reflect the new cache structure and any changes in configuration or usage. This includes:
  - Updating `docs/data_source_manager.md`, `docs/vision_data_client.md`, `docs/cache_manager.md` to describe the new cache path structure and parameters.
  - Updating diagrams (e.g., in `docs/core_workflow_in_mermaid.md`) if cache paths are visually represented.
  - Updating `docs/caching_migration_guide.md` if the migration script is implemented or if there are changes to caching configuration for users.
- **Documentation Locations:**
  - `docs/data_source_manager.md`, `docs/vision_data_client.md`, `docs/cache_manager.md`, `docs/core_workflow_in_mermaid.md`, `docs/caching_migration_guide.md`
- **Estimated Effort:** Low
- **Timeline:** 1 day

### Step 6: Update Example Scripts/Notebooks

- **Description:** Review and update any example scripts, Jupyter notebooks, or usage examples to ensure they are consistent with the new cache structure. If necessary, add examples demonstrating how to use the new parameters in `CacheKeyManager.get_cache_path`.
- **Locations:**
  - `examples/`, `notebooks/` (or similar directories)
- **Estimated Effort:** Low
- **Timeline:** 1 day

### Step 7: Cache Migration Script (Optional)

- **Description:** Develop a script to migrate existing cache files from the old structure to the new structure. This script will:
  1. Scan the cache directory for files in the old structure
  2. For each file, read the data and metadata
  3. Create the corresponding path in the new structure
  4. Save the data to the new location
  5. Optionally, remove the old files after successful migration
  6. Generate a report of successfully migrated files and any errors
- **Code Location:** `scripts/migrate_cache.py`
- **Estimated Effort:** Medium
- **Timeline:** 2 days

#### Migration Script Outline

```python
import asyncio
from pathlib import Path
import pandas as pd
from typing import List, Dict, Any
import logging

from utils.cache_validator import CacheKeyManager, VisionCacheManager
from utils.logger_setup import get_logger

logger = get_logger(__name__, "INFO", show_path=False)

async def migrate_cache_file(
    old_path: Path,
    cache_dir: Path,
    symbol: str,
    interval: str,
    date: str,
    remove_old: bool = False
) -> bool:
    """Migrate a single cache file to the new structure.

    Args:
        old_path: Path to the existing cache file
        cache_dir: Base cache directory for the new structure
        symbol: Trading pair symbol
        interval: Time interval
        date: Date string in YYYY-MM-DD format
        remove_old: Whether to remove the old file after migration

    Returns:
        True if migration successful, False otherwise
    """
    try:
        # Load data from old cache file
        df = await VisionCacheManager.load_from_cache(old_path)
        if df is None or df.empty:
            logger.warning(f"Empty or invalid cache file: {old_path}")
            return False

        # Determine new path
        date_obj = pd.to_datetime(date).to_pydatetime()
        new_path = CacheKeyManager.get_cache_path(
            cache_dir, symbol, interval, date_obj,
            exchange="binance", market_type="spot",
            data_nature="klines", packaging_frequency="daily"
        )

        # Save to new location
        await VisionCacheManager.save_to_cache(df, new_path, date_obj)

        # Remove old file if requested
        if remove_old and new_path.exists():
            old_path.unlink()

        logger.info(f"Migrated: {old_path} -> {new_path}")
        return True

    except Exception as e:
        logger.error(f"Error migrating {old_path}: {e}")
        return False

async def scan_and_migrate(
    base_dir: Path,
    target_dir: Path,
    remove_old: bool = False
) -> Dict[str, int]:
    """Scan and migrate all cache files in the old structure.

    Args:
        base_dir: Base directory of the old cache structure
        target_dir: Base directory for the new cache structure
        remove_old: Whether to remove old files after migration

    Returns:
        Statistics about the migration
    """
    stats = {"success": 0, "failed": 0, "skipped": 0}

    # Find all .arrow files in the old structure
    for symbol_dir in base_dir.iterdir():
        if not symbol_dir.is_dir():
            continue

        symbol = symbol_dir.name

        for interval_dir in symbol_dir.iterdir():
            if not interval_dir.is_dir():
                continue

            interval = interval_dir.name

            for arrow_file in interval_dir.glob("*.arrow"):
                date = arrow_file.stem  # YYYY-MM-DD

                # Skip if already migrated
                date_obj = pd.to_datetime(date).to_pydatetime()
                new_path = CacheKeyManager.get_cache_path(
                    target_dir, symbol, interval, date_obj,
                    exchange="binance", market_type="spot",
                    data_nature="klines", packaging_frequency="daily"
                )

                if new_path.exists():
                    stats["skipped"] += 1
                    continue

                # Migrate the file
                success = await migrate_cache_file(
                    arrow_file, target_dir, symbol, interval, date, remove_old
                )

                if success:
                    stats["success"] += 1
                else:
                    stats["failed"] += 1

    return stats

async def main():
    # Configuration
    base_dir = Path("./data/cache")  # Old cache structure base
    target_dir = Path("./data/cache_new")  # New cache structure base
    remove_old = False  # Set to True to remove old files after migration

    # Run migration
    logger.info("Starting cache migration...")
    stats = await scan_and_migrate(base_dir, target_dir, remove_old)

    # Report results
    logger.info(f"Migration complete! Results:")
    logger.info(f"  Successfully migrated: {stats['success']}")
    logger.info(f"  Failed migrations: {stats['failed']}")
    logger.info(f"  Skipped (already exist): {stats['skipped']}")

if __name__ == "__main__":
    asyncio.run(main())
```

## 4. Timeline Summary

| Step                                       | Estimated Effort | Timeline (Days) |
| ------------------------------------------ | ---------------- | --------------- |
| 1. Modify `CacheKeyManager.get_cache_path` | Low              | 1               |
| 2. Update Usages of `get_cache_path`       | Medium           | 2               |
| 3. Update Cache Managers                   | Low              | 1               |
| 4. Testing and Validation                  | Medium           | 3               |
| 5. Documentation Update                    | Low              | 1               |
| 6. Update Example Scripts/Notebooks        | Low              | 1               |
| 7. Cache Migration Script (Optional)       | Medium           | 2               |
| **Total Estimated Timeline**               |                  | **11 days**     |

## 5. Benefits of Upgraded Cache Structure

1. **Improved Organization:** The cache directory structure will be more organized and easier to navigate, mirroring the Binance Vision API.
2. **Enhanced Scalability:** The system will be better prepared to support multiple exchanges, market types, and data natures in the future.
3. **Increased Maintainability:** The code will be more maintainable due to the clearer and more structured cache organization.
4. **Future-Proofing:** The upgraded structure will make it easier to integrate new data sources and expand the system's capabilities.

## 6. Rollback Plan

If any issues arise during or after the cache structure upgrade, we can easily rollback to the previous version by following these steps:

1. Reverting the changes made to `CacheKeyManager.get_cache_path` in `utils/cache_validator.py`.
2. Reverting the updates in all files where `get_cache_path` was used.
3. Deploying the previous version of the code.

No data loss is expected during rollback, as the old cache structure will remain intact unless the migration script (Step 7) is executed.

## 7. Testing Strategy

The testing strategy will heavily emphasize thorough testing to catch potential dependency breaks and ensure a regression-free upgrade. Key areas of focus include:

1. **Correct Path Generation:** Verify that `CacheKeyManager.get_cache_path` generates correct cache paths for various combinations of exchange, market type, data nature, `packaging_frequency`, symbol, interval, and date. **Unit tests are crucial here.**
2. **Successful Cache Operations:** Confirm that data can be successfully saved to and loaded from the cache using the new path structure in `VisionDataClient` and `DataSourceManager`, with integration tests covering different `packaging_frequency` values. **Focus on end-to-end workflow testing.**
3. **Cache Hit/Miss Accuracy:** Test that cache hit and miss scenarios are correctly identified and handled with the new structure across various use cases. **Cover various cache scenarios in integration tests.**
4. **Data Integrity:** Ensure that data integrity is maintained throughout the caching process with the new structure, including checksum validation and data format consistency.
5. **Backward Compatibility:** Verify that existing functionalities and tests continue to work as expected after the cache structure upgrade to prevent regressions. **Explicitly test scenarios relying on the old cache path structure to ensure backward compatibility is maintained.** **Run existing test suites and add new regression tests.**

## 8. Considerations

- **Backward Compatibility:** While default parameters are used to maintain backward compatibility, users who have customized cache paths or logic might need to adjust their configurations.
- **Testing Thoroughness:** Comprehensive testing is crucial to ensure the stability and reliability of the upgraded cache system. Allocate sufficient time and resources for thorough testing and bug fixing. Emphasize testing for both new and old cache paths to ensure backward compatibility.
- **Documentation Update:** Ensure documentation is updated as a dedicated step (Step 6) to accurately reflect the new cache structure, configuration, and usage patterns for users.
- **Dependency Risks:** The upgrade process involves updating multiple components and workflows, which can introduce new dependency risks. Carefully review and test each component to ensure that the upgrade does not break existing functionality or introduce new bugs.
- **Migration Strategy:** Consider whether to migrate existing cache files or simply start using the new structure for new requests. Migration can be resource-intensive but provides a cleaner transition, while allowing both structures to coexist may be more flexible but adds complexity.
- **Monitoring:** After implementation, monitor cache hit/miss rates to ensure the new structure is performing as expected and that there are no regressions in cache efficiency.

This roadmap provides a structured plan for upgrading the cache structure to align with the Binance Vision API and prepare for future expansions. By following these steps and considerations, we can ensure a smooth and successful upgrade process.
