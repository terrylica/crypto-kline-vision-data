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

### Step 2: Update Usages of `get_cache_path`

- **Description:** Identify all locations in the codebase where `CacheKeyManager.get_cache_path` is called and update them to include the new parameters. For initial integration with `VisionDataClient` and `DataSourceManager` for Binance spot klines, the default parameter values can be used. **This step has a high risk of breaking dependencies if usages are missed or incorrectly updated. Use code search tools and manual code review to ensure all instances are found and updated correctly. Pay close attention to passing the correct parameters in each context, and verify default parameters are used appropriately for backward compatibility.**
- **Code Change Locations:**
  - `core/vision_data_client.py`
  - `core/data_source_manager.py`
  - `core/cache_manager.py`
- **Estimated Effort:** Medium
- **Timeline:** 2 days

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

## 4. Timeline Summary

| Step                                       | Estimated Effort | Timeline (Days) |
| ------------------------------------------ | ---------------- | --------------- |
| 1. Modify `CacheKeyManager.get_cache_path` | Low              | 1               |
| 2. Update Usages of `get_cache_path`       | Medium           | 2               |
| 3. Update Cache Managers                   | Low              | 1               |
| 4. Testing and Validation                  | Medium           | 3               |
| 5. Documentation Update                    | Low              | 1               |
| 6. Update Example Scripts/Notebooks        | Low              | 1               |
| **Total Estimated Timeline**               |                  | **9 days**      |

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

No data loss is expected during rollback, as the old cache structure will remain intact unless the migration script (Step 4) is executed.

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

This roadmap provides a structured plan for upgrading the cache structure to align with the Binance Vision API and prepare for future expansions. By following these steps and considerations, we can ensure a smooth and successful upgrade process.
