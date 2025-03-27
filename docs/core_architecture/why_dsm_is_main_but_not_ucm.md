# DataSourceManager vs. UnifiedCacheManager: Roles and Responsibilities

This document outlines the distinct roles of `DataSourceManager` and `UnifiedCacheManager` within the market data system, clarifying why `DataSourceManager` is considered the overarching controller while caching, managed by `UnifiedCacheManager`, is a service within it.

## DataSourceManager: The Overarching Controller

The `DataSourceManager` module acts as the central orchestrator and decision-maker for market data retrieval. It embodies the core business logic of the system, providing a unified and intelligent interface for accessing data.

**Key Responsibilities:**

- **Central Point of Access:** `DataSourceManager` is the primary entry point for users requesting market data. Clients interact with `DataSourceManager`'s `get_data` method, not directly with caching or specific data clients.
- **Intelligent Source Selection:** It implements the core logic for choosing the optimal data source (Vision API or REST API) based on request parameters like data volume and time range. This decision-making process is independent of caching and is central to its control function.
- **Workflow Orchestration:** `DataSourceManager` orchestrates the complete data retrieval process, which includes request validation, time window adjustments, **data format standardization**, cache management, data source selection, and fetching from the chosen source. It ensures that the final output is consistently formatted regardless of whether the data comes from the cache, Vision API, or REST API.
- **Abstraction Layer:** It provides a high-level abstraction, shielding users from the complexities of interacting with different APIs and managing caching mechanisms. Users interact with a single, consistent `get_data` interface.

## UnifiedCacheManager: The Managed Caching Service

In contrast, `UnifiedCacheManager` is a specialized component focused solely on the technical implementation of caching. It operates as a managed service _within_ the broader data retrieval system controlled by `DataSourceManager`.

**Key Responsibilities:**

- **Specialized Caching Functionality:** `UnifiedCacheManager`'s role is limited to the technical aspects of caching:
  - **Storage Management:** Organizes cached data in a structured directory.
  - **Data Persistence:** Saves DataFrames to efficient Arrow files.
  - **Data Retrieval:** Loads DataFrames from the Arrow cache.
  - **Metadata Management:** Maintains a metadata index (`cache_index.json`) to track cached data and its integrity.
  - **Cache Invalidation:** Removes outdated or corrupted cache entries.
  - **Integrity Checks:** Validates cache file checksums and existence.
- **Passive and Reactive Role:** `UnifiedCacheManager` is passive; it doesn't initiate actions. It _reacts_ to requests from `DataSourceManager` to perform caching operations (save, load, validate, invalidate).
- **Dependency on DataSourceManager:** `UnifiedCacheManager` is a service that is **utilized by** `DataSourceManager` to provide caching functionality. The dependency is unidirectional; `UnifiedCacheManager` does not control or depend on `DataSourceManager`.
- **Technical Focus, No Business Logic:** `UnifiedCacheManager`'s logic is purely technical, focused on efficient storage and retrieval. It does not contain business logic related to data source selection, high-level data validation, or the overall data retrieval strategy.

## Analogy: The Restaurant Kitchen

To illustrate the relationship, consider a restaurant kitchen:

- **`DataSourceManager`:** The **Head Chef** - Manages the entire kitchen, plans menus (data requests), decides on ingredients (data sources), directs cooking processes (data retrieval workflow), and manages the pantry (caching).
- **`UnifiedCacheManager`:** The **Pantry & Inventory Manager** - A critical service for not just storing and retrieving ingredients (cached data), but also for managing their quality and freshness (data integrity checks). The Pantry Manager, under the Head Chef's direction, ensures ingredients are properly stored, checks for spoilage (cache invalidation), and verifies inventory (metadata management). However, the pantry doesn't decide what dishes to cook or when to use ingredients; it simply provides storage, retrieval, and basic quality control as directed by the Head Chef.
- **`VisionDataClient` & `EnhancedRetriever`:** The **Sous Chefs/Line Cooks** - Skilled at preparing specific dishes (fetching data from specific APIs) under the Head Chef's direction.

## Conclusion

While caching, implemented by `UnifiedCacheManager`, is a vital feature for performance and efficiency, it is a _managed service_ within the system. `DataSourceManager` is the **overarching controller** because it orchestrates the entire data retrieval process, makes intelligent decisions about data sources, and utilizes caching as one of its tools to optimize data delivery. `DataSourceManager` embodies the core business logic of data access, while `UnifiedCacheManager` provides a specialized technical service to support it.
