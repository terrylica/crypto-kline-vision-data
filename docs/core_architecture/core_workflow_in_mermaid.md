# Market Data Retrieval Workflow

```mermaid
graph LR
    A["Start: Data Request<br/>symbol, time range, interval"] --> B["**Check Cache (Daily)?**<br/>use_cache=True<br/><br/><sup>User preference & config</sup>"]
    B -- Yes --> C["**Cache Hit (Daily)?**<br/>Valid & Recent Data for Day?<br/><br/><sup>Metadata & checksum validation</sup><br/><sup>Data freshness threshold</sup>"]
    B -- No --> D["**Data Source Selection**<br/>_should_use_vision_api<br/><br/><sup>Estimate data points</sup><br/><sup>Vision API for large requests</sup>"]
    C -- Yes --> E["**Load Data from Cache**<br/>UnifiedCacheManager.load_from_cache<br/><br/><sup>Fast daily retrieval</sup><br/><sup>REST API boundary aligned</sup>"] --> F["Return Data<br/>DataFrame from Cache"]
    C -- No --> D
    D --> G1["**Vision API (Primary)**<br/>VisionDataClient.fetch<br/><br/><sup>Download-First Approach</sup><br/><sup>No pre-checking - faster retrieval</sup>"]
    G1 --> G{"**Vision API Fetch**<br/>VisionDataClient._download_data<br/><br/><sup>Direct download with dynamic concurrency</sup><br/><sup>Aligned boundaries via ApiBoundaryValidator</sup>"}
    G -- Success --> I{"**Save to Cache (Daily)?**<br/>UnifiedCacheManager.save_to_cache<br/><br/><sup>Saves with REST API-aligned boundaries</sup><br/><sup>using TimeRangeManager.align_vision_api_to_rest</sup>"}
    G -- Fail --> H["**Automatic Fallback**<br/>RestDataClient.fetch<br/><br/><sup>Transparent fallback for the user</sup><br/><sup>Same consistent interface</sup>"]
    H -- Success --> K{"**Save to Cache (Daily)?**<br/>UnifiedCacheManager.save_to_cache<br/><br/><sup>Caches successful REST API data</sup><br/><sup>Same format as Vision API data</sup>"}
    H -- Fail --> M["**Error Handling**<br/>raise Exception<br/><br/><sup>Retrieval failure</sup><br/><sup>Logged error details</sup>"]
    I --> J["Return Data<br/>DataFrame from Vision API<br/><br/><sup>Aligned with REST API boundaries</sup>"]
    K --> L["Return Data<br/>DataFrame from REST API"]
    E --> N["End: Data Retrieval<br/>Returns DataFrame"]
    F --> N
    J --> N
    L --> N
    M --> N
    style I fill:#f9f,stroke:#333,stroke-width:2px,color:#000
    style K fill:#f9f,stroke:#333,stroke-width:2px,color:#000
    style B fill:#ccf,stroke:#333,stroke-width:2px,color:#000,shape:rect
    style C fill:#ccf,stroke:#333,stroke-width:2px,color:#000,shape:rect
    style D fill:#ccf,stroke:#333,stroke-width:2px,color:#000,shape:rect
    style G1 fill:#cfc,stroke:#333,stroke-width:2px,color:#000
    style H fill:#cfc,stroke:#333,stroke-width:2px,color:#000,stroke-dasharray: 5, 5
    style E fill:#cfc,stroke:#333,stroke-width:2px,color:#000
    style G fill:#eee,stroke:#333,stroke-width:2px,color:#000
    style M fill:#fee,stroke:#333,stroke-width:2px,color:#000
```

## Updated Workflow Overview

This diagram illustrates the improved market data retrieval workflow with two key optimizations:

1. **Download-First Approach**: The Vision API client now uses a direct download-first approach without pre-checking file existence, significantly improving performance.

2. **Automatic Fallback**: If Vision API fails to retrieve data, the system automatically and transparently falls back to REST API.

The workflow retains the existing advantages while adding these performance and reliability improvements.

## Process Description

The data retrieval process begins with a user request for market data. The system first checks for valid REST API-aligned cached data. If found, it's immediately returned.

Otherwise, the data source selection process is triggered:

- **Primary Path (Vision API with Download-First)**:

  - The system tries Vision API first for most requests, especially larger historical ones
  - Uses download-first approach (no pre-checking) for optimal performance
  - Applies dynamic concurrency optimization based on batch size
  - Downloads data by day, combines results, and caches with REST API-aligned boundaries

- **Automatic Fallback Path (REST API)**:
  - If Vision API fails or returns no data, the system automatically falls back to REST API
  - This fallback is transparent to the user - same interface and data format
  - REST API data is also cached for future retrieval

All data sources (Vision API, REST API, and cache) deliver consistent results with identical time boundaries, ensuring a seamless experience regardless of which source ultimately provides the data.

## Key Benefits

1. **Improved Performance**: The download-first approach eliminates unnecessary HEAD requests
2. **Higher Reliability**: Automatic fallback ensures data retrieval even when Vision API is unavailable
3. **Optimized Resource Usage**: Dynamic concurrency adjustment based on batch size
4. **Consistent Data Format**: All sources return identical data structure
5. **Transparent Experience**: Users don't need to worry about which source provides the data
