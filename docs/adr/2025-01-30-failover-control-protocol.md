---
status: accepted
date: 2025-01-30
decision-maker: terrylica
consulted: Claude Code agents
research-method: Production experience + Binance API analysis
---

# Failover Control Protocol (FCP) for Data Retrieval

## Context and Problem Statement

Retrieving market data from Binance requires handling multiple data sources with different characteristics:

- **Local cache**: Fastest, but may be stale or incomplete
- **Vision API**: Bulk historical data on AWS S3, but ~48h delay
- **REST API**: Real-time, but rate-limited and slower

How do we ensure reliable data retrieval while maximizing performance?

## Decision Drivers

- Minimize network requests (cost, rate limits)
- Maximize data freshness for recent data
- Handle network failures gracefully
- Provide consistent API regardless of data source

## Considered Options

1. **Single source** - Always use REST API
2. **User-selected source** - Let user choose which source to use
3. **Failover Control Protocol** - Automatic cascade with validation

## Decision Outcome

Chosen option: **Failover Control Protocol (FCP)** because it provides the best balance of performance, reliability, and simplicity for callers.

### FCP Flow

```
┌─────────────────────────────────────────────────────────────┐
│                    DataSourceManager.get_data()              │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
                    ┌─────────────────┐
                    │  1. Check Cache │ ──── Hit ───▶ Return
                    └─────────────────┘
                              │ Miss
                              ▼
                    ┌─────────────────┐
                    │ 2. Vision API   │ ──── OK ────▶ Cache + Return
                    │    (S3 bulk)    │
                    └─────────────────┘
                              │ Fail/Stale
                              ▼
                    ┌─────────────────┐
                    │ 3. REST API     │ ──── OK ────▶ Cache + Return
                    │    (real-time)  │
                    └─────────────────┘
                              │ Fail
                              ▼
                       Raise Exception
```

### Consequences

**Good:**

- Cache hits are sub-millisecond (Arrow mmap)
- Vision API provides bulk historical data efficiently
- REST API fills gaps and provides recent data
- Each source validates data before caching
- Transparent to caller - single `get_data()` call

**Bad:**

- More complex internal implementation
- Debug logging needed to understand which source was used
- Cache invalidation requires external management

## Data Source Characteristics

| Source | Latency   | Freshness  | Rate Limits | Use Case                 |
| ------ | --------- | ---------- | ----------- | ------------------------ |
| Cache  | <1ms      | Stale      | None        | Previously fetched data  |
| Vision | 1-5s      | ~48h delay | None        | Historical bulk data     |
| REST   | 100-500ms | Real-time  | 1200/min    | Recent data, gap filling |

## Implementation Details

1. **Cache Manager** (`cache_manager.py`): Arrow files with mmap for fast reads
2. **Vision Client** (`vision_data_client.py`): FSSpec-based S3 access
3. **REST Client** (`rest_data_client.py`): httpx with retry logic
4. **DSM Orchestrator** (`data_source_manager.py`): FCP logic and validation

## More Information

- `src/data_source_manager/core/sync/data_source_manager.py` - Main DSM class
- `src/data_source_manager/core/sync/dsm_lib.py` - High-level `fetch_market_data()`
- `docs/core_architecture/` - Detailed architecture documentation
