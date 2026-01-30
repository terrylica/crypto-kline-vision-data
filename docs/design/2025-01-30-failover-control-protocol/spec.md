# FCP Implementation Specification

**ADR**: [Failover Control Protocol](/docs/adr/2025-01-30-failover-control-protocol.md)

## Overview

Implementation details for the Failover Control Protocol (FCP) that manages automatic data source failover.

## Core Components

### DataSourceManager

**File**: `src/data_source_manager/core/sync/data_source_manager.py`

```python
class DataSourceManager:
    """Main entry point with FCP implementation."""

    def get_data(
        self,
        symbol: str,
        start_time: datetime,
        end_time: datetime,
        interval: Interval,
    ) -> pl.DataFrame:
        """Fetch data using FCP priority: Cache → Vision → REST."""
```

### Cache Manager

**File**: `src/data_source_manager/core/providers/binance/cache_manager.py`

- Uses Apache Arrow for fast mmap reads
- One file per day per symbol/interval
- Path: `~/.cache/data_source_manager/{provider}/{market}/{symbol}/{interval}/{date}.arrow`

### Vision Client

**File**: `src/data_source_manager/core/providers/binance/vision_data_client.py`

- FSSpec-based S3 access to Binance Vision
- ~48h delay for new data availability
- No rate limits

### REST Client

**File**: `src/data_source_manager/core/providers/binance/rest_data_client.py`

- httpx with retry logic
- 1200 requests/minute rate limit
- Real-time data availability

## FCP Decision Flow

```
┌─────────────────────────────────────────────────────────────┐
│                 DataSourceManager.get_data()                │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
                    ┌─────────────────┐
                    │  1. Check Cache │
                    │  (Arrow mmap)   │
                    └─────────────────┘
                         │         │
                      Hit │         │ Miss
                         ▼         │
                    Return data    │
                                   ▼
                    ┌─────────────────┐
                    │ 2. Vision API   │
                    │ (S3 historical) │
                    └─────────────────┘
                         │         │
                      OK │         │ 403/Fail
                         ▼         │
                    Cache + Return │
                                   ▼
                    ┌─────────────────┐
                    │ 3. REST API     │
                    │ (real-time)     │
                    └─────────────────┘
                         │         │
                      OK │         │ Fail
                         ▼         ▼
               Cache + Return  Raise Error
```

## Caching Rules

### When to Cache

- Complete days from Vision API
- Complete days from REST API (historical, not today)

### When NOT to Cache

- Partial days (still accumulating)
- Today's data (incomplete)
- Future timestamps (invalid)
- Error responses

## Error Handling

| Error            | Source | Action                   |
| ---------------- | ------ | ------------------------ |
| 403 Forbidden    | Vision | Fall through to REST     |
| 429 Rate Limit   | REST   | Raise RateLimitError     |
| Connection Error | Any    | Fall through or raise    |
| Invalid Symbol   | Any    | Raise InvalidSymbolError |

## Verification Checklist

- [ ] Cache hit returns data in <10ms
- [ ] Vision fallback triggers on cache miss
- [ ] REST fallback triggers on Vision 403
- [ ] Complete days are cached after fetch
- [ ] Partial days are NOT cached
- [ ] Rate limits are respected
- [ ] All timestamps use UTC
