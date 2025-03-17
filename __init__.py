#!/usr/bin/env python

"""Binance Data Service package for efficient market data retrieval.

This package provides tools for downloading and caching market data from Binance Vision.
The primary interface is the VisionDataClient, which uses Apache Arrow MMAP for
optimal performance in data storage and retrieval.

Key Features:
- Efficient data retrieval using Apache Arrow MMAP
- Automatic caching with zero-copy reads
- Timezone-aware timestamp handling
- Column-based data access
- Concurrent download management
"""

from core.vision_constraints import TimestampedDataFrame
from core.vision_data_client import VisionDataClient

__all__ = ["VisionDataClient", "TimestampedDataFrame"]
