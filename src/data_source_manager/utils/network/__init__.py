#!/usr/bin/env python
"""Network utilities subpackage.

This subpackage provides network-related utilities including:
- HTTP client factory functions
- Download handling with progress tracking
- API request utilities with retry logic
- Vision API download management
- Connectivity testing

# ADR: docs/adr/2026-01-30-claude-code-infrastructure.md
# Refactoring: Split from network_utils.py (761 lines) for modularity
"""

from data_source_manager.utils.network.api import (
    make_api_request,
    test_connectivity,
)
from data_source_manager.utils.network.client_factory import (
    Client,
    create_client,
    create_httpx_client,
    safely_close_client,
)
from data_source_manager.utils.network.download import (
    DownloadHandler,
    DownloadProgressTracker,
    download_files_concurrently,
)
from data_source_manager.utils.network.exceptions import (
    DownloadException,
    DownloadStalledException,
    RateLimitException,
)
from data_source_manager.utils.network.vision_download import (
    VisionDownloadManager,
)

__all__ = [
    # Client factory
    "Client",
    "DownloadException",
    "DownloadHandler",
    "DownloadProgressTracker",
    "DownloadStalledException",
    "RateLimitException",
    "VisionDownloadManager",
    "create_client",
    "create_httpx_client",
    # Download handling
    "download_files_concurrently",
    # API utilities
    "make_api_request",
    "safely_close_client",
    "test_connectivity",
]
