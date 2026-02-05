#!/usr/bin/env python
"""Network utilities for HTTP requests, downloads, and connectivity testing.

DEPRECATED: This module is a backward-compatibility re-export from the network/ subpackage.
Import directly from data_source_manager.utils.network for new code.

# ADR: docs/adr/2026-01-30-claude-code-infrastructure.md
# Refactoring: Original 761-line module split into network/ subpackage
"""

# Re-export everything from the network subpackage
from data_source_manager.utils.network import (
    Client,
    DownloadException,
    DownloadHandler,
    DownloadProgressTracker,
    DownloadStalledException,
    RateLimitException,
    VisionDownloadManager,
    create_client,
    create_httpx_client,
    download_files_concurrently,
    make_api_request,
    safely_close_client,
    test_connectivity,
)

__all__ = [
    "Client",
    "DownloadException",
    "DownloadHandler",
    "DownloadProgressTracker",
    "DownloadStalledException",
    "RateLimitException",
    "VisionDownloadManager",
    "create_client",
    "create_httpx_client",
    "download_files_concurrently",
    "make_api_request",
    "safely_close_client",
    "test_connectivity",
]
