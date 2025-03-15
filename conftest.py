#!/usr/bin/env python
"""Pytest configuration for Binance Data Service tests."""




def pytest_configure(config):
    """Register custom markers."""
    config.addinivalue_line(
        "markers",
        "asyncio_disabled: mark test to run without asyncio",
    )


def pytest_collection_modifyitems(items):
    """Handle asyncio_disabled marker."""
    for item in items:
        if item.get_closest_marker("asyncio_disabled"):
            if hasattr(item, "fixturenames"):
                item.fixturenames = [name for name in item.fixturenames if not name.startswith("asyncio")]
