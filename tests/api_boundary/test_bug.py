#!/usr/bin/env python
"""Simplified test to reproduce endpoint URL issues."""

import pytest
import inspect
import sys
import importlib
from datetime import datetime, timezone, timedelta
from utils.market_constraints import MarketType, ChartType, Interval, get_endpoint_url
from utils.api_boundary_validator import ApiBoundaryValidator
from utils.logger_setup import logger


# Configure pytest-asyncio default event loop scope
pytestmark = pytest.mark.asyncio(loop_scope="function")


@pytest.mark.asyncio
async def test_direct_url_construction():
    """Test direct URL construction with different approaches."""
    logger.info("=== Environment Information ===")
    logger.info(f"Python version: {sys.version}")
    logger.info(f"pytest version: {pytest.__version__}")

    # Check module sources to identify any potential module loading issues
    logger.info("=== Module Sources ===")
    for name in ["utils.market_constraints", "utils.api_boundary_validator"]:
        module = importlib.import_module(name)
        logger.info(f"{name} loaded from: {module.__file__}")

    logger.info("=== ChartType Definition ===")
    chart_type_src = inspect.getsource(ChartType)
    logger.info(
        chart_type_src[:200] + "..." if len(chart_type_src) > 200 else chart_type_src
    )

    # Test with string literal
    url_from_string = get_endpoint_url(MarketType.SPOT, "klines")
    logger.info("=== URL Construction ===")
    logger.info(f"URL from string: {url_from_string}")

    # Test with enum value
    url_from_enum = get_endpoint_url(MarketType.SPOT, ChartType.KLINES.endpoint)
    logger.info(f"URL from enum: {url_from_enum}")

    # Test string interpolation with enum
    url_with_string_interp = get_endpoint_url(
        MarketType.SPOT, ChartType.KLINES.endpoint
    )
    logger.info(f"URL with string interpolation: {url_with_string_interp}")

    # Get _call_api method from ApiBoundaryValidator to examine it
    logger.info("=== ApiBoundaryValidator._call_api ===")
    call_api_src = inspect.getsource(ApiBoundaryValidator._call_api)
    logger.info(call_api_src[:200] + "..." if len(call_api_src) > 200 else call_api_src)

    # Assert they should be the same
    assert (
        url_from_string == url_from_enum
    ), "URLs should match whether using string or enum"
    assert "ChartType.KLINES" not in url_from_enum, "Enum name should not appear in URL"

    # Test in the context where API validator uses it
    validator = ApiBoundaryValidator(MarketType.SPOT)
    try:
        # Get a URL from _call_api method indirectly (can't call directly as it's private)
        # Call a harmless method that uses _call_api
        logger.info("=== Testing URL construction via ApiBoundaryValidator ===")

        # Use a past time range to avoid issues with future data
        end_time = datetime.now(timezone.utc) - timedelta(days=2)
        start_time = end_time - timedelta(minutes=5)

        logger.info(f"Time range: {start_time} to {end_time}")

        valid = await validator.is_valid_time_range(
            start_time, end_time, Interval.MINUTE_1
        )
        logger.info(f"is_valid_time_range returned {valid}")
    finally:
        await validator.close()


# Note: The direct asyncio.run() invocation is removed to comply with the requirement
# to use scripts/op/run_tests_parallel.sh as the entry point
