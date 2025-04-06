#!/usr/bin/env python
"""Tests for the DataValidation.validate_query_time_boundaries function."""

import pytest
from datetime import datetime, timezone, timedelta
import json

from utils.validation import DataValidation
from utils.logger_setup import logger


def test_normal_date_range(caplog_unified):
    """Test validate_query_time_boundaries with normal date range."""
    caplog_unified.set_level("INFO")

    # Set up test data
    now = datetime.now(timezone.utc)
    start_time = now - timedelta(days=7)
    end_time = now

    # Test normal case (past to present)
    start, end, metadata = DataValidation.validate_query_time_boundaries(
        start_time, end_time, handle_future_dates="error"
    )

    # Verify results
    assert start == start_time, "Start time should be unchanged"
    assert end == end_time, "End time should be unchanged"
    assert not metadata.get("is_truncated", False), "Should not be truncated"
    assert isinstance(metadata.get("warnings", []), list), "Warnings should be a list"

    # Log for debugging
    logger.info(f"Metadata: {json.dumps(str(metadata), indent=2)}")


def test_future_date_error():
    """Test that future dates raise errors when handle_future_dates='error'."""
    # Set up test data with future end time
    now = datetime.now(timezone.utc)
    start_time = now - timedelta(days=7)
    future_time = now + timedelta(days=1)

    # Test future case with error handling
    with pytest.raises(ValueError) as excinfo:
        DataValidation.validate_query_time_boundaries(
            start_time, future_time, handle_future_dates="error"
        )

    # Verify the error message mentions future
    assert "future" in str(excinfo.value).lower(), "Error should mention future date"


def test_future_date_truncation():
    """Test future date truncation when handle_future_dates='truncate'."""
    # Set up test data with future end time
    now = datetime.now(timezone.utc)
    start_time = now - timedelta(days=7)
    future_time = now + timedelta(days=1)

    # Test truncation case
    start, end, metadata = DataValidation.validate_query_time_boundaries(
        start_time, future_time, handle_future_dates="truncate"
    )

    # Verify results
    assert start == start_time, "Start time should be unchanged"
    assert end <= now, "End time should be truncated to current time or earlier"
    assert metadata.get("is_truncated", False), "Should be marked as truncated"
    assert (
        len(metadata.get("warnings", [])) > 0
    ), "Should include warning about truncation"

    # Check for warning about truncation
    warnings = metadata.get("warnings", [])
    truncation_warning = next((w for w in warnings if "truncated" in w.lower()), None)
    assert truncation_warning is not None, "Should warn about truncation"


def test_future_date_allow():
    """Test future date passing when handle_future_dates='allow'."""
    # Set up test data with future end time
    now = datetime.now(timezone.utc)
    start_time = now - timedelta(days=7)
    future_time = now + timedelta(days=1)

    # Test allowing future dates
    start, end, metadata = DataValidation.validate_query_time_boundaries(
        start_time, future_time, handle_future_dates="allow"
    )

    # Verify results
    assert start == start_time, "Start time should be unchanged"
    assert end == future_time, "End time should remain in future"
    assert not metadata.get("is_truncated", False), "Should not be truncated"
    assert (
        len(metadata.get("warnings", [])) > 0
    ), "Should include warning about future date"

    # Check for warning about future date
    warnings = metadata.get("warnings", [])
    future_warning = next((w for w in warnings if "future" in w.lower()), None)
    assert future_warning is not None, "Should warn about future date"


def test_invalid_handle_future_dates():
    """Test that invalid handle_future_dates value raises ValueError."""
    now = datetime.now(timezone.utc)
    start_time = now - timedelta(days=7)
    future_time = now + timedelta(days=1)

    # Test with invalid handler
    with pytest.raises(ValueError) as excinfo:
        DataValidation.validate_query_time_boundaries(
            start_time, future_time, handle_future_dates="invalid_option"
        )

    # Verify error message mentions the invalid option
    error_message = str(excinfo.value).lower()
    assert "invalid" in error_message, "Error should mention invalid option"
    assert "handle_future_dates" in error_message, "Error should mention parameter name"


def test_both_dates_in_future():
    """Test when both start and end dates are in the future."""
    now = datetime.now(timezone.utc)
    future_start = now + timedelta(days=1)
    future_end = now + timedelta(days=2)

    # Test with both dates in future, should error
    with pytest.raises(ValueError) as excinfo:
        DataValidation.validate_query_time_boundaries(
            future_start, future_end, handle_future_dates="error"
        )

    # Verify error message mentions start time
    assert "start time" in str(excinfo.value).lower(), "Error should mention start time"

    # Test with truncation
    start, end, metadata = DataValidation.validate_query_time_boundaries(
        future_start, future_end, handle_future_dates="truncate"
    )

    # Both should be truncated to now
    assert start <= now, "Start time should be truncated to current time"
    assert end <= now, "End time should be truncated to current time"
    assert metadata.get("is_truncated", False), "Should be marked as truncated"
    assert (
        len(metadata.get("warnings", [])) >= 2
    ), "Should include warnings for both dates"
