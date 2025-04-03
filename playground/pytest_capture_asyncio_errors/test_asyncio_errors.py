#!/usr/bin/env python
"""Test file demonstrating asyncio errors and regular assertion failures."""

import asyncio
import pytest
from datetime import datetime, timezone


# Regular test with assertion error
def test_simple_assertion():
    """A simple test with an assertion error."""
    expected = 42
    actual = 43
    assert expected == actual, f"Expected {expected}, got {actual}"


# Asyncio test with an uncaught task
@pytest.mark.asyncio
async def test_asyncio_task_destroyed():
    """Test that creates a task but doesn't await it."""
    # Create a task but don't await it - causes "Task was destroyed but it is pending"
    asyncio.create_task(asyncio.sleep(0.1))
    # This will exit without waiting for the task to complete


# Asyncio test with an explicit failure
@pytest.mark.asyncio
async def test_asyncio_with_assertion():
    """An asyncio test with an assertion error."""
    result = await asyncio.sleep(0.1)
    assert result is not None, "Sleep should return a value"


# Asyncio test that fails with an exception
@pytest.mark.asyncio
async def test_asyncio_with_exception():
    """An asyncio test that raises an exception."""

    async def failing_coroutine():
        await asyncio.sleep(0.1)
        raise ValueError("This is a deliberate error")

    await failing_coroutine()


# Nested asyncio task test
@pytest.mark.asyncio
async def test_nested_asyncio_tasks():
    """Test with nested asyncio tasks where a child task fails."""

    async def child_task():
        await asyncio.sleep(0.1)
        # This will be an uncaught exception in a task
        raise RuntimeError("Error in child task")

    # Start task but don't await it
    task = asyncio.create_task(child_task())

    # Let the test finish without handling the task error
    await asyncio.sleep(0.2)

    # The test will pass but the task will fail after the test


# Test with datetime assertion
@pytest.mark.asyncio
async def test_datetime_comparison():
    """Test comparing datetime objects."""
    dt1 = datetime(2023, 1, 1, tzinfo=timezone.utc)
    dt2 = datetime(2023, 1, 2, tzinfo=timezone.utc)

    # This will fail with a nice diff
    assert dt1 == dt2, "Dates should match"


@pytest.mark.asyncio
async def test_task_not_awaited():
    """Test that creates an unawaited task that raises an exception."""

    async def failing_task():
        await asyncio.sleep(0.1)
        raise ValueError("This task was not awaited properly")

    # Create a task but don't await it
    asyncio.create_task(failing_task())
    await asyncio.sleep(0.2)  # Allow task to execute and fail


@pytest.mark.asyncio
async def test_assertion_error():
    """Test with a simple assertion error."""
    await asyncio.sleep(0.1)
    assert False, "This is a deliberate assertion failure"


@pytest.mark.asyncio
async def test_success():
    """A successful test to contrast with the failures."""
    await asyncio.sleep(0.1)
    assert True


@pytest.mark.asyncio
async def test_exception_in_test():
    """Test that raises an exception directly."""
    await asyncio.sleep(0.1)
    raise RuntimeError("This is a deliberate exception in test")
