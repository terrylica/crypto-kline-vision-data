# Pytest Asyncio and Parallel Logging

This playground demonstrates best practices for working with asyncio and logging in pytest, especially in parallel execution environments using pytest-xdist.

## Key Components

- **Enhanced Conftest Configuration**: Proper setup for pytest-asyncio loop scope to avoid KeyError issues with pytest-xdist.
- **Unified Logging Approach**: Integration with the new `tests/utils/unified_logging.py` module for consistent and reliable log capture in all test environments.

## Features Demonstrated

1. **Asyncio Configuration**: Setting `asyncio_default_fixture_loop_scope = function` to avoid KeyError issues in parallel execution.
2. **Parallel-Safe Logging**: Using the unified logging abstraction that works with pytest-xdist.
3. **Serial Test Marking**: Using `@pytest.mark.serial` to mark tests that should run serially.
4. **Comprehensive Log Capture**: Capturing and asserting logs in both sync and async test contexts.
5. **Advanced Logging Features**: Using context managers for temporary log level changes.

## Unified Logging Abstraction

This playground now uses the centralized `tests/utils/unified_logging.py` module which provides:

- `caplog_unified`: A modern fixture that works seamlessly with pytest-xdist
- `caplog_xdist_compatible`: A backward-compatible fixture for existing tests
- `assert_log_contains`: A helper function for asserting log messages
- Context manager support for temporary log level changes

## Running the Examples

To run the tests in this playground, use the following command from the project root:

```bash
scripts/op/run_tests_parallel.sh playground/pytest_parallel_async_logging
```

Or to run in sequential mode:

```bash
scripts/op/run_tests_parallel.sh -s playground/pytest_parallel_async_logging
```

## Example Code

Here's a simple example of using the unified logging approach:

```python
@pytest.mark.asyncio
async def test_async_logging_with_unified_caplog(caplog_unified):
    # Set the capture level to DEBUG
    caplog_unified.set_level(logging.DEBUG)

    # Log some messages
    logger.debug("Async debug message")
    logger.info("Starting async operation")

    # Perform async operation
    await asyncio.sleep(0.1)
    logger.info("Async operation completed")

    # Verify logs were captured properly using the helper assertion function
    assert_log_contains(caplog_unified, "Async debug message", logging.DEBUG)
    assert_log_contains(caplog_unified, "Starting async operation", logging.INFO)
    assert_log_contains(caplog_unified, "Async operation completed", logging.INFO)
```

## Context Manager for Temporary Log Level Changes

The `at_level` context manager allows for temporary changes to the log level:

```python
# Set base level to ERROR (high threshold)
caplog_unified.set_level(logging.ERROR)

# Temporarily lower threshold to INFO within context
with caplog_unified.at_level(logging.INFO):
    logger.info("Info message inside context - should be captured")
    logger.debug("Debug message inside context - should be filtered")
```

## Recommendations

1. Use `@pytest.mark.asyncio` to mark async tests individually rather than using module-level marking
2. Use `caplog_unified` or `caplog_xdist_compatible` fixtures for log capture instead of pytest's native caplog
3. Set `asyncio_default_fixture_loop_scope = function` in your pytest config
4. Use `@pytest.mark.serial` for tests that shouldn't run in parallel
5. Consider filter logs by module name when testing in noisy environments
