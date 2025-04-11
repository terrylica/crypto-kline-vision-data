# Testing Documentation

This directory contains documentation on testing practices, utilities, and infrastructure for the codebase.

## Key Components

### Unified Logging Abstraction

The [unified logging abstraction](unified_logging.md) provides a robust solution for capturing logs during test execution, particularly in parallel environments using pytest-xdist. This implementation follows Occam's Razor and the Liskov Substitution Principle to create a comprehensive yet simple approach.

### Running Tests

Tests are executed using the `scripts/op/run_tests_parallel.sh` script, which supports:

- Running tests in parallel or sequentially
- Interactive test selection
- Log capturing and error summaries
- Profiling and visualization options

See the script's help message for details:

```bash
scripts/op/run_tests_parallel.sh -h
```

## Best Practices

1. **Use the Unified Logging Approach**

   - Use `caplog_unified` for all new tests
   - Leverage helper functions like `assert_log_contains` for cleaner assertions
   - Take advantage of the context manager for temporary log level changes

2. **Mark Serial Tests Appropriately**

   - Use `@pytest.mark.serial` for tests that should not run in parallel
   - Keep most tests parallel-friendly for faster execution

3. **Configure Asyncio Properly**

   - Use `@pytest.mark.asyncio` for async tests
   - Ensure `asyncio_default_fixture_loop_scope = function` is set
   - Clean up asyncio resources properly

4. **Avoid Mocking**
   - Use real-world data for tests
   - Implement actual integration tests against real components
   - Document special test cases with appropriate markers

## Directory Structure

- `unified_logging.md`: Documentation for the unified logging abstraction
- Additional documentation will be added as more unified abstractions are developed

## Future Directions

- Unified fixture management
- Standardized approach for API testing
- Enhanced assertion helpers
- Test data management utilities
