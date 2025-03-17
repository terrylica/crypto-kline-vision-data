# Utility Scripts Documentation

This document describes the utility scripts available in the Binance Data Services project to help with development, testing, and troubleshooting.

## Table of Contents

1. [Test Runner Script](#test-runner-script)
   - [Interactive Mode](#interactive-mode)
   - [Command Line Mode](#command-line-mode)
   - [Coverage Options](#coverage-options)
   - [Available Test Modes](#available-test-modes)

## Test Runner Script

The `scripts/run_tests.sh` script provides a convenient way to run tests with various configurations.

### Interactive Mode

When run without arguments, the script enters interactive mode, which guides you through a menu-based selection process:

```bash
./scripts/run_tests.sh
```

The interactive mode lets you:

1. Choose which tests to run (specific files or all tests)
2. Select a test mode (standard, verbose, debug, etc.)
3. Configure log levels and output capture options

### Command Line Mode

For automation or quick execution, you can provide arguments directly:

```bash
./scripts/run_tests.sh [test_path] [log_level] [additional_pytest_args]
```

Examples:

```bash
# Run specific test file with INFO log level
./scripts/run_tests.sh tests/test_vision_api_core_functionality.py INFO

# Run all tests with DEBUG log level and coverage reporting
./scripts/run_tests.sh tests/ DEBUG --cov=. --cov-report=term
```

### Coverage Options

When running with coverage, the script shows coverage reports in the terminal:

```bash
./scripts/run_tests.sh tests/ INFO --cov=. --cov-report=term
```

Coverage flags:

- `--cov=.`: Enable coverage for all modules
- `--cov-report=term`: Display coverage summary in terminal

### Available Test Modes

The interactive mode offers several preset test modes:

1. **Standard**: Normal tests with INFO level logging
2. **Verbose**: Detailed output with DEBUG level logging
3. **Quiet**: Minimal output with ERROR level logging
4. **Debug**: DEBUG level with pdb for debugging failures
5. **Performance**: Shows timing information for the slowest tests
6. **Coverage**: Code coverage analysis with terminal reporting
7. **Parallel**: Run tests across multiple workers (if pytest-xdist is installed)
8. **Custom**: Configure options manually

## Tips and Tricks

- For viewing coverage information, use the terminal output mode (`--cov-report=term`)
- For detailed coverage inspection, you can add `--cov-report=term-missing` to see which lines are not covered
- When running in a container, the test runner automatically detects the environment
- Setting `DEBUG_SCRIPT=true` before running the script will show additional debug information
- Choose the Custom mode to configure the test environment precisely for your needs
