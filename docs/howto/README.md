# How-To Guides

Quick reference guides for common tasks in the codebase.

## Code Quality

- [Removing Unused Imports](remove_unused_imports.md) - Find and remove unused imports
- [Code Quality Checks](code_quality_checks.md) - Perform various code quality checks

## HTTP Client

- [Using curl_cffi](using_curl_cffi.md) - Best practices for the curl_cffi HTTP client

## Testing

Run tests using our parallel test script:

```bash
# Run all tests
scripts/op/run_tests_parallel.sh

# Run specific test directory
scripts/op/run_tests_parallel.sh tests/cache_structure
```

## AWS CLI Commands

Always use the `--no-cli-pager` flag with AWS CLI commands:

```bash
# Good
aws s3 ls --no-cli-pager

# Bad
aws s3 ls
```

## Git Operations

Move or rename files using git:

```bash
# Move a file (preserves history)
git mv old_path/file.py new_path/file.py

# Rename a file
git mv old_name.py new_name.py

# Move a directory
git mv old_dir/ new_dir/
```
