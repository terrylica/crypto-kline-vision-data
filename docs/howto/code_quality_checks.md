# Code Quality Checks

## Linting with Pylint

```bash
# Full pylint scan
python -m pylint --recursive=y .

# Specific checks only
python -m pylint --disable=all --enable=unused-import,undefined-variable --recursive=y .
```

## Common Code Checks

### Remove Unused Imports

```bash
# Find
python -m pylint --disable=all --enable=unused-import --recursive=y .

# Fix
autoflake --remove-all-unused-imports --recursive --in-place .
```

## Automation

### Run Tests

```bash
# Run all tests
scripts/op/run_tests_parallel.sh

# Run specific test directory
scripts/op/run_tests_parallel.sh tests/cache_structure
```

### Pre-commit Configuration

Add to `.pre-commit-config.yaml`:

```yaml
repos:
  - repo: https://github.com/pycqa/pylint
    rev: v2.17.0
    hooks:
      - id: pylint
        args: ["--disable=all", "--enable=unused-import,undefined-variable"]

  - repo: https://github.com/pre-commit/mirrors-mypy
    rev: v1.4.1
    hooks:
      - id: mypy
```
