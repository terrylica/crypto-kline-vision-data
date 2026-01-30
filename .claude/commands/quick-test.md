---
name: quick-test
description: Run quick unit tests to validate changes
disable-model-invocation: true
---

# Quick Test

Run fast unit tests to validate code changes.

## Usage

`/quick-test` - Run all unit tests
`/quick-test $ARGUMENTS` - Run tests matching pattern

## Commands

### All Unit Tests

```bash
uv run -p 3.13 pytest tests/unit/ -v --tb=short
```

### With Coverage

```bash
uv run -p 3.13 pytest tests/unit/ --cov=src/data_source_manager --cov-report=term-missing
```

### Specific Test Pattern

```bash
uv run -p 3.13 pytest tests/unit/ -v -k "$ARGUMENTS"
```

### Fast Fail (stop on first error)

```bash
uv run -p 3.13 pytest tests/unit/ -v -x
```

## Test Locations

| Directory            | Type         | Speed | Network |
| -------------------- | ------------ | ----- | ------- |
| `tests/unit/`        | Unit tests   | ~0.5s | No      |
| `tests/integration/` | Integration  | ~5s   | Yes     |
| `tests/fcp_pm/`      | FCP protocol | ~2s   | Maybe   |
| `tests/okx/`         | OKX provider | ~3s   | Yes     |

## Markers

```bash
# Skip slow tests
uv run -p 3.13 pytest tests/ -v -m "not slow"

# Only integration tests
uv run -p 3.13 pytest tests/ -v -m integration

# Only OKX tests
uv run -p 3.13 pytest tests/okx/ -v -m okx
```
