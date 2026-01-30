# Data Source Manager

**Navigation**: [docs/INDEX.md](docs/INDEX.md) | [Examples](examples/) | [Tests](tests/)

Professional market data integration package with Failover Control Protocol (FCP) for reliable data retrieval from Binance Vision API, REST API, and local Apache Arrow cache.

---

## Quick Reference

| Command                                   | Purpose                   |
| ----------------------------------------- | ------------------------- |
| `uv run -p 3.13 pytest tests/unit/`       | Run unit tests            |
| `uv run -p 3.13 pytest tests/okx/ -m okx` | Run OKX integration tests |
| `uv run -p 3.13 ruff check --fix .`       | Lint and auto-fix         |
| `uv run -p 3.13 ruff format .`            | Format code               |
| `mise run release:dry`                    | Preview semantic-release  |
| `mise run release:full`                   | Run full release          |

---

## Python Version Policy (CRITICAL)

**Python 3.13 ONLY. Never use 3.14 or any other version.**

- All `uv run` commands: use `--python 3.13` or `-p 3.13`
- Never change Python version in `mise.toml`, `.python-version`, or `pyproject.toml`
- If a tool requires a different Python version, stop and ask

---

## Code Style

- **Imports**: Use absolute imports with `data_source_manager.` prefix
- **Type hints**: Required for all public functions
- **Docstrings**: Google style (enforced by ruff)
- **Line length**: 120 characters max
- **Formatting**: ruff format (replaces black)

---

## Package Architecture

```
src/data_source_manager/
├── core/
│   ├── sync/                  # Synchronous data managers
│   │   ├── data_source_manager.py   # Main DSM class with FCP
│   │   └── dsm_lib.py              # High-level fetch functions
│   └── providers/
│       └── binance/           # Binance-specific implementations
│           ├── vision_data_client.py    # Binance Vision API
│           ├── rest_data_client.py      # REST API fallback
│           └── cache_manager.py         # Arrow cache
└── utils/
    ├── market_constraints.py   # Enums: DataProvider, MarketType, Interval
    ├── loguru_setup.py         # Logging configuration
    └── for_core/              # Internal utilities
```

**Key classes**:

- `DataSourceManager` - Main entry point with FCP
- `DataSourceConfig` - Configuration for DSM instances
- `DataProvider`, `MarketType`, `Interval` - Core enums

---

## Failover Control Protocol (FCP)

Data retrieval follows this priority:

1. **Cache** - Local Arrow files (fastest)
2. **Vision API** - Binance Vision on AWS S3 (bulk historical)
3. **REST API** - Binance REST (real-time, rate-limited)

Recent data (~48h) typically not in Vision API, falls through to REST.

---

## Testing

```bash
# Unit tests only (fast, no network)
uv run -p 3.13 pytest tests/unit/ -v

# Integration tests (requires network)
uv run -p 3.13 pytest tests/integration/ -v

# OKX API tests (marked @pytest.mark.okx)
uv run -p 3.13 pytest tests/okx/ -m okx -v

# All tests
uv run -p 3.13 pytest tests/ -v
```

**Test markers**:

- `@pytest.mark.integration` - External service calls
- `@pytest.mark.okx` - OKX-specific tests
- `@pytest.mark.serial` - Must run sequentially

---

## Release Process

Semantic-release with conventional commits:

```bash
# Preflight checks
mise run release:preflight

# Dry run (preview version bump)
mise run release:dry

# Full release
mise run release:full
```

**Commit types**: `feat:` (minor), `fix:` (patch), `feat!:` or `BREAKING CHANGE:` (major)

---

## Environment Setup

```bash
# Install dependencies
uv sync --dev

# Set up mise environment (loads GH_TOKEN from .mise.local.toml)
mise trust

# Verify setup
uv run -p 3.13 python -c "from data_source_manager import DataSourceManager; print('OK')"
```

**Required for release**: Create `.mise.local.toml` from `.mise.local.toml.example` with GH_TOKEN.

---

## Project-Specific Warnings

- **HTTP timeouts**: All HTTP clients MUST have explicit `timeout=` parameter
- **Bare except**: Never use bare `except:` - always catch specific exceptions
- **Generic Exception**: Avoid `except Exception` in production code (BLE001)
- **Process spawning**: Be cautious with subprocess calls - see `~/.claude/CLAUDE.md` for process storm prevention

---

## Claude Code Skills

For detailed usage guidance, see [docs/skills/](docs/skills/):

- **dsm-usage** - DataSourceManager API usage with FCP
- **dsm-testing** - Testing patterns and pytest markers

---

## Related Documentation

- @README.md - Installation and basic usage
- @docs/INDEX.md - Documentation navigation hub
- @examples/sync/README.md - CLI demo documentation
- @examples/lib_module/README.md - Library usage examples
