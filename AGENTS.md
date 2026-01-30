# AI Agent Instructions - Data Source Manager

> This file is synced with CLAUDE.md for Claude Code compatibility.
> For Claude Code-specific features (skills, commands, agents), see CLAUDE.md.

## Project Overview

Professional market data integration package with Failover Control Protocol (FCP) for reliable data retrieval from Binance Vision API, REST API, and local Apache Arrow cache.

## Critical Constraints

**Python 3.13 ONLY** - Never use other versions.

## Quick Commands

| Command                             | Purpose           |
| ----------------------------------- | ----------------- |
| `uv run -p 3.13 pytest tests/unit/` | Run unit tests    |
| `uv run -p 3.13 ruff check --fix .` | Lint and auto-fix |
| `uv run -p 3.13 ruff format .`      | Format code       |
| `mise run release:dry`              | Preview release   |

## Package Structure

```
src/data_source_manager/
├── core/sync/data_source_manager.py  # Main DSM class with FCP
├── core/providers/binance/           # Binance API implementations
└── utils/market_constraints.py       # Core enums
```

## Code Style

- Absolute imports: `from data_source_manager.utils import ...`
- Type hints required for public functions
- Google-style docstrings
- 120 char line length
- ruff for linting and formatting

## Common Mistakes to Avoid

1. **Missing HTTP timeout**: All HTTP calls need explicit `timeout=`
2. **Bare except**: Always catch specific exceptions
3. **Naive datetime**: Use `datetime.now(timezone.utc)`, not `datetime.now()`
4. **Wrong symbol format**: BTCUSDT for spot/futures, BTCUSD_PERP for coin-margined

## Verification Checklist

Before completing any task:

1. `uv run -p 3.13 ruff check --fix .` - Lint must pass
2. `uv run -p 3.13 pytest tests/unit/ -v` - Tests must pass
3. Import check: `python -c "from data_source_manager import DataSourceManager"`

## Key Documentation

- README.md - Installation and usage
- docs/INDEX.md - Documentation hub
- docs/adr/ - Architecture Decision Records
