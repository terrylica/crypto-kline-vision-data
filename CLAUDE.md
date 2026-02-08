# Crypto Kline Vision Data

Professional market data integration with Failover Control Protocol (FCP).

**Current Version**: See [GitHub Releases](https://github.com/terrylica/crypto-kline-vision-data/releases)

## Navigation

| Topic              | Document                                                 |
| ------------------ | -------------------------------------------------------- |
| Source Code        | [src/](src/) ([CLAUDE.md](src/CLAUDE.md))                |
| Documentation      | [docs/](docs/) ([CLAUDE.md](docs/CLAUDE.md))             |
| Examples           | [examples/](examples/) ([CLAUDE.md](examples/CLAUDE.md)) |
| Tests              | [tests/](tests/) ([CLAUDE.md](tests/CLAUDE.md))          |
| Scripts            | [scripts/](scripts/) ([CLAUDE.md](scripts/CLAUDE.md))    |
| Claude Code Config | [.claude/settings.md](.claude/settings.md)               |
| ADRs               | [docs/adr/](docs/adr/)                                   |
| Benchmarks         | [docs/benchmarks/](docs/benchmarks/README.md)            |
| Troubleshooting    | [docs/TROUBLESHOOTING.md](docs/TROUBLESHOOTING.md)       |

## Essential Commands

| Command                    | Purpose                    |
| -------------------------- | -------------------------- |
| `mise run test`            | Run unit tests             |
| `mise run check:all`       | Lint + format + typecheck  |
| `mise run quick`           | Quick validation           |
| `mise run claude:validate` | Validate Claude Code setup |
| `mise run release:dry`     | Preview semantic-release   |

Run `mise run help` for full task list.

---

## Python Version Policy (CRITICAL)

**Python 3.13 ONLY. Never use 3.14 or any other version.**

- All `uv run` commands: use `--python 3.13` or `-p 3.13`
- Never change Python version in `mise.toml`, `.python-version`, or `pyproject.toml`
- If a tool requires a different Python version, stop and ask

---

## Code Style

- **Imports**: Use absolute imports with `ckvd.` prefix
- **Type hints**: Required for all public functions
- **Docstrings**: Google style (enforced by ruff)
- **Line length**: 120 characters max
- **Formatting**: ruff format (replaces black)

---

## Package Architecture

```
src/ckvd/
├── core/
│   ├── sync/                  # Synchronous data managers
│   │   ├── crypto_kline_vision_data.py   # Main CKVD class with FCP
│   │   ├── ckvd_types.py             # DataSource, CKVDConfig
│   │   └── ckvd_lib.py               # High-level fetch functions
│   └── providers/
│       └── binance/           # Binance-specific implementations
│           ├── vision_data_client.py    # Binance Vision API
│           ├── rest_data_client.py      # REST API fallback
│           └── cache_manager.py         # Arrow cache
└── utils/
    ├── market_constraints.py   # Re-export from market/ subpackage
    ├── market/                 # Enums and validation (refactored)
    │   ├── enums.py           # DataProvider, MarketType, Interval, ChartType
    │   ├── validation.py      # Symbol validation functions
    │   └── capabilities.py    # Market capabilities
    ├── loguru_setup.py         # Logging configuration
    └── for_core/              # Internal utilities (FCP helpers, exceptions)
```

**Key classes**:

- `CryptoKlineVisionData` - Main entry point with FCP
- `CKVDConfig` - Configuration for CKVD instances (in `ckvd_types.py`)
- `DataSource` - Enum for data source selection (AUTO, REST, VISION, CACHE)
- `DataProvider`, `MarketType`, `Interval` - Core enums (from `utils/market/`)

---

## Failover Control Protocol (FCP)

Data retrieval follows this priority:

1. **Cache** - Local Arrow files (fastest)
2. **Vision API** - Binance Vision on AWS S3 (bulk historical)
3. **REST API** - Binance REST (real-time, rate-limited)

Recent data (~48h) typically not in Vision API, falls through to REST.

---

## Performance

<!-- SSoT-OK: Version reference for feature availability documentation -->

| Comparison             | Winner    | Speedup   | Memory Savings |
| ---------------------- | --------- | --------- | -------------- |
| Polars vs Pandas FCP   | Polars    | **6.35x** | **92.1%**      |
| Streaming vs In-Memory | Streaming | **1.42x** | **49.5%**      |

See [docs/benchmarks/](docs/benchmarks/README.md) for full benchmark data.

---

## API Boundary Output

**Default output: Pandas** (backward compatible)

```python
# Default: Returns pd.DataFrame
df = manager.get_data("BTCUSDT", start, end, Interval.HOUR_1)

# Opt-in Polars: Returns pl.DataFrame (zero-copy, faster)
df = manager.get_data("BTCUSDT", start, end, Interval.HOUR_1, return_polars=True)
```

| Parameter             | Default | Output Type    | Use Case               |
| --------------------- | ------- | -------------- | ---------------------- |
| `return_polars=False` | Yes     | `pd.DataFrame` | Backward compatibility |
| `return_polars=True`  | No      | `pl.DataFrame` | Maximum performance    |

**Internal processing**: Always Polars (LazyFrames + streaming engine)

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
uv run -p 3.13 python -c "from ckvd import CryptoKlineVisionData; print('OK')"
```

**Required for release**: Create `.mise.local.toml` from `.mise.local.toml.example` with GH_TOKEN.

---

## Verification (CRITICAL)

**YOU MUST verify work before considering any task complete.**

```bash
# 1. Lint check (must pass)
uv run -p 3.13 ruff check --fix .

# 2. Unit tests (must pass)
uv run -p 3.13 pytest tests/unit/ -v

# 3. Import check (must succeed)
uv run -p 3.13 python -c "from ckvd import CryptoKlineVisionData; print('OK')"
```

For data-related changes, also verify:

- Timestamps are UTC (`datetime.now(timezone.utc)`)
- Symbol format matches market type
- FCP fallback behavior works as expected

---

## Pattern Preferences

Use positive alternatives instead of prohibitions:

| Instead of                      | Prefer                            |
| ------------------------------- | --------------------------------- |
| `requests.get(url)`             | `httpx.get(url, timeout=30)`      |
| `except:` or `except Exception` | `except SpecificError as e:`      |
| `datetime.now()`                | `datetime.now(timezone.utc)`      |
| Manual symbol strings           | `MarketType` enum validation      |
| Subprocess without check        | `subprocess.run(..., check=True)` |

See `~/.claude/CLAUDE.md` for process storm prevention with subprocess calls.

---

## Claude Code Skills

For detailed usage guidance, see @docs/skills/:

- @docs/skills/ckvd-usage/SKILL.md - CryptoKlineVisionData API usage with FCP
- @docs/skills/ckvd-testing/SKILL.md - Testing patterns and pytest markers
- @docs/skills/ckvd-research/SKILL.md - Codebase research (runs in subagent)
- @docs/skills/ckvd-fcp-monitor/SKILL.md - FCP monitoring and diagnostics

---

## Context Rules

Domain-specific rules in `.claude/rules/` (Claude loads on demand):

- @.claude/rules/binance-api.md - Rate limits, error codes, API endpoints
- @.claude/rules/timestamp-handling.md - UTC requirements, open_time semantics
- @.claude/rules/dataframe-operations.md - DataFrame handling, OHLCV validation
- @.claude/rules/caching-patterns.md - Cache structure, invalidation rules
- @.claude/rules/symbol-formats.md - Market-specific symbol format requirements
- @.claude/rules/error-handling.md - Exception hierarchy, recovery patterns
- @.claude/rules/fcp-protocol.md - FCP decision logic, debugging

---

## Custom Agents

Specialized subagents for delegation (in `.claude/agents/`):

| Agent                 | Purpose                                |
| --------------------- | -------------------------------------- |
| silent-failure-hunter | Finds silent failures and bare excepts |
| fcp-debugger          | Diagnoses FCP issues                   |
| api-reviewer          | Reviews code for API consistency       |
| test-writer           | Writes tests following CKVD patterns   |
| data-fetcher          | Fetches data with proper FCP handling  |

---

## Custom Commands

Slash commands in `.claude/commands/`:

| Command        | Purpose                           |
| -------------- | --------------------------------- |
| /debug-fcp     | Debug FCP behavior for a symbol   |
| /quick-test    | Run quick verification tests      |
| /review-ckvd   | Review code against CKVD patterns |
| /fetch-data    | Fetch market data with validation |
| /validate-data | Validate DataFrame structure      |
| /feature-dev   | Guided feature development        |

---

## Link Conventions

| Target         | Format          | Example                          |
| -------------- | --------------- | -------------------------------- |
| This repo docs | Repo-root (`/`) | `[ADR](/docs/adr/file.md)`       |
| Skill-internal | Relative (`./`) | `[Guide](./references/guide.md)` |
| External       | Full URL        | `[Docs](https://example.com)`    |

---

## Related

@README.md | @docs/INDEX.md | @docs/TROUBLESHOOTING.md | @docs/GLOSSARY.md
