# Data Source Manager

Professional market data integration with Failover Control Protocol (FCP).

## Navigation

| Topic              | Document                                                 |
| ------------------ | -------------------------------------------------------- |
| Documentation Hub  | [docs/INDEX.md](docs/INDEX.md)                           |
| Examples           | [examples/](examples/) ([CLAUDE.md](examples/CLAUDE.md)) |
| Tests              | [tests/](tests/) ([CLAUDE.md](tests/CLAUDE.md))          |
| Claude Code Config | [.claude/settings.md](.claude/settings.md)               |
| ADRs               | [docs/adr/](docs/adr/)                                   |
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

## Verification (CRITICAL)

**YOU MUST verify work before considering any task complete.**

```bash
# 1. Lint check (must pass)
uv run -p 3.13 ruff check --fix .

# 2. Unit tests (must pass)
uv run -p 3.13 pytest tests/unit/ -v

# 3. Import check (must succeed)
uv run -p 3.13 python -c "from data_source_manager import DataSourceManager; print('OK')"
```

For data-related changes, also verify:

- Timestamps are UTC (`datetime.now(timezone.utc)`)
- Symbol format matches market type
- FCP fallback behavior works as expected

---

## Common Mistakes to Avoid

- **HTTP timeouts**: All HTTP clients MUST have explicit `timeout=` parameter
- **Bare except**: Never use bare `except:` - always catch specific exceptions
- **Generic Exception**: Avoid `except Exception` in production code (BLE001)
- **Process spawning**: Be cautious with subprocess calls - see `~/.claude/CLAUDE.md` for process storm prevention
- **Naive datetime**: Always use `datetime.now(timezone.utc)`, never `datetime.now()`
- **Wrong symbol format**: BTCUSDT for spot/futures, BTCUSD_PERP for coin-margined

---

## Claude Code Skills

For detailed usage guidance, see @docs/skills/:

- @docs/skills/dsm-usage/SKILL.md - DataSourceManager API usage with FCP
- @docs/skills/dsm-testing/SKILL.md - Testing patterns and pytest markers
- @docs/skills/dsm-research/SKILL.md - Codebase research (runs in subagent)
- @docs/skills/dsm-fcp-monitor/SKILL.md - FCP monitoring and diagnostics

---

## Context Rules

Domain-specific rules in `.claude/rules/` (Claude loads on demand):

- @.claude/rules/binance-api.md - Rate limits, error codes, API endpoints
- @.claude/rules/timestamp-handling.md - UTC requirements, open_time semantics
- @.claude/rules/dataframe-operations.md - Polars preference, OHLCV validation
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
| test-writer           | Writes tests following DSM patterns    |
| data-fetcher          | Fetches data with proper FCP handling  |

---

## Custom Commands

Slash commands in `.claude/commands/`:

| Command        | Purpose                           |
| -------------- | --------------------------------- |
| /debug-fcp     | Debug FCP behavior for a symbol   |
| /quick-test    | Run quick verification tests      |
| /review-dsm    | Review code against DSM patterns  |
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

## Recent Lessons Learned

**2026-01-30**: PreToolUse hooks validate commands BEFORE execution (exit 2 blocks). [Hooks README](/.claude/hooks/README.md)

**2026-01-30**: Domain-specific CLAUDE.md files (examples/, tests/) load lazily for context isolation. [Design Spec](/docs/design/2026-01-30-claude-code-infrastructure/spec.md)

**2026-01-30**: Skills need `user-invocable: true` and `$ARGUMENTS` placeholder for slash command usage. [Anthropic Best Practices](https://platform.claude.com/docs/en/agents-and-tools/agent-skills/best-practices)

**2025-01-30**: FCP priority is Cache → Vision → REST. Vision has ~48h delay for new data. [FCP ADR](/docs/adr/2025-01-30-failover-control-protocol.md)

---

## Related Documentation

- @README.md - Installation and basic usage
- @docs/INDEX.md - Documentation navigation hub
- @docs/TROUBLESHOOTING.md - Common issues and solutions
- @docs/GLOSSARY.md - Domain terminology
- @examples/sync/README.md - CLI demo documentation
- @examples/lib_module/README.md - Library usage examples
