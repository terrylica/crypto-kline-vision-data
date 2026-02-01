# Data Source Manager

Professional market data integration with Failover Control Protocol (FCP).

## Navigation

| Topic              | Document                                                 |
| ------------------ | -------------------------------------------------------- |
| Source Code        | [src/](src/) ([CLAUDE.md](src/CLAUDE.md))                |
| Documentation      | [docs/](docs/) ([CLAUDE.md](docs/CLAUDE.md))             |
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

## Session Management

**Context is the primary constraint.** Performance degrades as context fills (~20k baseline, ~180k for work).

| Workflow       | When to Use             | How                                      |
| -------------- | ----------------------- | ---------------------------------------- |
| `/clear`       | Between unrelated tasks | Reset context completely                 |
| `/compact X`   | Long task, need focus   | Summarize with specific focus            |
| Document-Clear | Complex multi-step task | Dump progress to markdown, then `/clear` |
| Subagent       | Exploration/research    | Task tool keeps main context clean       |

**Rules of thumb**:

- After 2 failed corrections → `/clear` and rewrite prompt
- Investigation without scope fills context → always bound searches
- Run `/context` mid-session to monitor token usage

**Proactive delegation**: Agents with "Use proactively" in description auto-trigger.

**Personal preferences**: Use `CLAUDE.local.md` (gitignored) for individual settings.

---

## Recent Lessons Learned

**2026-02-01**: DRY audit identified consolidation opportunities - dual logger systems, cache function wrappers, scattered symbol validation. [RESUME.md](/docs/RESUME.md)

**2026-02-01**: Example files must use `data_source_manager.` prefix imports, not relative imports from `__init__`.

**2026-01-30**: Permission patterns in `.claude/settings.json` - allow/deny rules with gitignore syntax. [Official Docs](https://code.claude.com/docs/en/settings)

**2026-01-30**: Path-specific rules load via `paths:` frontmatter when working with matching files. [Design Spec](/docs/design/2026-01-30-claude-code-infrastructure/spec.md)

**2026-01-30**: 5 hooks (SessionStart, UserPromptSubmit, PreToolUse, PostToolUse, Stop). [README](/.claude/hooks/README.md)

**2026-01-30**: Lazy-loaded CLAUDE.md in subdirs (src/, docs/, examples/, tests/).

**2025-01-30**: FCP priority is Cache → Vision → REST. [FCP ADR](/docs/adr/2025-01-30-failover-control-protocol.md)

---

## Related

@README.md | @docs/INDEX.md | @docs/TROUBLESHOOTING.md | @docs/GLOSSARY.md
