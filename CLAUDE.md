# Crypto Kline Vision Data (CKVD)

High-performance market data integration with Failover Control Protocol (FCP).

**Package**: `crypto-kline-vision-data` (PyPI) | **Import**: `ckvd` | **Class**: `CryptoKlineVisionData`

**Version**: See [GitHub Releases](https://github.com/terrylica/crypto-kline-vision-data/releases)

---

## Hub Navigation

Each directory has its own CLAUDE.md with domain-specific context, loaded on demand.

| Directory     | CLAUDE.md                                    | Owns                                                                         |
| ------------- | -------------------------------------------- | ---------------------------------------------------------------------------- |
| `src/`        | [src/CLAUDE.md](src/CLAUDE.md)               | Package structure, FCP, exceptions, `__probe__`, symbol security, timestamps |
| `tests/`      | [tests/CLAUDE.md](tests/CLAUDE.md)           | Test commands, markers, fixtures, mocking                                    |
| `docs/`       | [docs/CLAUDE.md](docs/CLAUDE.md)             | ADRs, skills, benchmarks, troubleshooting                                    |
| `examples/`   | [examples/CLAUDE.md](examples/CLAUDE.md)     | Example conventions, NDJSON telemetry                                        |
| `scripts/`    | [scripts/CLAUDE.md](scripts/CLAUDE.md)       | Dev scripts, mise tasks, cache tools, Vision data utilities                  |
| `playground/` | [playground/CLAUDE.md](playground/CLAUDE.md) | Experimental prototypes (not production)                                     |

**Also**: [.claude/settings.md](.claude/settings.md) | [docs/INDEX.md](docs/INDEX.md) | [docs/GLOSSARY.md](docs/GLOSSARY.md) | [docs/TROUBLESHOOTING.md](docs/TROUBLESHOOTING.md)

---

## Critical Policies

### Python Version (CRITICAL)

**Python 3.13 ONLY. Never use 3.14 or any other version.**

- All `uv run` commands: use `-p 3.13`
- Never change version in `mise.toml`, `.python-version`, or `pyproject.toml`

### Verification (CRITICAL)

**YOU MUST verify work before considering any task complete.**

```bash
uv run -p 3.13 ruff check --fix .                                         # Lint
uv run -p 3.13 pytest tests/unit/ -v                                      # Unit tests
uv run -p 3.13 python -c "from ckvd import CryptoKlineVisionData; print('OK')"  # Import
```

---

## Quick Reference

### Essential Commands

| Command                | Purpose                   |
| ---------------------- | ------------------------- |
| `mise run test`        | Run unit tests            |
| `mise run check:all`   | Lint + format + typecheck |
| `mise run quick`       | Quick validation          |
| `mise run release:dry` | Preview semantic-release  |

### FCP Priority

Cache (~1ms) → Vision API (~1-5s) → REST API (~100-500ms)

Recent data (~48h) not in Vision API, falls through to REST. See [src/CLAUDE.md](src/CLAUDE.md) for implementation details.

### API Boundary

```python
# Default: pd.DataFrame (backward compatible)
df = manager.get_data("BTCUSDT", start, end, Interval.HOUR_1)

# Opt-in: pl.DataFrame (zero-copy, faster)
df = manager.get_data("BTCUSDT", start, end, Interval.HOUR_1, return_polars=True)
```

Internal processing always uses Polars (LazyFrames + streaming engine).

### AI Agent Discovery

```python
from ckvd.__probe__ import discover_api, get_capabilities
```

Stateless JSON-serializable API introspection. See [src/CLAUDE.md](src/CLAUDE.md#ai-agent-introspection-__probe__py) for details.

### Cache Toggle

`use_cache=False` disables cache read/write. `CKVD_ENABLE_CACHE=false` env var also works. `enforce_source=CACHE` + `use_cache=False` raises `RuntimeError`. See [docs/TROUBLESHOOTING.md](docs/TROUBLESHOOTING.md#cache-toggle-issues).

### Exception `.details`

All exceptions carry `.details: dict[str, Any]` (default `{}`, never `None`). Machine-parseable error context. See [src/CLAUDE.md](src/CLAUDE.md#exception-hierarchy).

### Environment Variables

| Variable                 | Purpose                      | Default |
| ------------------------ | ---------------------------- | ------- |
| `CKVD_LOG_LEVEL`         | Log level (DEBUG/INFO/ERROR) | ERROR   |
| `CKVD_ENABLE_CACHE`      | Enable/disable Arrow cache   | true    |
| `CKVD_USE_POLARS_OUTPUT` | Default to Polars output     | false   |

### Security

Symbol inputs validated against `^[A-Z0-9_-]{1,30}$` — rejects path traversal, null bytes, slashes. See [src/CLAUDE.md](src/CLAUDE.md#symbol-formats).

### Code Style

- **Imports**: Absolute with `ckvd.` prefix
- **Type hints**: Required for all public functions
- **Formatting**: `ruff format` (140 char line length)
- **Timestamps**: Always `datetime.now(timezone.utc)` — never naive

### Pattern Preferences

| Instead of                      | Prefer                            |
| ------------------------------- | --------------------------------- |
| `requests.get(url)`             | `httpx.get(url, timeout=30)`      |
| `except:` or `except Exception` | `except SpecificError as e:`      |
| `datetime.now()`                | `datetime.now(timezone.utc)`      |
| Manual symbol strings           | `MarketType` enum validation      |
| Subprocess without check        | `subprocess.run(..., check=True)` |

---

## Environment Setup

```bash
uv sync --dev                    # Install dependencies
mise trust                       # Load env (GH_TOKEN from .mise.local.toml)
```

**Release**: semantic-release v25+ (global npm install, no per-repo node_modules). Config: `.releaserc.yml`. Run: `mise run release:full` / `mise run release:dry`. Requires GH_TOKEN in `.mise.local.toml`. CHANGELOG.md is auto-generated.

**Commit trailers** (hook-enforced):

```
SRED-Type: experimental-development | applied-research | basic-research | support-work
SRED-Claim: CKVD
```

---

## Claude Code Extensions

### Skills (progressive disclosure in `docs/skills/`)

| Skill       | Guide                                  |
| ----------- | -------------------------------------- |
| CKVD usage  | @docs/skills/ckvd-usage/SKILL.md       |
| Testing     | @docs/skills/ckvd-testing/SKILL.md     |
| Research    | @docs/skills/ckvd-research/SKILL.md    |
| FCP monitor | @docs/skills/ckvd-fcp-monitor/SKILL.md |

### Agents (`.claude/agents/`)

| Agent                 | Purpose                                |
| --------------------- | -------------------------------------- |
| silent-failure-hunter | Finds silent failures and bare excepts |
| fcp-debugger          | Diagnoses FCP issues                   |
| api-reviewer          | Reviews code for API consistency       |
| test-writer           | Writes tests following CKVD patterns   |
| data-fetcher          | Fetches data with proper FCP handling  |

### Commands (`.claude/commands/`)

`/review-ckvd` | `/feature-dev`

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
