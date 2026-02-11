# Scripts Directory

Utility scripts for development, maintenance, and deployment.

**Hub**: [Root CLAUDE.md](../CLAUDE.md) | **Siblings**: [src/](../src/CLAUDE.md) | [tests/](../tests/CLAUDE.md) | [docs/](../docs/CLAUDE.md) | [examples/](../examples/CLAUDE.md) | [playground/](../playground/CLAUDE.md)

---

## Directory Structure

| Directory                                                           | Purpose                               |
| ------------------------------------------------------------------- | ------------------------------------- |
| `dev/`                                                              | Development utilities (linting, etc.) |
| [`arrow_cache/`](arrow_cache/CLAUDE.md)                             | Cache management and diagnostics      |
| [`binance_vision_api_aws_s3/`](binance_vision_api_aws_s3/CLAUDE.md) | Binance Vision data availability      |
| [`funding_rate_downloader/`](funding_rate_downloader/CLAUDE.md)     | Funding rate data utilities           |

## Key Scripts

| Script                   | Purpose                                   |
| ------------------------ | ----------------------------------------- |
| `publish-to-pypi.sh`     | PyPI publishing (use mise tasks)          |
| `claude-code-migrate.sh` | Claude Code session migration for renames |

---

## Development Scripts (dev/)

Located in `scripts/dev/`:

- Ruff linting and formatting
- Vulture dead code detection
- pytest-xdist parallel test execution
- rope refactoring utilities

---

## Preferred: Use mise Tasks

Most script functionality is available via mise tasks:

```bash
# Instead of running scripts directly
mise run test           # Run tests
mise run check:all      # Lint + format + typecheck
mise run release:full   # Full release (semantic-release)
```

See `mise run help` for all available tasks.

---

## Related

- @mise.toml - Task definitions
- @docs/benchmarks/ - Performance benchmark scripts
