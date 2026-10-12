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
| `publish-to-pypi.sh`     | PyPI publishing (run by `release.sh`)     |
| `claude-code-migrate.sh` | Claude Code session migration for renames |

---

## Development Scripts (dev/)

Located in `scripts/dev/`:

- Ruff linting and formatting
- Vulture dead code detection
- pytest-xdist parallel test execution
- rope refactoring utilities

---

## Preferred: Use moon Tasks

Repo tasks live in `moon.yml` (`moon query tasks` lists them):

```bash
moon run ckvd:test      # Run unit tests
moon run ckvd:check     # Full offline gate
moon run ckvd:release   # Full release: scripts/release.sh (preflight, semantic-release, verify, PyPI, postflight)
```

---

## Related

- @moon.yml - Task definitions
- @release.sh - Release orchestrator (phases in `release/`)
- @docs/benchmarks/ - Performance benchmark scripts
