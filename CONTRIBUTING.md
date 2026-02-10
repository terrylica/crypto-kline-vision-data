# Contributing to Crypto Kline Vision Data

Thank you for your interest in contributing! This guide will help you get started.

## Development Setup

### Prerequisites

- Python 3.13 (enforced via `.python-version`)
- [uv](https://docs.astral.sh/uv/) for Python package management
- [mise](https://mise.jdx.dev/) for task orchestration
- Node.js 25+ (for semantic-release)

### Initial Setup

```bash
# Clone the repository
git clone https://github.com/terrylica/crypto-kline-vision-data.git
cd crypto-kline-vision-data

# Install dependencies
uv sync --dev

# Trust mise configuration
mise trust

# Verify setup
uv run -p 3.13 python -c "from ckvd import CryptoKlineVisionData; print('OK')"
```

## Development Workflow

### Running Tests

```bash
# Unit tests (fast, no network)
mise run test

# All tests
mise run test:all

# Integration tests (requires network)
mise run test:integration

# Test with coverage report
mise run test:cov
```

### Code Quality

```bash
# Lint and auto-fix
mise run lint

# Format code
mise run format

# Type checking
mise run typecheck

# Run all checks
mise run check:all
```

### Making Changes

1. Create a feature branch: `git checkout -b feat/my-feature`
2. Make your changes
3. Run checks: `mise run check:all && mise run test`
4. Commit with conventional commit format (see below)
5. Push and open a PR

## Commit Message Convention

This project uses [Conventional Commits](https://www.conventionalcommits.org/) for automated versioning:

| Prefix                     | Version Bump  | Example                               |
| -------------------------- | ------------- | ------------------------------------- |
| `feat:`                    | Minor (0.X.0) | `feat: add OKX provider support`      |
| `fix:`                     | Patch (0.0.X) | `fix: handle timeout in REST client`  |
| `BREAKING CHANGE:` trailer | Major (X.0.0) | Body trailer: `BREAKING CHANGE: ...`  |
| `docs:`                    | No release    | `docs: update API documentation`      |
| `chore:`                   | No release    | `chore: update dependencies`          |
| `refactor:`                | No release    | `refactor: simplify cache logic`      |
| `test:`                    | No release    | `test: add coverage for gap detector` |

### SRED Trailers (Required)

All commits must include SR&ED trailers for tax credit compliance:

```
feat: implement new feature

Description of what was done.

SRED-Type: support-work
SRED-Claim: CKVD-FEATURE
```

Valid SRED-Type values:

- `experimental-development` - Technological advancement through systematic work
- `applied-research` - Scientific knowledge with practical application
- `support-work` - Programming, testing, data collection supporting SR&ED

## Code Style

- **Python version**: 3.13 ONLY (never use other versions)
- **Imports**: Absolute imports with `ckvd.` prefix
- **Type hints**: Required for all public functions
- **Docstrings**: Google style
- **Line length**: 140 characters
- **Formatting**: ruff format (enforced)

## Project Structure

```
src/ckvd/
├── core/
│   ├── sync/              # Synchronous data managers (CKVD, ckvd_lib)
│   └── providers/         # Data provider implementations (Binance, etc.)
└── utils/                 # Utility modules
```

## Need Help?

- Read [CLAUDE.md](/CLAUDE.md) for project context
- Check [docs/INDEX.md](/docs/INDEX.md) for documentation navigation
- Review ADRs in [docs/adr/](/docs/adr/) for design decisions
