# Crypto Kline Vision Data

High-performance market data integration with Failover Control Protocol (FCP).

**Package**: `crypto-kline-vision-data` | **Import**: `ckvd` | **Python**: 3.13

[![GitHub Release](https://img.shields.io/github/v/release/terrylica/crypto-kline-vision-data)](https://github.com/terrylica/crypto-kline-vision-data/releases)

## Features

- **Failover Control Protocol (FCP)**: Cache (~1ms) → Vision API (~1-5s) → REST API (~100-500ms) — automatic failover, retry, and gap detection
- **Apache Arrow Cache**: Memory-mapped local cache for instant repeated access
- **Binance Vision API**: Bulk historical data from AWS S3 (no rate limits, ~48h delay)
- **Binance REST API**: Real-time data with built-in rate limit handling ([Spot](https://developers.binance.com/docs/binance-spot-api-docs/rest-api/general-endpoints), [USDS-M Futures](https://developers.binance.com/docs/derivatives/usds-margined-futures/general-info), [Coin-M Futures](https://developers.binance.com/docs/derivatives/coin-margined-futures/general-info))
- **Polars Engine**: Internal Polars LazyFrames + streaming; pandas or Polars output at API boundary
- **AI Agent Introspection**: `__probe__.py` module for stateless API discovery
- **Security**: Symbol validation (CWE-22 path traversal prevention)
- **Machine-Parseable Errors**: All exceptions carry `.details` dict

## Quick Start

```bash
git clone https://github.com/terrylica/crypto-kline-vision-data.git
cd crypto-kline-vision-data
uv sync --dev
```

```python
from ckvd import CryptoKlineVisionData, DataProvider, MarketType, Interval
from datetime import datetime, timedelta, timezone

manager = CryptoKlineVisionData.create(DataProvider.BINANCE, MarketType.FUTURES_USDT)

end = datetime.now(timezone.utc)
start = end - timedelta(days=7)

df = manager.get_data("BTCUSDT", start, end, Interval.HOUR_1)
print(f"Loaded {len(df)} bars")
manager.close()
```

Or as a Git dependency in your `pyproject.toml`:

```toml
dependencies = [
    "crypto-kline-vision-data @ git+https://github.com/terrylica/crypto-kline-vision-data.git"
]
```

## For Claude Code Users

This repository is optimized for [Claude Code](https://docs.anthropic.com/en/docs/claude-code) with a **hub-and-spoke CLAUDE.md architecture**. When Claude Code opens any directory, it automatically loads that directory's `CLAUDE.md` — giving it domain-specific context, conventions, and deep links to related documentation.

**Start here**: [`CLAUDE.md`](CLAUDE.md) (root hub) — then Claude Code discovers everything else autonomously.

### Hub-and-Spoke Architecture

```
CLAUDE.md (root hub)
├── src/CLAUDE.md         → Package structure, FCP, exceptions, __probe__, security
├── tests/CLAUDE.md       → Test commands, markers, fixtures, mocking patterns
├── docs/CLAUDE.md        → ADRs, skills, benchmarks, troubleshooting
├── examples/CLAUDE.md    → Example conventions, NDJSON telemetry schema
├── scripts/CLAUDE.md     → Dev scripts, mise tasks, cache tools
└── playground/CLAUDE.md  → Experimental prototypes
```

Each spoke contains only the context relevant to that directory. Claude Code loads them on demand — no context window waste.

### What Claude Code Gets

- **Skills** in `docs/skills/` — progressive disclosure guides for usage, testing, research, FCP monitoring
- **Agents** in `.claude/agents/` — specialized subagents (API reviewer, test writer, FCP debugger, silent failure hunter)
- **Commands** in `.claude/commands/` — `/review-ckvd`, `/feature-dev`
- **Full API surface** via `from ckvd.__probe__ import discover_api` — JSON-serializable metadata for agent introspection

### Tips for Working with Claude Code

1. **Just ask** — Claude Code reads the relevant CLAUDE.md files automatically when you work in a directory
2. **Use skills** — ask Claude to "fetch BTCUSDT data" or "run tests" and it discovers the right patterns
3. **Use agents** — `@silent-failure-hunter` to audit code, `@test-writer` to generate tests
4. **Use probe** — `from ckvd.__probe__ import discover_api` for programmatic API discovery

## Examples

```bash
# Via mise tasks
mise run demo:quickstart          # Minimal FCP usage
mise run demo:features            # Feature engineering pipeline
mise run demo:cache               # Cache toggle mechanisms
mise run demo:logging             # Logging configuration
mise run demo:datetime            # Timezone handling
mise run demo:one-second          # 1s interval (SPOT only)
mise run demo:lazy                # Lazy initialization

# Or directly
uv run -p 3.13 python examples/quick_start.py
```

All examples emit **NDJSON telemetry** to `examples/logs/events.jsonl`. See [examples/CLAUDE.md](examples/CLAUDE.md) for schema and parsing.

## API Reference

### Core API

```python
from ckvd import CryptoKlineVisionData, DataProvider, MarketType, Interval

# Manager-based (recommended)
manager = CryptoKlineVisionData.create(DataProvider.BINANCE, MarketType.FUTURES_USDT)
df = manager.get_data("BTCUSDT", start, end, Interval.HOUR_1)
manager.close()

# High-level function
from ckvd import fetch_market_data, ChartType
df, elapsed, count = fetch_market_data(
    provider=DataProvider.BINANCE, market_type=MarketType.SPOT,
    chart_type=ChartType.KLINES, symbol="BTCUSDT",
    interval=Interval.HOUR_1, start_time=start, end_time=end
)
```

### Market Types and Symbols

| Market Type    | Symbol Format    | Example     |
| -------------- | ---------------- | ----------- |
| `SPOT`         | `{BASE}{QUOTE}`  | BTCUSDT     |
| `FUTURES_USDT` | `{BASE}{QUOTE}`  | BTCUSDT     |
| `FUTURES_COIN` | `{BASE}USD_PERP` | BTCUSD_PERP |

### Output Formats

```python
# Default: pandas DataFrame (backward compatible)
df = manager.get_data("BTCUSDT", start, end, Interval.HOUR_1)

# Opt-in: Polars DataFrame (zero-copy, faster)
df = manager.get_data("BTCUSDT", start, end, Interval.HOUR_1, return_polars=True)
```

### Error Handling

```python
from ckvd.utils.for_core.rest_exceptions import RateLimitError, RestAPIError
from ckvd.utils.for_core.vision_exceptions import VisionAPIError

try:
    df = manager.get_data(...)
except RateLimitError as e:
    print(f"Rate limited, retry after {e.retry_after}s. Details: {e.details}")
except (RestAPIError, VisionAPIError) as e:
    print(f"Error: {e}. Details: {e.details}")
```

### Environment Variables

| Variable                 | Purpose                      | Default |
| ------------------------ | ---------------------------- | ------- |
| `CKVD_LOG_LEVEL`         | Log level (DEBUG/INFO/ERROR) | ERROR   |
| `CKVD_ENABLE_CACHE`      | Enable/disable cache         | true    |
| `CKVD_USE_POLARS_OUTPUT` | Zero-copy Polars output      | false   |

## Development

```bash
uv sync --dev                    # Install dependencies
mise trust                       # Load environment
uv run -p 3.13 pytest tests/unit/ -v   # Run tests (399 passing)
uv run -p 3.13 ruff check --fix .      # Lint
```

See [CLAUDE.md](CLAUDE.md) for full development conventions, commit trailers, and release workflow.

## Documentation

| Resource                                           | Purpose                                 |
| -------------------------------------------------- | --------------------------------------- |
| [CLAUDE.md](CLAUDE.md)                             | Root hub — start here for Claude Code   |
| [docs/INDEX.md](docs/INDEX.md)                     | Documentation navigation                |
| [docs/TROUBLESHOOTING.md](docs/TROUBLESHOOTING.md) | Common issues and solutions             |
| [docs/GLOSSARY.md](docs/GLOSSARY.md)               | Domain terminology                      |
| [examples/](examples/)                             | Runnable examples with NDJSON telemetry |
| [CHANGELOG.md](CHANGELOG.md)                       | Release history (auto-generated)        |

## License

MIT License — See [LICENSE](LICENSE) file for details.
