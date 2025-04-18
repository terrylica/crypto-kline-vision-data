# fcp-demo: Failover Control Protocol

This CLI tool demonstrates the Failover Control Protocol (FCP) mechanism,
which automatically retrieves Bitcoin data from multiple sources:

1. **Cache** (Local Arrow files)
2. **VISION API**
3. **REST API**

It displays real-time source information about where each data point comes from.

## Time Range Priority Hierarchy

### 1. `--days` or `-d` flag (HIGHEST PRIORITY)

- If provided, overrides any `--start-time` and `--end-time` values
- Calculates range as `[current_time - days, current_time]`
- Example: `--days 5` will fetch data from 5 days ago until now

### 2. `--start-time` and `--end-time` (SECOND PRIORITY)

- Used only when BOTH are provided AND `--days` is NOT provided
- Defines exact time range to fetch data from
- Example: `--start-time 2025-04-10 --end-time 2025-04-15`

### 3. Default Behavior (FALLBACK)

- If neither of the above conditions are met
- Uses default `days=3` to calculate range as `[current_time - 3 days, current_time]`

## Usage

```bash
fcp-demo [OPTIONS]
```

## Options

| Option | Description |
|--------|-------------|
| `-s, --symbol TEXT` | Symbol to fetch data for  [default: BTCUSDT] |
| `-m, --market [spot\|um\|cm\|futures_usdt\|futures_coin]` | Market type (spot, um, cm)  [default: spot] |
| `-i, --interval TEXT` | Time interval for klines/premiums  [default: 1m] |
| `-ct, --chart-type [klines\|fundingRate]` | Chart type (klines, premiums)  [default: klines] |
| `-st, --start-time TEXT` | [SECOND PRIORITY] Start time in ISO format (YYYY-MM-DDTHH:MM:SS) or YYYY-MM-DD. Used only if both --start-time AND --end-time are provided AND --days is NOT provided |
| `-et, --end-time TEXT` | [SECOND PRIORITY] End time in ISO format (YYYY-MM-DDTHH:MM:SS) or YYYY-MM-DD. Used only if both --start-time AND --end-time are provided AND --days is NOT provided |
| `-d, --days INTEGER` | [HIGHEST PRIORITY] Number of days of data to fetch. If provided, overrides --start-time and --end-time  [default: 3] |
| `-es, --enforce-source [AUTO\|REST\|VISION]` | Force specific data source (default: AUTO)  [default: AUTO] |
| `-r, --retries INTEGER` | Maximum number of retry attempts  [default: 3] |
| `-nc, --no-cache` | Disable caching (cache is enabled by default) |
| `-cc, --clear-cache` | Clear the cache directory before running |
| `-fcp, --test-fcp` | Run the special test for Failover Control Protocol (FCP) mechanism |
| `-pc, --prepare-cache` | Pre-populate cache with the first segment of data (only used with --test-fcp) |
| `-gd, --gen-doc` | Generate Markdown documentation from Typer help into docs/fcp_demo/ directory |
| `-glc, --gen-lint-config` | Generate markdown linting configuration files along with documentation (only used with --gen-doc) |
| `-l, --log-level [DEBUG\|INFO\|WARNING\|ERROR\|CRITICAL\|D\|I\|W\|E\|C]` | Set the log level (default: INFO). Shorthand options: D=DEBUG, I=INFO, W=WARNING, E=ERROR, C=CRITICAL  [default: INFO] |
| `--help` | Show this message and exit. |

## Examples

### Basic Usage

```bash
./examples/dsm_sync_simple/fcp_demo.py
./examples/dsm_sync_simple/fcp_demo.py --symbol ETHUSDT --market spot
```

### Time Range Options (By Priority)

```bash
# PRIORITY 1: Using --days flag (overrides any start/end times)
./examples/dsm_sync_simple/fcp_demo.py -s BTCUSDT -d 7
  
# PRIORITY 2: Using start and end times (only if --days is NOT provided)
./examples/dsm_sync_simple/fcp_demo.py -s BTCUSDT -st 2025-04-05T00:00:00 -et 2025-04-06T00:00:00
  
# FALLBACK: No time flags (uses default days=3)
./examples/dsm_sync_simple/fcp_demo.py -s BTCUSDT
```

### Market Types

```bash
./examples/dsm_sync_simple/fcp_demo.py -s BTCUSDT -m um
./examples/dsm_sync_simple/fcp_demo.py -s BTCUSD_PERP -m cm
```

### Different Intervals

```bash
./examples/dsm_sync_simple/fcp_demo.py -s BTCUSDT -i 5m
./examples/dsm_sync_simple/fcp_demo.py -s BTCUSDT -i 1h
./examples/dsm_sync_simple/fcp_demo.py -s SOLUSDT -m spot -i 1s -cc -l D -st 2025-04-14T15:31:01 -et 2025-04-14T15:32:01
```

### Data Source Options

```bash
./examples/dsm_sync_simple/fcp_demo.py -s BTCUSDT -es REST
./examples/dsm_sync_simple/fcp_demo.py -s BTCUSDT -nc
./examples/dsm_sync_simple/fcp_demo.py -s BTCUSDT -cc
```

### Testing FCP Mechanism

```bash
./examples/dsm_sync_simple/fcp_demo.py -s BTCUSDT -fcp
./examples/dsm_sync_simple/fcp_demo.py -s BTCUSDT -fcp -pc
```

### Documentation Generation

```bash
# Generate documentation with typer-cli format (default)
./examples/dsm_sync_simple/fcp_demo.py -gd

# Generate documentation with linting configuration files
./examples/dsm_sync_simple/fcp_demo.py -gd -glc
```

### Combined Examples

```bash
./examples/dsm_sync_simple/fcp_demo.py -s ETHUSDT -m um -i 15m -st 2025-04-01 -et 2025-04-10 -r 5 -l DEBUG
./examples/dsm_sync_simple/fcp_demo.py -s ETHUSD_PERP -m cm -i 5m -d 10 -fcp -pc -l D -cc
```
