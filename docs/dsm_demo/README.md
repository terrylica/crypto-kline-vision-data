# dsm_demo: Failover Control Protocol (FCP)

This CLI tool retrieves data from multiple sources:

1. **Cache (Local Arrow files)**
2. **VISION API**
3. **REST API**

It displays real-time source information about where each data point comes from.

## Usage

```bash
dsm_demo [OPTIONS]
```

## Options

- **`-p, --provider [binance]`**: Data provider (binance, tradestation)  [default: binance]
- **`-m, --market [spot|um|cm]`**: Market type (spot, um, cm)  [default: spot]
- **`-ct, --chart-type [klines|fundingRate]`**: Chart type (klines, premiums)  [default: klines]
- **`-s, --symbol TEXT`**: Symbol to fetch data for  [default: BTCUSDT]
- **`-i, --interval TEXT`**: Time interval for klines/premiums  [default: 1m]
- **`-st, --start-time TEXT`**: Start time in ISO format (YYYY-MM-DDTHH:MM:SS) or YYYY-MM-DD. Can be used alone with --days to fetch forward, or with --end-time for exact range
- **`-et, --end-time TEXT`**: End time in ISO format (YYYY-MM-DDTHH:MM:SS) or YYYY-MM-DD. Can be used alone with --days to fetch backward, or with --start-time for exact range
- **`-d, --days INTEGER`**: Number of days of data to fetch. If used with --end-time, fetches data backward from end time. If used with --start-time, fetches data forward from start time. If used alone, fetches data backward from current time  [default: 3]
- **`-es, --enforce-source [AUTO|REST|VISION]`**: Force specific data source (default: AUTO)  [default: AUTO]
- **`-r, --retries INTEGER`**: Maximum number of retry attempts  [default: 3]
- **`-nc, --no-cache`**: Disable caching (cache is enabled by default)
- **`-cc, --clear-cache`**: Clear the cache directory before running
- **`-gd, --gen-doc`**: Generate Markdown documentation from Typer help into docs/dsm_demo/ directory
- **`-glc, --gen-lint-config`**: Generate markdown linting configuration files along with documentation (only used with --gen-doc)
- **`-l, --log-level [D|I|W|E|C]`**: Set the log level (default: I). D, I, W, E, C  [default: I]
- **`--help`**: Show this message and exit.
- **`-h, --help`**: Show this message and exit.

## Examples

### End Time Backward Retrieval with Log Control

#### End time with days and ERROR log level (complex case)

```bash
./examples/sync/dsm_demo.py -s BTCUSDT -et 2025-04-14T15:59:59 -i 3m -d 5 -l E
```

### Time Range CLI Examples

#### End time with days (fetch backward from end time)

```bash
./examples/sync/dsm_demo.py -s BTCUSDT -et 2025-04-15 -d 7
```

#### Start time with days (fetch forward from start time)

```bash
./examples/sync/dsm_demo.py -s BTCUSDT -st 2025-04-05 -d 10
```

#### Exact time range (start time to end time)

```bash
./examples/sync/dsm_demo.py -s BTCUSDT -st 2025-04-05 -et 2025-04-15
```

#### Days only (fetch backward from current time)

```bash
./examples/sync/dsm_demo.py -s BTCUSDT -d 7
```

#### Default (3 days backward from current time)

```bash
./examples/sync/dsm_demo.py -s BTCUSDT
```

### Market Types

```bash
./examples/sync/dsm_demo.py -s BTCUSDT -m um
./examples/sync/dsm_demo.py -s BTCUSD_PERP -m cm
```

### Data Provider Options

```bash
./examples/sync/dsm_demo.py -s BTCUSDT -p binance
./examples/sync/dsm_demo.py -s BTCUSDT -p tradestation
```

### Different Intervals

```bash
./examples/sync/dsm_demo.py -s BTCUSDT -i 5m
./examples/sync/dsm_demo.py -s BTCUSDT -i 1h
./examples/sync/dsm_demo.py -s SOLUSDT -m spot -i 1s  -cc -l D -st 2025-04-14T15:31:01 -et 2025-04-14T15:32:01
```

### Data Source Options

```bash
./examples/sync/dsm_demo.py -s BTCUSDT -es REST
./examples/sync/dsm_demo.py -s BTCUSDT -nc
./examples/sync/dsm_demo.py -s BTCUSDT -cc
```

### Documentation Generation

#### Generate documentation

```bash
./examples/sync/dsm_demo.py -gd
```

#### Generate documentation with linting configuration files

```bash
./examples/sync/dsm_demo.py -gd -glc
```

### Combined Examples

```bash
./examples/sync/dsm_demo.py -s ETHUSDT -m um -i 15m -st 2025-04-01 -et 2025-04-10 -r 5 -l D
./examples/sync/dsm_demo.py -s ETHUSD_PERP -m cm -i 5m -d 10 -l D -cc
./examples/sync/dsm_demo.py -s BTCUSDT -p binance -es VISION -m spot -i 1m -st 2025-04-01 -et 2025-04-03
```
