# CKVD Demo Module

## Overview

This module demonstrates the programmatic use of `src/ckvd/core/sync/ckvd_lib.py` functions to fetch historical market data from Binance.

## Features

- Backward data retrieval from specified end time
- Configurable parameters (symbol, interval, time range)
- Output visualization with rich formatting
- Multiple retrieval examples

## Usage

```bash
# Run the demo
./examples/lib_module/ckvd_demo_module.py
```

## Example Code

```python
from ckvd.ckvd.core.sync.ckvd_lib import (
    setup_environment,
    process_market_parameters,
    fetch_market_data,
)

# Configure parameters
symbol = "BTCUSDT"
end_time = "2025-04-14T15:59:59"
interval = "1m"
days = 10

# Process market parameters
provider_enum, market_type, chart_type_enum, symbol, interval_enum = (
    process_market_parameters(
        provider="binance",
        market="spot",
        chart_type="klines",
        symbol=symbol,
        interval=interval,
    )
)

# Fetch data
df, elapsed_time, records = fetch_market_data(
    provider=provider_enum,
    market_type=market_type,
    chart_type=chart_type_enum,
    symbol=symbol,
    interval=interval_enum,
    end_time=end_time,
    days=days,
)
```

## Related Tools

This module complements the CLI tool (`examples/sync/ckvd_demo_cli.py`) by providing a programmatic interface for the same functionality.
