# Binance Data Service

A high-performance, robust package for efficient market data retrieval from multiple data providers, including [Binance Vision](https://data.binance.vision/) and Binance REST ([Spot](https://developers.binance.com/docs/binance-spot-api-docs/rest-api/general-endpoints), [USDS-Margined Futures](https://developers.binance.com/docs/derivatives/usds-margined-futures/general-info), [Coin-Margined Futures](https://developers.binance.com/docs/derivatives/coin-margined-futures/general-info)) using Apache Arrow MMAP for optimal performance.

## Important Documentation

- **[DSM Demo Documentation](docs/dsm_demo/README.md)**: Interactive demonstration of the Failover Control Protocol mechanism, the core data retrieval strategy that ensures robust and efficient data collection from multiple sources.
- **[Always Focus Demo Rule](.cursor/rules/always_focus_demo.mdc)**: The authoritative instruction file guiding the Cursor Agent to strictly adhere to the demo plan and maintain focus on the Failover Control Protocol demonstration.

## API Documentation

The `docs/api` folder provides in-depth documentation on data source characteristics and retrieval mechanisms, including:

- **[Binance Vision Klines API](docs/api/binance_vision_klines.md)**: Source of Vision API kline data retrieval details.
- **[Binance REST Klines API](docs/api/binance_rest_klines.md)**: Source of REST API kline data retrieval details.

## Data Initialization and Shortlisting

1. Initialization

- Execute `scripts/binance_vision_api_aws_s3/fetch_binance_data_availability.sh` to build `scripts/binance_vision_api_aws_s3/reports/spot_synchronal.csv`
- The archaic word _synchronal_ contextually means the Binance Exchanges crypto base pair that we're interested in monitoring, because they must be active in the SPOT, UM and CM market of the Binance Exchange.
- `scripts/binance_vision_api_aws_s3/reports/spot_synchronal.csv` contains only the Binance SPOT market symbols, their earliest date available, and their available intervals (i.e. 1s, 1m, 3m, ..., 1d), and which base pairs (e.g. BTC) are also on the UM and CM markets.

1. Shortlisting

- To exclude specific symbols from subsequent operations below, simply remove their corresponding lines from `spot_synchronal.csv`
