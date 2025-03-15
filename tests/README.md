# Run PyTest tests with

```bash
cd /workspaces/ml-feature-set && PYTHONPATH=. pytest ml_feature_set/binance_data_services/tests -v --log-cli-level=INFO --asyncio-mode=auto
```

Or with a more verbose output:

```bash
cd /workspaces/ml-feature-set && python3 -m pytest ml_feature_set/binance_data_services/tests -v --log-cli-level=INFO --asyncio-mode=auto --capture=no --tb=short
```
