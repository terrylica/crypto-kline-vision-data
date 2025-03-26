# Example Scripts

This directory contains example scripts demonstrating different ways to use the Binance data services library.

## Available Examples

### 1. Recommended Data Retrieval (`recommended_data_retrieval.py`)

Shows the recommended approach for retrieving market data using `DataSourceManager`.

```bash
python -m examples.recommended_data_retrieval
```

Key features:

1. **Automatic Source Selection**: Chooses the optimal data source based on the time range
2. **Integrated Caching**: Automatic caching for improved performance
3. **Comprehensive Error Handling**: Gracefully handles API errors
4. **Flexible Configuration**: Supports various market types and intervals

## Cache Directories

The example scripts create the following cache directories:

- `./cache/` - for the recommended approach

## Running All Examples

To run all examples sequentially:

```bash
for example in recommended_data_retrieval; do
  echo "Running $example..."
  python -m examples.$example
done
```

## Notes

- These examples are designed to demonstrate basic usage patterns
- Error handling is simplified for clarity
- For production use, consider adding additional validation and error handling
- API calls are rate-limited to avoid exceeding Binance's limits
