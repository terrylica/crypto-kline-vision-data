#!/usr/bin/env python3
"""
Clean Feature Engineering Example

This example demonstrates how to use DSM with suppressed logging for clean,
professional output in feature engineering workflows.

Before: 15+ lines of logging boilerplate + cluttered output
After: 1 line of configuration + clean output

Usage:
    python examples/clean_feature_engineering_example.py
"""

import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

# ============================================================================
# SOLUTION: Single line to suppress DSM logging noise
# ============================================================================
os.environ["DSM_LOG_LEVEL"] = "CRITICAL"

# Add project root to path
project_root = Path(__file__).parent.parent
if str(project_root) not in sys.path:
    sys.path.append(str(project_root))

# Now import DSM - no more logging boilerplate needed!
from data_source_manager.core.sync.data_source_manager import DataSourceManager
from data_source_manager.utils.market_constraints import DataProvider, Interval, MarketType


def main():
    """Demonstrate clean feature engineering workflow."""

    print("🚀 Starting feature engineering workflow...")
    print("=" * 50)

    # Create DSM instance - minimal logging
    print("📊 Initializing Data Source Manager...")
    dsm = DataSourceManager.create(DataProvider.BINANCE, MarketType.SPOT)

    # Define time range for feature extraction
    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(days=1)  # Last 24 hours

    symbols = ["BTCUSDT", "ETHUSDT", "ADAUSDT"]

    print(f"⏰ Time range: {start_time.strftime('%Y-%m-%d %H:%M')} to {end_time.strftime('%Y-%m-%d %H:%M')}")
    print(f"🎯 Processing {len(symbols)} symbols...")
    print()

    # Process each symbol with clean output
    results = {}

    for i, symbol in enumerate(symbols, 1):
        print(f"[{i}/{len(symbols)}] Processing {symbol}...", end=" ")

        try:
            # Fetch data - DSM logs are suppressed, only our output shows
            data = dsm.get_data(
                symbol=symbol,
                start_time=start_time,
                end_time=end_time,
                interval=Interval.MINUTE_1,
            )

            if not data.empty:
                # Simple feature extraction example
                features = {
                    "records": len(data),
                    "avg_price": data["close"].mean() if "close" in data.columns else 0,
                    "volatility": data["close"].std() if "close" in data.columns else 0,
                    "volume_total": data["volume"].sum() if "volume" in data.columns else 0,
                }
                results[symbol] = features
                print(f"✅ {features['records']} records")
            else:
                print("⚠️  No data available")
                results[symbol] = None

        except Exception as e:
            print(f"❌ Error: {str(e)[:50]}...")
            results[symbol] = None

    # Clean up resources
    dsm.close()

    print()
    print("📈 Feature Engineering Results:")
    print("=" * 50)

    # Display results in a clean format
    for symbol, features in results.items():
        if features:
            print(
                f"{symbol:>10}: {features['records']:>6} records | "
                f"Avg: ${features['avg_price']:>8.2f} | "
                f"Vol: {features['volatility']:>6.2f} | "
                f"Total Volume: {features['volume_total']:>12,.0f}"
            )
        else:
            print(f"{symbol:>10}: No data available")

    print()
    print("✅ Feature engineering workflow completed successfully!")
    print("🎯 Notice: Clean output with no DSM logging noise")

    # Show the difference
    print()
    print("💡 What you DON'T see (thanks to DSM_LOG_LEVEL=CRITICAL):")
    print("   - Cache checking messages")
    print("   - FCP step-by-step logs")
    print("   - DataFrame processing details")
    print("   - API call debugging info")
    print("   - Hundreds of internal DSM logs")
    print()
    print("🚀 Perfect for feature engineering pipelines!")


if __name__ == "__main__":
    main()
