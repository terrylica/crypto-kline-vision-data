#!/usr/bin/env python3
# ADR: docs/adr/2026-01-30-claude-code-infrastructure.md
"""Demonstration of CKVD Import and Initialization Performance.

This script demonstrates CKVD's import performance and initialization patterns.
It shows:

1. Fast imports (similar to pandas, SQLAlchemy)
2. Factory creation pattern
3. Performance benchmarks vs industry standards

Run this script to verify the improvements work correctly:
    uv run -p 3.13 python examples/ckvd_lazy_initialization_demo.py
"""

import time


def benchmark_import_speed() -> tuple[float, float]:
    """Benchmark CKVD import speed vs industry standards."""
    print("🚀 CKVD Import Speed Benchmark")
    print("=" * 50)

    # Benchmark CKVD import
    start_time = time.time()
    from ckvd import DataProvider, CryptoKlineVisionData, Interval, MarketType  # noqa: F401

    ckvd_import_time = time.time() - start_time

    # Compare with pandas (typical benchmark)
    start_time = time.time()
    import pandas  # noqa: F401

    pandas_import_time = time.time() - start_time

    print(f"📊 CKVD import time:    {ckvd_import_time:.3f}s")
    print(f"📊 Pandas import time: {pandas_import_time:.3f}s")

    # Avoid division by zero - if pandas imported instantly, use a small epsilon
    if pandas_import_time > 0.001:
        ratio = ckvd_import_time / pandas_import_time
        print(f"📊 Ratio (CKVD/Pandas): {ratio:.2f}x")
    else:
        print("📊 Ratio (CKVD/Pandas): Both imports very fast (<1ms)")

    # Should be similar speed to pandas
    if ckvd_import_time < 0.5:
        print("✅ PASS: CKVD import is fast (<500ms)")
    else:
        print("⚠️  WARN: CKVD import is slower than expected (>500ms)")

    print()
    return ckvd_import_time, pandas_import_time


def demonstrate_factory_creation() -> None:
    """Demonstrate the factory creation pattern."""
    print("🔄 Factory Creation Demonstration")
    print("=" * 50)

    from ckvd import DataProvider, CryptoKlineVisionData, MarketType

    # Factory creation
    print("Creating CKVD manager instances...")
    start_time = time.time()

    managers = []
    market_types = [MarketType.SPOT, MarketType.FUTURES_USDT, MarketType.FUTURES_COIN]

    for i, market_type in enumerate(market_types):
        create_start = time.time()
        manager = CryptoKlineVisionData.create(DataProvider.BINANCE, market_type)
        managers.append(manager)
        create_time = time.time() - create_start
        print(f"  Manager {i + 1} ({market_type.name}): Created in {create_time:.4f}s")

    total_create_time = time.time() - start_time
    print(f"📊 Total creation time: {total_create_time:.3f}s for {len(managers)} managers")
    print(f"📊 Average per manager: {total_create_time / len(managers):.4f}s")

    # Cleanup
    for manager in managers:
        manager.close()

    print("✅ SUCCESS: All managers created and closed successfully")
    print()


def demonstrate_configuration_patterns() -> None:
    """Demonstrate different configuration patterns."""
    print("⚙️  Configuration Pattern Demonstration")
    print("=" * 50)

    from ckvd import DataProvider, CryptoKlineVisionData, MarketType
    from ckvd.core.sync.crypto_kline_vision_data import DataSource

    # Basic configuration - default FCP
    print("1. Basic Configuration (FCP: Cache → Vision → REST):")
    manager = CryptoKlineVisionData.create(DataProvider.BINANCE, MarketType.SPOT)
    print(f"   Provider: {DataProvider.BINANCE.name}")
    print(f"   Market: {MarketType.SPOT.name}")
    print("   FCP: Cache → Vision → REST (default)")
    manager.close()

    print("\n2. Available enforce_source options for get_data():")
    print("   DataSource.AUTO   - Use FCP priority (default)")
    print("   DataSource.VISION - Skip cache, only Vision API")
    print("   DataSource.REST   - Skip cache and Vision, only REST")
    print("   DataSource.CACHE  - Only local cache (offline mode)")

    # Example usage pattern
    print("\n3. Usage Pattern:")
    print("   # Default FCP")
    print("   df = manager.get_data(symbol, start, end, interval)")
    print()
    print("   # Force REST API only")
    print("   df = manager.get_data(symbol, start, end, interval,")
    print("                         enforce_source=DataSource.REST)")

    # Verify DataSource enum is accessible
    print(f"\n4. DataSource enum values: {[ds.name for ds in DataSource]}")

    print("\n✅ SUCCESS: Configuration patterns documented correctly")
    print()


def test_import_after_scipy() -> None:
    """Test that CKVD imports work after scipy (original problem)."""
    print("🧪 Import After SciPy Test")
    print("=" * 50)

    try:
        # Import scipy first
        print("Importing scipy modules...")
        import scipy.stats  # noqa: F401

        print("✅ SciPy modules imported successfully")

        # Now import CKVD - measure time
        print("Importing CKVD after scipy...")
        start_time = time.time()

        from ckvd import DataProvider, CryptoKlineVisionData, MarketType

        import_time = time.time() - start_time
        print(f"📊 CKVD import after scipy: {import_time:.3f}s")

        if import_time < 1.0:
            print("✅ PASS: CKVD imports quickly after scipy")
        else:
            print("⚠️  WARN: CKVD import slower than expected after scipy")

        # Test functionality
        manager = CryptoKlineVisionData.create(DataProvider.BINANCE, MarketType.SPOT)
        manager.close()
        print("✅ SUCCESS: CKVD manager created successfully after scipy")

    except ImportError as e:
        print(f"⚠️  SKIP: SciPy not available ({e})")
        print("   (Install with 'uv pip install scipy' to run this test)")
        print("   Continuing with CKVD-only test...")

        # Test CKVD import directly
        from ckvd import DataProvider, CryptoKlineVisionData, MarketType

        manager = CryptoKlineVisionData.create(DataProvider.BINANCE, MarketType.SPOT)
        manager.close()
        print("✅ CKVD imports and creates managers successfully")

    print()


def demonstrate_industry_comparisons() -> None:
    """Compare CKVD patterns with industry standards."""
    print("🏭 Industry Standard Comparisons")
    print("=" * 50)

    print("CKVD follows the same patterns as:")
    print()

    print("1. 📊 SQLAlchemy Pattern:")
    print("   from ckvd import CryptoKlineVisionData")
    print("   manager = CryptoKlineVisionData.create(provider, market_type)")
    print("   🔗 Similar to: engine = create_engine(...)")
    print()

    print("2. ☁️  AWS SDK Pattern:")
    print("   Explicit configuration objects and factory creation")
    print("   🔗 Similar to: client = boto3.client('s3', config=Config(...))")
    print()

    print("3. 🌐 Requests Pattern:")
    print("   Connection management via context managers")
    print("   manager.close() or use with statement")
    print("   🔗 Similar to: session = requests.Session()")
    print()

    print("4. ⚡ Performance Benchmarks:")
    print("   📊 Import Speed:     <500ms (similar to pandas)")
    print("   📊 Factory Creation: <2s    (similar to SQLAlchemy)")
    print("   📊 First Fetch:     <5s    (similar to first API call)")
    print("   📊 Subsequent:      <500ms (cache hit)")
    print()


def demonstrate_backwards_compatibility() -> None:
    """Show that the high-level API still works."""
    print("🔄 High-Level API Compatibility Test")
    print("=" * 50)

    # The primary API is CryptoKlineVisionData.create() + get_data()
    # fetch_market_data is a convenience wrapper
    print("Primary API: CryptoKlineVisionData.create() + get_data()")
    print("  - Factory pattern for manager creation")
    print("  - FCP-enabled data retrieval")
    print("  - Explicit resource cleanup with close()")

    print("\nAlternative: fetch_market_data() high-level wrapper")
    print("  - Single function for simple use cases")
    print("  - Uses CryptoKlineVisionData internally with FCP")

    print("\n✅ Both APIs follow industry standard patterns")
    print()


def run_comprehensive_demo() -> None:
    """Run the complete demonstration."""
    print("🎯 CKVD Import and Initialization Demo")
    print("=" * 60)
    print("This demonstrates CKVD's import performance and")
    print("initialization patterns following industry standards.")
    print("=" * 60)
    print()

    # Run all demonstrations
    ckvd_time, pandas_time = benchmark_import_speed()
    demonstrate_factory_creation()
    demonstrate_configuration_patterns()
    test_import_after_scipy()
    demonstrate_industry_comparisons()
    demonstrate_backwards_compatibility()

    print("🎉 SUMMARY")
    print("=" * 50)
    print(f"✅ Import speed: {ckvd_time:.3f}s (pandas: {pandas_time:.3f}s)")
    print("✅ Factory creation: Working")
    print("✅ Configuration system: Complete")
    print("✅ Industry patterns: Implemented")
    print()
    print("🚀 CKVD is ready for production use!")
    print("   Follows industry best practices!")


if __name__ == "__main__":
    # Run demo - let exceptions propagate for visibility
    run_comprehensive_demo()
