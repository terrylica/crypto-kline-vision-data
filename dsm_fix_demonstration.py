#!/usr/bin/env python3
"""
DSM Auto-Reindex Fix Demonstration

This script demonstrates the fix for the critical DSM bug where auto_reindex=False
was still creating 66.67% NaN values despite the parameter being set to False.

BEFORE FIX:
- auto_reindex=False still created artificial NaN values
- Time boundaries were always aligned, expanding requested ranges
- Missing segments were always fetched from APIs
- Users got complete time series with NaN padding even when they didn't want it

AFTER FIX:
- auto_reindex=False returns only available data with 0% NaN values
- Time boundaries use exact user ranges when auto_reindex=False
- API calls are skipped when cache data is available and auto_reindex=False
- Users get clean data suitable for signal processing libraries
"""

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np

# Add project root to path
PROJECT_ROOT = Path(__file__).parent
sys.path.insert(0, str(PROJECT_ROOT))

from data_source_manager.core.sync.data_source_manager import DataSourceManager
from data_source_manager.utils.market_constraints import DataProvider, Interval, MarketType


def demonstrate_fix():
    """Demonstrate the DSM auto_reindex=False fix."""
    
    print("🚀 DSM AUTO_REINDEX=FALSE FIX DEMONSTRATION")
    print("=" * 60)
    print("This demonstrates the fix for the critical bug where auto_reindex=False")
    print("was still creating 66.67% NaN values when users wanted clean data.")
    print()
    
    # Test configuration
    symbol = "BTCUSDT"
    interval = Interval.SECOND_1
    
    # Use a recent time range for demonstration
    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(minutes=10)  # 10 minutes = 600 seconds
    
    print(f"Test Parameters:")
    print(f"  Symbol: {symbol}")
    print(f"  Interval: {interval.value}")
    print(f"  Time Range: {start_time.strftime('%H:%M:%S')} to {end_time.strftime('%H:%M:%S')}")
    print(f"  Expected Records: ~600 (10 minutes * 60 seconds)")
    print()
    
    # Create DSM instance with minimal logging for clean output
    dsm = DataSourceManager.create(
        DataProvider.BINANCE, 
        MarketType.SPOT,
        quiet_mode=True  # Suppress most logging for clean demonstration
    )
    
    try:
        print("📊 TESTING auto_reindex=False (FIXED BEHAVIOR)")
        print("-" * 50)
        
        # Test the fixed auto_reindex=False behavior
        df_fixed = dsm.get_data(
            symbol=symbol,
            start_time=start_time,
            end_time=end_time,
            interval=interval,
            auto_reindex=False  # This should now work correctly
        )
        
        # Analyze the results
        total_records = len(df_fixed)
        nan_count = df_fixed.isnull().sum().sum()
        
        if total_records > 0:
            # Calculate NaN percentage
            total_cells = total_records * len(df_fixed.columns)
            nan_percentage = (nan_count / total_cells) * 100
            
            # Check data quality
            if 'close' in df_fixed.columns:
                close_prices = df_fixed['close'].values
                finite_prices = close_prices[np.isfinite(close_prices)]
                data_quality = len(finite_prices) / len(close_prices) * 100 if len(close_prices) > 0 else 0
            else:
                data_quality = 0
            
            print(f"✅ Records Retrieved: {total_records}")
            print(f"✅ NaN Values: {nan_count}")
            print(f"✅ NaN Percentage: {nan_percentage:.2f}%")
            print(f"✅ Data Quality: {data_quality:.1f}% finite values")
            
            # Success criteria
            if nan_count == 0:
                print("🎉 SUCCESS: auto_reindex=False returned clean data with 0% NaN values!")
                print("🎉 This data is now suitable for signal processing libraries!")
            else:
                print(f"❌ ISSUE: auto_reindex=False still created {nan_count} NaN values")
                print("❌ The fix may need additional work")
            
            # Show data sample
            if total_records > 0:
                print(f"\n📋 Sample Data (first 3 records):")
                print(df_fixed.head(3).to_string())
                
                # Show time coverage
                if 'open_time' in df_fixed.columns:
                    actual_start = df_fixed['open_time'].min()
                    actual_end = df_fixed['open_time'].max()
                    coverage_seconds = (actual_end - actual_start).total_seconds()
                    print(f"\n⏰ Time Coverage:")
                    print(f"   Actual Range: {actual_start.strftime('%H:%M:%S')} to {actual_end.strftime('%H:%M:%S')}")
                    print(f"   Coverage: {coverage_seconds:.0f} seconds ({coverage_seconds/60:.1f} minutes)")
        else:
            print("❌ No data returned - this could indicate network issues or data unavailability")
        
        print("\n📊 TESTING auto_reindex=True (COMPARISON)")
        print("-" * 50)
        
        # Test auto_reindex=True for comparison
        df_reindexed = dsm.get_data(
            symbol=symbol,
            start_time=start_time,
            end_time=end_time,
            interval=interval,
            auto_reindex=True  # Traditional behavior with complete time series
        )
        
        total_records_reindexed = len(df_reindexed)
        nan_count_reindexed = df_reindexed.isnull().sum().sum()
        
        if total_records_reindexed > 0:
            total_cells_reindexed = total_records_reindexed * len(df_reindexed.columns)
            nan_percentage_reindexed = (nan_count_reindexed / total_cells_reindexed) * 100
            
            print(f"✅ Records Retrieved: {total_records_reindexed}")
            print(f"✅ NaN Values: {nan_count_reindexed}")
            print(f"✅ NaN Percentage: {nan_percentage_reindexed:.2f}%")
            
            expected_records = int((end_time - start_time).total_seconds())
            completeness = (total_records_reindexed / expected_records) * 100 if expected_records > 0 else 0
            print(f"✅ Completeness: {completeness:.1f}% ({total_records_reindexed}/{expected_records})")
        
        print(f"\n📊 COMPARISON SUMMARY")
        print("-" * 50)
        print(f"auto_reindex=False: {total_records:4d} records, {nan_count:5d} NaN values")
        print(f"auto_reindex=True:  {total_records_reindexed:4d} records, {nan_count_reindexed:5d} NaN values")
        print()
        
        # Final assessment
        if total_records > 0 and nan_count == 0:
            print("🎯 RESULT: ✅ SUCCESS!")
            print("🎉 The DSM auto_reindex=False fix is working correctly!")
            print("🎉 Users can now get clean financial data for signal processing!")
            print()
            print("💡 Use Cases Enabled:")
            print("   • High-frequency trading algorithms")
            print("   • Signal processing with scipy, librosa, PySDKit")
            print("   • Machine learning feature engineering")
            print("   • Statistical analysis without artificial gaps")
            return True
        else:
            print("🎯 RESULT: ❌ NEEDS MORE WORK")
            print("❌ The fix is not completely working yet")
            print("❌ Please continue debugging the DSM implementation")
            return False
            
    except Exception as e:
        print(f"❌ ERROR during demonstration: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        dsm.close()


def show_signal_processing_example():
    """Show how the fixed DSM data can be used with signal processing libraries."""
    
    print("\n🎵 SIGNAL PROCESSING EXAMPLE")
    print("=" * 60)
    print("This shows how the fixed DSM data works with signal processing libraries.")
    print()
    
    try:
        # Get clean data from DSM
        dsm = DataSourceManager.create(
            DataProvider.BINANCE, 
            MarketType.SPOT,
            quiet_mode=True
        )
        
        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(minutes=5)
        
        df = dsm.get_data(
            symbol="BTCUSDT",
            start_time=start_time,
            end_time=end_time,
            interval=Interval.SECOND_1,
            auto_reindex=False  # Get clean data without NaN padding
        )
        
        dsm.close()
        
        if df.empty or 'close' not in df.columns:
            print("❌ No price data available for signal processing example")
            return
        
        # Extract price data
        prices = df['close'].values
        print(f"📊 Price Data: {len(prices)} points")
        print(f"   Range: ${prices.min():.2f} - ${prices.max():.2f}")
        print(f"   Mean: ${prices.mean():.2f}")
        print(f"   Std: ${prices.std():.2f}")
        
        # Check for NaN values
        nan_count = np.isnan(prices).sum()
        print(f"   NaN values: {nan_count} ({nan_count/len(prices)*100:.1f}%)")
        
        # Test basic signal processing operations
        print(f"\n🔬 Signal Processing Operations:")
        
        # Calculate returns
        if len(prices) > 1:
            returns = np.diff(prices) / prices[:-1]
            print(f"   ✅ Returns calculation: {len(returns)} return values")
            print(f"      Mean return: {returns.mean()*100:.4f}%")
            print(f"      Volatility: {returns.std()*100:.4f}%")
        
        # Moving average
        if len(prices) >= 10:
            window = min(10, len(prices)//2)
            moving_avg = np.convolve(prices, np.ones(window)/window, mode='valid')
            print(f"   ✅ Moving average ({window}-period): {len(moving_avg)} values")
        
        # Test with scipy if available
        try:
            import scipy.stats
            # Normality test
            if len(prices) > 8:
                statistic, p_value = scipy.stats.normaltest(prices)
                print(f"   ✅ scipy.stats.normaltest: statistic={statistic:.3f}, p={p_value:.3f}")
        except ImportError:
            print("   ⚠️  scipy not available")
        
        print(f"\n✅ All operations completed successfully with clean DSM data!")
        print(f"🎉 No more 'buffer is not finite everywhere' errors!")
        
    except Exception as e:
        print(f"❌ Signal processing example failed: {e}")
        import traceback
        traceback.print_exc()


def main():
    """Run the complete demonstration."""
    
    success = demonstrate_fix()
    
    if success:
        show_signal_processing_example()
    
    print(f"\n{'='*60}")
    if success:
        print("🏆 DEMONSTRATION COMPLETE: DSM auto_reindex=False fix is working!")
        print("🎉 Signal processing libraries can now use DSM data without errors!")
    else:
        print("❌ DEMONSTRATION FAILED: The fix needs more work")
    print(f"{'='*60}")
    
    return success


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1) 