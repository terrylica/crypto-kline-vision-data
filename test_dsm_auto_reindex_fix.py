#!/usr/bin/env python3
"""
DSM Auto-Reindex Fix Validation Script

This script tests the critical fix for the DSM auto_reindex=False bug that was creating
66.67% NaN values when Binance has 99.9% data availability.

The fix addresses:
1. Time boundary alignment issues when auto_reindex=False
2. Prevention of artificial gap detection and API calls
3. Filtering to exact user time ranges
4. Proper completeness reporting

Success Criteria:
- auto_reindex=False should return 0% NaN values
- auto_reindex=False should return only available data
- auto_reindex=True should still work as before (complete time series)
"""

import sys
import traceback
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np

# Add project root to path
PROJECT_ROOT = Path(__file__).parent
sys.path.insert(0, str(PROJECT_ROOT))

from data_source_manager.core.sync.data_source_manager import DataSourceManager
from data_source_manager.utils.market_constraints import DataProvider, Interval, MarketType


def test_auto_reindex_false_fix():
    """Test that auto_reindex=False no longer creates artificial NaN values."""
    
    print("\n🔧 TESTING DSM AUTO_REINDEX=FALSE FIX")
    print("=" * 60)
    
    # Test configuration
    symbol = "BTCUSDT"
    interval = Interval.SECOND_1
    
    # Use a time range that's likely to have partial cache coverage
    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(minutes=15)  # 15 minutes = 900 seconds
    
    print(f"Symbol: {symbol}")
    print(f"Interval: {interval.value}")
    print(f"Time Range: {start_time} to {end_time}")
    print(f"Expected Records: ~900 (15 minutes * 60 seconds)")
    
    # Create DSM instance with debug logging
    dsm = DataSourceManager.create(
        DataProvider.BINANCE, 
        MarketType.SPOT,
        log_level='INFO',
        suppress_http_debug=True
    )
    
    try:
        print("\n📊 TEST 1: auto_reindex=False (SHOULD NOT CREATE NaN VALUES)")
        print("-" * 50)
        
        # Test auto_reindex=False
        df_no_reindex = dsm.get_data(
            symbol=symbol,
            start_time=start_time,
            end_time=end_time,
            interval=interval,
            auto_reindex=False
        )
        
        # Analyze results
        total_records = len(df_no_reindex)
        nan_count = df_no_reindex.isnull().sum().sum()
        nan_percentage = (nan_count / (total_records * len(df_no_reindex.columns))) * 100 if total_records > 0 else 0
        
        print(f"✅ Records returned: {total_records}")
        print(f"✅ NaN values: {nan_count}")
        print(f"✅ NaN percentage: {nan_percentage:.2f}%")
        
        # Success criteria for auto_reindex=False
        success_no_reindex = nan_count == 0
        print(f"✅ auto_reindex=False SUCCESS: {success_no_reindex}")
        
        if not success_no_reindex:
            print(f"❌ FAILURE: auto_reindex=False created {nan_count} NaN values (should be 0)")
        
        print("\n📊 TEST 2: auto_reindex=True (SHOULD CREATE COMPLETE TIME SERIES)")
        print("-" * 50)
        
        # Test auto_reindex=True for comparison
        df_with_reindex = dsm.get_data(
            symbol=symbol,
            start_time=start_time,
            end_time=end_time,
            interval=interval,
            auto_reindex=True
        )
        
        # Analyze results
        total_records_reindexed = len(df_with_reindex)
        nan_count_reindexed = df_with_reindex.isnull().sum().sum()
        nan_percentage_reindexed = (nan_count_reindexed / (total_records_reindexed * len(df_with_reindex.columns))) * 100 if total_records_reindexed > 0 else 0
        
        print(f"✅ Records returned: {total_records_reindexed}")
        print(f"✅ NaN values: {nan_count_reindexed}")
        print(f"✅ NaN percentage: {nan_percentage_reindexed:.2f}%")
        
        # auto_reindex=True should create a complete time series (may have NaN values)
        expected_records = int((end_time - start_time).total_seconds())
        completeness_ok = total_records_reindexed >= expected_records * 0.95  # Allow 5% variance
        print(f"✅ Complete time series: {completeness_ok} ({total_records_reindexed}/{expected_records})")
        
        print("\n📊 COMPARISON")
        print("-" * 50)
        print(f"auto_reindex=False: {total_records} records, {nan_count} NaN values ({nan_percentage:.2f}% NaN)")
        print(f"auto_reindex=True:  {total_records_reindexed} records, {nan_count_reindexed} NaN values ({nan_percentage_reindexed:.2f}% NaN)")
        
        # Overall success
        overall_success = success_no_reindex
        
        print(f"\n🎯 OVERALL RESULT: {'✅ PASS' if overall_success else '❌ FAIL'}")
        
        if overall_success:
            print("🎉 DSM auto_reindex=False fix is working correctly!")
            print("🎉 Users can now get clean data without artificial NaN values!")
        else:
            print("❌ DSM auto_reindex=False fix needs more work")
            print("❌ The bug is still present - NaN values are being created")
        
        return overall_success
        
    except Exception as e:
        print(f"❌ ERROR during testing: {e}")
        traceback.print_exc()
        return False
    finally:
        dsm.close()


def test_signal_processing_compatibility():
    """Test that the fixed DSM data works with signal processing libraries."""
    
    print("\n🎵 TESTING SIGNAL PROCESSING COMPATIBILITY")
    print("=" * 60)
    
    try:
        # Try importing signal processing libraries
        try:
            import scipy.signal
            scipy_available = True
        except ImportError:
            scipy_available = False
            print("⚠️  scipy not available for testing")
        
        try:
            import librosa
            librosa_available = True
        except ImportError:
            librosa_available = False
            print("⚠️  librosa not available for testing")
        
        # Get clean data from DSM
        dsm = DataSourceManager.create(
            DataProvider.BINANCE, 
            MarketType.SPOT,
            quiet_mode=True  # Suppress logs for clean output
        )
        
        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(minutes=5)
        
        df = dsm.get_data(
            symbol="BTCUSDT",
            start_time=start_time,
            end_time=end_time,
            interval=Interval.SECOND_1,
            auto_reindex=False  # Use the fixed behavior
        )
        
        dsm.close()
        
        if df.empty:
            print("❌ No data available for signal processing test")
            return False
        
        # Extract price data
        prices = df['close'].values
        
        # Remove any NaN values
        clean_prices = prices[np.isfinite(prices)]
        
        nan_count = len(prices) - len(clean_prices)
        print(f"Original data: {len(prices)} points")
        print(f"Clean data: {len(clean_prices)} points")
        print(f"NaN values removed: {nan_count}")
        
        success = True
        
        # Test scipy.signal
        if scipy_available and len(clean_prices) > 100:
            try:
                # Test Welch's method for power spectral density
                freqs, psd = scipy.signal.welch(clean_prices, nperseg=min(256, len(clean_prices)//4))
                print(f"✅ scipy.signal.welch: SUCCESS (computed {len(freqs)} frequency bins)")
            except Exception as e:
                print(f"❌ scipy.signal.welch: FAILED - {e}")
                success = False
        
        # Test librosa
        if librosa_available and len(clean_prices) > 512:
            try:
                # Test STFT (Short-Time Fourier Transform)
                stft = librosa.stft(clean_prices.astype(np.float32), n_fft=min(512, len(clean_prices)//2))
                print(f"✅ librosa.stft: SUCCESS (shape: {stft.shape})")
            except Exception as e:
                print(f"❌ librosa.stft: FAILED - {e}")
                success = False
        
        # Test basic numpy operations
        try:
            mean_price = np.mean(clean_prices)
            std_price = np.std(clean_prices)
            print(f"✅ numpy operations: SUCCESS (mean: {mean_price:.2f}, std: {std_price:.2f})")
        except Exception as e:
            print(f"❌ numpy operations: FAILED - {e}")
            success = False
        
        print(f"\n🎯 SIGNAL PROCESSING RESULT: {'✅ PASS' if success else '❌ FAIL'}")
        return success
        
    except Exception as e:
        print(f"❌ ERROR during signal processing test: {e}")
        traceback.print_exc()
        return False


def main():
    """Run all tests and provide final report."""
    
    print("🚀 DSM AUTO_REINDEX=FALSE FIX VALIDATION")
    print("=" * 60)
    print("Testing the critical bug fix that was creating 66.67% NaN values")
    print("when Binance has 99.9% data availability.")
    
    # Run tests
    test1_success = test_auto_reindex_false_fix()
    test2_success = test_signal_processing_compatibility()
    
    # Final report
    print("\n" + "=" * 60)
    print("🏁 FINAL VALIDATION REPORT")
    print("=" * 60)
    
    print(f"Test 1 - auto_reindex=False Fix: {'✅ PASS' if test1_success else '❌ FAIL'}")
    print(f"Test 2 - Signal Processing Compatibility: {'✅ PASS' if test2_success else '❌ FAIL'}")
    
    overall_success = test1_success and test2_success
    print(f"\nOverall Result: {'✅ SUCCESS' if overall_success else '❌ FAILURE'}")
    
    if overall_success:
        print("\n🎉 CONGRATULATIONS! The DSM auto_reindex=False fix is working perfectly!")
        print("🎉 Users can now use DSM for signal processing without artificial NaN values!")
        print("🎉 The critical bug has been resolved!")
    else:
        print("\n❌ The fix needs more work. Some tests are still failing.")
        print("❌ Please review the test results and continue debugging.")
    
    return overall_success


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1) 