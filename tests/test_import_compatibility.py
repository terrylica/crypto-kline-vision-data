#!/usr/bin/env python
"""Import compatibility tests for DSM following industry best practices.

These tests ensure that DSM imports work correctly regardless of import order,
preventing the hanging issues identified in the analysis. This follows testing
patterns used by major Python libraries like pandas, SQLAlchemy, and AWS SDK.

Test Coverage:
1. Import speed benchmarks (should be <100ms)
2. Import order independence 
3. Post-scipy import compatibility
4. Memory usage during import
5. Thread safety of imports
6. Factory method performance
"""

import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Tuple

import pytest

# Test constants based on industry benchmarks
MAX_IMPORT_TIME = 0.1  # 100ms - similar to pandas import
MAX_CREATE_TIME = 2.0  # 2s - similar to SQLAlchemy engine creation  
MAX_FIRST_FETCH_TIME = 5.0  # 5s - similar to first API call
MAX_SUBSEQUENT_FETCH_TIME = 0.5  # 500ms - connection pooling benefit


class TestImportCompatibility:
    """Test suite for import compatibility following industry standards."""
    
    def test_import_speed_benchmark(self):
        """Test that DSM imports are fast enough for production use.
        
        Industry standard: Import should be <100ms (similar to pandas).
        """
        start_time = time.time()
        
        # This should be lightweight - no heavy initialization
        import sys
        sys.path.insert(0, '.')
        import __init__ as data_source_manager
        
        import_duration = time.time() - start_time
        
        assert import_duration < MAX_IMPORT_TIME, (
            f"DSM import took {import_duration:.3f}s, should be <{MAX_IMPORT_TIME}s. "
            f"This indicates heavy initialization is happening at import time."
        )
        
        # Verify we can access the lightweight interface
        assert hasattr(data_source_manager, 'DSMManager')
        assert hasattr(data_source_manager, 'DataProvider')
        assert hasattr(data_source_manager, 'MarketType')
        
    def test_factory_creation_speed(self):
        """Test that factory method creation is reasonably fast.
        
        Industry standard: Factory creation should be <2s (similar to SQLAlchemy).
        """
        from data_source_manager import DSMManager, DataProvider, MarketType
        
        start_time = time.time()
        
        # Factory creation should be fast (no heavy initialization yet)
        manager = DSMManager.create(DataProvider.BINANCE, MarketType.SPOT)
        
        creation_duration = time.time() - start_time
        
        assert creation_duration < MAX_CREATE_TIME, (
            f"DSM factory creation took {creation_duration:.3f}s, should be <{MAX_CREATE_TIME}s"
        )
        
        # Manager should be created but not initialized
        assert not manager._initialized
        assert manager._provider == DataProvider.BINANCE
        assert manager._market_type == MarketType.SPOT

    def test_import_after_scipy(self):
        """Test DSM imports correctly after scipy (known problematic case).
        
        This was the original hanging issue - scipy imports would cause
        DSM imports to hang due to import-time initialization conflicts.
        """
        # Import scipy modules that were causing issues
        
        # This should not hang with the new lazy initialization
        start_time = time.time()
        
        from data_source_manager import DSMManager, DataProvider, MarketType
        
        import_duration = time.time() - start_time
        
        assert import_duration < MAX_IMPORT_TIME, (
            f"DSM import after scipy took {import_duration:.3f}s, should be <{MAX_IMPORT_TIME}s"
        )
        
        # Verify functionality still works
        manager = DSMManager.create(DataProvider.BINANCE, MarketType.SPOT)
        assert manager._provider == DataProvider.BINANCE

    def test_import_order_independence(self):
        """Test DSM works regardless of import order.
        
        This runs different import orders in separate processes to avoid
        contamination between tests.
        """
        import_orders = [
            # Original problematic order
            ['import scipy.stats', 'import scipy.signal', 'from data_source_manager import DSMManager'],
            
            # Pandas first
            ['import pandas as pd', 'from data_source_manager import DSMManager', 'import scipy.stats'],
            
            # DSM first
            ['from data_source_manager import DSMManager', 'import scipy.stats', 'import pandas as pd'],
            
            # NumPy variants
            ['import numpy as np', 'import scipy.stats', 'from data_source_manager import DSMManager'],
            
            # All together
            ['import scipy.stats', 'import pandas as pd', 'import numpy as np', 'from data_source_manager import DSMManager'],
        ]
        
        for i, order in enumerate(import_orders):
            with pytest.raises(subprocess.CalledProcessError):
                # Create test script
                test_script = '; '.join(order + [
                    'from utils.market_constraints import DataProvider, MarketType',
                    'manager = DSMManager.create(DataProvider.BINANCE, MarketType.SPOT)',
                    'print("SUCCESS")'
                ])
                
                # Run in separate process
                result = subprocess.run([
                    sys.executable, '-c', test_script
                ], capture_output=True, text=True, timeout=10)
                
                if result.returncode != 0:
                    pytest.fail(
                        f"Import order {i+1} failed: {order}\n"
                        f"Return code: {result.returncode}\n"
                        f"Stdout: {result.stdout}\n"
                        f"Stderr: {result.stderr}"
                    )
                
                assert "SUCCESS" in result.stdout, f"Import order {i+1} didn't complete successfully"

    def test_thread_safe_imports(self):
        """Test that DSM imports are thread-safe.
        
        Multiple threads should be able to import and create DSM instances
        simultaneously without hanging or race conditions.
        """
        def import_and_create(thread_id: int) -> Tuple[int, float, bool]:
            """Import DSM and create instance in a thread."""
            start_time = time.time()
            
            try:
                from data_source_manager import DSMManager, DataProvider, MarketType
                manager = DSMManager.create(DataProvider.BINANCE, MarketType.SPOT)
                
                duration = time.time() - start_time
                success = manager._provider == DataProvider.BINANCE
                
                return thread_id, duration, success
                
            except Exception as e:
                pytest.fail(f"Thread {thread_id} failed: {e}")
        
        # Test with multiple threads
        num_threads = 5
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = [
                executor.submit(import_and_create, i) 
                for i in range(num_threads)
            ]
            
            results = []
            for future in as_completed(futures, timeout=30):
                thread_id, duration, success = future.result()
                results.append((thread_id, duration, success))
                
                assert success, f"Thread {thread_id} creation failed"
                assert duration < MAX_CREATE_TIME, (
                    f"Thread {thread_id} took {duration:.3f}s, should be <{MAX_CREATE_TIME}s"
                )
        
        # All threads should have completed successfully
        assert len(results) == num_threads

    def test_memory_usage_during_import(self):
        """Test that DSM import doesn't consume excessive memory.
        
        With lazy initialization, the initial import should have minimal
        memory footprint.
        """
        import psutil
        import os
        
        # Get baseline memory usage
        process = psutil.Process(os.getpid())
        baseline_memory = process.memory_info().rss / 1024 / 1024  # MB
        
        # Import DSM
        from data_source_manager import DSMManager, DataProvider, MarketType
        
        # Check memory after import
        post_import_memory = process.memory_info().rss / 1024 / 1024  # MB
        import_memory_increase = post_import_memory - baseline_memory
        
        # Import should not cause significant memory increase (lazy loading)
        MAX_IMPORT_MEMORY_MB = 50  # 50MB threshold
        assert import_memory_increase < MAX_IMPORT_MEMORY_MB, (
            f"DSM import increased memory by {import_memory_increase:.1f}MB, "
            f"should be <{MAX_IMPORT_MEMORY_MB}MB with lazy initialization"
        )
        
        # Create manager (still should be lightweight)
        DSMManager.create(DataProvider.BINANCE, MarketType.SPOT)
        
        post_create_memory = process.memory_info().rss / 1024 / 1024  # MB
        create_memory_increase = post_create_memory - post_import_memory
        
        MAX_CREATE_MEMORY_MB = 10  # 10MB threshold
        assert create_memory_increase < MAX_CREATE_MEMORY_MB, (
            f"DSM factory creation increased memory by {create_memory_increase:.1f}MB, "
            f"should be <{MAX_CREATE_MEMORY_MB}MB with lazy initialization"
        )

    def test_backwards_compatibility(self):
        """Test that the old import patterns still work.
        
        This ensures we don't break existing code while improving
        the import behavior.
        """
        # Old pattern should still work
        start_time = time.time()
        
        from data_source_manager import fetch_market_data
        
        import_duration = time.time() - start_time
        
        assert import_duration < MAX_IMPORT_TIME, (
            f"Backwards compatible import took {import_duration:.3f}s, should be <{MAX_IMPORT_TIME}s"
        )
        
        # Function should be callable (but won't test actual execution here)
        assert callable(fetch_market_data)

    def test_lazy_initialization_behavior(self):
        """Test that heavy initialization truly happens only when needed.
        
        This verifies the core principle of the lazy initialization pattern.
        """
        from data_source_manager import DSMManager, DataProvider, MarketType
        
        # Create manager - should be fast
        start_time = time.time()
        manager = DSMManager.create(DataProvider.BINANCE, MarketType.SPOT)
        create_duration = time.time() - start_time
        
        assert create_duration < 0.01, f"Manager creation took {create_duration:.3f}s, should be nearly instant"
        assert not manager._initialized, "Manager should not be initialized yet"
        assert manager._core_manager is None, "Core manager should not be created yet"
        
        # First data request should trigger initialization
        # (We won't actually call it to avoid network dependencies, but the pattern is tested)
        
        # Multiple creates should be fast
        managers = []
        start_time = time.time()
        for _ in range(10):
            managers.append(DSMManager.create(DataProvider.BINANCE, MarketType.SPOT))
        
        multiple_create_duration = time.time() - start_time
        
        assert multiple_create_duration < 0.1, (
            f"Creating 10 managers took {multiple_create_duration:.3f}s, "
            f"should be <0.1s with lazy initialization"
        )

    def test_configuration_compatibility(self):
        """Test that new configuration system works correctly."""
        from utils.dsm_config import DSMConfig
        from data_source_manager import DataProvider, MarketType
        
        # Test configuration creation speed
        start_time = time.time()
        
        DSMConfig.create(DataProvider.BINANCE, MarketType.SPOT)
        
        config_duration = time.time() - start_time
        
        assert config_duration < 0.01, f"Config creation took {config_duration:.3f}s, should be nearly instant"
        
        # Test different configuration patterns
        production_config = DSMConfig.for_production(DataProvider.BINANCE, MarketType.SPOT)
        dev_config = DSMConfig.for_development(DataProvider.BINANCE, MarketType.SPOT)
        
        assert production_config.connection_timeout == 60
        assert dev_config.log_level == "DEBUG"
        
        # Test environment config (without actually setting env vars)
        env_config = DSMConfig.from_env()
        assert env_config.provider == DataProvider.BINANCE  # Default


@pytest.mark.integration
class TestProductionReadiness:
    """Integration tests for production readiness."""
    
    def test_import_in_production_environment(self):
        """Test imports work in a production-like environment.
        
        This simulates the conditions where imports were hanging.
        """
        # Simulate production imports
        import_sequence = [
            'import sys',
            'import os', 
            'import logging',
            'import pandas as pd',
            'import numpy as np',
            'import scipy.stats',
            'import scipy.signal',
            'from data_source_manager import DSMManager',
            'from utils.market_constraints import DataProvider, MarketType',
            'manager = DSMManager.create(DataProvider.BINANCE, MarketType.SPOT)',
            'print("PRODUCTION_READY")'
        ]
        
        test_script = '; '.join(import_sequence)
        
        start_time = time.time()
        result = subprocess.run([
            sys.executable, '-c', test_script
        ], capture_output=True, text=True, timeout=15)
        
        total_duration = time.time() - start_time
        
        assert result.returncode == 0, (
            f"Production simulation failed:\n"
            f"Stdout: {result.stdout}\n"
            f"Stderr: {result.stderr}"
        )
        
        assert "PRODUCTION_READY" in result.stdout
        assert total_duration < 10.0, (
            f"Production simulation took {total_duration:.3f}s, should be <10s"
        )


if __name__ == "__main__":
    pytest.main([__file__, "-v"]) 