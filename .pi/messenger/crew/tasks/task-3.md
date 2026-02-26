# Example Scripts API Validation

Test all examples in examples/ directory - verify they import correctly and execute without errors. Run each example with uv run -p 3.13 python examples/<file>: quick_start.py, ckvd_cache_control_example.py, ckvd_lazy_initialization_demo.py, ckvd_logging_demo.py, clean_feature_engineering_example.py, and any in sync/ subdirectory. Broadcast which examples fail and why (API changes, import errors, etc.).
