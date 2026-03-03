"""Performance benchmarks for Round 14: Import & Startup Optimization.

# ADR: docs/adr/2026-01-30-claude-code-infrastructure.md

Validates:
1. utils/__init__.py uses lazy __getattr__ (not eager imports)
2. market/__init__.py uses lazy __getattr__ (not eager imports)
3. pendulum is deferred until configure_session_logging() is called
4. All lazy-loaded attributes are still importable
5. Bare import (import ckvd) stays fast
"""

import inspect
import subprocess
import sys


class TestLazyLoadingCorrectness:
    """Verify all lazy-loaded attributes resolve correctly."""

    def test_utils_enums_importable(self):
        """Core enums should be importable from ckvd.utils."""
        from ckvd.utils import ChartType, DataProvider, Interval, MarketType

        assert ChartType is not None
        assert DataProvider is not None
        assert Interval is not None
        assert MarketType is not None

    def test_utils_config_importable(self):
        """Config constants should be importable from ckvd.utils."""
        from ckvd.utils import (
            KLINE_COLUMNS,
            FeatureFlags,
            create_empty_dataframe,
            standardize_column_names,
        )

        assert KLINE_COLUMNS is not None
        assert FeatureFlags is not None
        assert create_empty_dataframe is not None
        assert standardize_column_names is not None

    def test_utils_time_importable(self):
        """Time utilities should be importable from ckvd.utils."""
        from ckvd.utils import (
            align_time_boundaries,
            datetime_to_milliseconds,
            enforce_utc_timezone,
            filter_dataframe_by_time,
        )

        assert align_time_boundaries is not None
        assert datetime_to_milliseconds is not None
        assert enforce_utc_timezone is not None
        assert filter_dataframe_by_time is not None

    def test_utils_validation_importable(self):
        """Validation utilities should be importable from ckvd.utils."""
        from ckvd.utils import (
            ALL_COLUMNS,
            DataFrameValidator,
            ValidationError,
            calculate_checksum,
        )

        assert ALL_COLUMNS is not None
        assert DataFrameValidator is not None
        assert ValidationError is not None
        assert calculate_checksum is not None

    def test_market_functions_importable(self):
        """Market functions should be importable from ckvd.utils.market."""
        from ckvd.utils.market import (
            MARKET_CAPABILITIES,
            MarketCapabilities,
            get_endpoint_url,
            get_market_capabilities,
            validate_symbol_for_market_type,
        )

        assert get_endpoint_url is not None
        assert get_market_capabilities is not None
        assert validate_symbol_for_market_type is not None
        assert MARKET_CAPABILITIES is not None
        assert MarketCapabilities is not None

    def test_all_utils_all_items_importable(self):
        """Every item in utils.__all__ should be importable."""
        import ckvd.utils

        for name in ckvd.utils.__all__:
            val = getattr(ckvd.utils, name)
            assert val is not None, f"ckvd.utils.{name} resolved to None"

    def test_all_market_all_items_importable(self):
        """Every item in market.__all__ should be importable."""
        import ckvd.utils.market

        for name in ckvd.utils.market.__all__:
            val = getattr(ckvd.utils.market, name)
            assert val is not None, f"ckvd.utils.market.{name} resolved to None"


class TestLazyLoadingMechanism:
    """Verify __getattr__ lazy loading is in use."""

    def test_utils_has_getattr(self):
        """utils/__init__.py should define __getattr__ for lazy loading."""
        import ckvd.utils

        source = inspect.getsource(ckvd.utils)
        assert "def __getattr__" in source, "utils/__init__.py should use __getattr__"
        assert "_LAZY_IMPORTS" in source, "utils/__init__.py should have _LAZY_IMPORTS dict"

    def test_market_has_getattr(self):
        """market/__init__.py should define __getattr__ for lazy loading."""
        import ckvd.utils.market

        source = inspect.getsource(ckvd.utils.market)
        assert "def __getattr__" in source, "market/__init__.py should use __getattr__"
        assert "_LAZY_IMPORTS" in source, "market/__init__.py should have _LAZY_IMPORTS dict"

    def test_utils_no_eager_imports(self):
        """utils/__init__.py should not have eager from-imports of submodules."""
        import ckvd.utils

        source = inspect.getsource(ckvd.utils)
        # Should NOT eagerly import from submodules
        assert "from .market_constraints import" not in source
        assert "from .config import" not in source
        assert "from .time_utils import" not in source
        assert "from .validation import" not in source

    def test_market_no_eager_imports(self):
        """market/__init__.py should not have eager from-imports of submodules."""
        import ckvd.utils.market

        source = inspect.getsource(ckvd.utils.market)
        # Should NOT eagerly import from submodules
        assert "from ckvd.utils.market.capabilities import" not in source
        assert "from ckvd.utils.market.endpoints import" not in source
        assert "from ckvd.utils.market.enums import" not in source
        assert "from ckvd.utils.market.validation import" not in source


class TestPendulumDeferred:
    """Verify pendulum is not imported at module level."""

    def test_pendulum_not_in_loguru_setup_source(self):
        """loguru_setup.py should not import pendulum at module level."""
        from ckvd.utils import loguru_setup

        source = inspect.getsource(loguru_setup)
        # Should not have 'import pendulum' at module level
        lines = source.split("\n")
        for line in lines:
            stripped = line.strip()
            if stripped == "import pendulum":
                # This is a module-level import (not indented inside a function)
                # Check if it's at the top level (no indentation)
                if not line.startswith(" ") and not line.startswith("\t"):
                    raise AssertionError(
                        "pendulum should not be imported at module level in loguru_setup"
                    )

    def test_pendulum_not_loaded_after_loguru_import(self):
        """Importing loguru_setup should not trigger pendulum import."""
        # Run in subprocess for clean sys.modules state
        result = subprocess.run(
            [
                sys.executable,
                "-c",
                "import ckvd.utils.loguru_setup; import sys; print('pendulum' not in sys.modules)",
            ],
            capture_output=True,
            text=True,
            check=True,
        )
        assert result.stdout.strip() == "True", (
            f"pendulum should not be in sys.modules after loguru_setup import. "
            f"stdout={result.stdout.strip()}"
        )


class TestImportTimeBenchmark:
    """Benchmark import times."""

    def test_bare_import_fast(self):
        """'import ckvd' should be fast (< 50ms)."""
        result = subprocess.run(
            [
                sys.executable,
                "-c",
                "import time; s=time.perf_counter(); import ckvd; print(f'{(time.perf_counter()-s)*1000:.0f}')",
            ],
            capture_output=True,
            text=True,
            check=True,
        )
        elapsed_ms = int(result.stdout.strip())
        assert elapsed_ms < 50, f"Bare 'import ckvd' took {elapsed_ms}ms, expected < 50ms"

    def test_full_import_reasonable(self):
        """'from ckvd import CryptoKlineVisionData' should complete in reasonable time."""
        # Run 3 times and take median
        times = []
        for _ in range(3):
            result = subprocess.run(
                [
                    sys.executable,
                    "-c",
                    "import time; s=time.perf_counter(); from ckvd import CryptoKlineVisionData; print(f'{(time.perf_counter()-s)*1000:.0f}')",
                ],
                capture_output=True,
                text=True,
                check=True,
            )
            times.append(int(result.stdout.strip()))

        median_ms = sorted(times)[1]
        # Target < 600ms (reasonable for cold start with heavy deps)
        assert median_ms < 600, (
            f"Full import took {median_ms}ms (median of {times}), expected < 600ms"
        )
