"""Conftest file for data sources tests.

This file contains fixtures and setup for properly testing the data source
selection and fallback mechanisms.
"""

import os
import sys
from pathlib import Path
import pytest

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent.parent.absolute()
sys.path.insert(0, str(PROJECT_ROOT))

# Create custom modules path to maintain import consistency
from typing import Generator
import types


@pytest.fixture(scope="session", autouse=True)
def fix_import_paths() -> Generator[None, None, None]:
    """Fix import paths to ensure tests can import core and utils modules properly."""
    # Add project root to path
    if str(PROJECT_ROOT) not in sys.path:
        sys.path.insert(0, str(PROJECT_ROOT))

    # Create mock packages for imports if they don't exist
    modules_to_check = ["utils", "core"]
    for module_name in modules_to_check:
        if module_name not in sys.modules:
            module_path = PROJECT_ROOT / module_name
            if module_path.exists():
                if module_name not in sys.modules:
                    # Create a mock module
                    mod = types.ModuleType(module_name)
                    mod.__path__ = [str(module_path)]
                    sys.modules[module_name] = mod

    yield

    # Clean up is not strictly necessary but included for completeness
    # We don't remove the added paths from sys.path as they might be needed by other tests
