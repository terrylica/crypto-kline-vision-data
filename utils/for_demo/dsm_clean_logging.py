#!/usr/bin/env python3
"""Clean logging utilities for DSM feature engineering workflows.

This module provides utilities for using DSM with clean, minimal logging output
that's ideal for feature engineering and production workflows where log noise
is a significant concern.

Key Features:
- Context manager for clean DSM usage
- Automatic HTTP logging suppression
- Feature engineering friendly output
- Zero-configuration clean logging
- Backward compatible with existing code

Example Usage:
    >>> import contextlib
    >>> from utils.for_demo.dsm_clean_logging import get_clean_market_data
    >>> from utils.market_constraints import Interval
    >>> from datetime import datetime
    >>>
    >>> # Clean context manager - no HTTP debug noise
    >>> with get_clean_market_data() as dsm:
    ...     data = dsm.get_data(
    ...         symbol="SOLUSDT",
    ...         start_time=datetime(2024, 1, 1),
    ...         end_time=datetime(2024, 1, 2),
    ...         interval=Interval.SECOND_1,
    ...     )
    ...     print(f"✅ Extracted features from {len(data)} 1-second bars")
    ...     # Your feature engineering code here...
"""

import contextlib
import logging
from typing import Iterator

from core.sync.data_source_manager import DataSourceManager
from utils.market_constraints import DataProvider, MarketType


@contextlib.contextmanager
def get_clean_market_data(
    provider: DataProvider = DataProvider.BINANCE,
    market_type: MarketType = MarketType.SPOT,
    **kwargs
) -> Iterator[DataSourceManager]:
    """Clean DSM context manager with suppressed logging.
    
    This context manager provides a clean DSM instance with HTTP logging
    suppressed by default, making it ideal for feature engineering workflows
    where log noise is problematic.
    
    Args:
        provider: Data provider (default: BINANCE)
        market_type: Market type (default: SPOT)
        **kwargs: Additional parameters passed to DataSourceManager.create()
        
    Yields:
        DataSourceManager: Configured DSM instance with clean logging
        
    Example:
        >>> from utils.for_demo.dsm_clean_logging import get_clean_market_data
        >>> from utils.market_constraints import Interval
        >>> from datetime import datetime
        >>>
        >>> # Clean usage - minimal logging output
        >>> with get_clean_market_data() as dsm:
        ...     data = dsm.get_data(
        ...         symbol="BTCUSDT",
        ...         start_time=datetime(2024, 1, 1),
        ...         end_time=datetime(2024, 1, 2),
        ...         interval=Interval.MINUTE_1,
        ...     )
        ...     print(f"📊 Retrieved {len(data)} records")
        ...     # Feature engineering code here...
    """
    # Set default clean logging parameters if not specified
    clean_defaults = {
        'log_level': 'WARNING',
        'suppress_http_debug': True,
        'quiet_mode': False,
    }
    
    # Merge user kwargs with clean defaults (user kwargs take precedence)
    config = {**clean_defaults, **kwargs}
    
    dsm = None
    try:
        dsm = DataSourceManager.create(provider, market_type, **config)
        yield dsm
    finally:
        if dsm:
            dsm.close()


@contextlib.contextmanager
def get_quiet_market_data(
    provider: DataProvider = DataProvider.BINANCE,
    market_type: MarketType = MarketType.SPOT,
    **kwargs
) -> Iterator[DataSourceManager]:
    """Completely quiet DSM context manager for production use.
    
    This context manager provides a DSM instance that only shows errors
    and critical messages, making it ideal for production feature engineering
    where any logging output is unwanted.
    
    Args:
        provider: Data provider (default: BINANCE)
        market_type: Market type (default: SPOT)
        **kwargs: Additional parameters passed to DataSourceManager.create()
        
    Yields:
        DataSourceManager: Configured DSM instance with quiet logging
        
    Example:
        >>> from utils.for_demo.dsm_clean_logging import get_quiet_market_data
        >>> from utils.market_constraints import Interval
        >>> from datetime import datetime
        >>>
        >>> # Completely quiet - only errors will be shown
        >>> with get_quiet_market_data() as dsm:
        ...     data = dsm.get_data(
        ...         symbol="SOLUSDT",
        ...         start_time=datetime(2024, 1, 1, 12, 0),
        ...         end_time=datetime(2024, 1, 1, 12, 1),
        ...         interval=Interval.SECOND_1,
        ...     )
        ...     # Calculate microstructure features
        ...     realized_variance = data['close'].diff().pow(2).sum()
        ...     print(f"🧮 Realized variance: {realized_variance:.6f}")
    """
    # Set quiet mode parameters
    quiet_config = {
        'quiet_mode': True,
        'suppress_http_debug': True,
    }
    
    # Merge user kwargs with quiet defaults (user kwargs take precedence)
    config = {**quiet_config, **kwargs}
    
    dsm = None
    try:
        dsm = DataSourceManager.create(provider, market_type, **config)
        yield dsm
    finally:
        if dsm:
            dsm.close()


@contextlib.contextmanager
def get_debug_market_data(
    provider: DataProvider = DataProvider.BINANCE,
    market_type: MarketType = MarketType.SPOT,
    **kwargs
) -> Iterator[DataSourceManager]:
    """Debug DSM context manager for troubleshooting.
    
    This context manager provides a DSM instance with full debug logging
    enabled, including HTTP request details, for troubleshooting issues.
    
    Args:
        provider: Data provider (default: BINANCE)
        market_type: Market type (default: SPOT)
        **kwargs: Additional parameters passed to DataSourceManager.create()
        
    Yields:
        DataSourceManager: Configured DSM instance with debug logging
        
    Example:
        >>> from utils.for_demo.dsm_clean_logging import get_debug_market_data
        >>> from utils.market_constraints import Interval
        >>> from datetime import datetime
        >>>
        >>> # Full debug mode - see all HTTP requests and responses
        >>> with get_debug_market_data() as dsm:
        ...     data = dsm.get_data(
        ...         symbol="BTCUSDT",
        ...         start_time=datetime(2024, 1, 1),
        ...         end_time=datetime(2024, 1, 2),
        ...         interval=Interval.MINUTE_1,
        ...     )
        ...     print(f"🔍 Debug: Retrieved {len(data)} records with full logging")
    """
    # Set debug mode parameters
    debug_config = {
        'log_level': 'DEBUG',
        'suppress_http_debug': False,
        'quiet_mode': False,
    }
    
    # Merge user kwargs with debug defaults (user kwargs take precedence)
    config = {**debug_config, **kwargs}
    
    dsm = None
    try:
        dsm = DataSourceManager.create(provider, market_type, **config)
        yield dsm
    finally:
        if dsm:
            dsm.close()


def suppress_http_logging() -> None:
    """Suppress HTTP logging globally for all loggers.
    
    This function can be called at the module level to suppress HTTP
    logging for the entire session, providing a simple workaround
    until users migrate to the new DSM logging parameters.
    
    This is the exact workaround mentioned in the user's report.
    
    Example:
        >>> from utils.for_demo.dsm_clean_logging import suppress_http_logging
        >>> from core.sync.data_source_manager import DataSourceManager
        >>> from utils.market_constraints import DataProvider, MarketType
        >>>
        >>> # Apply global HTTP logging suppression
        >>> suppress_http_logging()
        >>>
        >>> # Now all DSM usage will have clean output
        >>> dsm = DataSourceManager.create(DataProvider.BINANCE, MarketType.SPOT)
        >>> # ... use dsm normally with clean output
    """
    # Suppress noisy HTTP logging globally
    logging.getLogger('httpcore').setLevel(logging.WARNING)
    logging.getLogger('httpx').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('requests').setLevel(logging.WARNING)


def configure_clean_logging(log_level: str = 'WARNING') -> None:
    """Configure clean logging for DSM usage.
    
    This function provides a simple way to configure clean logging
    for DSM without needing to pass parameters to every create() call.
    
    Args:
        log_level: Logging level to use for DSM operations
        
    Example:
        >>> from utils.for_demo.dsm_clean_logging import configure_clean_logging
        >>> from core.sync.data_source_manager import DataSourceManager
        >>> from utils.market_constraints import DataProvider, MarketType
        >>>
        >>> # Configure clean logging once
        >>> configure_clean_logging('INFO')
        >>>
        >>> # All subsequent DSM instances will have clean output
        >>> dsm = DataSourceManager.create(DataProvider.BINANCE, MarketType.SPOT)
    """
    from utils.loguru_setup import logger
    
    # Configure DSM logging level
    logger.configure_level(log_level)
    
    # Suppress HTTP logging
    suppress_http_logging()


# Convenience function for backward compatibility
def clean_dsm_context():
    """Alias for get_clean_market_data() for backward compatibility."""
    return get_clean_market_data()


if __name__ == "__main__":
    """Demo script showing clean logging utilities."""
    import pendulum
    from rich.console import Console
    from utils.market_constraints import Interval
    
    console = Console()
    
    console.print("🧪 [bold]DSM Clean Logging Demo[/bold]")
    console.print()
    
    # Demo 1: Clean logging context manager
    console.print("📡 [bold cyan]Demo 1: Clean Market Data Context[/bold cyan]")
    start_time = pendulum.now().subtract(days=2)
    end_time = start_time.add(minutes=5)
    
    with get_clean_market_data() as dsm:
        console.print(f"Fetching BTCUSDT data from {start_time.format('YYYY-MM-DD HH:mm')} to {end_time.format('YYYY-MM-DD HH:mm')}")
        
        data = dsm.get_data(
            symbol="BTCUSDT",
            start_time=start_time.to_datetime_string(),
            end_time=end_time.to_datetime_string(),
            interval=Interval.MINUTE_1,
        )
        
        console.print(f"✅ Retrieved {len(data)} records with clean output")
        console.print()
    
    # Demo 2: Quiet mode for feature engineering
    console.print("🔇 [bold cyan]Demo 2: Quiet Mode for Feature Engineering[/bold cyan]")
    start_time = pendulum.now().subtract(days=1)
    end_time = start_time.add(seconds=60)
    
    with get_quiet_market_data() as dsm:
        console.print(f"Fetching SOLUSDT microstructure data...")
        
        data = dsm.get_data(
            symbol="SOLUSDT",
            start_time=start_time.to_datetime_string(),
            end_time=end_time.to_datetime_string(),
            interval=Interval.SECOND_1,
        )
        
        if len(data) > 0:
            # Calculate sample microstructure features
            realized_variance = data['close'].diff().pow(2).sum()
            buy_pressure = (data['close'] > data['open']).mean()
            
            console.print(f"🧮 Sample features from {len(data)} 1-second bars:")
            console.print(f"   realized_variance: {realized_variance:.6f}")
            console.print(f"   buy_pressure_ratio: {buy_pressure:.6f}")
        else:
            console.print("⚠️  No data retrieved (this is normal for very recent data)")
        
        console.print()
    
    console.print("✨ [bold green]Demo completed - notice the clean output![/bold green]") 