#!/usr/bin/env python3
# ADR: docs/adr/2026-01-30-claude-code-infrastructure.md
"""Application paths management.

This module provides centralized management of application data directories,
cache locations, and other filesystem paths using platformdirs for
cross-platform compatibility.
"""

import os
from pathlib import Path

import platformdirs

from data_source_manager.utils.loguru_setup import logger

# Application information
APP_NAME = "data-source-manager"
APP_AUTHOR = "eon-labs"

# Environment variable names for overriding default paths
ENV_VAR_PREFIX = "RDS"  # Data Source Manager
ENV_CACHE_DIR = f"{ENV_VAR_PREFIX}_CACHE_DIR"
ENV_DATA_DIR = f"{ENV_VAR_PREFIX}_DATA_DIR"
ENV_CONFIG_DIR = f"{ENV_VAR_PREFIX}_CONFIG_DIR"
ENV_LOG_DIR = f"{ENV_VAR_PREFIX}_LOG_DIR"


def get_cache_dir() -> Path:
    """Get the application cache directory.

    Uses platformdirs to determine the appropriate location for the current platform,
    and respects environment variables for overriding the default location.

    Returns:
        Path: The path to the cache directory
    """
    # Check if environment variable is set
    env_cache_dir = os.environ.get(ENV_CACHE_DIR)
    if env_cache_dir:
        cache_dir = Path(env_cache_dir)
        logger.debug(f"Using cache directory from environment: {cache_dir}")
        return cache_dir

    # Get platform-specific cache directory
    cache_dir = Path(platformdirs.user_cache_path(APP_NAME, APP_AUTHOR))
    logger.debug(f"Using platform cache directory: {cache_dir}")
    return cache_dir


def get_log_dir() -> Path:
    """Get the application log directory.

    Returns:
        Path: Path to the log directory
    """
    # Check if environment variable is set
    env_log_dir = os.environ.get(ENV_LOG_DIR)
    if env_log_dir:
        log_dir = Path(env_log_dir)
        logger.debug(f"Using log directory from environment: {log_dir}")
        return log_dir

    # Use platform-specific log directory
    log_dir = Path(platformdirs.user_log_path(APP_NAME, APP_AUTHOR))
    logger.debug(f"Using platform log directory: {log_dir}")
    return log_dir


def get_data_dir() -> Path:
    """Get the application data directory.

    Returns:
        Path: Path to the data directory
    """
    # Check if environment variable is set
    env_data_dir = os.environ.get(ENV_DATA_DIR)
    if env_data_dir:
        data_dir = Path(env_data_dir)
        logger.debug(f"Using data directory from environment: {data_dir}")
        return data_dir

    # Use platform-specific data directory
    data_dir = Path(platformdirs.user_data_path(APP_NAME, APP_AUTHOR))
    logger.debug(f"Using platform data directory: {data_dir}")
    return data_dir


def get_config_dir() -> Path:
    """Get the application config directory.

    Returns:
        Path: Path to the config directory
    """
    # Check if environment variable is set
    env_config_dir = os.environ.get(ENV_CONFIG_DIR)
    if env_config_dir:
        config_dir = Path(env_config_dir)
        logger.debug(f"Using config directory from environment: {config_dir}")
        return config_dir

    # Use platform-specific config directory
    config_dir = Path(platformdirs.user_config_path(APP_NAME, APP_AUTHOR))
    logger.debug(f"Using platform config directory: {config_dir}")
    return config_dir


def get_market_cache_dir(market_type: str, provider: str = "binance", chart_type: str = "klines") -> Path:
    """Get the cache directory for specific market data.

    This ensures consistency in how market data is organized in the cache,
    following the same structure regardless of where the base cache directory is.

    Args:
        market_type: The market type (spot, um, cm)
        provider: The data provider (default: binance)
        chart_type: The chart type (default: klines)

    Returns:
        Path: The full path to the market cache directory
    """
    # Map market type to directory structure
    market_path = market_type.lower()
    if market_path in ("futures_usdt", "um"):
        market_path = "futures/um"
    elif market_path in ("futures_coin", "cm"):
        market_path = "futures/cm"
    elif market_path in ("spot"):
        market_path = "spot"

    # Get base cache directory and construct full path
    cache_dir = get_cache_dir()
    market_cache_dir = cache_dir / "data" / provider.lower() / market_path / "daily" / chart_type.lower()

    # Ensure the directory exists
    market_cache_dir.mkdir(parents=True, exist_ok=True)

    return market_cache_dir


def ensure_dir_exists(path: Path) -> Path:
    """Ensure a directory exists, creating it if necessary.

    Args:
        path: The path to ensure exists

    Returns:
        Path: The same path, after ensuring it exists
    """
    path.mkdir(parents=True, exist_ok=True)
    return path


def create_app_dirs() -> dict[str, Path]:
    """Create all application directories if they don't exist.

    Returns:
        Dict[str, Path]: Dictionary of created directories
    """
    dirs = {
        "cache": ensure_dir_exists(get_cache_dir()),
        "data": ensure_dir_exists(get_data_dir()),
        "logs": ensure_dir_exists(get_log_dir()),
        "config": ensure_dir_exists(get_config_dir()),
    }

    logger.debug(f"Application directories created/verified: {dirs}")
    return dirs
