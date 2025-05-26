#!/usr/bin/env python3
"""
Utility functions for managing cache in DSM Demo applications.

This module provides functions for cache directory management and verification
for the Failover Control Protocol (FCP) demonstrations.
"""

import os
import shutil
from pathlib import Path

from rich import print

from utils.app_paths import (
    ENV_CACHE_DIR,
    create_app_dirs,
    get_cache_dir,
    get_market_cache_dir,
)
from utils.logger_setup import logger


def get_dsm_demo_cache_dir() -> Path:
    """Get the specific DSM demo cache directory.

    Returns:
        Path: The path to the DSM demo cache directory
    """
    cache_dir = get_cache_dir() / "dsm-demo"
    cache_dir.mkdir(parents=True, exist_ok=True)
    return cache_dir


def clear_cache_directory(cache_dir=None):
    """Remove the cache directory and its contents.

    Args:
        cache_dir: Path to the cache directory (default: platform-specific cache dir)

    Returns:
        bool: True if operation was successful, False otherwise
    """
    if cache_dir is None:
        cache_dir = get_dsm_demo_cache_dir()

    cache_path = Path(cache_dir)
    if cache_path.exists():
        logger.info(f"Clearing cache directory: {cache_path}")
        print(f"[bold yellow]Removing cache directory: {cache_path}[/bold yellow]")
        try:
            shutil.rmtree(cache_path, ignore_errors=True)
            print("[bold green]Cache directory removed successfully[/bold green]")
            return True
        except Exception as e:
            logger.error(f"Error removing cache directory: {e}")
            print(f"[bold red]Error removing cache directory: {e}[/bold red]")
            return False
    else:
        logger.info(f"Cache directory does not exist: {cache_path}")
        print(f"[bold yellow]Cache directory does not exist: {cache_path}[/bold yellow]")
        return False


def ensure_cache_directory(cache_dir=None):
    """Ensure that the cache directory exists.

    Args:
        cache_dir: Path to the cache directory (default: platform-specific cache dir)

    Returns:
        Path: Path object for the created/existing cache directory
    """
    if cache_dir is None:
        cache_dir = get_dsm_demo_cache_dir()

    cache_path = Path(cache_dir)
    cache_path.mkdir(parents=True, exist_ok=True)
    logger.debug(f"Ensured cache directory exists: {cache_path}")
    return cache_path


def clear_all_cache_directories():
    """Clear all application cache directories used by data-source-manager.

    This is more comprehensive than just clearing the dsm-demo directory,
    as it will also clear market data caches.

    Returns:
        bool: True if operation was successful
    """
    try:
        # Get the base cache directory
        base_cache_dir = get_cache_dir()
        if base_cache_dir.exists():
            logger.info(f"Clearing all cache directories under: {base_cache_dir}")
            print(f"[bold yellow]Removing all cache directories under: {base_cache_dir}[/bold yellow]")

            # Count cleared files and directories for reporting
            dirs_cleared = 0
            files_cleared = 0

            # Clear specific cache areas
            cache_paths = [
                base_cache_dir / "data",  # Market data cache
                base_cache_dir / "dsm-demo",  # DSM demo cache
            ]

            for path in cache_paths:
                if path.exists():
                    # Count the files and directories before removal
                    for _, _, files in os.walk(path):
                        files_cleared += len(files)
                    dirs_cleared += sum(1 for _ in path.glob("**/*") if _.is_dir())

                    # Remove the directory
                    shutil.rmtree(path, ignore_errors=True)
                    logger.info(f"Removed cache directory: {path}")

            print(f"[bold green]Cache cleared: {files_cleared} files in {dirs_cleared} directories[/bold green]")
            return True
        logger.info(f"Cache directory does not exist: {base_cache_dir}")
        print(f"[bold yellow]Cache directory does not exist: {base_cache_dir}[/bold yellow]")
        return True  # Return True because there's nothing to clear

    except Exception as e:
        logger.error(f"Error clearing all cache directories: {e}")
        print(f"[bold red]Error clearing all cache directories: {e}[/bold red]")
        return False


def verify_project_root():
    """Package-aware verification function that always returns True.

    This function exists for backward compatibility but no longer attempts to
    change directories or verify the location, as the package uses platformdirs
    to handle paths correctly regardless of installation method.

    Returns:
        bool: Always returns True
    """
    # Show current working directory in debug logs
    cwd = Path.cwd()
    logger.debug(f"Current working directory: {cwd}")

    # Initialize application directories
    app_dirs = create_app_dirs()
    logger.debug(f"Application directories: {app_dirs}")

    # Always return True, no longer checking for specific directories
    return True


def print_cache_info():
    """Print information about cache directories for user reference."""
    cache_dir = get_cache_dir()
    demo_cache_dir = get_dsm_demo_cache_dir()

    print("\n[bold blue]Cache Directory Information[/bold blue]")
    print(f"[cyan]Base cache directory:[/cyan] {cache_dir}")
    print(f"[cyan]DSM demo cache:[/cyan] {demo_cache_dir}")
    print(f"[cyan]Environment variable:[/cyan] {ENV_CACHE_DIR}")

    # Print if environment variable is set
    env_cache_dir = os.environ.get(ENV_CACHE_DIR)
    if env_cache_dir:
        print(f"[green]Using environment override: {env_cache_dir}[/green]")

    # Check if directories exist and print status
    print("\n[cyan]Directory Status:[/cyan]")
    print(
        f"• Base cache exists: [{'green' if cache_dir.exists() else 'red'}]{cache_dir.exists()}[/{'green' if cache_dir.exists() else 'red'}]"
    )
    print(
        f"• Demo cache exists: [{'green' if demo_cache_dir.exists() else 'red'}]{demo_cache_dir.exists()}[/{'green' if demo_cache_dir.exists() else 'red'}]"
    )

    # Show market cache examples
    spot_cache = get_market_cache_dir("spot")
    um_cache = get_market_cache_dir("um")
    cm_cache = get_market_cache_dir("cm")

    print("\n[cyan]Market Cache Directories:[/cyan]")
    print(f"• SPOT: {spot_cache}")
    print(f"• UM Futures: {um_cache}")
    print(f"• CM Futures: {cm_cache}")

    print("\n[yellow]Note: Use the -cc flag to clear cache directories[/yellow]")
