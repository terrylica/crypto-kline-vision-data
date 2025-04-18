#!/usr/bin/env python3
"""
Utility functions for managing cache in DSM Demo applications.

This module provides functions for cache directory management and verification
for the Failover Control Protocol (FCP) demonstrations.
"""

import os
import shutil
from pathlib import Path
from utils.logger_setup import logger
from rich import print

# Default cache directory
DEFAULT_CACHE_DIR = Path("./cache")


def clear_cache_directory(cache_dir=DEFAULT_CACHE_DIR):
    """Remove the cache directory and its contents.

    Args:
        cache_dir: Path to the cache directory (default: ./cache)

    Returns:
        bool: True if operation was successful, False otherwise
    """
    cache_path = Path(cache_dir)
    if cache_path.exists():
        logger.info(f"Clearing cache directory: {cache_path}")
        print(f"[bold yellow]Removing cache directory: {cache_path}[/bold yellow]")
        try:
            shutil.rmtree(cache_path, ignore_errors=True)
            print(f"[bold green]Cache directory removed successfully[/bold green]")
            return True
        except Exception as e:
            logger.error(f"Error removing cache directory: {e}")
            print(f"[bold red]Error removing cache directory: {e}[/bold red]")
            return False
    else:
        logger.info(f"Cache directory does not exist: {cache_path}")
        print(
            f"[bold yellow]Cache directory does not exist: {cache_path}[/bold yellow]"
        )
        return False


def ensure_cache_directory(cache_dir=DEFAULT_CACHE_DIR):
    """Ensure that the cache directory exists.

    Args:
        cache_dir: Path to the cache directory (default: ./cache)

    Returns:
        Path: Path object for the created/existing cache directory
    """
    cache_path = Path(cache_dir)
    cache_path.mkdir(parents=True, exist_ok=True)
    logger.debug(f"Ensured cache directory exists: {cache_path}")
    return cache_path


def verify_project_root():
    """Verify that we're running from the project root directory.

    Returns:
        bool: True if running from project root, False otherwise
    """
    if os.path.isdir("core") and os.path.isdir("utils") and os.path.isdir("examples"):
        # Already in project root
        print("Running from project root directory")
        return True

    # Try to navigate to project root if we're in the example directory
    if os.path.isdir("../../core") and os.path.isdir("../../utils"):
        os.chdir("../..")
        print(f"Changed to project root directory: {os.getcwd()}")
        return True

    print("[bold red]Error: Unable to locate project root directory[/bold red]")
    print(
        "Please run this script from either the project root or the examples/sync directory"
    )
    return False
