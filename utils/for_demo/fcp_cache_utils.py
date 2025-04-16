#!/usr/bin/env python3
"""
Cache utilities for the Failover Control Protocol (FCP) mechanism.
"""

import shutil
from pathlib import Path
from utils.logger_setup import logger
from rich import print


def clear_cache_directory(cache_dir):
    """
    Remove the cache directory and its contents.

    Args:
        cache_dir: Path object pointing to the cache directory
    """
    if cache_dir.exists():
        logger.info(f"Clearing cache directory: {cache_dir}")
        print(f"[bold yellow]Removing cache directory: {cache_dir}[/bold yellow]")
        shutil.rmtree(cache_dir, ignore_errors=True)
        print(f"[bold green]Cache directory removed successfully[/bold green]")
    else:
        logger.info(f"Cache directory does not exist: {cache_dir}")
        print(f"[bold yellow]Cache directory does not exist: {cache_dir}[/bold yellow]")
