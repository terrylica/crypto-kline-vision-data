#!/usr/bin/env python
"""
Test script to verify the simplified cleanup_client function.

This script tests the simplified cleanup_client implementation to ensure
it effectively cleans up HTTP clients and their associated timeout tasks.
"""

import asyncio
import time
from datetime import datetime, timezone

from utils.logger_setup import logger
from utils.network_utils import create_client, safely_close_client
from utils.async_cleanup import cleanup_client
from rich import print

# Set up logging
logger.setup_root(level="DEBUG", show_filename=True)


async def test_original_safely_close_client():
    """Test the original safely_close_client function directly for comparison."""
    logger.info("===== TESTING original safely_close_client() =====")
    
    # Create a client
    client = create_client(timeout=30.0)
    
    # Make a simple request to ensure the client has active connections
    try:
        resp = await client.get("https://api.binance.com/api/v3/ping")
        logger.info(f"Made test request, status: {resp.status_code}")
    except Exception as e:
        logger.error(f"Error making test request: {e}")
    
    # Log active tasks before cleanup
    force_timeout_tasks = []
    for task in asyncio.all_tasks():
        task_str = str(task)
        if "_force_timeout" in task_str and not task.done():
            force_timeout_tasks.append(task)
    
    logger.info(f"Found {len(force_timeout_tasks)} active force_timeout tasks before cleanup")
    
    # Use safely_close_client to clean up
    start = time.time()
    await safely_close_client(client)
    elapsed = time.time() - start
    
    # Log active tasks after cleanup
    force_timeout_tasks = []
    for task in asyncio.all_tasks():
        task_str = str(task)
        if "_force_timeout" in task_str and not task.done():
            force_timeout_tasks.append(task)
    
    logger.info(f"Found {len(force_timeout_tasks)} active force_timeout tasks after cleanup")
    logger.info(f"safely_close_client() completed in {elapsed:.3f}s")


async def test_simplified_cleanup_client():
    """Test the simplified cleanup_client function."""
    logger.info("===== TESTING simplified cleanup_client() =====")
    
    # Create a client
    client = create_client(timeout=30.0)
    
    # Make a simple request to ensure the client has active connections
    try:
        resp = await client.get("https://api.binance.com/api/v3/ping")
        logger.info(f"Made test request, status: {resp.status_code}")
    except Exception as e:
        logger.error(f"Error making test request: {e}")
    
    # Log active tasks before cleanup
    force_timeout_tasks = []
    for task in asyncio.all_tasks():
        task_str = str(task)
        if "_force_timeout" in task_str and not task.done():
            force_timeout_tasks.append(task)
    
    logger.info(f"Found {len(force_timeout_tasks)} active force_timeout tasks before cleanup")
    
    # Use simplified cleanup_client to clean up
    start = time.time()
    await cleanup_client(client)
    elapsed = time.time() - start
    
    # Log active tasks after cleanup
    force_timeout_tasks = []
    for task in asyncio.all_tasks():
        task_str = str(task)
        if "_force_timeout" in task_str and not task.done():
            force_timeout_tasks.append(task)
    
    logger.info(f"Found {len(force_timeout_tasks)} active force_timeout tasks after cleanup")
    logger.info(f"simplified cleanup_client() completed in {elapsed:.3f}s")


async def main():
    """Run all tests."""
    logger.info("===== STARTING SIMPLIFIED CLEANUP TEST =====")
    logger.info(f"Current time: {datetime.now(timezone.utc)}")
    
    try:
        await test_original_safely_close_client()
    except Exception as e:
        logger.error(f"Error during original safely_close_client test: {str(e)}")
    
    try:
        await test_simplified_cleanup_client()
    except Exception as e:
        logger.error(f"Error during simplified cleanup_client test: {str(e)}")
    
    logger.info("===== SIMPLIFIED CLEANUP TEST COMPLETE =====")


if __name__ == "__main__":
    # Run the tests
    asyncio.run(main()) 