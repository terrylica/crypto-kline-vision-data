#!/usr/bin/env python
"""
Test script to verify minimum viable cleanup for HTTP clients.

This script tests the essential cleanup functions to ensure they work
correctly without redundancy:

1. safely_close_client from network_utils.py - The primary cleanup function
2. cleanup_client from async_cleanup.py - Wrapper around safely_close_client
3. _cancel_force_timeout_tasks from async_cleanup.py - Utility to cancel timeout tasks

The goal is to determine if all these functions are necessary or if some
are redundant and can be removed.
"""

import asyncio
import gc
import time
from datetime import datetime, timezone, timedelta

from core.data_source_manager import DataSourceManager, DataSource
from utils.market_constraints import MarketType, Interval
from utils.logger_setup import logger
from utils.network_utils import create_client, safely_close_client
from utils.async_cleanup import cleanup_client, cleanup_all_force_timeout_tasks
from rich import print

# Set up logging
logger.setup_root(level="DEBUG", show_filename=True)


async def test_safely_close_client():
    """Test the safely_close_client function directly."""
    logger.info("===== TESTING safely_close_client() =====")
    
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


async def test_cleanup_client():
    """Test the cleanup_client function."""
    logger.info("===== TESTING cleanup_client() =====")
    
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
    
    # Use cleanup_client to clean up
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
    logger.info(f"cleanup_client() completed in {elapsed:.3f}s")


async def test_cleanup_all_force_timeout_tasks():
    """Test the cleanup_all_force_timeout_tasks function."""
    logger.info("===== TESTING cleanup_all_force_timeout_tasks() =====")
    
    # Create clients that will have force_timeout tasks
    clients = [create_client(timeout=30.0) for _ in range(3)]
    
    # Make simple requests to ensure the clients have active connections
    for i, client in enumerate(clients):
        try:
            resp = await client.get("https://api.binance.com/api/v3/ping")
            logger.info(f"Made test request with client {i}, status: {resp.status_code}")
        except Exception as e:
            logger.error(f"Error making test request with client {i}: {e}")
    
    # Log active tasks before cleanup
    force_timeout_tasks = []
    for task in asyncio.all_tasks():
        task_str = str(task)
        if "_force_timeout" in task_str and not task.done():
            force_timeout_tasks.append(task)
    
    logger.info(f"Found {len(force_timeout_tasks)} active force_timeout tasks before cleanup")
    
    # Use cleanup_all_force_timeout_tasks to clean up
    start = time.time()
    tasks_cleaned = await cleanup_all_force_timeout_tasks()
    elapsed = time.time() - start
    
    logger.info(f"cleanup_all_force_timeout_tasks() cleaned {tasks_cleaned} tasks in {elapsed:.3f}s")
    
    # Manually clean up the clients
    for i, client in enumerate(clients):
        try:
            client.close()
        except Exception as e:
            logger.error(f"Error closing client {i}: {e}")


async def test_with_data_source_manager():
    """Test cleanup with DataSourceManager."""
    logger.info("===== TESTING CLEANUP WITH DataSourceManager =====")
    
    # Create manager
    manager = None
    try:
        manager = DataSourceManager(market_type=MarketType.SPOT, use_cache=False)
        
        # Define symbols and time range
        symbol = "ETHUSDT"
        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(hours=1)
        
        logger.info(f"Time range for verification: {start_time} to {end_time}")
        
        # Retrieve data
        start_op = time.time()
        df = await asyncio.wait_for(
            manager.get_data(
                symbol=symbol,
                start_time=start_time,
                end_time=end_time,
                interval=Interval.MINUTE_1,
                enforce_source=DataSource.REST,
            ),
            timeout=30,  # 30-second timeout
        )
        elapsed = time.time() - start_op
        
        if df.empty:
            logger.warning(f"No data retrieved for {symbol}")
        else:
            logger.info(
                f"Successfully retrieved {len(df)} records for {symbol} in {elapsed:.2f}s"
            )
            print(f"\nDataFrame for {symbol}:")
            print(df.head())
            print("=" * 80)
    
    except Exception as e:
        logger.error(f"Error in test with DataSourceManager: {str(e)}")
    finally:
        # Check active tasks before cleanup
        force_timeout_tasks_before = []
        for task in asyncio.all_tasks():
            task_str = str(task)
            if "_force_timeout" in task_str and not task.done():
                force_timeout_tasks_before.append(task)
        
        logger.info(f"Found {len(force_timeout_tasks_before)} active force_timeout tasks before cleanup")
        
        # Ensure proper cleanup
        if manager:
            try:
                start = time.time()
                await manager.__aexit__(None, None, None)
                elapsed = time.time() - start
                logger.info(f"DataSourceManager cleanup completed in {elapsed:.3f}s")
            except Exception as e:
                logger.error(f"Error during manager cleanup: {str(e)}")
        
        # Check active tasks after cleanup
        force_timeout_tasks_after = []
        for task in asyncio.all_tasks():
            task_str = str(task)
            if "_force_timeout" in task_str and not task.done():
                force_timeout_tasks_after.append(task)
        
        logger.info(f"Found {len(force_timeout_tasks_after)} active force_timeout tasks after cleanup")


async def main():
    """Run all tests."""
    logger.info("===== STARTING CLEANUP FUNCTION TESTS =====")
    logger.info(f"Current time: {datetime.now(timezone.utc)}")
    
    try:
        await test_safely_close_client()
    except Exception as e:
        logger.error(f"Error during safely_close_client test: {str(e)}")
    
    try:
        await test_cleanup_client()
    except Exception as e:
        logger.error(f"Error during cleanup_client test: {str(e)}")
    
    try:
        await test_cleanup_all_force_timeout_tasks()
    except Exception as e:
        logger.error(f"Error during cleanup_all_force_timeout_tasks test: {str(e)}")
    
    try:
        await test_with_data_source_manager()
    except Exception as e:
        logger.error(f"Error during DataSourceManager test: {str(e)}")
    
    logger.info("===== CLEANUP FUNCTION TESTS COMPLETE =====")


if __name__ == "__main__":
    # Run the tests
    asyncio.run(main()) 