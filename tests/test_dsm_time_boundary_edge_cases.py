#!/usr/bin/env python
"""Comprehensive boundary tests for DataSourceManager with 1-second data.

Focuses on edge cases and temporal relationships with detailed logging.

This file concentrates on specific edge case behaviors at time boundaries:
1. Handling of microsecond precision at boundaries
2. Cross-minute/cross-hour boundaries
3. Zero-duration and near-zero requests
4. Alignment rules application in practice

Related tests:
- For general timestamp format/precision: see test_data_source_manager_format_precision.py
- For year boundary tests: see test_data_source_manager_year_boundary.py
- For date validation: see test_data_source_manager_consolidated.py
"""

import pytest
import arrow
import pandas as pd
from datetime import datetime, timedelta, timezone
from typing import Dict, Any
import pytest_asyncio

from utils.logger_setup import get_logger
from core.data_source_manager import DataSourceManager
from utils.market_constraints import Interval, MarketType

logger = get_logger(__name__, "INFO", show_path=False, rich_tracebacks=True)

# Test configuration
TEST_SYMBOL = "BTCUSDT"
TEST_INTERVAL = Interval.SECOND_1  # Only supported interval


def log_test_case_header(case_number: int, description: str) -> None:
    """Log test case header with visual separation."""
    logger.info("")
    logger.info("=" * 80)
    logger.info(f"TEST CASE {case_number}: {description}")
    logger.info("-" * 80)


def log_validation_results(results: Dict[str, Any]) -> None:
    """Log validation results in human-readable format."""
    logger.info("Validation Results:")
    for key, value in results.items():
        if isinstance(value, pd.Timestamp):
            logger.info(f"  - {key}: {value.isoformat()}")
        elif isinstance(value, timedelta):
            logger.info(f"  - {key}: {value} ({(value.total_seconds()):.2f} seconds)")
        else:
            logger.info(f"  - {key}: {value}")
    logger.info("=" * 80)


@pytest_asyncio.fixture
async def manager():
    """Create DataSourceManager instance with fresh components."""
    async with DataSourceManager(market_type=MarketType.SPOT) as mgr:
        yield mgr


@pytest.mark.real
@pytest.mark.asyncio
async def test_boundary_conditions(manager: DataSourceManager):
    """Test various temporal boundary conditions with detailed explanations."""
    current_time = arrow.utcnow()
    test_cases = [
        {
            "case_number": 1,
            "description": "Full second alignment",
            "start": current_time.shift(seconds=-10, microseconds=-500_000),
            "end": current_time.shift(seconds=-5, microseconds=-250_000),
            "expected_records": 5,  # 5 records: [49,50,51,52,53] inclusive after rounding
        },
        {
            "case_number": 2,
            "description": "Microsecond-aligned start",
            "start": current_time.shift(seconds=-5, microseconds=-500_000),
            "end": current_time.shift(seconds=-2, microseconds=-250_000),
            "expected_records": 3,  # 3 records: [54,55,56] inclusive after rounding
        },
        {
            "case_number": 3,
            "description": "Cross-minute boundary",
            "start": current_time.shift(seconds=-15, microseconds=-500_000),
            "end": current_time.shift(seconds=-5, microseconds=+750_000),
            "expected_records": 11,  # 11 records: [58,59,00,01,02,03,04,05,06,07,08] inclusive after rounding
        },
        {
            "case_number": 4,
            "description": "Zero-duration request",
            "start": current_time.shift(seconds=-1),
            "end": current_time.shift(seconds=-1),
            "expected_records": 0,
        },
    ]

    for case in test_cases:
        # Convert Arrow times to datetime objects
        start_time = case["start"].datetime
        end_time = case["end"].datetime

        log_test_case_header(case["case_number"], case["description"])
        logger.info("Input Parameters:")
        logger.info(f"  Start: {start_time.isoformat()}")
        logger.info(f"  End:   {end_time.isoformat()}")
        logger.info(f"\nExpected Behavior:")
        logger.info(f"  - Time range: {end_time - start_time} duration")
        logger.info(f"  - Expected records: {case['expected_records']}")
        logger.info(
            f"  - First record should be >= {start_time.replace(microsecond=0).isoformat()}"
        )
        logger.info(
            f"  - Last record should be <= {end_time.replace(microsecond=0).isoformat()}"
        )  # Updated to <= instead of <

        try:
            # Execute data fetch
            if case["case_number"] == 4:  # Zero-duration request
                with pytest.raises(
                    ValueError, match="Start date must be before end date"
                ):
                    df = await manager.get_data(
                        symbol=TEST_SYMBOL,
                        start_time=start_time,
                        end_time=end_time,
                        use_cache=False,
                    )
                logger.info("Zero-duration request correctly raised ValueError")
                continue  # Skip the rest of the validation for this case

            df = await manager.get_data(
                symbol=TEST_SYMBOL,
                start_time=start_time,
                end_time=end_time,
                use_cache=False,
            )

            # Log adjusted time window
            logger.info("Adjusted time window for proper alignment:")
            logger.info(
                f"Original:  {start_time.isoformat()} -> {end_time.isoformat()}"
            )
            if not df.empty:
                # Convert index values to datetime using pandas datetime accessor
                first_time = df.index[0].to_pydatetime()  # type: ignore
                last_time = df.index[-1].to_pydatetime()  # type: ignore
                logger.info(
                    f"Actual:    {first_time.isoformat()} -> {last_time.isoformat()}"
                )
            else:
                logger.info("Actual:    No data returned")

            # Validate results
            validation = {
                "First timestamp": df.index[0] if not df.empty else "N/A",
                "Last timestamp": df.index[-1] if not df.empty else "N/A",
                "Record count": len(df),
                "Actual duration": (
                    df.index[-1] - df.index[0] if len(df) > 1 else timedelta(0)
                ),
                "Status": "PASSED" if len(df) == case["expected_records"] else "FAILED",
            }

            # Additional boundary checks
            if not df.empty:
                validation["First >= start"] = df.index[0] >= start_time.replace(
                    microsecond=0
                )
                validation["Last <= end"] = df.index[-1] <= end_time.replace(
                    microsecond=0
                )  # Updated to <= instead of <
                validation["No future data"] = df.index[-1] < datetime.now(timezone.utc)

            log_validation_results(validation)

            # Final assertions
            assert len(df) == case["expected_records"], "Record count mismatch"
            if not df.empty:
                assert df.index[0] >= start_time.replace(
                    microsecond=0
                ), "First timestamp too early"
                assert df.index[-1] <= end_time.replace(
                    microsecond=0
                ), "Last timestamp too late"  # Updated to <= instead of <
                assert df.index[-1] < datetime.now(timezone.utc), "Contains future data"

        except Exception as e:
            if (
                case["case_number"] == 4
                and isinstance(e, ValueError)
                and str(e) == "Start date must be before end date"
            ):
                logger.info("Zero-duration request correctly raised ValueError")
            else:
                logger.error(f"Test failed: {str(e)}")
                raise

    # Special case: Empty result validation
    empty_case = test_cases[3]
    start_time = empty_case["start"].datetime
    end_time = empty_case["end"].datetime

    with pytest.raises(ValueError, match="Start date must be before end date"):
        df = await manager.get_data(
            symbol=TEST_SYMBOL,
            start_time=start_time,
            end_time=end_time,
            use_cache=False,
        )

    logger.info("\nTesting Empty Result Handling:")
    logger.info(f"Requested range: {start_time.isoformat()} to {end_time.isoformat()}")
    logger.info("Expected ValueError raised successfully")
