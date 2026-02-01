# polars-exception: Debug utilities work with pandas DataFrames for timestamp analysis
# ADR: docs/adr/2026-01-30-claude-code-infrastructure.md
"""TIMEZONE-AWARE FAIL-FAST TIMESTAMP DEBUGGING UTILITIES.

Philosophy: FAIL-FAST with MAXIMUM DEBUG INFORMATION
- No failovers, no failsafes - precise exceptions with full context
- Timezone-aware logging for datetime confusion elimination
- Rich exception details for rapid debugging
- Aggressive validation with informative error messages

For TiRex Context Stability experiments and DSM time filtering operations.
"""

import pandas as pd
from datetime import datetime
from typing import Union

# Use DSM's loguru setup for consistent logging
from data_source_manager.utils.loguru_setup import logger


class TimezoneDebugError(Exception):
    """Specialized exception for timezone debugging with rich context."""

    def __init__(self, message: str, context: dict | None = None) -> None:
        """Initialize TimezoneDebugError with message and context.

        Args:
            message: Error description.
            context: Additional debugging context dictionary.
        """
        self.context = context or {}
        detailed_msg = f"TIMEZONE DEBUG ERROR: {message}"
        if self.context:
            detailed_msg += f"\nCONTEXT: {self.context}"
        super().__init__(detailed_msg)


def _format_timezone_info(dt: Union[datetime, pd.Timestamp]) -> str:
    """Format datetime with detailed timezone information for debugging."""
    if pd.isna(dt):
        return "NaT (Not-a-Time)"

    if hasattr(dt, "tz") and dt.tz is not None:
        tz_name = str(dt.tz)
        utc_offset = dt.strftime("%z")
        return f"{dt.isoformat()} [{tz_name} UTC{utc_offset}]"
    if hasattr(dt, "tzinfo") and dt.tzinfo is not None:
        tz_name = str(dt.tzinfo)
        utc_offset = dt.strftime("%z")
        return f"{dt.isoformat()} [{tz_name} UTC{utc_offset}]"
    return f"{dt.isoformat()} [NAIVE-NO-TIMEZONE] ⚠️"


def _get_timestamp_series(df: pd.DataFrame, time_column: str):
    """Helper to get timestamp series whether it's a column or index."""
    if time_column in df.columns:
        return df[time_column]
    return df.index

def trace_dataframe_timestamps(
    df: pd.DataFrame,
    time_column: str,
    start_time: Union[datetime, pd.Timestamp],
    end_time: Union[datetime, pd.Timestamp]
) -> None:
    """FAIL-FAST timezone-aware timestamp tracing with rich debug information.

    NO GRACEFUL DEGRADATION - Fails immediately with full context on issues.
    """
    if df.empty:
        raise TimezoneDebugError(
            "DataFrame is empty - cannot trace timestamps",
            {"time_column": time_column, "start_time": start_time, "end_time": end_time}
        )

    # FAIL-FAST: Validate time column exists (column or index)
    if time_column not in df.columns and df.index.name != time_column:
        available_columns = list(df.columns)
        raise TimezoneDebugError(
            f"Time column '{time_column}' not found in DataFrame columns or index",
            {
                "requested_column": time_column,
                "available_columns": available_columns,
                "dataframe_shape": df.shape,
                "dataframe_index_name": df.index.name,
                "suggestion": f"Use index.name='{df.index.name}' if timestamps are in index"
            }
        )

    # Get timezone information for all timestamps (unified access)
    timestamp_series = _get_timestamp_series(df, time_column)

    if time_column in df.columns:
        sample_timestamp = timestamp_series.iloc[0] if len(df) > 0 else None
    else:
        # Handle DatetimeIndex - use different access methods
        sample_timestamp = timestamp_series[0] if len(df) > 0 else None

    min_ts = timestamp_series.min()
    max_ts = timestamp_series.max()

    # Rich timezone-aware logging
    logger.debug("🕐 [TIMEZONE TRACE] TIMESTAMP FILTERING OPERATION")
    logger.debug(f"📊 [TIMEZONE TRACE] DataFrame: {len(df)} rows, column='{time_column}'")
    logger.debug("🎯 [TIMEZONE TRACE] Filter Range:")
    logger.debug(f"  ▶️  START: {_format_timezone_info(start_time)}")
    logger.debug(f"  ▶️  END:   {_format_timezone_info(end_time)}")
    logger.debug("🗃️  [TIMEZONE TRACE] Data Range:")
    logger.debug(f"  📈 MIN:   {_format_timezone_info(min_ts)}")
    logger.debug(f"  📈 MAX:   {_format_timezone_info(max_ts)}")

    # FAIL-FAST: Detect timezone mismatches
    start_tz = getattr(start_time, "tzinfo", None) or getattr(start_time, "tz", None)
    end_tz = getattr(end_time, "tzinfo", None) or getattr(end_time, "tz", None)
    data_tz = getattr(sample_timestamp, "tzinfo", None) or getattr(sample_timestamp, "tz", None)

    if start_tz != end_tz:
        raise TimezoneDebugError(
            "Start and end times have different timezones",
            {
                "start_timezone": str(start_tz),
                "end_timezone": str(end_tz),
                "start_time": _format_timezone_info(start_time),
                "end_time": _format_timezone_info(end_time)
            }
        )

    if start_tz is None and data_tz is not None:
        logger.warning("⚠️  [TIMEZONE TRACE] TIMEZONE MISMATCH DETECTED:")
        logger.warning("  🚨 Filter times are NAIVE (no timezone)")
        logger.warning(f"  🕐 Data timestamps have timezone: {data_tz}")
        logger.warning("  ⚡ This may cause filtering inconsistencies!")

    # Log exact boundary analysis (unified approach)
    timestamp_series = _get_timestamp_series(df, time_column)
    exact_start_matches = df[timestamp_series == start_time]
    exact_end_matches = df[timestamp_series == end_time]

    logger.debug("🎯 [TIMEZONE TRACE] Boundary Matches:")
    logger.debug(f"  ✅ Exact START matches: {len(exact_start_matches)}")
    logger.debug(f"  ✅ Exact END matches:   {len(exact_end_matches)}")

    # Sample timestamp logging with timezone details
    sample_size = min(3, len(df))
    logger.debug(f"📋 [TIMEZONE TRACE] Sample Timestamps (first {sample_size}):")
    for i in range(sample_size):
        timestamp = timestamp_series.iloc[i] if time_column in df.columns else timestamp_series[i]
        logger.debug(f"  [{i}] {_format_timezone_info(timestamp)}")


def analyze_filter_conditions(
    df: pd.DataFrame,
    start_time: Union[datetime, pd.Timestamp],
    end_time: Union[datetime, pd.Timestamp],
    time_column: str
) -> None:
    """FAIL-FAST timezone-aware filter condition analysis with detailed results."""
    if df.empty:
        raise TimezoneDebugError("Cannot analyze filter conditions on empty DataFrame")

    if time_column not in df.columns and df.index.name != time_column:
        raise TimezoneDebugError(
            f"Time column '{time_column}' missing for filter analysis",
            {
                "available_columns": list(df.columns),
                "dataframe_index_name": df.index.name,
                "suggestion": f"Use index.name='{df.index.name}' if timestamps are in index"
            }
        )

    # Analyze each condition separately with timezone awareness
    timestamp_series = _get_timestamp_series(df, time_column)
    start_condition = timestamp_series >= start_time
    end_condition = timestamp_series <= end_time
    both_conditions = start_condition & end_condition

    logger.debug("🔍 [TIMEZONE TRACE] FILTER CONDITION ANALYSIS:")
    logger.debug(f"  ▶️  >= START ({_format_timezone_info(start_time)}): {start_condition.sum()}/{len(df)} rows")
    logger.debug(f"  ▶️  <= END   ({_format_timezone_info(end_time)}):   {end_condition.sum()}/{len(df)} rows")
    logger.debug(f"  ✅ BOTH CONDITIONS:                                    {both_conditions.sum()}/{len(df)} rows")

    # FAIL-FAST: Check for impossible conditions
    if both_conditions.sum() == 0 and len(df) > 0:
        data_min = timestamp_series.min()
        data_max = timestamp_series.max()

        raise TimezoneDebugError(
            "NO ROWS MATCH FILTER CONDITIONS - Possible timezone/range error",
            {
                "filter_start": _format_timezone_info(start_time),
                "filter_end": _format_timezone_info(end_time),
                "data_range_min": _format_timezone_info(data_min),
                "data_range_max": _format_timezone_info(data_max),
                "total_rows": len(df)
            }
        )

    # Find boundary issues
    if start_condition.sum() == 0:
        logger.warning("⚠️  [TIMEZONE TRACE] NO ROWS >= start_time - Start boundary too late?")

    if end_condition.sum() == 0:
        logger.warning("⚠️  [TIMEZONE TRACE] NO ROWS <= end_time - End boundary too early?")


def compare_filtered_results(
    original_df: pd.DataFrame,
    filtered_df: pd.DataFrame,
    start_time: Union[datetime, pd.Timestamp],
    end_time: Union[datetime, pd.Timestamp],
    time_column: str
) -> None:
    """FAIL-FAST comparison with timezone-aware validation and rich error context."""
    logger.debug("📊 [TIMEZONE TRACE] FILTERING RESULTS COMPARISON:")
    logger.debug(f"  📥 INPUT:  {len(original_df)} rows")
    logger.debug(f"  📤 OUTPUT: {len(filtered_df)} rows")
    logger.debug(f"  🗑️  REMOVED: {len(original_df) - len(filtered_df)} rows")

    if len(filtered_df) > len(original_df):
        raise TimezoneDebugError(
            "IMPOSSIBLE: Filtered DataFrame has MORE rows than original",
            {
                "original_rows": len(original_df),
                "filtered_rows": len(filtered_df),
                "filter_range": f"{_format_timezone_info(start_time)} to {_format_timezone_info(end_time)}"
            }
        )

    # Validate that filtered data is actually within bounds
    if len(filtered_df) > 0:
        filtered_timestamp_series = _get_timestamp_series(filtered_df, time_column)
        filtered_min = filtered_timestamp_series.min()
        filtered_max = filtered_timestamp_series.max()

        # FAIL-FAST: Check boundary violations
        if filtered_min < start_time:
            raise TimezoneDebugError(
                "BOUNDARY VIOLATION: Filtered data contains timestamps BEFORE start_time",
                {
                    "start_time": _format_timezone_info(start_time),
                    "filtered_min": _format_timezone_info(filtered_min),
                    "violation_magnitude": str(start_time - filtered_min)
                }
            )

        if filtered_max > end_time:
            raise TimezoneDebugError(
                "BOUNDARY VIOLATION: Filtered data contains timestamps AFTER end_time",
                {
                    "end_time": _format_timezone_info(end_time),
                    "filtered_max": _format_timezone_info(filtered_max),
                    "violation_magnitude": str(filtered_max - end_time)
                }
            )

        logger.debug("✅ [TIMEZONE TRACE] BOUNDARY VALIDATION PASSED:")
        logger.debug(f"  📈 Filtered range: {_format_timezone_info(filtered_min)} to {_format_timezone_info(filtered_max)}")
        logger.debug("  ✅ All timestamps within bounds")

    # Check for data loss at exact boundaries
    if len(original_df) > 0:
        original_timestamp_series = _get_timestamp_series(original_df, time_column)
        original_at_start = (original_timestamp_series == start_time).sum()

        if len(filtered_df) > 0:
            filtered_timestamp_series = _get_timestamp_series(filtered_df, time_column)
            filtered_at_start = (filtered_timestamp_series == start_time).sum()
        else:
            filtered_at_start = 0

        if original_at_start > 0 and filtered_at_start == 0:
            raise TimezoneDebugError(
                "DATA LOSS: Exact start_time boundary data was filtered out",
                {
                    "start_time": _format_timezone_info(start_time),
                    "original_matches_at_start": original_at_start,
                    "filtered_matches_at_start": filtered_at_start
                }
            )

        logger.debug(f"🎯 [TIMEZONE TRACE] Start boundary preservation: {filtered_at_start}/{original_at_start} rows")
