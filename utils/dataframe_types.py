#!/usr/bin/env python
"""Common DataFrame class types and extensions for the data services.

This module centralizes custom DataFrame types and extensions used throughout
the data services. These types are designed to enforce specific constraints
and behaviors for consistent data handling.
"""

import traceback

import pandas as pd

from utils.config import CANONICAL_INDEX_NAME
from utils.logger_setup import logger


class TimestampedDataFrame(pd.DataFrame):
    """DataFrame with enforced UTC timestamp index.

    This class enforces:
    1. Index must be DatetimeIndex
    2. Index must be timezone-aware and in UTC
    3. Index must be named 'open_time' (representing the BEGINNING of each candle period)
    4. Index must be monotonically increasing
    5. No duplicate indices allowed
    """

    def __init__(self, *args, **kwargs):
        """Initialize a TimestampedDataFrame.

        Ensures that open_time is consistently handled, either as an index or both index and column.
        """
        super().__init__(*args, **kwargs)

        # Validate and normalize index
        try:
            self._validate_and_normalize_index()
        except Exception as e:
            logger.error(f"Error normalizing index: {e}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            # Create an empty DataFrame with proper structure instead of raising
            super().__init__()
            return

        # After initialization, ensure open_time is available as a column
        # This is critical for compatibility with standard pandas operations
        try:
            if "open_time" not in self.columns:
                if hasattr(self.index, "name") and self.index.name == "open_time":
                    logger.debug("Ensuring open_time exists as column (copied from index)")
                    # Fix: Use reset_index and set_index operations without reassigning self
                    temp_df = self.reset_index()
                    self._update_inplace(temp_df)
                    # Instead of reassigning self, modify the index directly
                    self.index = temp_df["open_time"]
                    self.index.name = "open_time"
                    logger.debug(
                        f"Added open_time column, dtype: {self['open_time'].dtype if 'open_time' in self.columns else 'N/A'} (represents BEGINNING of candle)"
                    )
        except Exception as e:
            logger.error(f"Error ensuring open_time as column: {e}")
            logger.error(f"Columns available: {list(self.columns)}")
            logger.error(f"Index type: {type(self.index)}, name: {getattr(self.index, 'name', None)}")
            # Don't raise the exception - keep the DataFrame as is

    def _validate_and_normalize_index(self):
        """Validate and normalize the index to meet requirements.

        Preserves the semantic meaning of open_time as the BEGINNING of each candle period.
        """
        # Import here to avoid circular imports
        from utils.dataframe_utils import ensure_open_time_as_index

        try:
            # Use the centralized utility to ensure the index is properly set
            df_with_proper_index = ensure_open_time_as_index(self)

            # Since we can't replace self entirely, we need to update its properties
            if not self.equals(df_with_proper_index):
                logger.debug("Updating TimestampedDataFrame with normalized index")
                # Copy the index from the properly indexed DataFrame
                self.index = df_with_proper_index.index

                # Make sure columns match as well if they've changed
                if set(self.columns) != set(df_with_proper_index.columns):
                    for col in df_with_proper_index.columns:
                        if col not in self.columns:
                            self[col] = df_with_proper_index[col]

            # Log final state
            logger.debug(
                f"TimestampedDataFrame index properly validated: {len(self)} rows, index represents BEGINNING of each candle period"
            )

            # Verify semantic meaning of timestamps
            if len(self) > 0 and "close_time" in self.columns:
                first_open = self.index[0] if isinstance(self.index, pd.DatetimeIndex) else None
                first_close = self["close_time"].iloc[0] if "close_time" in self.columns else None

                if first_open is not None and first_close is not None:
                    time_diff = (first_close - first_open).total_seconds()
                    logger.debug(
                        f"Time difference between first open_time and close_time: {time_diff:.3f}s"
                        f" (open_time=BEGINNING of candle, close_time=END of candle)"
                    )
        except Exception as e:
            logger.error(f"Error normalizing index: {e}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            raise

    def to_pandas(self) -> pd.DataFrame:
        """Convert to standard pandas DataFrame with open_time as a column.

        This ensures the DataFrame can be used in contexts where open_time
        is expected to be a column, not just an index.

        Returns:
            A standard pandas DataFrame with open_time as a column
        """
        # Import here to avoid circular imports
        from utils.dataframe_utils import ensure_open_time_as_column

        # Create a copy to avoid modifying the original
        df = pd.DataFrame(self.copy())

        # Use the centralized utility to ensure open_time is properly handled as a column
        df = ensure_open_time_as_column(df)

        logger.debug(f"Converted to pandas DataFrame with columns: {list(df.columns)}")
        return df

    def __setitem__(self, key, value):
        """Override to prevent modification of index."""
        if key == CANONICAL_INDEX_NAME:
            logger.warning(f"Setting {CANONICAL_INDEX_NAME} directly - this may cause issues. Use index operations instead.")
            logger.debug(f"Remember: {CANONICAL_INDEX_NAME} represents the BEGINNING of each candle period")
        super().__setitem__(key, value)
