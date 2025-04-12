from typing import Dict, Optional
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone

from utils.logger_setup import logger

from utils.market_constraints import MarketType, DataProvider, Interval
from core.sync.data_source_manager import DataSourceManager
from core.sync.rest_data_client import RestDataClient


class SchemaStandardizer:
    """
    A utility class to standardize DataFrames returned from different data sources
    (REST API, VISION API, cache) to ensure they have consistent column names, ordering,
    and data types. The REST API schema is used as the standard.
    """

    def __init__(self, market_type: MarketType, symbol: str, interval: str):
        """
        Initialize the SchemaStandardizer with market type, symbol, and interval.

        Args:
            market_type: The market type (spot, um, cm)
            symbol: The trading symbol
            interval: The time interval (e.g., 1m, 5m, 1h)
        """
        self.market_type = market_type
        self.symbol = symbol
        self.interval = interval

        # Initialize DataSourceManager with only parameters its constructor accepts
        self.dsm = DataSourceManager(
            market_type=market_type, provider=DataProvider.BINANCE
        )

        self.reference_schema = None

    def get_reference_schema(
        self, start_time: Optional[datetime] = None, end_time: Optional[datetime] = None
    ) -> Dict:
        """
        Get the reference schema from the REST API.

        Args:
            start_time: The start time for the data query
            end_time: The end time for the data query

        Returns:
            A dictionary mapping column names to their data types
        """
        if self.reference_schema is not None:
            return self.reference_schema

        # Use recent timeframe if not specified
        if start_time is None or end_time is None:
            end_time = datetime.now(timezone.utc)
            start_time = end_time - timedelta(
                minutes=60
            )  # Increased time range for better chance of data
            logger.debug(f"Using default time range: {start_time} to {end_time}")

        # Convert string interval to Interval enum
        interval_enum = self._parse_interval(self.interval)
        logger.debug(f"Using interval: {self.interval} (enum: {interval_enum})")

        # Fetch data from REST API to use as reference
        try:
            logger.debug(f"Initializing REST client directly for {self.symbol}")
            # Create REST client directly for more reliable access
            rest_client = RestDataClient(
                market_type=self.market_type,
                retry_count=3,
                symbol=self.symbol,
                interval=interval_enum,
            )

            logger.debug(
                f"Fetching reference schema from REST API for {self.symbol} from {start_time} to {end_time}"
            )

            # Use direct REST client fetch for more reliable results
            df_rest = rest_client.fetch(
                symbol=self.symbol,
                interval=interval_enum,
                start_time=start_time,
                end_time=end_time,
            )

            if df_rest is None or len(df_rest) == 0:
                logger.error(
                    f"Failed to retrieve reference schema from REST API - direct method"
                )

                # Fall back to DSM's get_data method
                logger.debug("Trying DataSourceManager.get_data as fallback")
                df_rest = self.dsm.get_data(
                    symbol=self.symbol,
                    start_time=start_time,
                    end_time=end_time,
                    interval=interval_enum,
                )

                if df_rest is None or len(df_rest) == 0:
                    logger.error(
                        f"Failed to retrieve reference schema from REST API - DSM method"
                    )
                    return {}

            # Store column names and data types
            self.reference_schema = {col: df_rest[col].dtype for col in df_rest.columns}
            logger.info(
                f"Successfully retrieved reference schema with {len(df_rest)} records and {len(self.reference_schema)} columns"
            )
            logger.debug(f"Reference schema: {self.reference_schema}")
            return self.reference_schema

        except Exception as e:
            logger.error(f"Error fetching reference schema: {e}")
            import traceback

            logger.debug(f"Traceback: {traceback.format_exc()}")
            return {}

    def _parse_interval(self, interval_str: str) -> Interval:
        """
        Parse a string interval to an Interval enum.

        Args:
            interval_str: String interval (e.g., "1m", "5m", "1h")

        Returns:
            Interval enum
        """
        # Map common interval strings to Interval enum values
        interval_map = {
            "1s": Interval.SECOND_1,
            "1m": Interval.MINUTE_1,
            "3m": Interval.MINUTE_3,
            "5m": Interval.MINUTE_5,
            "15m": Interval.MINUTE_15,
            "30m": Interval.MINUTE_30,
            "1h": Interval.HOUR_1,
            "2h": Interval.HOUR_2,
            "4h": Interval.HOUR_4,
            "6h": Interval.HOUR_6,
            "8h": Interval.HOUR_8,
            "12h": Interval.HOUR_12,
            "1d": Interval.DAY_1,
            "3d": Interval.DAY_3,
            "1w": Interval.WEEK_1,
            "1M": Interval.MONTH_1,
        }

        if interval_str not in interval_map:
            logger.warning(f"Unknown interval '{interval_str}', using 1m as default")
            return Interval.MINUTE_1

        return interval_map[interval_str]

    def standardize_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Standardize a DataFrame to match the reference schema.

        Args:
            df: The DataFrame to standardize

        Returns:
            A standardized DataFrame with consistent column names, order, and data types
        """
        if df is None or len(df) == 0:
            logger.warning("Cannot standardize empty DataFrame")
            return df

        # Get reference schema if not already loaded
        reference_schema = self.get_reference_schema()
        if not reference_schema:
            logger.error("No reference schema available for standardization")
            return df

        standardized_df = df.copy()

        # Check for missing columns in the input DataFrame
        missing_cols = [
            col for col in reference_schema if col not in standardized_df.columns
        ]
        if missing_cols:
            logger.warning(f"Adding missing columns: {missing_cols}")
            for col in missing_cols:
                # Add missing columns with appropriate data type and fill with NaN or 0
                if np.issubdtype(reference_schema[col], np.number):
                    standardized_df[col] = 0
                else:
                    standardized_df[col] = None

        # Check for extra columns in the input DataFrame
        extra_cols = [
            col for col in standardized_df.columns if col not in reference_schema
        ]
        if extra_cols:
            logger.warning(f"Removing extra columns: {extra_cols}")
            standardized_df = standardized_df.drop(columns=extra_cols)

        # Reorder columns to match reference schema
        ref_column_order = list(reference_schema.keys())
        standardized_df = standardized_df[ref_column_order]

        # Convert data types to match reference schema
        for col, dtype in reference_schema.items():
            try:
                standardized_df[col] = standardized_df[col].astype(dtype)
            except Exception as e:
                logger.warning(f"Failed to convert column '{col}' to type {dtype}: {e}")

        return standardized_df

    def standardize_cache_data(self, start_time: datetime, end_time: datetime) -> None:
        """
        Standardize all cached data to match the reference schema,
        and save the standardized data back to the cache.

        Args:
            start_time: The start time for the data to standardize
            end_time: The end time for the data to standardize
        """
        try:
            # Ensure times are timezone-aware
            if start_time.tzinfo is None:
                start_time = start_time.replace(tzinfo=timezone.utc)
            if end_time.tzinfo is None:
                end_time = end_time.replace(tzinfo=timezone.utc)

            # Convert string interval to Interval enum
            interval_enum = self._parse_interval(self.interval)

            # Get data from cache - we need to use get_data method with use_cache flag
            cache_manager = self.dsm.cache_manager
            if cache_manager is None:
                logger.warning("Cache manager is not available")
                return

            # Check if there's data in the cache
            cache_file_path = cache_manager.get_cache_file_path(
                market_type=self.market_type,
                symbol=self.symbol,
                interval=str(interval_enum),
            )

            if not cache_file_path.exists():
                logger.info(
                    f"No cached data to standardize for {self.symbol} ({self.market_type})"
                )
                return

            # Attempt to read from cache
            df_cache = cache_manager.get_from_cache(
                market_type=self.market_type,
                symbol=self.symbol,
                interval=str(interval_enum),
                start_time=start_time,
                end_time=end_time,
            )

            if df_cache is None or len(df_cache) == 0:
                logger.info(
                    f"No cached data to standardize for {self.symbol} ({self.market_type})"
                )
                return

            # Standardize data
            standardized_df = self.standardize_dataframe(df_cache)

            # Save standardized data back to cache
            logger.info(
                f"Saving standardized data to cache for {self.symbol} ({self.market_type})"
            )
            cache_manager.save_to_cache(
                market_type=self.market_type,
                symbol=self.symbol,
                interval=str(interval_enum),
                df=standardized_df,
            )

        except Exception as e:
            logger.error(f"Error standardizing cache data: {e}")


def standardize_dsm_output(
    df: pd.DataFrame, market_type: MarketType, symbol: str, interval: str
) -> pd.DataFrame:
    """
    Convenience function to standardize DataFrame from any source.
    Can be used as a wrapper for DataSourceManager output.

    Args:
        df: DataFrame to standardize
        market_type: Market type
        symbol: Trading symbol
        interval: Time interval

    Returns:
        Standardized DataFrame
    """
    if df is None or len(df) == 0:
        return df

    standardizer = SchemaStandardizer(market_type, symbol, interval)
    return standardizer.standardize_dataframe(df)
