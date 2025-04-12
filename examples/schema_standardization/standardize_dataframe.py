#!/usr/bin/env python
from utils.logger_setup import logger
from rich import print
import argparse
from pathlib import Path
import sys
import pandas as pd

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))

from utils.market_constraints import MarketType
from utils.schema_standardizer import SchemaStandardizer


def main():
    parser = argparse.ArgumentParser(
        description="Standardize a DataFrame to match REST API schema"
    )
    parser.add_argument(
        "--market-type",
        "-m",
        type=str,
        choices=["SPOT", "UM", "CM"],
        required=True,
        help="Market type (SPOT, UM, CM)",
    )
    parser.add_argument(
        "--symbol", "-s", type=str, required=True, help="Trading symbol (e.g. BTCUSDT)"
    )
    parser.add_argument(
        "--interval",
        "-i",
        type=str,
        required=True,
        help="Time interval (e.g. 1m, 5m, 1h)",
    )
    parser.add_argument(
        "--file",
        "-f",
        type=str,
        required=True,
        help="CSV file containing the DataFrame to standardize",
    )
    parser.add_argument(
        "--output",
        "-o",
        type=str,
        default=None,
        help="Output CSV file (default: standardized_<input_filename>)",
    )
    parser.add_argument(
        "--timestamp-col",
        "-t",
        type=str,
        default="open_time",
        help="Name of timestamp column in input file (default: open_time)",
    )
    parser.add_argument(
        "--show-diff",
        action="store_true",
        help="Show differences between original and standardized data",
    )

    args = parser.parse_args()

    # Convert market type string to enum
    market_type = MarketType[args.market_type]

    input_path = Path(args.file)
    if not input_path.exists():
        logger.error(f"Input file not found: {input_path}")
        return

    if args.output:
        output_path = Path(args.output)
    else:
        output_path = input_path.parent / f"standardized_{input_path.name}"

    logger.info(f"Reading DataFrame from {input_path}")

    # Read input DataFrame
    try:
        df = pd.read_csv(input_path)
        logger.info(
            f"Loaded DataFrame with {len(df)} rows and {len(df.columns)} columns"
        )
    except Exception as e:
        logger.error(f"Failed to read input file: {e}")
        return

    # Convert timestamp column to datetime if it's not already
    if args.timestamp_col in df.columns:
        if pd.api.types.is_numeric_dtype(df[args.timestamp_col]):
            # Convert from milliseconds to datetime
            df[args.timestamp_col] = pd.to_datetime(df[args.timestamp_col], unit="ms")
        elif not pd.api.types.is_datetime64_dtype(df[args.timestamp_col]):
            # Try to parse as string datetime
            df[args.timestamp_col] = pd.to_datetime(df[args.timestamp_col])

    # Create standardizer
    standardizer = SchemaStandardizer(
        market_type=market_type, symbol=args.symbol, interval=args.interval
    )

    # Get reference schema
    reference_schema = standardizer.get_reference_schema()
    if not reference_schema:
        logger.error("Failed to retrieve reference schema from REST API")
        return

    logger.info(f"Reference schema from REST API: {reference_schema}")

    # Get original schema
    original_schema = {col: df[col].dtype for col in df.columns}
    logger.info(f"Original schema: {original_schema}")

    # Find differences
    if args.show_diff:
        missing_cols = [col for col in reference_schema if col not in original_schema]
        extra_cols = [col for col in original_schema if col not in reference_schema]
        different_type_cols = [
            col
            for col in reference_schema
            if col in original_schema
            and str(reference_schema[col]) != str(original_schema[col])
        ]

        if missing_cols:
            logger.warning(f"❌ Missing columns in original data: {missing_cols}")
        if extra_cols:
            logger.warning(f"❌ Extra columns in original data: {extra_cols}")
        if different_type_cols:
            logger.warning(f"❌ Columns with different types:")
            for col in different_type_cols:
                logger.warning(
                    f"  - {col}: original={original_schema[col]}, reference={reference_schema[col]}"
                )

    # Standardize the DataFrame
    standardized_df = standardizer.standardize_dataframe(df)
    logger.info(
        f"Standardized DataFrame has {len(standardized_df)} rows and {len(standardized_df.columns)} columns"
    )

    # Save standardized DataFrame
    standardized_df.to_csv(output_path, index=False)
    logger.info(f"✅ Standardized DataFrame saved to {output_path}")

    # Show sample of standardized data
    if args.show_diff:
        logger.info("Sample of standardized data (first 5 rows):")
        print(standardized_df.head(5))


if __name__ == "__main__":
    main()
