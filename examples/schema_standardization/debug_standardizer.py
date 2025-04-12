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


def format_dtype(dtype):
    """Format data type for readable display"""
    return str(dtype)


def debug_dataframe_schema(df: pd.DataFrame, reference_schema: dict):
    """Debug a DataFrame schema against a reference schema"""
    df_schema = {col: df[col].dtype for col in df.columns}

    print("\n[bold cyan]Debugging DataFrame Schema[/bold cyan]")
    print(f"DataFrame has {len(df)} rows and {len(df.columns)} columns")

    # Check column presence and order
    missing_cols = [col for col in reference_schema if col not in df_schema]
    extra_cols = [col for col in df_schema if col not in reference_schema]

    ref_cols_set = set(reference_schema.keys())
    df_cols_set = set(df_schema.keys())
    common_cols = list(ref_cols_set.intersection(df_cols_set))

    # Print all columns with their data types in a table format
    print("\n[bold cyan]Column Analysis:[/bold cyan]")
    print(
        f"{'Column':<30} | {'Current Type':<20} | {'Reference Type':<20} | {'Status':<10}"
    )
    print(f"{'-'*30} | {'-'*20} | {'-'*20} | {'-'*10}")

    for col in sorted(set(list(reference_schema.keys()) + list(df_schema.keys()))):
        if col in df_schema and col in reference_schema:
            # Column exists in both
            current_type = format_dtype(df_schema[col])
            ref_type = format_dtype(reference_schema[col])

            if current_type == ref_type:
                status = "[green]Match[/green]"
            else:
                status = "[yellow]Type Diff[/yellow]"

            print(f"{col:<30} | {current_type:<20} | {ref_type:<20} | {status:<10}")
        elif col in df_schema:
            # Extra column
            current_type = format_dtype(df_schema[col])
            print(f"{col:<30} | {current_type:<20} | {'N/A':<20} | [red]Extra[/red]")
        else:
            # Missing column
            ref_type = format_dtype(reference_schema[col])
            print(f"{col:<30} | {'N/A':<20} | {ref_type:<20} | [red]Missing[/red]")

    # Summary
    print("\n[bold cyan]Schema Summary:[/bold cyan]")
    if missing_cols:
        print(f"[red]Missing columns ({len(missing_cols)}): {missing_cols}[/red]")
    else:
        print("[green]✓ No missing columns[/green]")

    if extra_cols:
        print(f"[yellow]Extra columns ({len(extra_cols)}): {extra_cols}[/yellow]")
    else:
        print("[green]✓ No extra columns[/green]")

    # Check column order
    df_cols = list(df_schema.keys())
    ref_cols = list(reference_schema.keys())

    if all(col in ref_cols for col in df_cols) and len(df_cols) == len(ref_cols):
        order_correct = True
        for i, (df_col, ref_col) in enumerate(zip(df_cols, ref_cols)):
            if df_col != ref_col:
                print(
                    f"[red]Column order mismatch at position {i}: expected '{ref_col}', got '{df_col}'[/red]"
                )
                order_correct = False
                break

        if order_correct:
            print("[green]✓ Column order matches reference schema[/green]")
        else:
            print("[red]✗ Column order does not match reference schema[/red]")

    # Check data types
    type_mismatches = []
    for col in common_cols:
        df_type = format_dtype(df_schema[col])
        ref_type = format_dtype(reference_schema[col])

        if df_type != ref_type:
            type_mismatches.append((col, df_type, ref_type))

    if type_mismatches:
        print(f"[yellow]Type mismatches ({len(type_mismatches)}):[/yellow]")
        for col, df_type, ref_type in type_mismatches:
            print(f"  - Column '{col}': current={df_type}, reference={ref_type}")
    else:
        print("[green]✓ All data types match reference schema[/green]")

    # Examine sample data
    print("\n[bold cyan]Sample Data (first 5 rows):[/bold cyan]")
    print(df.head(5))


def main():
    parser = argparse.ArgumentParser(
        description="Debug DataFrame schema and standardization"
    )

    parser.add_argument(
        "--input", "-i", type=str, required=True, help="CSV file to analyze and debug"
    )
    parser.add_argument(
        "--output",
        "-o",
        type=str,
        required=False,
        help="Output file for standardized data",
    )
    parser.add_argument(
        "--market-type",
        "-m",
        type=str,
        choices=["SPOT", "UM", "CM"],
        default="SPOT",
        help="Market type (SPOT, UM, CM)",
    )
    parser.add_argument(
        "--symbol",
        "-s",
        type=str,
        default="BTCUSDT",
        help="Trading symbol (e.g. BTCUSDT)",
    )
    parser.add_argument(
        "--interval", type=str, default="1m", help="Time interval (e.g. 1m, 5m, 1h)"
    )
    parser.add_argument(
        "--timestamp-col",
        "-t",
        type=str,
        default="open_time",
        help="Name of timestamp column in input file (default: open_time)",
    )
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")

    args = parser.parse_args()

    # Convert market type string to enum
    market_type = MarketType[args.market_type]

    # Read input file
    input_path = Path(args.input)
    if not input_path.exists():
        logger.error(f"Input file not found: {input_path}")
        return

    logger.info(f"Reading file: {input_path}")
    try:
        df = pd.read_csv(input_path)
        logger.info(
            f"Loaded DataFrame with {len(df)} rows and {len(df.columns)} columns"
        )
    except Exception as e:
        logger.error(f"Failed to read input file: {e}")
        return

    # Convert timestamp column to datetime if needed
    if args.timestamp_col in df.columns:
        if pd.api.types.is_numeric_dtype(df[args.timestamp_col]):
            # Convert from milliseconds to datetime
            df[args.timestamp_col] = pd.to_datetime(df[args.timestamp_col], unit="ms")
            logger.info(
                f"Converted '{args.timestamp_col}' from milliseconds to datetime"
            )
        elif not pd.api.types.is_datetime64_dtype(df[args.timestamp_col]):
            # Try to parse as string datetime
            df[args.timestamp_col] = pd.to_datetime(df[args.timestamp_col])
            logger.info(f"Converted '{args.timestamp_col}' from string to datetime")

    # Create standardizer and get reference schema
    standardizer = SchemaStandardizer(
        market_type=market_type, symbol=args.symbol, interval=args.interval
    )

    # Get reference schema
    reference_schema = standardizer.get_reference_schema()
    if not reference_schema:
        logger.error("Failed to retrieve reference schema from REST API")
        return

    # Debug the DataFrame schema
    logger.info(f"Reference schema from REST API: {reference_schema}")
    debug_dataframe_schema(df, reference_schema)

    # Always standardize and save if output is specified
    if args.output:
        logger.info("Standardizing DataFrame to fix schema issues...")
        standardized_df = standardizer.standardize_dataframe(df)

        output_path = Path(args.output)

        # Save the standardized DataFrame
        standardized_df.to_csv(output_path, index=False)
        logger.info(f"Saved standardized DataFrame to {output_path}")

        # Show the standardized schema
        logger.info("Standardized DataFrame schema:")
        debug_dataframe_schema(standardized_df, reference_schema)


if __name__ == "__main__":
    main()
