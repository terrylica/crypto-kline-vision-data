#!/usr/bin/env python

"""
Script to update logger usage in the codebase.
This script removes the rich_tracebacks parameter from get_logger calls.
"""

import os
import re
import glob

# Pattern to match get_logger calls with rich_tracebacks parameter
GET_LOGGER_PATTERN = r"get_logger\(([^)]+)rich_tracebacks\s*=\s*True([^)]*)\)"


# Replacement function that removes rich_tracebacks parameter
def remove_rich_tracebacks(match):
    before = match.group(1)
    after = match.group(2)

    # Remove trailing comma if it exists
    if before.rstrip().endswith(","):
        before = before.rstrip()[:-1].rstrip()

    return f"get_logger({before}{after})"


def process_file(file_path):
    """Process a single file to update logger usage."""
    try:
        with open(file_path, "r") as f:
            content = f.read()

        # If the file contains rich_tracebacks in get_logger calls
        if "rich_tracebacks" in content and "get_logger" in content:
            # Replace the pattern
            updated_content = re.sub(
                GET_LOGGER_PATTERN, remove_rich_tracebacks, content
            )

            # Only write if changes were made
            if content != updated_content:
                with open(file_path, "w") as f:
                    f.write(updated_content)
                print(f"Updated: {file_path}")
                return True
    except Exception as e:
        print(f"Error processing {file_path}: {e}")

    return False


def main():
    """Find and update all Python files in the workspace."""
    # Find all Python files
    python_files = glob.glob(
        "/workspaces/binance-data-services/**/*.py", recursive=True
    )

    # Count of files updated
    updated_count = 0

    # Process each file
    for file_path in python_files:
        if process_file(file_path):
            updated_count += 1

    print(f"\nUpdated {updated_count} files to remove rich_tracebacks parameter.")


if __name__ == "__main__":
    main()
