#!/usr/bin/env python3
"""
README.md Generator for Markdown Directories
============================================

This utility script recursively scans directories and creates empty README.md files
in any directory containing two or more markdown (.md) files that doesn't already
have a README.md file.

Purpose:
--------
- Identify directories with significant markdown content (2+ files)
- Ensure each such directory has a README.md for documentation organization
- Facilitate better project navigation and documentation structure

Usage:
------
# Run with default settings (current directory)
./create_readme_for_md_dirs.py

# Specify a starting directory
./create_readme_for_md_dirs.py /path/to/start/dir

# Preview changes without creating files
./create_readme_for_md_dirs.py --dry-run
./create_readme_for_md_dirs.py -d

Features:
---------
- Recursive directory traversal
- Intelligent detection of directories needing README.md
- Skip directories already containing README.md
- Dry-run mode for previewing changes
- Detailed logging of actions taken
"""

from pathlib import Path

import typer

from data_source_manager.utils.config import MIN_FILES_FOR_README
from data_source_manager.utils.loguru_setup import logger

app = typer.Typer()


def count_md_files(directory: Path) -> int:
    """Count markdown files in a directory (excluding README.md)."""
    return len([f for f in directory.glob("*.md") if f.name.lower() != "readme.md"])


def needs_readme(directory: Path) -> bool:
    """Check if directory needs a README.md file."""
    return count_md_files(directory) >= MIN_FILES_FOR_README and not (directory / "README.md").exists()


def process_directory(directory: Path, dry_run: bool = False) -> int:
    """Process a directory and create README.md if needed."""
    created_count = 0

    # Check current directory
    if needs_readme(directory):
        readme_path = directory / "README.md"
        if not dry_run:
            readme_path.touch()
            logger.info(f"Created {readme_path}")
        else:
            logger.info(f"Would create {readme_path} (dry run)")
        created_count += 1

    # Recursively process subdirectories
    for subdir in directory.iterdir():
        if subdir.is_dir():
            created_count += process_directory(subdir, dry_run)

    return created_count


@app.command()
def main(
    start_dir: str = typer.Argument(".", help="Starting directory path"),
    dry_run: bool = typer.Option(False, "--dry-run", "-d", help="Show what would be done without creating files"),
):
    """
    Recursively find directories with 2 or more markdown files (.md) and create
    an empty README.md file if one doesn't already exist.
    """
    start_path = Path(start_dir).resolve()

    if not start_path.exists() or not start_path.is_dir():
        logger.error(f"Invalid directory: {start_path}")
        raise typer.Exit(1)

    logger.info(f"Scanning directories starting from: {start_path}")

    created_count = process_directory(start_path, dry_run)

    if dry_run:
        logger.info(f"Would create {created_count} README.md files (dry run)")
    else:
        logger.info(f"Created {created_count} README.md files")


if __name__ == "__main__":
    app()
