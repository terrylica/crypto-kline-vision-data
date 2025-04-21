#!/usr/bin/env python3
"""
Clear cache and log directories for testing the DSM Demo.
Supports local and remote filesystems via fsspec.
"""

import fsspec
import typer
from pathlib import Path
import json
from typing import List
from rich import print


app = typer.Typer(help="Clear cache and log directories utility")


@app.callback(context_settings={"help_option_names": ["--help", "-h"]})
def callback():
    """Clear cache and log directories utility."""


@app.command()
def clear(
    dirs: List[str] = typer.Option(
        ["cache", "logs"],
        "--dirs",
        "-d",
        help="Directories to clear (default: cache, logs)",
    ),
    filesystem: str = typer.Option(
        "file",
        "--filesystem",
        "-f",
        help="Filesystem protocol (file, s3, gs, etc.)",
    ),
    storage_options: str = typer.Option(
        None,
        "--storage-options",
        "-s",
        help='Storage options as JSON string (e.g. \'{"key": "value"}\')',
    ),
    test_mode: bool = typer.Option(
        False,
        "--test",
        "-t",
        help="Run in test mode (simulate operations without deleting files)",
    ),
    yes: bool = typer.Option(
        False,
        "--yes",
        "-y",
        help="Skip confirmation prompt and proceed with deletion",
    ),
    version: bool = typer.Option(
        False,
        "--version",
        "-v",
        help="Show version information and exit",
    ),
):
    """
    Clear specified directories by removing all files while preserving directory structure.

    This utility removes all files from the specified directories and their subdirectories,
    then removes any empty subdirectories. The base directories themselves are preserved.

    For the cache directory, the cache_metadata.json file is also deleted if present.

    Supports various filesystem types via fsspec (local, S3, GCS, etc.).
    """
    if version:
        print("Clear Cache Utility v1.1.0")
        return

    directories = dirs if dirs else ["cache", "logs"]

    # Parse storage options if provided
    options = {}
    if storage_options:
        try:
            options = json.loads(storage_options)
        except json.JSONDecodeError:
            print("[bold red]Error:[/bold red] Invalid JSON for storage options")
            return

    if test_mode:
        print(f"[yellow]TEST MODE: No files will actually be deleted[/yellow]")
        yes = True  # Skip confirmation in test mode

    if not yes:
        print(
            f"[bold red]WARNING[/bold red]: This will delete all files in these directories using {filesystem} protocol:"
        )
        for dir_path in directories:
            print(f"  - {dir_path}")
        response = input("Are you sure you want to continue? (y/N): ")
        if response.lower() != "y":
            print("[green]Operation cancelled[/green]")
            return

    # Process each directory
    for dir_path in directories:
        clear_directory(dir_path, filesystem, options, test_mode=test_mode)


def clear_directory(
    directory: str,
    protocol: str = "file",
    storage_options: dict = None,
    test_mode: bool = False,
):
    """Clear all files and empty subdirectories in the given directory using fsspec."""
    if storage_options is None:
        storage_options = {}

    try:
        # Create filesystem object
        fs = fsspec.filesystem(protocol, **storage_options)

        # Check if directory exists
        if not fs.exists(directory):
            print(f"[yellow]Directory {directory} does not exist[/yellow]")
            return

        # Ensure directory exists (create if it doesn't)
        fs.makedirs(directory, exist_ok=True)

        # For better path handling, get the full path
        if protocol == "file":
            path = Path(directory)
            if not path.is_absolute():
                directory = str(path.absolute())

        # First pass: Find and delete all files
        files_deleted = 0
        all_files = []

        # Walk all directories and collect files
        def walk_dir(path):
            nonlocal all_files
            try:
                entries = fs.ls(path, detail=True)
                for entry in entries:
                    entry_path = entry["name"]
                    if entry["type"] == "file":
                        all_files.append(entry_path)
                    elif entry["type"] == "directory":
                        walk_dir(entry_path)
            except Exception as e:
                print(f"Error walking directory {path}: {e}")

        # Start recursive walk
        walk_dir(directory)
        print(f"Found {len(all_files)} files to delete")

        # Delete all files
        for file_path in all_files:
            if test_mode:
                print(f"Would delete: {file_path}")
                files_deleted += 1
            else:
                fs.rm(file_path)
                print(f"Deleted: {file_path}")
                files_deleted += 1

        # Second pass: Find and remove empty directories
        dirs_removed = 0

        # Recursively get all directories
        all_dirs = []

        def get_dirs(path):
            nonlocal all_dirs
            try:
                entries = fs.ls(path, detail=True)
                for entry in entries:
                    entry_path = entry["name"]
                    if entry["type"] == "directory":
                        all_dirs.append(entry_path)
                        get_dirs(entry_path)
            except Exception as e:
                print(f"Error getting directories in {path}: {e}")

        # Collect all directories
        get_dirs(directory)

        # Sort directories by depth (deepest first)
        all_dirs.sort(key=lambda x: len(Path(x).parts), reverse=True)
        print(f"Found {len(all_dirs)} directories to check")

        # Remove empty directories
        for dir_path in all_dirs:
            try:
                if len(fs.ls(dir_path)) == 0:
                    if test_mode:
                        print(f"Would remove empty directory: {dir_path}")
                        dirs_removed += 1
                    else:
                        fs.rmdir(dir_path)
                        print(f"Removed empty directory: {dir_path}")
                        dirs_removed += 1
            except Exception as e:
                print(f"Error removing directory {dir_path}: {e}")

        # Special handling for cache metadata if in cache directory
        dir_path = Path(directory)
        if dir_path.name == "cache" or str(dir_path).endswith("/cache"):
            metadata_file = str(Path(directory) / "cache_metadata.json")
            if fs.exists(metadata_file):
                if test_mode:
                    print(f"Would delete: {metadata_file}")
                    files_deleted += 1
                else:
                    fs.rm(metadata_file)
                    print(f"Deleted: {metadata_file}")
                    files_deleted += 1

        if test_mode:
            print(f"[green]Test complete for directory: {directory}[/green]")
        else:
            print(f"[green]Successfully cleared directory: {directory}[/green]")

        print(
            f"[green]Files {('would be ' if test_mode else '')}deleted: {files_deleted}, Empty directories {('would be ' if test_mode else '')}removed: {dirs_removed}[/green]"
        )

    except Exception as e:
        print(f"[bold red]Error clearing {directory}: {e}[/bold red]")


if __name__ == "__main__":
    app()
