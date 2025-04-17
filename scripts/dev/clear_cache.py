#!/usr/bin/env python3
"""
Clear cache and log directories for testing the FCP demo.
"""

from pathlib import Path
import typer
from utils.logger_setup import logger
from rich import print


app = typer.Typer(help="Clear cache and log directories utility")


@app.command()
def clear(
    dirs: list[str] = typer.Option(
        ["cache", "logs", "examples/dsm_sync_simple/logs"],
        "--dirs",
        "-d",
        help="Directories to clear (default: cache, logs, examples/dsm_sync_simple/logs)",
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
    """
    if version:
        print("Clear Cache Utility v1.0.0")
        return

    directories = dirs if dirs else ["cache", "logs", "examples/dsm_sync_simple/logs"]

    if not yes:
        print(
            f"[bold red]WARNING[/bold red]: This will delete all files in these directories:"
        )
        for dir_path in directories:
            print(f"  - {dir_path}")
        response = input("Are you sure you want to continue? (y/N): ")
        if response.lower() != "y":
            print("[green]Operation cancelled[/green]")
            return

    # Process each directory
    for dir_path in directories:
        clear_directory(Path(dir_path))


def clear_directory(directory: Path):
    """Clear all files and empty subdirectories in the given directory."""
    if not directory.exists():
        print(f"[yellow]Directory {directory} does not exist[/yellow]")
        return

    try:
        # Create the directory if it doesn't exist
        directory.mkdir(exist_ok=True, parents=True)

        # Delete all files in the directory
        for item in directory.glob("**/*"):
            if item.is_file():
                item.unlink()
                print(f"Deleted: {item}")

        # Delete empty directories (from deepest to shallowest)
        for item in sorted(
            list(directory.glob("**/*")), key=lambda x: len(str(x)), reverse=True
        ):
            if item.is_dir() and not any(item.iterdir()):
                item.rmdir()
                print(f"Removed empty directory: {item}")

        # Special handling for cache metadata if in cache directory
        if directory.name == "cache":
            metadata_file = directory / "cache_metadata.json"
            if metadata_file.exists():
                metadata_file.unlink()
                print(f"Deleted: {metadata_file}")

        print(f"[green]Successfully cleared directory: {directory}[/green]")
    except Exception as e:
        print(f"[bold red]Error clearing {directory}: {e}[/bold red]")


if __name__ == "__main__":
    app()
