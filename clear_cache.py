#!/usr/bin/env python3
"""
Clear the cache directory for testing the FCP demo.
"""

import os
import shutil
from pathlib import Path
import typer
from rich import print

app = typer.Typer()


@app.command()
def clear_cache(cache_dir: str = "cache", confirm: bool = False):
    """
    Clear the cache directory for testing.

    Args:
        cache_dir: Path to the cache directory
        confirm: Skip confirmation prompt if true
    """
    cache_path = Path(cache_dir)

    if not cache_path.exists():
        print(f"[yellow]Cache directory {cache_dir} does not exist[/yellow]")
        return

    if not confirm:
        print(
            f"[bold red]WARNING[/bold red]: This will delete all files in {cache_dir}"
        )
        response = input("Are you sure you want to continue? (y/N): ")
        if response.lower() != "y":
            print("[green]Operation cancelled[/green]")
            return

    # Remove the files but keep the directory
    try:
        # Delete all files in the cache directory
        for item in cache_path.glob("**/*"):
            if item.is_file():
                item.unlink()
                print(f"Deleted: {item}")

        # Delete empty directories
        for item in list(cache_path.glob("**/*"))[
            ::-1
        ]:  # Reverse to delete deepest first
            if item.is_dir() and not any(item.iterdir()):
                item.rmdir()
                print(f"Removed empty directory: {item}")

        # Delete the metadata file
        metadata_file = cache_path / "cache_metadata.json"
        if metadata_file.exists():
            metadata_file.unlink()
            print(f"Deleted: {metadata_file}")

        print(f"[green]Successfully cleared cache directory: {cache_dir}[/green]")
    except Exception as e:
        print(f"[bold red]Error clearing cache: {e}[/bold red]")


if __name__ == "__main__":
    app()
