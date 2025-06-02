#!/usr/bin/env python3
"""
Migration Script: Convert from utils.logger_setup to utils.loguru_setup

This script automatically updates import statements throughout the DSM codebase
to use the new loguru-based logging system instead of the old logger_setup.

Usage:
    python scripts/dev/migrate_to_loguru.py [--dry-run] [--path PATH]

Options:
    --dry-run: Show what would be changed without making actual changes
    --path: Specific path to migrate (default: entire workspace)
    --help: Show this help message

The script will:
1. Find all Python files with 'from utils.loguru_setup import logger'
2. Replace with 'from utils.loguru_setup import logger'
3. Preserve all existing logging calls (they work unchanged)
4. Create a backup of modified files (unless --no-backup is specified)
"""

import shutil
from pathlib import Path

import typer
from rich.console import Console
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.table import Table

console = Console()

# Files to exclude from migration
EXCLUDE_PATTERNS = [
    "__pycache__",
    ".git",
    ".venv",
    "venv",
    "node_modules",
    ".pytest_cache",
    "*.pyc",
    "*.pyo",
    "*.egg-info",
]

# The import patterns to replace
OLD_IMPORT = "from utils.loguru_setup import logger"
NEW_IMPORT = "from utils.loguru_setup import logger"


def should_exclude_file(file_path: Path) -> bool:
    """Check if a file should be excluded from migration."""
    return any(pattern in str(file_path) for pattern in EXCLUDE_PATTERNS)


def find_python_files(root_path: Path) -> list[Path]:
    """Find all Python files in the given path."""
    python_files = []

    for file_path in root_path.rglob("*.py"):
        if not should_exclude_file(file_path):
            python_files.append(file_path)

    return python_files


def check_file_needs_migration(file_path: Path) -> bool:
    """Check if a file contains the old import statement."""
    try:
        with open(file_path, encoding="utf-8") as f:
            content = f.read()
            return OLD_IMPORT in content
    except Exception as e:
        console.print(f"[red]Error reading {file_path}: {e}[/red]")
        return False


def migrate_file(file_path: Path, dry_run: bool = False, create_backup: bool = True) -> tuple[bool, str]:
    """Migrate a single file from old import to new import.

    Returns:
        Tuple of (success, message)
    """
    try:
        with open(file_path, encoding="utf-8") as f:
            original_content = f.read()

        # Replace the import statement
        new_content = original_content.replace(OLD_IMPORT, NEW_IMPORT)

        if new_content == original_content:
            return True, "No changes needed"

        if dry_run:
            return True, "Would be migrated"

        # Create backup if requested
        if create_backup:
            backup_path = file_path.with_suffix(file_path.suffix + ".backup")
            shutil.copy2(file_path, backup_path)

        # Write the new content
        with open(file_path, "w", encoding="utf-8") as f:
            f.write(new_content)

        return True, "Migrated successfully"

    except Exception as e:
        return False, f"Error: {e}"


def main(
    path: str = typer.Option(".", help="Path to migrate (default: current directory)"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Show what would be changed without making changes"),
    no_backup: bool = typer.Option(False, "--no-backup", help="Don't create backup files"),
    verbose: bool = typer.Option(False, "-v", "--verbose", help="Show verbose output"),
):
    """Migrate DSM codebase from utils.logger_setup to utils.loguru_setup."""

    console.print(
        Panel.fit(
            "[bold blue]DSM Logger Migration Tool[/bold blue]\nConverting from utils.logger_setup to utils.loguru_setup",
            border_style="blue",
        )
    )

    root_path = Path(path).resolve()

    if not root_path.exists():
        console.print(f"[red]Error: Path {root_path} does not exist[/red]")
        raise typer.Exit(1)

    console.print(f"[blue]Scanning path:[/blue] {root_path}")

    # Find all Python files
    with Progress(SpinnerColumn(), TextColumn("[progress.description]{task.description}"), console=console) as progress:
        task = progress.add_task("Finding Python files...", total=None)
        python_files = find_python_files(root_path)
        progress.update(task, completed=True)

    console.print(f"[green]Found {len(python_files)} Python files[/green]")

    # Check which files need migration
    files_to_migrate = []
    with Progress(SpinnerColumn(), TextColumn("[progress.description]{task.description}"), console=console) as progress:
        task = progress.add_task("Checking files for migration...", total=len(python_files))

        for file_path in python_files:
            if check_file_needs_migration(file_path):
                files_to_migrate.append(file_path)
            progress.advance(task)

    if not files_to_migrate:
        console.print("[green]✅ No files need migration![/green]")
        return

    console.print(f"[yellow]Found {len(files_to_migrate)} files that need migration[/yellow]")

    if dry_run:
        console.print("[blue]DRY RUN MODE - No files will be modified[/blue]")

    # Create results table
    table = Table(title="Migration Results")
    table.add_column("File", style="cyan")
    table.add_column("Status", style="green")
    table.add_column("Message")

    # Migrate files
    success_count = 0
    error_count = 0

    with Progress(SpinnerColumn(), TextColumn("[progress.description]{task.description}"), console=console) as progress:
        task = progress.add_task("Migrating files...", total=len(files_to_migrate))

        for file_path in files_to_migrate:
            success, message = migrate_file(file_path, dry_run, not no_backup)

            if success:
                success_count += 1
                status = "✅ Success"
                style = "green"
            else:
                error_count += 1
                status = "❌ Error"
                style = "red"

            # Show relative path for cleaner display
            rel_path = file_path.relative_to(root_path)
            table.add_row(str(rel_path), status, message)

            if verbose:
                console.print(f"[{style}]{status}[/{style}] {rel_path}: {message}")

            progress.advance(task)

    # Show results
    console.print(table)

    # Summary
    console.print(
        Panel.fit(
            f"[bold]Migration Summary[/bold]\n"
            f"✅ Successful: {success_count}\n"
            f"❌ Errors: {error_count}\n"
            f"📁 Total files processed: {len(files_to_migrate)}",
            border_style="green" if error_count == 0 else "yellow",
        )
    )

    if not dry_run and success_count > 0:
        console.print("\n[green]Migration completed![/green]")
        console.print("[blue]Next steps:[/blue]")
        console.print("1. Test your application to ensure everything works")
        console.print("2. Set log level with: export DSM_LOG_LEVEL=DEBUG")
        console.print("3. Optional: Set log file with: export DSM_LOG_FILE=./logs/dsm.log")

        if not no_backup:
            console.print("\n[yellow]Note:[/yellow] Backup files (.backup) were created for all modified files")


if __name__ == "__main__":
    typer.run(main)
