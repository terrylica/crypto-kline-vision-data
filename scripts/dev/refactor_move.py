#!/usr/bin/env python3
"""
Move files with git mv, refactor imports with Rope, and verify with Ruff.

This script helps move Python files while automatically handling import refactoring
and verifying the changes don't introduce import-related errors.
"""

import subprocess
import sys
from datetime import datetime
from pathlib import Path
import typer
from typing import List, Optional
from rope.base.project import Project
from rope.refactor.rename import Rename

from utils.logger_setup import logger

# Configure typer app with explicit help options to ensure -h works
app = typer.Typer(
    help="Move files with git, refactor imports, and verify with Ruff",
    context_settings={"help_option_names": ["--help", "-h"]},
)


def git_mv(old_path: str, new_path: str, dry_run: bool = False) -> bool:
    """Move a file using git mv."""
    if dry_run:
        logger.info(f"[DRY-RUN] git mv {old_path} {new_path}")
        return True
    else:
        logger.debug(f"Running: git mv {old_path} {new_path}")
        try:
            subprocess.run(["git", "mv", old_path, new_path], check=True)
            logger.info(f"Moved {old_path} -> {new_path}")
            return True
        except subprocess.CalledProcessError as e:
            logger.error(f"Git command failed: {e}")
            return False


def rope_refactor_imports(
    project_path: str, file_path: str, dry_run: bool = False
) -> bool:
    """Refactor imports for a moved file using Rope."""
    if dry_run:
        logger.info(f"[DRY-RUN] Rope would refactor imports in {file_path}")
        return True

    logger.debug(f"Refactoring imports in {file_path} via Rope")
    try:
        project = Project(project_path)
        resource = project.get_file(file_path)
        rename = Rename(project, resource)
        changes = rename.get_changes(file_path)
        project.do(changes)
        project.close()
        logger.info(f"Imports updated for {file_path}")
        return True
    except Exception as e:
        logger.error(f"Rope refactoring failed: {e}")
        return False


def run_ruff(project_path: str, dry_run: bool = False) -> bool:
    """Run Ruff to check for import-related issues."""
    if dry_run:
        logger.info(
            f"[DRY-RUN] Ruff would run: ruff check {project_path} --select F821,F822,F823"
        )
        return True

    logger.info("Running Ruff sanity check...")
    try:
        result = subprocess.run(
            ["ruff", "check", project_path, "--select", "F821,F822,F823"],
            capture_output=True,
            text=True,
        )
        if result.stdout:
            logger.warning(f"Ruff detected issues:\n{result.stdout}")
            return False
        else:
            logger.info("Ruff found no import-related issues.")
            return True
    except Exception as e:
        logger.error(f"Ruff check failed: {e}")
        return False


@app.command()
def move(
    moves: List[str] = typer.Argument(
        ...,
        help="Pairs of source and destination paths: old1.py:new1.py old2.py:new2.py ...",
    ),
    project: str = typer.Option(
        ".", "--project", "-p", help="Root of your Python project"
    ),
    dry_run: bool = typer.Option(
        False, "--dry-run", "-d", help="Show what would happen without making changes"
    ),
    verbose: int = typer.Option(
        0,
        "--verbose",
        "-v",
        count=True,
        help="Increase verbosity: -v for INFO, -vv for DEBUG",
    ),
    log_file: Optional[str] = typer.Option(
        None,
        "--log-file",
        "-l",
        help="Custom log file name (default: refactor_YYYYMMDD_HHMMSS.log)",
    ),
):
    """
    Move files with git mv, refactor imports via Rope, and verify with Ruff.

    This command handles moving Python files while ensuring imports remain valid.
    It first uses git mv to move the files, then uses Rope to update imports,
    and finally runs Ruff to verify no import errors were introduced.
    """
    # Setup log file if not provided
    if not log_file:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = f"refactor_{timestamp}.log"

    # Set the log level
    if verbose >= 2:
        logger.setLevel(20)  # DEBUG in rich logger
    elif verbose == 1:
        logger.setLevel(30)  # INFO in rich logger

    logger.debug(f"Arguments: moves={moves}, project={project}, dry_run={dry_run}")

    # Process each move operation
    success = True
    project_path = Path(project)

    for move_pair in moves:
        try:
            old_path, new_path = move_pair.split(":")
        except ValueError:
            logger.error(f"Invalid move format '{move_pair}'; use old_path:new_path")
            success = False
            continue

        logger.info(f"Processing move: {old_path} -> {new_path}")

        # Ensure destination directory exists
        new_path_obj = Path(new_path)
        if not new_path_obj.parent.exists() and not dry_run:
            new_path_obj.parent.mkdir(parents=True, exist_ok=True)
            logger.info(f"Created directory: {new_path_obj.parent}")

        # Move the file with git
        if not git_mv(old_path, new_path, dry_run):
            success = False
            continue

        # Refactor imports
        if not rope_refactor_imports(str(project_path), new_path, dry_run):
            success = False

    # Final verification with Ruff
    if not run_ruff(str(project_path), dry_run):
        success = False

    if success:
        logger.info("All operations completed successfully.")
        return 0
    else:
        logger.error("Some operations failed. Check the log for details.")
        return 1


if __name__ == "__main__":
    sys.exit(app())
