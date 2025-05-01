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

# Define the specific Ruff import-related codes to check
RUFF_IMPORT_CHECKS = [
    "F821",  # Undefined name
    "F822",  # Undefined name '...' in __all__
    "F823",  # Local variable '...' referenced before assignment
    "F401",  # F401: 'module.name' imported but unused
    "F402",  # F402: Module 'module' imported more than once
    "F403",  # F403: 'from module import *' used; unable to detect undefined names
    "F632",  # F632: Use of `in <constant>` where <constant> is a list or tuple. Use a set instead.
    "F841",  # F841: Local variable '...' is assigned to but never used
    "I001",  # Unsorted imports
    "ARG",  # Unused arguments
    "B006",  # Mutable argument default
    "B008",  # Function call in default argument
    "PLC0415",  # Import outside top level
]

# Configure typer app with explicit help options to ensure -h works
app = typer.Typer(
    help="Move files with git, refactor imports, and verify with Ruff",
    context_settings={"help_option_names": ["--help", "-h"]},
)


def git_mv(old_path: str, new_path: str, dry_run: bool = False) -> bool:
    """Move a file using git mv."""
    # Check if source file exists
    if not Path(old_path).exists():
        logger.error(f"Source file does not exist: {old_path}")
        return False

    # Ensure destination directory exists
    new_path_obj = Path(new_path)
    if not new_path_obj.parent.exists() and not dry_run:
        logger.debug(f"Creating destination directory: {new_path_obj.parent}")
        new_path_obj.parent.mkdir(parents=True, exist_ok=True)
        logger.info(f"Created directory: {new_path_obj.parent}")

    if dry_run:
        logger.info(f"[DRY-RUN] git mv {old_path} {new_path}")
        return True
    else:
        logger.debug(f"Running: git mv {old_path} {new_path}")
        try:
            # Create the destination directory first
            if not new_path_obj.parent.exists():
                new_path_obj.parent.mkdir(parents=True, exist_ok=True)
                logger.info(f"Created directory: {new_path_obj.parent}")

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

    # Skip refactoring for non-Python files
    if not file_path.endswith(".py"):
        logger.info(f"Skipping import refactoring for non-Python file: {file_path}")
        return True

    # Check if the file exists
    if not Path(file_path).exists():
        logger.error(f"Cannot refactor imports: File not found: {file_path}")
        return False

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
            f"[DRY-RUN] Ruff would run: ruff check {project_path} --select {','.join(RUFF_IMPORT_CHECKS)}"
        )
        return True

    logger.info("Running Ruff sanity check...")
    try:
        result = subprocess.run(
            [
                "ruff",
                "check",
                project_path,
                "--select",
                ",".join(RUFF_IMPORT_CHECKS),
            ],
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


def fix_ruff_issues(project_path: str, dry_run: bool = False) -> bool:
    """Fix import-related issues using Ruff's auto-fix capability.

    This runs after Rope refactoring to clean up any remaining import issues.

    Args:
        project_path: Path to the project root
        dry_run: Whether to actually run the fixes or just log

    Returns:
        True if fixes were applied (or would be in dry run), False if errors occurred
    """
    if dry_run:
        logger.info(
            f"[DRY-RUN] Ruff would fix: ruff check {project_path} --select {','.join(RUFF_IMPORT_CHECKS)} --fix"
        )
        return True

    logger.info("Applying Ruff fixes to import-related issues...")
    try:
        result = subprocess.run(
            [
                "ruff",
                "check",
                project_path,
                "--select",
                ",".join(RUFF_IMPORT_CHECKS),
                "--fix",
            ],
            capture_output=True,
            text=True,
        )

        if "error:" in result.stderr.lower():
            logger.error(f"Ruff fix encountered errors:\n{result.stderr}")
            return False

        fixed_count = result.stdout.count("fixed")
        if fixed_count > 0:
            logger.info(f"Ruff automatically fixed {fixed_count} import-related issues")
        else:
            logger.info("No import-related issues required fixing by Ruff")

        return True
    except Exception as e:
        logger.error(f"Ruff fix failed: {e}")
        return False


def run_ruff_pre_check(project_path: str, dry_run: bool = False) -> bool:
    """Run Ruff to check for import-related issues before we start moving files.

    This helps identify existing issues before refactoring, to avoid attributing
    pre-existing issues to the refactoring process.

    Args:
        project_path: Path to the project root
        dry_run: Whether to actually run the check or just log

    Returns:
        True if no issues found or in dry run mode, False if issues found
    """
    if dry_run:
        logger.info(
            f"[DRY-RUN] Ruff would run pre-check: ruff check {project_path} --select {','.join(RUFF_IMPORT_CHECKS)}"
        )
        return True

    logger.info("Running Ruff pre-check for existing import issues...")
    try:
        result = subprocess.run(
            [
                "ruff",
                "check",
                project_path,
                "--select",
                ",".join(RUFF_IMPORT_CHECKS),
            ],
            capture_output=True,
            text=True,
        )
        if result.stdout:
            logger.warning(f"Ruff detected pre-existing issues:\n{result.stdout}")
            logger.warning(
                "These issues exist before refactoring. They may not be caused by the move operation."
            )
            answer = input("Continue despite pre-existing issues? (y/n): ").lower()
            return answer == "y"
        else:
            logger.info("Ruff pre-check passed: no pre-existing import issues found.")
            return True
    except Exception as e:
        logger.error(f"Ruff pre-check failed: {e}")
        return False


def update_import_paths(
    project_path: str, old_path: str, new_path: str, dry_run: bool = False
) -> bool:
    """Update import paths across the codebase by using grep and sed.

    Args:
        project_path: Path to the project root
        old_path: Original path of the file
        new_path: New path of the file
        dry_run: Whether to actually run the fixes or just log

    Returns:
        True if operation was successful, False otherwise
    """
    if dry_run:
        logger.info(
            f"[DRY-RUN] Would update import paths from {old_path} to {new_path}"
        )
        return True

    # Convert file paths to import paths (remove .py, replace / with .)
    def path_to_import(path):
        path = str(path)
        if path.endswith(".py"):
            path = path[:-3]
        return path.replace("/", ".")

    old_import = path_to_import(old_path)
    new_import = path_to_import(new_path)

    logger.info(f"Updating import paths from '{old_import}' to '{new_import}'")

    try:
        # Find all Python files that might import the moved file
        grep_result = subprocess.run(
            ["grep", "-r", f"import.*{old_import}", "--include=*.py", project_path],
            capture_output=True,
            text=True,
        )

        if grep_result.returncode not in [0, 1]:  # 0 = matches found, 1 = no matches
            logger.error(f"Error searching for imports: {grep_result.stderr}")
            return False

        # Parse results to get files with imports
        files_with_imports = []
        for line in grep_result.stdout.splitlines():
            if ":" in line:
                file_path = line.split(":", 1)[0]
                files_with_imports.append(file_path)

        if not files_with_imports:
            logger.info(f"No files found importing {old_import}")
            return True

        logger.info(f"Found {len(files_with_imports)} files with imports to update")

        # Update imports in each file
        for file_path in files_with_imports:
            try:
                with open(file_path, "r") as f:
                    content = f.read()

                # Replace various import forms
                # from old_import import X
                updated_content = content.replace(
                    f"from {old_import} import", f"from {new_import} import"
                )
                # import old_import
                updated_content = updated_content.replace(
                    f"import {old_import}", f"import {new_import}"
                )
                # import old_import as X
                updated_content = updated_content.replace(
                    f"import {old_import} as", f"import {new_import} as"
                )

                if content != updated_content:
                    with open(file_path, "w") as f:
                        f.write(updated_content)
                    logger.info(f"Updated imports in {file_path}")
            except Exception as e:
                logger.error(f"Failed to update imports in {file_path}: {e}")

        return True
    except Exception as e:
        logger.error(f"Error updating import paths: {e}")
        return False


@app.command()
def move(
    moves: List[str] = typer.Argument(
        ...,
        help="Pairs of source and destination paths in format 'source.py:destination.py'. Source must exist.",
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
    skip_validation: bool = typer.Option(
        False, "--skip-validation", "-s", help="Skip file existence validation"
    ),
    skip_pre_check: bool = typer.Option(
        False,
        "--skip-pre-check",
        "-S",
        help="Skip Ruff pre-check for existing import issues",
    ),
    auto_fix_imports: bool = typer.Option(
        False,
        "--auto-fix",
        "-a",
        help="Auto-fix import issues with Ruff after refactoring",
    ),
):
    """
    Move files with git mv, refactor imports via Rope, and verify with Ruff.

    This command handles moving Python files while ensuring imports remain valid.
    It first uses git mv to move the files, then uses Rope to update imports,
    and finally runs Ruff to verify no import errors were introduced.

    Example:
        ./refactor_move.py move "existing_file.py:new_location.py"
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

    # Run pre-check if not skipped
    if not skip_pre_check:
        if not run_ruff_pre_check(str(project_path), dry_run):
            logger.error("Pre-check failed. Exiting.")
            return 1
    else:
        logger.info("Skipping pre-check for existing import issues as requested.")

    for move_pair in moves:
        try:
            old_path, new_path = move_pair.split(":")
        except ValueError:
            logger.error(f"Invalid move format '{move_pair}'; use old_path:new_path")
            success = False
            continue

        logger.info(f"Processing move: {old_path} -> {new_path}")

        # Check if source exists and destination doesn't already exist
        old_path_obj = Path(old_path)
        new_path_obj = Path(new_path)

        if not old_path_obj.exists():
            if new_path_obj.exists() and not skip_validation:
                logger.warning(
                    f"Source file {old_path} does not exist, but destination {new_path} does - assuming already moved"
                )
                continue
            elif skip_validation:
                logger.warning(
                    f"Source file {old_path} does not exist, but proceeding due to --skip-validation"
                )
            else:
                logger.error(f"Source file does not exist: {old_path}")
                success = False
                continue

        # Move the file with git
        if not git_mv(old_path, new_path, dry_run):
            success = False
            continue

        # Update import paths across the codebase
        if not update_import_paths(str(project_path), old_path, new_path, dry_run):
            logger.warning(
                f"Failed to update import paths for {old_path} -> {new_path}"
            )
            # Continue anyway as Rope might fix some issues

        # Refactor imports
        if not rope_refactor_imports(str(project_path), new_path, dry_run):
            success = False

    # Final verification with Ruff
    if not run_ruff(str(project_path), dry_run):
        logger.warning("Ruff check found issues after refactoring")

        # Try to auto-fix import issues if enabled
        if auto_fix_imports and not dry_run:
            logger.info("Attempting to auto-fix import issues with Ruff...")
            if fix_ruff_issues(str(project_path), dry_run):
                logger.info("Auto-fix applied, running final verification")
                if run_ruff(str(project_path), dry_run):
                    logger.info("All issues fixed automatically!")
                else:
                    logger.error(
                        "Some issues remain after auto-fix. Manual intervention required."
                    )
                    success = False
            else:
                logger.error("Auto-fix failed. Manual intervention required.")
                success = False
        else:
            if auto_fix_imports and dry_run:
                logger.info(
                    "[DRY-RUN] Would attempt to auto-fix import issues with Ruff"
                )
            else:
                logger.warning(
                    "Use --auto-fix flag to attempt automatic fix of import issues"
                )
            success = False

    if success:
        logger.info("All operations completed successfully.")
        return 0
    else:
        logger.error("Some operations failed. Check the log for details.")
        return 1


@app.command()
def check_ruff(
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
    Run Ruff to check for import-related issues on the current codebase state.
    """
    # Setup log file if not provided
    if not log_file:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = f"ruff_check_{timestamp}.log"

    # Set the log level
    if verbose >= 2:
        logger.setLevel(20)  # DEBUG in rich logger
    elif verbose == 1:
        logger.setLevel(30)  # INFO in rich logger

    logger.debug(f"Arguments: project={project}, dry_run={dry_run}")

    success = run_ruff(str(Path(project)), dry_run)

    if success:
        logger.info("Ruff check completed successfully.")
        return 0
    else:
        logger.error("Ruff check failed. Check the log for details.")
        return 1


if __name__ == "__main__":
    sys.exit(app())
