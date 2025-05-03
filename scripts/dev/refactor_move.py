#!/usr/bin/env python3
"""
Move files with git mv, refactor imports with Rope, and verify with Ruff.

This script helps move Python files while automatically handling import refactoring
and verifying the changes don't introduce import-related errors.
"""

import subprocess
import sys
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import List, Optional

import git  # Use GitPython instead of subprocess
import tomli
import typer

# Import print from rich for consistent styling
from rich.console import Console
from rich.prompt import Confirm
from rope.base.libutils import path_to_resource
from rope.base.project import Project
from rope.refactor.move import MoveModule

from utils.logger_setup import logger

# Define constants for verbose levels (using logger's internal constants)
VERBOSE_DEBUG = 2
VERBOSE_INFO = 1

# No need to set logger level here, it will be handled by setup_logging

console = Console()


# Find the project root (where pyproject.toml is located)
def find_project_root() -> Path:
    """Find the project root by locating pyproject.toml."""
    current_dir = Path.cwd()
    while current_dir != current_dir.parent:
        if (current_dir / "pyproject.toml").exists():
            return current_dir
        current_dir = current_dir.parent
    return Path.cwd()  # Fallback to current directory if not found


# Read Ruff configuration from pyproject.toml
def read_ruff_config() -> List[str]:
    """Read Ruff linting rules from pyproject.toml. Raises exception if not found."""
    project_root = find_project_root()
    pyproject_path = project_root / "pyproject.toml"

    with open(pyproject_path, "rb") as f:
        pyproject_data = tomli.load(f)

    # Get the lint.select value from the Ruff configuration
    ruff_config = pyproject_data.get("tool", {}).get("ruff", {})
    lint_config = ruff_config.get("lint", {})

    if isinstance(lint_config, dict) and "select" in lint_config:
        return lint_config["select"]

    # For older Ruff configurations (pre-v0.1.0)
    if "select" in ruff_config:
        return ruff_config["select"]

    raise ValueError("No Ruff select rules found in pyproject.toml")


# Read the Ruff rules from pyproject.toml - single source of truth
RUFF_IMPORT_CHECKS = read_ruff_config()
logger.debug(f"Using Ruff rules from pyproject.toml: {','.join(RUFF_IMPORT_CHECKS)}")

# Configure typer app with explicit help options to ensure -h works
app = typer.Typer(
    help="Move files with git, refactor imports, and verify with Ruff",
    context_settings={"help_option_names": ["--help", "-h"]},
)

# Define all commands using the decorator approach
moves_argument = typer.Argument(
    ...,
    help="Pairs of source and destination paths in format 'source.py:destination.py'. Source must exist.",
)
project_option = typer.Option(
    ".", "--project", "-p", help="Root of your Python project"
)
dry_run_option = typer.Option(
    False, "--dry-run", "-d", help="Show what would happen without making changes"
)
verbose_option = typer.Option(
    0,
    "--verbose",
    "-v",
    count=True,
    help="Increase verbosity: -v for INFO, -vv for DEBUG",
)
log_file_option = typer.Option(
    None,
    "--log-file",
    "-l",
    help="Custom log file name (default: refactor_YYYYMMDD_HHMMSS.log)",
)


@contextmanager
def rope_project(project_path: Path):
    """Context manager for Rope projects to ensure proper cleanup."""
    project = Project(str(project_path))
    try:
        yield project
    finally:
        project.close()


def git_mv(old_path: Path, new_path: Path, dry_run: bool = False) -> bool:
    """Move a file using git mv."""
    # Check if source file exists
    if not old_path.exists():
        logger.error(f"Source file does not exist: {old_path}")
        return False

    if dry_run:
        logger.info(f"[DRY-RUN] git mv {old_path} {new_path}")
        return True

    try:
        # Use GitPython to move the file
        repo = git.Repo(search_parent_directories=True)
        repo.git.mv(str(old_path), str(new_path))
        logger.info(f"Moved {old_path} -> {new_path}")
        return True
    except git.GitCommandError as e:
        logger.error(f"Git command failed: {e}")
        return False


def run_command(cmd: List[str], dry_run: bool = False) -> subprocess.CompletedProcess:
    """Run a command with proper error handling and dry-run support."""
    command_str = " ".join(cmd)
    logger.debug(f"Run command: {command_str}")

    if dry_run:
        logger.info(f"[DRY-RUN] Would run: {command_str}")
        # Return a dummy CompletedProcess for dry-run
        return subprocess.CompletedProcess(cmd, 0, stdout="", stderr="")

    try:
        return subprocess.run(cmd, check=True, text=True, capture_output=True)
    except subprocess.CalledProcessError as e:
        logger.error(f"Command failed: {command_str}")
        logger.error(f"Return code: {e.returncode}")
        if e.stdout:
            logger.debug(f"Command stdout: {e.stdout}")
        if e.stderr:
            logger.debug(f"Command stderr: {e.stderr}")
        raise


def run_ruff(project_path: Path, dry_run: bool = False) -> bool:
    """Run Ruff to check for import-related issues."""
    if dry_run:
        logger.info(
            f"[DRY-RUN] Ruff would run: ruff check {project_path} --select {','.join(RUFF_IMPORT_CHECKS)}"
        )
        return True

    logger.info("Running Ruff sanity check...")
    try:
        cmd = [
            "ruff",
            "check",
            str(project_path),
            "--select",
            ",".join(RUFF_IMPORT_CHECKS),
        ]
        result = run_command(cmd, dry_run)

        # If stdout contains "All checks passed!" or is empty, it's a success
        if not result.stdout or "All checks passed!" in result.stdout:
            logger.info("Ruff found no import-related issues.")
            return True

        # Otherwise, there are actual issues
        logger.warning(f"Ruff detected issues:\n{result.stdout}")
        return False
    except Exception as e:
        logger.error(f"Ruff check failed: {e}")
        return False


def fix_ruff_issues(project_path: Path, dry_run: bool = False) -> bool:
    """Fix import-related issues using Ruff's auto-fix capability."""
    if dry_run:
        logger.info(
            f"[DRY-RUN] Ruff would fix: ruff check {project_path} --select {','.join(RUFF_IMPORT_CHECKS)} --fix"
        )
        return True

    logger.info("Applying Ruff fixes to import-related issues...")
    try:
        cmd = [
            "ruff",
            "check",
            str(project_path),
            "--select",
            ",".join(RUFF_IMPORT_CHECKS),
            "--fix",
        ]
        result = run_command(cmd, dry_run)

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


def run_import_checks(project_path: Path, dry_run: bool = False) -> bool:
    """
    Run both Ruff and Pylint import checks to ensure comprehensive coverage.

    This function coordinates both tools to catch different types of import errors:
    - Ruff catches most style and simple import issues
    - Pylint catches ModuleNotFoundError cases which Ruff might miss

    Returns:
        bool: True if both checks pass, False otherwise
    """
    ruff_success = run_ruff(project_path, dry_run)
    pylint_success = run_pylint(project_path, dry_run)

    return ruff_success and pylint_success


def run_ruff_pre_check(project_path: Path, dry_run: bool = False) -> bool:
    """Run import checks to detect issues before we start moving files."""
    if dry_run:
        logger.info(
            f"[DRY-RUN] Would run pre-check for import issues on {project_path}"
        )
        return True

    logger.info("Running pre-check for existing import issues...")

    with console.status("[bold yellow]Running pre-check with Ruff and Pylint..."):
        success = run_import_checks(project_path, dry_run)

    if success:
        logger.info("Pre-check passed: no pre-existing import issues found.")
        return True

    # If there are issues, warn and ask for confirmation
    logger.warning(
        "Pre-check detected import issues. These issues exist before refactoring."
    )
    logger.warning(
        "They may not be caused by the move operation but could make it harder to detect new issues."
    )
    # Use rich's Confirm for better UX
    return Confirm.ask("Continue despite pre-existing issues?")


def update_import_paths(
    project_path: Path, old_path: Path, new_path: Path, dry_run: bool = False
) -> bool:
    """Update import paths across the codebase using Rope."""
    if dry_run:
        logger.info(
            f"[DRY-RUN] Would update import paths from {old_path} to {new_path} using Rope"
        )
        return True

    logger.info(f"Updating import paths from '{old_path}' to '{new_path}' using Rope")

    try:
        # Use context manager for Rope project
        with rope_project(project_path) as project:
            # Create relative paths to the project root
            old_rel_path = str(
                old_path.relative_to(project_path)
                if old_path.is_absolute()
                else old_path
            )
            new_rel_path = str(
                new_path.relative_to(project_path)
                if new_path.is_absolute()
                else new_path
            )

            # Get the resources
            old_resource = path_to_resource(project, old_rel_path)

            # Use MoveModule for Python files
            if old_path.suffix == ".py":
                # Create the destination folder if it doesn't exist
                new_parent = new_path.parent
                new_parent.mkdir(parents=True, exist_ok=True)

                # Get the destination folder resource
                dest_folder = str(Path(new_rel_path).parent)
                if not dest_folder:
                    dest_folder = "."

                # Get the new module name
                new_module_name = new_path.stem

                # Perform the move
                mover = MoveModule(project, old_resource)
                changes = mover.get_changes(dest_folder, new_module_name)
                project.do(changes)

                logger.info(
                    "Rope successfully updated import references across the codebase"
                )
                return True
            logger.info(
                f"Skipping Rope import refactoring for non-Python file: {old_path}"
            )
            return True

    except Exception as e:
        logger.error(f"Error updating import paths with Rope: {e}")
        return False


def run_pylint(project_path: Path, dry_run: bool = False) -> bool:
    """Run Pylint to check for import-related issues."""
    if dry_run:
        logger.info(
            f"[DRY-RUN] Pylint would run: pylint --disable=all --enable=import-error {project_path}"
        )
        return True

    logger.info("Running Pylint to check for import issues...")
    try:
        cmd = [
            "pylint",
            "--disable=all",
            "--enable=import-error",
            str(project_path),
        ]
        result = run_command(cmd, dry_run)

        # Pylint returns non-zero on import errors, so we need to parse output
        if "E0401" not in result.stdout and "E0401" not in result.stderr:
            logger.info("Pylint found no import errors.")
            return True

        # If we get here, there are import errors
        logger.warning(f"Pylint detected import errors:\n{result.stdout}")
        return False
    except Exception as e:
        logger.error(f"Pylint check failed: {e}")
        return False


def setup_logging(verbose: int, log_file: Optional[str] = None) -> str:
    """Configure logging settings and return log filename."""
    # Setup log file if not provided
    if not log_file:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = f"refactor_{timestamp}.log"

    # Set the log level based on verbosity
    if verbose >= VERBOSE_DEBUG:
        logger.setLevel(logger.DEBUG)
    elif verbose == VERBOSE_INFO:
        logger.setLevel(logger.INFO)

    return log_file


def process_move_pair(
    move_pair: str, project_path: Path, dry_run: bool, skip_validation: bool
) -> bool:
    """Process a single move operation pair."""
    try:
        old_path, new_path = move_pair.split(":")
        old_path_obj = Path(old_path)
        new_path_obj = Path(new_path)
    except ValueError:
        logger.error(f"Invalid move format '{move_pair}'; use old_path:new_path")
        return False

    logger.info(f"Processing move: {old_path} -> {new_path}")

    # Check if source exists and destination doesn't already exist
    if not old_path_obj.exists():
        if new_path_obj.exists() and not skip_validation:
            logger.warning(
                f"Source file {old_path} does not exist, but destination {new_path} does - assuming already moved"
            )
            return True
        if skip_validation:
            logger.warning(
                f"Source file {old_path} does not exist, but proceeding due to --skip-validation"
            )
        else:
            logger.error(f"Source file does not exist: {old_path}")
            return False

    # Move the file with git
    if not git_mv(old_path_obj, new_path_obj, dry_run):
        return False

    # Update import paths across the codebase using Rope
    if not update_import_paths(project_path, old_path_obj, new_path_obj, dry_run):
        logger.warning(f"Failed to update import paths for {old_path} -> {new_path}")
        return False

    return True


def handle_auto_fix(project_path: Path, dry_run: bool, auto_fix_imports: bool) -> bool:
    """Handle auto-fixing of import issues if enabled."""
    if auto_fix_imports and not dry_run:
        logger.info("Attempting to auto-fix import issues...")
        if fix_ruff_issues(project_path, dry_run):
            logger.info("Auto-fix applied, running final verification")
            if run_import_checks(project_path, dry_run):
                logger.info("All issues fixed automatically!")
                return True
            logger.error(
                "Some issues remain after auto-fix. Manual intervention required."
            )
            return False
        logger.error("Auto-fix failed. Manual intervention required.")
        return False
    if auto_fix_imports and dry_run:
        logger.info("[DRY-RUN] Would attempt to auto-fix import issues")
    else:
        logger.warning("Use --auto-fix flag to attempt automatic fix of import issues")
    return False


class MoveConfig:
    """Configuration class for the move command to reduce function arguments."""

    def __init__(
        self,
        moves: List[str],
        project: str = ".",
        dry_run: bool = False,
        verbose: int = 0,
        log_file: Optional[str] = None,
        skip_validation: bool = False,
        skip_pre_check: bool = False,
        auto_fix_imports: bool = False,
    ):
        self.moves = moves
        self.project = project
        self.dry_run = dry_run
        self.verbose = verbose
        self.log_file = log_file
        self.skip_validation = skip_validation
        self.skip_pre_check = skip_pre_check
        self.auto_fix_imports = auto_fix_imports
        self.project_path = Path(project).resolve()


@app.command()
def move(
    moves: List[str] = moves_argument,
    project: str = project_option,
    dry_run: bool = dry_run_option,
    verbose: int = verbose_option,
    log_file: Optional[str] = log_file_option,
    skip_validation: bool = typer.Option(
        False, "--skip-validation", "-s", help="Skip file existence validation"
    ),
    skip_pre_check: bool = typer.Option(
        False,
        "--skip-pre-check",
        "-S",
        help="Skip pre-check for existing import issues",
    ),
    auto_fix_imports: bool = typer.Option(
        False,
        "--auto-fix",
        "-a",
        help="Auto-fix import issues with Ruff after refactoring",
    ),
):
    """
    Move files with git mv, refactor imports via Rope, and verify with Ruff and Pylint.

    This command handles moving Python files while ensuring imports remain valid.
    It first uses git mv to move the files, then uses Rope to update imports,
    and finally runs import checks to verify no import errors were introduced.

    Example:
        ./refactor_move.py move "existing_file.py:new_location.py"
    """
    # Create config object
    config = MoveConfig(
        moves=moves,
        project=project,
        dry_run=dry_run,
        verbose=verbose,
        log_file=log_file,
        skip_validation=skip_validation,
        skip_pre_check=skip_pre_check,
        auto_fix_imports=auto_fix_imports,
    )

    return execute_move(config)


def execute_move(config: MoveConfig) -> int:
    """Execute the move operation using the provided configuration."""
    # Setup logging
    config.log_file = setup_logging(config.verbose, config.log_file)

    logger.debug(
        f"Arguments: moves={config.moves}, project={config.project}, dry_run={config.dry_run}"
    )
    logger.debug(f"Using Ruff rules: {','.join(RUFF_IMPORT_CHECKS)}")

    # Process each move operation
    success = True

    # Run pre-check if not skipped
    if not config.skip_pre_check:
        if not run_ruff_pre_check(config.project_path, config.dry_run):
            logger.error("Pre-check failed. Exiting.")
            return 1
    else:
        logger.info("Skipping pre-check for existing import issues as requested.")

    # Process all move pairs
    with console.status("[bold green]Processing file moves...") as status:
        for move_pair in config.moves:
            status.update(f"[bold green]Processing: {move_pair}")
            success = (
                process_move_pair(
                    move_pair,
                    config.project_path,
                    config.dry_run,
                    config.skip_validation,
                )
                and success
            )

    # Final verification with both Ruff and Pylint
    with console.status("[bold yellow]Verifying imports..."):
        if not run_import_checks(config.project_path, config.dry_run):
            logger.warning("Import checks found issues after refactoring")

            # Try to auto-fix import issues if enabled
            if not handle_auto_fix(
                config.project_path, config.dry_run, config.auto_fix_imports
            ):
                success = False

    if success:
        console.print("[bold green]✓ All operations completed successfully.")
        return 0
    console.print("[bold red]✗ Some operations failed. Check the log for details.")
    return 1


@app.command(name="check")
def check_imports(
    project: str = project_option,
    dry_run: bool = dry_run_option,
    verbose: int = verbose_option,
    log_file: Optional[str] = log_file_option,
):
    """
    Run Ruff and Pylint to check for import-related issues on the current codebase state.
    """
    # Setup logging
    if not log_file:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = f"import_check_{timestamp}.log"

    # Use the same setup_logging function for consistency
    setup_logging(verbose, log_file)

    logger.debug(f"Arguments: project={project}, dry_run={dry_run}")
    logger.debug(f"Using Ruff rules: {','.join(RUFF_IMPORT_CHECKS)}")

    project_path = Path(project).resolve()

    with console.status("[bold yellow]Checking imports with Ruff and Pylint..."):
        success = run_import_checks(project_path, dry_run)

    if success:
        console.print("[bold green]✓ All import checks completed successfully.")
        return 0
    console.print("[bold red]✗ Some import checks failed. Check the log for details.")
    return 1


if __name__ == "__main__":
    sys.exit(app())
