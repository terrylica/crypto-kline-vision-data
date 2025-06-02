#!/usr/bin/env python3
"""
Generate a comprehensive Ruff linter report.

This script analyzes Ruff linter output and provides:
1. Error code summary
2. Top files with issues
3. Top file/error combinations
4. Error code explanations

Usage:
    ./scripts/dev/linter_report.py [--sort-by CODE|FILE|COUNT] [--exclude PATTERN]
"""

import json
import re
import subprocess
import sys
from collections import Counter, defaultdict
from pathlib import Path

import typer
from rich.console import Console
from rich.table import Table

from utils.loguru_setup import logger

# Initialize console for rich output
console = Console()

# Create Typer app
app = typer.Typer(help="Generate comprehensive Ruff linter report")


def get_error_explanation(code):
    """Dynamically fetch error explanation from ruff.

    Args:
        code: The error code (e.g., "PLR0912")

    Returns:
        A string explanation of the error code
    """
    try:
        # Call ruff rule to get explanation
        result = subprocess.run(
            ["ruff", "rule", code],
            capture_output=True,
            text=True,
            timeout=3,
            check=False,  # Add timeout to prevent hanging
        )

        if result.returncode == 0 and result.stdout:
            # Extract the explanation (first line is usually the title)
            explanation_lines = result.stdout.strip().split("\n")
            if len(explanation_lines) > 1:
                # Return the first meaningful line that's not just the code
                for line in explanation_lines:
                    # Minimum meaningful line length to avoid short descriptions
                    MIN_LINE_LENGTH = 5
                    if line and not line.startswith(code) and len(line) > MIN_LINE_LENGTH:
                        return line.strip()
                # Fallback to the first line if no meaningful line found
                return explanation_lines[0].strip()
            return explanation_lines[0].strip()

        # If ruff rule doesn't work, try a more basic approach
        return f"Rule {code}"
    except (subprocess.SubprocessError, Exception) as e:
        logger.debug(f"Error getting explanation for {code}: {e}")
        return f"Rule {code}"


def run_ruff_check(exclude_pattern=None):
    """Run ruff check and return JSON output.

    Args:
        exclude_pattern: Optional regex pattern to exclude files

    Returns:
        List of issue dictionaries
    """
    try:
        cmd = ["ruff", "check", ".", "--output-format=json"]
        if exclude_pattern:
            cmd.extend(["--exclude", exclude_pattern])

        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=False,
        )

        # Ruff returns a non-zero exit code when it finds issues, which is expected
        # So we can't use check=True here

        if not result.stdout.strip():
            if result.stderr:
                logger.error(f"Error from ruff: {result.stderr}")
            return []

        try:
            return json.loads(result.stdout)
        except json.JSONDecodeError as e:
            logger.error(f"Error parsing ruff JSON output: {e}")
            logger.error(f"Raw output: {result.stdout[:500]}...")
            return []

    except subprocess.SubprocessError as e:
        logger.error(f"Error running ruff check: {e}")
        return []
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return []


def filter_issues_by_pattern(issues, exclude_pattern):
    """Filter issues by regex pattern on filename."""
    if not exclude_pattern:
        return issues

    try:
        pattern = re.compile(exclude_pattern)
        return [issue for issue in issues if not pattern.search(issue["filename"])]
    except re.error as e:
        logger.error(f"Invalid regex pattern '{exclude_pattern}': {e}")
        return issues


def generate_error_code_summary(issues, sort_by="count"):
    """Generate error code summary.

    Args:
        issues: List of issue dictionaries
        sort_by: How to sort the results ("count", "code")

    Returns:
        Counter of error codes
    """
    if not issues:
        console.print("[yellow]No issues found[/yellow]")
        return Counter()

    code_counter = Counter(issue["code"] for issue in issues)

    table = Table(title="Error Code Summary")
    table.add_column("Count", justify="right", style="cyan")
    table.add_column("Error Code", style="green")
    table.add_column("Description", style="yellow")

    # Sort according to preference
    items = sorted(code_counter.items(), key=lambda x: x[0]) if sort_by.lower() == "code" else code_counter.most_common()

    # Cache error explanations to avoid redundant subprocess calls
    explanation_cache = {}

    for code, count in items:
        if code not in explanation_cache:
            explanation_cache[code] = get_error_explanation(code)

        explanation = explanation_cache[code]
        table.add_row(str(count), code, explanation)

    console.print(table)
    return code_counter


def generate_file_summary(issues, sort_by="count"):
    """Generate file summary.

    Args:
        issues: List of issue dictionaries
        sort_by: How to sort the results ("count", "file")

    Returns:
        Counter of files
    """
    if not issues:
        return Counter()

    file_counter = Counter(issue["filename"] for issue in issues)

    table = Table(title="Top Files with Issues")
    table.add_column("Count", justify="right", style="cyan")
    table.add_column("File", style="green")

    # Sort according to preference
    items = sorted(file_counter.items(), key=lambda x: x[0])[:15] if sort_by.lower() == "file" else file_counter.most_common(15)

    for file, count in items:
        # Make the path relative to the workspace
        try:
            rel_path = Path(file).relative_to("/workspaces/data-source-manager")
        except ValueError:
            rel_path = file  # Fall back to full path if relative path fails

        table.add_row(str(count), str(rel_path))

    console.print(table)
    return file_counter


def generate_file_error_combinations(issues, sort_by="count"):
    """Generate file/error combinations.

    Args:
        issues: List of issue dictionaries
        sort_by: How to sort the results ("count", "file", "code")
    """
    if not issues:
        return

    combinations = Counter()
    # Track codes and their associated issues for description lookup
    code_examples = {}

    for issue in issues:
        code = issue["code"]
        key = f"{issue['filename']}: {code}"
        combinations[key] += 1

        # Store the first example of each code for later description lookup
        if code not in code_examples:
            code_examples[code] = issue

    table = Table(title="Top File/Error Combinations")
    table.add_column("Count", justify="right", style="cyan")
    table.add_column("File", style="green")
    table.add_column("Error", style="red")
    table.add_column("Description", style="yellow")

    # Sort according to preference
    if sort_by.lower() == "file":
        # Sort by filename first, then count (descending)
        items = sorted(
            combinations.items(),
            key=lambda x: (x[0].split(": ")[0], -combinations[x[0]]),
        )[:20]
    elif sort_by.lower() == "code":
        # Sort by error code first, then count (descending)
        items = sorted(
            combinations.items(),
            key=lambda x: (x[0].split(": ")[1], -combinations[x[0]]),
        )[:20]
    else:  # Default is by count
        items = combinations.most_common(20)

    # Cache error descriptions to avoid redundant lookups
    description_cache = {}

    for combo, count in items:
        try:
            file, code = combo.split(": ", 1)

            # Get description (with caching)
            if code not in description_cache:
                description_cache[code] = get_error_explanation(code)
            description = description_cache[code]

            # Make the path relative to the workspace
            try:
                rel_path = Path(file).relative_to("/workspaces/data-source-manager")
            except ValueError:
                rel_path = file  # Fall back to full path if relative path fails

            table.add_row(str(count), str(rel_path), code, description)
        except ValueError:
            # Handle unexpected format
            table.add_row(str(count), combo, "", "Unknown format")

    console.print(table)


def generate_file_error_breakdown(issues, sort_by="count"):
    """Generate breakdown of errors per file.

    Args:
        issues: List of issue dictionaries
        sort_by: How to sort the results ("count", "file")
    """
    if not issues:
        return

    file_errors = defaultdict(Counter)
    for issue in issues:
        file_errors[issue["filename"]][issue["code"]] += 1

    table = Table(title="File Error Breakdown (Top 10 Files)")
    table.add_column("File", style="green")
    table.add_column("Total Issues", justify="right", style="cyan")
    table.add_column("Error Breakdown", style="yellow")

    # Sort according to preference
    if sort_by.lower() == "file":
        # Sort by filename
        items = sorted(file_errors.items())[:10]
    else:  # Default is by count
        # Sort by total issues per file (descending)
        items = sorted(file_errors.items(), key=lambda x: sum(x[1].values()), reverse=True)[:10]

    for file, errors in items:
        try:
            rel_path = Path(file).relative_to("/workspaces/data-source-manager")
        except ValueError:
            rel_path = file  # Fall back to full path if relative path fails

        total = sum(errors.values())

        # Format the error breakdown
        # Sort by code for consistent output if requested
        error_items = sorted(errors.items()) if sort_by.lower() == "code" else errors.most_common()

        error_str = " ".join(f"{code}({count})" for code, count in error_items)

        table.add_row(str(rel_path), str(total), error_str)

    console.print(table)


def generate_fix_suggestions(issues):
    """Generate suggestions for fixing the most common issues."""
    if not issues:
        return

    # Group issues by code
    issue_types = defaultdict(list)
    for issue in issues:
        code = issue["code"]
        issue_types[code].append(issue)

    # Get the top 3 most common issues
    top_issues = sorted(issue_types.items(), key=lambda x: len(x[1]), reverse=True)[:3]

    console.print("\n[bold]Top Issues Fix Suggestions[/bold]")

    # Prepare explanation cache
    explanation_cache = {}

    for code, issues_list in top_issues:
        if code not in explanation_cache:
            explanation_cache[code] = get_error_explanation(code)

        explanation = explanation_cache[code]
        console.print(f"\n[bold green]{code}[/bold green]: {explanation}")
        console.print(f"[italic]Found in {len(issues_list)} places[/italic]")

        # Get examples from the actual issues
        if issues_list and "message" in issues_list[0]:
            sample_message = issues_list[0]["message"]
            console.print(f"[yellow]Example message:[/yellow] {sample_message}")

        # Get a sample location
        if issues_list:
            sample = issues_list[0]
            if "filename" in sample and "line_start" in sample:
                file_path = Path(sample["filename"]).relative_to("/workspaces/data-source-manager")
                line = sample.get("line_start", "?")
                console.print(f"[yellow]Sample location:[/yellow] {file_path}:{line}")


@app.command()
def main(
    sort_by: str = typer.Option("count", "--sort-by", "-s", help="Sort by: 'code', 'file', or 'count' (default)"),
    exclude_pattern: str = typer.Option(
        None,
        "--exclude",
        "-e",
        help="Regex pattern to exclude files (e.g. 'test|.git')",
    ),
    show_suggestions: bool = typer.Option(
        True,
        "--suggestions/--no-suggestions",
        "-g/-G",
        help="Show fix suggestions for common issues",
    ),
):
    """Generate a comprehensive Ruff linter report."""
    console.print("[bold magenta]Ruff Linter Report[/bold magenta]")
    console.print("Analyzing code issues...\n")

    # Run ruff and get issues
    issues = run_ruff_check(exclude_pattern)

    # Filter issues if needed (for custom patterns beyond what ruff supports)
    issues = filter_issues_by_pattern(issues, exclude_pattern)

    if not issues:
        console.print("[bold red]No issues found or error running ruff.[/bold red]")
        return

    # Generate reports
    console.print(f"[bold blue]Found {len(issues)} issues[/bold blue]\n")

    code_counter = generate_error_code_summary(issues, sort_by)
    console.print()

    file_counter = generate_file_summary(issues, sort_by)
    console.print()

    generate_file_error_combinations(issues, sort_by)
    console.print()

    generate_file_error_breakdown(issues, sort_by)
    console.print()

    # Show fix suggestions if requested
    if show_suggestions:
        generate_fix_suggestions(issues)
        console.print()

    # Print totals
    console.print(f"[bold]Total error codes:[/bold] {len(code_counter)}")
    console.print(f"[bold]Total files with issues:[/bold] {len(file_counter)}")

    if sort_by != "count":
        console.print(f"[italic]Results sorted by: {sort_by}[/italic]")

    if exclude_pattern:
        console.print(f"[italic]Excluded files matching: {exclude_pattern}[/italic]")


if __name__ == "__main__":
    try:
        app()
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        console.print(f"[bold red]Error:[/bold red] {e}")
        sys.exit(1)
