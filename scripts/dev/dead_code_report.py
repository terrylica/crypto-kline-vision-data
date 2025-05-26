#!/usr/bin/env python3
"""
Generate a comprehensive dead code report.

This script analyzes code and detects unused functions, variables, imports and unreachable code using Vulture.
It provides:
1. Summary of unused code by type
2. Files with most dead code
3. Dead code details with confidence levels
4. Suggestions for handling false positives

Usage:
    ./scripts/dev/dead_code_report.py [--min-confidence 60] [--sort-by SIZE|PATH|CONFIDENCE] [--exclude PATTERN]
"""

import re
import subprocess
import sys
from collections import Counter, defaultdict
from pathlib import Path

import typer
from rich.console import Console
from rich.table import Table

from utils.logger_setup import logger

# Initialize console for rich output
console = Console()

# Create Typer app
app = typer.Typer(help="Generate comprehensive dead code report using Vulture")

# Define constants
HIGH_CONFIDENCE_THRESHOLD = 90


def run_vulture(min_confidence=60, exclude_pattern=None, sort_by_size=False):
    """Run vulture and return its output.

    Args:
        min_confidence: Minimum confidence threshold (0-100)
        exclude_pattern: Optional regex pattern to exclude files
        sort_by_size: Whether to sort results by code size

    Returns:
        List of dead code items
    """
    try:
        cmd = ["vulture", "."]

        if min_confidence is not None:
            cmd.extend(["--min-confidence", str(min_confidence)])

        if exclude_pattern:
            cmd.extend(["--exclude", exclude_pattern])

        if sort_by_size:
            cmd.append("--sort-by-size")

        logger.debug(f"Running command: {' '.join(cmd)}")
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=False,
        )

        # Process the output to parse it into structured data
        items = []
        for line in result.stdout.splitlines():
            if not line.strip():
                continue

            # Parse vulture output lines which have format: file.py:line: unused thing 'name' (confidence%)
            match = re.match(
                r'(.+?):(\d+): (.+?) [\'"]?([^\'"]+)[\'"]? \((\d+)% confidence(?:, \d+ lines?)?\)',
                line,
            )
            if match:
                file_path, line_num, code_type, name, confidence = match.groups()
                items.append(
                    {
                        "filename": file_path,
                        "line": int(line_num),
                        "type": code_type,
                        "name": name,
                        "confidence": int(confidence),
                    }
                )
            else:
                logger.debug(f"Could not parse vulture output line: {line}")

        return items

    except subprocess.SubprocessError as e:
        logger.error(f"Error running vulture: {e}")
        return []
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return []


def generate_dead_code_summary(items, sort_by="confidence"):
    """Generate summary of dead code by type.

    Args:
        items: List of dead code items
        sort_by: How to sort the results ("confidence", "type", "count")

    Returns:
        Counter of dead code types
    """
    if not items:
        console.print("[yellow]No unused code found[/yellow]")
        return Counter()

    # Group by type of unused code
    type_counter = Counter(item["type"] for item in items)

    # Get confidence averages per type
    confidence_by_type = defaultdict(list)
    for item in items:
        confidence_by_type[item["type"]].append(item["confidence"])

    avg_confidence = {t: sum(confidences) / len(confidences) for t, confidences in confidence_by_type.items()}

    table = Table(title="Dead Code Summary by Type")
    table.add_column("Count", justify="right", style="cyan")
    table.add_column("Type", style="green")
    table.add_column("Avg Confidence", justify="right", style="yellow")

    # Sort according to preference
    if sort_by.lower() == "type":
        items = sorted(type_counter.items(), key=lambda x: x[0])
    elif sort_by.lower() == "confidence":
        items = sorted(type_counter.items(), key=lambda x: avg_confidence[x[0]], reverse=True)
    else:  # Default is by count
        items = type_counter.most_common()

    for code_type, count in items:
        table.add_row(str(count), code_type, f"{avg_confidence[code_type]:.1f}%")

    console.print(table)
    return type_counter


def generate_file_summary(items, sort_by="count"):
    """Generate file summary of dead code.

    Args:
        items: List of dead code items
        sort_by: How to sort the results ("count", "path")

    Returns:
        Counter of files
    """
    if not items:
        return Counter()

    file_counter = Counter(item["filename"] for item in items)

    # Get counts by type for each file
    types_by_file = defaultdict(Counter)
    for item in items:
        types_by_file[item["filename"]][item["type"]] += 1

    table = Table(title="Top Files with Dead Code")
    table.add_column("Count", justify="right", style="cyan")
    table.add_column("File", style="green")
    table.add_column("Breakdown by Type", style="yellow")

    # Sort according to preference
    if sort_by.lower() == "path":
        items = sorted(file_counter.items(), key=lambda x: x[0])[:15]
    else:  # Default is by count
        items = file_counter.most_common(15)

    for file, count in items:
        # Make the path relative to the workspace
        try:
            rel_path = Path(file).relative_to("/workspaces/data-source-manager")
        except ValueError:
            rel_path = file  # Fall back to full path if relative path fails

        # Format the type breakdown
        type_counts = types_by_file[file]
        breakdown = " ".join(f"{t}({c})" for t, c in type_counts.most_common())

        table.add_row(str(count), str(rel_path), breakdown)

    console.print(table)
    return file_counter


def generate_high_confidence_report(items):
    """Generate a report of high-confidence dead code.

    Args:
        items: List of dead code items
    """
    if not items:
        return

    # Filter for high confidence items (90%+)
    high_confidence = [item for item in items if item["confidence"] >= HIGH_CONFIDENCE_THRESHOLD]

    if not high_confidence:
        console.print(f"\n[[yellow]]No high-confidence ({HIGH_CONFIDENCE_THRESHOLD}%+) dead code found[[/yellow]]")
        return

    table = Table(title=f"High Confidence Dead Code ({HIGH_CONFIDENCE_THRESHOLD}%)")
    table.add_column("Confidence", justify="right", style="red")
    table.add_column("File", style="green")
    table.add_column("Line", style="cyan", justify="right")
    table.add_column("Type", style="yellow")
    table.add_column("Name", style="magenta")

    # Sort by confidence (highest first), then by file path
    sorted_items = sorted(high_confidence, key=lambda x: (-x["confidence"], x["filename"], x["line"]))

    for item in sorted_items:
        # Make the path relative to the workspace
        try:
            rel_path = Path(item["filename"]).relative_to("/workspaces/data-source-manager")
        except ValueError:
            rel_path = item["filename"]

        table.add_row(
            f"{item['confidence']}%",
            str(rel_path),
            str(item["line"]),
            item["type"],
            item["name"],
        )

    console.print(table)


def generate_false_positive_suggestions():
    """Generate suggestions for handling false positives."""
    console.print("\n[bold]Handling False Positives[/bold]")

    console.print("\n[bold green]1. Create a Whitelist[/bold green]")
    console.print("   Create a whitelist file to exclude known false positives:")
    console.print("   [cyan]vulture . --make-whitelist > whitelist.py[/cyan]")
    console.print("   [cyan]vulture . whitelist.py[/cyan]")

    console.print("\n[bold green]2. Prefix Unused Variables[/bold green]")
    console.print("   For function parameters or variables that can't be removed, prefix with underscore:")
    console.print("   [cyan]def process_data(data, _unused_param):[/cyan]")

    console.print("\n[bold green]3. Use the 'del' Keyword[/bold green]")
    console.print("   Delete unused variables to signal intentional non-use:")
    console.print("   [cyan]def example(x, y):[/cyan]")
    console.print("   [cyan]    del y  # Silence unused variable warning[/cyan]")
    console.print("   [cyan]    return x[/cyan]")

    console.print("\n[bold green]4. Noqa Comments for Imports[/bold green]")
    console.print("   For unused imports that are needed, use # noqa: F401:")
    console.print("   [cyan]import necessary_but_unused  # noqa: F401[/cyan]")


@app.command()
def main(
    min_confidence: int = typer.Option(60, "--min-confidence", "-c", help="Minimum confidence threshold (0-100)"),
    sort_by: str = typer.Option(
        "count",
        "--sort-by",
        "-s",
        help="Sort by: 'size', 'path', or 'confidence' or 'count' (default)",
    ),
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
        help="Show suggestions for handling false positives",
    ),
):
    """Generate a comprehensive dead code report using Vulture."""
    # Removed unused variable workspace_root

    console.print("[bold magenta]Dead Code Report[/bold magenta]")
    console.print("Analyzing unused code...\n")

    # Sort by size flag for vulture
    sort_by_size = sort_by.lower() == "size"

    # Run vulture and get unused code
    items = run_vulture(min_confidence, exclude_pattern, sort_by_size)

    if not items:
        console.print("[bold red]No unused code found or error running vulture.[/bold red]")
        return

    # Generate reports
    console.print(f"[bold blue]Found {len(items)} instances of unused code[/bold blue]\n")

    type_counter = generate_dead_code_summary(items, sort_by)
    console.print()

    file_counter = generate_file_summary(items, sort_by)
    console.print()

    generate_high_confidence_report(items)
    console.print()

    # Show suggestions for handling false positives if requested
    if show_suggestions:
        generate_false_positive_suggestions()

    # Print totals
    console.print(f"\n[bold]Total dead code types:[/bold] {len(type_counter)}")
    console.print(f"[bold]Total files with dead code:[/bold] {len(file_counter)}")
    console.print(f"[bold]Total instances of dead code:[/bold] {len(items)}")

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
