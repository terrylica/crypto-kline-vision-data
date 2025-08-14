#!/usr/bin/env python3
"""
Documentation utilities for DSM Demo CLI applications.

This module contains functions to generate Markdown documentation from Typer help text
using the official typer-cli tool for optimal GitHub-friendly output.
"""

import re
import subprocess
import sys
from pathlib import Path

import typer
from rich.console import Console
from rich.markdown import Markdown

from data_source_manager.utils.for_demo.dsm_help_content import (
    APP_BEHAVIOR,
    APP_TITLE,
    DATA_SOURCES,
    FCP_NAME,
    RETRIEVES_DATA,
    SAMPLE_COMMANDS,
)
from data_source_manager.utils.loguru_setup import logger


def generate_markdown_docs_with_typer_cli(
    app: typer.Typer,
    output_dir: str = "docs/dsm_demo_cli",
    filename: str = "README.md",
    cli_name: str | None = None,
):
    """Generate markdown documentation using typer-cli.

    This function uses the typer-cli utility to generate markdown docs
    automatically from a Typer application's commands and options.

    Note: The app parameter is required for API consistency with generate_markdown_docs,
    but is not directly used as typer-cli inspects the installed package directly.

    Args:
        app: Typer application (required for API consistency but not used directly)
        output_dir: Output directory path
        filename: Output filename
        cli_name: Name of the CLI command
    """
    # Log which app we're documenting
    module_name = app.__module__ if hasattr(app, "__module__") else "unknown_module"
    logger.info(f"Generating docs for app in module: {module_name}")

    # Create output directory if it doesn't exist
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    output_file = output_path / filename

    # Get the script path
    script_path = Path(sys.argv[0]).resolve()

    # If no CLI name is provided, use the script name
    if cli_name is None:
        cli_name = script_path.stem

    # Use typer utils docs command to first extract the info
    cmd = [
        "typer",
        str(script_path),
        "utils",
        "docs",
        "--name",
        cli_name,
        "--output",
        str(output_file) + ".temp",
    ]

    logger.info(f"Running typer-cli: {' '.join(cmd)}")
    subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        check=True,
    )

    # Read the generated file
    temp_file = Path(str(output_file) + ".temp")
    typer_content = temp_file.read_text()

    # Extract useful content from command_help_text
    help_sections = parse_command_help_text()

    # Format data sources as markdown list items with proper styling
    data_sources_md = "\n".join([f"{i + 1}. **{source}**" for i, source in enumerate(DATA_SOURCES)])

    # Create a new GitHub-friendly markdown file with APP_TITLE and formatted data sources
    markdown_content = f"""# {cli_name}: {FCP_NAME}

This CLI tool {RETRIEVES_DATA}:

{data_sources_md}

{APP_BEHAVIOR}.

## Usage

```bash
{cli_name} [OPTIONS]
```

"""

    # Extract options from typer_content
    options_match = re.search(r"\*\*Options\*\*:(.*?)(?:\Z|\*\*Commands\*\*:)", typer_content, re.DOTALL)
    if options_match:
        options_content = options_match.group(1).strip()
        options_list = re.findall(r"\* `([^`]+)`:(.*?)(?=\* |\Z)", options_content, re.DOTALL)

        # Instead of using a table, use standard markdown list format which avoids table linting issues
        options_formatted = []
        options_formatted.append("## Options\n")

        for option, description in options_list:
            # Clean up the description
            clean_description = description.strip()
            # Format with all on one line, no breaks
            clean_description = clean_description.replace("\n", " ").strip()

            # Format as bullet point with bold option
            options_formatted.append(f"- **`{option}`**: {clean_description}")

        options_formatted.append("- **`-h, --help`**: Show this message and exit.")

        options_text = "\n".join(options_formatted)
        markdown_content += f"{options_text}\n\n"

    # Add examples section using content directly from COMMAND_HELP_TEXT
    markdown_content += f"""## Examples

{help_sections["sample_commands"]}"""

    # Fix multiple consecutive blank lines (no more than one blank line in a row)
    markdown_content = re.sub(r"\n{3,}", "\n\n", markdown_content)

    # Write the final content to the output file
    output_file.write_text(markdown_content + "\n")  # Ensure trailing newline

    # Clean up the temporary file
    if temp_file.exists():
        temp_file.unlink()

    logger.info(f"Generated GitHub-friendly documentation at {output_file}")
    return output_file


def parse_command_help_text():
    """Parse the command help text to extract sections.

    Returns:
        dict: A dictionary with extracted and formatted sections
    """
    sections = {}

    # Process sample commands
    sample_commands_content = process_sample_commands(SAMPLE_COMMANDS)
    sections["sample_commands"] = sample_commands_content

    # Return an empty dict if no sections were found to prevent KeyError
    return sections or {"sample_commands": ""}


def process_sample_commands(sample_commands_text):
    """Process sample commands section to convert from Rich format to Markdown.

    Args:
        sample_commands_text: Raw sample commands text from dsm_help_content

    Returns:
        str: Markdown-formatted sample commands section
    """
    # Remove the cyan header
    sample_text = re.sub(r"\[bold cyan\]Sample Commands\[/bold cyan\]", "", sample_commands_text).strip()

    # Process section headers - remove rich formatting and convert to markdown headers
    sample_text = sample_text.replace("[green]", "### ").replace("[/green]", "")

    # Split into lines for processing
    lines = sample_text.split("\n")
    formatted_lines = []
    section_headers = {}  # Map to track which sections have content
    current_section = None
    command_groups = []
    current_group = {"description": None, "commands": [], "section": None}

    for line in lines:
        clean_line = line.strip()

        # Skip empty lines in initial processing
        if not clean_line:
            continue

        # Handle section headers
        if clean_line.startswith("### "):
            # Start new section
            if current_group["commands"]:
                command_groups.append(current_group)
                current_group = {
                    "description": None,
                    "commands": [],
                    "section": None,
                }

            current_section = clean_line
            # Don't append section header yet - we'll only do this if it has content
            section_headers[current_section] = False  # Initialize section as empty
            current_group["section"] = current_section

        # Handle command descriptions with '>'
        elif clean_line.startswith(">"):
            # If we have commands in the current group, save it and start a new one
            if current_group["commands"]:
                command_groups.append(current_group)
                current_group = {
                    "description": None,
                    "commands": [],
                    "section": current_section,
                }

            # Convert to heading and remove trailing colon if present
            desc = clean_line.replace(">", "").strip()
            if desc.endswith(":"):
                desc = desc[:-1]  # Remove trailing colon
            current_group["description"] = desc

        # Handle command lines - now looking for dsm-demo-cli instead of ./
        elif clean_line.startswith("dsm-demo-cli"):
            current_group["commands"].append(clean_line)
            if current_section:
                section_headers[current_section] = True  # Mark section as having content

    # Don't forget the last group
    if current_group["commands"]:
        command_groups.append(current_group)

    # Now format, but only include sections with content
    previous_section = None
    for group in command_groups:
        # Only add section header if this is the first group in a section with content
        current_section = group["section"]
        if current_section and current_section != previous_section and section_headers[current_section]:
            # Add a blank line before the section header (unless it's the first one)
            if formatted_lines:
                formatted_lines.append("")
            formatted_lines.append(current_section)
            formatted_lines.append("")
            previous_section = current_section

        if group["description"]:
            # Add heading without excessive blank lines
            formatted_lines.append(f"#### {group['description']}")
            formatted_lines.append("")

        if group["commands"]:
            # Add code block without excessive blank lines
            formatted_lines.append("```bash")
            for cmd in group["commands"]:
                formatted_lines.append(cmd)
            formatted_lines.append("```")
            formatted_lines.append("")

    # Ensure there are no consecutive blank lines
    clean_lines = []
    prev_line_empty = False
    for line in formatted_lines:
        is_empty = line.strip() == ""
        if not (is_empty and prev_line_empty):
            clean_lines.append(line)
        prev_line_empty = is_empty

    # Join the lines back together
    return "\n".join(clean_lines).strip()


def generate_markdown_docs(
    app: typer.Typer,
    output_dir: str = "docs/dsm_demo_cli",
    filename: str = "README.md",
    gen_lint_config: bool = False,
    cli_name: str | None = None,
):
    """Generate Markdown documentation from a Typer app.

    This function generates GitHub-friendly documentation using the official typer-cli tool.

    Args:
        app: The Typer app to generate documentation for
        output_dir: Directory to save the generated documentation
        filename: Name of the output file
        gen_lint_config: Whether to generate linting configuration files (ignored - linting config generation has been removed)
        cli_name: The name to use for the CLI app in the docs

    Returns:
        Path: Path to the generated documentation file
    """
    logger.info(f"Using typer-cli for GitHub-friendly documentation of {APP_TITLE}")
    result = generate_markdown_docs_with_typer_cli(app, output_dir, filename, cli_name=cli_name)

    # Note: Markdownlint configuration generation has been removed
    if gen_lint_config:
        logger.info("Markdownlint configuration generation has been disabled")

    # Print success message
    logger.info(f"Documentation for {APP_TITLE} generated successfully using typer-cli")

    # Preview the markdown in the console
    console = Console()
    output_file = Path(output_dir) / filename
    console.print(Markdown(output_file.read_text()))

    return result


def verify_and_install_typer_cli() -> bool:
    """Verify typer-cli is installed and install if needed.

    Returns:
        bool: True if typer-cli is available after verification/installation
    """
    import shutil
    import subprocess
    import sys

    from data_source_manager.utils.loguru_setup import logger

    typer_cli_available = shutil.which("typer") is not None

    if not typer_cli_available:
        logger.info("typer-cli not found. Installing typer-cli for optimal documentation...")
        try:
            subprocess.run(
                [sys.executable, "-m", "pip", "install", "typer-cli"],
                check=True,
                capture_output=True,
            )
            logger.info("typer-cli installed successfully")
            return True
        except Exception as e:
            logger.warning(f"Could not install typer-cli: {e}")
            return False

    return True
