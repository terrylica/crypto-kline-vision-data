#!/usr/bin/env python3
"""
Documentation utilities for DSM Demo CLI applications.

This module contains functions to generate Markdown documentation from Typer help text
using the official typer-cli tool for optimal GitHub-friendly output.
"""

from pathlib import Path
import typer
import subprocess
import sys
import re
import shutil
from rich.console import Console
from rich.markdown import Markdown
import textwrap

from utils.logger_setup import logger
from utils.for_demo.dsm_help_content import (
    COMMAND_HELP_TEXT,
)


def is_typer_cli_available():
    """Check if typer-cli is installed and available."""
    return shutil.which("typer") is not None


def generate_markdown_docs_with_typer_cli(
    app: typer.Typer,
    output_dir: str = "docs/dsm_demo",
    filename: str = "README.md",
    cli_name: str = None,
):
    """Generate Markdown documentation using the typer-cli tool.

    This uses the official Typer CLI tool for better GitHub-friendly docs.

    Args:
        app: The Typer app to generate documentation for
        output_dir: Directory to save the generated documentation
        filename: Name of the output file
        cli_name: The name to use for the CLI app in the docs

    Returns:
        Path: Path to the generated documentation file or None if failed
    """
    # Create output directory if it doesn't exist
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    output_file = output_path / filename

    # Get the script path
    script_path = Path(sys.argv[0]).resolve()

    # If no CLI name is provided, use the script name
    if cli_name is None:
        cli_name = script_path.stem

    try:
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
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=False,
        )

        if result.returncode != 0:
            logger.error(f"typer-cli failed: {result.stderr}")
            return None

        # Read the generated file
        temp_file = Path(str(output_file) + ".temp")
        if not temp_file.exists():
            logger.error(f"Expected temporary file not found: {temp_file}")
            return None

        typer_content = temp_file.read_text()

        # Extract useful content from command_help_text
        help_sections = parse_command_help_text(COMMAND_HELP_TEXT)

        # No line length constraints - identity function
        def wrap_text(text, width=None):
            # Simply return the text as is without any wrapping
            return text

        # Create a new GitHub-friendly markdown file
        markdown_content = f"""# {cli_name}: Failover Control Protocol

This CLI tool demonstrates the Failover Control Protocol (FCP) mechanism,
which automatically retrieves Bitcoin data from multiple sources:

1. **Cache** (Local Arrow files)
2. **VISION API**
3. **REST API**

It displays real-time source information about where each data point comes from.

## Time Range Options

### Priority and Calculation Details

{help_sections["time_range_options"]}

## Usage

```bash
{cli_name} [OPTIONS]
```

"""

        # Extract options from typer_content
        options_match = re.search(
            r"\*\*Options\*\*:(.*?)(?:\Z|\*\*Commands\*\*:)", typer_content, re.DOTALL
        )
        if options_match:
            options_content = options_match.group(1).strip()
            options_list = re.findall(
                r"\* `([^`]+)`:(.*?)(?=\* |\Z)", options_content, re.DOTALL
            )

            # Instead of using a table, use standard markdown list format which avoids table linting issues
            options_formatted = []
            options_formatted.append("## Options\n")

            for option, description in options_list:
                # Clean up the description
                description = description.strip()
                # Format with all on one line, no breaks
                description = description.replace("\n", " ").strip()

                # Format as bullet point with bold option
                options_formatted.append(f"- **`{option}`**: {description}")

            options_formatted.append("- **`-h, --help`**: Show this message and exit.")

            options_text = "\n".join(options_formatted)
            markdown_content += f"{options_text}\n\n"

        # Add examples section using content directly from COMMAND_HELP_TEXT
        markdown_content += f"""## Examples

{help_sections["sample_commands"]}"""

        # No more line wrapping - simply use the content as is
        # markdown_content = wrap_text(markdown_content)  # Removed

        # Fix multiple consecutive blank lines (no more than one blank line in a row)
        markdown_content = re.sub(r"\n{3,}", "\n\n", markdown_content)

        # Write the final content to the output file
        output_file.write_text(markdown_content + "\n")  # Ensure trailing newline

        # Clean up the temporary file
        if temp_file.exists():
            temp_file.unlink()

        logger.info(f"Generated GitHub-friendly documentation at {output_file}")
        return output_file
    except Exception as e:
        logger.error(f"Error generating GitHub-friendly documentation: {e}")
        return None


def parse_command_help_text(command_help_text):
    """Parse the command help text to extract sections.

    Args:
        command_help_text: The help text from dsm_help_content.py

    Returns:
        dict: A dictionary with extracted and formatted sections
    """
    sections = {}

    # Extract time range options section
    time_range_match = re.search(
        r"\[bold cyan\]Time Range Options\[/bold cyan\](.*?)\[bold cyan\]Sample Commands",
        command_help_text,
        re.DOTALL,
    )
    if time_range_match:
        time_range_text = time_range_match.group(1).strip()

        # Process the content directly with simpler replacements
        # Don't add the "Priority and Calculation Details" heading here as it's already added in the template
        formatted_content = ""

        # Replace section headers (removing colons)
        formatted_content += (
            time_range_text.replace(
                "[green]1. End Time with Days[/green]", "\n#### 1. End Time with Days\n"
            )
            .replace(
                "[green]2. Start Time with Days[/green]",
                "\n#### 2. Start Time with Days\n",
            )
            .replace(
                "[green]3. Exact Time Range[/green]", "\n#### 3. Exact Time Range\n"
            )
            .replace("[green]4. Days Only[/green]", "\n#### 4. Days Only\n")
            .replace(
                "[green]5. Default Behavior (No Options)[/green]",
                "\n#### 5. Default Behavior (No Options)\n",
            )
        )

        # Process list items for proper markdown formatting
        formatted_content = formatted_content.replace("  - Use ", "- **Usage:** Use ")
        formatted_content = formatted_content.replace(
            "  - Calculates ", "- **Calculation:** Calculates "
        )
        formatted_content = formatted_content.replace(
            "  - Example: ", "- **Example:** "
        )
        formatted_content = formatted_content.replace(
            "  - Provide ", "- **Usage:** Provide "
        )
        formatted_content = formatted_content.replace("  - If ", "- **Condition:** If ")
        formatted_content = formatted_content.replace(
            "  - Equivalent ", "- **Equivalent:** "
        )

        # Add proper code backticks
        formatted_content = formatted_content.replace("as [", "as `[").replace(
            "]", "]`"
        )

        sections["time_range_options"] = formatted_content

    # Extract sample commands section using regex with manual processing
    sample_commands_match = re.search(
        r"\[bold cyan\]Sample Commands\[/bold cyan\](.*?)$",
        command_help_text,
        re.DOTALL,
    )
    if sample_commands_match:
        sample_text = sample_commands_match.group(1).strip()

        # First, process section headers - remove trailing colons
        sample_text = sample_text.replace("[green]", "### ").replace("[/green]", "")

        # Split into lines for processing
        lines = sample_text.split("\n")
        formatted_lines = []
        section_headers = {}  # Map to track which sections have content
        current_section = None
        command_groups = []
        current_group = {"description": None, "commands": [], "section": None}

        for line in lines:
            line = line.strip()

            # Skip empty lines in initial processing
            if not line:
                continue

            # Handle section headers
            if line.startswith("### "):
                # Start new section
                if current_group["commands"]:
                    command_groups.append(current_group)
                    current_group = {
                        "description": None,
                        "commands": [],
                        "section": None,
                    }

                current_section = line
                # Don't append section header yet - we'll only do this if it has content
                section_headers[current_section] = False  # Initialize section as empty
                current_group["section"] = current_section

            # Handle command descriptions with '>'
            elif line.startswith(">"):
                # If we have commands in the current group, save it and start a new one
                if current_group["commands"]:
                    command_groups.append(current_group)
                    current_group = {
                        "description": None,
                        "commands": [],
                        "section": current_section,
                    }

                # Convert to heading and remove trailing colon if present
                desc = line.replace(">", "").strip()
                if desc.endswith(":"):
                    desc = desc[:-1]  # Remove trailing colon
                current_group["description"] = desc

            # Handle command lines
            elif line.startswith("./"):
                current_group["commands"].append(line)
                if current_section:
                    section_headers[current_section] = (
                        True  # Mark section as having content
                    )

        # Don't forget the last group
        if current_group["commands"]:
            command_groups.append(current_group)

        # Now format, but only include sections with content
        previous_section = None
        for group in command_groups:
            # Only add section header if this is the first group in a section with content
            current_section = group["section"]
            if (
                current_section
                and current_section != previous_section
                and section_headers[current_section]
            ):
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
        sections["sample_commands"] = "\n".join(clean_lines).strip()

    # Return an empty dict if no sections were found to prevent KeyError
    return sections or {"time_range_options": "", "sample_commands": ""}


def generate_markdown_docs(
    app: typer.Typer,
    output_dir: str = "docs/dsm_demo",
    filename: str = "README.md",
    gen_lint_config: bool = False,
    cli_name: str = None,
):
    """Generate Markdown documentation from a Typer app.

    This function generates GitHub-friendly documentation using the official typer-cli tool.
    If typer-cli is not available, it will log an error message and raise an exception.

    Args:
        app: The Typer app to generate documentation for
        output_dir: Directory to save the generated documentation
        filename: Name of the output file
        gen_lint_config: Whether to generate linting configuration files (ignored - linting config generation has been removed)
        cli_name: The name to use for the CLI app in the docs

    Returns:
        Path: Path to the generated documentation file

    Raises:
        RuntimeError: When typer-cli is not installed
    """
    # Check if typer-cli is available
    if not is_typer_cli_available():
        logger.error("typer-cli not found. Documentation cannot be generated.")
        logger.error("Please install typer-cli manually with: pip install typer-cli")
        raise RuntimeError(
            "typer-cli is required for documentation generation but was not found"
        )

    # Now we're sure typer-cli is available
    logger.info("Using typer-cli for GitHub-friendly documentation")
    result = generate_markdown_docs_with_typer_cli(
        app, output_dir, filename, cli_name=cli_name
    )

    if result:
        # Note: Markdownlint configuration generation has been removed
        if gen_lint_config:
            logger.info("Markdownlint configuration generation has been disabled")

        # Print success message
        logger.info("Documentation generated successfully using typer-cli")

        # Preview the markdown in the console
        console = Console()
        output_file = Path(output_dir) / filename
        console.print(Markdown(output_file.read_text()))

    return result
