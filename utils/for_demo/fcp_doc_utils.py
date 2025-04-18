#!/usr/bin/env python3
"""
Documentation utilities for FCP demo CLI applications.

This module contains functions to generate Markdown documentation from Typer help text.
"""

from pathlib import Path
import typer
import pendulum
import subprocess
import sys
import re
import json
from rich.console import Console
from rich.markdown import Markdown

from utils.logger_setup import logger


def generate_markdown_docs(
    app: typer.Typer,
    output_dir: str = "docs/fcp_demo",
    filename: str = "README.md",
    gen_lint_config: bool = False,
):
    """Generate Markdown documentation from a Typer app.

    This function captures the help output from a Typer app and converts it to
    a structured Markdown document.

    Args:
        app: The Typer app to generate documentation for
        output_dir: Directory to save the generated documentation
        filename: Name of the output file
        gen_lint_config: Whether to generate linting configuration files

    Returns:
        Path: Path to the generated documentation file
    """
    # Create output directory if it doesn't exist
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    # Get the current timestamp
    timestamp = pendulum.now().format("YYYY-MM-DD HH:mm:ss.SSS")

    # Get help text by running the script with --help flag
    script_path = Path(sys.argv[0]).resolve()
    try:
        # Run the script with --help to capture its output
        result = subprocess.run(
            [str(script_path), "--help"],
            capture_output=True,
            text=True,
            check=False,  # Don't raise exception on non-zero exit
        )
        help_text = result.stdout.strip()

        # If stdout is empty, try stderr (some programs output help to stderr)
        if not help_text and result.stderr:
            help_text = result.stderr.strip()

        # If still empty, fall back to a default message
        if not help_text:
            help_text = "Unable to capture help text. Please run the script with --help flag manually."
    except Exception as e:
        logger.error(f"Error capturing help text: {e}")
        help_text = f"Error capturing help text: {e}"

    # Check if the help text already includes the "Sample Commands" section
    if "Sample Commands:" in help_text:
        # Don't add additional examples - use the ones from the built-in help
        examples_section = """
## Documentation Generation Examples

For convenience, you can generate this documentation using:

```bash
# Generate this documentation
./examples/dsm_sync_simple/fcp_demo.py --gen-doc
./examples/dsm_sync_simple/fcp_demo.py -gd

# Generate documentation with linting configuration files
./examples/dsm_sync_simple/fcp_demo.py -gd -glc
```
"""
    else:
        # If there's no sample commands section in the help text, add our full examples
        examples_section = """
## Examples

Here are some examples of how to use this command:

### Basic Usage

```bash
./examples/dsm_sync_simple/fcp_demo.py
./examples/dsm_sync_simple/fcp_demo.py --symbol ETHUSDT --market spot
```

### Different Time Ranges

```bash
# Using days parameter (highest priority)
./examples/dsm_sync_simple/fcp_demo.py -s BTCUSDT -d 7

# Using explicit start and end times
./examples/dsm_sync_simple/fcp_demo.py -s BTCUSDT -st 2025-04-05T00:00:00 -et 2025-04-06T00:00:00
```

### Market Types

```bash
./examples/dsm_sync_simple/fcp_demo.py -s BTCUSDT -m um
./examples/dsm_sync_simple/fcp_demo.py -s BTCUSD_PERP -m cm
```

### Documentation Generation

```bash
# Generate this documentation
./examples/dsm_sync_simple/fcp_demo.py --gen-doc
./examples/dsm_sync_simple/fcp_demo.py -gd

# Generate documentation with linting configuration files
./examples/dsm_sync_simple/fcp_demo.py -gd -glc
```
"""

    # Format the markdown content
    markdown_content = f"""# FCP Demo CLI Documentation

Generated on: {timestamp}

## Overview

This documentation was automatically generated from the Typer CLI help text.

## Command Line Interface

```console
{help_text}
```
{examples_section}
"""

    # Fix markdown linting issues:
    # 1. Remove multiple consecutive blank lines (MD012)
    markdown_content = re.sub(r"\n{3,}", "\n\n", markdown_content)

    # Add an additional cleanup to ensure no trailing multiple blank lines
    markdown_content = markdown_content.rstrip() + "\n"

    # Write to the output file
    output_file = output_path / filename
    output_file.write_text(markdown_content)

    logger.info(f"Generated documentation at {output_file}")

    # Create linting configuration files if requested
    if gen_lint_config:
        # Create a markdownlint config file to ignore line length in code blocks
        markdownlint_config = {"MD013": {"code_blocks": False, "tables": False}}

        config_file = output_path / ".markdownlint.json"
        config_file.write_text(json.dumps(markdownlint_config, indent=2))
        logger.info(f"Created markdownlint config at {config_file}")

    # Print the markdown to the console if desired
    console = Console()
    console.print(Markdown(markdown_content))

    return output_file
