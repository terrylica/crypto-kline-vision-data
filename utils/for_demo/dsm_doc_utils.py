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
import json
import shutil
from rich.console import Console
from rich.markdown import Markdown

from utils.logger_setup import logger


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

        # Get help text by running the script with --help flag
        try:
            help_result = subprocess.run(
                [str(script_path), "--help"],
                capture_output=True,
                text=True,
                check=False,
            )
            help_text = help_result.stdout.strip()
            if not help_text and help_result.stderr:
                help_text = help_result.stderr.strip()
        except Exception as e:
            logger.warning(f"Could not get help text: {e}")
            help_text = None

        # Create a new GitHub-friendly markdown file
        markdown_content = f"""# {cli_name}: Failover Control Protocol

This CLI tool demonstrates the Failover Control Protocol (FCP) mechanism,
which automatically retrieves Bitcoin data from multiple sources:

1. **Cache** (Local Arrow files)
2. **VISION API**
3. **REST API**

It displays real-time source information about where each data point comes from.

## Time Range Priority Hierarchy

### 1. `--days` or `-d` flag (HIGHEST PRIORITY)

- If provided, overrides any `--start-time` and `--end-time` values
- Calculates range as `[current_time - days, current_time]`
- Example: `--days 5` will fetch data from 5 days ago until now

### 2. `--start-time` and `--end-time` (SECOND PRIORITY)

- Used only when BOTH are provided AND `--days` is NOT provided
- Defines exact time range to fetch data from
- Example: `--start-time 2025-04-10 --end-time 2025-04-15`

### 3. Default Behavior (FALLBACK)

- If neither of the above conditions are met
- Uses default `days=3` to calculate range as `[current_time - 3 days, current_time]`

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

            table_header = "| Option | Description |\n|--------|-------------|\n"
            table_rows = []

            for option, description in options_list:
                # Clean up the description
                description = description.strip()

                # Escape pipe characters
                option_escaped = option.replace("|", "\\|")
                description_escaped = description.replace("|", "\\|")

                table_rows.append(f"| `{option_escaped}` | {description_escaped} |")

            options_table = table_header + "\n".join(table_rows)
            markdown_content += f"## Options\n\n{options_table}\n\n"

        # Add examples section
        if help_text:
            # Extract examples from help text if available
            examples_section = """## Examples

### Basic Usage

```bash
./examples/sync/dsm_demo.py
./examples/sync/dsm_demo.py --symbol ETHUSDT --market spot
```

### Time Range Options (By Priority)

```bash
# PRIORITY 1: Using --days flag (overrides any start/end times)
./examples/sync/dsm_demo.py -s BTCUSDT -d 7
  
# PRIORITY 2: Using start and end times (only if --days is NOT provided)
./examples/sync/dsm_demo.py -s BTCUSDT -st 2025-04-05T00:00:00 -et 2025-04-06T00:00:00
  
# FALLBACK: No time flags (uses default days=3)
./examples/sync/dsm_demo.py -s BTCUSDT
```

### Market Types

```bash
./examples/sync/dsm_demo.py -s BTCUSDT -m um
./examples/sync/dsm_demo.py -s BTCUSD_PERP -m cm
```

### Different Intervals

```bash
./examples/sync/dsm_demo.py -s BTCUSDT -i 5m
./examples/sync/dsm_demo.py -s BTCUSDT -i 1h
./examples/sync/dsm_demo.py -s SOLUSDT -m spot -i 1s -cc -l D -st 2025-04-14T15:31:01 -et 2025-04-14T15:32:01
```

### Data Source Options

```bash
./examples/sync/dsm_demo.py -s BTCUSDT -es REST
./examples/sync/dsm_demo.py -s BTCUSDT -nc
./examples/sync/dsm_demo.py -s BTCUSDT -cc
```

### Testing FCP Mechanism

```bash
./examples/sync/dsm_demo.py -s BTCUSDT -fcp
./examples/sync/dsm_demo.py -s BTCUSDT -fcp -pc
```

### Documentation Generation

```bash
# Generate documentation with typer-cli format (default)
./examples/sync/dsm_demo.py -gd

# Generate documentation with linting configuration files
./examples/sync/dsm_demo.py -gd -glc
```

### Combined Examples

```bash
./examples/sync/dsm_demo.py -s ETHUSDT -m um -i 15m -st 2025-04-01 -et 2025-04-10 -r 5 -l DEBUG
./examples/sync/dsm_demo.py -s ETHUSD_PERP -m cm -i 5m -d 10 -fcp -pc -l D -cc
```"""

            markdown_content += examples_section

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
        gen_lint_config: Whether to generate linting configuration files
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
        # Create linting configuration files if requested
        if gen_lint_config:
            output_path = Path(output_dir)
            # Define the markdownlint configuration with proper Python booleans
            # The json.dumps will convert Python's True/False to JSON true/false
            markdownlint_config = {
                "MD013": {"code_blocks": False, "tables": False},
                "MD014": False,  # Disable dollar signs used before commands
                "MD040": False,  # Disable requiring language in code blocks
                "MD047": False,  # Disable requiring single newline at end of file
            }
            config_file = output_path / ".markdownlint.json"
            config_file.write_text(json.dumps(markdownlint_config, indent=2))
            logger.info(f"Created markdownlint config at {config_file}")

        # Print success message
        logger.info("Documentation generated successfully using typer-cli")

        # Preview the markdown in the console
        console = Console()
        output_file = Path(output_dir) / filename
        console.print(Markdown(output_file.read_text()))

    return result
