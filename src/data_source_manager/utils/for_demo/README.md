# DSM Demo Help Content Management

This directory contains utilities for DSM Demo applications, with a focus on maintaining the DRY (Don't Repeat Yourself) principle for help content.

## Centralized Help Content Approach

We've implemented a centralized approach to help content management to avoid duplication and make maintenance easier:

1. `dsm_help_content.py` serves as the single source of truth for all help text content
2. Other modules import the specific content they need from this central module

## CLI Option Definitions

All CLI options are centralized in a comprehensive structure within `dsm_help_content.py`:

```python
CLI_OPTIONS = {
    "symbol": {
        "long_flag": "--symbol",
        "short_flag": "-s",
        "help": "Symbol to fetch data for",
        "default": "BTCUSDT",
    },
    # ...more options
}
```

This approach centralizes:

- Long option names (`--symbol`)
- Short option names/shorthands (`-s`)
- Help text descriptions
- Default values
- Logical grouping of related options

## Structure Overview

- `dsm_help_content.py` - Contains all help text constants and CLI option definitions
- `dsm_app_options.py` - Uses the centralized CLI option definitions for Typer app options
- `dsm_cli_utils.py` - Uses the centralized help content for CLI utility functions
- `examples/sync/dsm_demo_cli.py` - Uses the centralized help content via `__doc__ = MAIN_DOCSTRING`

## Benefits

- **Single Source of Truth**: All help content and CLI option definitions are in one place
- **Consistency**: Changes to flags, help text, or defaults are applied uniformly
- **Maintainability**: Easier to update, extend, or localize help content
- **Reduced Duplication**: No repetition of the same information in multiple files
- **Flexibility**: Easy to add new options or modify existing ones without touching multiple files
- **Self-Documentation**: The structure itself documents the available CLI options

## Usage Pattern

To use the centralized CLI option definitions:

```python
# Import the CLI options
from utils.for_demo.dsm_help_content import CLI_OPTIONS

# Use them in your Typer options
typer.Option(
    CLI_OPTIONS["symbol"]["default"],
    CLI_OPTIONS["symbol"]["long_flag"],
    CLI_OPTIONS["symbol"]["short_flag"],
    help=CLI_OPTIONS["symbol"]["help"],
)
```

## Rich Text Formatting Support

The help content supports Rich library's text formatting for enhanced console output. Examples:

- `[bold red]Important warning[/bold red]`
- `[green]Success message[/green]`
- `[cyan]Information text[/cyan]`
