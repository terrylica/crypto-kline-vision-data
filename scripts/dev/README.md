# Development Scripts README

This directory contains various scripts used for development, testing, and maintenance tasks within the project.

## Scripts

### [`clear_cache.py`](clear_cache.py)

This Python script is used to clear specified cache and log directories. It supports clearing directories on local and various remote filesystems (like S3, GCS) using `fsspec`. It can recursively delete files and remove empty subdirectories while preserving the base directories. It includes options for specifying directories, filesystem protocol, storage options, running in test mode, skipping confirmation, showing version, and creating directories if missing.

### [`create_readme_for_md_dirs.py`](create_readme_for_md_dirs.py)

This Python script recursively scans directories and creates empty `README.md` files in any directory containing two or more markdown (`.md`) files that doesn't already have a `README.md` file. Its purpose is to help organize documentation by ensuring directories with significant markdown content have a starting point for documentation.

### [`dead_code_report.py`](dead_code_report.py)

This Python script generates a comprehensive dead code report using the `vulture` tool. It analyzes code to detect unused functions, variables, imports, and unreachable code. The report provides a summary of unused code by type, lists files with the most dead code, and details the dead code found with confidence levels.

### [`install_code2prompt.sh`](install_code2prompt.sh)

This shell script automates the installation of the `code2prompt` command-line tool (written in Rust). It checks for and installs Rust/Cargo if needed, then uses `cargo install` to install `code2prompt`. It also creates a small example directory with sample Python files and provides basic usage examples after installation.

### [`linter_report.py`](linter_report.py)

This Python script generates a comprehensive report based on the output of the Ruff linter. It provides an error code summary, lists top files with issues, shows top file/error combinations, and includes explanations for the error codes. It helps in analyzing and prioritizing linter issues across the codebase.

### [`refactor_move.py`](refactor_move.py)

This Python script facilitates moving Python files by using `git mv` and automatically refactoring import statements across the codebase using the `rope` library. It also includes a verification step using `ruff` to ensure that the refactoring did not introduce new import-related errors.

### [`remove_empty_dirs.sh`](remove_empty_dirs.sh)

This shell script finds and removes empty directories recursively from the current location downwards. It uses the `find` command with the `-empty` and `-delete` options and includes exclusions for `.git` and `.venv` directories.

### [`run_tests_parallel.sh`](run_tests_parallel.sh)

This shell script is designed to run `pytest` tests efficiently, primarily in parallel using `pytest-xdist`. It handles tests marked for serial execution separately, offers an interactive test selection mode, allows passing additional arguments to `pytest`, configures asyncio for stability, provides a detailed error and warning summary, and supports performance profiling.
