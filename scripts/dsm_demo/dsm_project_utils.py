#!/usr/bin/env python3
"""
Project utilities for the Failover Control Protocol (FCP) mechanism.
"""

import os
from rich import print


def verify_project_root():
    """
    Verify that we're running from the project root directory.

    Returns:
        bool: True if already in project root or successfully changed to it, False otherwise
    """
    if os.path.isdir("core") and os.path.isdir("utils") and os.path.isdir("examples"):
        # Already in project root
        print("Running from project root directory")
        return True

    # Try to navigate to project root if we're in the example directory
    if os.path.isdir("../../core") and os.path.isdir("../../utils"):
        os.chdir("../..")
        print(f"Changed to project root directory: {os.getcwd()}")
        return True

    print("[bold red]Error: Unable to locate project root directory[/bold red]")
    print(
        "Please run this script from either the project root or the examples/sync directory"
    )
    return False
