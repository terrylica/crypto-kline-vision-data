#!/usr/bin/env python3
"""A third test file for the autoflake pre-commit hook."""


def greet(name):
    """Greet a user."""
    return f"Hello, {name}!"


if __name__ == "__main__":
    print(greet("World"))
