#!/usr/bin/env python3
"""Example module demonstrating Python package documentation best practices.

This module illustrates the proper way to document Python code following
the principles outlined in python_package_principles.mdc. It demonstrates:

1. Module-level docstrings
2. Class documentation
3. Function documentation
4. Type hints
5. Examples in docstrings

Key components:
- ExampleConfig: Configuration class demonstrating attrs usage
- fetch_example_data: Main function showing parameter documentation
- validate_example_input: Utility function with validation logic
"""

import logging
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Union

import attr
import pendulum
from rich.console import Console

# Set up module-level logger
logger = logging.getLogger(__name__)


# Example enum showing good documentation
class ExampleType:
    """Types of examples available in the system.

    These constants define the different types of examples that can be
    processed by the example module functions.
    """

    TYPE_A = "type_a"
    """Type A examples: Simple demonstration examples."""

    TYPE_B = "type_b"
    """Type B examples: More complex examples with additional features."""

    TYPE_C = "type_c"
    """Type C examples: Advanced examples for specific use cases."""


@attr.s(auto_attribs=True, slots=True, frozen=True)
class ExampleConfig:
    """Configuration for example data retrieval.

    This class demonstrates the proper use of attrs for creating
    immutable configuration objects with validation.

    Attributes:
        example_type: The type of example to process
        max_items: Maximum number of items to retrieve
        include_metadata: Whether to include metadata in results
        cache_path: Path to the cache directory
        timeout_seconds: Timeout for network operations in seconds

    Example:
        >>> config = ExampleConfig(
        ...     example_type=ExampleType.TYPE_A,
        ...     max_items=100,
        ...     include_metadata=True
        ... )
        >>> print(config.example_type)
        type_a
    """

    example_type: str = attr.ib(
        default=ExampleType.TYPE_A,
        validator=attr.validators.in_(
            [
                ExampleType.TYPE_A,
                ExampleType.TYPE_B,
                ExampleType.TYPE_C,
            ]
        ),
    )
    max_items: int = attr.ib(
        default=50,
        validator=attr.validators.instance_of(int),
    )
    include_metadata: bool = attr.ib(
        default=False,
        validator=attr.validators.instance_of(bool),
    )
    cache_path: str = attr.ib(
        default="./cache",
        validator=attr.validators.instance_of(str),
    )
    timeout_seconds: int = attr.ib(
        default=30,
        validator=[
            attr.validators.instance_of(int),
            lambda _, __, value: value > 0,
        ],
    )

    @classmethod
    def from_dict(cls, config_dict: Dict[str, Union[str, int, bool]]) -> "ExampleConfig":
        """Create a configuration object from a dictionary.

        Args:
            config_dict: Dictionary containing configuration values

        Returns:
            Configured ExampleConfig instance

        Raises:
            ValueError: If required fields are missing or invalid

        Example:
            >>> config = ExampleConfig.from_dict({
            ...     "example_type": "type_b",
            ...     "max_items": 200
            ... })
            >>> print(config.example_type)
            type_b
            >>> print(config.include_metadata)  # Uses default
            False
        """
        return cls(**config_dict)


def validate_example_input(
    identifier: str,
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
    days: Optional[int] = None,
) -> Tuple[datetime, datetime]:
    """Validate and normalize time range parameters.

    This function demonstrates proper parameter validation and normalization,
    with clear documentation of parameters, return values, and exceptions.

    Args:
        identifier: Unique identifier for the example
        start_time: Start time for data range (UTC)
        end_time: End time for data range (UTC)
        days: Number of days to include (alternative to start_time)

    Returns:
        Tuple containing normalized (start_time, end_time)

    Raises:
        ValueError: If time parameters are invalid or incompatible

    Example:
        >>> from datetime import datetime
        >>> start, end = validate_example_input(
        ...     "example1",
        ...     end_time=datetime(2023, 1, 10),
        ...     days=5
        ... )
        >>> print(start.date())
        2023-01-05
        >>> print(end.date())
        2023-01-10
    """
    if not identifier:
        raise ValueError("Identifier cannot be empty")

    # Convert datetime objects to pendulum for better timezone handling
    if end_time is None:
        end_time = pendulum.now("UTC")
    else:
        end_time = pendulum.instance(end_time)

    # Calculate start_time based on days if provided
    if start_time is None and days is not None:
        if days <= 0:
            raise ValueError("Days must be positive")
        start_time = end_time.subtract(days=days)
    elif start_time is not None:
        start_time = pendulum.instance(start_time)
    else:
        raise ValueError("Either start_time or days must be provided")

    # Ensure start_time is before end_time
    if start_time >= end_time:
        raise ValueError("start_time must be before end_time")

    return start_time, end_time


def fetch_example_data(
    identifier: str,
    example_type: str = ExampleType.TYPE_A,
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
    days: Optional[int] = None,
    use_cache: bool = True,
    config: Optional[ExampleConfig] = None,
) -> Tuple[Optional[List[Dict]], float, int]:
    """Fetch example data for the specified parameters.

    This function demonstrates a well-documented main interface function
    with proper parameter documentation, return value description, and
    usage examples.

    Args:
        identifier: Unique identifier for the example
        example_type: Type of example to fetch (from ExampleType)
        start_time: Start datetime (UTC)
        end_time: End datetime (UTC)
        days: Number of days to fetch (backward from end_time)
        use_cache: Whether to use the local cache
        config: Optional configuration object (overrides other parameters)

    Returns:
        Tuple containing:
        - List of dictionaries with example data (or None if error)
        - Elapsed time in seconds
        - Number of records retrieved

    Example:
        >>> data, elapsed_time, count = fetch_example_data(
        ...     identifier="example1",
        ...     example_type=ExampleType.TYPE_A,
        ...     end_time=datetime(2023, 1, 10),
        ...     days=5,
        ...     use_cache=True
        ... )
        >>> print(f"Retrieved {count} records in {elapsed_time:.2f} seconds")
        Retrieved 100 records in 1.25 seconds

    Note:
        This is a simplified example. In a real implementation, this function
        would include error handling, logging, and actual data retrieval logic.
    """
    start_time_copy = start_time
    end_time_copy = end_time

    # Use configuration object if provided
    if config is not None:
        example_type = config.example_type
    else:
        # Create default config
        config = ExampleConfig(
            example_type=example_type,
            include_metadata=True,
        )

    # Validate and normalize time parameters
    start_time, end_time = validate_example_input(
        identifier=identifier,
        start_time=start_time_copy,
        end_time=end_time_copy,
        days=days,
    )

    # Log operation start
    logger.info(
        "Fetching %s data for %s from %s to %s",
        example_type,
        identifier,
        start_time.isoformat(),
        end_time.isoformat(),
    )

    # Simulate data retrieval (in a real implementation, this would be actual logic)
    start = pendulum.now()

    # Simulate different data sizes based on example type
    if example_type == ExampleType.TYPE_A:
        record_count = 100
    elif example_type == ExampleType.TYPE_B:
        record_count = 250
    else:
        record_count = 500

    # Create sample data
    data = [
        {
            "id": f"{identifier}_{i}",
            "timestamp": start_time.add(hours=i).isoformat(),
            "value": i * 1.5,
            "type": example_type,
        }
        for i in range(min(record_count, config.max_items))
    ]

    # Calculate elapsed time
    elapsed_time = (pendulum.now() - start).total_seconds()

    logger.info(
        "Retrieved %d records in %.2f seconds",
        len(data),
        elapsed_time,
    )

    return data, elapsed_time, len(data)


if __name__ == "__main__":
    # Example of running the module directly
    console = Console()
    console.print("[bold green]Example Module Demonstration[/bold green]")

    # Set up logging
    logging.basicConfig(level=logging.INFO)

    # Example usage
    data, elapsed_time, count = fetch_example_data(
        identifier="demo_example",
        example_type=ExampleType.TYPE_B,
        end_time=pendulum.now("UTC"),
        days=7,
        use_cache=True,
    )

    console.print(f"[bold]Retrieved {count} records in {elapsed_time:.2f} seconds[/bold]")
    console.print(f"First record: {data[0]}")
    console.print(f"Last record: {data[-1]}")
