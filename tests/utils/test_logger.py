import logging
import pytest
from utils.logger_setup import get_logger


def test_logger_with_caplog(caplog):
    """Test that our custom logger works correctly with pytest's caplog fixture."""
    # Set the capture level to DEBUG to see all logs
    caplog.set_level(logging.DEBUG)

    # Create a logger
    test_logger = get_logger("test_logger")

    # Generate some log messages
    test_logger.debug("This is a debug message")
    test_logger.info("This is an info message")
    test_logger.warning("This is a warning message")
    test_logger.error("This is an error message")

    # Print out caplog records for debugging
    print("\nCAPLOG RECORDS:")
    for i, record in enumerate(caplog.records):
        print(f"Record {i}: {record}")
        print(f"  - name: {record.name}")
        print(f"  - levelname: {record.levelname}")
        print(f"  - message: {record.message}")

    # Verify that messages are captured
    assert len(caplog.records) > 0, "No log records were captured!"

    # Check specific messages (now with direct string matching)
    assert any(record.message == "This is an info message" for record in caplog.records)
    assert any(
        record.message == "This is a warning message" for record in caplog.records
    )
    assert any(
        record.message == "This is an error message" for record in caplog.records
    )


def test_multiple_log_levels_with_caplog(caplog):
    """Test capturing different log levels."""
    # Clear any previous records
    caplog.clear()

    # Set level to WARNING
    caplog.set_level(logging.WARNING)

    logger = get_logger("multiple_levels")
    logger.info("Info should not be captured")
    logger.warning("Warning should be captured")

    # Print out caplog records for debugging
    print("\nCAPLOG RECORDS (WARNING level):")
    for i, record in enumerate(caplog.records):
        print(f"Record {i}: {record.message} (level: {record.levelname})")

    # Only WARNING or higher should be captured
    assert not any(record.levelname == "INFO" for record in caplog.records)
    assert any(
        record.message == "Warning should be captured" for record in caplog.records
    )
