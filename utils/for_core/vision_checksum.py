#!/usr/bin/env python3
"""
Utility module for Binance Vision API checksum verification.

This module provides functions to verify the integrity of files downloaded
from the Binance Vision API using SHA-256 checksums.
"""

import hashlib
import re
from pathlib import Path
from typing import Tuple, Optional, Dict

from utils.logger_setup import logger
from rich import print as rprint


def verify_file_checksum(
    file_path: Path, checksum_path: Path
) -> Tuple[bool, Optional[str]]:
    """
    Verify the integrity of a file downloaded from Binance Vision API by comparing
    its SHA-256 checksum with the expected value from the checksum file.

    Args:
        file_path: Path to the data file to verify
        checksum_path: Path to the corresponding checksum file

    Returns:
        A tuple of (success, error_message)
        - success: True if checksum verification passed, False otherwise
        - error_message: Error message if verification failed, None if passed
    """
    try:
        # Ensure both files exist
        if not file_path.exists():
            error_msg = f"Data file not found: {file_path}"
            logger.error(error_msg)
            return False, error_msg

        if not checksum_path.exists():
            error_msg = f"Checksum file not found: {checksum_path}"
            logger.error(error_msg)
            return False, error_msg

        # Read expected checksum from file
        expected_checksum = extract_checksum_from_file(checksum_path)

        # If we couldn't extract a valid checksum, this is an error
        if expected_checksum is None:
            error_msg = (
                f"Could not extract checksum from {checksum_path.name} - "
                f"checksum verification failed. Data integrity cannot be verified."
            )
            logger.warning(error_msg)
            return False, error_msg

        # Calculate checksum using hashlib
        actual_checksum = calculate_sha256_direct(file_path)
        logger.debug(f"Calculated checksum: {actual_checksum}")

        # Compare checksums (case-insensitive)
        if actual_checksum and actual_checksum.lower() == expected_checksum.lower():
            logger.info(f"Checksum verification passed for {file_path.name}")
            return True, None
        else:
            error_msg = (
                f"Checksum verification failed for {file_path.name}. "
                f"Expected: {expected_checksum}, Actual: {actual_checksum}. "
                f"Data integrity compromised - possible corruption or tampering."
            )
            logger.critical(error_msg)
            return False, error_msg

    except Exception as e:
        error_msg = f"Error verifying checksum for {file_path.name}: {e}"
        logger.error(error_msg)
        return False, error_msg


def extract_checksum_from_file(checksum_path: Path) -> Optional[str]:
    """
    Extract the SHA-256 checksum from a Binance Vision API checksum file.

    Binance checksum files are typically in the format:
    "<sha256_hash>  <filename>"

    For example:
    "d0a6fd261d2bf9c6c61b113714724e682760b025c449b19c90a1c4f00ede3e9c  BTCUSDT-1m-2025-04-13.zip"

    This function is designed to be robust against various formats that might be encountered.

    Args:
        checksum_path: Path to the checksum file

    Returns:
        Extracted SHA-256 checksum or None if not found/invalid
    """
    if not checksum_path.exists():
        logger.error(f"Checksum file not found: {checksum_path}")
        return None

    try:
        # First, try reading file as text
        try:
            text_content = checksum_path.read_text(errors="replace").strip()
            logger.debug(f"Checksum content (text): '{text_content}'")
        except Exception as text_error:
            logger.debug(f"Reading as text failed: {text_error}, trying binary mode")
            try:
                # If text reading fails, try binary
                with open(checksum_path, "rb") as f:
                    binary_content = f.read()
                    text_content = binary_content.decode(
                        "utf-8", errors="replace"
                    ).strip()
                    logger.debug(f"Checksum content (binary): '{text_content}'")
            except Exception as bin_error:
                logger.error(
                    f"Failed to read checksum file in binary mode: {bin_error}"
                )
                return None

        # If the content is empty, return None
        if not text_content:
            logger.warning(f"Checksum file is empty: {checksum_path}")
            return None

        # Try multiple extraction methods

        # Method 1: Common case - file contains a single line with hash and filename
        # Format: <sha256_hash>  <filename>
        if " " in text_content and len(text_content.split(" ", 1)[0]) == 64:
            checksum = text_content.split(" ", 1)[0].strip()
            logger.debug(f"Extracted checksum from standard format: {checksum}")
            if is_valid_sha256(checksum):
                return checksum

        # Method 2: Look for a 64-character hex string anywhere in the content
        match = re.search(r"([a-fA-F0-9]{64})", text_content)
        if match:
            checksum = match.group(1)
            logger.debug(f"Extracted checksum using regex: {checksum}")
            if is_valid_sha256(checksum):
                return checksum

        # Method 3: Try finding all words and look for a 64-char hex string
        words = re.findall(r"\b\w+\b", text_content)
        for word in words:
            if len(word) == 64 and is_valid_sha256(word):
                logger.debug(f"Extracted checksum from word list: {word}")
                return word

        # If we reached here, we couldn't find a valid checksum
        logger.warning(f"Could not extract SHA-256 hash from file: {checksum_path}")
        logger.debug(f"File content: '{text_content}'")
        return None

    except Exception as e:
        logger.error(f"Error extracting checksum from {checksum_path}: {e}")
        import traceback

        logger.debug(f"Extraction error traceback: {traceback.format_exc()}")
        return None


def calculate_sha256_direct(file_path: Path) -> str:
    """
    Calculate SHA-256 checksum directly using hashlib.

    Args:
        file_path: Path to the file

    Returns:
        Hexadecimal string of the SHA-256 checksum
    """
    sha256 = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(16384), b""):
            sha256.update(chunk)
    return sha256.hexdigest()


def is_valid_sha256(text: str) -> bool:
    """
    Check if the given text is a valid SHA-256 hash.

    Args:
        text: String to check

    Returns:
        True if the string is a valid SHA-256 hash, False otherwise
    """
    return bool(re.match(r"^[a-fA-F0-9]{64}$", text))


def get_checksum_url(data_url: str) -> str:
    """
    Generate the correct URL for a checksum file based on the data file URL.

    The Binance Vision API uses .zip.CHECKSUM extension for checksum files.

    Args:
        data_url: URL of the data file

    Returns:
        URL of the corresponding checksum file
    """
    # Ensure data URL ends with .zip
    if not data_url.endswith(".zip"):
        raise ValueError(f"Data URL must end with .zip: {data_url}")

    # Add .CHECKSUM to the .zip extension
    return f"{data_url}.CHECKSUM"


def calculate_checksums_multiple_methods(file_path: Path) -> Dict[str, str]:
    """
    Calculate file checksum using SHA-256 method.

    This function was simplified to only use the direct SHA-256 calculation
    method for consistency and reliability.

    Args:
        file_path: Path to the file to calculate checksum for

    Returns:
        Dictionary with method name as key and checksum value
    """
    if not isinstance(file_path, Path):
        file_path = Path(file_path)

    if not file_path.exists():
        logger.error(f"File not found: {file_path}")
        return {"sha256": ""}

    try:
        # Calculate using direct hashlib method
        checksum = calculate_sha256_direct(file_path)
        return {"sha256": checksum}
    except Exception as e:
        logger.error(f"Error calculating checksum for {file_path}: {e}")
        return {"sha256": ""}


def verify_checksum_cli(file_path: str, checksum_path: str = None) -> None:
    """
    CLI-friendly function to verify a file's checksum against a checksum file.

    If checksum_path is not provided, it will attempt to find a file with the same name
    plus .CHECKSUM extension in the same directory.

    Args:
        file_path: Path to the data file
        checksum_path: Path to the checksum file (optional)
    """
    # Convert to Path objects
    file_path_obj = Path(file_path)

    # If checksum_path is not provided, look for it in the same directory
    if checksum_path is None:
        checksum_path_obj = Path(f"{file_path}.CHECKSUM")
    else:
        checksum_path_obj = Path(checksum_path)

    # Verify the checksum
    success, error = verify_file_checksum(file_path_obj, checksum_path_obj)

    # Print results
    if success:
        rprint(
            f"[green]✓ Checksum verification successful for {file_path_obj.name}[/green]"
        )
    else:
        rprint(f"[red]✗ Checksum verification failed: {error}[/red]")
