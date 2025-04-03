#!/usr/bin/env python3
"""
Detailed Failure Summary Generator

This script generates a comprehensive failure summary for pytest tests
with full file paths and line numbers for better debugging.
"""

import sys
import subprocess
import re
from collections import defaultdict


def generate_detailed_failure_summary(test_path):
    """Generate a detailed summary of test failures with full paths and line numbers."""
    # Run pytest with minimal formatting to get clean output for parsing
    cmd = ["pytest", test_path, "-v", "--no-header", "--no-summary", "-p", "no:pretty"]
    result = subprocess.run(cmd, capture_output=True, text=True)

    # Parse the results
    failures = []
    lines = result.stdout.split("\n")

    # First pass: identify all failed tests and their file paths
    failed_tests = {}
    for i, line in enumerate(lines):
        if "FAILED" in line and "::test_" in line:
            match = re.search(r"(.*\.py)::(\w+)", line)
            if match:
                file_path = match.group(1)
                function_name = match.group(2)
                failed_tests[function_name] = {
                    "file_path": file_path,
                    "function_name": function_name,
                    "line_idx": i,  # Save the line index for later processing
                }

    # Second pass: find detailed error information for each failed test
    for function_name, test_info in failed_tests.items():
        file_path = test_info["file_path"]
        start_idx = test_info["line_idx"]

        # Look for function definition (to get line number)
        func_line = None
        error_line = None
        error_type = None
        error_message = None

        # Search in a window after the test failure line
        for i in range(start_idx, min(start_idx + 30, len(lines))):
            line = lines[i]

            # Look for function definition line
            if f"def {function_name}" in line:
                func_line_match = re.search(r":(\d+):", line)
                if func_line_match:
                    func_line = func_line_match.group(1)

            # Look for the error line
            if file_path in line and ":" in line:
                error_line_match = re.search(
                    r"{}:(\d+)".format(re.escape(file_path)), line
                )
                if error_line_match:
                    error_line = error_line_match.group(1)

            # Look for error type and message (these are usually on E lines)
            if line.strip().startswith("E "):
                if not error_type:
                    type_match = re.search(
                        r"E\s+(AssertionError|ValueError|RuntimeError|TypeError|Exception)(?::\s*(.*))?",
                        line,
                    )
                    if type_match:
                        error_type = type_match.group(1)
                        if type_match.group(2):
                            error_message = type_match.group(2)

            # Also look in traceback lines
            if not error_type and ("Error:" in line or "Exception:" in line):
                type_match = re.search(
                    r"(AssertionError|ValueError|RuntimeError|TypeError|Exception)(?::\s*(.*))?",
                    line,
                )
                if type_match:
                    error_type = type_match.group(1)
                    if type_match.group(2):
                        error_message = type_match.group(2)

        # Add to failures if we have at least essential information
        if file_path and function_name and (error_line or func_line) and error_type:
            failures.append(
                {
                    "file_path": file_path,
                    "function_name": function_name,
                    "function_line": func_line or "N/A",
                    "error_line": error_line or "N/A",
                    "error_type": error_type,
                    "error_message": error_message or "",
                }
            )

    # If we didn't get good results, try a more direct approach
    if not failures and failed_tests:
        # Run pytest again with more detailed output
        cmd = ["pytest", test_path, "-v", "--tb=long", "--no-header", "-p", "no:pretty"]
        result = subprocess.run(cmd, capture_output=True, text=True)
        lines = result.stdout.split("\n")

        for function_name, test_info in failed_tests.items():
            file_path = test_info["file_path"]

            # Use more direct pattern matching
            function_pattern = re.compile(rf"def\s+{function_name}\(")
            error_line_pattern = re.compile(rf"{re.escape(file_path)}:(\d+)")
            error_type_pattern = re.compile(
                r"(AssertionError|ValueError|RuntimeError|TypeError|Exception)"
            )

            func_line = None
            error_line = None
            error_type = None
            error_message = None

            for line in lines:
                # Find function definition line
                func_match = function_pattern.search(line)
                if func_match and ": " in line:
                    line_match = re.search(r":(\d+):", line)
                    if line_match:
                        func_line = line_match.group(1)

                # Find error line
                err_line_match = error_line_pattern.search(line)
                if err_line_match:
                    error_line = err_line_match.group(1)

                # Find error type and message
                if not error_type and "Error:" in line or "Exception:" in line:
                    type_match = error_type_pattern.search(line)
                    if type_match:
                        error_type = type_match.group(1)
                        message_match = re.search(f"{error_type}:\s+(.*)", line)
                        if message_match:
                            error_message = message_match.group(1)

            # Add to failures with whatever we found
            failures.append(
                {
                    "file_path": file_path,
                    "function_name": function_name,
                    "function_line": func_line or "N/A",
                    "error_line": error_line or "N/A",
                    "error_type": error_type or "Unknown",
                    "error_message": error_message or "",
                }
            )

    # As a last resort, use yet another approach if we still don't have good data
    if not failures or all(
        f["function_line"] == "N/A" and f["error_line"] == "N/A" for f in failures
    ):
        # Run pytest with full traceback
        cmd = ["pytest", test_path, "--no-header", "--tb=native", "-v"]
        result = subprocess.run(cmd, capture_output=True, text=True)
        output = result.stdout + "\n" + result.stderr

        # Extract file and line information from tracebacks
        for function_name, test_info in failed_tests.items():
            file_path = test_info["file_path"]

            # Find sections related to this test
            test_pattern = re.compile(rf"{re.escape(file_path)}::\s*{function_name}")
            error_pattern = re.compile(
                rf'File "{re.escape(file_path)}",\s*line\s*(\d+)'
            )

            test_sections = []
            in_section = False
            section_start = 0

            # Identify test sections
            lines = output.split("\n")
            for i, line in enumerate(lines):
                if test_pattern.search(line):
                    if in_section:
                        test_sections.append((section_start, i - 1))
                    in_section = True
                    section_start = i
                elif (
                    in_section
                    and i > section_start + 5
                    and re.match(r"_{3,}|={3,}", line)
                ):
                    test_sections.append((section_start, i - 1))
                    in_section = False

            if in_section:
                test_sections.append((section_start, len(lines) - 1))

            # Process test sections
            for start, end in test_sections:
                section_lines = lines[start : end + 1]
                section_text = "\n".join(section_lines)

                error_line = None
                error_type = None
                error_message = None

                # Find error line
                err_matches = error_pattern.findall(section_text)
                if err_matches:
                    error_line = err_matches[
                        -1
                    ]  # Last match is usually the error location

                # Find error type and message
                type_match = re.search(
                    r"(AssertionError|ValueError|RuntimeError|TypeError|Exception):\s*(.*)",
                    section_text,
                )
                if type_match:
                    error_type = type_match.group(1)
                    error_message = type_match.group(2)

                if error_line or error_type:
                    # Only add if we found something useful
                    failures.append(
                        {
                            "file_path": file_path,
                            "function_name": function_name,
                            "function_line": "N/A",  # We're not looking for function lines in this approach
                            "error_line": error_line or "N/A",
                            "error_type": error_type or "Unknown",
                            "error_message": error_message or "",
                        }
                    )

    # Print the table
    if failures:
        # Remove duplicates while preserving order
        seen = set()
        unique_failures = []
        for f in failures:
            key = (f["file_path"], f["function_name"])
            if key not in seen:
                seen.add(key)
                unique_failures.append(f)

        failures = unique_failures

        # Calculate column widths based on content
        max_file_width = max(60, max(len(f["file_path"]) for f in failures) + 2)
        max_func_width = max(25, max(len(f["function_name"]) for f in failures) + 2)
        max_error_width = max(20, max(len(f["error_type"]) for f in failures) + 2)

        # Print table header
        print("-" * (max_file_width + max_func_width + 40))
        print(
            f"{'File Path':<{max_file_width}} {'Function':<{max_func_width}} {'Func Line':<10} {'Error Line':<10} {'Error Type':<{max_error_width}}"
        )
        print("-" * (max_file_width + max_func_width + 40))

        # Print each failure
        for failure in failures:
            print(
                f"{failure['file_path']:<{max_file_width}} {failure['function_name']:<{max_func_width}} {failure['function_line']:<10} {failure['error_line']:<10} {failure['error_type']:<{max_error_width}}"
            )

            # Print error message if available
            if failure["error_message"]:
                msg = failure["error_message"]
                if len(msg) > 100:
                    msg = msg[:97] + "..."
                print(f"  Error Message: {msg}")
                print()
    else:
        print("No failures found or could not parse failures.")


if __name__ == "__main__":
    if len(sys.argv) > 1:
        test_path = sys.argv[1]
    else:
        test_path = "tests/"
    generate_detailed_failure_summary(test_path)
