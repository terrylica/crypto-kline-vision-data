# Python Package Documentation Checklist

Use this checklist to verify that modules, classes, and functions in the Data Source Manager package comply with the Python package principles.

## Module-Level Documentation

- [ ] Module has a comprehensive module-level docstring
- [ ] Docstring explains the module's purpose
- [ ] Docstring lists key classes and functions
- [ ] Docstring explains how the module fits into the broader package
- [ ] Docstring includes usage examples where appropriate

## Class Documentation

- [ ] Each class has a comprehensive class-level docstring
- [ ] Docstring explains the class's purpose
- [ ] Docstring documents all attributes
- [ ] Docstring includes usage examples where appropriate
- [ ] `attrs` classes use appropriate validators
- [ ] `attrs` classes specify `frozen=True` for immutability where appropriate
- [ ] `attrs` classes use `slots=True` for memory efficiency
- [ ] Class methods have appropriate docstrings

## Function Documentation

- [ ] Each function has a comprehensive docstring
- [ ] Docstring explains the function's purpose
- [ ] Docstring documents all parameters
- [ ] Docstring documents return values
- [ ] Docstring documents exceptions raised
- [ ] Docstring includes usage examples where appropriate
- [ ] Function uses appropriate type hints

## Import Structure

- [ ] Module imports are organized logically
- [ ] External imports are separated from internal imports
- [ ] No wildcard imports (`from module import *`)
- [ ] Import only what is needed

## API Design

- [ ] Functions and methods follow consistent parameter ordering
- [ ] Functions use keyword-only arguments where appropriate
- [ ] Enums are used for constrained values
- [ ] Constants are defined at module level
- [ ] Functions and methods follow single-responsibility principle

## Naming Conventions

- [ ] Module names follow snake_case convention
- [ ] Class names follow PascalCase convention
- [ ] Function names follow snake_case convention
- [ ] Constants use UPPER_SNAKE_CASE
- [ ] Names are descriptive and meaningful

## Error Handling

- [ ] Functions validate input parameters
- [ ] Functions raise appropriate exceptions with helpful messages
- [ ] Error cases are documented in docstrings

## CLI Tools

- [ ] CLI tools include comprehensive help text
- [ ] CLI tools use typer for command-line parsing
- [ ] CLI tools provide short flags for all options
- [ ] CLI tools include appropriate error handling
- [ ] CLI tools validate input parameters

## Tests

- [ ] Module has corresponding unit tests
- [ ] Tests cover both normal and error cases
- [ ] Tests validate docstring examples

## Packaging

- [ ] Module is properly included in `__init__.py` exports
- [ ] Module is properly included in package configuration

## How to Use This Checklist

1. For each module in the package, copy this checklist to a new file or section
2. Review the module against each checklist item
3. Mark items as complete or incomplete
4. Address any incomplete items before considering the module complete

Remember that not all items will apply to every module, but most should be applicable to most modules.
