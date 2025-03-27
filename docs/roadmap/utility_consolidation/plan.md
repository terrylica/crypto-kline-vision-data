# Roadmap for Consolidating Utility Files

## Goal

To consolidate and refactor utility files in the `utils` directory to improve maintainability, reduce code duplication, and enhance consistency, without introducing breaking changes to the `core` scripts.

## Phases

This consolidation will be performed in phases to minimize risk and allow for iterative testing and validation.

## Phase 1: Time Utility Consolidation

- **Objective**: Consolidate time-related functionalities from `time_alignment.py` and relevant parts of `api_boundary_validator.py` into a single, cohesive time utility module.
- **Tasks**:
  1. **Identify Core Time Functions**: Review `time_alignment.py` and `api_boundary_validator.py` to identify all functions related to time manipulation, timezone handling, and interval calculations.
  2. **Create `time_utils.py`**: Create a new file `utils/time_utils.py`.
  3. **Move Time Functions**: Move relevant functions from `time_alignment.py` and `api_boundary_validator.py` to `utils/time_utils.py`. Ensure to:
     - Maintain original function signatures for backward compatibility.
     - Resolve any naming conflicts and ensure clear, consistent naming.
     - Consolidate duplicated timezone handling logic into a single function in `time_utils.py`.
  4. **Update Imports**: Update all files in `utils` and `core` that currently import from `time_alignment.py` and `api_boundary_validator.py` (for time functions) to import from `utils/time_utils.py` instead.
  5. **Deprecate Old Functions**: In `time_alignment.py` and `api_boundary_validator.py`, mark the moved time-related functions as deprecated, but keep them as wrappers that call the new functions in `utils/time_utils.py`. This will provide a transition period and avoid immediate breaking changes. Add clear deprecation warnings using the `warnings` module.
  6. **Testing**:
     - Run existing tests using `scripts/run_tests_parallel.sh tests/core tests/utils` to ensure no regressions are introduced in core functionality.
     - Create new unit tests specifically for `utils/time_utils.py` to test all consolidated time functions thoroughly.

## Phase 2: Validation Logic Consolidation

- **Objective**: Combine validation functionalities from `cache_validator.py` and `validation.py` into a unified validation module.
- **Tasks**:
  1. **Identify Core Validation Functions**: Review `cache_validator.py` and `validation.py` to identify all validation functions, including data validation, cache validation, and input validation.
  2. **Create `validation_utils.py`**: Create a new file `utils/validation_utils.py`.
  3. **Move Validation Functions**: Move relevant functions from `cache_validator.py` and `validation.py` to `utils/validation_utils.py`. Ensure to:
     - Maintain original function signatures.
     - Consolidate any overlapping validation logic and error handling.
     - Refactor `CacheValidationError` and related classes/enums into `validation_utils.py` for better organization.
  4. **Update Imports**: Update all files in `utils` and `core` that currently import from `cache_validator.py` and `validation.py` to import from `utils/validation_utils.py` for validation functions.
  5. **Deprecate Old Validation Classes/Functions**: In `cache_validator.py` and `validation.py`, mark the moved validation functions and classes as deprecated, using wrapper functions that call the new implementations in `utils/validation_utils.py`. Add deprecation warnings.
  6. **Testing**:
     - Run existing tests using `scripts/run_tests_parallel.sh tests/core tests/utils`.
     - Create new unit tests for `utils/validation_utils.py` to cover all consolidated validation logic.

## Phase 3: HTTP Client and Download Handling Consolidation

- **Objective**: Consolidate HTTP client creation and download handling logic from `download_handler.py` and `http_client_factory.py` into a single module focused on network operations.
- **Tasks**:
  1. **Review HTTP Client and Download Logic**: Analyze `download_handler.py` and `http_client_factory.py` to understand HTTP client creation, configuration, retry mechanisms, and download progress tracking.
  2. **Create `network_utils.py`**: Create a new file `utils/network_utils.py`.
  3. **Move HTTP Client and Download Functions**: Move relevant classes and functions from `download_handler.py` and `http_client_factory.py` to `utils/network_utils.py`. Ensure to:
     - Maintain key function signatures (e.g., `download_file` in `DownloadHandler`, `create_client` in `HttpClientFactory`).
     - Consolidate HTTP client creation logic and configurations.
     - Ensure `DownloadHandler` in `network_utils.py` uses the client factory from the same module.
  4. **Update Imports**: Update imports in `utils` and `core` to use `utils/network_utils.py` for HTTP client and download functionalities.
  5. **Deprecate Old Classes/Functions**: In `download_handler.py` and `http_client_factory.py`, deprecate the moved classes and functions, providing wrappers that call the new implementations in `utils/network_utils.py`. Add deprecation warnings.
  6. **Testing**:
     - Run existing tests using `scripts/run_tests_parallel.sh tests/core tests/utils`.
     - Create unit tests for `utils/network_utils.py`, focusing on HTTP client creation, request handling, and download functionalities.

## Phase 4: Deprecated Code Removal and Final Cleanup

- **Objective**: Remove deprecated wrapper functions and classes from the original utility files (`time_alignment.py`, `cache_validator.py`, `validation.py`, `download_handler.py`, `http_client_factory.py`) and perform final code cleanup.
- **Tasks**:
  1. **Remove Deprecated Wrappers**: After ensuring all imports have been updated and the system is stable with the deprecated wrappers, remove the wrapper functions and classes from the original utility files.
  2. **Update Documentation**: Update any relevant documentation (including code comments and external documentation if any) to reflect the new utility module structure and usage.
  3. **Final Testing**: Run all tests (`scripts/run_tests_parallel.sh tests/core tests/utils`) one last time to confirm no issues after removing deprecated code.
  4. **Code Review**: Conduct a final code review of all consolidated utility modules to ensure code quality, consistency, and clarity.

## Dependencies

- Each phase depends on the successful completion and testing of the previous phase.
- Testing is a critical dependency for each task and phase.

## Testing Strategy

- **Unit Tests**: Create comprehensive unit tests for each new consolidated utility module (`time_utils.py`, `validation_utils.py`, `network_utils.py`).
- **Integration Tests**: Existing core script tests will serve as integration tests, ensuring that the utility consolidations do not break core functionalities.
- **Parallel Test Execution**: Utilize `scripts/run_tests_parallel.sh` for efficient test execution.
- **Monitor Deprecation Warnings**: Pay close attention to deprecation warnings during testing to ensure a smooth transition and identify any missed import updates.

## Rollback Plan

- **Version Control**: Utilize Git for version control. Commit changes frequently and use branches for each phase.
- **Rollback Procedure**: If a phase introduces critical issues, rollback to the last stable commit on the main branch.
- **Feature Flags (Optional)**: For higher-risk changes, consider using feature flags to enable/disable new utility modules, allowing for easier rollback and controlled rollout.

## Timeline

- Each phase is estimated to take approximately 2-5 days, depending on complexity and testing requirements.
- The entire consolidation process is estimated to take 2-3 weeks.

This roadmap provides a structured approach to consolidate the utility files, ensuring minimal disruption to the core scripts and a more maintainable codebase in the long run.
