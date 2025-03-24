#!/bin/bash
# Run tests with the correct configuration
# Usage: ./scripts/run_tests.sh [test_path] [log_level] [additional_pytest_args]
#   test_path: Optional path to a specific test file or directory
#   log_level: Optional log level (DEBUG, INFO, WARNING, ERROR) - defaults to INFO
#   additional_pytest_args: Optional additional pytest arguments
#   
# Coverage examples:
#   ./scripts/run_tests.sh tests/ INFO --cov=. --cov-report=term
#   ./scripts/run_tests.sh tests/test_vision_api_core_functionality.py DEBUG --cov=.

set -e

clear

# Define colors
CYAN='\033[0;36m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
MAGENTA='\033[0;35m'
RED='\033[0;31m'
BOLD='\033[1m'
RESET='\033[0m'

# Enable debug mode if DEBUG_SCRIPT environment variable is set
DEBUG_SCRIPT=${DEBUG_SCRIPT:-false}

# Simple debug logging function
debug_log() {
  if [ "$DEBUG_SCRIPT" = true ]; then
    echo -e "${MAGENTA}[DEBUG] $1${RESET}" >&2
  fi
}

# Function to print command before executing
print_command() {
  echo -e "\n${BOLD}${BLUE}▶ EXECUTING COMMAND:${RESET}"
  echo -e "${BOLD}${CYAN}$ $1${RESET}"
  echo -e "${CYAN}----------------------------------------------------${RESET}\n"
}

debug_log "Starting test runner script"

SCRIPT_DIR=$(dirname "$0")
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${PROJECT_ROOT}"

debug_log "Project root: $PROJECT_ROOT"

# Check if we're running within VS Code's devcontainer
if [ -n "$VSCODE_REMOTE_CONTAINERS_SESSION" ] || [ -n "$REMOTE_CONTAINERS" ] || [ -n "$CODESPACES" ]; then
    IS_DEVCONTAINER=true
    debug_log "Running in devcontainer environment"
else
    IS_DEVCONTAINER=false
    debug_log "Running in standard environment"
fi

# Set default values
LOG_LEVEL="INFO"
ADDITIONAL_ARGS=""

# Helper function to print a section header
print_header() {
  echo
  echo -e "${BOLD}${BLUE}▶ $1${RESET}"
  echo -e "${CYAN}----------------------------------------------------${RESET}"
}

# Helper function to print options with colors
print_options() {
  local options=("$@")
  for i in "${!options[@]}"; do
    echo -e "  ${BOLD}$((i+1)).${RESET} ${options[$i]}"
  done
}

# Function to print usage
usage() {
    echo "Usage: $0 [test_path] [log_level] [additional_args]"
    echo "  test_path:       Path to test file or directory (optional)"
    echo "  log_level:       Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL) (optional)"
    echo "  additional_args: Additional arguments to pass to pytest (optional)"
    echo ""
    echo "  Coverage flags:"
    echo "    --cov=.                 Enable coverage for all modules"
    echo "    --cov-report=term       Show coverage report in terminal"
    echo ""
    echo "  Or select an option from the menu."
}

# Function to check if a pytest plugin is installed
check_pytest_plugin() {
    local plugin_name=$1
    local plugin_import_name=${2:-$plugin_name}
    
    debug_log "Checking for pytest plugin: $plugin_name"
    
    local cmd="python -c \"import pytest; import $plugin_import_name\" 2>/dev/null"
    print_command "$cmd"
    
    if eval "$cmd"; then
        debug_log "Plugin $plugin_name is installed"
        return 0
    else
        debug_log "Plugin $plugin_name is NOT installed"
        return 1
    fi
}

# Interactive mode if no arguments provided
if [ $# -eq 0 ]; then
  echo -e "\n${BOLD}${GREEN}==================== PYTEST RUNNER ====================${RESET}"
  echo -e "${YELLOW}Running in interactive mode${RESET}"
  echo -e "${GREEN}=====================================================${RESET}\n"
  
  # Find all test files for selection
  find_cmd="find tests -name \"test_*.py\" | sort"
  print_command "$find_cmd"
  mapfile -t TEST_FILES < <(eval "$find_cmd")
  
  # Define test directories based on the new folder structure
  TEST_DIRS=(
    "All tests (tests/)"
    "1-second interval tests (tests/interval_1s/)"
    "New intervals tests (tests/interval_new/)"
  )
  
  # Display test selection options
  print_header "TEST SELECTION"
  print_options "${TEST_DIRS[@]}"
  
  # Then add each test file as an option
  for i in "${!TEST_FILES[@]}"; do
    echo -e "  ${BOLD}$((i+1+${#TEST_DIRS[@]})).${RESET} ${TEST_FILES[$i]}"
  done
  
  read -rp "$(echo -e "${YELLOW}Enter selection (1-$((${#TEST_DIRS[@]}+${#TEST_FILES[@]})), default: 1): ${RESET}")" test_selection
  
  if [[ -z "$test_selection" ]]; then
    echo -e "${GREEN}Using default: All tests (tests/)${RESET}"
    TEST_PATH="tests/"
  elif [[ $test_selection -ge 1 && $test_selection -le ${#TEST_DIRS[@]} ]]; then
    case $test_selection in
      1) TEST_PATH="tests/" ;;
      2) TEST_PATH="tests/interval_1s/" ;;
      3) TEST_PATH="tests/interval_new/" ;;
    esac
    echo -e "${GREEN}Selected: ${TEST_DIRS[$((test_selection-1))]}${RESET}"
  elif [[ $test_selection -gt ${#TEST_DIRS[@]} && $test_selection -le $((${#TEST_DIRS[@]}+${#TEST_FILES[@]})) ]]; then
    TEST_PATH="${TEST_FILES[$((test_selection-${#TEST_DIRS[@]}-1))]}"
    echo -e "${GREEN}Selected: ${TEST_PATH}${RESET}"
  else
    echo -e "${RED}Invalid selection. Using default: tests/${RESET}"
    TEST_PATH="tests/"
  fi
  echo -e "${BOLD}Selected test path:${RESET} ${CYAN}$TEST_PATH${RESET}"
  
  # Check for required plugins
  HAS_COVERAGE=false
  HAS_XDIST=false
  
  if check_pytest_plugin "pytest_cov"; then
    HAS_COVERAGE=true
  fi
  
  if check_pytest_plugin "xdist"; then
    HAS_XDIST=true
  fi
  
  # Preset combinations
  print_header "TEST MODE"
  TEST_MODES=(
    "Standard (INFO logs, normal output)"
    "Verbose (DEBUG logs, all output visible)"
    "Quiet (ERROR logs only, minimal output)"
    "Debug (DEBUG logs with PDB on failure)"
    "Performance (INFO logs with duration reports)"
  )
  
  # Only add coverage option if available
  if $HAS_COVERAGE; then
    TEST_MODES+=("Coverage (code coverage analysis)")
  else
    debug_log "pytest-cov not installed, hiding Coverage option"
  fi
  
  # Only add parallel option if available
  if $HAS_XDIST; then
    TEST_MODES+=("Parallel (8 workers)")
  else
    debug_log "pytest-xdist not installed, hiding Parallel option"
  fi
  
  # Add warnings summary option
  TEST_MODES+=("Warnings Summary (collect and display warnings/errors at end)")
  
  # Add custom option at the end
  TEST_MODES+=("Custom (configure options manually)")
  
  print_options "${TEST_MODES[@]}"
  
  read -rp "$(echo -e "${YELLOW}Enter selection (1-${#TEST_MODES[@]}, default: 1): ${RESET}")" mode_selection
  
  if [[ -z "$mode_selection" ]]; then
    echo -e "${GREEN}Using default: Standard mode${RESET}"
    mode_selection=1
  elif [[ $mode_selection -lt 1 || $mode_selection -gt ${#TEST_MODES[@]} ]]; then
    echo -e "${RED}Invalid selection. Using default: Standard mode${RESET}"
    mode_selection=1
  fi
  
  # Set options based on selected mode
  case $mode_selection in
    1) # Standard
      LOG_LEVEL="INFO"
      ADDITIONAL_ARGS=""
      CAPTURE_FLAG="--capture=tee-sys"
      echo -e "${GREEN}Selected mode: Standard${RESET}"
      ;;
    2) # Verbose
      LOG_LEVEL="DEBUG"
      ADDITIONAL_ARGS="-xvs"
      CAPTURE_FLAG="--capture=no"
      echo -e "${MAGENTA}Selected mode: Verbose${RESET}"
      ;;
    3) # Quiet
      LOG_LEVEL="ERROR"
      ADDITIONAL_ARGS="-q"
      CAPTURE_FLAG="--capture=fd"
      echo -e "${BLUE}Selected mode: Quiet${RESET}"
      ;;
    4) # Debug
      LOG_LEVEL="DEBUG"
      ADDITIONAL_ARGS="--pdb"
      CAPTURE_FLAG="--capture=no"
      echo -e "${RED}Selected mode: Debug${RESET}"
      ;;
    5) # Performance
      LOG_LEVEL="INFO"
      ADDITIONAL_ARGS="--durations=10"
      CAPTURE_FLAG="--capture=tee-sys"
      echo -e "${YELLOW}Selected mode: Performance${RESET}"
      ;;
    6) # Coverage (if available)
      if $HAS_COVERAGE; then
        LOG_LEVEL="INFO"
        ADDITIONAL_ARGS="--cov=. --cov-report=term"
        CAPTURE_FLAG="--capture=tee-sys"
        echo -e "${BLUE}Selected mode: Coverage${RESET}"
      else
        # This should not happen as the option should not be shown if not available
        echo -e "${RED}Error: Coverage plugin not installed. Please install with: pip install pytest-cov${RESET}"
        exit 1
      fi
      ;;
    7) # Parallel (if available)
      if $HAS_XDIST; then
        LOG_LEVEL="INFO"
        ADDITIONAL_ARGS="-n8"
        CAPTURE_FLAG="--capture=tee-sys"
        echo -e "${CYAN}Selected mode: Parallel${RESET}"
      else
        # This should not happen as the option should not be shown if not available
        echo -e "${RED}Error: Parallel plugin not installed. Please install with: pip install pytest-xdist${RESET}"
        exit 1
      fi
      ;;
    8) # Warnings Summary
      LOG_LEVEL="WARNING"
      ADDITIONAL_ARGS="-v --showlocals -W error --tb=short -x --no-header"
      CAPTURE_FLAG="--capture=fd"
      USE_CUSTOM_FORMAT=true
      echo -e "${YELLOW}Selected mode: Warnings Summary${RESET}"
      ;;
    9) # Custom
      # Select log level
      print_header "LOG LEVEL"
      LOG_LEVELS=("DEBUG" "INFO" "WARNING" "ERROR")
      
      # Display log levels with colors
      echo -e "  ${BOLD}1.${RESET} ${MAGENTA}DEBUG${RESET}"
      echo -e "  ${BOLD}2.${RESET} ${GREEN}INFO${RESET}"
      echo -e "  ${BOLD}3.${RESET} ${YELLOW}WARNING${RESET}"
      echo -e "  ${BOLD}4.${RESET} ${RED}ERROR${RESET}"
      
      read -rp "$(echo -e "${YELLOW}Enter selection (1-${#LOG_LEVELS[@]}, default: 2): ${RESET}")" log_selection
      
      if [[ -z "$log_selection" ]]; then
        echo -e "${GREEN}Using default: INFO${RESET}"
        LOG_LEVEL="INFO"
      elif [[ $log_selection -ge 1 && $log_selection -le ${#LOG_LEVELS[@]} ]]; then
        LOG_LEVEL="${LOG_LEVELS[$((log_selection-1))]}"
      else
        echo -e "${RED}Invalid selection. Using default: INFO${RESET}"
        LOG_LEVEL="INFO"
      fi
      
      # Select output capture mode
      print_header "OUTPUT CAPTURE"
      CAPTURE_MODES=(
        "tee-sys (show output but capture for reports)"
        "no (show all output in real-time)"
        "fd (capture output, only show on failure)"
      )
      
      print_options "${CAPTURE_MODES[@]}"
      
      read -rp "$(echo -e "${YELLOW}Enter selection (1-${#CAPTURE_MODES[@]}, default: 1): ${RESET}")" capture_selection
      
      if [[ -z "$capture_selection" ]]; then
        echo -e "${GREEN}Using default: tee-sys${RESET}"
        CAPTURE_FLAG="--capture=tee-sys"
      elif [[ $capture_selection -ge 1 && $capture_selection -le ${#CAPTURE_MODES[@]} ]]; then
        case $capture_selection in
          1) CAPTURE_FLAG="--capture=tee-sys" ;;
          2) CAPTURE_FLAG="--capture=no" ;;
          3) CAPTURE_FLAG="--capture=fd" ;;
        esac
      else
        echo -e "${RED}Invalid selection. Using default: tee-sys${RESET}"
        CAPTURE_FLAG="--capture=tee-sys"
      fi
      
      # Common pytest options
      print_header "PYTEST OPTIONS"
      PYTEST_OPTIONS=(
        "None (no additional args)"
        "-xvs (verbose with output capture disabled)"
        "--pdb (debug on failure)"
        "-m real (run tests with 'real' marker)"
        "-m integration (run tests with 'integration' marker)" 
        "--durations=10 (show 10 slowest tests)"
      )
      
      # Display pytest options
      print_options "${PYTEST_OPTIONS[@]}"
      
      read -rp "$(echo -e "${YELLOW}Enter selection (1-${#PYTEST_OPTIONS[@]}, default: 1): ${RESET}")" args_selection
      
      if [[ -z "$args_selection" ]]; then
        echo -e "${GREEN}Using default: None (no additional args)${RESET}"
        ADDITIONAL_ARGS=""
      elif [[ $args_selection -ge 1 && $args_selection -le ${#PYTEST_OPTIONS[@]} ]]; then
        selected_args="${PYTEST_OPTIONS[$((args_selection-1))]}"
        case "$selected_args" in
          "None"*)
            ADDITIONAL_ARGS=""
            ;;
          "-xvs"*)
            ADDITIONAL_ARGS="-xvs"
            ;;
          "--pdb"*)
            ADDITIONAL_ARGS="--pdb"
            ;;
          "-m real"*)
            ADDITIONAL_ARGS="-m real"
            ;;
          "-m integration"*)
            ADDITIONAL_ARGS="-m integration"
            ;;
          "--durations"*)
            ADDITIONAL_ARGS="--durations=10"
            ;;
        esac
      else
        echo -e "${RED}Invalid selection. Using default: None${RESET}"
        ADDITIONAL_ARGS=""
      fi
      
      # Custom args option
      read -rp "$(echo -e "${YELLOW}Enter custom pytest args (leave empty for none): ${RESET}")" custom_args
      if [ -n "$custom_args" ]; then
        if [ -n "$ADDITIONAL_ARGS" ]; then
          ADDITIONAL_ARGS="$ADDITIONAL_ARGS $custom_args"
        else
          ADDITIONAL_ARGS="$custom_args"
        fi
      fi
      ;;
  esac
  
  # Color the log level based on selection
  case "$LOG_LEVEL" in
    "DEBUG")
      log_color="${MAGENTA}"
      ;;
    "INFO")
      log_color="${GREEN}"
      ;;
    "WARNING")
      log_color="${YELLOW}"
      ;;
    "ERROR")
      log_color="${RED}"
      ;;
  esac
  
  echo -e "${BOLD}Selected log level:${RESET} ${log_color}$LOG_LEVEL${RESET}"
  echo -e "${BOLD}Output capture:${RESET} ${CYAN}${CAPTURE_FLAG#--capture=}${RESET}"
  echo -e "${BOLD}Additional args:${RESET} ${CYAN}${ADDITIONAL_ARGS:-None}${RESET}"
  
else
  # Use command line arguments
  TEST_PATH=${1:-tests}
  LOG_LEVEL=${2:-INFO}
  shift 2 2>/dev/null || true
  ADDITIONAL_ARGS="$*"
  CAPTURE_FLAG="--capture=tee-sys"
  
  debug_log "Using command line arguments:"
  debug_log "  TEST_PATH: $TEST_PATH"
  debug_log "  LOG_LEVEL: $LOG_LEVEL"
  debug_log "  ADDITIONAL_ARGS: $ADDITIONAL_ARGS"
  
  # Set color for log level
  case "$LOG_LEVEL" in
    "DEBUG")
      log_color="${MAGENTA}"
      ;;
    "INFO")
      log_color="${GREEN}"
      ;;
    "WARNING")
      log_color="${YELLOW}"
      ;;
    "ERROR")
      log_color="${RED}"
      ;;
    *)
      log_color="${GREEN}"
      ;;
  esac
  
  # Check if coverage flags are in the additional args
  if [[ "$ADDITIONAL_ARGS" == *"--cov"* ]]; then
    debug_log "Coverage flags detected in arguments"
    # Remove any --no-open-report flags as they're no longer needed
    ADDITIONAL_ARGS=${ADDITIONAL_ARGS/--no-open-report/}
  fi
  
  # Check if we need to install plugins
  if [[ "$ADDITIONAL_ARGS" == *"--cov"* ]] && ! check_pytest_plugin "pytest_cov"; then
    echo -e "${YELLOW}Warning: Coverage plugin not installed. Installing pytest-cov...${RESET}"
    install_cmd="pip install pytest-cov"
    print_command "$install_cmd"
    eval "$install_cmd"
  fi
  
  if [[ "$ADDITIONAL_ARGS" == *"-n"* ]] && ! check_pytest_plugin "xdist"; then
    echo -e "${YELLOW}Warning: Parallel testing plugin not installed. Installing pytest-xdist...${RESET}"
    install_cmd="pip install pytest-xdist"
    print_command "$install_cmd"
    eval "$install_cmd"
  fi
fi

# Print summary header
echo
echo -e "${BOLD}${GREEN}========================================================================${RESET}"
echo -e "${BOLD}${BLUE}                       PYTEST COMMAND SUMMARY                          ${RESET}"
echo -e "${BOLD}${GREEN}========================================================================${RESET}"
echo -e "${BOLD}Test path:${RESET}       ${CYAN}${TEST_PATH}${RESET}"
echo -e "${BOLD}Log level:${RESET}       ${log_color}${LOG_LEVEL}${RESET}"
echo -e "${BOLD}Output capture:${RESET}  ${CYAN}${CAPTURE_FLAG#--capture=}${RESET}"
if [ -n "$ADDITIONAL_ARGS" ]; then
  echo -e "${BOLD}Additional args:${RESET} ${CYAN}${ADDITIONAL_ARGS}${RESET}"
fi
echo -e "${BOLD}${GREEN}========================================================================${RESET}"
echo

# Run pytest with the recommended flags
echo -e "${YELLOW}Running tests...${RESET}\n"

# Create a temporary pytest.ini for the warnings summary option
if [ "$mode_selection" = "8" ] && [ "$USE_CUSTOM_FORMAT" = "true" ]; then
  echo -e "${MAGENTA}[DEBUG] Setting up custom log format for Warnings Summary mode${RESET}"
  
  # Create a temporary pytest.ini with the desired log format
  echo -e "${GREEN}Creating temporary pytest.ini with custom log format${RESET}"
  cat_cmd="cat > pytest.ini << EOF
[pytest]
log_format = %(levelname)s %(message)s
EOF"
  print_command "$cat_cmd"
  eval "$cat_cmd"
  debug_log "Created temporary pytest.ini with custom log format"
  
  # Verify the file was created correctly
  if [ -f pytest.ini ]; then
    echo -e "${GREEN}Custom log format enabled - removed timestamps${RESET}"
    if [ "$DEBUG_SCRIPT" = true ]; then
      echo -e "${MAGENTA}[DEBUG] Contents of pytest.ini:${RESET}"
      cat_cmd="cat pytest.ini"
      print_command "$cat_cmd"
      eval "$cat_cmd"
    fi
  else
    echo -e "${RED}Failed to create pytest.ini - will use default log format${RESET}"
  fi
fi

# Construct the full pytest command
PYTEST_CMD="PYTHONPATH=${PROJECT_ROOT} pytest \"${TEST_PATH}\" -vv --log-cli-level=${LOG_LEVEL} --asyncio-mode=auto ${CAPTURE_FLAG} ${ADDITIONAL_ARGS}"

# Print the command before running it
print_command "$PYTEST_CMD"

# Run the command
eval "$PYTEST_CMD"

PYTEST_EXIT_CODE=$?
debug_log "pytest exited with code $PYTEST_EXIT_CODE"

# Remove temporary pytest.ini if we created one
if [ "$mode_selection" = "8" ] && [ "$USE_CUSTOM_FORMAT" = "true" ]; then
  rm_cmd="rm -f pytest.ini"
  print_command "$rm_cmd"
  eval "$rm_cmd"
  debug_log "Removed temporary pytest.ini"
fi

# Completely remove the coverage report opening code
if [ $PYTEST_EXIT_CODE -eq 0 ]; then
  echo -e "\n${BOLD}${GREEN}Tests completed successfully!${RESET}"
else
  echo -e "\n${BOLD}${RED}Tests failed with exit code $PYTEST_EXIT_CODE${RESET}"
fi

echo "Completed successfully!"
exit $PYTEST_EXIT_CODE 