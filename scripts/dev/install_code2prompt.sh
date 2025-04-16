#!/bin/bash

# install_code2prompt.sh
# A script to install and set up code2prompt on Linux
# Created: April 11, 2025

# Set terminal colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Print formatted message
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if a command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Install Rust and Cargo if not already installed
install_rust() {
    if ! command_exists cargo; then
        log_info "Installing Rust and Cargo..."
        curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
        
        # Source the environment
        source "$HOME/.cargo/env"
        
        if command_exists cargo; then
            log_success "Rust and Cargo installed successfully."
        else
            log_error "Failed to install Rust and Cargo."
            log_info "Please try installing manually:"
            log_info "curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh"
            exit 1
        fi
    else
        log_info "Rust and Cargo are already installed."
    fi
}

# Install code2prompt via Cargo
install_code2prompt() {
    log_info "Installing code2prompt..."
    
    if ! command_exists cargo; then
        log_error "Cargo not found. Please make sure Rust is installed correctly."
        exit 1
    fi
    
    cargo install code2prompt
    
    if command_exists code2prompt; then
        log_success "code2prompt installed successfully!"
    else
        log_error "Failed to install code2prompt."
        exit 1
    fi
}

# Create example directory with sample files
create_examples() {
    local example_dir="code2prompt_example"
    
    if [ -d "$example_dir" ]; then
        log_info "Example directory already exists: $example_dir"
    else
        log_info "Creating example directory structure..."
        mkdir -p "$example_dir/utils"
        
        # Create main.py
        cat > "$example_dir/main.py" << 'EOF'
#!/usr/bin/env python3
import pendulum
from utils.logger_setup import logger

def get_current_time():
    """Returns the current time with milliseconds in UTC."""
    return pendulum.now().format("YYYY-MM-DD HH:mm:ss.SSS")

def main():
    logger.info(f"Current time: {get_current_time()}")
    logger.info("This is a sample script to test code2prompt")

if __name__ == "__main__":
    main()
EOF
        
        # Create utils/helpers.py
        cat > "$example_dir/utils/helpers.py" << 'EOF'
#!/usr/bin/env python3
import pendulum
from typing import List, Dict, Any

def filter_data(data: List[Dict[str, Any]], key: str, value: Any) -> List[Dict[str, Any]]:
    """
    Filter a list of dictionaries based on key-value pairs.
    
    Args:
        data: List of dictionaries to filter
        key: Dictionary key to filter on
        value: Value to compare against
        
    Returns:
        Filtered list of dictionaries
    """
    return [item for item in data if item.get(key) == value]

def format_timestamp(timestamp: int) -> str:
    """
    Format a Unix timestamp into human-readable format with milliseconds.
    
    Args:
        timestamp: Unix timestamp in milliseconds
        
    Returns:
        Formatted datetime string
    """
    dt = pendulum.from_timestamp(timestamp / 1000)
    return dt.format("YYYY-MM-DD HH:mm:ss.SSS")
EOF
        
        # Make scripts executable
        chmod +x "$example_dir/main.py"
        chmod +x "$example_dir/utils/helpers.py"
        
        log_success "Example files created in $example_dir/"
    fi
}

# Show usage examples
show_usage() {
    echo
    echo -e "${GREEN}🚀 code2prompt is now installed!${NC}"
    echo
    echo -e "${YELLOW}Usage Examples:${NC}"
    echo
    
    echo -e "${BLUE}Basic usage - Generate prompt from a directory:${NC}"
    echo -e "${GREEN}code2prompt -p code2prompt_example${NC}"
    echo
    
    echo -e "${BLUE}Generate prompt with token count:${NC}"
    echo -e "${GREEN}code2prompt -p code2prompt_example --tokens${NC}"
    echo
    
    echo -e "${BLUE}Save output to a file:${NC}"
    echo -e "${GREEN}code2prompt generate -p code2prompt_example -o prompt_output.md${NC}"
    echo
    
    echo -e "${BLUE}Analyze a codebase:${NC}"
    echo -e "${GREEN}code2prompt analyze -p code2prompt_example${NC}"
    echo
    
    echo -e "${BLUE}Analyze with tree format:${NC}"
    echo -e "${GREEN}code2prompt analyze -p code2prompt_example --format tree${NC}"
    echo
    
    echo -e "${BLUE}Interactive file selection:${NC}"
    echo -e "${GREEN}code2prompt -p code2prompt_example -i${NC}"
    echo
    
    echo -e "${YELLOW}For more options:${NC}"
    echo -e "${GREEN}code2prompt --help${NC}"
    echo
}

# Main execution flow
main() {
    log_info "Starting code2prompt installation..."
    
    # Check if code2prompt is already installed
    if command_exists code2prompt; then
        log_info "code2prompt is already installed. Checking version..."
        code2prompt --version
        create_examples
        show_usage
        return 0
    fi
    
    # Install Rust if not present
    install_rust
    
    # Install code2prompt
    install_code2prompt
    
    # Create example files
    create_examples
    
    # Display usage examples
    show_usage
    
    log_success "Setup completed successfully!"
    
    if [ -f "$HOME/.cargo/env" ]; then
        log_info "To ensure Cargo is in your PATH for future sessions, you may need to run:"
        log_info "source \$HOME/.cargo/env"
    fi
    
    return 0
}

# Run the script
main
exit $? 