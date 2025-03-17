#!/bin/bash

# Setup GitHub CLI in DevContainer
# This script installs and configures GitHub CLI for use in DevContainers

echo "Setting up GitHub CLI for DevContainer..."

# Check if GitHub CLI is already installed
if ! command -v gh &> /dev/null; then
    echo "Installing GitHub CLI..."
    curl -fsSL https://cli.github.com/packages/githubcli-archive-keyring.gpg | sudo dd of=/usr/share/keyrings/githubcli-archive-keyring.gpg
    sudo chmod go+r /usr/share/keyrings/githubcli-archive-keyring.gpg
    echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/githubcli-archive-keyring.gpg] https://cli.github.com/packages stable main" | sudo tee /etc/apt/sources.list.d/github-cli.list > /dev/null
    sudo apt update
    sudo apt install gh -y
fi

# Configure Git to use HTTPS instead of SSH
echo "Configuring Git to use HTTPS for GitHub..."
git config --global url."https://github.com/".insteadOf git@github.com:

# Remind user to authenticate
echo ""
echo "GitHub CLI setup complete!"
echo ""
echo "To authenticate with GitHub, run:"
echo "  gh auth login --web"
echo ""
echo "After authentication, set your Git user information:"
echo "  git config --global user.name \"Your Name\""
echo "  git config --global user.email \"your.email@example.com\""
echo ""
echo "You can also use GitHub CLI to create PRs, view issues, and more:"
echo "  gh pr create        # Create a pull request"
echo "  gh issue list       # List issues"
echo "  gh repo view        # View repository details"
echo ""

echo "Setup completed successfully!"
