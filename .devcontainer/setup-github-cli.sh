#!/bin/bash
set -e

# Setup GitHub CLI in DevContainer
# This script configures GitHub CLI for minimal viable access to GitHub repositories

echo "Configuring GitHub CLI for repository access..."

# Configure Git to use HTTPS instead of SSH (more reliable in containers)
git config --global url."https://github.com/".insteadOf git@github.com:

# Set default no-pager for GitHub CLI
echo 'export GH_PAGER=""' >> ~/.bashrc

# Create a simple helper function for GitHub CLI in bashrc
cat >> ~/.bashrc << 'EOF'

# GitHub CLI helper function
ghauth() {
  echo "Authenticating with GitHub..."
  gh auth login --web
  echo "Setting up Git identity (required for commits)..."
  echo "Run: git config --global user.name \"Your Name\""
  echo "Run: git config --global user.email \"your@email.com\""
}
EOF

echo ""
echo "✅ GitHub CLI configuration complete!"
echo ""
echo "To authenticate, type 'ghauth' in your terminal"
echo ""
echo "GitHub CLI is ready to use with commands like:"
echo "  gh repo clone REPO              # Clone a repository"
echo "  gh pr create                    # Create a pull request"
echo "  gh pr checkout NUMBER           # Check out a pull request"
echo "  gh pr merge                     # Merge a pull request"
echo ""
