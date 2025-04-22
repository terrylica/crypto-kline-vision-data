#!/usr/bin/env bash
# Install git hooks for the binance-data-services repository

set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
REPO_ROOT="$( cd "$SCRIPT_DIR/.." && pwd )"
HOOKS_SOURCE_DIR="$REPO_ROOT/scripts/git-hooks"
HOOKS_TARGET_DIR="$REPO_ROOT/.git/hooks"

echo "Installing git hooks from $HOOKS_SOURCE_DIR to $HOOKS_TARGET_DIR"

# Check if autoflake is installed
if ! command -v autoflake &> /dev/null; then
    echo "⚠️  autoflake not found. Installing..."
    pip install autoflake
fi

# Copy each hook and make it executable
for hook in "$HOOKS_SOURCE_DIR"/*; do
    hook_name=$(basename "$hook")
    target_path="$HOOKS_TARGET_DIR/$hook_name"
    
    cp "$hook" "$target_path"
    chmod +x "$target_path"
    
    echo "✅ Installed $hook_name"
done

echo "✨ Git hooks installed successfully!"
echo "These hooks will run whenever you commit (including via the Source Control UI in Cursor/VS Code)." 