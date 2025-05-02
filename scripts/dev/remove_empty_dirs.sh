#!/usr/bin/env bash
# Remove all empty directories recursively from repository
# Excludes .git, .cursor, and other hidden directories

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
cd "$ROOT_DIR" || exit 1

echo "🧹 [$(date '+%Y-%m-%d %H:%M:%S')] Scanning for empty directories..."

# Set the maximum depth to a high value to ensure we get all nested dirs
MAX_DEPTH=100

# First pass - get list of all empty directories and sort in reverse
# (process deepest directories first to handle nested empty dirs)
EMPTY_DIRS=$(find . -type d -empty -not -path "./.git*" -not -path "./.cursor*" \
    -not -path "*/\.*" -not -path "*/venv*" -not -path "*/node_modules*" \
    -maxdepth "$MAX_DEPTH" 2>/dev/null | sort -r)

# Counter for removed directories
REMOVED_COUNT=0

if [ -n "$EMPTY_DIRS" ]; then
    echo "🗑️  [$(date '+%Y-%m-%d %H:%M:%S')] Found empty directories to remove:"
    
    while IFS= read -r dir; do
        # Double-check the directory is still empty before removing
        # This handles cases where parent dirs might no longer be empty
        # after removing children
        if [ -d "$dir" ] && [ -z "$(ls -A "$dir" 2>/dev/null)" ]; then
            echo "    Removing: $dir"
            rmdir "$dir" 2>/dev/null && ((REMOVED_COUNT++)) || echo "    ⚠️ Failed to remove: $dir"
        fi
    done <<< "$EMPTY_DIRS"
    
    echo "✅ [$(date '+%Y-%m-%d %H:%M:%S')] Removed $REMOVED_COUNT empty directories"
else
    echo "✨ [$(date '+%Y-%m-%d %H:%M:%S')] No empty directories found - workspace is clean"
fi

# Second pass - check if any remain (possible if permissions issues occurred)
REMAINING=$(find . -type d -empty -not -path "./.git*" -not -path "./.cursor*" \
    -not -path "*/\.*" -not -path "*/venv*" -not -path "*/node_modules*" \
    -maxdepth "$MAX_DEPTH" 2>/dev/null)

if [ -n "$REMAINING" ]; then
    echo "⚠️ [$(date '+%Y-%m-%d %H:%M:%S')] Warning: Some empty directories could not be removed:"
    echo "$REMAINING" | sed 's/^/    /'
fi

echo "🎉 [$(date '+%Y-%m-%d %H:%M:%S')] Directory cleanup completed!" 