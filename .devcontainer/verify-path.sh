#!/bin/bash
# Script to verify PATH and installed binaries

# Display PATH
echo "Current PATH: $PATH"

# Check if ~/.local/bin is in PATH
if [[ ":$PATH:" == *":/home/vscode/.local/bin:"* ]]; then
    echo "✅ /home/vscode/.local/bin is in PATH"
else
    echo "❌ /home/vscode/.local/bin is NOT in PATH"
fi

# List binaries in ~/.local/bin
echo -e "\nBinaries in ~/.local/bin:"
ls -la /home/vscode/.local/bin/

# Check specific binaries
echo -e "\nChecking for specific binaries:"
for binary in dsm-demo-cli dsm-demo-module pytest pylint twine
do
    if command -v $binary &> /dev/null; then
        echo "✅ $binary found at $(which $binary)"
    else
        echo "❌ $binary not found in PATH"
    fi
done

# Display Python module paths
echo -e "\nPython module search paths:"
python -c "import sys; print('\n'.join(sys.path))" 