# Final Steps for Renaming Project

The codebase has been updated to use the new project name "data-source-manager". The following manual steps need to be completed to finalize the process:

## 1. Recreate Virtual Environment

The virtual environment often contains paths and references related to the old project name. Remove and recreate it:

```bash
# Remove existing virtual environment
rm -rf .venv

# Create a new virtual environment
python3 -m venv .venv

# Activate the virtual environment
source .venv/bin/activate

# Install the package in development mode
pip install -e ".[dev]"
```

## 2. Rename Remote GitHub Repository

Go to the GitHub repository settings and rename the repository:

1. Navigate to https://github.com/Eon-Labs/raw-data-services
2. Go to Settings > General
3. Under "Repository name", change `raw-data-services` to `data-source-manager`
4. Click "Rename"

## 3. Update Local Git Remote URL

After renaming the repository on GitHub, update the remote URL in your local repository:

```bash
git remote set-url origin git@github.com:Eon-Labs/data-source-manager.git
```

Verify that the remote URL has been updated:

```bash
git remote -v
```

## 4. Final Review and Test

Run the project's tests to ensure everything is working correctly:

```bash
# Run all tests
pytest

# Try running one of the demo scripts
dsm-demo-cli --help
```

If all tests pass and the CLI tools work as expected, the renaming process is complete!
