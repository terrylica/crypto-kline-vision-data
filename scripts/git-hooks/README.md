# Git Hooks

This directory contains Git hooks to help maintain code quality in the repository.

## Available Hooks

### pre-commit

The `pre-commit` hook runs before each commit and performs the following actions:

- Removes unused imports from Python files using `autoflake`
- Prevents commits if unused imports are found, requiring you to stage the cleaned files

## Installation

You can install the hooks in two ways:

### 1. Automatic Installation

Run the installation script:

```bash
./scripts/install-git-hooks.sh
```

### 2. Manual Installation

To install hooks manually:

1. Copy the desired hook to your `.git/hooks` directory:

   ```bash
   cp scripts/git-hooks/pre-commit .git/hooks/pre-commit
   ```

2. Make it executable:
   ```bash
   chmod +x .git/hooks/pre-commit
   ```

## Compatibility

These hooks work with:

- Command line `git commit`
- Source Control UI commit button in VS Code/Cursor
- Any other Git client that respects the standard Git hooks mechanism

## Skipping Hooks

If you need to skip the hooks for a specific commit:

```bash
git commit --no-verify -m "Your commit message"
```

Or in VS Code/Cursor Source Control UI, use the "..." menu and select "Commit (Skip Hooks)".
