# DSM Hooks

Project-specific Claude Code hooks for data-source-manager.

## Available Hooks

### dsm-code-guard.sh (PostToolUse)

Detects silent failure patterns in Python code:

- **E722**: Bare `except:` clause
- **BLE001**: Generic `except Exception`
- **S110**: Silent `except: pass`
- **PLW1510**: subprocess without `check=True`
- **DSM-specific**: Naive `datetime.now()` without timezone
- **DSM-specific**: HTTP requests without explicit `timeout=`

## Installation

These hooks can be installed to your Claude Code settings:

```json
{
  "hooks": {
    "PostToolUse": [
      {
        "matcher": "Write|Edit",
        "hooks": [
          {
            "type": "command",
            "command": "/Users/terryli/eon/data-source-manager/.claude/hooks/dsm-code-guard.sh",
            "timeout": 5000
          }
        ]
      }
    ]
  }
}
```

Or use the global itp-hooks plugin which provides comprehensive coverage.

## Philosophy

Following the [Code Correctness Hook Policy](https://github.com/terrylica/cc-skills):

- **CHECK**: Silent failure patterns that can cause runtime bugs
- **DON'T CHECK**: Cosmetic issues like unused imports (IDE responsibility)
