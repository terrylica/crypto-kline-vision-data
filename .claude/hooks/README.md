# DSM Hooks

Project-specific Claude Code hooks for data-source-manager.

## Available Hooks

### dsm-skill-suggest.sh (UserPromptSubmit)

Analyzes user prompts and suggests relevant DSM skills based on keywords.

**Trigger Keywords:**

| Skill           | Keywords                                          |
| --------------- | ------------------------------------------------- |
| dsm-usage       | fetch, data, klines, OHLCV, market data, Binance  |
| dsm-testing     | test, pytest, mock, fixture, coverage             |
| dsm-fcp-monitor | FCP, failover, cache miss/hit, slow, diagnos      |
| dsm-research    | how does, understand, find, explore, architecture |

**Behavior**: Provides suggestions via feedback (non-blocking).

### dsm-bash-guard.sh (PreToolUse)

Validates bash commands BEFORE execution for DSM-specific safety.

**Blocked Operations (exit code 2):**

| Pattern                               | Reason                             |
| ------------------------------------- | ---------------------------------- |
| `rm -rf .cache/data_source_manager`   | Use `mise run cache:clear` instead |
| Python version changes via pyenv/mise | DSM requires Python 3.13 ONLY      |
| Force push to main/master             | Use feature branches               |
| Direct `pip install`                  | Use `uv add <package>`             |
| `git reset --hard` without ref        | Dangerous without explicit commit  |

**Warnings:**

- pytest without `uv run -p 3.13`

### dsm-code-guard.sh (PostToolUse)

Detects silent failure patterns in Python code AFTER file writes.

**General Python Patterns:**

- **E722**: Bare `except:` clause
- **BLE001**: Generic `except Exception`
- **S110**: Silent `except: pass`
- **PLW1510**: subprocess without `check=True`

**DSM-Specific Patterns:**

- Naive `datetime.now()` without timezone
- HTTP requests without explicit `timeout=`
- `DataSourceManager.create()` without `manager.close()`
- Async functions using sync `DataSourceManager` (mixing patterns)
- COIN-margined symbol format (`_PERP`) with wrong market type
- Returning DataFrame without validation (`len(df) > 0` or `df.empty`)
- Using Pandas when Polars preferred (informational, use `# polars-exception` to suppress)

## Hook Configuration

The hooks are configured in `hooks.json`:

```json
{
  "hooks": {
    "UserPromptSubmit": [
      { "matcher": ".*", "hooks": [{ "command": "dsm-skill-suggest.sh" }] }
    ],
    "PreToolUse": [
      { "matcher": "Bash", "hooks": [{ "command": "dsm-bash-guard.sh" }] }
    ],
    "PostToolUse": [
      { "matcher": "Write|Edit", "hooks": [{ "command": "dsm-code-guard.sh" }] }
    ]
  }
}
```

## Exit Codes

| Code | Meaning                                   |
| ---- | ----------------------------------------- |
| 0    | Allow execution                           |
| 2    | Block execution (sends message to Claude) |

## Code Correctness Philosophy

**Principle**: Only check for **silent failure patterns** - code that fails without visible errors.

### Rules Checked (Runtime Bugs)

| Rule    | Pattern                    | Why Checked                      |
| ------- | -------------------------- | -------------------------------- |
| E722    | Bare `except:`             | Swallows SystemExit, KeyboardInt |
| S110    | `except: pass`             | Silent failure, data integrity   |
| BLE001  | `except Exception`         | Too broad, hides specific errors |
| PLW1510 | subprocess without `check` | Silent command failures          |

### Rules NOT Checked (Cosmetic)

| Rule | Pattern          | Why NOT Checked                     |
| ---- | ---------------- | ----------------------------------- |
| F401 | Unused imports   | Development-in-progress, re-exports |
| F841 | Unused variables | IDE responsibility                  |
| I    | Import sorting   | Pre-commit/IDE handles              |
| E/W  | PEP8 style       | Not a runtime issue                 |

### Rationale

**Unused imports are NOT checked** because:

1. Development-in-progress (imports before code)
2. Intentional re-exports (`__init__.py`)
3. Type-only imports (`TYPE_CHECKING`)
4. IDE/pre-commit responsibility, not interactive hooks

Following [cc-skills Code Correctness Policy](https://github.com/terrylica/cc-skills/blob/main/plugins/itp-hooks/CLAUDE.md#code-correctness-philosophy)

## Related

- [Hooks Reference](https://code.claude.com/docs/en/hooks)
- [Claude Code Hooks Best Practices](https://www.datacamp.com/tutorial/claude-code-hooks)
