# Claude Code Infrastructure

This directory contains Claude Code extensions for AI-assisted development of data-source-manager.

## Directory Structure

```
.claude/
├── settings.json    # Permission rules (team-shared)
├── settings.md      # Human-readable config documentation
├── agents/          # Specialized subagents
├── commands/        # Slash commands
├── hooks/           # Project-specific hooks
└── rules/           # Context rules
```

## Settings

**`.claude/settings.json`** - Permission rules for tool access control.

| Rule Type | Effect                          | Example            |
| --------- | ------------------------------- | ------------------ |
| allow     | Permit matching tool calls      | `Bash(uv run *)`   |
| deny      | Block regardless of other rules | `Read(.env*)`      |
| ask       | Prompt for approval             | `Bash(git push *)` |

**Key denials**:

- `.env*`, `.mise.local.toml` - Secret files
- `Bash(pip install *)` - Use uv instead
- `Bash(git push --force *)` - Dangerous git operations
- `Bash(python3.14 *)` - Wrong Python version

**Personal overrides**: Use `.claude/settings.local.json` (gitignored).

## Agents

Agents run in separate context windows for specialized tasks.

| Agent                 | Color  | Purpose                                | Tools                        |
| --------------------- | ------ | -------------------------------------- | ---------------------------- |
| api-reviewer          | red    | Reviews code for API consistency       | Read, Grep, Glob             |
| data-fetcher          | green  | Fetches data with proper FCP handling  | Read, Grep, Glob, Bash       |
| test-writer           | blue   | Writes tests following DSM patterns    | Read, Write, Edit, Bash, ... |
| silent-failure-hunter | red    | Finds silent failures and bare excepts | Read, Grep, Glob             |
| fcp-debugger          | yellow | Diagnoses FCP issues                   | Read, Grep, Glob, Bash       |

**Usage:**

```
"Use the silent-failure-hunter agent to review this code"
"Launch fcp-debugger to investigate the cache miss"
```

## Commands

Slash commands for common workflows.

| Command        | Purpose                           |
| -------------- | --------------------------------- |
| /debug-fcp     | Debug FCP behavior for a symbol   |
| /quick-test    | Run quick verification tests      |
| /review-dsm    | Review code against DSM patterns  |
| /fetch-data    | Fetch market data with validation |
| /validate-data | Validate DataFrame structure      |
| /feature-dev   | Guided feature development        |

## Rules

Context rules that Claude loads on demand when relevant.

| Rule                    | Topic                          |
| ----------------------- | ------------------------------ |
| binance-api.md          | Rate limits, error codes       |
| timestamp-handling.md   | UTC requirements, open_time    |
| dataframe-operations.md | Polars preference, OHLCV       |
| caching-patterns.md     | Cache structure, invalidation  |
| symbol-formats.md       | Market-specific symbol formats |
| error-handling.md       | Exception hierarchy, recovery  |
| fcp-protocol.md         | FCP decision logic, debugging  |

## Hooks

Project-specific hooks for code quality and safety (5 total).

| Hook                 | Event            | Purpose                                    |
| -------------------- | ---------------- | ------------------------------------------ |
| dsm-session-start.sh | SessionStart     | Load FCP context at session start          |
| dsm-skill-suggest.sh | UserPromptSubmit | Suggest relevant skills based on keywords  |
| dsm-bash-guard.sh    | PreToolUse       | Block dangerous commands before execution  |
| dsm-code-guard.sh    | PostToolUse      | Detect silent failure patterns (11 checks) |
| dsm-final-check.sh   | Stop             | Final validation at session end            |

**Blocked by PreToolUse:**

- Cache deletion (use `mise run cache:clear`)
- Python version changes
- Force push to main/master
- Direct pip install (use uv)

**Detected by PostToolUse:**

- Bare except, except Exception, except: pass
- Subprocess without check=True
- Naive datetime, HTTP without timeout
- DSM-specific patterns (symbol format, DataFrame validation)

## Related Documentation

- [CLAUDE.md](/CLAUDE.md) - Main project instructions
- [docs/INDEX.md](/docs/INDEX.md) - Documentation navigation
- [docs/skills/](/docs/skills/) - Progressive disclosure skills
