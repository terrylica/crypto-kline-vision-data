# Claude Code Infrastructure

This directory contains Claude Code extensions for AI-assisted development of data-source-manager.

## Directory Structure

```
.claude/
├── agents/          # Specialized subagents
├── commands/        # Slash commands
├── hooks/           # Project-specific hooks
└── rules/           # Context rules
```

## Agents

Agents run in separate context windows for specialized tasks.

| Agent                 | Purpose                                | Tools                         |
| --------------------- | -------------------------------------- | ----------------------------- |
| api-reviewer          | Reviews code for API consistency       | Read, Grep, Glob              |
| data-fetcher          | Fetches data with proper FCP handling  | Read, Grep, Glob, Bash        |
| test-writer           | Writes tests following DSM patterns    | Read, Grep, Glob, Edit, Write |
| silent-failure-hunter | Finds silent failures and bare excepts | Read, Grep, Glob              |
| fcp-debugger          | Diagnoses FCP issues                   | Read, Grep, Glob, Bash        |

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

Project-specific hooks for code quality and safety.

| Hook                 | Event            | Purpose                                    |
| -------------------- | ---------------- | ------------------------------------------ |
| dsm-skill-suggest.sh | UserPromptSubmit | Suggest relevant skills based on keywords  |
| dsm-bash-guard.sh    | PreToolUse       | Block dangerous commands before execution  |
| dsm-code-guard.sh    | PostToolUse      | Detect silent failure patterns (11 checks) |

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
