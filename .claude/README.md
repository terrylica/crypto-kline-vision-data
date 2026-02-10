# Claude Code Infrastructure

This directory contains Claude Code extensions for AI-assisted development of crypto-kline-vision-data.

## Directory Structure

```
.claude/
├── settings.json    # Permission rules (team-shared)
├── settings.md      # Human-readable config documentation
├── agents/          # Specialized subagents
├── commands/        # Slash commands
└── hooks/           # Project-specific hooks
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
- `Bash(python3.14 *)`, `Bash(python3.12 *)` - Wrong Python version

**Personal overrides**: Use `.claude/settings.local.json` (gitignored).

**Plugin marketplace**: `cc-skills` is configured via `extraKnownMarketplaces`.

## Agents

Agents run in separate context windows for specialized tasks.

| Agent                 | Color  | Purpose                                | Tools                        |
| --------------------- | ------ | -------------------------------------- | ---------------------------- |
| api-reviewer          | red    | Reviews code for API consistency       | Read, Grep, Glob             |
| data-fetcher          | green  | Fetches data with proper FCP handling  | Read, Grep, Glob, Bash       |
| test-writer           | blue   | Writes tests following CKVD patterns   | Read, Write, Edit, Bash, ... |
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
| /review-ckvd   | Review code against CKVD patterns |
| /fetch-data    | Fetch market data with validation |
| /validate-data | Validate DataFrame structure      |
| /feature-dev   | Guided feature development        |

## Domain Context

Domain-specific rules (Binance API, exceptions, symbols, timestamps, caching, FCP) are in [src/CLAUDE.md](/src/CLAUDE.md) — loaded on demand when working with source code.

## Hooks

Project-specific hooks for code quality and safety (5 total).

| Hook                  | Event            | Purpose                                    |
| --------------------- | ---------------- | ------------------------------------------ |
| ckvd-session-start.sh | SessionStart     | Load FCP context at session start          |
| ckvd-skill-suggest.sh | UserPromptSubmit | Suggest relevant skills based on keywords  |
| ckvd-bash-guard.sh    | PreToolUse       | Block dangerous commands before execution  |
| ckvd-code-guard.sh    | PostToolUse      | Detect silent failure patterns (11 checks) |
| ckvd-final-check.sh   | Stop             | Final validation at session end            |

**Blocked by PreToolUse:**

- Cache deletion (use `mise run cache:clear`)
- Python version changes
- Force push to main/master
- Direct pip install (use uv)

**Detected by PostToolUse:**

- Bare except, except Exception, except: pass
- Subprocess without check=True
- Naive datetime, HTTP without timeout
- CKVD-specific patterns (symbol format, DataFrame validation)

## Architecture Pattern

This infrastructure follows the **CKVD Claude Code Infrastructure Pattern**, which can be adopted by other projects.

### Component Hierarchy

```
.claude/                    # Claude Code extensions (team-shared)
├── settings.json           # Permission rules (committed)
├── settings.local.json     # Personal overrides (gitignored)
├── agents/                 # 5 specialized subagents
│   ├── {name}.md           # YAML frontmatter + instructions
│   └── ...
├── commands/               # 6 slash commands
│   ├── {name}.md           # YAML frontmatter + workflow
│   └── ...
└── hooks/                  # 5 lifecycle hooks
    ├── hooks.json          # Configuration with descriptions
    ├── {name}.sh           # Executable scripts
    └── README.md           # Hook documentation
```

Domain-specific context (Binance API, FCP, exceptions, symbols, timestamps, caching) lives in nested CLAUDE.md spokes — see [src/CLAUDE.md](/src/CLAUDE.md).

### Adoption Checklist

| Component     | Required | Purpose                           |
| ------------- | -------- | --------------------------------- |
| settings.json | Yes      | Team permission rules             |
| agents/       | Optional | Specialized task delegation       |
| commands/     | Optional | Repeatable workflows              |
| hooks/        | Optional | Automation and guards             |
| CLAUDE.md     | Yes      | Hub-and-spoke progressive context |

### Design Principles

1. **Separation of concerns**: Each component has single responsibility
2. **Team-shareable**: Commit settings.json, gitignore settings.local.json
3. **Progressive disclosure**: Domain context in nested CLAUDE.md spokes, loaded on demand
4. **Safety first**: Deny rules for secrets, dangerous operations

## Related Documentation

- [CLAUDE.md](/CLAUDE.md) - Main project instructions
- [docs/INDEX.md](/docs/INDEX.md) - Documentation navigation
- [docs/skills/](/docs/skills/) - Progressive disclosure skills
- [ADR](/docs/adr/2026-01-30-claude-code-infrastructure.md) - Infrastructure decision record
