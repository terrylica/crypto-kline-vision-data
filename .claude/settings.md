# Claude Code Settings Documentation

Human-readable documentation for CKVD Claude Code configuration.

## Overview

This project uses Claude Code extensions for AI-assisted development:

- **5 Agents**: Specialized subagents for task delegation
- **6 Commands**: Slash commands for common workflows
- **4 Skills**: Progressive disclosure documentation
- **Domain context**: In nested CLAUDE.md spokes (see [src/CLAUDE.md](/src/CLAUDE.md))

## Active Configuration

### Agents

| Agent                 | Color  | Tools                  | Trigger Keywords                     |
| --------------------- | ------ | ---------------------- | ------------------------------------ |
| api-reviewer          | red    | Read, Grep, Glob       | "review", "API", "consistency"       |
| data-fetcher          | green  | Read, Grep, Glob, Bash | "fetch", "data", "market"            |
| fcp-debugger          | yellow | Read, Grep, Glob, Bash | "FCP", "failover", "cache miss"      |
| silent-failure-hunter | red    | Read, Grep, Glob       | "silent", "except", "error handling" |
| test-writer           | blue   | Read, Bash, Grep, Glob | "test", "pytest", "coverage"         |

### Commands

| Command        | Side Effects | Description                  |
| -------------- | ------------ | ---------------------------- |
| /debug-fcp     | Yes          | Run FCP diagnostic scripts   |
| /fetch-data    | Yes          | Fetch real market data       |
| /quick-test    | Yes          | Run actual tests             |
| /feature-dev   | No           | Guided feature development   |
| /review-ckvd   | No           | Review code against patterns |
| /validate-data | No           | Validate DataFrame structure |

Commands with side effects have `disable-model-invocation: true`.

### Domain Context

Domain-specific rules (Binance API, exceptions, symbols, timestamps, caching, FCP) are in [src/CLAUDE.md](/src/CLAUDE.md) — loaded on demand when working with source code.

### Skills (in docs/skills/)

| Skill            | Context | Agent   | allowed-tools          | Purpose                         |
| ---------------- | ------- | ------- | ---------------------- | ------------------------------- |
| ckvd-usage       | -       | -       | Read, Bash             | CryptoKlineVisionData API guide |
| ckvd-testing     | -       | -       | Read, Bash, Grep, Glob | Testing patterns and pytest     |
| ckvd-research    | fork    | Explore | (agent's tools)        | Codebase research (subagent)    |
| ckvd-fcp-monitor | fork    | -       | Read, Bash, Grep, Glob | FCP monitoring and diagnostics  |

## Hooks Configuration

Project hooks are in `.claude/hooks/hooks.json`:

```json
{
  "hooks": {
    "UserPromptSubmit": [
      { "matcher": ".*", "hooks": [{ "command": "ckvd-skill-suggest.sh" }] }
    ],
    "PreToolUse": [
      { "matcher": "Bash", "hooks": [{ "command": "ckvd-bash-guard.sh" }] }
    ],
    "PostToolUse": [
      {
        "matcher": "Write|Edit",
        "hooks": [{ "command": "ckvd-code-guard.sh" }]
      }
    ],
    "Stop": [{ "hooks": [{ "command": "ckvd-final-check.sh" }] }]
  }
}
```

### Skill Suggest (UserPromptSubmit)

The `ckvd-skill-suggest.sh` hook provides suggestions based on prompt keywords:

| Keyword             | Suggested Skill   |
| ------------------- | ----------------- |
| fetch, data, klines | /ckvd-usage       |
| test, pytest, mock  | /ckvd-testing     |
| FCP, cache miss     | /ckvd-fcp-monitor |
| how does, explore   | /ckvd-research    |

### Bash Guard (PreToolUse)

The `ckvd-bash-guard.sh` blocks dangerous operations:

| Blocked               | Reason                     |
| --------------------- | -------------------------- |
| Cache deletion        | Use `mise run cache:clear` |
| Python version change | CKVD requires 3.13 ONLY    |
| Force push to main    | Use feature branches       |
| Direct pip install    | Use `uv add`               |

### Code Guard (PostToolUse)

The `ckvd-code-guard.sh` hook validates:

| Check             | Pattern              | Severity |
| ----------------- | -------------------- | -------- |
| Bare except       | `except:`            | CRITICAL |
| Generic Exception | `except Exception`   | HIGH     |
| Silent pass       | `except: pass`       | CRITICAL |
| Naive datetime    | `datetime.now()`     | HIGH     |
| Missing timeout   | HTTP without timeout | HIGH     |

## Verification

Run infrastructure validation:

```bash
# Verify Claude Code setup
uv run -p 3.13 python docs/skills/ckvd-usage/scripts/validate_infrastructure.py

# Or via mise
mise run claude:validate
```

## Authoring Checklist

From [Anthropic best practices](https://platform.claude.com/docs/en/agents-and-tools/agent-skills/best-practices):

### Core Quality

- [x] Descriptions include what skill does AND when to use it
- [x] SKILL.md body under 500 lines (largest: 154 lines)
- [x] Consistent terminology throughout
- [x] File references one level deep
- [x] Third-person descriptions (not "I" or "You")
- [x] Workflow checklists for complex operations

### Structure

- [x] CLAUDE.md under 300 lines (hub: 154, largest spoke: 286)
- [x] Side-effect commands have `disable-model-invocation: true`
- [x] Skills have `user-invocable: true` and `$ARGUMENTS`
- [x] Agents have explicit `tools` field (prevents inheriting all tools)
- [x] Domain-specific CLAUDE.md files (src/, docs/, examples/, tests/) for lazy loading
- [x] 5 hooks: SessionStart, UserPromptSubmit, PreToolUse, PostToolUse, Stop

### Testing

- [x] Unit tests pass (19/19)
- [x] Infrastructure validation passes (23/23 checks)

## Related

- [CLAUDE.md](/CLAUDE.md) - Main instructions
- [.claude/README.md](README.md) - Directory overview
- [ADR](/docs/adr/2026-01-30-claude-code-infrastructure.md) - Infrastructure decision record
