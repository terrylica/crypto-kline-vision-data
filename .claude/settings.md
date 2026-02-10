# Claude Code Settings Documentation

Human-readable documentation for CKVD Claude Code configuration.

## Overview

This project uses Claude Code extensions for AI-assisted development:

- **5 Agents**: Specialized subagents for task delegation
- **2 Commands**: Slash commands for common workflows
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

| Command      | Side Effects | Description                  |
| ------------ | ------------ | ---------------------------- |
| /feature-dev | No           | Guided feature development   |
| /review-ckvd | No           | Review code against patterns |

### Domain Context

Domain-specific rules (Binance API, exceptions, symbols, timestamps, caching, FCP) are in [src/CLAUDE.md](/src/CLAUDE.md) — loaded on demand when working with source code.

### Skills (in docs/skills/)

| Skill            | Context | Agent   | allowed-tools          | Purpose                         |
| ---------------- | ------- | ------- | ---------------------- | ------------------------------- |
| ckvd-usage       | -       | -       | Read, Bash             | CryptoKlineVisionData API guide |
| ckvd-testing     | -       | -       | Read, Bash, Grep, Glob | Testing patterns and pytest     |
| ckvd-research    | fork    | Explore | (agent's tools)        | Codebase research (subagent)    |
| ckvd-fcp-monitor | fork    | -       | Read, Bash, Grep, Glob | FCP monitoring and diagnostics  |

## Permissions

Configured in `settings.json`:

### Allow Rules

All standard tools are allowed: Bash, Edit, Write, Read, Grep, Glob, Task, WebFetch, WebSearch.

### Deny Rules

| Rule                       | Reason                  |
| -------------------------- | ----------------------- |
| `Read(.env*)`              | Secret files            |
| `Read(.mise.local.toml)`   | Secret files            |
| `Read(**/*.key)`           | Key files               |
| `Read(**/secrets/**)`      | Secret directories      |
| `Read(**/*credential*)`    | Credential files        |
| `Read(~/.ssh/id_*)`        | SSH keys                |
| `Bash(rm -rf *)`           | Dangerous deletion      |
| `Bash(sudo *)`             | Privilege escalation    |
| `Bash(pip install *)`      | Use uv instead          |
| `Bash(git push --force *)` | Use feature branches    |
| `Bash(git reset --hard *)` | Dangerous git operation |
| `Bash(python3.14 *)`       | Wrong Python version    |
| `Bash(python3.12 *)`       | Wrong Python version    |
| `Bash(python3.11 *)`       | Wrong Python version    |

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
- [x] Skills have `user-invocable: true` and `$ARGUMENTS`
- [x] Agents have explicit `tools` field (prevents inheriting all tools)
- [x] Domain-specific CLAUDE.md files (src/, docs/, examples/, tests/) for lazy loading

## Related

- [CLAUDE.md](/CLAUDE.md) - Main instructions
- [ADR](/docs/adr/2026-01-30-claude-code-infrastructure.md) - Infrastructure decision record
