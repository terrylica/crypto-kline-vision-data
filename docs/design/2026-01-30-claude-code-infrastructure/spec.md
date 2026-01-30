---
adr: 2026-01-30-claude-code-infrastructure
source: session-continuation
implementation-status: completed
phase: phase-3
last-updated: 2026-01-30
---

# Claude Code Infrastructure Implementation Specification

**ADR**: [Claude Code Infrastructure](/docs/adr/2026-01-30-claude-code-infrastructure.md)

## Overview

Implementation details for the Claude Code infrastructure enabling AI-assisted development.

## Directory Structure

```
data-source-manager/
├── CLAUDE.md                      # Main instructions (<300 lines)
├── examples/
│   └── CLAUDE.md                  # Example-specific context (lazy loaded)
├── tests/
│   └── CLAUDE.md                  # Test-specific context (lazy loaded)
├── .claude/
│   ├── agents/                    # Specialized subagents
│   │   ├── api-reviewer.md
│   │   ├── data-fetcher.md
│   │   ├── fcp-debugger.md
│   │   ├── silent-failure-hunter.md
│   │   └── test-writer.md
│   ├── commands/                  # Slash commands
│   │   ├── debug-fcp.md
│   │   ├── feature-dev.md
│   │   ├── fetch-data.md
│   │   ├── quick-test.md
│   │   ├── review-dsm.md
│   │   └── validate-data.md
│   ├── hooks/                     # Project-specific hooks
│   │   ├── dsm-code-guard.sh
│   │   ├── hooks.json
│   │   └── README.md
│   ├── rules/                     # Context rules
│   │   ├── binance-api.md
│   │   ├── caching-patterns.md
│   │   ├── dataframe-operations.md
│   │   ├── error-handling.md
│   │   ├── fcp-protocol.md
│   │   ├── symbol-formats.md
│   │   └── timestamp-handling.md
│   └── README.md
├── docs/
│   ├── skills/                    # Progressive disclosure
│   │   ├── dsm-usage/
│   │   │   ├── SKILL.md
│   │   │   ├── examples/
│   │   │   ├── references/
│   │   │   └── scripts/
│   │   ├── dsm-testing/
│   │   ├── dsm-research/
│   │   └── dsm-fcp-monitor/
│   ├── adr/                       # Architectural decisions
│   └── design/                    # Implementation specs
└── examples/                      # Runnable examples
```

## Agent Configuration

### Frontmatter Pattern

```yaml
---
name: agent-name
description: Use proactively when [trigger]. Performs [task].
tools: Read, Grep, Glob, Bash
model: sonnet
permissionMode: plan # Optional: plan, default, acceptEdits, dontAsk
skills: # Optional: preload skills into agent context
  - dsm-usage
  - dsm-testing
---
```

### Agent Features

| Agent                 | Read | Grep | Glob | Bash | Mode | Skills       |
| --------------------- | ---- | ---- | ---- | ---- | ---- | ------------ |
| api-reviewer          | ✓    | ✓    | ✓    | -    | plan | dsm-usage    |
| data-fetcher          | ✓    | ✓    | ✓    | ✓    | -    | dsm-usage    |
| fcp-debugger          | ✓    | ✓    | ✓    | ✓    | -    | dsm-fcp, dsm |
| silent-failure-hunter | ✓    | ✓    | ✓    | -    | plan | dsm-usage    |
| test-writer           | ✓    | ✓    | ✓    | ✓    | -    | dsm-testing  |

### Description Best Practices

Include "Use proactively" in descriptions to encourage automatic delegation:

```yaml
# GOOD: Proactive trigger
description: Use proactively after writing code. Reviews for anti-patterns.

# GOOD: Specific trigger
description: Use when debugging FCP issues - empty DataFrames, cache misses.

# LESS EFFECTIVE: No trigger guidance
description: Reviews code for quality.
```

## Command Configuration

### Frontmatter Pattern

```yaml
---
name: command-name
description: What this command does
disable-model-invocation: true # For side-effect commands
---
```

### Side-Effect Commands

Commands with `disable-model-invocation: true`:

- `/quick-test` - Runs actual tests
- `/fetch-data` - Fetches real market data
- `/debug-fcp` - Runs diagnostic scripts

## Skill Configuration

### Frontmatter Pattern

```yaml
---
name: skill-name
description: When Claude should use this skill
argument-hint: "[arg1] [arg2]"
user-invocable: true
context: fork # Optional: runs in separate context
agent: Explore # Optional: uses specific agent
---
```

### $ARGUMENTS Usage

Skills support `$ARGUMENTS` placeholder for user input:

```markdown
# Skill Title

Run operation for: $ARGUMENTS

## Instructions

...
```

## Hook Configuration

### hooks.json Pattern

```json
{
  "description": "DSM-specific hooks",
  "hooks": {
    "PostToolUse": [
      {
        "matcher": "Write|Edit",
        "hooks": [
          {
            "type": "command",
            "command": "${CLAUDE_PROJECT_ROOT}/.claude/hooks/dsm-code-guard.sh",
            "timeout": 5000
          }
        ]
      }
    ]
  }
}
```

### DSM Code Guard Checks

| Check             | Pattern                     | Severity |
| ----------------- | --------------------------- | -------- |
| Bare except       | `except:`                   | CRITICAL |
| Generic Exception | `except Exception`          | HIGH     |
| Silent pass       | `except: pass`              | CRITICAL |
| Missing timeout   | HTTP without timeout        | HIGH     |
| Naive datetime    | `datetime.now()` without tz | HIGH     |
| Missing close     | DSM without `close()`       | MEDIUM   |
| Sync/async mix    | async + sync DSM            | MEDIUM   |
| Wrong symbol      | `_PERP` with SPOT           | HIGH     |

## Context Rules

### Path-Specific Frontmatter

Rules use YAML frontmatter with `paths:` field for conditional loading:

```yaml
---
paths:
  - "src/data_source_manager/core/providers/binance/**/*.py"
  - "tests/integration/**/*.py"
---
# Rule Content

Guidelines for...
```

### Rule Path Mappings

| Rule                    | Path Patterns                                |
| ----------------------- | -------------------------------------------- |
| binance-api.md          | `providers/binance/**`, `tests/integration`  |
| timestamp-handling.md   | `src/**`, `examples/**`, `tests/**`          |
| dataframe-operations.md | `src/**`, `examples/**`, `tests/**`          |
| caching-patterns.md     | `core/sync/**`, `core/cache/**`              |
| symbol-formats.md       | `providers/binance/**`, `market_constraints` |
| error-handling.md       | `src/**`, `tests/**`                         |
| fcp-protocol.md         | `core/sync/**`, `core/providers/**`          |

### When Rules Load

Rules load automatically when Claude works with matching files:

| Rule                    | Also Triggered By      |
| ----------------------- | ---------------------- |
| binance-api.md          | API calls, rate limits |
| timestamp-handling.md   | datetime, timezone     |
| dataframe-operations.md | DataFrame, Polars      |
| caching-patterns.md     | cache, Arrow files     |
| symbol-formats.md       | symbol validation      |
| error-handling.md       | exceptions, try/except |
| fcp-protocol.md         | FCP, failover          |

## Monorepo-Style Loading

Claude Code loads CLAUDE.md files using a hierarchical strategy:

### Ancestor Loading (Upward)

When Claude Code starts, it walks up from cwd to root, loading all CLAUDE.md files:

```
/Users/user/eon/data-source-manager/tests/unit/
    ↑ loads tests/CLAUDE.md
    ↑ loads CLAUDE.md (root)
```

### Descendant Loading (Downward)

Subdirectory CLAUDE.md files load lazily when working with files in those directories.

### Content Placement

| File Location        | Contains                        |
| -------------------- | ------------------------------- |
| `CLAUDE.md`          | Project-wide conventions, FCP   |
| `examples/CLAUDE.md` | Example patterns, quick start   |
| `tests/CLAUDE.md`    | Test fixtures, mocking patterns |

## Verification Checklist

- [ ] CLAUDE.md is under 300 lines
- [ ] All agents have tools field
- [ ] Side-effect commands have disable-model-invocation
- [ ] Skills have user-invocable and $ARGUMENTS
- [ ] hooks.json uses ${CLAUDE_PROJECT_ROOT}
- [ ] All @ imports point to existing files
- [ ] Context rules cover all DSM domains
- [ ] Domain-specific CLAUDE.md in examples/ and tests/
