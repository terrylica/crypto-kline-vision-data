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

From [Official Subagents Docs](https://code.claude.com/docs/en/sub-agents):

```yaml
---
name: agent-name
description: Use proactively when [trigger]. Performs [task].
tools: Read, Grep, Glob, Bash
disallowedTools: Write, Edit # Optional: explicitly deny tools
model: sonnet # sonnet | opus | haiku | inherit
permissionMode: plan # default | acceptEdits | dontAsk | bypassPermissions | plan
color: red # Optional: visual identifier
skills: # Optional: preload skills into agent context
  - dsm-usage
  - dsm-testing
hooks: # Optional: lifecycle hooks scoped to agent
  PostToolUse:
    - matcher: "Write|Edit"
      hooks:
        - type: command
          command: "./script.sh"
---
```

### Supported Frontmatter Fields

| Field             | Required | Description                                      |
| ----------------- | -------- | ------------------------------------------------ |
| `name`            | Yes      | Unique identifier (lowercase, hyphens)           |
| `description`     | Yes      | When Claude should delegate to this subagent     |
| `tools`           | No       | Tools allowed (inherits all if omitted)          |
| `disallowedTools` | No       | Tools to deny (removed from inherited/specified) |
| `model`           | No       | Model: `sonnet`, `opus`, `haiku`, `inherit`      |
| `permissionMode`  | No       | Permission handling mode                         |
| `skills`          | No       | Skills to preload (full content injected)        |
| `hooks`           | No       | Lifecycle hooks (PreToolUse, PostToolUse, Stop)  |
| `color`           | No       | Visual identifier in UI                          |

### Agent Features

| Agent                 | Color  | Tools              | Mode | Skills       |
| --------------------- | ------ | ------------------ | ---- | ------------ |
| api-reviewer          | red    | Read, Grep, Glob   | plan | dsm-usage    |
| data-fetcher          | green  | Read, Grep, Glob,B | -    | dsm-usage    |
| fcp-debugger          | yellow | Read, Grep, Glob,B | -    | dsm-fcp, dsm |
| silent-failure-hunter | red    | Read, Grep, Glob   | plan | dsm-usage    |
| test-writer           | blue   | Read, Bash, Grep,G | -    | dsm-testing  |

### Agent Hooks Pattern

Agent hooks run only while the agent is active and are cleaned up when it finishes.

| Event         | Matcher Input | When It Fires                 |
| ------------- | ------------- | ----------------------------- |
| `PreToolUse`  | Tool name     | Before the subagent uses tool |
| `PostToolUse` | Tool name     | After the subagent uses tool  |
| `Stop`        | (none)        | When the subagent finishes    |

**DSM agents with hooks**:

| Agent       | Hook Event  | Purpose                      |
| ----------- | ----------- | ---------------------------- |
| test-writer | PostToolUse | Run code-guard on test files |

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
argument-hint: "[symbol] [--option value]" # Optional: help text for args
allowed-tools: Bash, Read # Optional: restrict tool access
disable-model-invocation: true # For side-effect commands
---
```

### Command Tool Restrictions

| Command        | allowed-tools                | Side Effects |
| -------------- | ---------------------------- | ------------ |
| /debug-fcp     | Bash, Read                   | Yes          |
| /fetch-data    | Bash, Read                   | Yes          |
| /quick-test    | Bash                         | Yes          |
| /validate-data | Bash, Read                   | No           |
| /review-dsm    | Bash, Read, Grep, Glob       | No           |
| /feature-dev   | Read, Grep, Glob, Bash, W, E | No           |

## Skill Configuration

### Frontmatter Pattern

```yaml
---
name: skill-name
description: When Claude should use this skill. TRIGGERS - keyword1, keyword2.
argument-hint: "[arg1] [arg2]"
user-invocable: true
allowed-tools: Read, Bash, Grep # Optional: tools allowed without permission
context: fork # Optional: runs in separate context
agent: Explore # Optional: uses specific agent
adr: docs/adr/YYYY-MM-DD-related-decision.md # Optional: link to ADR
---
```

### Skill ADR Traceability

| Skill           | ADR Reference                        | Purpose             |
| --------------- | ------------------------------------ | ------------------- |
| dsm-fcp-monitor | 2025-01-30-failover-control-protocol | FCP diagnostics     |
| dsm-usage       | -                                    | General usage guide |
| dsm-testing     | -                                    | Testing patterns    |
| dsm-research    | -                                    | Codebase research   |

### Skill Self-Evolution Pattern

Skills with `adr:` field should include `references/evolution-log.md`:

```
skill-name/
├── SKILL.md
├── references/
│   └── evolution-log.md    # Reverse-chronological improvement log
└── scripts/
```

### Skill Tool Permissions

| Skill           | allowed-tools          | Purpose                    |
| --------------- | ---------------------- | -------------------------- |
| dsm-usage       | Read, Bash             | Read docs, run scripts     |
| dsm-testing     | Read, Bash, Grep, Glob | Full test workflow access  |
| dsm-research    | (inherits from agent)  | Uses Explore agent's tools |
| dsm-fcp-monitor | Read, Bash, Grep, Glob | Diagnostic and monitoring  |

### Skill Context Isolation

| Skill           | context | Reason                              |
| --------------- | ------- | ----------------------------------- |
| dsm-research    | fork    | Exploration keeps main context lean |
| dsm-fcp-monitor | fork    | Diagnostic scripts run in isolation |
| dsm-usage       | -       | Inline execution for quick fetches  |
| dsm-testing     | -       | Test results need main context      |

### $ARGUMENTS Usage

Skills support `$ARGUMENTS` placeholder for user input:

```markdown
# Skill Title

Run operation for: $ARGUMENTS

## Instructions

...
```

### Skill Quality Checklist (from Official Docs)

From [Anthropic skill authoring best practices](https://platform.claude.com/docs/en/agents-and-tools/agent-skills/best-practices):

**Core quality**:

- [x] Description includes both what the Skill does and when to use it
- [x] SKILL.md body is under 500 lines (largest: 154 lines)
- [x] Additional details are in separate files (references/)
- [x] Consistent terminology throughout
- [x] File references are one level deep
- [x] Progressive disclosure used appropriately

**DSM-specific**:

- [x] Workflow checklists for complex operations (dsm-testing)
- [x] ADR references for FCP-related skills (dsm-fcp-monitor)
- [x] Evolution-log for tracking skill improvements
- [x] Helper scripts for common operations

## Hook Configuration

### hooks.json Pattern

```json
{
  "description": "DSM-specific hooks",
  "hooks": {
    "SessionStart": [{ "hooks": [{ "command": "dsm-session-start.sh" }] }],
    "UserPromptSubmit": [
      { "matcher": ".*", "hooks": [{ "command": "dsm-skill-suggest.sh" }] }
    ],
    "PreToolUse": [
      { "matcher": "Bash", "hooks": [{ "command": "dsm-bash-guard.sh" }] }
    ],
    "PostToolUse": [
      { "matcher": "Write|Edit", "hooks": [{ "command": "dsm-code-guard.sh" }] }
    ],
    "Stop": [{ "hooks": [{ "command": "dsm-final-check.sh" }] }]
  }
}
```

### Hook Events Summary

| Event            | Purpose                    | Blocking | DSM Hook             |
| ---------------- | -------------------------- | -------- | -------------------- |
| SessionStart     | Load FCP context on start  | No       | dsm-session-start.sh |
| UserPromptSubmit | Suggest skills on prompt   | No       | dsm-skill-suggest.sh |
| PreToolUse       | Validate before execution  | Yes (2)  | dsm-bash-guard.sh    |
| PostToolUse      | Validate after file writes | No       | dsm-code-guard.sh    |
| Stop             | Final validation at end    | No       | dsm-final-check.sh   |

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

Rules use YAML frontmatter with `paths:` and optional `adr:` fields:

```yaml
---
adr: docs/adr/2025-01-30-failover-control-protocol.md # Optional: link to ADR
paths:
  - "src/data_source_manager/core/providers/binance/**/*.py"
  - "tests/integration/**/*.py"
---
# Rule Content

Guidelines for...
```

### Rule ADR Traceability

| Rule                    | ADR Reference                        |
| ----------------------- | ------------------------------------ |
| fcp-protocol.md         | 2025-01-30-failover-control-protocol |
| binance-api.md          | 2025-01-30-failover-control-protocol |
| caching-patterns.md     | 2025-01-30-failover-control-protocol |
| symbol-formats.md       | 2025-01-30-failover-control-protocol |
| timestamp-handling.md   | (general Python best practice)       |
| dataframe-operations.md | (general Python best practice)       |
| error-handling.md       | (general Python best practice)       |

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
| `src/CLAUDE.md`      | Source code patterns, classes   |
| `docs/CLAUDE.md`     | Documentation guide             |
| `examples/CLAUDE.md` | Example patterns, quick start   |
| `tests/CLAUDE.md`    | Test fixtures, mocking patterns |

## Best Practices Applied

### Context Optimization

Based on [Anthropic best practices](https://www.anthropic.com/engineering/claude-code-best-practices):

| Pattern                 | Implementation                               |
| ----------------------- | -------------------------------------------- |
| Token budgeting         | ~20k baseline, ~180k for work (documented)   |
| Positive alternatives   | Table of "Instead of X → Prefer Y" patterns  |
| CLAUDE.local.md         | Gitignored for personal preferences          |
| Document-Clear workflow | Documented in Session Management section     |
| Monorepo lazy loading   | 5 CLAUDE.md files across directory hierarchy |

### Writing Guidelines (from sources)

- **Include**: Non-obvious conventions, project-specific gotchas
- **Exclude**: Generic best practices, verbose explanations
- **Test**: "Would removing this cause Claude to make mistakes?"
- **Structure**: Project context → Code style → Commands → Gotchas

## Project Settings Configuration

### Permission Rules (settings.json)

Based on [Official Settings Docs](https://code.claude.com/docs/en/settings):

```json
{
  "permissions": {
    "allow": ["Bash(uv run *)", "Bash(mise run *)", "Bash(git *)"],
    "deny": [
      "Read(.env*)",
      "Read(.mise.local.toml)",
      "Bash(pip install *)",
      "Bash(git push --force *)"
    ]
  }
}
```

### Permission Rule Evaluation

| Priority | Rule Type | Effect                          |
| -------- | --------- | ------------------------------- |
| 1        | deny      | Block regardless of other rules |
| 2        | ask       | Prompt for approval             |
| 3        | allow     | Permit if matched               |

### Settings File Hierarchy

| Scope   | File                          | Purpose                 |
| ------- | ----------------------------- | ----------------------- |
| Managed | System directories (admin)    | IT-controlled settings  |
| Local   | `.claude/settings.local.json` | Personal (gitignored)   |
| Project | `.claude/settings.json`       | Team-shared (committed) |
| User    | `~/.claude/settings.json`     | Personal global         |

### Plugin Marketplace Configuration

From [Official Plugin Marketplace Docs](https://code.claude.com/docs/en/plugin-marketplaces):

```json
{
  "extraKnownMarketplaces": {
    "cc-skills": {
      "source": {
        "source": "github",
        "repo": "terrylica/cc-skills"
      }
    }
  }
}
```

Team members are automatically prompted to install the marketplace when they trust the project folder.

## Verification Checklist

- [ ] CLAUDE.md is under 300 lines
- [ ] All agents have tools field
- [ ] Side-effect commands have disable-model-invocation
- [ ] Skills have user-invocable and $ARGUMENTS
- [ ] hooks.json uses ${CLAUDE_PROJECT_ROOT}
- [ ] All @ imports point to existing files
- [ ] Context rules cover all DSM domains
- [ ] Domain-specific CLAUDE.md in examples/, tests/, src/
- [ ] CLAUDE.local.md in .gitignore
