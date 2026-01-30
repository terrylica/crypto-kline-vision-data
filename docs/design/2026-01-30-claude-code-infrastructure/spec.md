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

### Permission Modes

Based on [Official Subagents Docs](https://code.claude.com/docs/en/sub-agents) and [Claude Code Permissions Guide](https://www.eesel.ai/blog/claude-code-permissions):

| Mode                | Behavior                                             | Use Case               |
| ------------------- | ---------------------------------------------------- | ---------------------- |
| `default`           | Prompt for approval on file writes                   | Most agents            |
| `acceptEdits`       | Accept all edits, prompt for bash                    | Trusted edit agents    |
| `plan`              | Read-only, no file writes allowed                    | Analysis/review agents |
| `bypassPermissions` | Skip all permission checks (requires explicit allow) | CI/automation only     |

**Best practices**:

- Use `plan` for read-only agents (api-reviewer, silent-failure-hunter)
- Use `default` or `acceptEdits` for agents that write files
- Avoid `bypassPermissions` unless in controlled CI environments
- Use `disallowedTools` (not `tools`) for reliable tool restrictions

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

### Multi-Agent Orchestration Patterns

Based on [Anthropic Agent SDK](https://www.anthropic.com/engineering/building-agents-with-the-claude-agent-sdk) and [Task Tool Guide](https://dev.to/bhaidar/the-task-tool-claude-codes-agent-orchestration-system-4bf2):

**Built-in subagents**:

| Agent           | Purpose                          | Context  |
| --------------- | -------------------------------- | -------- |
| Explore         | Fast, read-only codebase search  | Isolated |
| Plan            | Design implementation strategies | Isolated |
| general-purpose | Multi-step autonomous tasks      | Full     |

**Orchestration patterns**:

| Pattern    | Description                        | Use Case                  |
| ---------- | ---------------------------------- | ------------------------- |
| Fan-Out    | Spawn multiple agents in parallel  | Independent searches      |
| Pipeline   | Sequential agent handoffs          | Dependent transformations |
| Map-Reduce | Distribute work, aggregate results | Large-scale analysis      |

**Subagent benefits**:

1. **Parallelization**: Multiple agents work on different tasks simultaneously
2. **Context isolation**: Subagents use own context window, return only relevant info
3. **Specialization**: Each agent optimized for specific task types

**DSM agent orchestration**:

| Scenario        | Pattern    | Agents Used                     |
| --------------- | ---------- | ------------------------------- |
| Code review     | Pipeline   | api-reviewer → silent-failure   |
| Data validation | Fan-Out    | data-fetcher (parallel symbols) |
| Test coverage   | Map-Reduce | test-writer per module          |

## Command Configuration

Based on [Official Slash Commands Docs](https://code.claude.com/docs/en/slash-commands) and [Claude Code Guide](https://www.eesel.ai/blog/slash-commands-claude-code).

### Commands vs Skills

| Feature                | `.claude/commands/`           | `.claude/skills/`                   |
| ---------------------- | ----------------------------- | ----------------------------------- |
| File structure         | Single `.md` file             | Directory + SKILL.md                |
| Supporting files       | No                            | scripts/, references/, examples/    |
| Claude auto-invocation | No                            | Yes (configurable)                  |
| Filename = command     | Yes (`commit.md` → `/commit`) | Yes (`review/SKILL.md` → `/review`) |

If both exist with same name, they work identically. Use skills for complex workflows needing supporting files.

### Frontmatter Pattern

```yaml
---
name: command-name
description: What this command does
argument-hint: "[symbol] [--option value]" # Optional: help text for args
allowed-tools: Bash, Read # Optional: restrict tool access
model: haiku # Optional: faster model for simple tasks
disable-model-invocation: true # For side-effect commands
---
```

### Model Selection for Commands

| Model    | Use Case                               |
| -------- | -------------------------------------- |
| `haiku`  | Quick fixes, simple formatting         |
| `sonnet` | Standard development tasks (default)   |
| `opus`   | Complex reasoning, architecture design |

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

From [Official Skills Docs](https://code.claude.com/docs/en/skills):

### Directory Structure

```
skill-name/
├── SKILL.md           # Main instructions (required, under 500 lines)
├── scripts/           # Executable Python/Bash scripts
├── references/        # Documentation loaded into context on demand
└── examples/          # Example output showing expected format
```

### Frontmatter Pattern

```yaml
---
name: skill-name
description: When Claude should use this skill. TRIGGERS - keyword1, keyword2.
argument-hint: "[arg1] [arg2]"
user-invocable: true # Show in /slash-command menu (default: true)
disable-model-invocation: true # Prevent Claude auto-triggering (default: false)
allowed-tools: Read, Bash, Grep # Tools allowed without permission
context: fork # Run in forked subagent context
agent: Explore # Subagent type when context: fork
model: sonnet # Model override for this skill
hooks: # Skill-scoped lifecycle hooks
  PostToolUse:
    - matcher: "Write"
      hooks: [{ "command": "./scripts/validate.sh" }]
adr: docs/adr/YYYY-MM-DD-related-decision.md # Link to ADR
---
```

### Supported Frontmatter Fields

| Field                      | Required    | Description                                       |
| -------------------------- | ----------- | ------------------------------------------------- |
| `name`                     | No          | Display name (defaults to directory name)         |
| `description`              | Recommended | What skill does and when to use it (for triggers) |
| `argument-hint`            | No          | Hint for autocomplete: `[issue-number]`           |
| `disable-model-invocation` | No          | Only user can invoke (for side-effect workflows)  |
| `user-invocable`           | No          | Hide from `/` menu (for background knowledge)     |
| `allowed-tools`            | No          | Tools Claude can use without permission           |
| `model`                    | No          | Model override for skill execution                |
| `context`                  | No          | Set to `fork` for subagent execution              |
| `agent`                    | No          | Subagent type: `Explore`, `Plan`, custom          |
| `hooks`                    | No          | Skill-scoped lifecycle hooks                      |

### String Substitutions

| Variable               | Description                                |
| ---------------------- | ------------------------------------------ |
| `$ARGUMENTS`           | All arguments passed when invoking skill   |
| `$ARGUMENTS[N]`        | Specific argument by 0-based index         |
| `$0`, `$1`, `$N`       | Shorthand for `$ARGUMENTS[N]`              |
| `${CLAUDE_SESSION_ID}` | Current session ID for logging/correlation |

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

Following [cc-skills hooks pattern](https://github.com/terrylica/cc-skills) with descriptions and notes:

```json
{
  "description": "DSM-specific hooks - enforces FCP patterns, silent failure detection",
  "notes": [
    "SessionStart: Loads FCP context into every session",
    "PreToolUse: BLOCKS dangerous operations (cache deletion, wrong Python)",
    "PostToolUse: WARNS about silent failure patterns (bare except, no timeout)"
  ],
  "hooks": {
    "SessionStart": [
      {
        "description": "Load FCP context at session start",
        "hooks": [{ "command": "dsm-session-start.sh", "timeout": 2000 }]
      }
    ],
    "PreToolUse": [
      {
        "description": "Block dangerous Bash commands",
        "matcher": "Bash",
        "hooks": [{ "command": "dsm-bash-guard.sh", "timeout": 3000 }]
      }
    ],
    "PostToolUse": [
      {
        "description": "Detect silent failure patterns",
        "matcher": "Write|Edit",
        "hooks": [{ "command": "dsm-code-guard.sh", "timeout": 5000 }]
      }
    ]
  }
}
```

### hooks.json Fields

| Field         | Purpose                                 |
| ------------- | --------------------------------------- |
| `description` | Top-level: overall hook purpose         |
| `notes`       | Array of quick-reference behavior notes |
| `description` | Per-hook: what this specific hook does  |
| `matcher`     | Regex pattern to match tools/events     |
| `timeout`     | Max execution time in milliseconds      |

### Hook Events Summary

| Event            | Purpose                    | Blocking | DSM Hook             |
| ---------------- | -------------------------- | -------- | -------------------- |
| SessionStart     | Load FCP context on start  | No       | dsm-session-start.sh |
| UserPromptSubmit | Suggest skills on prompt   | No       | dsm-skill-suggest.sh |
| PreToolUse       | Validate before execution  | Yes (2)  | dsm-bash-guard.sh    |
| PostToolUse      | Validate after file writes | No       | dsm-code-guard.sh    |
| Stop             | Final validation at end    | No       | dsm-final-check.sh   |

### Hook Exit Codes

Based on [Official Hooks Docs](https://code.claude.com/docs/en/hooks) and [DataCamp Tutorial](https://www.datacamp.com/tutorial/claude-code-hooks):

| Exit Code | Behavior                                             | Output Channel  |
| --------- | ---------------------------------------------------- | --------------- |
| 0         | Success, execution continues                         | stdout → user   |
| 2         | Blocking error, halts action (PreToolUse)            | stderr → Claude |
| Other     | Non-blocking error, execution continues with warning | stderr → user   |

### Hook JSON Output

Hooks can return structured JSON for sophisticated control:

```json
{
  "continue": true, // Whether Claude continues after hook
  "stopReason": "string", // Message shown when continue is false
  "suppressOutput": true, // Hide stdout from transcript mode
  "systemMessage": "string" // Warning message shown to user
}
```

**Priority order**: `continue: false` > JSON `"decision": "block"` > exit code 2

### Matcher Patterns

| Pattern           | Matches                    |
| ----------------- | -------------------------- |
| `Write`           | Exact tool match           |
| `Write\|Edit`     | Multiple tools (OR)        |
| `*` or empty      | All tools                  |
| `Bash(npm test*)` | Tool with argument pattern |
| `mcp__memory__.*` | MCP tools with regex       |

Note: Matchers are case-sensitive. `bash` won't match `Bash`.

### Advanced Hook Output Control

Based on [cc-skills Lifecycle Reference](https://github.com/terrylica/cc-skills):

| JSON Field           | Effect                                               |
| -------------------- | ---------------------------------------------------- |
| `additionalContext`  | Inject text into Claude's context (SessionStart)     |
| `updatedInput`       | Rewrite tool arguments before execution (PreToolUse) |
| `permissionDecision` | `allow\|deny\|ask` for nuanced permission handling   |
| `decision: block`    | Soft block with feedback to Claude (PostToolUse)     |

**Visibility patterns**:

| Output Type         | User Sees | Claude Sees |
| ------------------- | --------- | ----------- |
| Plain text stdout   | Yes       | No          |
| `systemMessage`     | Yes       | No          |
| `additionalContext` | No        | Yes         |
| `decision: block`   | Yes       | Yes         |

### Additional Hook Events

| Event              | Purpose                         | Use Case                  |
| ------------------ | ------------------------------- | ------------------------- |
| PostToolUseFailure | Fires when tool execution fails | Recovery/fallback logic   |
| PreCompact         | Fires before context compaction | Backup/archival workflows |

### Timeout Guidelines

| Hook Type      | Recommended Timeout | Reason                   |
| -------------- | ------------------- | ------------------------ |
| Validation     | 5000ms (5s)         | Quick syntax checks      |
| Git operations | 15000ms (15s)       | Network latency          |
| Network calls  | 30000ms (30s)       | External API variability |

**Note**: Timeouts are in milliseconds (e.g., `15000` not `15`).

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

Based on [Official Memory Docs](https://code.claude.com/docs/en/memory) and [Modular Rules Guide](https://claude-blog.setec.rs/blog/claude-code-rules-directory).

### Rules Directory Organization

**Flat Organization** (small/medium projects):

```
.claude/rules/
├── code-style.md    # Formatting and naming
├── testing.md       # Test requirements
├── security.md      # Security best practices
└── api-design.md    # API conventions
```

**Stack-Based Organization** (full-stack projects):

```
.claude/rules/
├── frontend/
│   ├── react-patterns.md
│   └── styling.md
├── backend/
│   ├── api-design.md
│   └── database.md
└── shared/
    └── typescript.md
```

**Domain-Based Organization** (DSM approach):

```
.claude/rules/
├── binance-api.md          # Exchange-specific
├── fcp-protocol.md         # Core protocol
├── caching-patterns.md     # Data layer
├── symbol-formats.md       # Domain types
├── timestamp-handling.md   # General patterns
├── dataframe-operations.md # Data processing
└── error-handling.md       # Exception patterns
```

### Symlink Support

Rules directory supports symlinks for sharing common rules across projects:

```bash
# Symlink a shared rules directory
ln -s ~/shared-claude-rules .claude/rules/shared

# Symlink individual rule files
ln -s ~/company-standards/security.md .claude/rules/security.md
```

Symlinks are resolved and their contents are loaded normally. Circular symlinks are detected and handled gracefully.

**Note**: The Glob tool does not traverse symlinks to directories. Use Read tool for symlinked content.

### User-Level Rules

Personal rules that apply to all projects in `~/.claude/rules/`:

```
~/.claude/rules/
├── preferences.md    # Personal coding preferences
└── workflows.md      # Preferred workflows
```

User-level rules are loaded before project rules, giving project rules higher priority.

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

### Glob Pattern Support

| Pattern             | Matches                               |
| ------------------- | ------------------------------------- |
| `**/*.ts`           | All TypeScript files in any directory |
| `src/**/*`          | All files under `src/` directory      |
| `*.md`              | Markdown files in the project root    |
| `src/**/*.{ts,tsx}` | TypeScript and TSX files under src    |
| `{src,lib}/**/*.py` | Python files in src or lib            |

Brace expansion is supported for matching multiple extensions or directories.

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

### Rules Best Practices

From [Official Memory Docs](https://code.claude.com/docs/en/memory):

| Practice                        | Guidance                                                  |
| ------------------------------- | --------------------------------------------------------- |
| Keep rules focused              | Each file should cover one topic (testing.md, api.md)     |
| Use descriptive filenames       | Filename should indicate what rules cover                 |
| Use conditional rules sparingly | Only add `paths` when rules truly apply to specific files |
| Organize with subdirectories    | Group related rules (frontend/, backend/)                 |
| Be specific                     | "Use 2-space indentation" > "Format code properly"        |

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

### Polyglot Monorepo Patterns

Based on [DEV.to Monorepo Article](https://dev.to/anvodev/how-i-organized-my-claudemd-in-a-monorepo-with-too-many-contexts-37k7):

**Three methods for context loading**:

| Method          | Approach                  | Context Impact             |
| --------------- | ------------------------- | -------------------------- |
| Reference (`@`) | Link to docs with `@path` | Loads at startup (heavier) |
| Conditional     | Reference without `@`     | Claude loads when needed   |
| Hierarchical    | CLAUDE.md per directory   | Auto-loads based on folder |

**Recommended**: Hierarchical for large monorepos. Deploy CLAUDE.md per service directory.

**Size guidelines**:

| Metric                | Target      |
| --------------------- | ----------- |
| Root CLAUDE.md        | < 10k words |
| Per-service CLAUDE.md | < 10k chars |
| Total distributed     | Split 80%+  |

**Example monorepo distribution**:

```
CLAUDE.md           (~9k chars)   # Shared conventions
frontend/CLAUDE.md  (~8k chars)   # React patterns
backend/CLAUDE.md   (~8k chars)   # API patterns
core/CLAUDE.md      (~7k chars)   # Domain logic
```

## CLAUDE.md Imports

Based on [Official Memory Docs](https://code.claude.com/docs/en/memory) and [MCPcat Guide](https://mcpcat.io/guides/reference-other-files/).

### Import Syntax

CLAUDE.md files can import additional files using `@path/to/file` syntax:

```markdown
See @README.md for project overview
See @docs/api-patterns.md for API conventions
See @package.json for available npm scripts
```

### Import Features

| Feature             | Description                                    |
| ------------------- | ---------------------------------------------- |
| Relative paths      | `@docs/guide.md` from current location         |
| Absolute paths      | `@/full/path/to/file.md`                       |
| Home directory      | `@~/.claude/my-project-instructions.md`        |
| Recursive imports   | Imported files can import others (max 5 hops)  |
| Code block immunity | Imports not evaluated inside code spans/blocks |

### Home Directory Imports

For team members with individual preferences not in version control:

```markdown
# Individual Preferences

- @~/.claude/my-project-instructions.md
```

This is an alternative to CLAUDE.local.md that works better across multiple git worktrees.

### Best Practices for Imports

| Practice            | Guidance                                      |
| ------------------- | --------------------------------------------- |
| Keep main file lean | Put details in separate files, reference them |
| Use sparingly       | Avoid creating a maze of references           |
| Max depth awareness | Recursive imports limited to 5 hops           |
| Test with `/memory` | Run `/memory` to see what files are loaded    |

## MCP Tool Search

Based on [Anthropic Advanced Tool Use](https://www.anthropic.com/engineering/advanced-tool-use) and [MCP Context Optimization](https://scottspence.com/posts/optimising-mcp-server-context-usage-in-claude-code).

### How Tool Search Works

| Step                | Description                                       |
| ------------------- | ------------------------------------------------- |
| Detection           | Claude Code checks if MCP tools exceed 10K tokens |
| Deferral            | Tools marked with `defer_loading: true`           |
| Search injection    | Tool Search tool provided instead of all tools    |
| On-demand discovery | Claude searches by keywords when needed           |
| Selective loading   | 3-5 relevant tools (~3K tokens) loaded per query  |

### Token Savings

| Metric              | Without Tool Search | With Tool Search |
| ------------------- | ------------------- | ---------------- |
| Typical MCP tokens  | ~134K               | ~5K              |
| Context reduction   | -                   | ~85%             |
| Accuracy (Opus 4.5) | 79.5%               | 88.1%            |

### Optimization Patterns

| Pattern                      | Implementation                                       |
| ---------------------------- | ---------------------------------------------------- |
| Better tool descriptions     | Keyword-rich, specific descriptions for discovery    |
| Server instructions          | Add `serverInstructions` for context about tools     |
| High-frequency tools upfront | Configure frequently-used tools to load at start     |
| Disable when needed          | Set `enable_tool_search: false` for specific servers |

### Monitoring Context Usage

- **`/context`**: See where tokens are going
- **`/doctor`**: Detailed MCP server token breakdown

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

### CLAUDE.md Organization

Based on [Gend.co Guide](https://www.gend.co/blog/claude-skills-claude-md-guide) and [Dometrain Blog](https://dometrain.com/blog/creating-the-perfect-claudemd-for-claude-code/):

**Recommended sections**:

| Section          | Content                                         |
| ---------------- | ----------------------------------------------- |
| Project Summary  | Brief description and high-level directory tree |
| Code Style       | Formatting, imports, naming conventions         |
| Workflow         | Branch naming, PR process, deployment           |
| Common Commands  | Build, test, lint commands                      |
| Project-Specific | Gotchas, warnings, non-obvious conventions      |

**Size guidance**:

- Keep CLAUDE.md under 300 lines (Anthropic teams: ~2.5k tokens)
- Ruthlessly prune: if Claude already does it correctly, delete the instruction
- Convert repeated corrections to hooks instead of instructions

**Anti-patterns to avoid**:

| Anti-pattern       | Problem                             | Fix                       |
| ------------------ | ----------------------------------- | ------------------------- |
| Over-specified     | Important rules lost in noise       | Prune, use hooks          |
| Mixed architecture | Claude copies inconsistent patterns | Explicitly choose one     |
| Generic advice     | Adds tokens without value           | Remove, Claude knows this |
| No workflows       | Inconsistent implementation         | Document team processes   |

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

## CI/CD & Headless Mode

Based on [Claude Code GitLab CI/CD](https://code.claude.com/docs/en/gitlab-ci-cd) and [Headless Mode Guide](https://dev.to/rajeshroyal/headless-mode-unleash-ai-in-your-cicd-pipeline-1imm).

### Headless Mode

Use `-p` flag for non-interactive automation:

```bash
# Basic headless execution
claude -p "analyze this file" --output-format stream-json

# Pipeline integration
claude -p "<prompt>" --json | next_command
```

### Automation Patterns

| Pattern    | Use Case                         | Example                        |
| ---------- | -------------------------------- | ------------------------------ |
| Fan-Out    | Large migrations, batch analysis | Process 2k files in parallel   |
| Pipelining | Integrate into data pipelines    | Claude output → next processor |
| Review     | Automated code review on PRs     | Label issues, flag code smells |

### CI/CD Best Practices

| Practice        | Implementation                                 |
| --------------- | ---------------------------------------------- |
| Stage gradually | Start with single repo/team, baseline metrics  |
| Quality gates   | Require build, lint, tests after AI changes    |
| Human review    | Include review steps for production changes    |
| Version control | Version all CI/CD configurations               |
| Latency targets | Review jobs ≤ 3-5 minutes                      |
| Token limits    | Cap max turns/output tokens for predictability |

### Security Considerations

- AI can introduce subtle bugs or misunderstand objectives
- Start with supervised tasks
- Human review before production changes
- Integrate security scanning at every pipeline stage

## Debugging & Troubleshooting

Based on [Official Best Practices](https://code.claude.com/docs/en/best-practices) and [ClaudeLog Troubleshooting](https://claudelog.com/troubleshooting/).

### Diagnostic Commands

| Command    | Purpose                                       |
| ---------- | --------------------------------------------- |
| `/doctor`  | Run diagnostics on installation               |
| `/context` | Check context window usage                    |
| `/memory`  | Show loaded CLAUDE.md and rules files         |
| `/compact` | Summarize history, preserve essential context |
| `/clear`   | Reset conversation completely                 |
| `/bug`     | Report issues with full context               |

### Context Management

| Usage Level | Action                       | Result          |
| ----------- | ---------------------------- | --------------- |
| ~72%        | Run `/compact`               | Reduces to ~36% |
| ~88%        | Commit changes, run `/clear` | Reduces to 0%   |
| Degraded    | Break into smaller tasks     | Better focus    |

**Signs of context depletion**: Short responses, forgotten instructions, inconsistent behavior.

### Debugging Flags

```bash
# Verbose logging
CLAUDE_DEBUG=1 claude

# MCP connection debugging
claude --mcp-debug

# Check configuration
claude config list
```

### Session Recovery

| Flag         | Purpose                        |
| ------------ | ------------------------------ |
| `--continue` | Resume last session            |
| `--resume`   | Choose from recent sessions    |
| `/rename`    | Name sessions for easy finding |

### Context Poisoning Prevention

When switching tasks, use explicit boundaries:

```
---NEW TASK---
Starting fresh implementation of [feature].
Do not carry over patterns from previous tasks.
```

## Extended Thinking

Based on [Anthropic Extended Thinking](https://www.anthropic.com/news/visible-extended-thinking) and [Extended Thinking Tips](https://platform.claude.com/docs/en/build-with-claude/prompt-engineering/extended-thinking-tips).

### When to Use Extended Thinking

| Task Type         | Recommended | Budget      |
| ----------------- | ----------- | ----------- |
| Math, physics     | Yes         | 16k+ tokens |
| Complex coding    | Yes         | 16k+ tokens |
| Analysis          | Yes         | 16k+ tokens |
| Simple queries    | No          | Standard    |
| Tool-heavy chains | Use "think" | N/A         |

**Default budget**: 31,999 tokens (Claude Code sweet spot).

### Best Practices

- Use high-level instructions ("think deeply") rather than prescriptive step-by-step
- Model creativity often exceeds human-prescribed processes
- For budgets > 32k, use batch processing
- "Ultrathink" keyword deprecated as of January 2026

### Think Tool vs Extended Thinking

| Feature              | Extended Thinking     | Think Tool                |
| -------------------- | --------------------- | ------------------------- |
| Best for             | Math, coding, physics | Complex tool chains       |
| Tool calls           | Not needed            | Multiple tools required   |
| Policy navigation    | Basic                 | Policy-heavy environments |
| Sequential decisions | Simple                | Each step builds on prior |

## Prompt Engineering

Based on [Claude 4 Best Practices](https://platform.claude.com/docs/en/build-with-claude/prompt-engineering/claude-4-best-practices) and [CLAUDE.md Optimization](https://arize.com/blog/claude-md-best-practices-learned-from-optimizing-claude-code-with-prompt-learning/).

### CLAUDE.md Prompt Structure

| Section              | Content                            |
| -------------------- | ---------------------------------- |
| Role                 | One-line project context           |
| Success criteria     | Bullet points of desired outcomes  |
| Constraints          | Bullet points of limitations       |
| Uncertainty handling | How to handle ambiguous situations |
| Output format        | Expected response structure        |

### Claude 4.x Specific Guidance

- Models trained for precise instruction following
- Be specific about desired output
- Explicitly request "above and beyond" behavior if wanted
- Use structured prompts (XML, JSON work well)

### Optimization Techniques

| Technique        | Impact       | Application           |
| ---------------- | ------------ | --------------------- |
| Clear sections   | +clarity     | INSTRUCTIONS, CONTEXT |
| XML tagging      | +parsing     | `<task>`, `<output>`  |
| Chain of thought | +reasoning   | Complex analysis      |
| Examples         | +consistency | Show expected format  |

**Research finding**: +10% boost on SWE Bench possible through CLAUDE.md optimization alone.

## Security Best Practices

Based on [Official Security Docs](https://code.claude.com/docs/en/security) and [Backslash Security Guide](https://www.backslash.security/blog/claude-code-security-best-practices).

### Secrets Protection

| Method               | Implementation                              |
| -------------------- | ------------------------------------------- |
| Deny rules           | `Read(**/.env*)` in settings.json           |
| Vaults               | Use Doppler, HashiCorp Vault, not plaintext |
| File permissions     | chmod 600 for sensitive files               |
| Network restrictions | Deny curl, wget for data exfiltration       |

### DSM settings.json Security Rules

```json
{
  "permissions": {
    "deny": [
      "Read(.env*)",
      "Read(.mise.local.toml)",
      "Read(**/credentials*)",
      "Read(**/.secrets/**)",
      "Bash(curl:*)",
      "Bash(wget:*)"
    ]
  }
}
```

### Key Principle

> Treat Claude like an untrusted but powerful intern. Give only minimum permissions needed, sandbox it, audit it.

### Security Checklist

- [ ] `.env*` files in deny rules
- [ ] Credentials directories blocked
- [ ] Network exfiltration commands blocked
- [ ] API keys in vault, not plaintext
- [ ] Regular key rotation (90 days)
- [ ] CLAUDE.local.md gitignored

## Testing Patterns

Based on [TDD with Claude Code](https://stevekinney.com/courses/ai-development/test-driven-development-with-claude) and [Developer Toolkit](https://developertoolkit.ai/en/claude-code/productivity-patterns/testing-integration/).

### TDD Workflow with Claude

| Phase    | Claude Action                         |
| -------- | ------------------------------------- |
| Red      | Write failing tests, verify they fail |
| Green    | Implement code until tests pass       |
| Refactor | Clean up while keeping tests green    |

Claude enters autonomous loop: write → run → analyze failures → adjust → repeat.

### DSM Test Configuration

```bash
# Unit tests (fast, no network)
uv run -p 3.13 pytest tests/unit/ -v

# Integration tests (network required)
uv run -p 3.13 pytest tests/integration/ -v --tb=short

# With coverage
uv run -p 3.13 pytest --cov=src/data_source_manager --cov-report=term-missing
```

### Test Automation via Hooks

PostToolUse hooks can automatically run tests after file edits:

```json
{
  "matcher": "Write|Edit",
  "hooks": [{ "command": "pytest tests/unit/ -q --maxfail=1" }]
}
```

### AI Test Generation Best Practices

| Practice             | Benefit                                   |
| -------------------- | ----------------------------------------- |
| Context in CLAUDE.md | Tests follow project conventions          |
| Edge cases           | AI catches cases developers overlook      |
| Framework matching   | Generates pytest/jest/junit appropriately |
| Deterministic tests  | Avoid sleeps, mock network and time       |

## Code Review Patterns

Based on [Anthropic Best Practices](https://www.anthropic.com/engineering/claude-code-best-practices) and [Code Review Plugin](https://github.com/anthropics/claude-code/blob/main/plugins/code-review/README.md).

### Review Types

| Type         | Focus                                     | Prompt Example                         |
| ------------ | ----------------------------------------- | -------------------------------------- |
| Quality      | Error handling, validation, edge cases    | "Review for error handling patterns"   |
| Security     | Token leakage, access controls, injection | "Check for security vulnerabilities"   |
| Architecture | Separation of concerns, coupling          | "Assess architecture patterns"         |
| Legacy       | Technical debt, deprecated patterns       | "Identify tech debt and modernization" |

### Local Review Best Practices

| Practice           | Benefit                                  |
| ------------------ | ---------------------------------------- |
| Review before PR   | Catch issues earlier, save review cycles |
| Focused reviews    | Target specific aspects (tests, docs)    |
| Manual approval    | Never auto-accept all proposed changes   |
| Targeted re-review | Confirm fixes resolved original issues   |

### Multi-Agent Review Pattern

```
Claude 1 (Coder)  →  Writes implementation
         ↓
Claude 2 (Reviewer)  →  Reviews code quality
         ↓
Claude 3 (Tester)  →  Writes/runs tests
```

Running multiple Claude instances in parallel provides comprehensive coverage.

### Auto-Review via Stop Hook

Configure Stop hook to trigger code review on modified files:

```bash
# Return exit code 2 to block and force review feedback
if [[ -n "$(git diff --name-only)" ]]; then
    echo "Modified files need review" >&2
    exit 2
fi
```

### Review Confidence Scoring

Official code-review plugin outputs issues with 80+ confidence (adjustable 0-100).

| Confidence | Action                              |
| ---------- | ----------------------------------- |
| 80-100     | High confidence - likely real issue |
| 50-79      | Medium - requires human judgment    |
| < 50       | Low - may be false positive         |

## Productivity Workflows

Based on [Claude Code Creator Workflow](https://www.infoq.com/news/2026/01/claude-code-creator-workflow/) and [Productivity Tips](https://www.f22labs.com/blogs/10-claude-code-productivity-tips-for-every-developer/).

### High-Impact Practices

| Practice              | Impact                                     |
| --------------------- | ------------------------------------------ |
| Plan mode first       | Good plan → Claude 1-shots implementation  |
| Parallel instances    | 5+ concurrent Claudes via iTerm2 tabs      |
| Custom slash commands | Reusable workflows run dozens of times/day |
| Session verification  | Claude reviews its own work before commit  |
| Git worktrees         | Isolated branches for safe experimentation |

### Workflow: Plan → Implement → Verify

```
1. /plan (refine until good)
   ↓
2. Auto-accept edits mode
   ↓
3. Claude 1-shots implementation
   ↓
4. Session verification
   ↓
5. Commit
```

### Efficiency Commands

| Command    | Purpose                                    |
| ---------- | ------------------------------------------ |
| `/plan`    | Enter plan mode, iterate on approach       |
| `/rewind`  | Roll back to earlier checkpoint            |
| `/compact` | Reduce context while preserving essentials |
| `/clear`   | Fresh start for new task                   |

### Measured Impact

| Metric             | Without Workflow | With Workflow |
| ------------------ | ---------------- | ------------- |
| Production bugs    | Baseline         | 70% fewer     |
| Debugging time     | Baseline         | 50% faster    |
| Test coverage      | 40%              | 90%           |
| Quality (verified) | 1x               | 2-3x          |

### Self-Learning CLAUDE.md Pattern

> "Anytime we see Claude do something incorrectly we add it to the CLAUDE.md, so Claude knows not to do it next time."

Evolving CLAUDE.md transforms the codebase into a self-learning system.

## Documentation Standards

Based on [Anthropic Best Practices](https://www.anthropic.com/engineering/claude-code-best-practices) and [Builder.io Guide](https://www.builder.io/blog/claude-md-guide).

### Document Size Guidelines

| Document Type    | Target Length   | Purpose                         |
| ---------------- | --------------- | ------------------------------- |
| README           | 500-1000 words  | Essential information only      |
| Feature spec     | 1500-3000 words | Detailed requirements, examples |
| Task-specific MD | 300-800 words   | Single objective, focused       |
| CLAUDE.md        | < 300 lines     | Project context and conventions |
| SKILL.md         | < 500 lines     | Workflow instructions           |

### Documentation Slash Commands

| Command        | Purpose                                 |
| -------------- | --------------------------------------- |
| `/init`        | Generate starter CLAUDE.md from project |
| `/docs`        | Generate comprehensive documentation    |
| `/update-docs` | Sync documentation with implementation  |
| `/release`     | Update changelogs, README for release   |

### Best Practices

- Split monolithic docs into focused files with cross-references
- Update READMEs and changelogs when completing tasks
- Define documentation standards in skills for consistency
- Use headless mode for automated documentation updates

### DSM Documentation Structure

| Document                  | Purpose                        |
| ------------------------- | ------------------------------ |
| `README.md`               | Project overview, quick start  |
| `CLAUDE.md`               | AI-friendly conventions        |
| `docs/INDEX.md`           | Navigation hub                 |
| `docs/RESUME.md`          | Session context for continuity |
| `docs/GLOSSARY.md`        | Domain terminology             |
| `docs/TROUBLESHOOTING.md` | Common issues and solutions    |

## Context Window Management

Based on [Claude Fast Context Management](https://claudefa.st/blog/guide/mechanics/context-management) and [Persistent Memory Architecture](https://dev.to/suede/the-architecture-of-persistent-memory-for-claude-code-17d).

### Token Monitoring Thresholds

| Utilization | Action                      | Rationale                        |
| ----------- | --------------------------- | -------------------------------- |
| < 60%       | Continue working            | Ample context capacity           |
| 60-80%      | Complete current task       | Prepare for transition           |
| 80%         | Exit and restart session    | Prevents performance degradation |
| > 80%       | Avoid multi-file operations | Preserve project-wide awareness  |

**Key principle**: Sessions that stop at 75% utilization produce higher-quality, more maintainable code.

### Task Complexity by Context Cost

| Task Type (High Context Cost)          | Task Type (Low Context Cost)           |
| -------------------------------------- | -------------------------------------- |
| Large-scale multi-file refactoring     | Single-file edits with clear scope     |
| Feature spanning multiple components   | Independent utility functions          |
| Debugging complex interaction patterns | Documentation updates                  |
| Code reviews requiring architecture    | Simple bug fixes with localized impact |

### Context Preservation Strategies

**CLAUDE.md as Free Context**: Automatically loaded at session start, survives restarts without consuming token budget per message.

**Checkpoint Notes Pattern**:

```markdown
## Session Checkpoint

- **Auth decision**: Using JWT with 15-minute expiry
- **Pattern used**: Repository pattern for data access
- **Integration point**: FCP protocol at CacheManager level
```

**Memory Budget Allocation** (from two-tier architecture):

| Memory Type  | Line Budget | Decay            |
| ------------ | ----------- | ---------------- |
| Architecture | 25 lines    | Never            |
| Decisions    | 25 lines    | Never            |
| Patterns     | 25 lines    | Never            |
| Gotchas      | 20 lines    | Never            |
| Progress     | 30 lines    | 7-day half-life  |
| Context      | 15 lines    | 30-day half-life |

### Compaction Best Practices

- Use `/compact` proactively before hitting 80% threshold
- Compaction is instant - Claude maintains background session memory
- Session memory stored at `~/.claude/projects/[project]/[session]/session_memory`
- Fresh sessions via `/clear` reduce prompt-drift and context contamination

### DSM-Specific Context Management

Given DSM's domain complexity (FCP, symbols, timestamps), apply these patterns:

| Domain          | CLAUDE.md? | Rule?    | Rationale                  |
| --------------- | ---------- | -------- | -------------------------- |
| FCP protocol    | Summary    | Full     | Complex, load on-demand    |
| Symbol formats  | Table      | Examples | Quick reference needed     |
| Timestamp rules | Critical   | Extended | UTC is non-negotiable      |
| Error handling  | Patterns   | Full     | Domain exceptions are deep |

## Model Selection & Routing

Based on [Claude Fast Model Selection](https://claudefa.st/blog/guide/performance/model-selection) and [Official Model Config](https://code.claude.com/docs/en/model-config).

### Model Capabilities

| Model  | Best For                                    | Speed   | Cost |
| ------ | ------------------------------------------- | ------- | ---- |
| Opus   | Architecture, complex refactoring, security | Slowest | 5x   |
| Sonnet | 90% of dev work, features, bugs, tests      | Fast    | 1x   |
| Haiku  | Simple edits, syntax, status checks         | Fastest | 0.3x |

### Task-Based Model Selection

| Task Type               | Recommended Model | Rationale                      |
| ----------------------- | ----------------- | ------------------------------ |
| Architectural decisions | Opus              | Maximum reasoning depth        |
| Multi-file refactoring  | Opus              | Complex dependency tracking    |
| Feature implementation  | Sonnet            | Best speed/quality balance     |
| Bug fixes               | Sonnet            | Good reasoning, fast iteration |
| Code reviews            | Sonnet            | Efficient pattern recognition  |
| Test writing            | Sonnet            | Familiar patterns, fast output |
| Syntax questions        | Haiku             | Simple lookups                 |
| Text transformations    | Haiku             | No reasoning needed            |

### Progressive Escalation Strategy

```
Default → Sonnet (90% of tasks)
    ↓
Complexity detected → Opus (architectural, multi-step)
    ↓
Simple operations → Haiku (file ops, status)
```

**Cost savings**: 60-80% compared to Opus-only usage.

### Mid-Session Switching Commands

| Command         | Effect                          |
| --------------- | ------------------------------- |
| `/model opus`   | Switch to Opus for current task |
| `/model sonnet` | Return to default Sonnet        |
| `/model haiku`  | Use Haiku for simple ops        |

### Agent Model Configuration

Configure model per-agent in YAML frontmatter:

```yaml
---
name: data-fetcher
model: haiku # Fast, simple data operations
tools: [Bash, Read]
---
```

| Agent                 | Model  | Rationale                     |
| --------------------- | ------ | ----------------------------- |
| api-reviewer          | sonnet | Code analysis needs reasoning |
| data-fetcher          | haiku  | Simple fetch operations       |
| fcp-debugger          | sonnet | Diagnostic analysis           |
| silent-failure-hunter | sonnet | Pattern detection             |
| test-writer           | sonnet | Test generation needs context |

### DSM Model Selection Guidelines

| DSM Task                     | Model  | Reason                          |
| ---------------------------- | ------ | ------------------------------- |
| FCP protocol debugging       | opus   | Complex state machine analysis  |
| Symbol format validation     | haiku  | Simple string operations        |
| Timestamp handling review    | sonnet | Pattern matching, good coverage |
| Cache invalidation logic     | opus   | Multi-layer dependency analysis |
| Data fetching implementation | sonnet | Standard API patterns           |

## Error Recovery & Troubleshooting

Based on [ClaudeLog Troubleshooting](https://claudelog.com/troubleshooting/) and [Debugging Best Practices](https://www.nathanonn.com/claude-code-debugging-visibility-methods/).

### Common Error Categories

| Category         | Symptoms                         | Recovery                          |
| ---------------- | -------------------------------- | --------------------------------- |
| Auth failure     | Invalid/expired API key          | `claude config`, check console    |
| Context overflow | Slow responses, degradation      | `/compact`, `/clear`, split tasks |
| Network issues   | 503 errors, timeouts             | Check status.anthropic.com, retry |
| Stuck loops      | Repeated errors, no progress     | `/clear`, restart session         |
| Performance      | Excessive memory, slow responses | Switch model, partition files     |

### Loop Detection & Recovery

| Pattern             | Cause                           | Resolution                          |
| ------------------- | ------------------------------- | ----------------------------------- |
| Apology-repeat loop | Same error 3+ times             | `/clear`, reframe the request       |
| Specification drift | Treats specs as goals           | Use explicit requirements in prompt |
| Image format loop   | Unsupported format (HEIC, etc.) | Convert to JPEG/PNG first           |
| Infinite retry      | Resource exhaustion             | Add max retries (3-5), backoff      |

### Diagnostic Commands

| Command    | Purpose                              |
| ---------- | ------------------------------------ |
| `/doctor`  | Automated issue detection            |
| `/context` | Check context window usage           |
| `/memory`  | View session memory state            |
| `/clear`   | Reset conversation, fix stuck states |
| `/compact` | Compress history before overflow     |

### Debugging Strategy: Instrumentation Over Argument

When stuck in debugging loops:

1. **Stop arguing** - Don't repeat "please fix this"
2. **Add visibility** - Ask Claude to add logging/tracing
3. **Request diagrams** - Forces systematic problem mapping
4. **Check assumptions** - Verify data flow, not just code logic

### DSM-Specific Error Recovery

| Error Scenario              | Recovery Action                        |
| --------------------------- | -------------------------------------- |
| FCP cache miss loops        | Use `/debug-fcp` command, check Vision |
| Symbol format rejection     | Verify market-specific format in rules |
| Timestamp conversion errors | Check UTC requirement, avoid naive dt  |
| Rate limit exhaustion       | Wait, check Binance rule in rules/     |
| DataFrame validation fails  | Use `/validate-data` command           |

### Prevention Patterns

- Use PreToolUse hooks to block dangerous commands before execution
- Use PostToolUse hooks to detect silent failure patterns early
- Configure deny rules for sensitive operations in settings.json
- Keep context under 80% to maintain Claude's reasoning quality

### Common Installation Issues

Based on [Official Troubleshooting](https://code.claude.com/docs/en/troubleshooting) and [ClaudeLog Guide](https://claudelog.com/troubleshooting/).

| Issue                       | Cause                  | Solution                           |
| --------------------------- | ---------------------- | ---------------------------------- |
| "command not found: claude" | Not in PATH            | Use native installer (recommended) |
| Node.js version error       | Requires Node.js 18+   | `node --version`, upgrade if < 18  |
| Permission denied           | File/directory access  | Check permissions, use sudo        |
| API key invalid             | Missing or expired key | `claude config`, check console     |
| WSL detection fails         | Using Windows npm      | Install Node via Linux package mgr |

### Diagnostic Quick Checklist

- [ ] Internet connection OK?
- [ ] API key valid (check console.anthropic.com)?
- [ ] Node.js version ≥ 18?
- [ ] Claude Code up to date?
- [ ] File permissions OK?
- [ ] Context not saturated (< 80%)?
- [ ] Hooks configured correctly?

### Reporting Issues

| Method        | Use Case                                     |
| ------------- | -------------------------------------------- |
| `/bug`        | Report issues with full context              |
| `/doctor`     | Run diagnostics on installation              |
| GitHub Issues | anthropics/claude-code for persistent issues |
| `--verbose`   | Detailed logging during operations           |
| `--mcp-debug` | Debug MCP configuration issues               |

## MCP Server Configuration

Based on [Official MCP Docs](https://code.claude.com/docs/en/mcp) and [Scott Spence MCP Guide](https://scottspence.com/posts/configuring-mcp-tools-in-claude-code).

### Configuration Locations

| Location              | Scope   | Purpose                              |
| --------------------- | ------- | ------------------------------------ |
| `~/.claude.json`      | user    | Personal MCP servers, all projects   |
| `.mcp.json`           | project | Shared with team via version control |
| `settings.local.json` | local   | Personal overrides (gitignored)      |

### MCP Server Structure

```json
{
  "mcpServers": {
    "server-name": {
      "type": "stdio",
      "command": "npx",
      "args": ["-y", "@mcp/server-package"],
      "env": {
        "API_KEY": "value"
      }
    }
  }
}
```

### Transport Types

| Type  | Use Case                        | Example               |
| ----- | ------------------------------- | --------------------- |
| stdio | Local processes, custom scripts | Node.js tools, Python |
| http  | Remote/cloud services           | External APIs         |

### Tool Naming Convention

MCP tools follow: `mcp__<server-name>__<tool-name>`

Example: `mcp__github__list_issues`

### Permission Configuration

```json
{
  "permissions": {
    "allow": ["mcp__github__*", "mcp__filesystem__read_file"],
    "deny": ["mcp__filesystem__delete_file"]
  }
}
```

### Context Window Optimization

| Strategy               | Effect                                  |
| ---------------------- | --------------------------------------- |
| Disable unused servers | Reduces tool definitions in context     |
| Use MCP tool search    | Dynamic loading when >10% context used  |
| Consolidate servers    | Fewer tool definitions, same capability |

**Tool search auto-activates** when MCP tool descriptions exceed 10% of context window.

### Security Best Practices

| Practice           | Implementation                            |
| ------------------ | ----------------------------------------- |
| TLS/HTTPS          | Encrypt all MCP server communications     |
| Least privilege    | Grant only required tool access           |
| Trust verification | Only use MCP servers from trusted sources |
| Env var secrets    | Never hardcode API keys in config         |

### Debugging MCP

| Command/Flag   | Purpose                           |
| -------------- | --------------------------------- |
| `/mcp`         | Verify server connectivity status |
| `--mcp-debug`  | Launch with debug logging enabled |
| `/permissions` | Add/remove tools from allowlist   |

### DSM MCP Considerations

For data-source-manager, consider these MCP integrations:

| Server           | Purpose                              |
| ---------------- | ------------------------------------ |
| filesystem       | Safe file operations with sandboxing |
| sequential-think | Complex FCP debugging workflows      |
| context7         | Up-to-date library documentation     |

## Workspace & Session Management

Based on [Official Workflows](https://code.claude.com/docs/en/common-workflows), [Git Worktrees with Claude Code](https://dev.to/datadeer/part-2-running-multiple-claude-code-sessions-in-parallel-with-git-worktree-165i), and [incident.io Blog](https://incident.io/blog/shipping-faster-with-claude-code-and-git-worktrees).

### Session Commands

| Command             | Purpose                             |
| ------------------- | ----------------------------------- |
| `claude --resume`   | Open conversation picker or by name |
| `/resume`           | Switch conversations mid-session    |
| `/rename <name>`    | Name session for later retrieval    |
| `claude --continue` | Resume most recent session          |

**Session storage**: Per project directory. Sessions from same git repo (including worktrees) appear in `/resume` picker.

### Git Worktree Pattern for Parallel Development

```bash
# Create worktree with feature branch
git worktree add ../dsm-worktrees/fcp-refactor -b feat/fcp-refactor

# Launch Claude in worktree
cd ../dsm-worktrees/fcp-refactor && claude

# List active worktrees
git worktree list

# Cleanup when done
git worktree remove ../dsm-worktrees/fcp-refactor
```

### When to Use Parallel Sessions

| Scenario                    | Parallel? | Rationale                     |
| --------------------------- | --------- | ----------------------------- |
| Long-running feature (>30m) | Yes       | Worth setup overhead          |
| Quick fix (<10m)            | No        | Setup time exceeds benefit    |
| Independent features        | Yes       | No cross-feature dependencies |
| Dependent changes           | No        | Sequential workflow required  |
| Architecture exploration    | Yes       | Compare multiple approaches   |

### Resource Considerations

| Resource  | Challenge                          | Mitigation                    |
| --------- | ---------------------------------- | ----------------------------- |
| Tokens    | Multiple sessions consume faster   | Reserve for complex work only |
| Ports     | Services conflict across worktrees | Use port offsets per worktree |
| Memory    | Multiple Claude instances          | Limit concurrent sessions     |
| Cognitive | Context-switching fatigue          | Focus on one, check other     |

### Context Isolation Benefits

- Each worktree has independent working directory
- Claude instances cannot interfere with each other
- All worktrees share Git history and remotes
- Perfect for simultaneous feature development

### DSM Parallel Development Patterns

| Task Pair                          | Parallel Viable? |
| ---------------------------------- | ---------------- |
| FCP refactor + New data source     | Yes              |
| Cache layer + Tests for cache      | No (dependent)   |
| Binance fixes + OKX implementation | Yes              |
| Symbol normalization + Docs update | Yes              |

## Configuration Sync & Team Sharing

Based on [Chezmoi Sync Guide](https://www.arun.blog/sync-claude-code-with-chezmoi-and-age/), [Dotfiles Sync](https://github.com/NovaAI-innovation/claude-code-mastery/blob/main/docs/guides/dotfiles-sync.md), and [Anthropic Best Practices](https://www.anthropic.com/engineering/claude-code-best-practices).

### CLAUDE.md Location Hierarchy

| Location              | Scope        | When Loaded                  |
| --------------------- | ------------ | ---------------------------- |
| `~/.claude/CLAUDE.md` | Global       | All Claude sessions          |
| `./CLAUDE.md`         | Project      | Working in this repo         |
| `./CLAUDE.local.md`   | Local only   | Personal overrides           |
| `parent/CLAUDE.md`    | Monorepo     | Inherited by child dirs      |
| `child/CLAUDE.md`     | Subdirectory | On-demand when working there |

### Team Sharing Strategy

| File                    | Commit to Git? | Reason                      |
| ----------------------- | -------------- | --------------------------- |
| `CLAUDE.md`             | Yes            | Share conventions with team |
| `CLAUDE.local.md`       | No             | Personal preferences        |
| `.claude/settings.json` | Yes            | Team permission rules       |
| `settings.local.json`   | No             | Personal tool overrides     |
| `.claude/agents/`       | Yes            | Shared agent configurations |
| `.claude/commands/`     | Yes            | Shared slash commands       |
| `.claude/rules/`        | Yes            | Domain-specific context     |
| `.claude/hooks/`        | Yes            | Team code quality checks    |

### Files to Sync via Dotfiles

```
~/.claude/
├── CLAUDE.md          # Sync (global preferences)
├── settings.json      # Sync (global permissions)
├── commands/          # Sync (personal commands)
├── skills/            # Sync (personal skills)
└── plugins/           # Sync (marketplace plugins)
```

**Do NOT sync**: Session data, cache files, project-specific files.

### Dotfiles Structure Example

```
dotfiles/
├── dot_claude/
│   ├── CLAUDE.md
│   ├── settings.json
│   ├── commands/
│   │   └── my-workflow.md
│   └── skills/
│       └── my-skill/
└── install.sh          # Symlinks to ~/.claude/
```

### Encrypted Sync with Chezmoi + Age

For sensitive configs (API keys in hooks):

```bash
# Initialize with age encryption
chezmoi init --apply

# Add encrypted file
chezmoi add --encrypt ~/.claude/settings.json

# Sync across machines
chezmoi update
```

### CLAUDE.md Maintenance Best Practices

| Practice         | Description                                     |
| ---------------- | ----------------------------------------------- |
| Prune regularly  | Remove instructions that don't affect behavior  |
| Test changes     | Observe if Claude's behavior shifts             |
| Use emphasis     | "IMPORTANT" or "YOU MUST" for critical rules    |
| Include in PRs   | Add CLAUDE.md changes with related code changes |
| Use `#` shortcut | Press `#` to add instructions Claude remembers  |

### DSM Configuration Sharing

| Component             | Committed? | Notes                              |
| --------------------- | ---------- | ---------------------------------- |
| Root CLAUDE.md        | Yes        | DSM conventions for all developers |
| .claude/settings.json | Yes        | Permission rules (no secrets)      |
| All agents/commands   | Yes        | Domain-specific workflows          |
| hooks/hooks.json      | Yes        | Code quality automation            |
| CLAUDE.local.md       | No         | Individual preferences             |

## Agentic Loop Best Practices

Based on [Anthropic Engineering Best Practices](https://www.anthropic.com/engineering/claude-code-best-practices), [Agentic Coding Guide](https://research.aimultiple.com/agentic-coding/), and [Claude Code Architecture](https://www.zenml.io/llmops-database/claude-code-agent-architecture-single-threaded-master-loop-for-autonomous-coding).

### Core Design Philosophy

Claude Code uses a **single-threaded master loop** that continues while responses include tool calls. Plain text responses terminate the loop and return control to the user.

**Key principle**: Simple, single-threaded master loop with disciplined tools delivers controllable autonomy.

### Completion Criteria

| Bad (Vague)                  | Good (Specific)                         |
| ---------------------------- | --------------------------------------- |
| "Make it better"             | "All tests pass with 80% coverage"      |
| "Improve the authentication" | "JWT refresh works and tests pass"      |
| "Fix the bugs"               | "No TypeScript errors, all tests green" |
| "Optimize the code"          | "Response time < 200ms, memory < 100MB" |

### Planning-First Pattern

1. **Research**: Ask Claude to explore the codebase
2. **Plan**: Request a plan before coding
3. **Confirm**: Review and approve the plan
4. **Execute**: Only then allow implementation

```
"Research the caching layer, then create a plan for adding Redis support.
Don't write any code until I approve the plan."
```

### Self-Correction & Feedback Loops

| Anti-Pattern                   | Better Approach                              |
| ------------------------------ | -------------------------------------------- |
| "Fix this"                     | Detail what went wrong and expected behavior |
| Repeat corrections 3+ times    | `/clear`, write better initial prompt        |
| Context polluted with failures | Start fresh with learned context             |
| Vague redirection              | Specific acceptance criteria                 |

### Subagent Delegation Pattern

```yaml
# .claude/agents/researcher.md
---
name: researcher
tools: [Read, Grep, Glob]
permissionMode: plan
---
Research the codebase without making changes.
```

**Use subagents when**:

- Task requires reading many files
- Specialized focus needed
- Want isolated context (doesn't pollute main conversation)

### Context Hygiene

| Signal                         | Action                        |
| ------------------------------ | ----------------------------- |
| Two failed correction attempts | `/clear`, reframe the request |
| Switching unrelated tasks      | `/clear` between tasks        |
| Context window > 80%           | `/compact` or start fresh     |
| Repeated same mistakes         | Add to CLAUDE.md, restart     |

### Flow-Based Architecture

```
[Decision Node] → [Analysis Node] → [Modification Node] → [Validation Node]
      ↓                                                          ↓
  Plan/Research                                            Tests/Checks
```

Task progression between nodes occurs automatically without manual intervention.

### DSM Agentic Patterns

| Task                        | Agentic Approach                       |
| --------------------------- | -------------------------------------- |
| Implement new data source   | Research → Plan → Implement → Test     |
| Debug FCP cache miss        | Diagnose with fcp-debugger agent       |
| Add symbol validation       | TDD: write tests first, then implement |
| Refactor timestamp handling | Plan mode → approval → execute         |

## IDE Integration

Based on [Official VS Code Docs](https://code.claude.com/docs/en/vs-code) and [IDE Integration Guide](https://apidog.com/blog/claude-code-ide-integrations/).

### Integration Options

| Option            | Installation                      | Best For                      |
| ----------------- | --------------------------------- | ----------------------------- |
| VS Code Extension | Extensions panel, search "Claude" | Visual workflow, diffs in IDE |
| Terminal in IDE   | Open terminal, run `claude`       | CLI power users               |
| External terminal | Run `claude`, then `/ide`         | Separate terminal preference  |

### Key Shortcuts

| Shortcut (Mac) | Shortcut (Win/Linux) | Action                   |
| -------------- | -------------------- | ------------------------ |
| `Cmd+Option+K` | `Alt+Ctrl+K`         | Insert file reference    |
| `Cmd+\``       | `Ctrl+\``            | Open integrated terminal |
| `Cmd+Shift+X`  | `Ctrl+Shift+X`       | Open Extensions panel    |

### Context Sharing Features

| Feature                   | Description                             |
| ------------------------- | --------------------------------------- |
| Selection context         | Current selection auto-shared           |
| Diff viewing              | View changes in VS Code diff viewer     |
| Terminal output reference | `@terminal:name` to reference output    |
| File reference            | `@File#L1-99` for specific line ranges  |
| Automatic diagnostics     | Lint/syntax errors shared automatically |

### Checkpoints & Rewind

The extension tracks Claude's file edits for rollback:

| Action                      | Effect                               |
| --------------------------- | ------------------------------------ |
| Fork conversation from here | Create branch from this point        |
| Rewind code to here         | Restore files to previous state      |
| Fork + Rewind               | Both: new branch with old code state |

### Conversation Continuity

Extension and CLI share conversation history:

```bash
# Resume extension conversation in CLI
claude --resume
# Then select from picker
```

### IDE Best Practices

| Practice                   | Reason                           |
| -------------------------- | -------------------------------- |
| Start at project root      | Full context available           |
| Review inline diffs        | Maintain control over changes    |
| Use `@terminal` references | Share errors without copy-paste  |
| Store API key in env var   | Security, not hardcoded          |
| Test incrementally         | Start small before complex tasks |

### Workflow Patterns

**Pair Programming Mode**:

- Ongoing sidebar conversation
- Brainstorm, explain, debug together
- You remain in control

**Autonomous Mode**:

- Larger refactoring tasks
- Claude creates plan for approval
- Watch progress, review diffs as created

### DSM IDE Workflow

| Task                 | IDE Integration Advantage            |
| -------------------- | ------------------------------------ |
| FCP debugging        | See cache state in terminal output   |
| Symbol validation    | Inline diffs show format changes     |
| Test development     | Run tests in terminal, share results |
| DataFrame operations | Review Polars/pandas diffs easily    |

## Usage Analytics & Cost Tracking

Based on [Official Analytics Docs](https://code.claude.com/docs/en/analytics) and [Analytics API](https://platform.claude.com/docs/en/build-with-claude/claude-code-analytics-api).

### Console Dashboard Metrics

| Metric                 | Description                               |
| ---------------------- | ----------------------------------------- |
| Lines of code accepted | Total lines written and accepted by users |
| Suggestion accept rate | % of Edit/Write/NotebookEdit acceptances  |
| Daily active users     | Unique users per day                      |
| Sessions               | Total coding sessions                     |
| Spend                  | Estimated daily API costs                 |

### Analytics API (Teams/Enterprise)

| Endpoint             | Data Returned                           |
| -------------------- | --------------------------------------- |
| Productivity metrics | Sessions, LOC, commits, PRs, tool usage |
| Token/cost data      | Usage by model (Opus/Sonnet/Haiku)      |
| User analytics       | DAU/WAU/MAU metrics                     |
| Contribution metrics | PRs and LOC shipped with Claude assist  |

**Data freshness**: Up to 1-hour delay for consistency.

### CLI Usage Commands

| Command    | Purpose                               |
| ---------- | ------------------------------------- |
| `/context` | Summary by category (current session) |
| `/cost`    | Session cost estimate                 |
| `ccusage`  | External tool for usage trends        |

### Cost Optimization Strategies

| Strategy              | Savings                                |
| --------------------- | -------------------------------------- |
| Use Sonnet by default | 5x cheaper than Opus                   |
| Delegate to Haiku     | 3x cheaper than Sonnet for simple ops  |
| Compact early         | Prevents expensive context overflow    |
| Disable unused MCP    | Reduces tool definition token overhead |
| Use model routing     | 60-80% savings vs Opus-only usage      |

### OpenTelemetry Integration

For advanced monitoring:

```yaml
# prometheus.yml
scrape_configs:
  - job_name: "claude-code"
    static_configs:
      - targets: ["localhost:9090"]
```

| Metric            | Type      | Purpose                         |
| ----------------- | --------- | ------------------------------- |
| API latency       | histogram | Response time distribution      |
| Token consumption | counter   | Usage tracking over time        |
| Success rate      | gauge     | Tool/command success percentage |
| Model usage       | counter   | Breakdown by Opus/Sonnet/Haiku  |

### DSM Cost Considerations

| Task Type               | Model  | Relative Cost | Justification            |
| ----------------------- | ------ | ------------- | ------------------------ |
| FCP architecture review | Opus   | High          | Complex reasoning needed |
| Symbol validation       | Haiku  | Low           | Simple string ops        |
| Test writing            | Sonnet | Medium        | Balanced quality/cost    |
| Data fetching impl      | Sonnet | Medium        | Standard patterns        |
| Quick lookups           | Haiku  | Low           | Fast, simple queries     |

## Plugin & Marketplace Patterns

Based on [Official Plugin Docs](https://code.claude.com/docs/en/discover-plugins) and [Plugin Blog Post](https://claude.com/blog/claude-code-plugins).

### Plugin Components

| Component      | Purpose                                     |
| -------------- | ------------------------------------------- |
| Slash commands | Custom shortcuts for frequent operations    |
| Subagents      | Purpose-built agents for specialized tasks  |
| MCP servers    | Connect to tools via Model Context Protocol |
| Hooks          | Customize behavior at key workflow points   |

### Plugin Discovery Commands

| Command                    | Purpose                       |
| -------------------------- | ----------------------------- |
| `/plugin`                  | Install plugins interactively |
| `/plugin search <query>`   | Find plugins by keyword       |
| `/plugin install <name>`   | Install a specific plugin     |
| `/plugin list`             | Show installed plugins        |
| `/plugin uninstall <name>` | Remove a plugin               |

### Marketplace Structure

```json
// .claude-plugin/marketplace.json
{
  "name": "my-marketplace",
  "description": "Custom plugins for team",
  "plugins": [
    {
      "name": "my-plugin",
      "description": "What it does",
      "repository": "org/repo"
    }
  ]
}
```

### Plugin Architecture Pattern

```
Slash command → Agent → Subagent (haiku) → External tool
```

**Design principles**:

- Claude orchestrates, external tools execute
- CLI or script-based integrations (curl, gemini-cli, codex)
- No MCP servers, no background processes, no daemons
- Plugin loads, runs, exits

### Creating a Plugin

```
my-plugin/
├── .claude-plugin/
│   └── plugin.json          # Plugin metadata
├── .claude/
│   ├── agents/              # Subagent definitions
│   ├── commands/            # Slash commands
│   ├── hooks/               # Lifecycle hooks
│   └── rules/               # Context rules
└── README.md                # Documentation
```

### Marketplace Sources

| Marketplace             | Type        | Purpose                      |
| ----------------------- | ----------- | ---------------------------- |
| claude-plugins-official | Official    | Anthropic-maintained plugins |
| Community marketplaces  | Third-party | User-contributed plugins     |
| Team marketplace        | Private     | Internal organization tools  |

### DSM Plugin Considerations

| Plugin Opportunity   | Benefit                           |
| -------------------- | --------------------------------- |
| dsm-data-validator   | Automated OHLCV validation        |
| dsm-fcp-diagnostics  | FCP debugging workflow            |
| dsm-symbol-formatter | Market-specific symbol conversion |
| dsm-cache-inspector  | Cache state visualization         |

### Adding Custom Marketplace

In `.claude/settings.json`:

```json
{
  "extraKnownMarketplaces": ["github.com/your-org/your-marketplace"]
}
```

## Enterprise & Team Deployment

Based on [Official Enterprise Docs](https://code.claude.com/docs/en/third-party-integrations) and [Team/Enterprise Guide](https://support.claude.com/en/articles/11845131-using-claude-code-with-your-team-or-enterprise-plan).

### Deployment Options

| Option                | Best For                     | Key Features                |
| --------------------- | ---------------------------- | --------------------------- |
| Claude for Teams      | Smaller teams, quick start   | Self-service, collab tools  |
| Claude for Enterprise | Large orgs, compliance needs | SSO, RBAC, managed policies |
| Anthropic Console     | Individual developers        | PAYG, API key auth          |
| Amazon Bedrock        | AWS-native deployments       | AWS IAM, CloudTrail         |
| Google Vertex AI      | GCP-native deployments       | GCP IAM, Audit Logs         |
| Microsoft Foundry     | Azure-native deployments     | Azure RBAC, Monitor         |

### Enterprise Features

| Feature                 | Description                        |
| ----------------------- | ---------------------------------- |
| SSO & domain capture    | Centralized authentication         |
| Role-based permissions  | Granular access control            |
| Compliance API          | Real-time programmatic access      |
| Managed policy settings | Organization-wide configurations   |
| Usage analytics         | LOC accepted, suggestion rate, DAU |
| Spend controls          | Per-user and org-level budgets     |

### Organization Onboarding Best Practices

| Practice                    | Benefit                              |
| --------------------------- | ------------------------------------ |
| Invest in CLAUDE.md         | Claude understands codebase faster   |
| Create "one-click" install  | Drives adoption across teams         |
| Start with guided usage     | New users learn paradigm gradually   |
| Configure security policies | Managed permissions, cannot override |
| Centralize MCP config       | Check `.mcp.json` into codebase      |

### Multi-Level CLAUDE.md Deployment

| Level        | Location                                            | Purpose                 |
| ------------ | --------------------------------------------------- | ----------------------- |
| Organization | `/Library/Application Support/ClaudeCode/CLAUDE.md` | Company-wide standards  |
| Repository   | `./CLAUDE.md`                                       | Project architecture    |
| Team/Feature | `./feature/CLAUDE.md`                               | Domain-specific context |

### Cloud Provider Configuration

```bash
# Amazon Bedrock
export CLAUDE_CODE_USE_BEDROCK=1
export AWS_REGION=us-east-1

# Google Vertex AI
export CLAUDE_CODE_USE_VERTEX=1
export CLOUD_ML_REGION=us-east5
export ANTHROPIC_VERTEX_PROJECT_ID=your-project-id

# Microsoft Foundry
export CLAUDE_CODE_USE_FOUNDRY=1
export ANTHROPIC_FOUNDRY_RESOURCE=your-resource
```

### LLM Gateway Pattern

For centralized management:

```bash
# Route through gateway
export ANTHROPIC_BASE_URL='https://your-llm-gateway.com'

# Benefits:
# - Centralized usage tracking across teams
# - Custom rate limiting or budgets
# - Centralized authentication management
```

### DSM Team Deployment Considerations

| Consideration           | Recommendation                       |
| ----------------------- | ------------------------------------ |
| Shared CLAUDE.md        | Check into git, team benefits        |
| Agents for common tasks | 5 agents (reviewer, fetcher, etc.)   |
| Rules for domains       | 7 rules (FCP, symbols, timestamps)   |
| Hooks for quality       | 5 hooks (guards, validators)         |
| MCP servers             | .mcp.json checked in for consistency |

## Keyboard Shortcuts & Productivity

Based on [Official Interactive Mode](https://code.claude.com/docs/en/interactive-mode) and [Keyboard Shortcuts Guide](https://nikiforovall.blog/claude-code-rules/tips-and-tricks/keyboard-shortcuts/).

### Essential Editing Shortcuts

| Shortcut      | Action                       |
| ------------- | ---------------------------- |
| `Ctrl+A`      | Move to start of line        |
| `Ctrl+E`      | Move to end of line          |
| `Option+F`    | Move forward one word        |
| `Option+B`    | Move backward one word       |
| `Ctrl+W`      | Delete previous word         |
| `Escape` (2x) | Clear input / browse history |
| `Ctrl+C` (2x) | Hard exit                    |
| `Shift+Enter` | Multi-line input             |

**Note**: Option/Alt shortcuts require configuring Option as Meta in terminal settings.

### Command History

| Shortcut     | Action                        |
| ------------ | ----------------------------- |
| `Up Arrow`   | Previous command              |
| `Down Arrow` | Next command                  |
| `Ctrl+R`     | Reverse search                |
| `Ctrl+S`     | Forward search (after Ctrl+R) |

### Tab Completion

| Example            | Expands To              |
| ------------------ | ----------------------- |
| `/com<Tab>`        | `/commit`               |
| `/read src/c<Tab>` | `/read src/components/` |
| `Ctrl+Space`       | Show suggestions        |

### Session Management

| Command/Shortcut    | Action                        |
| ------------------- | ----------------------------- |
| `?`                 | Show available shortcuts      |
| `claude --resume`   | Resume previous session       |
| `claude --continue` | Continue most recent session  |
| `Escape` (2x)       | Browse history, restore point |

### Terminal Setup for Shift+Enter

| Terminal         | Setup Required?       |
| ---------------- | --------------------- |
| iTerm2           | Works out of box      |
| WezTerm          | Works out of box      |
| Ghostty          | Works out of box      |
| Kitty            | Works out of box      |
| VS Code terminal | Run `/terminal-setup` |
| Alacritty        | Run `/terminal-setup` |
| Zed              | Run `/terminal-setup` |
| Warp             | Run `/terminal-setup` |

### Shell Aliases for Productivity

```bash
# Add to ~/.bashrc or ~/.zshrc
alias c="claude"
alias cr="claude --resume"
alias cc="claude --continue"
alias ci="claude --init"
```

### DSM Productivity Tips

| Tip                     | Benefit                         |
| ----------------------- | ------------------------------- |
| Use `/quick-test` often | Fast feedback on changes        |
| `/debug-fcp` for cache  | Quick FCP diagnostics           |
| `Ctrl+R` for commands   | Find previous mise/git commands |
| Tab-complete file paths | Faster file references          |

## Sandboxing & Permission Modes

Based on [Official Sandboxing Docs](https://code.claude.com/docs/en/sandboxing) and [Anthropic Engineering Blog](https://www.anthropic.com/engineering/claude-code-sandboxing).

### Permission Modes

| Mode              | Behavior                            | Use Case                  |
| ----------------- | ----------------------------------- | ------------------------- |
| default           | Prompt for all tool uses            | Interactive development   |
| acceptEdits       | Auto-approve file operations        | Trusted editing workflows |
| plan              | Read-only, no write operations      | Research and exploration  |
| bypassPermissions | Auto-approve all (use with caution) | Controlled CI/CD only     |

Set in `settings.json`:

```json
{
  "defaultMode": "acceptEdits"
}
```

### Sandbox Isolation

| Boundary   | Default Behavior                           |
| ---------- | ------------------------------------------ |
| Filesystem | Read/write to cwd and subdirs              |
| Filesystem | Read-only for rest of system (some denied) |
| Network    | Optional isolation for autonomous mode     |

**Impact**: Sandboxing reduces permission prompts by ~84% in internal usage.

### OS-Level Primitives

| OS    | Technology | Purpose                      |
| ----- | ---------- | ---------------------------- |
| Linux | bubblewrap | Filesystem/network isolation |
| macOS | seatbelt   | Sandbox enforcement          |

### Bash Tool Control Patterns

```json
{
  "permissions": {
    "allow": [
      "Bash(npm run:*)",
      "Bash(npm test:*)",
      "Bash(git:*)",
      "Bash(mise run:*)"
    ],
    "deny": ["Read(./.env*)", "Bash(npm publish:*)", "Bash(rm -rf:*)"]
  }
}
```

### Safe Autonomous Mode

For CI/CD or automated workflows:

```bash
# Run in Docker with network isolation
docker run --network none \
  -v $(pwd):/workspace \
  claude-code --dangerously-skip-permissions \
  --disallowedTools "Bash(curl:*),Bash(wget:*)"
```

**Key flags**:

- `--disallowedTools`: Works correctly in all modes
- `--allowedTools`: May be ignored with bypassPermissions (known issue)

### PreToolUse Hooks as Controls

```bash
#!/bin/bash
# dsm-bash-guard.sh - block dangerous commands
TOOL_INPUT="$1"
if echo "$TOOL_INPUT" | grep -qE "(rm -rf|drop table|--force)"; then
  echo "Blocked: dangerous command pattern detected"
  exit 2  # Exit 2 = block
fi
exit 0  # Exit 0 = allow
```

### DSM Permission Configuration

| Tool Pattern               | Rule  | Reason               |
| -------------------------- | ----- | -------------------- |
| `Bash(uv run:*)`           | allow | Standard development |
| `Bash(mise run:*)`         | allow | Task runner          |
| `Bash(git push --force:*)` | deny  | Prevent force push   |
| `Bash(pip install:*)`      | deny  | Use uv instead       |
| `Read(.env*)`              | deny  | Protect secrets      |

## GitHub Actions Integration

Based on [Official GitHub Actions Docs](https://code.claude.com/docs/en/github-actions) and [Claude Code Action](https://github.com/anthropics/claude-code-action).

### Claude Code Action Features

| Feature               | Description                             |
| --------------------- | --------------------------------------- |
| @claude mentions      | Responds to mentions in PRs/issues      |
| Code review           | Analyzes changes, suggests improvements |
| Code implementation   | Implements fixes, refactoring, features |
| Intelligent detection | Auto-selects mode based on context      |
| Multi-provider auth   | Anthropic, Bedrock, Vertex, Foundry     |

### Quick Setup

```bash
# In Claude Code terminal
/install-github-app
```

This guides through GitHub App installation and secret configuration.

### Workflow File Example

```yaml
# .github/workflows/claude.yml
name: Claude Code
on:
  issue_comment:
    types: [created]
  pull_request_review_comment:
    types: [created]

permissions:
  contents: write
  pull-requests: write
  issues: write

jobs:
  claude:
    runs-on: ubuntu-latest
    steps:
      - uses: anthropics/claude-code-action@v1
        with:
          anthropic_api_key: ${{ secrets.ANTHROPIC_API_KEY }}
```

### Code Review Plugin

| Command                  | Output              |
| ------------------------ | ------------------- |
| `/code-review`           | Terminal output     |
| `/code-review --comment` | Posts as PR comment |

**Review process**:

- Launches 4 review agents in parallel
- Scores each issue for confidence
- Outputs issues with confidence ≥80

### Workflow Permissions

| Permission           | Required For                       |
| -------------------- | ---------------------------------- |
| contents: write      | Pushing changes, creating branches |
| pull-requests: write | Creating/updating PRs, comments    |
| issues: write        | Responding to issue comments       |

### Authentication Options

| Provider          | Environment Variables        |
| ----------------- | ---------------------------- |
| Anthropic Direct  | `ANTHROPIC_API_KEY`          |
| Amazon Bedrock    | AWS credentials + region     |
| Google Vertex     | GCP credentials + project ID |
| Microsoft Foundry | Foundry resource + API key   |

### Best Practices

| Practice                | Benefit                      |
| ----------------------- | ---------------------------- |
| CLAUDE.md in repo root  | Context for CI/CD runs       |
| Explicit permissions    | Required for PR/push actions |
| Use @claude mentions    | Interactive code assistance  |
| Set up secrets properly | Never expose API keys        |

### DSM GitHub Actions Considerations

**Note**: Per project policy, DSM does not use GitHub Actions for testing or linting (local-first philosophy). However, Claude Code Actions can be used for:

| Use Case                  | Allowed?                |
| ------------------------- | ----------------------- |
| PR code review            | Yes                     |
| Issue response automation | Yes                     |
| Test/lint execution       | No (local-first policy) |
| Deployment automation     | Yes                     |

## Observability & Tracing

Real-time debugging and monitoring for Claude Code sessions.

### OpenTelemetry Configuration

Enable telemetry with environment variables:

```bash
export CLAUDE_CODE_ENABLE_TELEMETRY=1
export OTEL_METRICS_EXPORTER=otlp
export OTEL_LOGS_EXPORTER=otlp
export OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317
export OTEL_EXPORTER_OTLP_PROTOCOL=grpc
```

### Metrics Available

| Metric Category  | Data Points                      |
| ---------------- | -------------------------------- |
| Session Tracking | Active sessions, duration, LOC   |
| Token Usage      | Input, output, cache by model    |
| Cost Analysis    | Spending by model, session, team |
| Tool Performance | Execution frequency, success %   |
| Performance      | API latency, error rates         |

### Observability Stack

| Component  | Purpose         | Default Port |
| ---------- | --------------- | ------------ |
| OTel       | Data collection | 4317 (gRPC)  |
| Prometheus | Metrics storage | 9090         |
| Loki       | Log aggregation | 3100         |
| Grafana    | Visualization   | 3000         |

### Claude HUD

Real-time terminal statusline showing:

- Context health (token utilization %)
- Tool activity (recent tool calls)
- Agent status (active subagents)
- Task progress (completion %)

### Dev-Agent-Lens

Proxy-based observability for Claude Code:

```
Claude Code → LiteLLM Proxy → Dev-Agent-Lens → Arize/Phoenix
```

Features:

- OpenInference spans for each tool call
- Structured JSON input/output correlation
- Model prompts side-by-side with tool results

### Privacy Controls

| Variable                | Purpose                             |
| ----------------------- | ----------------------------------- |
| OTEL_LOG_USER_PROMPTS=1 | Enable prompt logging (off default) |
| OTEL_INCLUDE_SESSION_ID | Include session ID in metrics       |
| OTEL_INCLUDE_VERSION    | Include CLI version in metrics      |

### DSM Observability Integration

```bash
# Start local observability stack
make -C ~/.claude/observability up

# Run Claude Code with telemetry
CLAUDE_CODE_ENABLE_TELEMETRY=1 claude

# View dashboards
open http://localhost:3000  # Grafana
```

## Custom MCP Tools Development

Building custom tools for Claude Code via Model Context Protocol.

### Transport Methods

| Transport | Use Case              | Configuration               |
| --------- | --------------------- | --------------------------- |
| STDIO     | Local tools           | Default, runs in subprocess |
| HTTP      | Remote/shared servers | URL endpoint                |
| SSE       | Streaming tools       | Server-sent events          |

### FastMCP Quick Start

Create a custom tool with FastMCP:

```python
# tools/dice_server.py
from fastmcp import FastMCP

mcp = FastMCP("dice-tools")

@mcp.tool
def roll_dice(n_dice: int) -> list[int]:
    """Roll n_dice 6-sided dice and return results."""
    import random
    return [random.randint(1, 6) for _ in range(n_dice)]

if __name__ == "__main__":
    mcp.run()
```

### Installation Methods

**Automated (recommended)**:

```bash
fastmcp install claude-code tools/dice_server.py
```

**With dependencies**:

```bash
fastmcp install claude-code tools/server.py \
    --with requests \
    --with-requirements requirements.txt \
    --env API_KEY=xxx
```

**Manual registration**:

```bash
claude mcp add dice-tools python tools/dice_server.py
```

### MCP Configuration

Add to `.mcp.json` in project root:

```json
{
  "mcpServers": {
    "dice-tools": {
      "command": "python",
      "args": ["tools/dice_server.py"],
      "env": {
        "API_KEY": "${DICE_API_KEY}"
      }
    },
    "remote-api": {
      "url": "https://api.example.com/mcp",
      "transport": "http"
    }
  }
}
```

### Resource Access

Reference MCP resources in prompts:

```
@server:protocol://resource/path
```

### Prompt Access

MCP prompts available as slash commands:

```
/mcp__servername__promptname
```

### DSM Custom Tools

DSM-specific MCP tools could include:

| Tool                | Purpose                       |
| ------------------- | ----------------------------- |
| dsm-data-validator  | Validate DataFrame structures |
| dsm-symbol-resolver | Resolve symbol formats        |
| dsm-cache-inspector | Query FCP cache state         |
| dsm-rate-limiter    | Check rate limit status       |

### Tool Development Best Practices

| Practice                 | Rationale                        |
| ------------------------ | -------------------------------- |
| Type all parameters      | Enables Tool Search optimization |
| Include docstrings       | Shown in tool descriptions       |
| Return structured data   | JSON-serializable for processing |
| Handle errors gracefully | Return error info, don't crash   |
| Log operations           | Debug via STDIO stderr           |

## Memory Management & Session Persistence

Comprehensive memory hierarchy and session management for Claude Code.

### Memory Hierarchy (Priority Order)

| Memory Type    | Location                                            | Scope           | Shared With    |
| -------------- | --------------------------------------------------- | --------------- | -------------- |
| Managed Policy | `/Library/Application Support/ClaudeCode/CLAUDE.md` | Organization    | All users      |
| Project Memory | `./CLAUDE.md` or `./.claude/CLAUDE.md`              | Team-shared     | Source control |
| Project Rules  | `./.claude/rules/*.md`                              | Modular rules   | Source control |
| User Memory    | `~/.claude/CLAUDE.md`                               | Personal global | Just you       |
| Local Memory   | `./CLAUDE.local.md`                                 | Personal local  | Just you       |

### Memory File Discovery

Claude Code reads memories recursively:

1. Starts in current working directory
2. Recurses up to (not including) root `/`
3. Discovers CLAUDE.md files in subtrees when reading files there
4. Uses `--add-dir` flag for additional directories

### CLAUDE.md Imports

Reference files with `@path/to/import` syntax:

```markdown
See @README for project overview and @package.json for npm commands.

# Additional Instructions

- Git workflow @docs/git-instructions.md
- Personal prefs @~/.claude/my-project-instructions.md
```

Import limits:

- Relative and absolute paths allowed
- Max depth: 5 hops recursive imports
- Not evaluated inside code spans/blocks
- View loaded memories with `/memory`

### Context Rules with Path Scoping

Rules can target specific files with YAML frontmatter:

```yaml
---
paths:
  - "src/api/**/*.ts"
  - "lib/**/*.ts"
---
# API Development Rules
- All endpoints must include input validation
```

Glob patterns supported:

| Pattern             | Matches                       |
| ------------------- | ----------------------------- |
| `**/*.ts`           | All TypeScript files anywhere |
| `src/**/*`          | All files under src/          |
| `*.md`              | Markdown files in root        |
| `src/**/*.{ts,tsx}` | Both .ts and .tsx in src/     |

### Session Management

| Command              | Purpose                           |
| -------------------- | --------------------------------- |
| `claude --continue`  | Continue most recent conversation |
| `claude --resume ID` | Resume specific session by ID     |
| `/resume`            | Open session picker               |
| `/rename name`       | Name current session              |

Session storage location: `~/.claude/projects/`

### Session Picker Shortcuts

| Shortcut | Action                    |
| -------- | ------------------------- |
| `↑`/`↓`  | Navigate sessions         |
| `→`/`←`  | Expand/collapse grouped   |
| `Enter`  | Select and resume         |
| `P`      | Preview session content   |
| `R`      | Rename session            |
| `/`      | Search filter             |
| `A`      | Toggle all projects       |
| `B`      | Filter current git branch |

### Context Compaction

Claude Code auto-compacts at 95% capacity:

| Command    | Purpose                       |
| ---------- | ----------------------------- |
| `/compact` | Manually compact conversation |
| `/clear`   | Clear all context             |
| `/cost`    | Check token usage             |

Best practice: Every 30-45 min check `/cost`, compact if >50k tokens.

### Git Worktrees for Parallel Sessions

Run multiple Claude sessions with code isolation:

```bash
# Create worktree with new branch
git worktree add ../project-feature-a -b feature-a

# Run Claude in worktree
cd ../project-feature-a && claude

# List all worktrees
git worktree list

# Remove when done
git worktree remove ../project-feature-a
```

Benefits:

- Independent file state per worktree
- Shared Git history and remotes
- No interference between Claude instances

## Multi-File Refactoring Patterns

Coordinated editing across multiple files in a single operation.

### Plan Mode for Complex Changes

Plan Mode uses read-only operations before proposing changes.

**Activate Plan Mode:**

| Method         | Command                                    |
| -------------- | ------------------------------------------ |
| During session | `Shift+Tab` (cycles modes)                 |
| Start in plan  | `claude --permission-mode plan`            |
| Headless query | `claude --permission-mode plan -p "query"` |

Plan Mode indicator: `⏸ plan mode on`

### Refactoring Workflow

```
> I need to refactor our authentication system to use OAuth2.
> Create a detailed migration plan.
```

Claude analyzes then creates comprehensive plan. Refine with:

```
> What about backward compatibility?
> How should we handle database migration?
```

Edit plan directly: Press `Ctrl+G` to open in text editor.

### Large-Scale Refactoring

For 100+ file refactors:

1. Claude generates detailed refactoring plan
2. Shows proposed module structure
3. Identifies duplication points
4. Proposes implementation sequence
5. Asks for approval before changing

Example: 193-file refactor consolidating status fields.

### Incremental Refactoring

Claude breaks large tasks into stages:

1. Extract common functionality to shared utilities
2. Improve error handling
3. Update deprecated API usage
4. Run tests after each stage

### Best Practices

| Practice                  | Rationale                    |
| ------------------------- | ---------------------------- |
| Commit before refactoring | Easy rollback via git        |
| Use Plan Mode first       | Review before execution      |
| Request small increments  | Testable changes             |
| Ask about compatibility   | Catch breaking changes early |
| Keep focus narrow         | Avoid scope creep            |

### CLAUDE.md Rules for Refactoring

Add to project CLAUDE.md:

```markdown
# Refactoring Rules

- Never refactor code unless explicitly asked
- Always use Plan Mode for multi-file changes
- Run tests after each refactoring stage
- Preserve backward compatibility unless told otherwise
```

### Custom Refactoring Commands

Create in `.claude/commands/`:

```yaml
---
name: refactor-plan
description: Create refactoring plan with risk assessment
user-invocable: true
---

Analyze the specified code and create a refactoring plan that:
1. Identifies code smells and improvement opportunities
2. Proposes specific changes with rationale
3. Assesses risks for each change
4. Suggests implementation order
5. Lists affected tests
```

### DSM Refactoring Considerations

For data-source-manager specific refactoring:

| Area                 | Consideration                    |
| -------------------- | -------------------------------- |
| FCP protocol         | Maintain cache key compatibility |
| Symbol formats       | Keep market-specific handling    |
| Error hierarchy      | Preserve exception inheritance   |
| DataFrame operations | Maintain Polars compatibility    |
| Rate limiting        | Don't break backoff logic        |

## Streaming Output & Formatting

Control output format for scripts, CI/CD, and automation.

### Output Format Options

| Format      | Flag                          | Output Type             |
| ----------- | ----------------------------- | ----------------------- |
| Text        | `--output-format text`        | Plain text (default)    |
| JSON        | `--output-format json`        | Full conversation log   |
| Stream JSON | `--output-format stream-json` | Real-time NDJSON stream |

### Streaming JSON for Automation

```bash
# Stream output as NDJSON
claude -p "analyze this code" --output-format stream-json

# Include partial messages
claude -p --output-format stream-json --include-partial-messages "query"
```

### Stream Chaining

Pipe conversations together:

```bash
# Input format for piped conversations
claude -p --output-format json --input-format stream-json
```

### Parsing with jq

Extract text content in real-time:

```bash
claude -p "query" --output-format stream-json | jq -j '.content // empty'
```

### CI/CD Integration

```json
// package.json
{
  "scripts": {
    "lint:claude": "claude -p 'linter: report issues as filename:line description'"
  }
}
```

### Budget and Turn Limits

| Flag               | Purpose              |
| ------------------ | -------------------- |
| `--max-budget-usd` | Cap spending per run |
| `--max-turns`      | Limit agentic turns  |

```bash
claude -p --max-budget-usd 5.00 --max-turns 10 "complex task"
```

### Verbose Mode

Enable turn-by-turn logging:

```bash
claude --verbose -p "query"
```

Toggle in interactive mode: `Ctrl+O`

### System Prompt Customization

| Flag                          | Behavior              | Modes               |
| ----------------------------- | --------------------- | ------------------- |
| `--system-prompt`             | Replace entire prompt | Interactive + Print |
| `--system-prompt-file`        | Replace from file     | Print only          |
| `--append-system-prompt`      | Append to default     | Interactive + Print |
| `--append-system-prompt-file` | Append from file      | Print only          |

### DSM CLI Usage

```bash
# Run DSM analysis with JSON output
claude -p "analyze FCP cache hit rate for BTCUSDT" --output-format json > analysis.json

# Pipe build errors to Claude
cat build-error.txt | claude -p 'explain root cause' > diagnosis.txt
```

## Vision & Multimodal Analysis

Using Claude Code's image understanding capabilities.

### Adding Images to Conversations

| Method        | How to Use                                |
| ------------- | ----------------------------------------- |
| Drag and drop | Drag image into Claude Code window        |
| Paste         | Copy image, paste with Ctrl+V (not Cmd+V) |
| File path     | "Analyze this: /path/to/image.png"        |

### Image Limits

| Limit                  | Value             |
| ---------------------- | ----------------- |
| Max size               | 8000x8000 px      |
| Max images (API)       | 100 per request   |
| Max images (claude.ai) | 20 per request    |
| Multi-image limit      | 2000x2000 px each |

### Analysis Prompts

```
> What does this image show?
> Describe the UI elements in this screenshot
> Are there any problematic elements in this diagram?
```

### Error Screenshot Analysis

```
> Here's a screenshot of the error. What's causing it?
```

### Design to Code

```
> Generate CSS to match this design mockup
> What HTML structure would recreate this component?
```

### Chart and Diagram Analysis

```
> Analyze this architecture diagram
> Extract data from this chart
> Review this database schema diagram
```

### OCR Capabilities

Claude can accurately transcribe text from:

- Retail receipts and labels
- Logistics documents
- Financial statements
- Screenshots with text

### Limitations

| Limitation         | Details                           |
| ------------------ | --------------------------------- |
| No person ID       | Cannot identify named individuals |
| Low quality images | May hallucinate details           |
| Rotated images     | Reduced accuracy                  |
| Small images       | Issues under 200 pixels           |

### Opening Images

When Claude references images (e.g., `[Image #1]`):

- macOS: `Cmd+Click` to open in default viewer
- Windows/Linux: `Ctrl+Click` to open

### DSM Vision Use Cases

| Use Case              | Application                     |
| --------------------- | ------------------------------- |
| Chart analysis        | Analyze candlestick screenshots |
| Error screenshots     | Debug from terminal screenshots |
| API response viewing  | Analyze JSON viewer screenshots |
| Architecture diagrams | Review FCP flow diagrams        |

## @ File Reference Syntax

Efficiently include file content in conversations without tool calls.

### Basic Syntax

```
@path/to/file.js          # Include file content
@src/components/          # List directory contents
@README.md                # Project root file
```

### Tab Completion

Type `@` followed by partial path, then Tab to autocomplete:

```
@src/comp<Tab>   # Completes to @src/components/
@pack<Tab>       # Completes to @package.json
```

### CLAUDE.md Imports

Import files into memory with @ syntax:

```markdown
See @README.md for project overview
See @docs/api-patterns.md for API conventions
See @package.json for available npm scripts

# Personal preferences

- @~/.claude/my-project-instructions.md
```

### Import Characteristics

| Property    | Behavior                        |
| ----------- | ------------------------------- |
| Paths       | Relative or absolute allowed    |
| Max depth   | 5 hops recursive                |
| Code blocks | Not evaluated inside code spans |
| View loaded | Use `/memory` command           |

### Nested Directory Discovery

```
project/
├── CLAUDE.md              # Loaded at launch
├── src/
│   └── CLAUDE.md          # Loaded when reading src/ files
└── tests/
    └── CLAUDE.md          # Loaded when reading tests/ files
```

### MCP Resource References

```
@github:repos/owner/repo/issues      # GitHub MCP
@server:protocol://resource/path     # Generic MCP
```

### Rules vs Imports

| Approach                | Use Case                   |
| ----------------------- | -------------------------- |
| `.claude/rules/`        | Auto-loaded modular rules  |
| `@imports` in CLAUDE.md | Explicit referenced files  |
| CLAUDE.local.md         | Personal local preferences |

### Best Practices

| Practice                    | Rationale                     |
| --------------------------- | ----------------------------- |
| Use sparingly               | Avoid creating reference maze |
| Consistent @ prefix         | Deterministic loading         |
| Critical instructions first | More prominent in CLAUDE.md   |
| Tab completion              | Faster than typing full paths |

### DSM @ Reference Examples

```markdown
# In CLAUDE.md

See @docs/GLOSSARY.md for domain terminology
See @src/core/errors.py for exception hierarchy
See @.claude/rules/fcp-protocol.md for FCP details
```

## Chrome Browser Integration

Web automation directly from Claude Code terminal.

### Prerequisites

| Requirement       | Version                                       |
| ----------------- | --------------------------------------------- |
| Google Chrome     | Latest stable                                 |
| Claude for Chrome | See [Chrome Web Store][chrome-ext] for latest |
| Claude Code CLI   | Run `claude update` for latest                |
| Plan              | Pro, Team, or Enterprise                      |

[chrome-ext]: https://chromewebstore.google.com/detail/claude/fcoeoabgfenejglbffodgkkbkcdhcgfn

### Enable Chrome Integration

```bash
# Start with Chrome flag
claude --chrome

# Or enable during session
> /chrome
```

### Capabilities

| Capability          | Description                        |
| ------------------- | ---------------------------------- |
| Live debugging      | Read console errors, fix code      |
| Design verification | Compare UI to Figma mocks          |
| Web app testing     | Form validation, visual regression |
| Authenticated apps  | Google Docs, Gmail, Notion, etc.   |
| Data extraction     | Scrape structured information      |
| Task automation     | Data entry, form filling           |
| Session recording   | Record GIFs of interactions        |

### Example Workflows

**Test local web app:**

```
I updated login form validation. Open localhost:3000, submit with
invalid data, and check error messages appear correctly.
```

**Debug with console logs:**

```
Open the dashboard and check console for errors on page load.
```

**Automate form filling:**

```
Read contacts.csv and for each row, go to crm.example.com,
click "Add Contact", fill name, email, phone fields.
```

**Extract data:**

```
Go to product listings, extract name, price, availability
for each item. Save as CSV.
```

**Record demo GIF:**

```
Record a GIF showing checkout flow from cart to confirmation.
```

### Browser Commands

Check available tools with `/mcp` → `claude-in-chrome`:

- Navigate pages
- Click and type
- Fill forms
- Scroll
- Read console logs
- Monitor network requests
- Manage tabs
- Resize windows
- Record GIFs

### Best Practices

| Practice                | Rationale                       |
| ----------------------- | ------------------------------- |
| Dismiss modal dialogs   | JS alerts block Claude commands |
| Use fresh tabs          | Avoid unresponsive tab issues   |
| Filter console output   | Reduce verbose logging noise    |
| Enable only when needed | Reduces context usage           |

### Troubleshooting

| Issue                  | Solution                       |
| ---------------------- | ------------------------------ |
| Extension not found    | Check versions, restart Chrome |
| Browser not responding | Dismiss modals, use new tab    |
| Permission errors      | Restart Chrome after install   |

### Enable by Default

```
> /chrome
> Select "Enabled by default"
```

Note: Increases context usage since browser tools always loaded.

### Limitations

| Limitation        | Details                       |
| ----------------- | ----------------------------- |
| Chrome only       | No Brave, Arc, other Chromium |
| WSL not supported | Windows Subsystem for Linux   |
| Visible window    | No headless mode              |
| Modal blocking    | JS alerts pause commands      |

## SDK & Programmatic Usage

Run Claude Code programmatically from CLI, Python, or TypeScript.

### SDK Options

| Interface  | Use Case                      |
| ---------- | ----------------------------- |
| CLI (`-p`) | Scripts, CI/CD, automation    |
| Python SDK | Native integration, callbacks |
| TypeScript | Node.js apps, web services    |

### Basic Headless Mode

```bash
# Single query
claude -p "What does the auth module do?"

# With tool permissions
claude -p "Fix the bug in auth.py" --allowedTools "Read,Edit,Bash"
```

### Structured Output

```bash
# JSON output with metadata
claude -p "Summarize project" --output-format json

# JSON Schema validation
claude -p "Extract function names" \
  --output-format json \
  --json-schema '{"type":"object","properties":{"functions":{"type":"array","items":{"type":"string"}}}}'
```

### Parse with jq

```bash
# Extract text result
claude -p "Summarize" --output-format json | jq -r '.result'

# Extract structured output
claude -p "Extract names" --output-format json --json-schema '...' | jq '.structured_output'
```

### Auto-Approve Tools

```bash
# Allow specific tools without prompting
claude -p "Run tests and fix failures" --allowedTools "Bash,Read,Edit"

# Permission rule syntax with wildcards
claude -p "Create commit" --allowedTools "Bash(git diff *),Bash(git commit *)"
```

### Continue Conversations

```bash
# Continue most recent
claude -p "Review codebase"
claude -p "Focus on database queries" --continue

# Resume by session ID
session_id=$(claude -p "Start review" --output-format json | jq -r '.session_id')
claude -p "Continue review" --resume "$session_id"
```

### Custom System Prompts

```bash
# Append to default (preserves Claude Code behavior)
gh pr diff "$1" | claude -p \
  --append-system-prompt "You are a security engineer. Review for vulnerabilities."

# Replace entire prompt
claude -p "Query" --system-prompt "You are a Python expert"
```

### Limits and Budget

```bash
# Set spending limit
claude -p "Complex task" --max-budget-usd 5.00

# Limit agentic turns
claude -p "Task" --max-turns 10
```

### DSM Programmatic Examples

```bash
# Analyze FCP cache
claude -p "Analyze FCP cache hits for BTCUSDT last 7 days" \
  --allowedTools "Read,Bash(uv run *)" \
  --output-format json > fcp-analysis.json

# CI integration
claude -p "Run pytest and fix failures" \
  --allowedTools "Bash(uv run pytest *),Read,Edit" \
  --max-turns 5
```

## Prompt Caching Optimization

Optimize cost and latency with automatic prompt caching.

### How Caching Works

Claude Code automatically enables prompt caching:

1. Static content (tools, instructions) cached at prompt start
2. Cache TTL: 5 minutes
3. Subsequent requests reuse cached prefix
4. Monitor with `/cost` command

### Cost Savings

| Operation   | Cost vs Base      |
| ----------- | ----------------- |
| Cache write | 125% (base + 25%) |
| Cache read  | 10% of base       |
| No cache    | 100% (base price) |

Potential savings: Up to 90% on repeated prompts.

### Minimum Cache Sizes

| Model Family      | Min Tokens |
| ----------------- | ---------- |
| Claude Opus 4.x   | 1024       |
| Claude Sonnet 4.x | 1024       |
| Claude Haiku 3.5  | 2048       |

### Cache Optimization Strategies

| Strategy                  | Benefit                       |
| ------------------------- | ----------------------------- |
| Static content first      | Maximizes cacheable prefix    |
| Tool definitions early    | Tools cached across requests  |
| System instructions start | Instructions cached           |
| Context before queries    | Context reused for follow-ups |

### MCP and Caching

MCP servers benefit significantly from caching:

- Complex tool schemas cached
- External knowledge cached
- Reduces context reprocessing

### Cache Breakpoints

Mark cacheable sections with `cache_control`:

```json
{
  "type": "text",
  "text": "...",
  "cache_control": { "type": "ephemeral" }
}
```

### Latency Reduction

Cache hits reduce latency by up to 85% by avoiding:

- Token processing for cached content
- Re-embedding of static context
- Repeated tool schema parsing

### DSM Caching Considerations

| Content             | Caching Value                |
| ------------------- | ---------------------------- |
| FCP protocol docs   | High - static, reused often  |
| Symbol format rules | High - static reference      |
| DataFrame schemas   | Medium - varies by operation |
| Error hierarchy     | High - stable, referenced    |
| Rate limit configs  | Low - may change frequently  |

### Monitoring Cache Performance

```bash
# Check token usage and cache stats
> /cost

# Output includes:
# - Input tokens (cached vs uncached)
# - Cache write/read costs
# - Total session cost
```

## Testing & Test Generation

AI-powered test creation, execution, and debugging.

### Test Generation Prompts

```
> Find functions in UserService.py not covered by tests
> Add tests for the authentication module
> Add test cases for edge conditions
> Run new tests and fix any failures
```

### Specialized Test Agents

Playwright ships specialized agents for Claude Code:

| Agent     | Purpose                           |
| --------- | --------------------------------- |
| Planner   | Exploration strategy              |
| Generator | Test code generation              |
| Healer    | Fix failures (up to 5 iterations) |

### Bounded QA Agents

Separation of concerns for QA:

| Agent    | Responsibility        |
| -------- | --------------------- |
| Analyst  | Feature analysis only |
| Sentinel | Auditing only         |
| Healer   | Debugging only        |

### Test-Driven Development (TDD)

```
> Write failing tests for the new feature first
> [Claude generates test cases]
> Now implement the feature to make tests pass
```

### Edge Case Generation

Claude generates comprehensive edge cases:

- Malformed JSON responses
- Empty responses
- Special characters in IDs
- Extremely long URLs (414 errors)
- Timeout scenarios
- Concurrent access

### Debugging Loop

When tests fail, Claude:

1. Reads test output and error logs
2. Identifies failure type (selector, timeout, assertion)
3. Suggests specific fixes
4. Updates broken selectors
5. Adds waits where needed
6. Re-runs failed tests

### Test Commands

Create in `.claude/commands/`:

```yaml
---
name: run-tests
description: Run tests and fix failures
user-invocable: true
---

Run the test suite with `uv run pytest`.
If any tests fail:
1. Analyze the failure output
2. Identify root cause
3. Fix the issue
4. Re-run tests
5. Repeat until all pass or 3 iterations
```

### DSM Testing Patterns

| Test Area         | Approach                         |
| ----------------- | -------------------------------- |
| FCP behavior      | Mock cache, verify decisions     |
| Rate limiting     | Test backoff with mock responses |
| Symbol validation | Property-based testing           |
| DataFrame ops     | Fixture-based with known data    |
| Error handling    | Verify exception hierarchy       |

## Documentation Generation

Automated documentation from code analysis.

### Documentation Types

| Type              | Example                        |
| ----------------- | ------------------------------ |
| Module docstrings | File-level purpose explanation |
| Function docs     | Parameters, returns, examples  |
| Inline comments   | Complex logic explanation      |
| API documentation | OpenAPI/Swagger generation     |
| Architecture docs | High-level system overview     |

### Generation Prompts

```
> Add JSDoc comments to undocumented functions in auth.js
> Generate Javadoc-style comments for all methods
> Add inline comments explaining the FCP decision logic
> Create API documentation for the data endpoints
```

### Comment Directives

Add directives in CLAUDE.md:

```markdown
# Comment Directives

When you see @implement in comments:

1. Use comment instructions to implement changes
2. Convert comment blocks to documentation blocks
3. Remove @implement after completion
```

### Code Commenting Example

Before:

```python
def fetch_data(symbol, start, end):
    # Complex logic here
    ...
```

After Claude documentation:

```python
def fetch_data(symbol: str, start: datetime, end: datetime) -> DataFrame:
    """
    Fetch historical OHLCV data for a symbol.

    Args:
        symbol: Trading pair symbol (e.g., "BTCUSDT")
        start: Start datetime (UTC)
        end: End datetime (UTC)

    Returns:
        Polars DataFrame with columns: open_time, open, high, low, close, volume

    Raises:
        RateLimitError: If API rate limit exceeded
        SymbolNotFoundError: If symbol doesn't exist
    """
    ...
```

### Documentation Standards

| Standard     | Languages      | Claude Support               |
| ------------ | -------------- | ---------------------------- |
| JSDoc        | JavaScript, TS | Full                         |
| Javadoc      | Java           | Full                         |
| Docstring    | Python         | Full (Google, NumPy, Sphinx) |
| RustDoc      | Rust           | Full                         |
| XML comments | C#             | Full                         |

### Iterative Improvement

1. Ask Claude to analyze undocumented code
2. Review initial documentation output
3. Refine with specific feedback
4. Request consistency checks

### DSM Documentation Patterns

| Area              | Documentation Focus           |
| ----------------- | ----------------------------- |
| FCP protocol      | Decision flow, cache keys     |
| Error hierarchy   | When to use each exception    |
| Symbol formats    | Market-specific examples      |
| DataFrame columns | Column names, types, meanings |
| Rate limiting     | Backoff strategy explanation  |

## Legacy Code Modernization

Systematic migration from legacy systems to modern architectures.

### Modernization Dimensions

| Dimension      | Transformation                 |
| -------------- | ------------------------------ |
| Architecture   | Monolith → Microservices       |
| Infrastructure | On-premise → Cloud-native      |
| Language       | Legacy → Modern (COBOL → Java) |
| Patterns       | Procedural → Object-oriented   |
| Testing        | Manual → Automated             |

### Migration Strategies

| Strategy      | Description                      |
| ------------- | -------------------------------- |
| Strangler Fig | Gradually replace old components |
| Facade Layer  | Create compatibility wrapper     |
| Incremental   | Start small, expand coverage     |
| Parallel Run  | Run old and new simultaneously   |
| Big Bang      | Full replacement (high risk)     |

### Phased Migration Approach

1. **Analysis Phase**: Understand existing codebase
2. **Structure Setup**: Create modern project structure
3. **Data Models**: Translate legacy data structures
4. **I/O Layer**: Build compatible interfaces
5. **Business Logic**: Convert core functionality
6. **Testing**: Dual test harness for validation

### COBOL Migration Example

Claude Code's approach:

```
> Analyze this COBOL program and create a migration plan to Java
> [Claude creates 5-phase plan]
> Start with the data model translation from copybooks to Java classes
> [Claude converts COBOL copybooks to Java POJOs]
```

### Best Practices

| Practice                  | Rationale                       |
| ------------------------- | ------------------------------- |
| Preserve business logic   | Core value of legacy system     |
| Generate regression tests | Verify behavior unchanged       |
| Start with simple CRUD    | Build confidence before complex |
| Maintain backwards compat | Enable gradual rollout          |
| Document as you go        | Capture domain knowledge        |

### Velocity Gains

Organizations report 2-10x velocity improvement with:

- 40-50% faster than manual work
- Maintained quality standards
- Enterprise controls preserved
- Weeks instead of years

### DSM Modernization Considerations

| Legacy Pattern    | Modern Equivalent     |
| ----------------- | --------------------- |
| Pandas DataFrames | Polars DataFrames     |
| requests library  | httpx with async      |
| Manual retries    | tenacity with backoff |
| Print debugging   | Structured logging    |
| Global state      | Dependency injection  |

## Code Review Patterns

AI-assisted pull request review and feedback.

### Review Workflow

```
> Review my staged changes for issues
> Summarize changes and suggest improvements
> Check for security vulnerabilities
> Verify coding standards compliance
```

### Review Types

| Type                | Focus Area                       |
| ------------------- | -------------------------------- |
| Security Review     | Vulnerabilities, injection risks |
| Style Review        | Coding standards, formatting     |
| Logic Review        | Correctness, edge cases          |
| Performance Review  | Optimization opportunities       |
| Architecture Review | Design patterns, coupling        |

### Confidence Scoring

Claude Code uses confidence scoring to:

- Filter out false positives
- Ensure actionable feedback
- Reduce review noise
- Focus on critical issues

### GitHub Actions Integration

```yaml
name: Claude Code Review
on:
  pull_request:
    types: [opened, synchronize]

jobs:
  review:
    runs-on: ubuntu-latest
    steps:
      - uses: anthropics/claude-code-action@v1
        with:
          anthropic_api_key: ${{ secrets.ANTHROPIC_API_KEY }}
```

### Review Prompts

**Concise review:**

```
Review this PR for:
1. Critical bugs that would cause failures
2. Security issues
3. Pass/fail recommendation with emoji
```

**Detailed review:**

```
Analyze this pull request:
1. Summarize the changes
2. Identify potential issues
3. Suggest improvements
4. Check test coverage
```

### Using Memory for Consistency

```
> We discussed the FCP caching pattern earlier
> Does this new code follow the same pattern?
> [Claude remembers and validates consistency]
```

### Auto-Fix Workflow

1. Review comments posted by human reviewer
2. Claude analyzes suggested changes
3. Claude implements fixes
4. Claude pushes updates to PR branch

### DSM Code Review Focus

| Area               | Review Checklist                 |
| ------------------ | -------------------------------- |
| FCP implementation | Cache key consistency, TTL       |
| Error handling     | Exception hierarchy, recovery    |
| Rate limiting      | Backoff parameters, retry logic  |
| Symbol validation  | Format correctness, market check |
| DataFrame ops      | Column types, null handling      |

### Custom Review Agent

Create in `.claude/agents/`:

```yaml
---
name: dsm-reviewer
description: Reviews DSM code for consistency
tools:
  - Read
  - Grep
  - Glob
---

You are a code reviewer for data-source-manager.
Focus on:
- FCP protocol compliance
- Error handling patterns
- Rate limiting correctness
- Symbol format validation
- DataFrame column consistency
```

## Performance & Large Codebase Optimization

Strategies for scaling Claude Code to enterprise codebases.

### Context Window Management

Context is the most important resource to manage:

| Issue                   | Consequence                        |
| ----------------------- | ---------------------------------- |
| Context fills up fast   | Performance degrades               |
| Files + output = tokens | Single session can use 50k+ tokens |
| Full context            | Claude "forgets" instructions      |
| Context rot             | Quality degrades even before limit |

### Context Commands

| Command               | Usage                        |
| --------------------- | ---------------------------- |
| `/cost`               | Check current token usage    |
| `/compact`            | Summarize and reduce context |
| `/compact Focus on X` | Directed compaction          |
| `/clear`              | Reset context entirely       |

### Subagent Delegation

Delegate research to subagents to preserve main context:

```
> Use subagents to investigate the authentication module
```

Benefits:

- Explores in separate context
- Reports findings only
- Main conversation stays clean
- No file content cluttering context

### Chunking Strategies

For large projects (>100k LOC):

1. **Create specs**: 5k token markdown spec of key components
2. **Focus scope**: One directory at a time
3. **Use imports**: CLAUDE.md @ imports for modular context
4. **Worktrees**: Parallel sessions with git worktrees

### Pre-Computation Spec

Before running Claude Code:

```markdown
# Project Spec (5k tokens max)

## Core Module

- Entry point: src/main.py
- Key classes: DataSourceManager, CacheClient
- Patterns: FCP, Repository pattern

## Dependencies

- polars for DataFrames
- httpx for HTTP
- tenacity for retries
```

Then prompt: "Here's the spec. How does the FCP module work?"

### Context Window Sizes

| Tier     | Token Limit |
| -------- | ----------- |
| Standard | 200k tokens |
| Extended | 1M tokens   |

1M tokens enables work across massive codebases without summarization.

### Performance Variance

| Factor         | Impact                    |
| -------------- | ------------------------- |
| Server demand  | Response time fluctuation |
| Model choice   | Opus slower than Sonnet   |
| Session length | Longer = more variance    |
| Time of day    | Peak hours = slower       |

### Model Selection for Performance

| Task                 | Recommended Model   |
| -------------------- | ------------------- |
| Quick queries        | Haiku (fastest)     |
| Standard coding      | Sonnet (balanced)   |
| Complex architecture | Opus (most capable) |
| Cost-sensitive       | Haiku               |
| Quality-critical     | Opus                |

### DSM Performance Patterns

| Pattern                  | Recommendation             |
| ------------------------ | -------------------------- |
| FCP debugging            | Use fcp-debugger agent     |
| Data exploration         | Subagent for reading files |
| Test generation          | Chunk by module            |
| Full codebase review     | Use api-reviewer agent     |
| Rate limit investigation | Focus on single exchange   |

### Memory Optimization

Claude Code 2.1+ includes 3x memory improvement:

- Better token efficiency
- Smarter context summarization
- Improved caching integration

### Scaling Checklist

- [ ] Check `/cost` every 30-45 minutes
- [ ] Use `/compact` at 50k+ tokens
- [ ] Delegate research to subagents
- [ ] Create project spec for large codebases
- [ ] Use worktrees for parallel work
- [ ] Match model to task complexity

## Security Scanning & SAST Integration

AI-powered security review combined with traditional static analysis.

### Built-in Security Review

```
> /security-review
```

Performs comprehensive security review of all pending changes.

### Security Review Capabilities

| Capability            | Description                          |
| --------------------- | ------------------------------------ |
| Contextual analysis   | Understands code semantics/intent    |
| Lower false positives | AI filters non-vulnerable patterns   |
| XSS detection         | Finds obvious cross-site scripting   |
| Injection detection   | Catches short-path command injection |
| Sensitive data        | Identifies credential exposure       |

### GitHub Actions Security Review

```yaml
name: Security Review
on: [pull_request]

jobs:
  security:
    runs-on: ubuntu-latest
    steps:
      - uses: anthropics/claude-code-security-review@v1
        with:
          anthropic_api_key: ${{ secrets.ANTHROPIC_API_KEY }}
```

### AI vs Traditional SAST

| Aspect                   | AI (Claude)   | Traditional SAST |
| ------------------------ | ------------- | ---------------- |
| Contextual understanding | Strong        | Pattern-based    |
| False positives          | Lower         | Higher           |
| Inter-procedural flow    | Weaker        | Stronger         |
| Taint analysis           | Basic         | Comprehensive    |
| Consistency              | Varies by run | Deterministic    |

### Recommended: Combined Approach

Pair Claude with traditional tools:

| Tool Type | Examples                |
| --------- | ----------------------- |
| SAST      | Semgrep, Bandit, CodeQL |
| DAST      | OWASP ZAP, StackHawk    |
| SCA       | Snyk, Dependabot        |

### Claude Secure Coding Rules

Add security rules to CLAUDE.md:

```markdown
# Security Rules

- Never generate code with hardcoded credentials
- Always validate and sanitize user input
- Use parameterized queries for database access
- Apply principle of least privilege
- Encrypt sensitive data at rest and in transit
```

### Known Limitations

| Limitation                | Mitigation                    |
| ------------------------- | ----------------------------- |
| Inconsistent results      | Run multiple scans            |
| Complex vuln detection    | Pair with traditional SAST    |
| Inter-procedural analysis | Use specialized tools         |
| Context compression       | Keep focused scope per review |

### DSM Security Focus Areas

| Area             | Security Concern              |
| ---------------- | ----------------------------- |
| API keys         | Credential exposure in logs   |
| Rate limiting    | DoS vulnerability             |
| Input validation | Symbol format injection       |
| Error messages   | Information disclosure        |
| Cache data       | Sensitive data in cache files |

## API Design & Generation

Claude Code for designing and generating REST APIs and documentation.

### API Generation Workflow

```
> Design a REST API for user authentication with JWT
> [Claude generates OpenAPI spec]
> Generate the Python implementation using FastAPI
> [Claude creates routes, models, validation]
> Add comprehensive API tests
```

### What Claude Generates

| Component     | Output                           |
| ------------- | -------------------------------- |
| OpenAPI spec  | Complete YAML/JSON specification |
| Models        | Pydantic/dataclass definitions   |
| Routes        | Endpoint handlers                |
| Validation    | Input/output schemas             |
| Tests         | API test suite                   |
| Documentation | Inline docs, README              |

### API Documentation Subagent

Create in `.claude/agents/`:

```yaml
---
name: api-documenter
description: Creates OpenAPI specs and SDK docs
tools:
  - Read
  - Write
  - Edit
  - Grep
---

You are an API documentation specialist.
Generate:
- OpenAPI 3.1 specifications
- Interactive API docs
- SDK client libraries
- Code examples for common operations
- Versioning strategy documentation
```

### OpenAPI Refactoring

Claude can extract patterns from large specs:

```
> Analyze this OpenAPI spec and extract common components
> [Claude identifies pagination, response patterns]
> Create reusable components for these patterns
> [Claude refactors to use $ref components]
```

### REST API Best Practices

| Practice        | Claude Implementation           |
| --------------- | ------------------------------- |
| Resource naming | Plural nouns, lowercase         |
| HTTP methods    | GET/POST/PUT/PATCH/DELETE       |
| Status codes    | Appropriate codes per operation |
| Error responses | Consistent error schema         |
| Pagination      | Cursor-based or offset          |
| Versioning      | URL or header based             |

### MCP OpenAPI Integration

Connect API specs via MCP:

```json
{
  "mcpServers": {
    "openapi": {
      "command": "npx",
      "args": ["@anthropics/mcp-openapi", "--spec", "api.yaml"]
    }
  }
}
```

### DSM API Design Patterns

| Endpoint Pattern   | DSM Application              |
| ------------------ | ---------------------------- |
| GET /data/{symbol} | Fetch historical data        |
| GET /symbols       | List available symbols       |
| GET /providers     | List data providers          |
| POST /validate     | Validate DataFrame structure |
| GET /cache/status  | FCP cache statistics         |

### API Architect Agent

```yaml
---
name: api-architect
description: Designs API architecture and patterns
tools:
  - Read
  - Grep
  - Glob
---

You are an API architecture specialist.
Focus on:
- RESTful design principles
- Domain-driven design boundaries
- OAuth2/JWT security patterns
- Rate limiting strategies
- API versioning approaches
```

## Database Schema & Migrations

Safe database schema changes with Claude Code as reviewer and generator.

### Role Definition

| Claude Does             | Claude Does NOT            |
| ----------------------- | -------------------------- |
| Generate migrations     | Apply migrations           |
| Review schema changes   | Access production data     |
| Create backfill scripts | Execute in production      |
| Analyze query plans     | Receive PII/sensitive data |

### Migration Script Generation

```
> Generate a migration to add user_preferences table
> [Claude creates migration with header, ALTER, INDEX, ROLLBACK]
> Make it idempotent and production-ready
```

### Migration Script Components

| Component        | Purpose                     |
| ---------------- | --------------------------- |
| Header comments  | Explain what migration does |
| ALTER TABLE      | Schema modifications        |
| CREATE INDEX     | Performance indexes         |
| ROLLBACK section | Undo instructions           |
| IF NOT EXISTS    | Idempotency checks          |

### Backfill Script Rules

Add to CLAUDE.md:

```markdown
# Database Backfill Rules

When generating backfill scripts:

- Must be idempotent (safe to run multiple times)
- Must avoid long table locks
- Use batch processing for large tables
- Include progress logging
- Never DELETE without WHERE clause
```

### MCP Database Integration

Connect databases via MCP:

```json
{
  "mcpServers": {
    "database": {
      "command": "npx",
      "args": ["@anthropics/mcp-database", "--connection", "postgres://..."]
    }
  }
}
```

Enables:

- Query execution
- Schema analysis
- Index recommendations
- Query plan analysis

### ORM Integration

Claude Code works with ORM migrations:

| ORM     | Migration Command                    |
| ------- | ------------------------------------ |
| Alembic | `alembic revision --autogenerate`    |
| EF Core | `dotnet ef migrations add`           |
| Django  | `python manage.py makemigrations`    |
| TypeORM | `npm run typeorm migration:generate` |

### DSM Database Patterns

| Pattern       | Application                |
| ------------- | -------------------------- |
| Cache tables  | FCP cache metadata storage |
| Time-series   | OHLCV data storage         |
| Config tables | Provider configuration     |
| Audit logs    | Data fetch history         |

## DevOps & Infrastructure Automation

CI/CD, IaC, and deployment automation with Claude Code.

### DevOps Capabilities

| Capability           | Description                |
| -------------------- | -------------------------- |
| CI/CD pipelines      | GitHub Actions, GitLab CI  |
| Container management | Docker, Kubernetes configs |
| IaC generation       | Terraform, CloudFormation  |
| Server management    | SSH, configuration files   |
| Monitoring setup     | Prometheus, Grafana        |

### CI/CD Pipeline Generation

```
> Create a GitHub Actions workflow for Python with:
> - Testing with pytest
> - Linting with ruff
> - Publishing to PyPI on release
```

Claude generates complete pipeline configurations.

### Deployment Strategies

| Strategy       | Use Case                        |
| -------------- | ------------------------------- |
| Blue-green     | Zero-downtime production        |
| Canary release | Gradual rollout with monitoring |
| Rolling update | Kubernetes default              |
| Feature flags  | Controlled feature exposure     |

### Terraform Skill

Create in `.claude/commands/`:

```yaml
---
name: terraform-plan
description: Generate Terraform for infrastructure
user-invocable: true
---
Generate Terraform configuration following:
  - Module-based organization
  - Remote state with locking
  - Environment-specific tfvars
  - Output documentation
  - Resource tagging standards
```

### DevOps Engineer Agent

```yaml
---
name: devops-engineer
description: CI/CD, Docker, Kubernetes, cloud infrastructure
tools:
  - Read
  - Write
  - Edit
  - Bash
  - Grep
---

You are a DevOps specialist.
Focus on:
- CI/CD pipeline design
- Container orchestration
- Infrastructure as Code
- Monitoring and alerting
- Security best practices
```

### Troubleshooting Deployments

Claude Code can:

- Analyze deployment logs
- Identify error patterns
- Suggest solutions
- Generate rollback scripts

### DSM DevOps Patterns

| Area               | Pattern                      |
| ------------------ | ---------------------------- |
| Testing            | Local-first (no GH Actions)  |
| Package management | uv for Python dependencies   |
| Task runner        | mise tasks for orchestration |
| Environment        | mise [env] for config        |
| Secrets            | Doppler for credentials      |

### Skills Ecosystem

500+ DevOps skills available:

| Category       | Examples                     |
| -------------- | ---------------------------- |
| Infrastructure | Terraform, Pulumi, Ansible   |
| Containers     | Docker, Kubernetes, Helm     |
| CI/CD          | GitHub Actions, GitLab CI    |
| Monitoring     | Prometheus, Datadog, Grafana |
| Security       | Vault, SOPS, AWS Secrets     |

## Monorepo & Multi-Package Patterns

Optimizing Claude Code for polyglot monorepos with multiple packages.

### CLAUDE.md Hierarchy for Monorepos

```
monorepo/
├── CLAUDE.md              # Root: 300 lines max, navigation hub
├── packages/
│   ├── core/
│   │   └── CLAUDE.md      # Core-specific context
│   ├── api/
│   │   └── CLAUDE.md      # API-specific context
│   └── web/
│       └── CLAUDE.md      # Web-specific context
├── .claude/
│   ├── rules/             # Shared rules (auto-loaded)
│   ├── agents/            # Shared agents
│   └── commands/          # Shared commands
└── docs/
    └── CLAUDE.md          # Docs-specific context
```

### Context Optimization

| Metric            | Target                    |
| ----------------- | ------------------------- |
| Root CLAUDE.md    | < 300 lines               |
| Package CLAUDE.md | < 10k words each          |
| Fresh session     | ~20k tokens (10% of 200k) |
| Available context | 180k for actual work      |

### Splitting Strategy

| Don't                      | Do                          |
| -------------------------- | --------------------------- |
| 47k word single file       | Split into package contexts |
| Load all contexts          | Load only relevant context  |
| Frontend guides in backend | Domain-specific separation  |

### Hub-Spoke Navigation

Root CLAUDE.md as navigation hub:

```markdown
# Monorepo Navigation

**Packages**: [core](packages/core/CLAUDE.md) | [api](packages/api/CLAUDE.md) | [web](packages/web/CLAUDE.md)

**Docs**: [INDEX](docs/INDEX.md) | [GLOSSARY](docs/GLOSSARY.md)

## Quick Reference

- Build: `mise run build`
- Test: `mise run test`
- Lint: `mise run lint`
```

### Cross-Package Dependencies

Track with workspace tools:

| Tool  | Detection                     |
| ----- | ----------------------------- |
| npm   | package.json workspaces       |
| Lerna | lerna.json packages           |
| Rush  | rush.json projects            |
| Nx    | nx.json implicit dependencies |
| uv    | uv.lock workspace resolution  |

### Monorepo MCP Configuration

Root `.mcp.json` for shared tools:

```json
{
  "mcpServers": {
    "workspace": {
      "command": "npx",
      "args": ["@anthropics/mcp-workspace"]
    }
  }
}
```

### Context Check Command

Monitor context usage during sessions:

```
> /context
```

Shows:

- Current token usage
- Loaded CLAUDE.md files
- Active rules
- Available headroom

### Best Practices

| Practice                   | Benefit                      |
| -------------------------- | ---------------------------- |
| Package-specific CLAUDE.md | Focused context per domain   |
| Shared .claude/ directory  | Reusable agents and commands |
| Hub-spoke navigation       | Easy cross-package discovery |
| @ imports for shared docs  | Modular loading              |
| mise tasks at root         | Unified command interface    |

### DSM Monorepo Structure

DSM follows monorepo patterns even as a single package:

```
data-source-manager/
├── CLAUDE.md              # Root: Quick reference
├── src/
│   └── CLAUDE.md          # Source code context
├── tests/
│   └── CLAUDE.md          # Testing context
├── docs/
│   └── CLAUDE.md          # Documentation context
├── examples/
│   └── CLAUDE.md          # Examples context
└── .claude/
    ├── agents/            # 5 specialized agents
    ├── commands/          # 6 slash commands
    ├── rules/             # 7 context rules
    └── hooks/             # 5 lifecycle hooks
```

### Scaling to True Monorepo

If DSM becomes a monorepo:

| Future Package | CLAUDE.md Focus           |
| -------------- | ------------------------- |
| @dsm/core      | FCP, caching, core types  |
| @dsm/binance   | Binance-specific patterns |
| @dsm/okx       | OKX-specific patterns     |
| @dsm/cli       | CLI commands, arguments   |
| @dsm/mcp       | MCP server implementation |

## Codebase Exploration & Semantic Search

Natural language queries for understanding and navigating codebases.

### Built-in Exploration

Claude Code understands codebase structure natively:

```
> Give me an overview of this codebase
> Explain the main architecture patterns used here
> What are the key data models?
> How is authentication handled?
```

### Explore Subagent

Use the Explore subagent for efficient search:

```
> Use the Explore subagent to find all error handling code
```

Benefits:

- Powered by Haiku (fast, efficient)
- Saves main conversation context
- Reports findings only

### Semantic Search MCP

Enhance search with semantic MCP plugins:

```json
{
  "mcpServers": {
    "claude-context": {
      "command": "npx",
      "args": ["@zilliztech/claude-context", "--index", "."]
    }
  }
}
```

Features:

- Hybrid search (BM25 + dense vector)
- Semantic code understanding
- Million-line codebase support

### Local Semantic Search

For no-API-cost option:

```json
{
  "mcpServers": {
    "context-local": {
      "command": "npx",
      "args": ["claude-context-local"]
    }
  }
}
```

Uses local embeddings with Google's EmbeddingGemma.

### Query Patterns

| Query Type   | Example                                  |
| ------------ | ---------------------------------------- |
| Architecture | "How does data flow through the system?" |
| Dependencies | "What does X depend on?"                 |
| Usage        | "Where is function Y called?"            |
| Patterns     | "What patterns are used for caching?"    |
| Changes      | "What would I need to change to add Z?"  |

### Exploration Workflow

1. **Start broad**: "Give me an overview of this codebase"
2. **Narrow down**: "Explain the authentication module"
3. **Deep dive**: "How does token refresh work?"
4. **Find code**: "Show me where tokens are validated"

### DSM Exploration Examples

| Query                              | Purpose                   |
| ---------------------------------- | ------------------------- |
| "How does FCP decide to fetch?"    | Understand caching logic  |
| "Where are symbols validated?"     | Find validation code      |
| "What happens on rate limit?"      | Trace error handling      |
| "How are DataFrames constructed?"  | Understand data pipeline  |
| "Where is the Binance API called?" | Find provider integration |

### Indexing for Large Codebases

For codebases over 100k LOC:

1. Pre-index with semantic search MCP
2. Use hybrid search (keyword + semantic)
3. Focus queries on specific domains
4. Leverage CLAUDE.md hierarchy

## Pair Programming Workflows

Human-AI collaboration patterns for effective development.

### Collaboration Modes

| Mode              | Use Case                             |
| ----------------- | ------------------------------------ |
| Interactive       | Permission prompts, review each step |
| Auto-accept edits | Trust Claude for simple tasks        |
| Full autonomy     | Isolated environment, sandboxed      |

### Progression Pattern

Start conservative, increase autonomy:

1. **Beginner**: Check everything Claude does
2. **Intermediate**: Auto-accept for trusted patterns
3. **Advanced**: Full autonomy in isolated environments

### Plan Mode Workflow

```
1. Start in Plan Mode (Shift+Tab or --permission-mode plan)
2. Iterate on plan until satisfied
3. Switch to auto-accept edits mode
4. Claude executes the plan (often 1-shot)
```

### Test-Driven Pair Programming

```
> Write tests based on these expected input/output pairs:
> Input: "BTCUSDT" -> Output: {"symbol": "BTCUSDT", "market": "futures"}
> Use TDD - write tests first, then implement
```

Being explicit about TDD prevents mock implementations.

### Team CLAUDE.md Practices

| Practice          | Benefit                    |
| ----------------- | -------------------------- |
| Document mistakes | Claude learns from errors  |
| Style conventions | Consistent code generation |
| Design guidelines | Architectural alignment    |
| @.claude PR tags  | Continuous improvement     |

### IDE Integration Benefits

| IDE       | Features                              |
| --------- | ------------------------------------- |
| VS Code   | Inline diffs, @-mentions, plan review |
| JetBrains | IDE diff viewing, context sharing     |
| Terminal  | Full CLI power, scripting             |

### Safe Autonomy Environments

For full autonomous mode:

- Isolated compute (VM or container)
- Isolated user account
- Docker image boundaries
- Sandboxed filesystem

### Collaboration Anti-Patterns

| Avoid                   | Instead                       |
| ----------------------- | ----------------------------- |
| Giving Claude free rein | Define clear boundaries       |
| Ignoring CLAUDE.md      | Document learnings            |
| Skip code review        | Review autonomous changes     |
| No rollback plan        | Commit before autonomous work |

### DSM Pair Programming Tips

| Task          | Recommended Approach         |
| ------------- | ---------------------------- |
| FCP changes   | Plan Mode + careful review   |
| Test writing  | Auto-accept after TDD prompt |
| Documentation | Auto-accept with style guide |
| New providers | Plan Mode + incremental      |
| Bug fixes     | Interactive for diagnosis    |

## Error Handling & Resilience Patterns

Retry logic, circuit breakers, and graceful degradation.

### Retry Pattern Components

| Component           | Purpose                                |
| ------------------- | -------------------------------------- |
| Max retries         | Prevent infinite loops (3-5)           |
| Exponential backoff | Increasing delay between retries       |
| Jitter              | ±25% randomness avoids thundering herd |
| Circuit breaker     | Stop retrying after threshold          |

### Exponential Backoff Implementation

```python
# DSM pattern for API retries
import tenacity

@tenacity.retry(
    wait=tenacity.wait_exponential(multiplier=1, min=1, max=60),
    stop=tenacity.stop_after_attempt(5),
    retry=tenacity.retry_if_exception_type(RateLimitError)
)
def fetch_with_retry():
    ...
```

### Rate Limit Handling (429 Errors)

| Strategy                | Benefit                  |
| ----------------------- | ------------------------ |
| Read retry-after header | Exact wait time          |
| Prompt caching          | 5x effective throughput  |
| Request queuing         | Smooth out bursts        |
| Tier optimization       | Higher limits with spend |

### Circuit Breaker States

```
CLOSED → OPEN → HALF-OPEN → CLOSED
   ↓        ↓        ↓
Normal   Blocking   Testing
```

- **CLOSED**: Normal operation
- **OPEN**: All requests fail fast
- **HALF-OPEN**: Test recovery

### Graceful Degradation

| Failure         | Degradation Strategy        |
| --------------- | --------------------------- |
| API timeout     | Return cached data          |
| Rate limit      | Queue and retry later       |
| Parse error     | Log and skip malformed      |
| Network failure | Exponential backoff + alert |

### Error Recovery Skill

Add to `.claude/commands/`:

```yaml
---
name: recover-errors
description: Diagnose and fix error patterns
user-invocable: true
---

Analyze error patterns in the codebase:
1. Find bare except clauses
2. Find missing error handling
3. Identify missing retries on network calls
4. Suggest improvements with proper patterns
```

### DSM Error Handling Rules

Add to CLAUDE.md:

```markdown
# Error Handling

- Always use specific exception types
- Implement retry with exponential backoff for API calls
- Log errors with structured context
- Never swallow exceptions silently
- Return FetchResult with failure info, not None
```

### DSM-Specific Patterns

| Error Type          | Handling Pattern             |
| ------------------- | ---------------------------- |
| RateLimitError      | Exponential backoff + cache  |
| SymbolNotFoundError | Return empty DataFrame + log |
| NetworkError        | Retry with circuit breaker   |
| ParseError          | Log + skip + alert           |
| CacheError          | Fallback to fresh fetch      |

### Known Issue: Rate Limit as Success

Claude Code returns "You've hit your limit" with exit code 0.

Mitigation:

- Check output content, not just exit code
- Parse for rate limit messages
- Implement output validation

## Cost Optimization & Token Reduction

Strategies for managing Claude Code costs effectively.

### Cost Benchmarks

| Metric                | Value                  |
| --------------------- | ---------------------- |
| Average daily cost    | $6 per developer       |
| 90th percentile daily | < $12                  |
| Monthly average       | $100-200 per developer |

### Model Pricing (per million tokens)

| Model         | Input | Output |
| ------------- | ----- | ------ |
| Claude Haiku  | $1    | $5     |
| Claude Sonnet | $3    | $15    |
| Claude Opus   | $5    | $25    |

### Model Selection Strategy

| Task Complexity   | Recommended Model | Cost Impact |
| ----------------- | ----------------- | ----------- |
| 80% of work       | Sonnet            | Baseline    |
| Complex reasoning | Opus              | +40%        |
| Simple queries    | Haiku             | -70%        |

Switching to Sonnet for routine work reduces costs 30-40%.

### Context Optimization

| Strategy               | Savings               |
| ---------------------- | --------------------- |
| `/clear` between tasks | Avoid stale context   |
| CLAUDE.md < 500 lines  | Smaller base context  |
| Skills (on-demand)     | Load only when needed |
| `.claudeignore`        | Skip irrelevant files |

### .claudeignore Example

```
node_modules/
.git/
*.log
dist/
build/
__pycache__/
.pytest_cache/
```

Prevent 50-90% wasted tokens from irrelevant files.

### Prompt Caching

| Requests | Savings |
| -------- | ------- |
| 1        | 0%      |
| 2+       | 90%     |

Caching reduces repeated content costs dramatically.

### Extended Thinking Management

Default budget: 31,999 tokens (billed as output).

| Task Type        | Thinking Budget    |
| ---------------- | ------------------ |
| Simple queries   | Disable in /config |
| Standard coding  | 8,000 tokens       |
| Complex planning | 31,999 (default)   |

### MCP Server Context

Each MCP server adds tool definitions to context.

Check with `/context`, disable unused servers.

### Batch Processing

API batch mode: 50% discount on requests.

### Subscription vs Pay-as-you-go

| Plan     | Cost       | Best For             |
| -------- | ---------- | -------------------- |
| Max $100 | $100/month | Heavy individual use |
| Max $200 | $200/month | Power users          |
| API      | Pay-as-go  | Variable/low usage   |

### DSM Cost Optimization

| Pattern                | Implementation         |
| ---------------------- | ---------------------- |
| Use Haiku for tests    | `--model haiku` for CI |
| Cache FCP rules        | Load via @import       |
| Focused sessions       | One task per session   |
| Subagents for research | Preserve main context  |

## Feature Flags & Configuration Toggles

Environment variables and configuration options.

### Permission Mode Toggle

Cycle with `Shift+Tab`:

```
Normal → Auto-Accept → Plan Mode
```

Or start directly:

```bash
claude --permission-mode plan
```

### Environment Variable Toggles

| Variable                              | Effect                    |
| ------------------------------------- | ------------------------- |
| `DISABLE_AUTOUPDATER=1`               | No auto-updates           |
| `DISABLE_TELEMETRY=1`                 | No Statsig telemetry      |
| `DISABLE_ERROR_REPORTING=1`           | No Sentry reports         |
| `DISABLE_COST_WARNINGS=1`             | Hide cost warnings        |
| `DISABLE_NON_ESSENTIAL_MODEL_CALLS=1` | Skip non-critical calls   |
| `MAX_THINKING_TOKENS=0`               | Disable extended thinking |

### Terminal & IDE Toggles

| Variable                                     | Effect                   |
| -------------------------------------------- | ------------------------ |
| `CLAUDE_CODE_DISABLE_TERMINAL_TITLE=1`       | No title updates         |
| `CLAUDE_BASH_MAINTAIN_PROJECT_WORKING_DIR=1` | Stay in project dir      |
| `CLAUDE_CODE_IDE_SKIP_AUTO_INSTALL=1`        | No IDE extension install |
| `USE_BUILTIN_RIPGREP=0`                      | Use system rg            |

### MCP Server Toggle

```
/mcp enable server-name
/mcp disable server-name
```

### Config Command

```
/config
```

Toggles available:

- Extended thinking
- Prompt suggestions
- Release channel (stable/latest)
- Auto-compaction

### Release Channels

| Channel | Description            |
| ------- | ---------------------- |
| stable  | Production-ready       |
| latest  | Newest features (beta) |

### DSM Feature Flag Patterns

Add to CLAUDE.md or settings:

```markdown
# Feature Toggles

When running tests: Use --model haiku
When in CI: Set DISABLE_AUTOUPDATER=1
For debugging: Use --verbose
For cost control: MAX_THINKING_TOKENS=8000
```

### Hooks vs CLAUDE.md

| Type      | Enforcement   | Use Case             |
| --------- | ------------- | -------------------- |
| Hooks     | Deterministic | Must-do rules        |
| CLAUDE.md | Suggestive    | Should-do guidelines |

Hooks are critical for steering Claude in complex repos.

## Session Logging & Debug Tools

Tools and techniques for debugging Claude Code sessions.

### Session Storage Location

```
~/.claude/projects/{project-hash}/
├── {session-id}.jsonl     # Full conversation transcript
├── ...
```

Sessions stored as JSONL with full message history.

### CLI Debug Flag

```bash
# Enable debug mode with category filtering
claude --debug "api,hooks"
claude --debug "api,mcp"

# Exclude categories
claude --debug "!statsig,!file"
```

### MCP Debug Mode

```bash
# Debug MCP configuration issues
claude --mcp-debug
```

### Verbose Mode

```bash
# Turn-by-turn logging
claude --verbose

# Toggle in session
Ctrl+O
```

### Session Log Viewers

| Tool               | Purpose                         |
| ------------------ | ------------------------------- |
| claude-code-log    | JSONL → HTML conversion         |
| claude-code-logger | Traffic analysis with chat mode |
| LangSmith          | Cloud-based tracing             |

### LangSmith Integration

Add to settings.json:

```json
{
  "CC_LANGSMITH_DEBUG": "true"
}
```

Traces include:

- User messages
- Tool calls
- Assistant responses

### Debugging Workflow

1. **Isolate issue**: Add logging statements
2. **Show Claude**: Provide console output, errors
3. **Understand cause**: Why behavior differs from expected
4. **Fix root cause**: Address underlying issue
5. **Prevent recurrence**: Add validation/error handling

### "Show, Don't Tell" Principle

Claude can't see what you see. Provide:

- Browser dev tools output
- Console logs
- Network request details
- Actual behavior screenshots

### Debug Command Template

Create in `.claude/commands/`:

```yaml
---
name: debug-issue
description: Systematic debugging workflow
user-invocable: true
---

Debug the reported issue:
1. Identify error type (runtime, logic, config)
2. Read relevant error logs
3. Trace code path to failure point
4. Identify root cause
5. Propose fix with explanation
6. Add tests to prevent regression
```

### Known Issues

| Issue                 | Mitigation                   |
| --------------------- | ---------------------------- |
| Debug dir grows large | Monitor ~/.claude/debug size |
| Memory leak in parser | Restart long sessions        |
| Log infinite loop     | Fixed in recent versions     |

### DSM Debugging Tips

| Problem           | Debug Approach              |
| ----------------- | --------------------------- |
| FCP cache miss    | Use fcp-debugger agent      |
| Rate limit errors | Check API response headers  |
| DataFrame issues  | Print schema with df.schema |
| Symbol validation | Log normalized vs raw       |
| Network failures  | Enable httpx debug logging  |

### Log Analysis Command

```bash
# Pipe logs to Claude for analysis
cat error.log | claude -p "Analyze these errors and suggest fixes"
```

### Session Metrics

Check session health with:

```
> /cost       # Token usage and costs
> /context    # Loaded files and context size
> /memory     # Loaded CLAUDE.md files
```

## Project Templates & Scaffolding

Rapidly bootstrap new projects with Claude Code templates.

### Template Types

| Template Type    | Purpose                       | Source                     |
| ---------------- | ----------------------------- | -------------------------- |
| Plugin templates | Claude Code marketplace items | cc-marketplace-boilerplate |
| Starter kits     | Full application scaffolds    | claude-starter-kit         |
| CLAUDE.md seeds  | Project-specific instructions | cc-skills patterns         |
| Skill templates  | Progressive disclosure skills | SKILL.md format            |

### Scaffolding Commands

```bash
# Create plugin from template
claude -p "Create a new Claude Code plugin with:
- Name: my-tool
- Type: skill with slash command
- Hook: PostToolUse for validation"

# Bootstrap monorepo CLAUDE.md
claude -p "Create CLAUDE.md hierarchy for this monorepo:
- Root with hub navigation
- Package-specific files in packages/*/
- Shared rules in .claude/rules/"
```

### Template Structure

```
templates/
├── plugin/
│   ├── CLAUDE.md           # Plugin instructions
│   ├── skills/             # Skill templates
│   │   └── SKILL.md.tmpl
│   ├── hooks/              # Hook templates
│   │   └── hook.sh.tmpl
│   └── manifest.json.tmpl
├── project/
│   ├── CLAUDE.md.tmpl      # Project CLAUDE.md
│   ├── .claude/            # Infrastructure
│   │   ├── settings.json.tmpl
│   │   └── rules/
│   └── docs/skills/        # Skills directory
└── monorepo/
    ├── root-CLAUDE.md.tmpl
    └── package-CLAUDE.md.tmpl
```

### Dynamic Generation

```python
# Generate CLAUDE.md from project analysis
from pathlib import Path

def generate_claude_md(project_root: Path) -> str:
    """Analyze project and generate appropriate CLAUDE.md."""
    # Detect package manager
    has_uv = (project_root / "pyproject.toml").exists()
    has_npm = (project_root / "package.json").exists()

    # Detect test framework
    has_pytest = (project_root / "tests").exists()
    has_jest = (project_root / "jest.config.js").exists()

    # Generate commands section
    commands = []
    if has_uv:
        commands.append("uv run pytest")
    if has_npm:
        commands.append("npm test")

    return template.render(
        package_manager="uv" if has_uv else "npm",
        test_command=commands[0] if commands else "echo 'No tests'",
        project_type=detect_project_type(project_root)
    )
```

### DSM Template

For data-source-manager style projects:

```markdown
# {Project Name}

## Commands

| Task       | Command             |
| ---------- | ------------------- |
| Test       | `uv run pytest`     |
| Lint       | `uv run ruff check` |
| Type check | `uv run pyright`    |

## Architecture

- Provider pattern for data sources
- FCP for failover control
- Polars for DataFrame operations

## Rules

When working with {domain}:

- @.claude/rules/{domain}-patterns.md
```

## Voice Input & Audio Mode

Enable voice interaction with Claude Code for hands-free development.

### Voice Input Methods

| Method              | Setup                          | Use Case          |
| ------------------- | ------------------------------ | ----------------- |
| VoiceMode MCP       | MCP server with speech-to-text | Full conversation |
| System dictation    | macOS Dictation (Fn Fn)        | Quick text input  |
| Whisper local       | whisper.cpp with microphone    | Privacy-focused   |
| Cloud transcription | OpenAI Whisper API             | High accuracy     |

### VoiceMode MCP Setup

```json
{
  "mcpServers": {
    "voicemode": {
      "command": "npx",
      "args": ["-y", "@anthropic/voicemode-mcp"],
      "env": {
        "OPENAI_API_KEY": "${OPENAI_API_KEY}"
      }
    }
  }
}
```

### Local Whisper Integration

```bash
# Install whisper.cpp
brew install whisper-cpp

# Record and transcribe
rec -c 1 -r 16000 -t wav - | whisper-cpp --model base.en

# Pipe to Claude
transcribe() {
    rec -c 1 -r 16000 -t wav -d 30 /tmp/voice.wav
    whisper-cpp --model base.en /tmp/voice.wav | claude
}
```

### Voice Workflow Patterns

```bash
# Voice-activated code review
alias voice-review='transcribe | xargs -I {} claude -p "Review this code: {}"'

# Voice commit messages
alias voice-commit='transcribe | xargs -I {} git commit -m "{}"'

# Voice task creation
alias voice-task='transcribe | xargs -I {} claude -p "Create task: {}"'
```

### Accessibility Benefits

Voice input enables:

- **Hands-free coding**: Continue working during RSI breaks
- **Rapid dictation**: Faster than typing for documentation
- **Accessibility**: Support for users with mobility limitations
- **Mobile development**: Code review from phone with voice

### DSM Voice Commands

Custom voice shortcuts for data-source-manager:

```bash
# Voice-activated FCP debug
alias voice-fcp='transcribe | xargs -I {} claude -p "Debug FCP for symbol: {}"'

# Voice data fetch
alias voice-fetch='transcribe | xargs -I {} claude -p "Fetch {} data for last 7 days"'

# Voice test run
alias voice-test='transcribe | xargs -I {} claude -p "Run tests matching: {}"'
```

### Privacy Considerations

| Mode          | Data Flow                  | Privacy Level |
| ------------- | -------------------------- | ------------- |
| Local Whisper | Audio never leaves machine | Highest       |
| VoiceMode MCP | Audio to OpenAI API        | Medium        |
| System dictat | Audio to Apple servers     | Medium        |
| Cloud only    | Audio to cloud provider    | Lowest        |

For sensitive codebases, use local Whisper:

```bash
# Privacy-first voice setup
export VOICE_MODE=local
export WHISPER_MODEL=base.en
export WHISPER_THREADS=4
```

## Context Engineering for Agents

Advanced context window management strategies for AI agents.

### Core Principles

Context engineering involves curating the optimal set of tokens during LLM inference:

- **System prompts**: Calibrate at the "right altitude" - avoid overly rigid or vague
- **Tool design**: Self-contained, robust, clear on intended use
- **Few-shot examples**: Canonical examples more effective than exhaustive edge cases
- **Token efficiency**: Find smallest set of high-signal tokens

### Just-In-Time Context

Maintain lightweight identifiers, dynamically load data using tools:

```
Agent maintains: file paths, queries, links
Agent retrieves: content on demand via tools

Benefits:
- Progressive disclosure through exploration
- Metadata signaling via file hierarchies
- Avoids loading irrelevant information
```

### Compaction Strategies

When approaching context limits:

| Strategy        | Description                            | Best For                |
| --------------- | -------------------------------------- | ----------------------- |
| Tool clearing   | Remove stale tool results              | Lightest touch (safest) |
| Summarization   | Distill to critical details            | Long conversations      |
| Context editing | Auto-clear while preserving flow       | Ongoing tasks           |
| Session restart | Fresh context with CLAUDE.md preserved | 80%+ utilization        |

### Structured Note-Taking

Maintain external memory for long-horizon tasks:

```markdown
# NOTES.md (Agent's External Memory)

## Objectives

- Implement FCP cache warming for BTCUSDT
- Add rate limit backoff to Binance adapter

## Progress

- [x] Cache structure analyzed
- [ ] Warming strategy designed

## Key Decisions

- Use polars for DataFrame operations (ADR: 2025-01-30)
- Binance rate limit: 1200 req/min
```

### Sub-Agent Architecture

Specialized agents with clean context windows:

```
Lead Agent (coordinator)
├── Explorer Agent (codebase search) → returns 1-2K token summary
├── Reviewer Agent (code review) → returns findings list
└── Tester Agent (test execution) → returns pass/fail + errors

Benefits:
- Clean separation of concerns
- Each agent explores extensively
- Condensed summaries preserve signal
```

### Context Window Signals

| Signal          | Action                     |
| --------------- | -------------------------- |
| 50% utilization | Normal operation           |
| 70% utilization | Consider task completion   |
| 80% utilization | Wrap up or restart session |
| 90% utilization | Auto-compact triggers      |

### DSM Context Patterns

For data-source-manager specifically:

```python
# Lightweight identifiers over full content
context = {
    "provider": "binance",
    "symbol": "BTCUSDT",
    "interval": "1h",
    "fcp_status": "cache_hit",
    # Not the full DataFrame - just metadata
    "df_shape": (1000, 6),
    "df_schema": ["open_time", "open", "high", "low", "close", "volume"]
}
```

## Team Collaboration Workflows

Patterns for team-based Claude Code development.

### Shared Configuration

Version-controlled project configuration ensures consistency:

```
.claude/
├── settings.json       # Team permission rules (committed)
├── settings.local.json # Personal overrides (gitignored)
├── agents/             # Shared subagents
├── commands/           # Team slash commands
└── rules/              # Domain knowledge

Benefits:
- All team members get same Claude behavior
- Consistent coding standards
- Unified architectural patterns
```

### CLAUDE.md as Team Knowledge

Each team maintains a CLAUDE.md documenting:

- **Mistakes**: What to avoid (learned from errors)
- **Best practices**: Style conventions, design guidelines
- **PR templates**: Review checklist, commit formats
- **Domain expertise**: Data pipeline dependencies, API quirks

### Parallel Session Management

Running multiple Claude instances with git worktrees:

```bash
# Create worktree for feature
git worktree add ../project-feature-a feature-a

# Launch Claude in isolated workspace
cd ../project-feature-a && claude

# Run /init to orient Claude to worktree
> /init
```

### Worktree Manager Pattern

```bash
# Custom function for quick worktree access
w() {
    project=$1
    feature=$2
    cmd=$3

    worktree_path=~/.worktrees/$project/$feature

    if [ ! -d "$worktree_path" ]; then
        git worktree add "$worktree_path" -b "$USER/$feature"
    fi

    if [ -n "$cmd" ]; then
        cd "$worktree_path" && $cmd
    else
        cd "$worktree_path"
    fi
}

# Usage
w dsm new-provider claude    # Open Claude on new-provider branch
w dsm new-provider git status # Run git status in worktree
```

### Multi-Agent Team Workflows

| Pattern           | Description                            | Use Case            |
| ----------------- | -------------------------------------- | ------------------- |
| Writer + Reviewer | One Claude writes, another reviews     | Quality assurance   |
| Writer + Tester   | One writes code, another writes tests  | TDD workflow        |
| Parallel features | Multiple Claudes on different features | Sprint acceleration |
| Scratchpad comms  | Agents share via working files         | Complex tasks       |

### GitHub Actions Integration

Automated workflows with Claude:

```yaml
# .github/workflows/claude-review.yml
on:
  pull_request:
    types: [opened, synchronize]

jobs:
  claude-review:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: Claude PR Review
        uses: anthropic/claude-code-action@v1
        with:
          prompt: |
            Review this PR for:
            - Code quality and patterns
            - DSM-specific conventions
            - Silent failure anti-patterns
```

### Incident Response Pattern

For production issues, teams use Claude for rapid diagnosis:

```bash
# Feed stack trace and docs to Claude
cat error.log | claude -p "Diagnose this error given our FCP implementation"

# Result: 10-15 min → 3 min resolution time
```

### Cross-Functional Collaboration

| Team         | Claude Usage                        |
| ------------ | ----------------------------------- |
| Product Eng  | First stop for bug identification   |
| Security     | TDD with Claude at each stage       |
| Data Science | CLAUDE.md for pipeline dependencies |
| Marketing    | Agentic workflows for campaigns     |

## Git Worktree Best Practices

Deep dive into parallel development with worktrees.

### Directory Structure

```
~/projects/
├── data-source-manager/         # Main repository
│   └── .git/
└── worktrees/                   # Parallel workspaces
    └── data-source-manager/
        ├── feature-a/           # Feature A worktree
        ├── feature-b/           # Feature B worktree
        └── experiment/          # Experiment worktree
```

### Worktree Commands

```bash
# List existing worktrees
git worktree list

# Create new worktree with new branch
git worktree add ../worktrees/dsm/feature-a -b feature-a

# Create worktree from existing branch
git worktree add ../worktrees/dsm/bugfix bugfix-branch

# Remove worktree when done
git worktree remove ../worktrees/dsm/feature-a

# Prune stale worktree info
git worktree prune
```

### Session Isolation Rules

| Rule                    | Reason                         |
| ----------------------- | ------------------------------ |
| One Claude per worktree | Prevents context collision     |
| Don't share files       | Agents overwrite each other    |
| Run /init per worktree  | Orient Claude to workspace     |
| Use separate terminals  | Visual isolation aids tracking |

### Resource Management

When running multiple Claude sessions:

```bash
# Check resource usage
ps aux | grep claude

# Limit concurrent sessions based on system RAM
# Recommendation: 1 session per 8GB RAM

# Each session may spawn subprocesses
# Monitor with: Activity Monitor or htop
```

### DSM Worktree Workflow

```bash
# 1. Create worktree for new data source
git worktree add ../worktrees/dsm/add-coinbase -b add-coinbase

# 2. Launch Claude in worktree
cd ../worktrees/dsm/add-coinbase && claude

# 3. Claude uses project CLAUDE.md (shared)
# 4. Implement in isolation
# 5. Create PR from worktree
git push -u origin add-coinbase
gh pr create

# 6. Cleanup after merge
cd ~/projects/data-source-manager
git worktree remove ../worktrees/dsm/add-coinbase
```

## Advanced Debugging Techniques

Systematic debugging strategies for Claude Code.

### Plan Mode for Complex Bugs

Use Plan Mode for sensitive or production bugs:

```
1. Enter Plan Mode
   > /plan

2. Paste error and context
   "This error started after adding authentication:
   [stack trace]"

3. Claude analyzes without making changes
4. Review proposed investigation plan
5. Approve and execute
```

Benefits:

- Prevents accidental fixes breaking working code
- Thorough investigation before action
- Safe for production environments

### Visibility-Based Debugging

Claude fails when it lacks visibility, not intelligence:

```bash
# Instead of guessing, add logging
> "Add logging that tracks the data flow through the FCP pipeline"

# Run the test
uv run pytest tests/test_fcp.py -v

# Paste output back to Claude
> "Here's the log output: [paste]"

# One-shot fix now possible
```

### Debug Workflow Template

Store in `.claude/commands/debug.md`:

```markdown
---
description: Systematic debugging workflow
---

## Debug: $ARGUMENTS

### 1. Reproduce

- [ ] Error message captured
- [ ] Stack trace available
- [ ] Recent changes identified

### 2. Isolate

- [ ] Minimal reproduction case
- [ ] Affected code path identified
- [ ] Dependencies ruled out

### 3. Understand

- [ ] Root cause identified
- [ ] Related code reviewed
- [ ] Edge cases considered

### 4. Fix

- [ ] Solution implemented
- [ ] Tests added/updated
- [ ] No regressions introduced

### 5. Prevent

- [ ] CLAUDE.md updated
- [ ] Error handling improved
- [ ] Documentation added
```

### Debugging Pattern Reference

| Bug Type       | Strategy                        | Claude Prompt                      |
| -------------- | ------------------------------- | ---------------------------------- |
| Runtime error  | Trace undefined values          | "Where does undefined originate?"  |
| Logic bug      | Step through with sample data   | "Walk through this with input X"   |
| Performance    | Analyze complexity, bottlenecks | "What's the time complexity here?" |
| Race condition | Review timing, async flow       | "Identify race conditions in this" |
| Integration    | Check auth, API contracts       | "Trace the integration flow"       |

### Debugger Integration (Pointbreak)

For step-through debugging with Claude:

```bash
# Start debugging session
> /debug src/data_source_manager/core/fcp.py

# Set breakpoints
> /breakpoint src/fcp.py:145

# Step through execution
> /step        # Next line
> /step into   # Into function
> /step out    # Exit function

# Inspect state
> /inspect cache_status
> /inspect locals
```

### DSM-Specific Debug Commands

```bash
# Debug FCP cache behavior
> /debug-fcp BTCUSDT

# Trace data flow
> "Add logging to trace OHLCV data from API to DataFrame"

# Investigate rate limits
> "Log all Binance API responses with headers"
```

## Autonomous Loop Patterns

Running Claude Code in autonomous mode for large tasks.

### Basic YOLO Loop

```bash
#!/bin/bash
# autonomous-loop.sh

PROMPT_FILE="task.md"
MAX_ITERATIONS=50

for i in $(seq 1 $MAX_ITERATIONS); do
    echo "=== Iteration $i of $MAX_ITERATIONS ==="
    cat "$PROMPT_FILE" | claude -p --dangerously-skip-permissions
    sleep 2
done
```

### Safe Autonomous Configuration

| Flag                             | Purpose                   |
| -------------------------------- | ------------------------- |
| `--dangerously-skip-permissions` | Enable unrestricted exec  |
| `--verbose`                      | Increase observability    |
| `--output-format=stream-json`    | Structured output parsing |

### Safety Requirements

1. **Sandboxed environment**: Docker or VM only
2. **Version control**: Git as safety net
3. **Rate limit awareness**: Max plan hits limits quickly
4. **Iteration caps**: Start with 10, increase carefully
5. **Output review**: Expect 25% discardable results

### Prompt Optimization

Keep prompts concise (3-5 sentences):

```markdown
# task.md

Improve test coverage for the FCP module.
Focus on edge cases in cache invalidation.
Run tests after each change.
```

Better than lengthy instruction sets.

### Use Cases for Autonomous Mode

| Task                 | Loop Effectiveness |
| -------------------- | ------------------ |
| Framework migration  | High               |
| Large-scale refactor | High               |
| Documentation gen    | High               |
| Test coverage        | High               |
| Exploratory research | Medium             |
| Precise bug fixes    | Low (use Plan)     |

### Self-Management Emergence

In autonomous mode, Claude develops its own workflow:

- Writes tests for itself
- Maintains scope awareness
- "Gives up" when stuck
- Creates notes for continuity

### DSM Autonomous Tasks

```markdown
# autonomous-dsm.md

Refactor all Binance adapter methods to use FCP.
Each method should:

1. Check cache first
2. Fetch from API if miss
3. Update cache on success
4. Handle rate limits with backoff

Run tests after each method refactor.
Commit working changes incrementally.
```

## Plugin Development Guide

Creating and distributing Claude Code plugins.

### Plugin Structure

```
my-plugin/
├── .claude-plugin/
│   └── plugin.json      # Manifest (required)
├── commands/            # Slash commands
│   └── hello.md
├── skills/              # Agent skills
│   └── code-review/
│       └── SKILL.md
├── agents/              # Custom agents
│   └── reviewer.md
├── hooks/               # Event handlers
│   └── hooks.json
├── .mcp.json            # MCP server configs
└── README.md            # Documentation
```

### Plugin Manifest

<!-- SSoT-OK: Example manifest, version is illustrative -->

```json
{
  "name": "dsm-tools",
  "description": "DSM development tools and patterns",
  "version": "<version>",
  "author": {
    "name": "DSM Team"
  },
  "repository": "https://github.com/org/dsm-tools",
  "license": "MIT"
}
```

### Standalone vs Plugin

| Aspect     | Standalone (`.claude/`) | Plugin                   |
| ---------- | ----------------------- | ------------------------ |
| Scope      | Single project          | Multi-project, shareable |
| Skill name | `/hello`                | `/plugin-name:hello`     |
| Hooks      | `settings.json`         | `hooks/hooks.json`       |
| Sharing    | Manual copy             | Marketplace install      |

### Creating a Skill

```markdown
# skills/dsm-review/SKILL.md

---

name: dsm-review
description: Reviews code for DSM patterns. Use when reviewing
DataSourceManager implementations or FCP code.

---

When reviewing DSM code, check for:

1. FCP protocol compliance
2. Proper timestamp handling (UTC)
3. Symbol format validation
4. Silent failure patterns (bare except)
5. DataFrame column naming
```

### Creating a Hook

```json
// hooks/hooks.json
{
  "hooks": {
    "PostToolUse": [
      {
        "matcher": "Write|Edit",
        "hooks": [
          {
            "type": "command",
            "command": "uv run ruff check $FILE --select=E722,S110"
          }
        ]
      }
    ]
  }
}
```

### Testing Plugins

```bash
# Load plugin for testing
claude --plugin-dir ./my-plugin

# Load multiple plugins
claude --plugin-dir ./plugin-one --plugin-dir ./plugin-two

# Verify components
> /help                    # See commands
> /agents                  # See agents
```

### Plugin Best Practices

1. **Structure**: Keep `.claude-plugin/` for manifest only
2. **Naming**: Descriptive, lowercase, hyphenated
3. **Version**: Use semantic versioning
4. **Docs**: Include README with examples
5. **Test**: Verify each component works

### Converting Standalone to Plugin

<!-- SSoT-OK: Example version placeholder -->

```bash
# 1. Create plugin structure
mkdir -p my-plugin/.claude-plugin
mkdir my-plugin/commands my-plugin/skills

# 2. Create manifest
cat > my-plugin/.claude-plugin/plugin.json << 'EOF'
{
  "name": "my-plugin",
  "description": "Migrated from standalone",
  "version": "<version>"
}
EOF

# 3. Copy existing files
cp -r .claude/commands/* my-plugin/commands/
cp -r .claude/skills/* my-plugin/skills/

# 4. Migrate hooks from settings.json to hooks/hooks.json

# 5. Test
claude --plugin-dir ./my-plugin
```

### DSM Plugin Example

A complete DSM-focused plugin:

<!-- SSoT-OK: Example plugin manifest -->

```json
// .claude-plugin/plugin.json
{
  "name": "dsm-dev",
  "description": "DataSourceManager development tools",
  "version": "<version>"
}
```

```markdown
## // commands/fetch-data.md

## description: Fetch market data with validation

Fetch $ARGUMENTS data:

1. Validate symbol format
2. Use FCP for cache/API logic
3. Validate DataFrame columns
4. Report data quality metrics
```

```markdown
## // skills/fcp-expert/SKILL.md

name: fcp-expert
description: Expertise in FCP protocol implementation

---

FCP implementation rules:

- Always check cache before API
- Use open_time as cache key
- Handle partial cache hits
- Respect rate limits
```

## Notification Systems

Alert systems for Claude Code task completion and input requests.

### Hook-Based Notifications

Configure in `~/.config/claude/settings.json`:

```json
{
  "hooks": {
    "Notification": [
      {
        "matcher": "*",
        "hooks": [
          {
            "type": "command",
            "command": "notify.sh 'Awaiting input' && afplay /System/Library/Sounds/Glass.aiff"
          }
        ]
      }
    ],
    "Stop": [
      {
        "hooks": [
          {
            "type": "command",
            "command": "notify.sh 'Task completed' && afplay /System/Library/Sounds/Hero.aiff"
          }
        ]
      }
    ]
  }
}
```

### Notification Script (macOS)

```bash
#!/bin/bash
# ~/.config/claude/notify.sh

MESSAGE="$1"
REPO=$(basename "$(git rev-parse --show-toplevel 2>/dev/null)" || echo "unknown")

terminal-notifier \
  -title "Claude Code" \
  -subtitle "$REPO" \
  -message "$MESSAGE" \
  -sender com.anthropic.claudefordesktop
```

### Sound Differentiation

| Event          | Sound      | Meaning          |
| -------------- | ---------- | ---------------- |
| Input needed   | Glass.aiff | Come back        |
| Task completed | Hero.aiff  | Done             |
| Error          | Basso.aiff | Attention needed |

### Cross-Platform Options

| Platform | Tool              | Setup                            |
| -------- | ----------------- | -------------------------------- |
| macOS    | terminal-notifier | `brew install terminal-notifier` |
| Linux    | notify-send       | Built-in on most distros         |
| Windows  | PowerShell toast  | Native Windows 10+               |
| All      | code-notify       | Cross-platform CLI               |

### MCP Server Notifications

For richer notifications:

```json
{
  "mcpServers": {
    "notify": {
      "command": "npx",
      "args": ["-y", "@nkyy/claude-code-notify-mcp"]
    }
  }
}
```

### Phone Notifications

For long-running tasks:

```bash
# Use Pushover or similar service
curl -s \
  --form-string "token=$PUSHOVER_TOKEN" \
  --form-string "user=$PUSHOVER_USER" \
  --form-string "message=Claude Code completed: $REPO" \
  https://api.pushover.net/1/messages.json
```

### Remote Development

For VSCode Remote SSH, use OSC escape sequences:

```bash
# notify.sh for remote sessions
printf '\033]777;notify;Claude Code;%s\007' "$MESSAGE"
```

## Model Selection & Routing

Choosing the right Claude model for each task.

### Model Aliases

| Alias        | Model               | Use Case                       |
| ------------ | ------------------- | ------------------------------ |
| `default`    | Account-dependent   | General usage                  |
| `sonnet`     | Sonnet 4.5          | Daily coding tasks             |
| `opus`       | Opus 4.5            | Complex reasoning              |
| `haiku`      | Haiku 4.5           | Fast, simple tasks             |
| `sonnet[1m]` | Sonnet + 1M context | Long sessions                  |
| `opusplan`   | Opus → Sonnet       | Plan with Opus, execute Sonnet |

### When to Use Each Model

**Opus** (Most Capable):

- System architecture design
- Complex multi-step reasoning
- Final review before merge
- Advanced analysis
- Long-horizon planning

**Sonnet** (Balanced Default):

- Day-to-day development
- Feature implementation
- Writing tests
- Code refactoring
- Documentation generation

**Haiku** (Fastest):

- Quick syntax questions
- Single-file edits
- Typo corrections
- UI scaffolding
- Small prompts

### Model Selection Commands

```bash
# At startup
claude --model opus
claude --model sonnet
claude --model haiku
claude --model opusplan

# During session
> /model opus
> /model sonnet

# Check current model
> /status
```

### OpusPlan Mode

Best of both worlds:

```
Plan Mode → Uses Opus (complex reasoning)
     ↓
Execution Mode → Switches to Sonnet (efficient coding)
```

### Cost Comparison

| Model  | Relative Cost | Best For              |
| ------ | ------------- | --------------------- |
| Opus   | 5x Sonnet     | Critical decisions    |
| Sonnet | 1x (baseline) | Most development      |
| Haiku  | 0.25x Sonnet  | High-frequency simple |

### Intelligent Routing (claude-router)

Third-party tools can auto-route based on complexity:

```bash
# Automatic model selection based on query
# Simple → Haiku
# Medium → Sonnet
# Complex → Opus

# Can reduce costs by up to 80%
```

### Environment Variables

```bash
# Override model mappings
export ANTHROPIC_DEFAULT_OPUS_MODEL="claude-opus-4-5-20251101"
export ANTHROPIC_DEFAULT_SONNET_MODEL="claude-sonnet-4-5-20250929"
export ANTHROPIC_DEFAULT_HAIKU_MODEL="claude-haiku-4-5-20251001"

# Subagent model
export CLAUDE_CODE_SUBAGENT_MODEL="claude-haiku-4-5-20251001"
```

### Prompt Caching Control

```bash
# Disable all caching
export DISABLE_PROMPT_CACHING=1

# Per-model caching control
export DISABLE_PROMPT_CACHING_HAIKU=1
export DISABLE_PROMPT_CACHING_SONNET=0
export DISABLE_PROMPT_CACHING_OPUS=0
```

### DSM Model Strategy

For data-source-manager development:

| Task                    | Model    | Reason                     |
| ----------------------- | -------- | -------------------------- |
| FCP architecture design | Opus     | Complex protocol reasoning |
| Provider implementation | Sonnet   | Standard coding            |
| Quick fix               | Haiku    | Speed                      |
| Code review             | OpusPlan | Thorough then efficient    |
| Test writing            | Sonnet   | Balanced                   |

## Enterprise Deployment

Deploying Claude Code for teams and organizations.

### Deployment Options

| Option                | Best For                 | Billing            |
| --------------------- | ------------------------ | ------------------ |
| Claude for Teams      | Small teams, quick start | $150/seat Premium  |
| Claude for Enterprise | Large orgs, compliance   | Contact Sales      |
| Anthropic Console     | Individual developers    | PAYG               |
| Amazon Bedrock        | AWS-native deployments   | PAYG through AWS   |
| Google Vertex AI      | GCP-native deployments   | PAYG through GCP   |
| Microsoft Foundry     | Azure-native deployments | PAYG through Azure |

### Enterprise Features

| Feature                 | Teams | Enterprise |
| ----------------------- | ----- | ---------- |
| Centralized billing     | Yes   | Yes        |
| Usage dashboard         | Yes   | Yes        |
| SSO integration         | No    | Yes        |
| Domain capture          | No    | Yes        |
| Role-based permissions  | No    | Yes        |
| Managed policy settings | No    | Yes        |
| Compliance API access   | No    | Yes        |

### Team Onboarding Process

```
1. Choose deployment option
   └── Teams, Enterprise, or Cloud Provider

2. Configure authentication
   └── SSO, API keys, or cloud credentials

3. Deploy shared configuration
   └── Organization CLAUDE.md
   └── Repository CLAUDE.md files
   └── .mcp.json for integrations

4. Roll out to team
   └── Installation instructions
   └── Authentication guide
   └── Training materials

5. Monitor and iterate
   └── Usage analytics
   └── Cost tracking
   └── Feedback collection
```

### CLAUDE.md Deployment Hierarchy

```
Organization Level:
/Library/Application Support/ClaudeCode/CLAUDE.md  # macOS
~/.config/claude/CLAUDE.md                          # Linux/User

Repository Level:
/repo/CLAUDE.md                                     # Project standards
/repo/packages/*/CLAUDE.md                          # Package-specific
```

### Cloud Provider Configuration

**Amazon Bedrock:**

```bash
export CLAUDE_CODE_USE_BEDROCK=1
export AWS_REGION=us-east-1
# Optional: LLM Gateway
export ANTHROPIC_BEDROCK_BASE_URL='https://gateway.company.com/bedrock'
```

**Google Vertex AI:**

```bash
export CLAUDE_CODE_USE_VERTEX=1
export CLOUD_ML_REGION=us-east5
export ANTHROPIC_VERTEX_PROJECT_ID=your-project-id
```

**Microsoft Foundry:**

```bash
export CLAUDE_CODE_USE_FOUNDRY=1
export ANTHROPIC_FOUNDRY_RESOURCE=your-resource
export ANTHROPIC_FOUNDRY_API_KEY=your-api-key
```

### Corporate Proxy Setup

```bash
# Route through corporate proxy
export HTTPS_PROXY='https://proxy.company.com:8080'

# Verify configuration
claude
> /status
```

### Managed Permissions

Enterprise admins can set organization-wide permissions that users cannot override:

```json
// Managed settings (admin-deployed)
{
  "managedPermissions": {
    "deny": ["Bash(rm -rf /)", "Write(.env*)", "Read(~/.ssh/*)"],
    "allow": ["Bash(uv run *)", "Bash(npm test *)"]
  }
}
```

### Security Best Practices

1. **Configure SSO**: Enforce single sign-on for all users
2. **Set spending limits**: Organization and individual caps
3. **Enable audit logs**: Track usage and actions
4. **Define allowed tools**: Restrict dangerous operations
5. **Review MCP servers**: Centrally configure integrations

### DSM Enterprise Considerations

For DSM teams:

- Deploy shared CLAUDE.md with FCP documentation
- Configure Binance API rate limits in managed settings
- Set up Doppler integration via MCP
- Track usage by developer for SRED claims

## AI Pair Programming Patterns

Effective collaboration patterns with Claude Code.

### Collaboration Modes

| Mode                 | Description                      | Best For            |
| -------------------- | -------------------------------- | ------------------- |
| Driver-Navigator     | Human drives, Claude navigates   | Learning new code   |
| Architect-Builder    | Human architects, Claude builds  | Feature development |
| Reviewer-Implementer | Human reviews, Claude implements | Rapid prototyping   |
| Ping-Pong TDD        | Alternating test/implementation  | Test-driven dev     |

### Driver-Navigator Pattern

```
Human (Driver):
  - Types code
  - Makes micro-decisions
  - Controls flow

Claude (Navigator):
  - Watches for bugs
  - Suggests improvements
  - Keeps big picture

Usage:
> "Watch as I implement this. Point out any issues."
```

### Architect-Builder Pattern

```
Human (Architect):
  - Defines structure
  - Sets constraints
  - Reviews results

Claude (Builder):
  - Implements details
  - Handles boilerplate
  - Executes plan

Usage:
> "Here's the architecture. Implement each component."
```

### Ping-Pong TDD

```
Round 1: Claude writes test
Round 2: Human reviews test
Round 3: Claude writes implementation
Round 4: Human reviews implementation
Repeat...

Usage:
> "Write a failing test for FCP cache expiration"
[Claude writes test]
> "Now implement to make it pass"
[Claude implements]
```

### Real-Time Collaboration

Claude Code supports live pair programming:

```
1. Human describes task
2. Claude proposes approach
3. Human approves/modifies
4. Claude implements
5. Human reviews changes in editor
6. Iterate on feedback
```

### Communication Patterns

**Thinking Aloud:**

```
> "I'm thinking about using a decorator here because..."
Claude: "That approach makes sense. You might also consider..."
```

**Rubber Duck Debugging:**

```
> "Walk through this code with me. I'll explain what each part does."
Claude: [Asks clarifying questions, spots issues]
```

**Code Review Mode:**

```
> "Review this PR for DSM patterns and silent failures"
Claude: [Detailed review with line-specific feedback]
```

### Skill Level Adaptation

| Developer Level | Claude Role            | Guidance Level |
| --------------- | ---------------------- | -------------- |
| Junior          | Mentor, explains why   | High           |
| Mid-level       | Collaborator, suggests | Medium         |
| Senior          | Tool, executes quickly | Low            |

### DSM Pair Programming

For data-source-manager development:

```markdown
## Effective DSM Pairing Prompts

"Implement the OKX adapter. Follow the Binance adapter pattern."

"Help me debug why FCP is returning stale data for ETHUSDT."

"Review this DataFrame transformation for correctness."

"Walk me through the timestamp handling in this provider."
```

### Session Continuity

Maintain context across pairing sessions:

```markdown
# SESSION.md (temporary file)

## Current Task

Implementing Coinbase adapter

## Decisions Made

- Using REST API (not WebSocket)
- Mapping to OHLCV format
- Following Binance adapter pattern

## Open Questions

- Rate limit strategy?
- Symbol format mapping?

## Next Steps

1. Implement get_historical_klines
2. Add rate limiting
3. Write tests
```

## Keyboard Shortcuts Reference

Essential shortcuts for productive Claude Code usage.

### General Controls

| Shortcut    | Description                     |
| ----------- | ------------------------------- |
| `Ctrl+C`    | Cancel current input/generation |
| `Ctrl+D`    | Exit Claude Code session        |
| `Ctrl+L`    | Clear terminal screen           |
| `Ctrl+O`    | Toggle verbose output           |
| `Ctrl+R`    | Reverse search command history  |
| `Ctrl+B`    | Background running tasks        |
| `Esc+Esc`   | Rewind code/conversation        |
| `Shift+Tab` | Toggle permission modes         |
| `Alt+P`     | Switch model                    |
| `Alt+T`     | Toggle extended thinking        |

### Text Editing

| Shortcut | Description                  |
| -------- | ---------------------------- |
| `Ctrl+K` | Delete to end of line        |
| `Ctrl+U` | Delete entire line           |
| `Ctrl+Y` | Paste deleted text           |
| `Alt+B`  | Move cursor back one word    |
| `Alt+F`  | Move cursor forward one word |
| `Ctrl+A` | Move to start of line        |
| `Ctrl+E` | Move to end of line          |
| `Ctrl+W` | Delete previous word         |

### Multiline Input

| Method        | Shortcut       | Terminal Support         |
| ------------- | -------------- | ------------------------ |
| Quick escape  | `\` + Enter    | All terminals            |
| macOS default | `Option+Enter` | macOS                    |
| Shift+Enter   | `Shift+Enter`  | iTerm2, WezTerm, Ghostty |
| Control seq   | `Ctrl+J`       | All terminals            |

### Quick Prefixes

| Prefix | Description                |
| ------ | -------------------------- |
| `/`    | Slash commands and skills  |
| `!`    | Bash mode (direct execute) |
| `@`    | File path mention          |

### Essential Commands

| Command    | Purpose                      |
| ---------- | ---------------------------- |
| `/init`    | Create CLAUDE.md for project |
| `/plan`    | Enter plan mode              |
| `/model`   | Switch Claude model          |
| `/cost`    | Show token usage             |
| `/context` | Visualize context usage      |
| `/rewind`  | Restore previous state       |
| `/compact` | Compact conversation         |
| `/clear`   | Clear conversation history   |
| `/resume`  | Resume previous session      |
| `/tasks`   | List background tasks        |

### Vim Mode

Enable with `/vim` or configure in `/config`:

| Command | Action               |
| ------- | -------------------- |
| `i`     | Insert mode          |
| `Esc`   | Normal mode          |
| `dd`    | Delete line          |
| `yy`    | Yank line            |
| `p`     | Paste                |
| `w/b`   | Word forward/back    |
| `ciw`   | Change inner word    |
| `di"`   | Delete inside quotes |

### Command History

| Shortcut | Description               |
| -------- | ------------------------- |
| `Up`     | Previous command          |
| `Down`   | Next command              |
| `Ctrl+R` | Reverse search            |
| `Ctrl+S` | Forward search (after ^R) |
| `Ctrl+G` | Cancel search             |
| `Tab`    | Accept match for editing  |
| `Enter`  | Accept and execute        |

### Backgrounding Commands

```bash
# Press Ctrl+B during command execution to background
# (tmux users: press twice due to prefix key)

# Common backgrounded commands:
- Build tools (webpack, vite)
- Test runners (pytest, jest)
- Dev servers (uvicorn, npm run dev)
```

## File Exclusion Patterns

Controlling what files Claude Code can access.

### Official Method: Settings Permissions

Configure in `.claude/settings.json`:

```json
{
  "permissions": {
    "deny": [
      "Read(.env*)",
      "Read(.envrc)",
      "Read(~/.aws/**)",
      "Read(~/.ssh/**)",
      "Read(*.pem)",
      "Read(*.key)",
      "Read(**/secrets/**)",
      "Read(**/credentials/**)"
    ]
  }
}
```

### Default Exclusions

Claude Code respects `.gitignore` by default during codebase analysis:

- `node_modules/`
- `__pycache__/`
- `.git/`
- Build outputs
- Cache directories

### Project-Level Permissions

In project `.claude/settings.json`:

```json
{
  "permissions": {
    "deny": ["Read(.mise.local.toml)", "Read(cache/raw/**)", "Read(*.parquet)"]
  }
}
```

### Third-Party `.claudeignore` Hook

For `.gitignore`-style exclusions, use the `claude-ignore` hook:

```bash
# Install hook
npm install -g claude-ignore

# Create .claudeignore
cat > .claudeignore << 'EOF'
# Secrets
.env*
*.pem
*.key

# Large files
*.parquet
cache/raw/

# Generated
dist/
build/
EOF
```

### DSM File Exclusions

For data-source-manager:

```json
{
  "permissions": {
    "deny": [
      "Read(.env*)",
      "Read(.mise.local.toml)",
      "Read(cache/raw/**)",
      "Read(*.parquet)",
      "Read(~/.doppler/**)"
    ]
  }
}
```

### Pattern Syntax

| Pattern        | Matches                    |
| -------------- | -------------------------- |
| `*.ext`        | Files with extension       |
| `**/dir/**`    | Any nested directory       |
| `~/.dir/**`    | Home directory paths       |
| `path/to/file` | Specific file              |
| `prefix*`      | Files starting with prefix |

### Verification

Check what's excluded:

```
> /permissions
# Shows current deny rules

> "What files can you not read?"
# Claude will list denied patterns
```

## Checkpointing & Rewind

Automatically track and restore Claude's edits.

### How Checkpoints Work

- Every user prompt creates a new checkpoint
- Checkpoints persist across sessions
- Automatically cleaned up after 30 days (configurable)
- Only tracks file edits made through Claude's tools

### Accessing Checkpoints

```
# Press Esc twice
Esc + Esc → Opens rewind menu

# Or use command
> /rewind
```

### Restore Options

| Option            | Description                        | Use Case              |
| ----------------- | ---------------------------------- | --------------------- |
| Conversation only | Rewind messages, keep code changes | Re-phrase prompt      |
| Code only         | Revert files, keep conversation    | Undo bad code changes |
| Both code + convo | Full restore to prior point        | Complete reset        |

### Common Use Cases

- **Exploring alternatives**: Try different implementations
- **Recovering from mistakes**: Undo buggy changes
- **Iterating on features**: Experiment with variations

### Critical Limitations

**NOT TRACKED:**

- Bash command changes (`rm`, `mv`, `cp`)
- External file modifications
- Changes from other sessions

```bash
# These CANNOT be undone via rewind:
rm file.txt           # Permanent
mv old.txt new.txt    # Permanent
cp source.txt dest.txt # Creates new file

# These CAN be undone:
# Claude editing via Write/Edit tools
```

### Checkpoints vs Git

| Aspect        | Checkpoints     | Git                 |
| ------------- | --------------- | ------------------- |
| Scope         | Session-level   | Project-level       |
| Tracking      | File edits only | All committed files |
| Persistence   | 30 days         | Permanent           |
| Collaboration | Single user     | Team-wide           |
| Best for      | Quick undo      | Long-term history   |

### DSM Checkpoint Workflow

```
1. Start working on FCP changes
2. Claude makes several edits
3. Something breaks

4. Press Esc+Esc
   → Choose "Code only" to revert
   → Keep conversation context

5. Provide better guidance
6. Continue iteration
```

## Status Line Configuration

Customize the bottom status display.

### Quick Setup

```
> /statusline
# Claude helps set up custom status line
```

### Manual Configuration

In `.claude/settings.json`:

```json
{
  "statusLine": {
    "type": "command",
    "command": "~/.claude/statusline.sh",
    "padding": 0
  }
}
```

### JSON Input Available

Your script receives session data via stdin:

```json
{
  "model": {
    "id": "claude-opus-4-5",
    "display_name": "Opus"
  },
  "workspace": {
    "current_dir": "/path/to/project",
    "project_dir": "/path/to/project"
  },
  "cost": {
    "total_cost_usd": 0.01234,
    "total_duration_ms": 45000,
    "total_lines_added": 156,
    "total_lines_removed": 23
  },
  "context_window": {
    "used_percentage": 42.5,
    "remaining_percentage": 57.5,
    "context_window_size": 200000
  }
}
```

### Simple Status Line Script

```bash
#!/bin/bash
# ~/.claude/statusline.sh

input=$(cat)

MODEL=$(echo "$input" | jq -r '.model.display_name')
DIR=$(echo "$input" | jq -r '.workspace.current_dir')
COST=$(echo "$input" | jq -r '.cost.total_cost_usd')
PERCENT=$(echo "$input" | jq -r '.context_window.used_percentage // 0')

# Git branch if available
BRANCH=""
if git rev-parse --git-dir > /dev/null 2>&1; then
    BRANCH=" | $(git branch --show-current)"
fi

printf "[%s] %s%s | $%.4f | %d%%" "$MODEL" "${DIR##*/}" "$BRANCH" "$COST" "$PERCENT"
```

### DSM Status Line

Custom status for data-source-manager:

```bash
#!/bin/bash
# .claude/statusline.sh

input=$(cat)

MODEL=$(echo "$input" | jq -r '.model.display_name')
COST=$(echo "$input" | jq -r '.cost.total_cost_usd')
PERCENT=$(echo "$input" | jq -r '.context_window.used_percentage // 0')

# Show context warning
if [ "$PERCENT" -gt 80 ]; then
    CTX="⚠️ ${PERCENT}%"
else
    CTX="${PERCENT}%"
fi

printf "[%s] DSM | $%.4f | %s" "$MODEL" "$COST" "$CTX"
```

### Third-Party Status Lines

| Tool             | Features                            |
| ---------------- | ----------------------------------- |
| ccstatusline     | Powerline, themes, React/Ink UI     |
| cc-statusline    | Git, model, costs, session time     |
| claude-dashboard | Widgets, rate limits, progress bars |

### Status Line Best Practices

1. **Keep concise**: Single line, essential info only
2. **Use colors**: ANSI codes for scannable display
3. **Show context %**: Alert when approaching limits
4. **Include model**: Know which model is active
5. **Track costs**: Real-time spending awareness

## Extended Thinking Mode

Enable deeper reasoning for complex problems.

### What is Extended Thinking

Extended thinking allows Claude to spend more time reasoning through complex problems before responding:

- Multiple sequential reasoning steps
- Accuracy improves with more "thinking tokens"
- Best for math, coding, and analysis

### Enabling Extended Thinking

```bash
# Via keyboard shortcut
Alt+T (toggle during session)

# Via command
> /thinking on
> /thinking off

# Via VS Code command menu
Click "/" → Toggle Extended Thinking
```

### Budget Tokens

| Budget   | Use Case                         |
| -------- | -------------------------------- |
| 1,024    | Minimum (simple problems)        |
| 8,192    | Standard complex reasoning       |
| 32,000   | Sweet spot for most tasks        |
| 100,000+ | Very complex (batch recommended) |

### Best Use Cases

| Task Type             | Extended Thinking Benefit |
| --------------------- | ------------------------- |
| Complex STEM problems | High                      |
| Architecture design   | High                      |
| Multi-step debugging  | High                      |
| Code review           | Medium                    |
| Simple edits          | Low                       |

### Prompting Tips

1. **Start general**: Let Claude determine reasoning approach
2. **Read thinking output**: Iterate based on Claude's process
3. **Use examples**: Show how to think through similar problems
4. **Be patient**: Complex tasks need time to process

### DSM Extended Thinking

For data-source-manager complex tasks:

```
# Enable for FCP architecture decisions
> /thinking on
> "Design the cache invalidation strategy for partial OHLCV data"

# Enable for complex debugging
> "Trace why rate limiting fails intermittently for Binance API"
```

## IDE Integration

VS Code and JetBrains integration for Claude Code.

### VS Code Extension

**Installation:**

```
1. Open Extensions (Cmd+Shift+X)
2. Search "Claude Code"
3. Click Install
```

**Key Features:**

- Side-by-side diff review
- @-mention files with line ranges
- Multiple conversation tabs
- Checkpoints and rewind
- Shared history with CLI

### VS Code Commands

| Command          | Shortcut             | Description                  |
| ---------------- | -------------------- | ---------------------------- |
| Focus Input      | `Cmd+Esc`/`Ctrl+Esc` | Toggle editor/Claude         |
| Open in New Tab  | `Cmd+Shift+Esc`      | New conversation tab         |
| Insert @-Mention | `Option+K`/`Alt+K`   | Reference current file       |
| New Conversation | `Cmd+N`/`Ctrl+N`     | Start fresh (Claude focused) |

### VS Code Settings

| Setting                 | Default   | Description                     |
| ----------------------- | --------- | ------------------------------- |
| `selectedModel`         | `default` | Model for new conversations     |
| `initialPermissionMode` | `default` | Approval mode                   |
| `autosave`              | `true`    | Save before Claude reads/writes |
| `useTerminal`           | `false`   | CLI mode instead of panel       |

### JetBrains Plugin

**Installation:**

```
1. Settings → Plugins → Marketplace
2. Search "Claude Code"
3. Install and restart
```

**Features:**

- Runs CLI in integrated terminal
- IDE diff viewer for changes
- Terminal-based interaction

### Extension vs CLI

| Feature           | VS Code Extension | CLI  |
| ----------------- | ----------------- | ---- |
| Graphical panel   | Yes               | No   |
| All commands      | Subset            | Full |
| MCP configuration | No (use CLI)      | Yes  |
| Checkpoints       | Yes               | Yes  |
| `!` bash shortcut | No                | Yes  |

### Shared Configuration

Extension and CLI share:

- Conversation history
- `~/.claude/settings.json`
- CLAUDE.md files
- Permission rules

Resume CLI session in extension:

```bash
claude --resume
# Opens interactive picker
```

### Git Worktrees in IDE

For parallel tasks:

```bash
# Create worktree for feature
git worktree add ../dsm-feature-a -b feature-a

# Open in new VS Code window
code ../dsm-feature-a

# Each window runs independent Claude session
```

### DSM IDE Workflow

```
1. Open VS Code in dsm root
2. Click Spark icon (top-right)
3. Select text for context
4. Ask Claude about FCP logic
5. Review diffs in side panel
6. Accept/reject changes
7. Use /rewind if needed
```

## Headless & Batch Processing

Running Claude Code programmatically without interactive prompts.

### Headless Mode Basics

```bash
# Non-interactive execution with -p flag
claude -p "Analyze this codebase and list potential issues"

# Output format control
claude -p "Generate changelog" --output-format stream-json

# Skip permissions (use with caution)
claude -p "Refactor all tests" --dangerously-skip-permissions
```

### CI/CD Integration

```yaml
# GitHub Actions example
jobs:
  code-review:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: Install Claude Code
        run: curl -fsSL https://claude.ai/install.sh | bash
      - name: Run Code Review
        run: claude -p "Review this PR for security issues" --output-format json
        env:
          ANTHROPIC_API_KEY: ${{ secrets.ANTHROPIC_API_KEY }}
```

### Batch Processing Patterns

```bash
# Process multiple files
for file in src/**/*.py; do
    claude -p "Add docstrings to $file" --dangerously-skip-permissions
done

# Parallel processing with xargs
find . -name "*.py" | xargs -P 4 -I {} claude -p "Review {}"
```

### Output Formats

| Format        | Use Case                |
| ------------- | ----------------------- |
| `text`        | Human-readable output   |
| `json`        | Structured data parsing |
| `stream-json` | Real-time progress      |

### Message Batches API

For large-scale processing:

- Submit batches of requests asynchronously
- Most complete within 1 hour
- Results accessible for 24 hours
- Cost-effective for bulk operations

### DSM Headless Tasks

```bash
# Batch refactor all providers
for provider in binance okx coinbase; do
    claude -p "Add FCP support to ${provider} adapter" \
           --dangerously-skip-permissions
done

# CI code review
claude -p "Review changes against DSM patterns:
- FCP compliance
- Timestamp handling
- Symbol format validation
- Silent failure patterns"
```

## Troubleshooting Guide

Common issues and solutions for Claude Code.

### Quick Diagnostics

```bash
# Run health check
claude doctor

# Show detailed logs
claude --verbose

# Debug MCP issues
claude --mcp-debug
```

### Common Issues

| Issue             | Solution                                  |
| ----------------- | ----------------------------------------- |
| Node not found    | Install via nvm, ensure PATH correct      |
| Permission denied | Fix npm permissions or use native install |
| Auth failures     | `/logout`, restart, re-authenticate       |
| High CPU/memory   | Use `/compact`, restart between tasks     |
| Slow search       | Install system ripgrep                    |
| IDE not detected  | Configure firewall (WSL2) or PATH         |

### Installation Fixes

**Native install (recommended):**

```bash
curl -fsSL https://claude.ai/install.sh | bash
```

**npm permission fix:**

```bash
sudo chown -R $(whoami) ~/.npm
```

**PATH fix:**

```bash
# Add to ~/.bashrc or ~/.zshrc
export PATH="$HOME/.local/bin:$PATH"
```

### Authentication Reset

```bash
# Full authentication reset
rm -rf ~/.config/claude-code/auth.json
claude  # Re-authenticate
```

### Configuration Reset

```bash
# Reset user settings
rm ~/.claude.json
rm -rf ~/.claude/

# Reset project settings
rm -rf .claude/
rm .mcp.json
```

### Performance Issues

1. **Context saturation**: Use `/compact` regularly
2. **Large codebases**: Add build dirs to `.gitignore`
3. **WSL slow search**: Move project to Linux filesystem

### Diagnostic Checklist

```
□ Internet connection OK?
□ API key valid?
□ Node.js ≥18 installed?
□ Claude Code up to date?
□ File permissions OK?
□ Context not saturated?
□ Hooks configured correctly?
□ MCP servers responding?
```

### Getting Help

1. **In-app**: `/bug` to report with context
2. **Diagnostics**: `/doctor` for health check
3. **GitHub**: File issues with environment details
4. **Ask Claude**: Built-in documentation access

### DSM-Specific Troubleshooting

| Issue                  | Solution                              |
| ---------------------- | ------------------------------------- |
| FCP cache issues       | Use `/debug-fcp SYMBOL` command       |
| Rate limit errors      | Check Binance API response headers    |
| DataFrame validation   | Print schema with `df.schema`         |
| Symbol format mismatch | Log normalized vs raw symbols         |
| Timestamp issues       | Verify UTC, check open_time alignment |

## MCP Server Configuration

Model Context Protocol servers for external tool integration.

### Server Types

| Transport | Use Case                    | Example                        |
| --------- | --------------------------- | ------------------------------ |
| HTTP      | Remote cloud services       | GitHub, Sentry, Notion         |
| SSE       | Server-sent events (legacy) | Asana (deprecated)             |
| stdio     | Local process execution     | Database tools, custom scripts |

### Adding Servers

```bash
# HTTP server (recommended for remote)
claude mcp add --transport http github https://api.githubcopilot.com/mcp/

# SSE server (legacy)
claude mcp add --transport sse asana https://mcp.asana.com/sse

# stdio server (local process)
claude mcp add --transport stdio db -- npx -y @bytebase/dbhub \
  --dsn "postgresql://readonly:pass@host:5432/db"
```

### Scopes

| Scope   | Storage            | Visibility        |
| ------- | ------------------ | ----------------- |
| local   | `~/.claude.json`   | You, this project |
| project | `.mcp.json` (repo) | Team (committed)  |
| user    | `~/.claude.json`   | You, all projects |

### Management Commands

```bash
# List all servers
claude mcp list

# Get server details
claude mcp get github

# Remove server
claude mcp remove github

# In-session management
> /mcp
```

### Authentication

For OAuth-enabled servers:

```
> /mcp
# Select server → Authenticate
# Follow browser flow
```

### Tool Search

When MCP tools exceed 10% context, tool search activates:

```bash
# Configure threshold
ENABLE_TOOL_SEARCH=auto:5 claude  # 5% threshold

# Always enable
ENABLE_TOOL_SEARCH=true claude

# Disable
ENABLE_TOOL_SEARCH=false claude
```

### Best Practices

1. **Use HTTP for remote**: Most widely supported
2. **Trust verification**: Only install trusted servers
3. **Scope appropriately**: Project for team, user for personal
4. **Monitor context**: Disable unused servers in long sessions
5. **Debug with flag**: `claude --mcp-debug`

### DSM MCP Configuration

For data-source-manager:

```json
// .mcp.json (committed)
{
  "mcpServers": {
    "db": {
      "type": "stdio",
      "command": "npx",
      "args": ["-y", "@bytebase/dbhub", "--dsn", "${DSM_DB_URL}"]
    }
  }
}
```

## Sandboxing & Security

OS-level isolation for safer autonomous execution.

### Sandbox Overview

Sandboxing provides:

- **Filesystem isolation**: Restrict file access
- **Network isolation**: Control domain access
- **Reduced prompts**: 84% fewer permission requests
- **OS enforcement**: macOS Seatbelt, Linux bubblewrap

### Enabling Sandbox

```
> /sandbox
# Select mode:
# - Auto-allow: Sandboxed commands run automatically
# - Regular: All commands need permission
```

### Filesystem Boundaries

| Access  | Default                           |
| ------- | --------------------------------- |
| Write   | Current directory + subdirs       |
| Read    | Entire computer (with exclusions) |
| Blocked | System files, config dirs         |

### Network Boundaries

- Only approved domains accessible
- New domains require permission
- All child processes inherit restrictions

### Security Benefits

**Protection against:**

- Prompt injection attacks
- Malicious dependencies
- Data exfiltration
- Unauthorized API calls

**Filesystem protection:**

```
Cannot modify:
- ~/.bashrc, ~/.zshrc
- /bin/, /usr/bin/
- ~/.ssh/
```

**Network protection:**

```
Cannot access:
- Unapproved domains
- Attacker-controlled servers
```

### Configuration

In `settings.json`:

```json
{
  "sandbox": {
    "network": {
      "allowedHosts": ["api.binance.com", "api.github.com"]
    },
    "filesystem": {
      "allowedPaths": ["${CLAUDE_PROJECT_ROOT}", "/tmp"]
    }
  }
}
```

### Permission Deny Rules

```json
{
  "permissions": {
    "deny": [
      "Read(.env*)",
      "Read(~/.ssh/**)",
      "Read(~/.aws/**)",
      "Bash(curl * | sh)",
      "Bash(rm -rf /)"
    ]
  }
}
```

### Best Practices

1. **Start restrictive**: Expand permissions as needed
2. **Monitor violations**: Review blocked attempts
3. **Combine with permissions**: Defense in depth
4. **Test configurations**: Verify workflow compatibility
5. **Use managed settings**: Enterprise policy enforcement

### DSM Security Configuration

```json
{
  "permissions": {
    "deny": [
      "Read(.env*)",
      "Read(.mise.local.toml)",
      "Read(~/.doppler/**)",
      "Bash(pip install *)",
      "Bash(git push --force *)"
    ],
    "allow": ["Bash(uv run *)", "Bash(mise run *)"]
  },
  "sandbox": {
    "network": {
      "allowedHosts": ["api.binance.com", "fapi.binance.com", "api.okx.com"]
    }
  }
}
```

## GitHub Actions Integration

### Overview

Claude Code GitHub Actions brings AI-powered automation to your workflow with `@claude` mentions in any PR or issue. Claude can analyze code, create pull requests, implement features, and fix bugs while following your project's standards.

### Quick Setup

```bash
# In Claude Code terminal
/install-github-app
```

This guides through:

1. Installing the Claude GitHub App
2. Adding ANTHROPIC_API_KEY to repository secrets
3. Creating the workflow file

### Basic Workflow

```yaml
# .github/workflows/claude.yml
name: Claude Code
on:
  issue_comment:
    types: [created]
  pull_request_review_comment:
    types: [created]
jobs:
  claude:
    runs-on: ubuntu-latest
    steps:
      - uses: anthropics/claude-code-action@v1
        with:
          anthropic_api_key: ${{ secrets.ANTHROPIC_API_KEY }}
          # Responds to @claude mentions in comments
```

### PR Review Workflow

```yaml
name: Code Review
on:
  pull_request:
    types: [opened, synchronize]
jobs:
  review:
    runs-on: ubuntu-latest
    steps:
      - uses: anthropics/claude-code-action@v1
        with:
          anthropic_api_key: ${{ secrets.ANTHROPIC_API_KEY }}
          prompt: "/review"
          claude_args: "--max-turns 5"
```

### Scheduled Automation

```yaml
name: Daily Report
on:
  schedule:
    - cron: "0 9 * * *"
jobs:
  report:
    runs-on: ubuntu-latest
    steps:
      - uses: anthropics/claude-code-action@v1
        with:
          anthropic_api_key: ${{ secrets.ANTHROPIC_API_KEY }}
          prompt: "Generate a summary of yesterday's commits and open issues"
          claude_args: "--model claude-opus-4-5-20251101"
```

### Trigger Phrases

Common `@claude` commands:

```
@claude implement this feature based on the issue description
@claude how should I implement user authentication for this endpoint?
@claude fix the TypeError in the user dashboard component
@claude review this PR for security issues
```

### Action Parameters

| Parameter           | Description                         | Required |
| ------------------- | ----------------------------------- | -------- |
| `prompt`            | Instructions (text or skill)        | No       |
| `claude_args`       | CLI arguments passed to Claude Code | No       |
| `anthropic_api_key` | Claude API key                      | Yes\*    |
| `github_token`      | GitHub token for API access         | No       |
| `trigger_phrase`    | Custom trigger (default: "@claude") | No       |
| `use_bedrock`       | Use AWS Bedrock                     | No       |
| `use_vertex`        | Use Google Vertex AI                | No       |

\*Required for direct Claude API, not for Bedrock/Vertex

### CLI Arguments via claude_args

```yaml
claude_args: "--max-turns 5 --model claude-sonnet-4-5-20250929 --mcp-config /path/to/config.json"
```

Common arguments:

- `--max-turns`: Maximum conversation turns (default: 10)
- `--model`: Model to use
- `--mcp-config`: Path to MCP configuration
- `--allowed-tools`: Comma-separated list of allowed tools
- `--debug`: Enable debug output

### Cloud Provider Integration

#### AWS Bedrock

```yaml
name: Claude PR Action
permissions:
  contents: write
  pull-requests: write
  issues: write
  id-token: write

on:
  issue_comment:
    types: [created]
  pull_request_review_comment:
    types: [created]

jobs:
  claude-pr:
    if: contains(github.event.comment.body, '@claude')
    runs-on: ubuntu-latest
    env:
      AWS_REGION: us-west-2
    steps:
      - uses: actions/checkout@v4

      - name: Generate GitHub App token
        id: app-token
        uses: actions/create-github-app-token@v2
        with:
          app-id: ${{ secrets.APP_ID }}
          private-key: ${{ secrets.APP_PRIVATE_KEY }}

      - name: Configure AWS Credentials (OIDC)
        uses: aws-actions/configure-aws-credentials@v4
        with:
          role-to-assume: ${{ secrets.AWS_ROLE_TO_ASSUME }}
          aws-region: us-west-2

      - uses: anthropics/claude-code-action@v1
        with:
          github_token: ${{ steps.app-token.outputs.token }}
          use_bedrock: "true"
          claude_args: "--model us.anthropic.claude-sonnet-4-5-20250929-v1:0 --max-turns 10"
```

#### Google Vertex AI

```yaml
name: Claude PR Action
permissions:
  contents: write
  pull-requests: write
  issues: write
  id-token: write

on:
  issue_comment:
    types: [created]
  pull_request_review_comment:
    types: [created]

jobs:
  claude-pr:
    if: contains(github.event.comment.body, '@claude')
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Generate GitHub App token
        id: app-token
        uses: actions/create-github-app-token@v2
        with:
          app-id: ${{ secrets.APP_ID }}
          private-key: ${{ secrets.APP_PRIVATE_KEY }}

      - name: Authenticate to Google Cloud
        id: auth
        uses: google-github-actions/auth@v2
        with:
          workload_identity_provider: ${{ secrets.GCP_WORKLOAD_IDENTITY_PROVIDER }}
          service_account: ${{ secrets.GCP_SERVICE_ACCOUNT }}

      - uses: anthropics/claude-code-action@v1
        with:
          github_token: ${{ steps.app-token.outputs.token }}
          trigger_phrase: "@claude"
          use_vertex: "true"
          claude_args: "--model claude-sonnet-4@20250514 --max-turns 10"
        env:
          ANTHROPIC_VERTEX_PROJECT_ID: ${{ steps.auth.outputs.project_id }}
          CLOUD_ML_REGION: us-east5
```

### Required Secrets

#### Direct Claude API

- `ANTHROPIC_API_KEY`: API key from console.anthropic.com

#### AWS Bedrock

- `AWS_ROLE_TO_ASSUME`: IAM role ARN for Bedrock access
- `APP_ID`: GitHub App ID
- `APP_PRIVATE_KEY`: GitHub App private key

#### Google Vertex AI

- `GCP_WORKLOAD_IDENTITY_PROVIDER`: Workload identity provider resource name
- `GCP_SERVICE_ACCOUNT`: Service account email
- `APP_ID`: GitHub App ID
- `APP_PRIVATE_KEY`: GitHub App private key

### CLAUDE.md in CI/CD

Your `CLAUDE.md` file is automatically loaded in GitHub Actions:

```markdown
# CLAUDE.md

## Code Review Criteria

- Check for security vulnerabilities
- Verify test coverage for new code
- Ensure error handling follows patterns

## PR Guidelines

- Squash commits before merge
- Require at least one approval
- Run full test suite
```

### Cost Optimization

**GitHub Actions costs:**

- Runs on GitHub-hosted runners consuming Actions minutes
- Set workflow-level timeouts to avoid runaway jobs
- Use concurrency controls to limit parallel runs

**API costs:**

- Each interaction consumes tokens based on prompt/response length
- Use specific @claude commands to reduce unnecessary calls
- Configure appropriate `--max-turns` to prevent excessive iterations

### Security Best Practices

1. **Never hardcode API keys** - Always use GitHub Secrets
2. **Limit action permissions** - Only grant what's necessary
3. **Review suggestions before merging** - Human oversight
4. **Use repository-specific configurations** - Least privilege
5. **Monitor Claude's activities** - Audit logs

### DSM GitHub Actions Workflow

```yaml
# .github/workflows/claude-dsm.yml
name: Claude DSM Assistant
on:
  issue_comment:
    types: [created]
  pull_request_review_comment:
    types: [created]

jobs:
  claude:
    if: contains(github.event.comment.body, '@claude')
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - uses: anthropics/claude-code-action@v1
        with:
          anthropic_api_key: ${{ secrets.ANTHROPIC_API_KEY }}
          claude_args: |
            --max-turns 10
            --allowed-tools Read,Grep,Glob,Bash
```

## DevContainer Configuration

### Overview

DevContainers provide isolated, secure environments for running Claude Code. The container's enhanced security measures allow `--dangerously-skip-permissions` for unattended autonomous operation.

### Key Features

- **Production-ready Node.js**: Built on Node.js 20 with essential dependencies
- **Security by design**: Custom firewall restricting network access
- **Developer-friendly tools**: git, ZSH, fzf, and more
- **VS Code integration**: Pre-configured extensions and settings
- **Session persistence**: Preserves history between container restarts
- **Cross-platform**: Compatible with macOS, Windows, and Linux

### Quick Start

1. Install VS Code and Remote - Containers extension
2. Clone repository with devcontainer configuration
3. Open in VS Code
4. Click "Reopen in Container" when prompted

### Configuration Files

The devcontainer setup consists of three primary components:

#### devcontainer.json

```json
{
  "name": "Claude Code Dev",
  "build": {
    "dockerfile": "Dockerfile"
  },
  "features": {
    "ghcr.io/devcontainers/features/node:1": {
      "version": "20"
    }
  },
  "postCreateCommand": "bash .devcontainer/init-firewall.sh",
  "customizations": {
    "vscode": {
      "extensions": ["ms-python.python", "ms-python.vscode-pylance"],
      "settings": {
        "terminal.integrated.defaultProfile.linux": "zsh"
      }
    }
  },
  "mounts": [
    "source=${localEnv:HOME}/.claude,target=/home/node/.claude,type=bind"
  ],
  "remoteUser": "node"
}
```

#### Dockerfile

```dockerfile
FROM node:20-bookworm

# Install essential tools
RUN apt-get update && apt-get install -y \
    git \
    zsh \
    fzf \
    iptables \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install Claude Code
RUN npm install -g @anthropic-ai/claude-code

# Set up non-root user
USER node
WORKDIR /home/node

# Configure shell
RUN sh -c "$(curl -fsSL https://raw.githubusercontent.com/ohmyzsh/ohmyzsh/master/tools/install.sh)" "" --unattended
```

#### init-firewall.sh

```bash
#!/bin/bash
set -e

# Default policy: drop outbound
iptables -P OUTPUT DROP

# Allow loopback
iptables -A OUTPUT -o lo -j ACCEPT

# Allow established connections
iptables -A OUTPUT -m state --state ESTABLISHED,RELATED -j ACCEPT

# Allow DNS
iptables -A OUTPUT -p udp --dport 53 -j ACCEPT

# Whitelist essential domains
ALLOWED_HOSTS=(
    "api.anthropic.com"
    "api.github.com"
    "github.com"
    "registry.npmjs.org"
    "pypi.org"
    "files.pythonhosted.org"
)

for host in "${ALLOWED_HOSTS[@]}"; do
    for ip in $(dig +short "$host" 2>/dev/null); do
        iptables -A OUTPUT -d "$ip" -j ACCEPT
    done
done

# Allow SSH
iptables -A OUTPUT -p tcp --dport 22 -j ACCEPT

echo "Firewall configured successfully"
```

### Security Model

**Multi-layered protection:**

1. **Precise access control**: Outbound connections restricted to whitelisted domains
2. **Default-deny policy**: Blocks all unspecified external network access
3. **Startup verification**: Validates firewall rules on initialization
4. **Container isolation**: Separated from host system

**What's allowed:**

- npm registry, PyPI
- GitHub, Claude API
- Outbound DNS and SSH

**What's blocked:**

- All other external network access
- Direct host filesystem access (except mounts)
- Privilege escalation

### Autonomous Mode

With firewall protection, run autonomously:

```bash
claude --dangerously-skip-permissions
```

**Warning**: Only use with trusted repositories. DevContainers don't prevent exfiltration of anything accessible in the container including Claude Code credentials.

### Use Cases

#### Secure Client Work

Isolate different client projects:

```json
{
  "name": "Client-A-Project",
  "build": {
    "dockerfile": "Dockerfile"
  },
  "mounts": ["source=${localWorkspaceFolder},target=/workspace,type=bind"],
  "containerEnv": {
    "CLIENT": "client-a"
  }
}
```

#### Team Onboarding

Consistent environment for all team members:

```json
{
  "name": "DSM Development",
  "features": {
    "ghcr.io/devcontainers/features/python:1": {
      "version": "3.13"
    }
  },
  "postCreateCommand": "uv sync && uv run pre-commit install"
}
```

#### CI/CD Mirror

Match development and production environments:

```yaml
# .github/workflows/test.yml
jobs:
  test:
    runs-on: ubuntu-latest
    container:
      image: ghcr.io/your-org/dsm-devcontainer:latest
    steps:
      - uses: actions/checkout@v4
      - run: uv run pytest
```

### Resource Configuration

```json
{
  "hostRequirements": {
    "cpus": 4,
    "memory": "8gb",
    "storage": "32gb"
  },
  "runArgs": ["--memory=4g", "--cpus=2"]
}
```

**Note**: 2GB RAM restriction makes Claude Code struggle. 4GB works well.

### MCP Server in DevContainer

```json
{
  "mounts": [
    "source=${localEnv:HOME}/.claude,target=/home/node/.claude,type=bind"
  ],
  "postCreateCommand": "claude mcp add filesystem /workspace"
}
```

### DSM DevContainer Configuration

```json
{
  "name": "DSM Development",
  "build": {
    "dockerfile": "Dockerfile",
    "context": ".."
  },
  "features": {
    "ghcr.io/devcontainers/features/python:1": {
      "version": "3.13"
    },
    "ghcr.io/devcontainers/features/node:1": {
      "version": "20"
    }
  },
  "postCreateCommand": "bash .devcontainer/init-firewall.sh && uv sync",
  "customizations": {
    "vscode": {
      "extensions": [
        "ms-python.python",
        "ms-python.vscode-pylance",
        "charliermarsh.ruff"
      ],
      "settings": {
        "python.defaultInterpreterPath": ".venv/bin/python"
      }
    }
  },
  "mounts": [
    "source=${localEnv:HOME}/.claude,target=/home/node/.claude,type=bind",
    "source=dsm-cache,target=/home/node/.cache,type=volume"
  ],
  "containerEnv": {
    "PYTHONDONTWRITEBYTECODE": "1"
  },
  "remoteUser": "node"
}
```

### Best Practices

1. **Use firewall rules**: Default-deny with explicit allowlist
2. **Mount credentials carefully**: Only what's needed
3. **Set resource limits**: Prevent runaway containers
4. **Version your devcontainer**: Track in git
5. **Test locally first**: Before using `--dangerously-skip-permissions`
6. **Monitor activities**: Even in containers

### Caveats

- **IDE integration limited**: No access to IDE diagnostics, open files, selected ranges
- **Terminal-only**: Claude operates through terminal interface
- **GitHub access**: Mounted auth still allows repository operations
- **Memory requirements**: 4GB minimum for smooth operation

## Structured Outputs

### Overview

Structured outputs constrain Claude's responses to follow a specific JSON schema, ensuring valid, parseable output for downstream processing. Two complementary features:

- **JSON outputs** (`output_config.format`): Get responses in a specific JSON format
- **Strict tool use** (`strict: true`): Guarantee schema validation on tool inputs

### Why Use Structured Outputs

Without structured outputs:

- Parsing errors from invalid JSON syntax
- Missing required fields
- Inconsistent data types
- Schema violations requiring retries

With structured outputs:

- **Always valid**: No `JSON.parse()` errors
- **Type safe**: Guaranteed field types and required fields
- **Reliable**: No retries needed for schema violations

### JSON Schema API

```python
import anthropic

client = anthropic.Anthropic()

response = client.messages.create(
    model="claude-sonnet-4-5",
    max_tokens=1024,
    messages=[
        {
            "role": "user",
            "content": "Extract contact info from: John Smith (john@example.com)"
        }
    ],
    output_config={
        "format": {
            "type": "json_schema",
            "schema": {
                "type": "object",
                "properties": {
                    "name": {"type": "string"},
                    "email": {"type": "string"}
                },
                "required": ["name", "email"],
                "additionalProperties": False
            }
        }
    }
)
```

### Pydantic Integration (Python)

```python
from pydantic import BaseModel
from anthropic import Anthropic

class ContactInfo(BaseModel):
    name: str
    email: str
    plan_interest: str
    demo_requested: bool

client = Anthropic()

# Using parse() method - automatic transformation and validation
response = client.messages.parse(
    model="claude-sonnet-4-5",
    max_tokens=1024,
    messages=[{"role": "user", "content": "Extract: John Smith..."}],
    output_format=ContactInfo,
)

# Access typed output directly
contact = response.parsed_output
print(contact.name, contact.email)
```

### Zod Integration (TypeScript)

```typescript
import Anthropic from "@anthropic-ai/sdk";
import { z } from "zod";
import { zodOutputFormat } from "@anthropic-ai/sdk/helpers/zod";

const ContactSchema = z.object({
  name: z.string(),
  email: z.string(),
  plan_interest: z.string(),
  demo_requested: z.boolean(),
});

const client = new Anthropic();

const response = await client.messages.create({
  model: "claude-sonnet-4-5",
  max_tokens: 1024,
  messages: [{ role: "user", content: "Extract: John Smith..." }],
  output_config: { format: zodOutputFormat(ContactSchema) },
});
```

### Strict Tool Use

Validate tool parameters for agentic workflows:

```python
response = client.messages.create(
    model="claude-sonnet-4-5",
    max_tokens=1024,
    messages=[{"role": "user", "content": "What's the weather in SF?"}],
    tools=[{
        "name": "get_weather",
        "description": "Get current weather",
        "strict": True,  # Enable strict mode
        "input_schema": {
            "type": "object",
            "properties": {
                "location": {"type": "string"},
                "unit": {"type": "string", "enum": ["celsius", "fahrenheit"]}
            },
            "required": ["location"],
            "additionalProperties": False
        }
    }]
)
```

**Guarantees:**

- Tool `input` strictly follows the `input_schema`
- Tool `name` is always valid
- Types are correct (no `"2"` instead of `2`)

### Combined Usage

Use JSON outputs and strict tool use together:

```python
response = client.messages.create(
    model="claude-sonnet-4-5",
    max_tokens=1024,
    messages=[{"role": "user", "content": "Plan a trip to Paris"}],
    # JSON outputs for response format
    output_config={
        "format": {
            "type": "json_schema",
            "schema": {
                "type": "object",
                "properties": {
                    "summary": {"type": "string"},
                    "next_steps": {"type": "array", "items": {"type": "string"}}
                },
                "required": ["summary", "next_steps"],
                "additionalProperties": False
            }
        }
    },
    # Strict tool use for validated parameters
    tools=[{
        "name": "search_flights",
        "strict": True,
        "input_schema": {
            "type": "object",
            "properties": {
                "destination": {"type": "string"},
                "date": {"type": "string", "format": "date"}
            },
            "required": ["destination", "date"],
            "additionalProperties": False
        }
    }]
)
```

### Common Use Cases

**Data Extraction:**

```python
class Invoice(BaseModel):
    invoice_number: str
    date: str
    total_amount: float
    line_items: List[dict]

response = client.messages.parse(
    model="claude-sonnet-4-5",
    output_format=Invoice,
    messages=[{"role": "user", "content": f"Extract: {invoice_text}"}]
)
```

**Classification:**

```python
class Classification(BaseModel):
    category: str
    confidence: float
    tags: List[str]
    sentiment: str

response = client.messages.parse(
    model="claude-sonnet-4-5",
    output_format=Classification,
    messages=[{"role": "user", "content": f"Classify: {feedback_text}"}]
)
```

### JSON Schema Limitations

**Supported:**

- Basic types: object, array, string, integer, number, boolean, null
- `enum`, `const`, `anyOf`, `allOf`
- `$ref`, `$def`, `definitions`
- String formats: date-time, date, email, uri, uuid
- Array `minItems` (0 or 1 only)

**Not Supported:**

- Recursive schemas
- Complex types within enums
- External `$ref`
- Numerical constraints (minimum, maximum)
- String constraints (minLength, maxLength)
- `additionalProperties` other than `false`

### SDK Transformation

SDKs automatically transform schemas with unsupported features:

1. Remove unsupported constraints
2. Update descriptions with constraint info
3. Add `additionalProperties: false`
4. Validate responses against original schema

### Performance Considerations

- **First request latency**: Grammar compilation on first use
- **Automatic caching**: Compiled grammars cached 24 hours
- **Cache invalidation**: Changes to schema structure invalidate cache
- **Token costs**: Schema explanation injected into system prompt

### Invalid Outputs

**Refusals** (`stop_reason: "refusal"`):

- Claude maintains safety properties
- Output may not match schema
- 200 status code, billed for tokens

**Token limit** (`stop_reason: "max_tokens"`):

- Output may be incomplete
- Retry with higher `max_tokens`

### DSM Structured Outputs Example

```python
from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime

class OHLCVBar(BaseModel):
    open_time: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float

class DataFetchResult(BaseModel):
    symbol: str
    interval: str
    bars: List[OHLCVBar]
    source: str
    cache_hit: bool
    fetch_duration_ms: Optional[float]

# Use in Claude API for structured data extraction
response = client.messages.parse(
    model="claude-sonnet-4-5",
    output_format=DataFetchResult,
    messages=[{"role": "user", "content": f"Parse this OHLCV response: {raw_data}"}]
)
```

## MCP Server Ecosystem

### Overview

The Model Context Protocol (MCP) provides a standardized way to connect Claude with external tools, data sources, and services. Essential MCP servers enhance Claude Code's capabilities.

### Core Development Servers

#### Sequential Thinking

Enables structured problem-solving with iterative refinement.

```bash
claude mcp add sequential-thinking npx -- -y @modelcontextprotocol/server-sequential-thinking
```

**Use cases:**

- Complex architectural decisions
- Multi-step debugging
- Design trade-off analysis

#### Context7

Fetches real-time documentation from source repositories.

```bash
claude mcp add --transport http context7 https://mcp.context7.com/mcp
```

**Usage in prompts:**

```
Create a React Server Component using Next.js 14 patterns - use context7
```

**Benefits:**

- Eliminates outdated API suggestions
- Version-specific documentation
- Current code examples

#### GitHub

Direct repository, PR, and CI/CD workflow management.

```bash
claude mcp add --transport http github https://api.githubcopilot.com/mcp/
```

**Use cases:**

- PR reviews and creation
- Issue management
- Workflow monitoring

#### Playwright

Web automation using accessibility trees.

```bash
claude mcp add playwright npx -- @playwright/mcp@latest
```

**Use cases:**

- End-to-end testing
- Web scraping
- Browser automation

#### Apidog

API specification integration and code generation.

```bash
claude mcp add apidog -- npx -y apidog-mcp-server@latest --oas=<openapi-url>
```

**Benefits:**

- Type-safe client generation
- API validation
- Documentation-implementation sync

### Cloud Infrastructure Servers

#### AWS

```bash
claude mcp add aws npx -- -y @aws/mcp-server-aws
```

**Capabilities:**

- Infrastructure provisioning
- Service management
- Real-time debugging

#### Cloudflare

```bash
claude mcp add cloudflare npx -- -y @cloudflare/mcp-server-cloudflare
```

**Capabilities:**

- Workers deployment
- KV storage management
- DNS configuration

#### Google Cloud Platform

```bash
claude mcp add gcp npx -- -y @anthropic/mcp-server-gcp
```

**Capabilities:**

- Cloud Run deployment
- BigQuery queries
- IAM management

### Observability Servers

#### Sentry

```bash
claude mcp add sentry npx -- -y @sentry/mcp-server
```

**Capabilities:**

- Error tracking
- Performance monitoring
- Release health

#### PostHog

```bash
claude mcp add posthog npx -- -y @posthog/mcp-server
```

**Capabilities:**

- User behavior analytics
- Feature flag management
- A/B testing data

### Project Management Servers

#### Linear

```bash
claude mcp add linear -- npx -y @anthropic/mcp-server-linear
```

**Capabilities:**

- Issue creation and management
- Sprint planning
- Workflow automation

#### Notion

```bash
claude mcp add notion -- npx -y @anthropic/mcp-server-notion
```

**Capabilities:**

- Page creation and editing
- Database queries
- Knowledge base access

### Desktop Commander

Local-first MCP server for filesystem and terminal control.

```bash
claude mcp add desktop-commander npx -- -y @wonderwhy-er/desktop-commander-mcp
```

**Capabilities:**

- Terminal command execution
- File system operations
- Diff-based code editing

### DSM MCP Configuration

```json
{
  "mcpServers": {
    "sequential-thinking": {
      "command": "npx",
      "args": ["-y", "@modelcontextprotocol/server-sequential-thinking"]
    },
    "context7": {
      "transport": "http",
      "url": "https://mcp.context7.com/mcp"
    },
    "filesystem": {
      "command": "npx",
      "args": [
        "-y",
        "@anthropic/mcp-server-filesystem",
        "${CLAUDE_PROJECT_ROOT}"
      ]
    }
  }
}
```

### Installation Best Practices

1. **Scope appropriately**: Use `--scope project` for project-specific servers
2. **Configure authentication**: Store tokens in environment variables
3. **Test connectivity**: Verify server responds before complex workflows
4. **Monitor usage**: Track API calls and rate limits
5. **Update regularly**: Keep servers updated for security and features

### MCP Server Selection Guide

| Need                  | Server              | Priority |
| --------------------- | ------------------- | -------- |
| Structured reasoning  | Sequential Thinking | High     |
| Current documentation | Context7            | High     |
| GitHub workflow       | GitHub              | High     |
| Browser automation    | Playwright          | Medium   |
| API development       | Apidog              | Medium   |
| Cloud infrastructure  | AWS/GCP/Cloudflare  | Medium   |
| Error tracking        | Sentry              | Medium   |
| Project management    | Linear/Notion       | Low      |

## Claude Agent SDK

### Overview

The Claude Agent SDK (formerly Claude Code SDK) enables building AI agents programmatically with the same tools, agent loop, and context management that power Claude Code.

### Installation

**Python:**

```bash
pip install claude-agent-sdk
```

**TypeScript:**

```bash
npm install @anthropic-ai/claude-agent-sdk
```

### Basic Usage

```python
import asyncio
from claude_agent_sdk import query, ClaudeAgentOptions

async def main():
    async for message in query(
        prompt="Find and fix the bug in auth.py",
        options=ClaudeAgentOptions(allowed_tools=["Read", "Edit", "Bash"])
    ):
        print(message)

asyncio.run(main())
```

```typescript
import { query } from "@anthropic-ai/claude-agent-sdk";

for await (const message of query({
  prompt: "Find and fix the bug in auth.py",
  options: { allowedTools: ["Read", "Edit", "Bash"] },
})) {
  console.log(message);
}
```

### Built-in Tools

| Tool            | Description                                    |
| --------------- | ---------------------------------------------- |
| Read            | Read any file in working directory             |
| Write           | Create new files                               |
| Edit            | Make precise edits to existing files           |
| Bash            | Run terminal commands, scripts, git operations |
| Glob            | Find files by pattern                          |
| Grep            | Search file contents with regex                |
| WebSearch       | Search the web                                 |
| WebFetch        | Fetch and parse web content                    |
| AskUserQuestion | Ask clarifying questions                       |

### Hooks

Run custom code at key points in agent lifecycle:

```python
from claude_agent_sdk import query, ClaudeAgentOptions, HookMatcher

async def log_file_change(input_data, tool_use_id, context):
    file_path = input_data.get('tool_input', {}).get('file_path', 'unknown')
    with open('./audit.log', 'a') as f:
        f.write(f"{datetime.now()}: modified {file_path}\n")
    return {}

async def main():
    async for message in query(
        prompt="Refactor utils.py",
        options=ClaudeAgentOptions(
            permission_mode="acceptEdits",
            hooks={
                "PostToolUse": [HookMatcher(matcher="Edit|Write", hooks=[log_file_change])]
            }
        )
    ):
        print(message)
```

**Available hooks:**

- `PreToolUse`: Before tool execution
- `PostToolUse`: After tool execution
- `Stop`: When agent stops
- `SessionStart`: At session start
- `SessionEnd`: At session end
- `UserPromptSubmit`: On user prompt

### Subagents

Define specialized agents for delegation:

```python
from claude_agent_sdk import query, ClaudeAgentOptions, AgentDefinition

async for message in query(
    prompt="Use the code-reviewer agent to review this codebase",
    options=ClaudeAgentOptions(
        allowed_tools=["Read", "Glob", "Grep", "Task"],
        agents={
            "code-reviewer": AgentDefinition(
                description="Expert code reviewer for quality and security",
                prompt="Analyze code quality and suggest improvements",
                tools=["Read", "Glob", "Grep"]
            )
        }
    )
):
    print(message)
```

### MCP Integration

Connect external systems via Model Context Protocol:

```python
async for message in query(
    prompt="Open example.com and describe what you see",
    options=ClaudeAgentOptions(
        mcp_servers={
            "playwright": {"command": "npx", "args": ["@playwright/mcp@latest"]}
        }
    )
):
    print(message)
```

### Session Management

Maintain context across exchanges:

```python
session_id = None

# First query: capture session ID
async for message in query(
    prompt="Read the authentication module",
    options=ClaudeAgentOptions(allowed_tools=["Read", "Glob"])
):
    if hasattr(message, 'subtype') and message.subtype == 'init':
        session_id = message.session_id

# Resume with full context
async for message in query(
    prompt="Now find all places that call it",
    options=ClaudeAgentOptions(resume=session_id)
):
    print(message.result)
```

### Permission Modes

| Mode                | Description                |
| ------------------- | -------------------------- |
| `bypassPermissions` | Skip all permission checks |
| `acceptEdits`       | Auto-approve file edits    |
| `askForPermission`  | Prompt for each action     |

### Authentication

Environment variables for cloud providers:

- **Anthropic API**: `ANTHROPIC_API_KEY`
- **AWS Bedrock**: `CLAUDE_CODE_USE_BEDROCK=1` + AWS credentials
- **Google Vertex**: `CLAUDE_CODE_USE_VERTEX=1` + GCP credentials
- **Microsoft Foundry**: `CLAUDE_CODE_USE_FOUNDRY=1` + Azure credentials

### Claude Code Features in SDK

Enable filesystem-based configuration:

```python
options = ClaudeAgentOptions(
    setting_sources=["project"]  # Load .claude/ config
)
```

| Feature        | Location                          |
| -------------- | --------------------------------- |
| Skills         | `.claude/skills/SKILL.md`         |
| Slash commands | `.claude/commands/*.md`           |
| Memory         | `CLAUDE.md`                       |
| Plugins        | Programmatic via `plugins` option |

### SDK vs CLI Comparison

| Use Case                | Best Choice |
| ----------------------- | ----------- |
| Interactive development | CLI         |
| CI/CD pipelines         | SDK         |
| Custom applications     | SDK         |
| One-off tasks           | CLI         |
| Production automation   | SDK         |

### DSM Agent Example

```python
import asyncio
from claude_agent_sdk import query, ClaudeAgentOptions, AgentDefinition

async def fetch_market_data():
    async for message in query(
        prompt="Fetch BTCUSDT data for the last 7 days and validate the result",
        options=ClaudeAgentOptions(
            allowed_tools=["Read", "Edit", "Bash", "Glob", "Task"],
            agents={
                "data-validator": AgentDefinition(
                    description="Validates OHLCV data quality",
                    prompt="Check for gaps, outliers, and data integrity",
                    tools=["Read", "Bash"]
                )
            },
            mcp_servers={
                "filesystem": {
                    "command": "npx",
                    "args": ["-y", "@anthropic/mcp-server-filesystem", "."]
                }
            }
        )
    ):
        if hasattr(message, "result"):
            return message.result

asyncio.run(fetch_market_data())
```

## Prompt Engineering Best Practices

### Overview

Claude 4.x models (Sonnet 4.5, Haiku 4.5, Opus 4.5) are trained for precise instruction following. These best practices optimize Claude's performance.

### General Principles

#### Be Explicit

Claude 4.x responds well to clear, explicit instructions.

**Less effective:**

```
Create an analytics dashboard
```

**More effective:**

```
Create an analytics dashboard. Include as many relevant features and
interactions as possible. Go beyond the basics to create a fully-featured
implementation.
```

#### Provide Context

Explain why the behavior matters:

**Less effective:**

```
NEVER use ellipses
```

**More effective:**

```
Your response will be read aloud by a text-to-speech engine, so never
use ellipses since the engine cannot pronounce them.
```

#### Long-Horizon Reasoning

Claude 4.5 excels at extended sessions with state tracking:

```
Your context window will be automatically compacted as it approaches its
limit, allowing you to continue working indefinitely. Do not stop tasks
early due to token budget concerns. Save progress before context refreshes.
Always be persistent and autonomous. Never artificially stop any task
early regardless of context remaining.
```

### Multi-Context Window Workflows

1. **Use different prompt for first window**: Set up framework (tests, scripts)
2. **Write tests in structured format**: Keep in `tests.json`
3. **Create quality-of-life tools**: Setup scripts like `init.sh`
4. **Starting fresh vs compacting**: Let Claude discover state from filesystem

```
Call pwd; you can only read and write files in this directory.
Review progress.txt, tests.json, and the git logs.
Run through integration test before implementing new features.
```

### State Management

**Structured formats for state data:**

```json
{
  "tests": [
    { "id": 1, "name": "authentication_flow", "status": "passing" },
    { "id": 2, "name": "user_management", "status": "failing" }
  ],
  "total": 200,
  "passing": 150,
  "failing": 25
}
```

**Unstructured for progress notes:**

```
Session 3 progress:
- Fixed authentication token validation
- Updated user model for edge cases
- Next: investigate user_management test failures
```

### Tool Usage Patterns

Claude 4.5 benefits from explicit direction:

**Less effective (suggests only):**

```
Can you suggest some changes to improve this function?
```

**More effective (takes action):**

```
Change this function to improve its performance.
```

**Prompt for proactive action:**

```xml
<default_to_action>
By default, implement changes rather than only suggesting them. If intent
is unclear, infer the most useful action and proceed using tools to
discover missing details instead of guessing.
</default_to_action>
```

**Prompt for conservative action:**

```xml
<do_not_act_before_instructions>
Do not jump into implementation unless clearly instructed. When intent is
ambiguous, default to providing information and recommendations rather
than taking action.
</do_not_act_before_instructions>
```

### Output Formatting

1. **Tell Claude what to do, not what not to do**
2. **Use XML format indicators**: `<smoothly_flowing_prose_paragraphs>`
3. **Match prompt style to desired output**
4. **Detailed prompts for formatting preferences**

```xml
<avoid_excessive_markdown_and_bullet_points>
When writing reports or analyses, write in clear, flowing prose using
complete paragraphs. Reserve markdown for `inline code`, code blocks,
and simple headings. Avoid **bold** and *italics*.

DO NOT use lists unless: a) presenting truly discrete items, or
b) user explicitly requests a list.

Your goal is readable, flowing text that guides the reader naturally.
</avoid_excessive_markdown_and_bullet_points>
```

### Parallel Tool Calling

Claude 4.x excels at parallel execution:

```xml
<use_parallel_tool_calls>
If calling multiple tools with no dependencies, make all independent calls
in parallel. Prioritize simultaneous actions over sequential. For example,
reading 3 files should be 3 parallel tool calls. Maximize parallel use
for speed. However, if some calls depend on previous results, call them
sequentially. Never use placeholders or guess missing parameters.
</use_parallel_tool_calls>
```

### Minimize Over-Engineering

```
Avoid over-engineering. Only make changes that are directly requested or
clearly necessary. Keep solutions simple and focused.

Don't add features, refactor code, or make "improvements" beyond what was
asked. A bug fix doesn't need surrounding code cleaned up.

Don't create helpers, utilities, or abstractions for one-time operations.
The right amount of complexity is the minimum needed for the current task.
```

### Code Exploration

```
ALWAYS read and understand relevant files before proposing code edits.
Do not speculate about code you have not inspected. If the user references
a specific file/path, you MUST open and inspect it before explaining or
proposing fixes. Be rigorous and persistent in searching code for key facts.
```

### Minimize Hallucinations

```xml
<investigate_before_answering>
Never speculate about code you have not opened. If the user references a
specific file, you MUST read it before answering. Investigate and read
relevant files BEFORE answering questions about the codebase. Never make
claims about code before investigating - give grounded, hallucination-free
answers.
</investigate_before_answering>
```

### Extended Thinking

Guide Claude's reasoning:

```
After receiving tool results, carefully reflect on their quality and
determine optimal next steps before proceeding. Use thinking to plan
and iterate based on new information.
```

### Frontend Design

```xml
<frontend_aesthetics>
Avoid generic "AI slop" aesthetic. Make creative, distinctive frontends.

Focus on:
- Typography: Choose beautiful, unique fonts. Avoid Arial, Inter.
- Color & Theme: Commit to cohesive aesthetic. Use CSS variables.
- Motion: Use animations for micro-interactions. CSS-only for HTML.
- Backgrounds: Create atmosphere and depth, not solid colors.

Avoid: overused fonts, clichéd purple gradients, predictable layouts.
</frontend_aesthetics>
```

### DSM-Specific Prompting

```xml
<dsm_development_context>
This is a financial data management library. Key patterns:

- Always use UTC timestamps (datetime.now(timezone.utc))
- Never use bare except clauses
- Validate symbol formats before API calls
- Check DataFrame structure (open_time, open, high, low, close, volume)
- Use Polars for DataFrames, not pandas
- FCP (Failover Control Protocol) handles data source fallback

When implementing features, follow these domain patterns strictly.
</dsm_development_context>
```

## Computer Use and Vision

### Overview

Computer use enables Claude to interact with desktop environments through screenshot capture, mouse control, keyboard input, and desktop automation. Vision capabilities allow Claude to process and understand images.

### Computer Use Tool

The computer use tool provides:

- **Screenshot capture**: See what's on screen
- **Mouse control**: Click, drag, move cursor
- **Keyboard input**: Type text and shortcuts
- **Desktop automation**: Interact with any application

### Quick Start

```python
import anthropic

client = anthropic.Anthropic()

response = client.beta.messages.create(
    model="claude-sonnet-4-5",
    max_tokens=1024,
    tools=[
        {
          "type": "computer_20250124",
          "name": "computer",
          "display_width_px": 1024,
          "display_height_px": 768,
          "display_number": 1,
        },
        {
          "type": "text_editor_20250728",
          "name": "str_replace_based_edit_tool"
        },
        {
          "type": "bash_20250124",
          "name": "bash"
        }
    ],
    messages=[{"role": "user", "content": "Save a picture of a cat to my desktop."}],
    betas=["computer-use-2025-01-24"]
)
```

### Available Actions

**Basic actions:**

- `screenshot`: Capture current display
- `left_click`: Click at coordinates
- `type`: Type text string
- `key`: Press key combination
- `mouse_move`: Move cursor

**Enhanced actions (Claude 4+):**

- `scroll`: Scroll with direction control
- `left_click_drag`: Click and drag
- `right_click`, `middle_click`: Additional buttons
- `double_click`, `triple_click`: Multiple clicks
- `hold_key`: Hold key for duration
- `wait`: Pause between actions

**Opus 4.5 additions:**

- `zoom`: View screen region at full resolution

### Action Examples

```json
// Take screenshot
{"action": "screenshot"}

// Click at position
{"action": "left_click", "coordinate": [500, 300]}

// Type text
{"action": "type", "text": "Hello, world!"}

// Scroll down
{
  "action": "scroll",
  "coordinate": [500, 400],
  "scroll_direction": "down",
  "scroll_amount": 3
}

// Shift+click for selection
{
  "action": "left_click",
  "coordinate": [500, 300],
  "text": "shift"
}
```

### Agent Loop

```python
async def computer_use_loop(client, prompt, max_iterations=10):
    messages = [{"role": "user", "content": prompt}]
    tools = [
        {"type": "computer_20250124", "name": "computer",
         "display_width_px": 1024, "display_height_px": 768}
    ]

    for _ in range(max_iterations):
        response = client.beta.messages.create(
            model="claude-sonnet-4-5",
            max_tokens=4096,
            messages=messages,
            tools=tools,
            betas=["computer-use-2025-01-24"]
        )

        messages.append({"role": "assistant", "content": response.content})

        tool_results = []
        for block in response.content:
            if block.type == "tool_use":
                result = execute_computer_action(block.input)
                tool_results.append({
                    "type": "tool_result",
                    "tool_use_id": block.id,
                    "content": result
                })

        if not tool_results:
            return messages

        messages.append({"role": "user", "content": tool_results})

    return messages
```

### Security Considerations

1. **Use dedicated VM or container** with minimal privileges
2. **Avoid sensitive data access** (credentials, personal info)
3. **Limit internet access** to allowlist of domains
4. **Require human confirmation** for consequential actions
5. **Isolate from sensitive accounts** and data

### Vision Capabilities

Claude can process images for:

- **OCR**: Extract text from screenshots
- **Chart interpretation**: Understand data visualizations
- **UI understanding**: Navigate interfaces
- **Diagram analysis**: Process technical diagrams

### Screenshot Automation (macOS)

Using Hammerspoon for seamless workflow:

```lua
-- ~/.hammerspoon/init.lua
hs.hotkey.bind({"cmd", "shift"}, "6", function()
    local task = hs.task.new("/usr/sbin/screencapture", function(exitCode, stdOut, stdErr)
        if exitCode == 0 then
            local pasteboard = hs.pasteboard.setContents(stdOut)
        end
    end, {"-i", "-c"})
    task:start()
end)
```

Press Cmd+Shift+6 to capture region and paste directly into terminal.

### Limitations

1. **Latency**: Slower than human interaction
2. **Vision accuracy**: May hallucinate coordinates
3. **Tool selection**: May use wrong tools for tasks
4. **Scrolling**: Better with explicit scroll actions
5. **Multi-app interaction**: Lower reliability

### Best Practices

1. **Specify simple, well-defined tasks**
2. **Request explicit verification**: "After each step, take a screenshot"
3. **Use keyboard shortcuts** for tricky UI elements
4. **Provide example screenshots** for repeatable tasks
5. **Set appropriate display resolution** (1024x768 to 1280x800)

### Coordinate Scaling

Handle high resolutions by scaling:

```python
import math

def get_scale_factor(width, height):
    """Calculate scale for API constraints (1568px max, 1.15MP total)."""
    long_edge = max(width, height)
    total_pixels = width * height

    long_edge_scale = 1568 / long_edge
    total_pixels_scale = math.sqrt(1_150_000 / total_pixels)

    return min(1.0, long_edge_scale, total_pixels_scale)

# Scale coordinates back up when executing
def execute_click(x, y, scale):
    screen_x = x / scale
    screen_y = y / scale
    perform_click(screen_x, screen_y)
```

### DSM Vision Use Cases

```python
# Validate trading chart screenshots
response = client.messages.create(
    model="claude-sonnet-4-5",
    max_tokens=1024,
    messages=[{
        "role": "user",
        "content": [
            {"type": "text", "text": "Analyze this BTCUSDT chart. What pattern do you see?"},
            {"type": "image", "source": {"type": "base64", "media_type": "image/png", "data": chart_base64}}
        ]
    }]
)
```

## Enterprise Deployment

### Overview

Organizations can deploy Claude Code through Anthropic directly or cloud providers. Most organizations benefit from Claude for Teams or Enterprise.

### Deployment Options

| Feature    | Teams/Enterprise | Console     | Bedrock | Vertex | Foundry |
| ---------- | ---------------- | ----------- | ------- | ------ | ------- |
| Best for   | Most orgs        | Individuals | AWS     | GCP    | Azure   |
| Billing    | Per-seat         | PAYG        | AWS     | GCP    | Azure   |
| SSO        | Enterprise       | No          | IAM     | IAM    | RBAC    |
| Web access | Yes              | No          | No      | No     | No      |

### Claude for Teams

Self-service with:

- Collaboration features
- Admin tools
- Billing management
- Usage dashboard

### Claude for Enterprise

Adds:

- SSO and domain capture
- Role-based permissions
- Compliance API access
- Managed policy settings

### Cloud Provider Setup

#### Amazon Bedrock

```bash
# Enable Bedrock
export CLAUDE_CODE_USE_BEDROCK=1
export AWS_REGION=us-east-1

# Optional: Corporate proxy
export HTTPS_PROXY='https://proxy.example.com:8080'

# Optional: LLM Gateway
export ANTHROPIC_BEDROCK_BASE_URL='https://gateway.example.com/bedrock'
export CLAUDE_CODE_SKIP_BEDROCK_AUTH=1
```

#### Google Vertex AI

```bash
# Enable Vertex
export CLAUDE_CODE_USE_VERTEX=1
export CLOUD_ML_REGION=us-east5
export ANTHROPIC_VERTEX_PROJECT_ID=your-project-id

# Optional: Corporate proxy
export HTTPS_PROXY='https://proxy.example.com:8080'

# Optional: LLM Gateway
export ANTHROPIC_VERTEX_BASE_URL='https://gateway.example.com/vertex'
export CLAUDE_CODE_SKIP_VERTEX_AUTH=1
```

#### Microsoft Foundry

```bash
# Enable Foundry
export CLAUDE_CODE_USE_FOUNDRY=1
export ANTHROPIC_FOUNDRY_RESOURCE=your-resource
export ANTHROPIC_FOUNDRY_API_KEY=your-api-key

# Optional: Corporate proxy
export HTTPS_PROXY='https://proxy.example.com:8080'
```

### Managed Permissions

Security teams configure permissions that cannot be overwritten locally:

```json
{
  "managed_permissions": {
    "deny": ["Read(.env*)", "Read(~/.aws/**)", "Bash(git push --force *)"],
    "allow": ["Bash(uv run *)", "Bash(npm *)"]
  }
}
```

### Organization Best Practices

1. **Invest in documentation**: Deploy CLAUDE.md files at org and repo levels
2. **Simplify deployment**: Create "one click" installation
3. **Start with guided usage**: Begin with Q&A and small fixes
4. **Configure security policies**: Managed permissions for sensitive actions
5. **Leverage MCP**: Connect ticket systems, error logs via MCP servers

### CLAUDE.md Deployment Levels

| Level        | Location                                            | Purpose              |
| ------------ | --------------------------------------------------- | -------------------- |
| Organization | `/Library/Application Support/ClaudeCode/CLAUDE.md` | Company standards    |
| Repository   | `CLAUDE.md` in repo root                            | Project architecture |
| Directory    | `src/CLAUDE.md`, `tests/CLAUDE.md`                  | Context-specific     |

### Admin Controls

- **Seat management**: Purchase and allocate seats
- **Spend controls**: Org and user-level limits
- **Usage analytics**: Lines accepted, suggestion rate
- **Policy settings**: Tool permissions, file access

### Security Features

- **SSO**: SAML/OIDC integration
- **Role-based access**: Admin/Member roles
- **Audit logs**: Activity tracking
- **Data retention**: Custom controls
- **Compliance API**: Programmatic usage access

### DSM Enterprise Configuration

```bash
# .envrc or team configuration
export CLAUDE_CODE_USE_BEDROCK=1
export AWS_REGION=us-west-2
export ANTHROPIC_BEDROCK_BASE_URL='https://internal-gateway.company.com/bedrock'

# Managed settings deployed via MDM
# /Library/Application Support/ClaudeCode/managed-settings.json
```

## Cost Management

### Overview

Token costs scale with context size. Average cost is ~$6/developer/day, with team usage ~$100-200/developer/month with Sonnet 4.5.

### Tracking Costs

```
/cost
Total cost:            $0.55
Total duration (API):  6m 19.7s
Total duration (wall): 6h 33m 10.2s
```

### 2026 Pricing

| Model             | Input | Output |
| ----------------- | ----- | ------ |
| Claude Haiku 4.5  | $1/M  | $5/M   |
| Claude Sonnet 4.5 | $3/M  | $15/M  |
| Claude Opus 4.5   | $5/M  | $25/M  |

### Optimization Strategies

1. **Clear between tasks**: `/clear` to remove stale context
2. **Use /compact**: `Focus on code samples and API usage`
3. **Choose right model**: Sonnet for most, Opus for complex
4. **Reduce MCP overhead**: `/context` to check, prefer CLI tools
5. **Move to skills**: Keep CLAUDE.md under ~500 lines
6. **Adjust thinking**: `MAX_THINKING_TOKENS=8000` for simple tasks
7. **Delegate to subagents**: Verbose ops return only summaries

### Rate Limits by Team Size

| Team Size | TPM/User  |
| --------- | --------- |
| 1-5       | 200k-300k |
| 20-50     | 50k-75k   |
| 100-500   | 15k-20k   |

### Prompt Caching

- 5-min cache: Write 1.25x, read 0.1x base
- 1-hour cache: Write 2x, read 0.1x base
- Up to 90% savings on repeated content

## Custom Tool Implementation

### Tool Definition

```json
{
  "name": "get_stock_price",
  "description": "Retrieves the current stock price for a ticker symbol. Must be valid symbol on NYSE or NASDAQ. Returns latest trade price in USD.",
  "input_schema": {
    "type": "object",
    "properties": {
      "ticker": {
        "type": "string",
        "description": "Stock ticker symbol, e.g. AAPL"
      }
    },
    "required": ["ticker"]
  }
}
```

### Best Practices

1. **Detailed descriptions** (3-4+ sentences)
2. **Use input_examples** for complex tools
3. **Strict mode** for guaranteed schema compliance

### Tool Runner (Python)

```python
from anthropic import beta_tool

@beta_tool
def get_weather(location: str, unit: str = "fahrenheit") -> str:
    """Get current weather in a location.

    Args:
        location: City and state, e.g. San Francisco, CA
        unit: Temperature unit (celsius/fahrenheit)
    """
    return json.dumps({"temperature": "20°C"})

runner = client.beta.messages.tool_runner(
    model="claude-sonnet-4-5",
    max_tokens=1024,
    tools=[get_weather],
    messages=[{"role": "user", "content": "Weather in Paris?"}]
)
```

### Tool Choice

| Type   | Behavior                 |
| ------ | ------------------------ |
| `auto` | Claude decides (default) |
| `any`  | Must use one tool        |
| `tool` | Must use specific tool   |
| `none` | Cannot use tools         |

### Parallel Tool Use

All results in single user message:

```json
{
  "role": "user",
  "content": [
    { "type": "tool_result", "tool_use_id": "toolu_01", "content": "Result 1" },
    { "type": "tool_result", "tool_use_id": "toolu_02", "content": "Result 2" }
  ]
}
```

### Error Handling

```json
{
  "type": "tool_result",
  "tool_use_id": "toolu_01",
  "content": "ConnectionError: service unavailable",
  "is_error": true
}
```

## Verification Checklist

### Infrastructure

- [ ] CLAUDE.md is under 300 lines
- [ ] All agents have tools field
- [ ] Side-effect commands have disable-model-invocation
- [ ] Skills have user-invocable and $ARGUMENTS
- [ ] hooks.json uses ${CLAUDE_PROJECT_ROOT}
- [ ] All @ imports point to existing files

### Context & Navigation

- [ ] Context rules cover all DSM domains
- [ ] Domain-specific CLAUDE.md in examples/, tests/, src/, docs/
- [ ] Hub-spoke navigation in nested CLAUDE.md files
- [ ] CLAUDE.local.md in .gitignore

### Security

- [ ] .env\* files in deny rules
- [ ] Credentials directories blocked
- [ ] Network exfiltration commands blocked
- [ ] settings.json committed (not settings.local.json)

## Memory Management

### Overview

Claude Code's memory system uses CLAUDE.md files at multiple levels to provide persistent instructions across sessions. Memory is hierarchical, with higher precedence for more specific locations.

### Memory Hierarchy (Precedence Order)

| Priority | Location                                        | Scope              | Controlled By |
| -------- | ----------------------------------------------- | ------------------ | ------------- |
| 1        | `.claude/settings.local.json`                   | Personal overrides | User          |
| 2        | `CLAUDE.local.md`                               | Personal project   | User          |
| 3        | `.claude/rules/*.md`                            | Modular project    | Team          |
| 4        | `CLAUDE.md`                                     | Project-wide       | Team          |
| 5        | `~/.claude/CLAUDE.md`                           | User global        | User          |
| 6        | `/Library/.../ClaudeCode/managed-settings.json` | Enterprise managed | Admin         |
| 7        | `/Library/.../ClaudeCode/CLAUDE.md`             | Enterprise policy  | Admin         |

### Memory Locations

1. **Managed Policy** (Enterprise): `/Library/Application Support/ClaudeCode/CLAUDE.md`
   - Deployed via MDM/Jamf
   - Read-only, cannot be overridden
   - Sets company-wide standards

2. **Project Memory**: `CLAUDE.md` in repository root
   - Checked into version control
   - Shared across team
   - Architecture, patterns, conventions

3. **User Memory**: `~/.claude/CLAUDE.md`
   - Personal preferences
   - Cross-project settings
   - Private API keys, tooling

4. **Local Memory**: `CLAUDE.local.md` (gitignored)
   - Personal project overrides
   - Work-in-progress notes
   - Experimental settings

### Memory Imports (@syntax)

Import external files into CLAUDE.md:

```markdown
## References

@docs/architecture.md
@.claude/rules/api-patterns.md
@../shared-patterns/error-handling.md
```

**Import behavior**:

- Relative paths from CLAUDE.md location
- Supports glob patterns: `@docs/**/*.md`
- Tab completion in Claude Code UI
- Imported content appears in system prompt

### Modular Rules (.claude/rules/)

Split large CLAUDE.md into focused rule files:

```
.claude/rules/
├── typescript.md      # TypeScript conventions
├── testing.md         # Test patterns
├── api-design.md      # API guidelines
└── security.md        # Security requirements
```

**Rule file frontmatter**:

```yaml
---
paths:
  - "src/**/*.ts"
  - "tests/**/*.ts"
description: TypeScript conventions
alwaysApply: false
---
# TypeScript Rules

- Use strict mode
- Prefer interfaces over types
```

**Frontmatter fields**:

| Field         | Type     | Purpose                                       |
| ------------- | -------- | --------------------------------------------- |
| `paths`       | string[] | Glob patterns that trigger this rule          |
| `description` | string   | Summary shown when rule is loaded             |
| `alwaysApply` | boolean  | Load regardless of file path (default: false) |

### Session Persistence

**Continue previous session**:

```bash
claude --continue          # Resume most recent
claude --resume            # Interactive session picker
```

**Session context includes**:

- Full conversation history
- Tool call results
- Generated file changes
- Memory file contents

### Memory Commands

```bash
/memory                    # View current memory files
/memory edit              # Edit project CLAUDE.md
/memory edit --user       # Edit user CLAUDE.md
```

### DSM Memory Structure

```
data-source-manager/
├── CLAUDE.md                    # Project hub (navigation + critical rules)
├── CLAUDE.local.md              # Personal overrides (gitignored)
├── src/CLAUDE.md                # Source-specific context
├── tests/CLAUDE.md              # Test-specific patterns
├── docs/CLAUDE.md               # Documentation guidelines
├── examples/CLAUDE.md           # Example conventions
└── .claude/
    ├── rules/                   # Domain-specific rules
    │   ├── fcp-protocol.md      # FCP decision logic
    │   ├── symbol-formats.md    # Market-specific symbols
    │   └── timestamp-handling.md # UTC requirements
    └── settings.json            # Permission rules
```

### Memory Best Practices

1. **Keep CLAUDE.md concise** (<500 lines for main file)
2. **Use imports for details**: Reference files, don't inline
3. **Leverage rules for domains**: Path-specific loading
4. **gitignore local files**: `CLAUDE.local.md`, `settings.local.json`
5. **Use directory CLAUDE.md**: Context-specific guidance

## Context Compaction

### Overview

Context compaction manages token usage when conversations grow large. Claude Code implements both server-side and client-side compaction strategies.

### Server-Side Context Editing

**Tool Result Clearing**:

- Server removes `tool_result` content from older turns
- Keeps structure and tool names visible
- Reduces tokens while preserving conversation flow

**Thinking Block Clearing**:

- Removes thinking/reasoning blocks from older turns
- Keeps final conclusions and outputs
- Significant token savings for extended thinking

### Client-Side Compaction

**Automatic trigger**: When context reaches ~85% capacity

**Manual trigger**:

```bash
/compact                           # Default summarization
/compact Focus on API patterns     # Guided summarization
```

**Process**:

1. Claude summarizes full conversation history
2. New turn starts with summary as system context
3. Original messages cleared from context
4. Recent tool results preserved

### SDK Compaction Support

```typescript
import { createAgent } from "@anthropic-ai/agent-sdk";

const agent = createAgent({
  model: "claude-sonnet-4-5",
  compaction: {
    type: "summarize",
    threshold: 0.8, // Compact at 80% context
    preserveRecent: 5, // Keep last 5 turns intact
  },
});
```

**Compaction types**:

| Type        | Behavior                             |
| ----------- | ------------------------------------ |
| `summarize` | AI summary of older messages         |
| `truncate`  | Remove oldest messages (no summary)  |
| `sliding`   | Rolling window of recent turns       |
| `smart`     | Hybrid: summarize + keep key content |

### Thresholds and Configuration

**Default thresholds**:

- Warning at 70% context usage
- Auto-compact at 85% context usage
- Hard limit at 95% (forces compaction)

**Environment configuration**:

```bash
export CLAUDE_COMPACT_THRESHOLD=0.8     # Trigger at 80%
export CLAUDE_COMPACT_PRESERVE=3        # Keep 3 recent turns
```

### Memory Tool Integration

After compaction, memory tools ensure continuity:

1. **Task list preserved**: TodoRead/TodoWrite state maintained
2. **File changes tracked**: Git diff shows modifications
3. **Key decisions logged**: Summary includes architectural choices

### DSM Compaction Strategy

```markdown
# In CLAUDE.md or CLAUDE.local.md

## Compaction Guidelines

When compacting DSM sessions, preserve:

- FCP decision context (which data sources, why)
- Symbol format discoveries (exchange-specific patterns)
- Error patterns encountered (rate limits, API quirks)
- Test results and debugging findings

Use: /compact Preserve FCP context, symbol formats, and error patterns
```

### Context Window Management

**Monitor usage**:

```bash
/context                   # Show current token usage
/cost                      # Show cost and usage stats
```

**Reduce context proactively**:

1. **Clear between tasks**: `/clear` for fresh context
2. **Use subagents**: Delegate verbose operations
3. **Summarize files**: Don't read entire files repeatedly
4. **Prune tool results**: Let server-side clearing work

### Compaction Best Practices

1. **Compact with guidance**: Specify what to preserve
2. **Use /clear for new tasks**: Fresh context is cheaper
3. **Monitor /context regularly**: Stay below 70%
4. **Let server clear tool results**: Don't fight it
5. **Design for compaction**: Commit decisions to files/tasks

## Batch Processing API

### Overview

The Message Batches API enables large-scale asynchronous processing at 50% cost savings. Process up to 100,000 requests per batch with most completing within 1 hour.

### When to Use Batch Processing

- Large-scale evaluations (thousands of test cases)
- Content moderation (user-generated content analysis)
- Data analysis (dataset insights/summaries)
- Bulk content generation (product descriptions)

### Batch Limits

| Limit               | Value                       |
| ------------------- | --------------------------- |
| Max requests        | 100,000 per batch           |
| Max size            | 256 MB per batch            |
| Processing time     | Up to 24 hours              |
| Results available   | 29 days after creation      |
| Workspace isolation | Batches scoped to workspace |

### Batch Pricing (50% Discount)

| Model             | Batch Input  | Batch Output  |
| ----------------- | ------------ | ------------- |
| Claude Opus 4.5   | $2.50 / MTok | $12.50 / MTok |
| Claude Sonnet 4.5 | $1.50 / MTok | $7.50 / MTok  |
| Claude Haiku 4.5  | $0.50 / MTok | $2.50 / MTok  |

### Creating a Batch (Python)

```python
import anthropic
from anthropic.types.message_create_params import MessageCreateParamsNonStreaming
from anthropic.types.messages.batch_create_params import Request

client = anthropic.Anthropic()

message_batch = client.messages.batches.create(
    requests=[
        Request(
            custom_id="dsm-eval-001",
            params=MessageCreateParamsNonStreaming(
                model="claude-sonnet-4-5",
                max_tokens=1024,
                messages=[{
                    "role": "user",
                    "content": "Analyze this market data pattern..."
                }]
            )
        ),
        Request(
            custom_id="dsm-eval-002",
            params=MessageCreateParamsNonStreaming(
                model="claude-sonnet-4-5",
                max_tokens=1024,
                messages=[{
                    "role": "user",
                    "content": "Evaluate this FCP decision..."
                }]
            )
        )
    ]
)
print(f"Batch ID: {message_batch.id}")
```

### Polling for Completion

```python
import time

while True:
    batch = client.messages.batches.retrieve(batch_id)
    if batch.processing_status == "ended":
        break
    print(f"Processing: {batch.request_counts.processing} remaining")
    time.sleep(60)
```

### Retrieving Results

```python
for result in client.messages.batches.results(batch_id):
    match result.result.type:
        case "succeeded":
            print(f"✓ {result.custom_id}: {result.result.message.content}")
        case "errored":
            print(f"✗ {result.custom_id}: {result.result.error}")
        case "expired":
            print(f"⏰ {result.custom_id}: Request expired")
        case "canceled":
            print(f"⊘ {result.custom_id}: Canceled")
```

### Result Types

| Type        | Description                      | Billed |
| ----------- | -------------------------------- | ------ |
| `succeeded` | Request completed successfully   | Yes    |
| `errored`   | Validation or server error       | No     |
| `canceled`  | Batch canceled before processing | No     |
| `expired`   | 24-hour limit reached            | No     |

### Prompt Caching with Batches

Combine batch processing with prompt caching for maximum savings:

```python
requests = [
    Request(
        custom_id=f"analysis-{i}",
        params=MessageCreateParamsNonStreaming(
            model="claude-sonnet-4-5",
            max_tokens=1024,
            system=[
                {"type": "text", "text": "You analyze market data patterns."},
                {
                    "type": "text",
                    "text": large_reference_document,  # Shared context
                    "cache_control": {"type": "ephemeral"}
                }
            ],
            messages=[{"role": "user", "content": f"Analyze pattern {i}"}]
        )
    )
    for i in range(1000)
]
```

**Cache hit rates**: 30-98% depending on traffic patterns. Use 1-hour cache duration for batches.

### Batch Best Practices

1. **Test with Messages API first**: Validate request shape before batching
2. **Use meaningful custom_ids**: Match results to requests (order not guaranteed)
3. **Break large datasets**: Multiple smaller batches for manageability
4. **Handle all result types**: Implement retry logic for errors
5. **Monitor request_counts**: Track succeeded/errored/expired ratios

### DSM Batch Use Cases

```python
# Example: Batch evaluation of FCP decisions
requests = []
for symbol in symbols:
    for scenario in scenarios:
        requests.append(Request(
            custom_id=f"{symbol}-{scenario}",
            params=MessageCreateParamsNonStreaming(
                model="claude-sonnet-4-5",
                max_tokens=2048,
                system=[{
                    "type": "text",
                    "text": dsm_fcp_context,  # FCP protocol reference
                    "cache_control": {"type": "ephemeral"}
                }],
                messages=[{
                    "role": "user",
                    "content": f"Evaluate FCP for {symbol} under {scenario}"
                }]
            )
        ))

batch = client.messages.batches.create(requests=requests)
```

## Context Window Management

### Overview

Claude's context window is the "working memory" for conversations. Understanding and managing context is critical for effective AI-assisted development.

### Context Window Sizes

| Model             | Standard    | Extended (Beta) |
| ----------------- | ----------- | --------------- |
| Claude Opus 4.5   | 200K tokens | -               |
| Claude Sonnet 4.5 | 200K tokens | 1M tokens       |
| Claude Sonnet 4   | 200K tokens | 1M tokens       |
| Claude Haiku 4.5  | 200K tokens | -               |

### Token Composition

```
Context Window = Input Tokens + Output Tokens

Input Tokens:
- System prompt (CLAUDE.md, rules, MCP tools)
- Conversation history
- Tool results
- File contents read

Output Tokens:
- Claude's responses
- Extended thinking (if enabled)
- Tool use requests
```

### Extended Thinking Token Management

When using extended thinking, thinking tokens:

- Count toward context window during generation
- Are **automatically stripped** from subsequent turns
- Are billed as output tokens only once

```
Turn 1: Input(10K) + Thinking(50K) + Output(5K) = 65K total
Turn 2: Input(15K) + Thinking(stripped) + Output(3K) = 18K (not 68K)
```

### 1M Context Window (Beta)

Available for Sonnet 4/4.5 in usage tier 4:

```python
from anthropic import Anthropic

client = Anthropic()

response = client.beta.messages.create(
    model="claude-sonnet-4-5",
    max_tokens=4096,
    messages=[{"role": "user", "content": large_codebase_analysis}],
    betas=["context-1m-2025-08-07"]
)
```

**1M Context Pricing**:

- Requests >200K tokens: 2x input, 1.5x output pricing
- Dedicated rate limits apply

### Context Awareness (Claude 4.5)

Claude Sonnet 4.5 and Haiku 4.5 track remaining context:

```
Start: <budget:token_budget>200000</budget:token_budget>
After tools: <system_warning>Token usage: 35000/200000; 165000 remaining</system_warning>
```

Benefits:

- Better long-running session management
- More effective multi-window workflows
- Precise token budget utilization

### Context Optimization Strategies

**1. Monitor Usage**

```bash
/context    # Show current token usage
/cost       # Show cost breakdown
```

**2. Manage MCP Servers**

```
Linear MCP: ~14K tokens (7% of 200K)
GitHub MCP: ~8K tokens
Docs MCP: ~5K tokens

Strategy: Disable unused MCPs during implementation
```

**3. Phase-Based Server Loading**

| Phase          | MCP Servers          |
| -------------- | -------------------- |
| Planning       | Linear, GitHub, Docs |
| Implementation | Code-related only    |
| Review         | Re-enable as needed  |

**4. CLAUDE.md Optimization**

```markdown
# Bad: Bloated CLAUDE.md

@docs/entire-api-reference.md # 50K tokens embedded

# Good: On-demand loading

See: docs/api/ for API reference # Load only when needed
```

**5. Subagent Delegation**

```
Main window: Complex task requires (X + Y + Z) * N tokens
Subagent: Processes (X + Y) * N, returns only Z tokens

Savings: (X + Y) * (N - 1) tokens
```

### DSM Context Strategy

```markdown
# In CLAUDE.md

## Context Guidelines

1. Start sessions with /clear for fresh context
2. Use /context after reading large files
3. Delegate verbose operations to subagents
4. Compact at 70% with: /compact Preserve FCP, symbols, errors
5. Disable unused MCP servers during implementation

## Token-Heavy Operations

| Operation           | Tokens | Strategy              |
| ------------------- | ------ | --------------------- |
| Full codebase scan  | 100K+  | Use Explore subagent  |
| Test suite analysis | 50K+   | Use test-writer agent |
| API reference       | 30K+   | Load on demand        |
```

### Context Window Best Practices

1. **Clear between tasks**: `/clear` for fresh context
2. **Monitor at 70%**: Proactive management prevents issues
3. **Delegate verbose ops**: Subagents return summaries only
4. **Disable unused MCPs**: Reclaim 10-20K tokens
5. **Don't re-read files**: One read per file per session
6. **Use extended thinking wisely**: Toggle off for simple tasks
7. **Leverage context awareness**: Trust Claude to manage its budget

## Custom Subagent Configuration

### Overview

Subagents are specialized AI assistants that handle specific tasks in isolated context windows. Each subagent has its own system prompt, tool access, and permissions.

### Benefits of Subagents

- **Preserve context**: Keep verbose operations out of main conversation
- **Enforce constraints**: Limit tools per task type
- **Control costs**: Route to cheaper models like Haiku
- **Specialize behavior**: Focused prompts for domains

### Built-in Subagents

| Subagent          | Model   | Tools     | Purpose                       |
| ----------------- | ------- | --------- | ----------------------------- |
| Explore           | Haiku   | Read-only | File discovery, code search   |
| Plan              | Inherit | Read-only | Codebase research for plans   |
| general-purpose   | Inherit | All       | Complex multi-step operations |
| Bash              | Inherit | Bash      | Terminal commands             |
| Claude Code Guide | Haiku   | Read-only | Questions about Claude Code   |

### Subagent File Format

```markdown
---
name: code-reviewer
description: Reviews code for quality and best practices
tools: Read, Glob, Grep
model: sonnet
---

You are a code reviewer. Analyze code and provide
specific, actionable feedback on quality and security.
```

### Frontmatter Fields

| Field             | Required | Description                          |
| ----------------- | -------- | ------------------------------------ |
| `name`            | Yes      | Lowercase with hyphens               |
| `description`     | Yes      | When Claude delegates to this agent  |
| `tools`           | No       | Allowlist (inherits all if omitted)  |
| `disallowedTools` | No       | Denylist (removed from inherited)    |
| `model`           | No       | `sonnet`, `opus`, `haiku`, `inherit` |
| `permissionMode`  | No       | Permission handling mode             |
| `skills`          | No       | Skills to preload                    |
| `hooks`           | No       | Lifecycle hooks scoped to agent      |

### Subagent Locations (Priority Order)

| Location            | Scope           | Priority |
| ------------------- | --------------- | -------- |
| `--agents` CLI flag | Current session | 1        |
| `.claude/agents/`   | Current project | 2        |
| `~/.claude/agents/` | All projects    | 3        |
| Plugin's `agents/`  | Plugin scope    | 4        |

### Permission Modes

| Mode                | Behavior                           |
| ------------------- | ---------------------------------- |
| `default`           | Standard permission checking       |
| `acceptEdits`       | Auto-accept file edits             |
| `dontAsk`           | Auto-deny prompts                  |
| `bypassPermissions` | Skip all checks (use with caution) |
| `plan`              | Read-only exploration              |

### CLI-Defined Subagents

```bash
claude --agents '{
  "fcp-analyzer": {
    "description": "Analyzes FCP decisions for DSM data sources",
    "prompt": "You analyze Failover Control Protocol decisions...",
    "tools": ["Read", "Grep", "Glob"],
    "model": "sonnet"
  }
}'
```

### Hooks in Subagent Frontmatter

```yaml
---
name: db-reader
description: Execute read-only database queries
tools: Bash
hooks:
  PreToolUse:
    - matcher: "Bash"
      hooks:
        - type: command
          command: "./scripts/validate-readonly-query.sh"
---
```

### Preload Skills into Subagents

```yaml
---
name: api-developer
description: Implement API endpoints following team conventions
skills:
  - api-conventions
  - error-handling-patterns
---
```

### Foreground vs Background Execution

| Mode       | Behavior                          | Permissions         |
| ---------- | --------------------------------- | ------------------- |
| Foreground | Blocks main conversation          | Interactive prompts |
| Background | Concurrent (Ctrl+B to background) | Pre-approved only   |

### Parallel Subagent Patterns

**Parallel Exploration**:

```
Use 4 Explore subagents in parallel to scan different directories:
- Agent 1: src/core/
- Agent 2: src/adapters/
- Agent 3: src/protocols/
- Agent 4: tests/
```

**Parallel Code Review**:

```
Launch parallel reviewers for:
- Security vulnerabilities
- Performance issues
- API consistency
```

### Context Management

- Subagents have independent 200K context windows
- Results return to main conversation (be mindful of output size)
- Auto-compaction at ~95% capacity
- Resume subagents with agent ID for continued work

### DSM Custom Subagents

```
.claude/agents/
├── api-reviewer.md         # Reviews API consistency
├── data-fetcher.md         # Tests data fetching with FCP
├── fcp-debugger.md         # Diagnoses FCP issues
├── silent-failure-hunter.md # Finds bare excepts
└── test-writer.md          # Writes tests following patterns
```

**Example: FCP Debugger**

```markdown
---
name: fcp-debugger
description: Diagnoses FCP cache misses and failover issues
tools: Read, Grep, Glob, Bash
model: sonnet
---

You are an FCP specialist for data-source-manager.

When invoked:

1. Check cache status with mise run cache:status
2. Review FCP decision logs
3. Identify fetch/cache/persist issues
4. Suggest fixes

FCP Decision Flow:

- FETCH_FRESH: API call required
- USE_CACHE: Valid cache exists
- CACHE_STALE: Update needed
- FAILOVER: Primary failed, try secondary
```

### Subagent Best Practices

1. **Design focused agents**: One task per subagent
2. **Limit tool access**: Grant only necessary permissions
3. **Write detailed descriptions**: Claude uses these for delegation
4. **Check into version control**: Share project subagents with team
5. **Use hooks for validation**: Conditional tool restrictions

## Git Worktree Multi-Agent Development

### Overview

Git worktrees enable running multiple Claude sessions simultaneously on different features, each in complete isolation.

### Why Worktrees

| Problem                | Solution                          |
| ---------------------- | --------------------------------- |
| One branch at a time   | Multiple checkouts simultaneously |
| Claude sessions clash  | Complete isolation per feature    |
| Context pollution      | Fresh context per worktree        |
| Sequential development | Parallel feature development      |

### Worktree vs Clone

| Approach | Storage     | .git Directory | Branches          |
| -------- | ----------- | -------------- | ----------------- |
| Clone    | Full copy   | Duplicated     | Independent repos |
| Worktree | Shared repo | Single, shared | Same repo         |

### Setup Directory Structure

```
~/projects/
├── data-source-manager/        # Main working copy
├── worktrees/
│   └── data-source-manager/
│       ├── feature-fcp-v2/     # Worktree 1
│       ├── fix-rate-limits/    # Worktree 2
│       └── refactor-adapters/  # Worktree 3
```

### Creating Worktrees

```bash
# From main repo directory
cd ~/projects/data-source-manager

# Create worktree for new feature
git worktree add ../worktrees/data-source-manager/feature-fcp-v2 -b feature-fcp-v2

# Create worktree from existing branch
git worktree add ../worktrees/data-source-manager/fix-rate-limits fix-rate-limits

# List worktrees
git worktree list
```

### Launching Parallel Sessions

```bash
# Terminal 1: Main feature
cd ~/projects/worktrees/data-source-manager/feature-fcp-v2
claude

# Terminal 2: Bug fix
cd ~/projects/worktrees/data-source-manager/fix-rate-limits
claude

# Terminal 3: Refactoring
cd ~/projects/worktrees/data-source-manager/refactor-adapters
claude
```

### Automation Script

```bash
#!/bin/bash
# w - Worktree helper function

w() {
    local project=$1
    local branch=$2
    local cmd=${3:-}

    local worktree_dir="$HOME/projects/worktrees/$project/$branch"

    if [ ! -d "$worktree_dir" ]; then
        # Create worktree
        cd "$HOME/projects/$project"
        git worktree add "$worktree_dir" -b "$branch" 2>/dev/null || \
        git worktree add "$worktree_dir" "$branch"
    fi

    if [ "$cmd" = "claude" ]; then
        cd "$worktree_dir" && claude
    elif [ -n "$cmd" ]; then
        cd "$worktree_dir" && $cmd
    else
        cd "$worktree_dir"
    fi
}

# Usage:
# w data-source-manager new-feature          # Create and enter
# w data-source-manager new-feature claude   # Launch Claude
# w data-source-manager new-feature "git status"
```

### Worktree + Claude Code Workflow

1. **Initialize worktree** for feature branch
2. **Launch Claude** in isolated context
3. **Use Plan Mode** for safe exploration
4. **Develop in parallel** (7+ concurrent sessions possible)
5. **Commit within worktree** (Claude can commit/push)
6. **Merge via PR** when feature complete
7. **Cleanup worktree** after merge

### Resource Considerations

| Sessions | Memory Impact | Recommendation       |
| -------- | ------------- | -------------------- |
| 1-3      | Low           | Standard development |
| 4-7      | Medium        | Close unused apps    |
| 8+       | High          | Use ephemeral envs   |

### Cleanup

```bash
# Remove worktree (keeps branch)
git worktree remove ../worktrees/data-source-manager/feature-fcp-v2

# Prune stale worktrees
git worktree prune

# List for cleanup
git worktree list
```

### DSM Worktree Patterns

```bash
# Parallel development scenarios

# Feature: FCP improvements
w data-source-manager feature/fcp-improvements claude

# Feature: New exchange adapter
w data-source-manager feature/kraken-adapter claude

# Bugfix: Rate limit handling
w data-source-manager fix/rate-limit-backoff claude

# Each Claude session has:
# - Isolated file state
# - Independent context window
# - Own commit history
# - No interference with others
```

### Best Practices

1. **Use naming conventions**: `feature/`, `fix/`, `refactor/`
2. **Prune regularly**: Clean up merged branches
3. **Organize by project**: `worktrees/{project}/{branch}`
4. **Use Plan Mode**: Safe exploration before changes
5. **Monitor resources**: Watch memory with many sessions
6. **Commit frequently**: Each worktree can commit independently

## Playwright Testing Integration

### Overview

Playwright MCP enables Claude Code to directly control browser automation, combining AI reasoning with real-time testing capabilities.

### Playwright Agent Types

| Agent     | Purpose                            | Output             |
| --------- | ---------------------------------- | ------------------ |
| Planner   | Explore app, generate test plans   | Markdown test plan |
| Generator | Convert plans to Playwright code   | Executable tests   |
| Healer    | Fix broken tests, update selectors | Patched tests      |

### Agent Workflow

```
1. Planner explores application
   ↓ Markdown test plan
2. Generator writes Playwright tests
   ↓ Executable test code
3. Healer repairs failures
   ↓ Updated, passing tests
```

### Setup

```bash
# Initialize Playwright agents with Claude Code
npx playwright init-agents --loop=claude
```

**Configuration** (`claude_desktop_config.json`):

```json
{
  "mcpServers": {
    "playwright": {
      "command": "npx",
      "args": ["@anthropic/mcp-server-playwright"]
    }
  }
}
```

### Planner Agent

Explores your application like a QA engineer:

```markdown
# Test Plan: Checkout Flow

## Scenarios

1. Guest checkout with valid payment
2. Registered user with saved address
3. Invalid credit card handling
4. Coupon code application
5. Shipping method selection

## Edge Cases

- Empty cart checkout attempt
- Session timeout during payment
- Network failure recovery
```

### Generator Agent

Converts plans to best-practice Playwright code:

```typescript
import { test, expect } from "@playwright/test";

test("guest checkout with valid payment", async ({ page }) => {
  await page.goto("/products");
  await page.getByRole("button", { name: "Add to Cart" }).click();
  await page.getByRole("link", { name: "Checkout" }).click();

  // Fill shipping info
  await page.getByLabel("Email").fill("guest@example.com");
  await page.getByLabel("Address").fill("123 Test St");

  // Complete payment
  await page.getByLabel("Card Number").fill("4242424242424242");
  await expect(page.getByText("Order Confirmed")).toBeVisible();
});
```

### Healer Agent

Automatically repairs broken tests:

```
Test failure: Element not found 'button[name="Add to Cart"]'

Healer analysis:
- Button text changed to "Add to Basket"
- Locator update: getByRole('button', { name: 'Add to Basket' })

Test re-run: PASSED
```

### Framework Auto-Detection

Claude adapts test generation to your stack:

| Framework       | Test Library            | Patterns                   |
| --------------- | ----------------------- | -------------------------- |
| React + Jest    | React Testing Library   | fireEvent, waitFor, screen |
| Vue + Vitest    | Vue Test Utils          | mount, wrapper.find        |
| Angular + Karma | Angular Testing Library | TestBed, ComponentFixture  |
| Playwright      | Cross-browser E2E       | Resilient selectors, waits |

### MCP-Enabled Testing Workflow

1. **Setup MCP Server** - Configure Playwright integration
2. **Receive Requirements** - Define features to test
3. **Plan Strategy** - Break into structured test plans
4. **Generate Code** - AI creates test scripts
5. **Execute via MCP** - Run tests with real-time feedback
6. **Analyze Results** - Review pass/fail outcomes
7. **Iterate** - Healer fixes failures automatically

### DSM Testing Patterns

```typescript
// tests/e2e/fcp-flow.spec.ts
import { test, expect } from "@playwright/test";

test.describe("FCP Data Flow", () => {
  test("fetches fresh data when cache expired", async ({ page }) => {
    // Clear cache
    await page.request.post("/api/cache/clear");

    // Trigger fetch
    const response = await page.request.get("/api/data/BTCUSDT");
    const data = await response.json();

    expect(data.source).toBe("FETCH_FRESH");
    expect(data.bars).toHaveLength(greaterThan(0));
  });

  test("uses cache for repeated requests", async ({ page }) => {
    // First request populates cache
    await page.request.get("/api/data/BTCUSDT");

    // Second request should use cache
    const response = await page.request.get("/api/data/BTCUSDT");
    const data = await response.json();

    expect(data.source).toBe("USE_CACHE");
  });
});
```

### Best Practices

1. **Use Planner for exploration**: Let AI discover edge cases
2. **Review generated code**: AI is co-pilot, not autopilot
3. **Enable Healer for maintenance**: Reduce test brittleness
4. **Use Page Object Models**: Structure for maintainability
5. **Run in CI with MCP**: Automated test execution

## Error Recovery and Resilience Patterns

### Overview

Resilience patterns prevent cascading failures and ensure graceful degradation when services fail. These patterns are essential for AI-assisted development with external APIs.

### Circuit Breaker Pattern

Prevents overwhelming failing services by tracking failures and opening the circuit when thresholds are exceeded.

**Three States**:

```
CLOSED (normal)     OPEN (failing)      HALF-OPEN (testing)
     │                   │                    │
     │ failure_count++   │ reject requests    │ allow 1 probe
     │                   │ return fallback    │
     ▼                   ▼                    ▼
[threshold?]──YES──▶[timeout?]──YES──▶[probe success?]
     │                   │                    │
     NO                  NO                   YES: CLOSED
     │                   │                    NO: OPEN
     ▼                   ▼
   continue           stay open
```

### Circuit Breaker Implementation

```python
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from typing import Callable, TypeVar

T = TypeVar('T')

class CircuitState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"

@dataclass
class CircuitBreaker:
    failure_threshold: int = 5
    recovery_timeout: timedelta = timedelta(seconds=30)
    half_open_max_calls: int = 3

    state: CircuitState = CircuitState.CLOSED
    failure_count: int = 0
    last_failure_time: datetime | None = None
    half_open_calls: int = 0

    def call(self, func: Callable[[], T], fallback: Callable[[], T]) -> T:
        if self.state == CircuitState.OPEN:
            if self._should_attempt_reset():
                self.state = CircuitState.HALF_OPEN
                self.half_open_calls = 0
            else:
                return fallback()

        try:
            result = func()
            self._on_success()
            return result
        except Exception as e:
            self._on_failure()
            return fallback()

    def _on_success(self):
        if self.state == CircuitState.HALF_OPEN:
            self.half_open_calls += 1
            if self.half_open_calls >= self.half_open_max_calls:
                self.state = CircuitState.CLOSED
                self.failure_count = 0

    def _on_failure(self):
        self.failure_count += 1
        self.last_failure_time = datetime.now()
        if self.failure_count >= self.failure_threshold:
            self.state = CircuitState.OPEN

    def _should_attempt_reset(self) -> bool:
        if self.last_failure_time is None:
            return True
        return datetime.now() - self.last_failure_time >= self.recovery_timeout
```

### Retry Pattern with Exponential Backoff

```python
import random
import time
from typing import Callable, TypeVar

T = TypeVar('T')

def retry_with_backoff(
    func: Callable[[], T],
    max_retries: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    jitter: float = 0.25,
) -> T:
    """
    Retry with exponential backoff and jitter.

    Formula: min(base * 2^attempt, max_delay) * (1 ± jitter)
    """
    for attempt in range(max_retries + 1):
        try:
            return func()
        except Exception as e:
            if attempt == max_retries:
                raise

            # Calculate delay with exponential backoff
            delay = min(base_delay * (2 ** attempt), max_delay)

            # Add jitter to prevent thundering herd
            jitter_range = delay * jitter
            delay += random.uniform(-jitter_range, jitter_range)

            time.sleep(delay)
```

### Combined Strategy

```python
class ResilientClient:
    def __init__(self):
        self.circuit_breaker = CircuitBreaker(
            failure_threshold=5,
            recovery_timeout=timedelta(seconds=30)
        )

    def fetch_data(self, symbol: str) -> DataFrame:
        def do_fetch():
            return retry_with_backoff(
                lambda: self._api_call(symbol),
                max_retries=3,
                base_delay=1.0
            )

        def fallback():
            # Return cached data or empty result
            return self._get_cached(symbol) or DataFrame()

        return self.circuit_breaker.call(do_fetch, fallback)
```

### Key Configuration Parameters

| Parameter         | Typical Value | Purpose                 |
| ----------------- | ------------- | ----------------------- |
| failure_threshold | 5             | Failures before opening |
| recovery_timeout  | 30s           | Wait before half-open   |
| max_retries       | 3-5           | Retry attempts          |
| base_delay        | 1s            | Initial backoff         |
| max_delay         | 60s           | Maximum backoff         |
| jitter            | 0.25 (25%)    | Randomization range     |

### Graceful Degradation Strategies

| Strategy           | When to Use                 | Example                  |
| ------------------ | --------------------------- | ------------------------ |
| Cached fallback    | Recent data acceptable      | Return stale market data |
| Default response   | Partial data acceptable     | Return empty DataFrame   |
| Queue for retry    | Eventual consistency OK     | Queue failed writes      |
| Alternative source | Redundant sources available | Try secondary exchange   |

### DSM Resilience Patterns

```python
# In src/core/resilient_fetcher.py

class ResilientDataFetcher:
    """FCP-aware data fetcher with circuit breaker and retry."""

    def __init__(self, primary: DataSource, fallback: DataSource):
        self.primary = primary
        self.fallback = fallback
        self.circuit = CircuitBreaker(
            failure_threshold=3,
            recovery_timeout=timedelta(seconds=60)
        )

    def get_data(self, symbol: str, start: datetime, end: datetime) -> DataFrame:
        def fetch_primary():
            return retry_with_backoff(
                lambda: self.primary.fetch(symbol, start, end),
                max_retries=2,
                base_delay=0.5
            )

        def fetch_fallback():
            # FCP: FAILOVER decision
            logger.warning(f"FAILOVER: {symbol} to secondary source")
            return self.fallback.fetch(symbol, start, end)

        return self.circuit.call(fetch_primary, fetch_fallback)
```

### Error Recovery Triggers

| Trigger                   | Recovery Action             |
| ------------------------- | --------------------------- |
| Task execution failure    | Retry with backoff          |
| Timeout                   | Open circuit, use fallback  |
| External service failure  | Circuit breaker + fallback  |
| Database transaction fail | Rollback + compensating txn |
| Cascade failure risk      | Open circuit immediately    |

### Best Practices

1. **Always use idempotency**: Only retry safe operations
2. **Add jitter**: Prevent thundering herd on recovery
3. **Set timeouts**: All external calls need timeouts
4. **Log state transitions**: Debug circuit breaker behavior
5. **Monitor metrics**: Track failure rates and circuit state
6. **Test failure modes**: Simulate failures in tests
7. **Configure per-service**: Different services need different thresholds

## Hooks Lifecycle Reference

### Overview

Hooks are shell commands that execute at specific lifecycle events in Claude Code. They enable automation, validation, and context injection.

### Hook Events

| Hook                 | When It Fires                   | Matcher    |
| -------------------- | ------------------------------- | ---------- |
| `SessionStart`       | Session begins or resumes       | source     |
| `UserPromptSubmit`   | User submits a prompt           | No         |
| `PreToolUse`         | Before tool execution           | Tool name  |
| `PermissionRequest`  | When permission dialog appears  | Tool name  |
| `PostToolUse`        | After tool succeeds             | Tool name  |
| `PostToolUseFailure` | After tool fails                | Tool name  |
| `SubagentStart`      | When spawning a subagent        | Agent name |
| `SubagentStop`       | When subagent finishes          | Agent name |
| `Stop`               | Claude finishes responding      | No         |
| `PreCompact`         | Before context compaction       | trigger    |
| `Setup`              | With --init/--maintenance flags | trigger    |
| `SessionEnd`         | Session terminates              | reason     |
| `Notification`       | Claude sends notifications      | type       |

### Configuration Format

```json
{
  "hooks": {
    "PreToolUse": [
      {
        "matcher": "Bash",
        "hooks": [
          {
            "type": "command",
            "command": "$CLAUDE_PROJECT_DIR/.claude/hooks/validate-bash.sh",
            "timeout": 30
          }
        ]
      }
    ]
  }
}
```

### Matcher Syntax

| Matcher         | Matches                |
| --------------- | ---------------------- |
| `"Write"`       | Exactly the Write tool |
| `"Edit\|Write"` | Edit OR Write (regex)  |
| `"Notebook.*"`  | All Notebook tools     |
| `"*"` or `""`   | All tools              |
| `"mcp__.*"`     | All MCP tools          |

### Exit Codes

| Exit Code | Behavior                                 |
| --------- | ---------------------------------------- |
| 0         | Success, stdout shown in verbose mode    |
| 2         | Blocking error, stderr shown to Claude   |
| Other     | Non-blocking error, stderr shown to user |

### Exit Code 2 Behavior by Event

| Event               | Exit Code 2 Effect                  |
| ------------------- | ----------------------------------- |
| `PreToolUse`        | Blocks tool call, stderr to Claude  |
| `PermissionRequest` | Denies permission, stderr to Claude |
| `PostToolUse`       | Shows stderr to Claude (tool ran)   |
| `UserPromptSubmit`  | Blocks prompt, erases it            |
| `Stop`              | Blocks stoppage, stderr to Claude   |
| `SubagentStop`      | Blocks stoppage, stderr to subagent |

### Hook Input JSON

```json
{
  "session_id": "abc123",
  "transcript_path": "~/.claude/projects/.../session.jsonl",
  "cwd": "/Users/...",
  "permission_mode": "default",
  "hook_event_name": "PreToolUse",
  "tool_name": "Bash",
  "tool_input": {
    "command": "npm run test",
    "description": "Run tests"
  },
  "tool_use_id": "toolu_01ABC123..."
}
```

### JSON Output (Advanced)

```json
{
  "hookSpecificOutput": {
    "hookEventName": "PreToolUse",
    "permissionDecision": "allow",
    "permissionDecisionReason": "Auto-approved npm command",
    "updatedInput": {
      "command": "npm run test --coverage"
    },
    "additionalContext": "Running in CI environment"
  }
}
```

### Permission Decisions

| Decision  | Effect                                  |
| --------- | --------------------------------------- |
| `"allow"` | Bypass permission, auto-approve         |
| `"deny"`  | Block tool call, reason shown to Claude |
| `"ask"`   | Show permission dialog to user          |

### Environment Variables

| Variable             | Description                    |
| -------------------- | ------------------------------ |
| `CLAUDE_PROJECT_DIR` | Project root directory         |
| `CLAUDE_PLUGIN_ROOT` | Plugin directory (for plugins) |
| `CLAUDE_ENV_FILE`    | File for persisting env vars   |
| `CLAUDE_CODE_REMOTE` | "true" in web, empty in CLI    |

### SessionStart with Environment

```bash
#!/bin/bash
# Persist environment variables for the session

if [ -n "$CLAUDE_ENV_FILE" ]; then
  echo 'export NODE_ENV=production' >> "$CLAUDE_ENV_FILE"
  echo 'export DEBUG=dsm:*' >> "$CLAUDE_ENV_FILE"
fi

# Add context for Claude
echo "Session started at $(date)"
exit 0
```

### Prompt-Based Hooks

Use LLM evaluation instead of bash:

```json
{
  "hooks": {
    "Stop": [
      {
        "hooks": [
          {
            "type": "prompt",
            "prompt": "Evaluate if Claude should stop. Context: $ARGUMENTS. Check if all tasks are complete. Return {\"ok\": true} or {\"ok\": false, \"reason\": \"...\"}",
            "timeout": 30
          }
        ]
      }
    ]
  }
}
```

### DSM Hook Examples

**PreToolUse: Block dangerous commands**

```json
{
  "hooks": {
    "PreToolUse": [
      {
        "matcher": "Bash",
        "hooks": [
          {
            "type": "command",
            "command": "$CLAUDE_PROJECT_DIR/.claude/hooks/dsm-bash-guard.sh"
          }
        ]
      }
    ]
  }
}
```

**PostToolUse: Detect silent failures**

```json
{
  "hooks": {
    "PostToolUse": [
      {
        "matcher": "Write|Edit",
        "hooks": [
          {
            "type": "command",
            "command": "$CLAUDE_PROJECT_DIR/.claude/hooks/dsm-code-guard.sh"
          }
        ]
      }
    ]
  }
}
```

**SessionStart: Load FCP context**

```json
{
  "hooks": {
    "SessionStart": [
      {
        "hooks": [
          {
            "type": "command",
            "command": "$CLAUDE_PROJECT_DIR/.claude/hooks/dsm-session-start.sh"
          }
        ]
      }
    ]
  }
}
```

## Settings Configuration Reference

### Settings File Hierarchy

| Scope   | Location                           | Audience         | Shared |
| ------- | ---------------------------------- | ---------------- | ------ |
| Managed | `/Library/.../ClaudeCode/` (macOS) | All users        | IT     |
| User    | `~/.claude/settings.json`          | Personal         | No     |
| Project | `.claude/settings.json`            | Team             | Git    |
| Local   | `.claude/settings.local.json`      | Personal/project | No     |

### Precedence Order (Highest First)

1. Managed settings
2. Command-line arguments
3. Local project settings
4. Shared project settings
5. User settings

### Permission Rules

```json
{
  "permissions": {
    "allow": ["Bash(uv run *)", "Bash(mise run *)", "Bash(git commit *)"],
    "ask": ["Bash(git push *)"],
    "deny": [
      "Bash(pip install *)",
      "Bash(sudo *)",
      "Read(.env*)",
      "Read(**/.env)"
    ]
  }
}
```

### Rule Evaluation Order

1. **Deny** rules checked first (always take precedence)
2. **Ask** rules checked second
3. **Allow** rules checked last

### Bash Wildcards

| Pattern             | Matches                           |
| ------------------- | --------------------------------- |
| `Bash(npm run *)`   | npm run test, npm run build       |
| `Bash(git * main)`  | git merge main, git rebase main   |
| `Bash(* --version)` | ls --version, git --version       |
| `Bash(ls *)`        | ls -la (NOT lsof - space matters) |
| `Bash(ls*)`         | ls, lsof, ls-files                |

### Read/Write Patterns

```json
{
  "permissions": {
    "deny": [
      "Read(./.env)",
      "Read(./.env.*)",
      "Read(./secrets/**)",
      "Read(**/*.key)",
      "Read(**/node_modules/**)"
    ]
  }
}
```

### Complete Settings Example

```json
{
  "permissions": {
    "allow": [
      "Bash(uv run *)",
      "Bash(mise run *)",
      "Bash(git commit *)",
      "Bash(git add *)"
    ],
    "ask": ["Bash(git push *)"],
    "deny": [
      "Bash(pip install *)",
      "Bash(python3.14 *)",
      "Bash(rm -rf *)",
      "Read(.env*)",
      "Read(**/.secrets/**)"
    ],
    "additionalDirectories": ["../shared-libs/"],
    "defaultMode": "acceptEdits"
  },
  "env": {
    "PYTHONDONTWRITEBYTECODE": "1",
    "UV_PYTHON": "3.13"
  },
  "hooks": {
    "PreToolUse": [
      {
        "matcher": "Bash",
        "hooks": [
          {
            "type": "command",
            "command": "$CLAUDE_PROJECT_DIR/.claude/hooks/dsm-bash-guard.sh"
          }
        ]
      }
    ],
    "PostToolUse": [
      {
        "matcher": "Write|Edit",
        "hooks": [
          {
            "type": "command",
            "command": "$CLAUDE_PROJECT_DIR/.claude/hooks/dsm-code-guard.sh"
          }
        ]
      }
    ]
  },
  "model": "claude-sonnet-4-5",
  "cleanupPeriodDays": 30,
  "respectGitignore": true
}
```

### Key Settings

| Setting                        | Purpose                              |
| ------------------------------ | ------------------------------------ |
| `permissions`                  | Allow/deny/ask rules                 |
| `env`                          | Environment variables                |
| `hooks`                        | Lifecycle hooks                      |
| `model`                        | Default model override               |
| `cleanupPeriodDays`            | Session cleanup period               |
| `additionalDirectories`        | Extra allowed directories            |
| `respectGitignore`             | Honor .gitignore in file picker      |
| `disableBypassPermissionsMode` | Block --dangerously-skip-permissions |

### Environment Variables

| Variable                          | Purpose                  |
| --------------------------------- | ------------------------ |
| `ANTHROPIC_API_KEY`               | API key                  |
| `ANTHROPIC_MODEL`                 | Override default model   |
| `CLAUDE_CODE_USE_BEDROCK`         | Use AWS Bedrock          |
| `CLAUDE_CODE_USE_VERTEX`          | Use Google Vertex AI     |
| `MAX_THINKING_TOKENS`             | Extended thinking budget |
| `BASH_DEFAULT_TIMEOUT_MS`         | Default bash timeout     |
| `CLAUDE_AUTOCOMPACT_PCT_OVERRIDE` | Auto-compact threshold   |
| `DISABLE_PROMPT_CACHING`          | Disable caching          |

### MCP Server Configuration

**User-level** (`~/.claude.json`):

```json
{
  "mcp_servers": {
    "memory": {
      "command": "npx",
      "args": ["mcp-server-memory"]
    }
  }
}
```

**Project-level** (`.mcp.json`):

```json
{
  "mcpServers": {
    "github": {
      "command": "npx",
      "args": ["@modelcontextprotocol/server-github"],
      "env": { "GITHUB_TOKEN": "..." }
    }
  }
}
```

### DSM Settings Configuration

```json
{
  "permissions": {
    "allow": [
      "Bash(uv run *)",
      "Bash(mise run *)",
      "Bash(pytest *)",
      "Bash(git commit *)",
      "Bash(git add *)"
    ],
    "ask": ["Bash(git push *)"],
    "deny": [
      "Bash(pip install *)",
      "Bash(python3.14 *)",
      "Bash(python3.12 *)",
      "Bash(rm -rf cache/)",
      "Read(.env*)",
      "Read(.mise.local.toml)"
    ]
  },
  "hooks": {
    "SessionStart": [
      {
        "hooks": [
          {
            "type": "command",
            "command": "$CLAUDE_PROJECT_DIR/.claude/hooks/dsm-session-start.sh"
          }
        ]
      }
    ],
    "PreToolUse": [
      {
        "matcher": "Bash",
        "hooks": [
          {
            "type": "command",
            "command": "$CLAUDE_PROJECT_DIR/.claude/hooks/dsm-bash-guard.sh"
          }
        ]
      }
    ],
    "PostToolUse": [
      {
        "matcher": "Write|Edit",
        "hooks": [
          {
            "type": "command",
            "command": "$CLAUDE_PROJECT_DIR/.claude/hooks/dsm-code-guard.sh"
          }
        ]
      }
    ],
    "Stop": [
      {
        "hooks": [
          {
            "type": "command",
            "command": "$CLAUDE_PROJECT_DIR/.claude/hooks/dsm-final-check.sh"
          }
        ]
      }
    ]
  }
}
```

## Skills Reference

### Overview

Skills extend Claude's capabilities with structured instructions. Create a `SKILL.md` file and Claude adds it to its toolkit, using it when relevant or when you invoke it with `/skill-name`.

### Skill Locations

| Location   | Path                               | Scope             |
| ---------- | ---------------------------------- | ----------------- |
| Enterprise | Managed settings                   | All org users     |
| Personal   | `~/.claude/skills/<name>/SKILL.md` | All your projects |
| Project    | `.claude/skills/<name>/SKILL.md`   | This project only |
| Plugin     | `<plugin>/skills/<name>/SKILL.md`  | Plugin scope      |

### SKILL.md Structure

```yaml
---
name: explain-code
description: Explains code with visual diagrams and analogies
---
When explaining code, always include:

1. **Start with an analogy**: Compare to everyday life
2. **Draw a diagram**: Use ASCII art for flow/structure
3. **Walk through the code**: Explain step-by-step
4. **Highlight a gotcha**: Common mistake or misconception
```

### Frontmatter Fields

| Field                      | Required    | Description                           |
| -------------------------- | ----------- | ------------------------------------- |
| `name`                     | No          | Display name (defaults to directory)  |
| `description`              | Recommended | When to use the skill                 |
| `argument-hint`            | No          | Hint for autocomplete                 |
| `disable-model-invocation` | No          | Only user can invoke (default: false) |
| `user-invocable`           | No          | Show in / menu (default: true)        |
| `allowed-tools`            | No          | Tools without permission prompts      |
| `model`                    | No          | Model to use when skill is active     |
| `context`                  | No          | Set to `fork` for subagent context    |
| `agent`                    | No          | Subagent type when context: fork      |
| `hooks`                    | No          | Hooks scoped to skill lifecycle       |

### String Substitutions

| Variable               | Description                        |
| ---------------------- | ---------------------------------- |
| `$ARGUMENTS`           | All arguments passed               |
| `$ARGUMENTS[N]`        | Specific argument by 0-based index |
| `$N`                   | Shorthand for `$ARGUMENTS[N]`      |
| `${CLAUDE_SESSION_ID}` | Current session ID                 |

### Invocation Control

| Frontmatter                      | You Invoke | Claude Invokes |
| -------------------------------- | ---------- | -------------- |
| (default)                        | Yes        | Yes            |
| `disable-model-invocation: true` | Yes        | No             |
| `user-invocable: false`          | No         | Yes            |

### Context Fork for Subagents

```yaml
---
name: deep-research
description: Research a topic thoroughly
context: fork
agent: Explore
---

Research $ARGUMENTS thoroughly:

1. Find relevant files using Glob and Grep
2. Read and analyze the code
3. Summarize findings with file references
```

**Execution**:

1. New isolated context created
2. Subagent receives skill content as prompt
3. Agent type determines tools/permissions
4. Results summarized and returned

### Dynamic Context Injection

Use `!`command\`\` for shell command preprocessing:

```yaml
---
name: pr-summary
description: Summarize changes in a pull request
context: fork
agent: Explore
---

## Pull request context
- PR diff: !`gh pr diff`
- PR comments: !`gh pr view --comments`
- Changed files: !`gh pr diff --name-only`

## Your task
Summarize this pull request...
```

### Supporting Files

```
my-skill/
├── SKILL.md           # Main instructions (required)
├── template.md        # Template for Claude
├── examples/
│   └── sample.md      # Example output
└── scripts/
    └── validate.sh    # Script Claude can execute
```

Reference from SKILL.md:

```markdown
## Additional resources

- For complete API details, see [reference.md](reference.md)
- For usage examples, see [examples.md](examples.md)
```

### DSM Skills

```
docs/skills/
├── dsm-usage/
│   ├── SKILL.md            # DataSourceManager API usage
│   ├── examples/           # Code examples
│   └── references/         # API reference
├── dsm-testing/
│   ├── SKILL.md            # Testing patterns
│   └── examples/           # Test examples
├── dsm-research/
│   └── SKILL.md            # Codebase research (context: fork)
└── dsm-fcp-monitor/
    └── SKILL.md            # FCP monitoring
```

**Example: DSM Research Skill**

```yaml
---
name: dsm-research
description: Research DSM codebase for patterns and implementations
context: fork
agent: Explore
user-invocable: true
---

Research the data-source-manager codebase for $ARGUMENTS:

1. Search for relevant files using Glob
2. Grep for implementation patterns
3. Read key files and analyze
4. Summarize findings with file:line references

Focus on:
- FCP decision logic
- Data source adapters
- Caching mechanisms
- Error handling patterns
```

### Skill Best Practices

1. **Keep SKILL.md under 500 lines**: Move details to supporting files
2. **Write clear descriptions**: Claude uses these for triggering
3. **Use context: fork for exploration**: Keep noise out of main context
4. **Add argument-hint**: Help users with autocomplete
5. **Test with both invocation methods**: `/skill-name` and natural language

## Plugin Marketplace Reference

### Overview

Plugin marketplaces distribute Claude Code extensions across teams. They provide centralized discovery, version tracking, and automatic updates.

### Marketplace Structure

```
my-marketplace/
├── .claude-plugin/
│   └── marketplace.json      # Marketplace catalog
├── plugins/
│   └── example-plugin/
│       ├── .claude-plugin/
│       │   └── plugin.json   # Plugin manifest
│       ├── skills/           # Skills
│       ├── commands/         # Slash commands
│       ├── agents/           # Specialized agents
│       └── hooks/            # Event hooks
```

### marketplace.json Schema

<!-- SSoT-OK: Example versions in Claude Code documentation -->

```json
{
  "name": "company-tools",
  "owner": {
    "name": "DevTools Team",
    "email": "devtools@example.com"
  },
  "metadata": {
    "description": "Company development tools",
    "version": "<version>",
    "pluginRoot": "./plugins"
  },
  "plugins": [
    {
      "name": "code-formatter",
      "source": "./plugins/formatter",
      "description": "Automatic code formatting",
      "version": "<version>"
    }
  ]
}
```

### Required Marketplace Fields

| Field     | Type   | Description                   |
| --------- | ------ | ----------------------------- |
| `name`    | string | Marketplace identifier        |
| `owner`   | object | Maintainer info (name, email) |
| `plugins` | array  | List of available plugins     |

### Plugin Entry Fields

| Field         | Type    | Description                         |
| ------------- | ------- | ----------------------------------- |
| `name`        | string  | Plugin identifier (required)        |
| `source`      | string  | Where to fetch (required)           |
| `description` | string  | Brief description                   |
| `version`     | string  | Plugin version                      |
| `author`      | object  | Author info                         |
| `homepage`    | string  | Documentation URL                   |
| `license`     | string  | SPDX identifier                     |
| `keywords`    | array   | Discovery tags                      |
| `strict`      | boolean | Require plugin.json (default: true) |

### Plugin Sources

**Relative paths** (same repo):

```json
{ "source": "./plugins/my-plugin" }
```

**GitHub**:

```json
{
  "source": {
    "source": "github",
    "repo": "owner/plugin-repo",
    "ref": "v2"
  }
}
```

**Git URL**:

```json
{
  "source": {
    "source": "url",
    "url": "https://gitlab.com/team/plugin.git"
  }
}
```

### Plugin Manifest (plugin.json)

<!-- SSoT-OK: Example version in Claude Code documentation -->

```json
{
  "name": "my-plugin",
  "description": "Plugin description",
  "version": "<version>",
  "author": { "name": "Author Name" },
  "commands": ["./commands/"],
  "agents": ["./agents/"],
  "hooks": "./hooks/hooks.json"
}
```

### Distribution Methods

**GitHub (recommended)**:

```bash
/plugin marketplace add owner/repo
```

**Other git hosts**:

```bash
/plugin marketplace add https://gitlab.com/company/plugins.git
```

**Local testing**:

```bash
/plugin marketplace add ./my-local-marketplace
```

### Installation Commands

```bash
# Add marketplace
/plugin marketplace add company/tools

# Install plugin
/plugin install code-formatter@company-tools

# Update marketplace
/plugin marketplace update

# Validate structure
/plugin validate .
```

### Project-Level Plugin Configuration

In `.claude/settings.json`:

```json
{
  "extraKnownMarketplaces": {
    "company-tools": {
      "source": {
        "source": "github",
        "repo": "your-org/claude-plugins"
      }
    }
  },
  "enabledPlugins": {
    "code-formatter@company-tools": true,
    "deployment-tools@company-tools": true
  }
}
```

### Enterprise Restrictions

In managed settings:

```json
{
  "strictKnownMarketplaces": [
    {
      "source": "github",
      "repo": "acme-corp/approved-plugins"
    }
  ]
}
```

### DSM Plugin Marketplace

<!-- SSoT-OK: Example version in Claude Code documentation -->

```json
{
  "name": "dsm-tools",
  "owner": {
    "name": "DSM Team"
  },
  "plugins": [
    {
      "name": "dsm-skills",
      "source": "./plugins/dsm-skills",
      "description": "DSM usage, testing, and research skills",
      "version": "<version>"
    },
    {
      "name": "dsm-agents",
      "source": "./plugins/dsm-agents",
      "description": "DSM specialized agents (FCP debugger, etc.)",
      "version": "<version>"
    }
  ]
}
```

### Best Practices

1. **Use kebab-case names**: Consistent identifiers
2. **Version your plugins**: Track changes with semver
3. **Include descriptions**: Help users discover plugins
4. **Test locally first**: Validate before distribution
5. **Use GitHub for public**: Built-in version control
6. **Pin versions in enterprise**: Stability over freshness

---

## Headless Mode and SDK

Claude Code supports headless operation for CI/CD pipelines, automation scripts, and programmatic usage without interactive terminals.

### CLI Flags for Headless Mode

```bash
# Basic headless execution with prompt
claude -p "Explain this code" --print

# Pipe input to Claude
cat file.py | claude -p "Review this code"

# Print-only mode (no interactive session)
claude --print "What does this function do?"
```

### Output Format Options

```bash
# Plain text output (default)
claude -p "Generate a function" --output-format text

# JSON output for parsing
claude -p "List all functions" --output-format json

# Streaming JSON for real-time processing
claude -p "Refactor this code" --output-format stream-json
```

### JSON Output Schema

```json
{
  "type": "result",
  "subtype": "success",
  "cost_usd": 0.003,
  "is_error": false,
  "duration_ms": 1234,
  "duration_api_ms": 1100,
  "num_turns": 1,
  "result": "Generated code here...",
  "session_id": "abc123"
}
```

### Streaming JSON Processing

```bash
# Process streaming output with jq
claude -p "Generate tests" --output-format stream-json | \
  jq -c 'select(.type == "assistant") | .message.content[0].text'

# Extract final result
claude -p "Analyze code" --output-format stream-json | \
  jq -s '.[-1] | select(.type == "result")'
```

### Auto-Approve Tools in Headless

```bash
# Approve all tools (use with caution)
claude -p "Run tests" --dangerously-skip-permissions

# Approve specific tool patterns
claude -p "Format code" --allowedTools "Bash(prettier *)" --allowedTools "Edit(*)"
```

### Session Continuation

```bash
# Continue last session
claude --continue

# Resume specific session
claude --resume abc123

# Continue with new prompt
claude --continue -p "Now add error handling"
```

### CI/CD Integration

```yaml
# GitHub Actions example
- name: Code Review with Claude
  env:
    ANTHROPIC_API_KEY: ${{ secrets.ANTHROPIC_API_KEY }}
  run: |
    git diff HEAD~1 | claude -p "Review these changes" --print
```

### Authentication in CI/CD

```bash
# Environment variable (recommended)
export ANTHROPIC_API_KEY=sk-ant-...

# Or via config file
echo '{"apiKey": "sk-ant-..."}' > ~/.claude/config.json
```

### Structured Output with JSON Schema

```bash
# Enforce output schema
claude -p "Extract function signatures" \
  --output-format json \
  --json-schema '{"type": "array", "items": {"type": "object", "properties": {"name": {"type": "string"}, "params": {"type": "array"}}}}'
```

### DSM Headless Patterns

```bash
# Validate FCP behavior
claude -p "Check FCP for BTCUSDT" \
  --allowedTools "Bash(uv run *)" \
  --output-format json

# Generate tests headlessly
claude -p "Write tests for src/data_source_manager/core/manager.py" \
  --allowedTools "Read(*)" --allowedTools "Write(tests/*)" \
  --print

# CI pipeline validation
cat src/data_source_manager/core/fcp.py | \
  claude -p "Check for silent failure patterns" \
  --output-format json | \
  jq '.result | test("bare except|except Exception") | not'
```

---

## CLAUDE.md Best Practices

Effective CLAUDE.md files optimize context usage while providing essential project guidance.

### Core Structure

```markdown
# Project Name

Brief description (1-2 sentences).

## Quick Start

Essential commands to get started.

## Architecture

Key patterns and decisions.

## Conventions

Code style, naming, testing patterns.

## Common Tasks

Frequent workflows with examples.
```

### Token Optimization Principles

1. **Keep CLAUDE.md under 500 lines**: Large files waste context on every message
2. **Use progressive disclosure**: Link to detailed docs instead of embedding
3. **Prefer tables over prose**: More scannable, fewer tokens
4. **Avoid redundancy**: Don't repeat what's in code comments or README
5. **Update regularly**: Remove outdated information

### What to Include

| Include                   | Exclude                   |
| ------------------------- | ------------------------- |
| Project-specific patterns | Generic language patterns |
| Critical invariants       | Obvious conventions       |
| Common gotchas            | Full API documentation    |
| Essential commands        | Exhaustive command lists  |
| Architecture decisions    | Implementation details    |
| Testing requirements      | Test case specifics       |

### Hub-Spoke Pattern

```markdown
# Root CLAUDE.md

**Navigation**: [src/](src/CLAUDE.md) | [tests/](tests/CLAUDE.md) | [docs/](docs/CLAUDE.md)

## Project Overview

...

## Key Patterns

...
```

Each subdirectory has focused CLAUDE.md:

```markdown
# src/CLAUDE.md

**Hub**: [Root CLAUDE.md](../CLAUDE.md) | **Siblings**: [tests/](../tests/CLAUDE.md)

## Directory-Specific Context

...
```

### Using @ Imports

```markdown
## References

For detailed FCP documentation:
@docs/skills/dsm-fcp-monitor/references/fcp-protocol.md

For testing patterns:
@docs/skills/dsm-testing/examples/
```

### Permission Configuration

```json
{
  "permissions": {
    "allow": [
      "Bash(uv run *)",
      "Bash(mise run *)",
      "Read(src/**)",
      "Edit(src/**)"
    ],
    "deny": ["Read(.env*)", "Bash(rm -rf *)", "Bash(git push --force *)"]
  }
}
```

### Verification Criteria Pattern

```markdown
## Testing Requirements

Before submitting changes:

1. `uv run pytest tests/unit/` passes
2. `uv run pytest tests/integration/` passes
3. `uv run ruff check src/` clean
4. `uv run mypy src/` no errors
```

### Explore-Plan-Code Workflow

```markdown
## Development Workflow

1. **Explore**: Understand existing code before changes
2. **Plan**: Outline approach, identify affected files
3. **Code**: Implement with tests
4. **Verify**: Run all checks before committing
```

### Context Efficiency Tips

1. **Use glob patterns**: `src/**/*.py` instead of listing files
2. **Reference by path**: "See `src/core/manager.py:45`" instead of copying code
3. **Link don't embed**: "@docs/architecture.md" instead of copying content
4. **Keep examples minimal**: Show pattern, not exhaustive cases

### DSM CLAUDE.md Hierarchy

```
CLAUDE.md                 # Root: Overview, commands, patterns
├── src/CLAUDE.md         # Source: Architecture, modules, imports
├── tests/CLAUDE.md       # Tests: Patterns, fixtures, markers
├── docs/CLAUDE.md        # Docs: ADR format, navigation
└── examples/CLAUDE.md    # Examples: Conventions, common patterns
```

### Anti-Patterns to Avoid

1. **Giant monolithic files**: Split into hierarchy
2. **Duplicating README**: CLAUDE.md is for AI, README for humans
3. **Listing every file**: Use glob patterns
4. **Full code snippets**: Reference by path instead
5. **Outdated information**: Review monthly
6. **Generic advice**: Only project-specific guidance

---

## MCP Server Configuration Reference

Comprehensive configuration patterns for Model Context Protocol (MCP) servers in Claude Code.

### Transport Types

**HTTP (Recommended for Remote)**:

```bash
# Add HTTP server
claude mcp add --transport http notion https://mcp.notion.com/mcp

# With authentication header
claude mcp add --transport http secure-api https://api.example.com/mcp \
  --header "Authorization: Bearer your-token"
```

**SSE (Deprecated)**:

```bash
# SSE is deprecated - use HTTP when available
claude mcp add --transport sse asana https://mcp.asana.com/sse
```

**Stdio (Local Servers)**:

```bash
# Add local stdio server
claude mcp add --transport stdio --env API_KEY=YOUR_KEY airtable \
  -- npx -y airtable-mcp-server

# Windows requires cmd wrapper
claude mcp add --transport stdio my-server -- cmd /c npx -y @some/package
```

### Configuration Scopes

| Scope   | Storage Location               | Use Case                           |
| ------- | ------------------------------ | ---------------------------------- |
| local   | ~/.claude.json                 | Personal servers, project-specific |
| project | .mcp.json (version controlled) | Team-shared servers                |
| user    | ~/.claude.json                 | Cross-project personal utilities   |

```bash
# Local scope (default)
claude mcp add --transport http stripe https://mcp.stripe.com

# Project scope (team-shared)
claude mcp add --transport http paypal --scope project https://mcp.paypal.com/mcp

# User scope (cross-project)
claude mcp add --transport http hubspot --scope user https://mcp.hubspot.com/anthropic
```

### Project .mcp.json Format

```json
{
  "mcpServers": {
    "database": {
      "type": "stdio",
      "command": "/path/to/server",
      "args": ["--config", "config.json"],
      "env": {
        "DB_URL": "${DB_URL}"
      }
    },
    "api": {
      "type": "http",
      "url": "${API_BASE_URL:-https://api.example.com}/mcp",
      "headers": {
        "Authorization": "Bearer ${API_KEY}"
      }
    }
  }
}
```

### Environment Variable Expansion

**Supported syntax**:

- `${VAR}` - Expands to value of VAR
- `${VAR:-default}` - Uses default if VAR not set

**Expansion locations**:

- `command` - Server executable path
- `args` - Command-line arguments
- `env` - Environment variables
- `url` - HTTP server URLs
- `headers` - Authentication headers

### Server Management Commands

```bash
# List all configured servers
claude mcp list

# Get details for specific server
claude mcp get github

# Remove a server
claude mcp remove github

# Check status within Claude Code
/mcp

# Import from Claude Desktop
claude mcp add-from-claude-desktop

# Add from JSON configuration
claude mcp add-json weather '{"type":"http","url":"https://api.weather.com/mcp"}'
```

### OAuth Authentication

```bash
# Add server requiring OAuth
claude mcp add --transport http sentry https://mcp.sentry.dev/mcp

# Authenticate within Claude Code
> /mcp
# Follow browser prompts to login

# Clear authentication
# Use /mcp menu > "Clear authentication"
```

### Tool Search Configuration

When MCP tools exceed context threshold, tool search activates:

| Value    | Behavior                                     |
| -------- | -------------------------------------------- |
| auto     | Activates at 10% context threshold (default) |
| auto:<N> | Custom threshold (e.g., auto:5 for 5%)       |
| true     | Always enabled                               |
| false    | Disabled, all tools loaded upfront           |

```bash
# Custom threshold
ENABLE_TOOL_SEARCH=auto:5 claude

# Disable tool search
ENABLE_TOOL_SEARCH=false claude
```

### Output Token Limits

```bash
# Warning threshold: 10,000 tokens (displays warning)
# Default limit: 25,000 tokens

# Increase for large outputs
export MAX_MCP_OUTPUT_TOKENS=50000
claude
```

### Debugging MCP Issues

```bash
# Launch with MCP debug flag
claude --mcp-debug

# Common fixes:
# - Increase timeout: MCP_TIMEOUT=10000 claude
# - Check authentication headers
# - Windows: Use cmd /c wrapper for npx
```

### Using Claude Code as MCP Server

```bash
# Start Claude as MCP server
claude mcp serve
```

**Claude Desktop configuration**:

```json
{
  "mcpServers": {
    "claude-code": {
      "type": "stdio",
      "command": "/full/path/to/claude",
      "args": ["mcp", "serve"],
      "env": {}
    }
  }
}
```

### Plugin-Provided MCP Servers

Plugins can bundle MCP servers:

**In .mcp.json at plugin root**:

```json
{
  "database-tools": {
    "command": "${CLAUDE_PLUGIN_ROOT}/servers/db-server",
    "args": ["--config", "${CLAUDE_PLUGIN_ROOT}/config.json"],
    "env": {
      "DB_URL": "${DB_URL}"
    }
  }
}
```

**Or inline in plugin.json**:

```json
{
  "name": "my-plugin",
  "mcpServers": {
    "plugin-api": {
      "command": "${CLAUDE_PLUGIN_ROOT}/servers/api-server",
      "args": ["--port", "8080"]
    }
  }
}
```

### Enterprise Managed MCP

**Option 1: Exclusive control with managed-mcp.json**:

Location:

- macOS: `/Library/Application Support/ClaudeCode/managed-mcp.json`
- Linux: `/etc/claude-code/managed-mcp.json`
- Windows: `C:\Program Files\ClaudeCode\managed-mcp.json`

```json
{
  "mcpServers": {
    "company-internal": {
      "type": "stdio",
      "command": "/usr/local/bin/company-mcp-server",
      "args": ["--config", "/etc/company/mcp-config.json"]
    }
  }
}
```

**Option 2: Policy-based allowlists/denylists**:

```json
{
  "allowedMcpServers": [
    { "serverName": "github" },
    { "serverCommand": ["npx", "-y", "approved-package"] },
    { "serverUrl": "https://mcp.company.com/*" }
  ],
  "deniedMcpServers": [
    { "serverName": "dangerous-server" },
    { "serverUrl": "https://*.untrusted.com/*" }
  ]
}
```

### DSM MCP Configuration

```json
{
  "mcpServers": {
    "dsm-database": {
      "type": "stdio",
      "command": "uv",
      "args": ["run", "-p", "3.13", "python", "-m", "dsm_mcp_server"],
      "env": {
        "DSM_CACHE_DIR": "${DSM_CACHE_DIR:-~/.cache/dsm}",
        "DSM_LOG_LEVEL": "INFO"
      }
    }
  }
}
```

### MCP Resources with @ Mentions

```
# Reference MCP resources
> Analyze @github:issue://123

# Multiple resources
> Compare @postgres:schema://users with @docs:file://database/user-model
```

### MCP Prompts as Commands

```
# Discover available prompts
> /

# Execute prompt without arguments
> /mcp__github__list_prs

# Execute prompt with arguments
> /mcp__github__pr_review 456
```

---

## Extended Thinking Reference

Extended thinking provides Claude enhanced reasoning capabilities for complex tasks through internal step-by-step reasoning.

### Supported Models

| Model             | Model ID                   | Features                          |
| ----------------- | -------------------------- | --------------------------------- |
| Claude Opus 4.5   | claude-opus-4-5-20251101   | Full thinking, block preservation |
| Claude Opus 4.1   | claude-opus-4-1-20250805   | Full thinking, interleaved        |
| Claude Opus 4     | claude-opus-4-20250514     | Full thinking, interleaved        |
| Claude Sonnet 4.5 | claude-sonnet-4-5-20250929 | Summarized thinking               |
| Claude Sonnet 4   | claude-sonnet-4-20250514   | Summarized thinking, interleaved  |
| Claude Haiku 4.5  | claude-haiku-4-5-20251001  | Summarized thinking               |

### Enabling Extended Thinking

```python
import anthropic

client = anthropic.Anthropic()

response = client.messages.create(
    model="claude-sonnet-4-5",
    max_tokens=16000,
    thinking={
        "type": "enabled",
        "budget_tokens": 10000
    },
    messages=[{
        "role": "user",
        "content": "Solve this complex problem step by step..."
    }]
)

# Process response with thinking blocks
for block in response.content:
    if block.type == "thinking":
        print(f"Thinking: {block.thinking}")
    elif block.type == "text":
        print(f"Response: {block.text}")
```

### Budget Token Guidelines

| Budget Range | Use Case                                      |
| ------------ | --------------------------------------------- |
| 1,024        | Minimum budget, simple reasoning              |
| 4,000-8,000  | Standard tasks, moderate complexity           |
| 16,000+      | Complex analysis, multi-step problems         |
| 32,000+      | Use batch processing (avoid network timeouts) |

**Key considerations**:

- `budget_tokens` must be less than `max_tokens`
- Budget is a target, not strict limit
- Claude may not use entire budget for simple tasks
- Start at minimum and increase incrementally

### Response Format

```json
{
  "content": [
    {
      "type": "thinking",
      "thinking": "Let me analyze this step by step...",
      "signature": "WaUjzkypQ2mUEVM..."
    },
    {
      "type": "text",
      "text": "Based on my analysis..."
    }
  ]
}
```

### Streaming Extended Thinking

```python
with client.messages.stream(
    model="claude-sonnet-4-5",
    max_tokens=16000,
    thinking={"type": "enabled", "budget_tokens": 10000},
    messages=[{"role": "user", "content": "Complex problem..."}],
) as stream:
    for event in stream:
        if event.type == "content_block_delta":
            if event.delta.type == "thinking_delta":
                print(event.delta.thinking, end="", flush=True)
            elif event.delta.type == "text_delta":
                print(event.delta.text, end="", flush=True)
```

### Extended Thinking with Tool Use

```python
# First request with thinking
response = client.messages.create(
    model="claude-sonnet-4-5",
    max_tokens=16000,
    thinking={"type": "enabled", "budget_tokens": 10000},
    tools=[weather_tool],
    messages=[{"role": "user", "content": "What's the weather in Paris?"}]
)

# Extract blocks
thinking_block = next(b for b in response.content if b.type == 'thinking')
tool_use_block = next(b for b in response.content if b.type == 'tool_use')

# Continue with tool result - MUST include thinking block
continuation = client.messages.create(
    model="claude-sonnet-4-5",
    max_tokens=16000,
    thinking={"type": "enabled", "budget_tokens": 10000},
    tools=[weather_tool],
    messages=[
        {"role": "user", "content": "What's the weather in Paris?"},
        {"role": "assistant", "content": [thinking_block, tool_use_block]},
        {"role": "user", "content": [{
            "type": "tool_result",
            "tool_use_id": tool_use_block.id,
            "content": "Current temperature: 88°F"
        }]}
    ]
)
```

### Interleaved Thinking (Claude 4 Models)

Enable thinking between tool calls:

```python
response = client.messages.create(
    model="claude-opus-4",
    max_tokens=16000,
    thinking={"type": "enabled", "budget_tokens": 10000},
    extra_headers={"anthropic-beta": "interleaved-thinking-2025-05-14"},
    tools=[calculator, database],
    messages=[{"role": "user", "content": "Calculate and compare..."}]
)
```

**Interleaved flow**:

```
User: "Calculate revenue and compare"
Turn 1: [thinking] → [tool_use: calculator]
  ↓ tool result
Turn 2: [thinking about result] → [tool_use: database]
  ↓ tool result
Turn 3: [thinking] → [text: final answer]
```

### Summarized vs Full Thinking

| Aspect             | Claude Sonnet 3.7 | Claude 4 Models     |
| ------------------ | ----------------- | ------------------- |
| Thinking output    | Full              | Summarized          |
| Billing            | Full tokens       | Full tokens         |
| Visible tokens     | Match billed      | Less than billed    |
| Block preservation | Not preserved     | Opus 4.5: preserved |

### Thinking with Prompt Caching

**System prompt caching** - preserved when thinking changes:

```python
response = client.messages.create(
    model="claude-sonnet-4-5",
    max_tokens=20000,
    thinking={"type": "enabled", "budget_tokens": 4000},
    system=[
        {"type": "text", "text": "System instructions..."},
        {"type": "text", "text": large_context, "cache_control": {"type": "ephemeral"}}
    ],
    messages=messages
)
```

**Message caching** - invalidated when thinking changes:

- Changing `budget_tokens` invalidates message cache
- System prompt cache remains valid
- Use 1-hour cache for long thinking sessions

### Redacted Thinking

Safety-flagged reasoning is encrypted:

```json
{
  "content": [
    {
      "type": "thinking",
      "thinking": "Normal reasoning...",
      "signature": "..."
    },
    { "type": "redacted_thinking", "data": "EmwKAhgBEgy3va3..." },
    { "type": "text", "text": "Final answer..." }
  ]
}
```

**Handling redacted blocks**:

- Pass back unmodified in subsequent requests
- Model can still use encrypted reasoning
- No impact on response quality

### Feature Compatibility

| Feature             | Compatible | Notes                            |
| ------------------- | ---------- | -------------------------------- |
| temperature         | No         | Must use defaults                |
| top_k               | No         | Must use defaults                |
| top_p               | Yes        | Values 0.95-1.0 only             |
| Forced tool use     | No         | Only auto or none                |
| Response prefilling | No         | Cannot pre-fill with thinking    |
| Streaming           | Yes        | Required for max_tokens > 21,333 |

### Context Window with Thinking

```
context window =
  (current input tokens - previous thinking tokens) +
  (thinking tokens + encrypted thinking tokens + text output tokens)
```

**With tool use**:

```
context window =
  (current input + previous thinking + tool use tokens) +
  (thinking + encrypted thinking + text output tokens)
```

### DSM Extended Thinking Patterns

```python
# Complex FCP analysis with extended thinking
response = client.messages.create(
    model="claude-opus-4",
    max_tokens=16000,
    thinking={"type": "enabled", "budget_tokens": 16000},
    messages=[{
        "role": "user",
        "content": """Analyze the FCP decision logic for BTCUSDT:
        - Current cache state
        - API availability
        - Data freshness requirements
        - Optimal fetch strategy"""
    }]
)

# Debug data inconsistencies with reasoning
response = client.messages.create(
    model="claude-sonnet-4-5",
    max_tokens=12000,
    thinking={"type": "enabled", "budget_tokens": 8000},
    messages=[{
        "role": "user",
        "content": "Debug why OHLCV data has gaps between 2024-01-15 and 2024-01-16"
    }]
)
```

### Best Practices

1. **Start small**: Begin with 1,024 tokens, increase as needed
2. **Monitor usage**: Track thinking tokens for cost optimization
3. **Use batch for large budgets**: >32k tokens need batch processing
4. **Preserve blocks**: Always pass thinking blocks back for tool use
5. **Consider latency**: Extended thinking increases response time
6. **Task selection**: Best for math, coding, analysis, complex reasoning

---

## GitHub Actions Integration Reference

Integrate Claude Code into CI/CD pipelines with @claude mentions, automated code review, and PR creation.

### Quick Setup

```bash
# In Claude Code terminal
/install-github-app
```

This installs the GitHub app and configures required secrets.

### Manual Setup

1. Install Claude GitHub app: <https://github.com/apps/claude>
2. Add `ANTHROPIC_API_KEY` to repository secrets
3. Copy workflow file to `.github/workflows/`

### Basic Workflow

```yaml
name: Claude Code
on:
  issue_comment:
    types: [created]
  pull_request_review_comment:
    types: [created]
jobs:
  claude:
    runs-on: ubuntu-latest
    steps:
      - uses: anthropics/claude-code-action@v1
        with:
          anthropic_api_key: ${{ secrets.ANTHROPIC_API_KEY }}
```

### @claude Mentions

In PR or issue comments:

```
@claude implement this feature based on the issue description
@claude how should I implement user authentication for this endpoint?
@claude fix the TypeError in the user dashboard component
@claude review this PR for security issues
```

### Action Parameters

| Parameter         | Description                               | Required |
| ----------------- | ----------------------------------------- | -------- |
| prompt            | Instructions (text or skill like /review) | No       |
| claude_args       | CLI arguments passed to Claude Code       | No       |
| anthropic_api_key | Claude API key                            | Yes\*    |
| github_token      | GitHub token for API access               | No       |
| trigger_phrase    | Custom trigger (default: "@claude")       | No       |
| use_bedrock       | Use AWS Bedrock                           | No       |
| use_vertex        | Use Google Vertex AI                      | No       |

\*Required for direct API, not for Bedrock/Vertex

### CLI Arguments via claude_args

```yaml
- uses: anthropics/claude-code-action@v1
  with:
    anthropic_api_key: ${{ secrets.ANTHROPIC_API_KEY }}
    prompt: "Review this PR"
    claude_args: |
      --max-turns 10
      --model claude-sonnet-4-5-20250929
      --append-system-prompt "Follow our coding standards"
```

Common arguments:

- `--max-turns`: Maximum conversation turns
- `--model`: Model to use
- `--mcp-config`: MCP configuration path
- `--allowed-tools`: Comma-separated allowed tools
- `--debug`: Enable debug output

### Code Review Workflow

```yaml
name: Code Review
on:
  pull_request:
    types: [opened, synchronize]
jobs:
  review:
    runs-on: ubuntu-latest
    steps:
      - uses: anthropics/claude-code-action@v1
        with:
          anthropic_api_key: ${{ secrets.ANTHROPIC_API_KEY }}
          prompt: "/review"
          claude_args: "--max-turns 5"
```

### Scheduled Automation

```yaml
name: Daily Report
on:
  schedule:
    - cron: "0 9 * * *"
jobs:
  report:
    runs-on: ubuntu-latest
    steps:
      - uses: anthropics/claude-code-action@v1
        with:
          anthropic_api_key: ${{ secrets.ANTHROPIC_API_KEY }}
          prompt: "Generate a summary of yesterday's commits and open issues"
          claude_args: "--model claude-opus-4-5-20251101"
```

### AWS Bedrock Integration

```yaml
name: Claude PR Action (Bedrock)
permissions:
  contents: write
  pull-requests: write
  issues: write
  id-token: write
on:
  issue_comment:
    types: [created]
jobs:
  claude-pr:
    if: contains(github.event.comment.body, '@claude')
    runs-on: ubuntu-latest
    env:
      AWS_REGION: us-west-2
    steps:
      - uses: actions/checkout@v4

      - name: Generate GitHub App token
        id: app-token
        uses: actions/create-github-app-token@v2
        with:
          app-id: ${{ secrets.APP_ID }}
          private-key: ${{ secrets.APP_PRIVATE_KEY }}

      - name: Configure AWS Credentials (OIDC)
        uses: aws-actions/configure-aws-credentials@v4
        with:
          role-to-assume: ${{ secrets.AWS_ROLE_TO_ASSUME }}
          aws-region: us-west-2

      - uses: anthropics/claude-code-action@v1
        with:
          github_token: ${{ steps.app-token.outputs.token }}
          use_bedrock: "true"
          claude_args: "--model us.anthropic.claude-sonnet-4-5-20250929-v1:0"
```

### Google Vertex AI Integration

```yaml
name: Claude PR Action (Vertex)
permissions:
  contents: write
  pull-requests: write
  issues: write
  id-token: write
on:
  issue_comment:
    types: [created]
jobs:
  claude-pr:
    if: contains(github.event.comment.body, '@claude')
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Generate GitHub App token
        id: app-token
        uses: actions/create-github-app-token@v2
        with:
          app-id: ${{ secrets.APP_ID }}
          private-key: ${{ secrets.APP_PRIVATE_KEY }}

      - name: Authenticate to Google Cloud
        id: auth
        uses: google-github-actions/auth@v2
        with:
          workload_identity_provider: ${{ secrets.GCP_WORKLOAD_IDENTITY_PROVIDER }}
          service_account: ${{ secrets.GCP_SERVICE_ACCOUNT }}

      - uses: anthropics/claude-code-action@v1
        with:
          github_token: ${{ steps.app-token.outputs.token }}
          use_vertex: "true"
          claude_args: "--model claude-sonnet-4@20250514"
        env:
          ANTHROPIC_VERTEX_PROJECT_ID: ${{ steps.auth.outputs.project_id }}
          CLOUD_ML_REGION: us-east5
```

### Required Permissions

```yaml
permissions:
  contents: write # Modify repository files
  pull-requests: write # Create PRs and push changes
  issues: write # Respond to issues
  id-token: write # For OIDC (Bedrock/Vertex)
```

### Security Best Practices

1. **Never commit API keys**: Use GitHub Secrets
2. **Limit permissions**: Only grant necessary scopes
3. **Review suggestions**: Always review before merging
4. **Use OIDC**: For Bedrock/Vertex, avoid static credentials
5. **Dedicate service accounts**: One per repository

### Cost Optimization

1. Use specific `@claude` commands to reduce API calls
2. Configure `--max-turns` to prevent excessive iterations
3. Set workflow-level timeouts to avoid runaway jobs
4. Use GitHub concurrency controls for parallel runs

### DSM GitHub Actions Integration

```yaml
name: DSM Code Review
on:
  pull_request:
    types: [opened, synchronize]
    paths:
      - "src/**/*.py"
      - "tests/**/*.py"
jobs:
  review:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - uses: anthropics/claude-code-action@v1
        with:
          anthropic_api_key: ${{ secrets.ANTHROPIC_API_KEY }}
          prompt: |
            Review this PR focusing on:
            - FCP protocol compliance
            - Silent failure patterns (bare except, etc.)
            - Timestamp handling (UTC requirements)
            - DataFrame operations (Polars patterns)
          claude_args: "--max-turns 5"
```

### Troubleshooting

| Issue                     | Solution                          |
| ------------------------- | --------------------------------- |
| Claude not responding     | Check GitHub App installation     |
| CI not running on commits | Use GitHub App, not Actions user  |
| Authentication errors     | Verify API key/OIDC configuration |
| @claude not triggering    | Ensure comment contains `@claude` |

---

## Claude Agent SDK Reference

Build production AI agents with Claude Code as a library for Python and TypeScript.

### Installation

```bash
# Python
pip install claude-agent-sdk

# TypeScript
npm install @anthropic-ai/claude-agent-sdk

# Claude Code runtime (required)
curl -fsSL https://claude.ai/install.sh | bash
```

### Basic Usage

**Python**:

```python
import asyncio
from claude_agent_sdk import query, ClaudeAgentOptions

async def main():
    async for message in query(
        prompt="Find and fix the bug in auth.py",
        options=ClaudeAgentOptions(allowed_tools=["Read", "Edit", "Bash"])
    ):
        print(message)

asyncio.run(main())
```

**TypeScript**:

```typescript
import { query } from "@anthropic-ai/claude-agent-sdk";

for await (const message of query({
  prompt: "Find and fix the bug in auth.py",
  options: { allowedTools: ["Read", "Edit", "Bash"] },
})) {
  console.log(message);
}
```

### Built-in Tools

| Tool            | Description                          |
| --------------- | ------------------------------------ |
| Read            | Read files in working directory      |
| Write           | Create new files                     |
| Edit            | Make precise edits to existing files |
| Bash            | Run terminal commands, scripts, git  |
| Glob            | Find files by pattern                |
| Grep            | Search file contents with regex      |
| WebSearch       | Search the web                       |
| WebFetch        | Fetch and parse web pages            |
| AskUserQuestion | Ask clarifying questions             |
| Task            | Invoke subagents                     |

### Permission Modes

| Mode              | Description                            |
| ----------------- | -------------------------------------- |
| bypassPermissions | No approval needed (fully autonomous)  |
| acceptEdits       | Auto-approve file edits                |
| default           | Require approval for sensitive actions |

```python
options = ClaudeAgentOptions(
    allowed_tools=["Read", "Glob", "Grep"],
    permission_mode="bypassPermissions"
)
```

### Hooks

Run custom code at key points in the agent lifecycle:

**Available events**: `PreToolUse`, `PostToolUse`, `PostToolUseFailure`, `Notification`, `UserPromptSubmit`, `SessionStart`, `SessionEnd`, `Stop`, `SubagentStart`, `SubagentStop`, `PreCompact`, `PermissionRequest`

**Python example**:

```python
from datetime import datetime
from claude_agent_sdk import query, ClaudeAgentOptions, HookMatcher

async def log_file_change(input_data, tool_use_id, context):
    file_path = input_data.get('tool_input', {}).get('file_path', 'unknown')
    with open('./audit.log', 'a') as f:
        f.write(f"{datetime.now()}: modified {file_path}\n")
    return {}

async def main():
    async for message in query(
        prompt="Refactor utils.py",
        options=ClaudeAgentOptions(
            permission_mode="acceptEdits",
            hooks={
                "PostToolUse": [HookMatcher(matcher="Edit|Write", hooks=[log_file_change])]
            }
        )
    ):
        print(message)
```

**TypeScript example**:

```typescript
import { query, HookCallback } from "@anthropic-ai/claude-agent-sdk";
import { appendFileSync } from "fs";

const logFileChange: HookCallback = async (input) => {
  const filePath = (input as any).tool_input?.file_path ?? "unknown";
  appendFileSync(
    "./audit.log",
    `${new Date().toISOString()}: modified ${filePath}\n`,
  );
  return {};
};

for await (const message of query({
  prompt: "Refactor utils.py",
  options: {
    permissionMode: "acceptEdits",
    hooks: {
      PostToolUse: [{ matcher: "Edit|Write", hooks: [logFileChange] }],
    },
  },
})) {
  console.log(message);
}
```

### Subagents

Define specialized agents for focused subtasks:

**Python**:

```python
from claude_agent_sdk import query, ClaudeAgentOptions, AgentDefinition

async for message in query(
    prompt="Use the code-reviewer agent to review this codebase",
    options=ClaudeAgentOptions(
        allowed_tools=["Read", "Glob", "Grep", "Task"],
        agents={
            "code-reviewer": AgentDefinition(
                description="Expert code reviewer for quality and security.",
                prompt="Analyze code quality and suggest improvements.",
                tools=["Read", "Glob", "Grep"]
            )
        }
    )
):
    print(message)
```

**TypeScript**:

```typescript
for await (const message of query({
  prompt: "Use the code-reviewer agent to review this codebase",
  options: {
    allowedTools: ["Read", "Glob", "Grep", "Task"],
    agents: {
      "code-reviewer": {
        description: "Expert code reviewer for quality and security.",
        prompt: "Analyze code quality and suggest improvements.",
        tools: ["Read", "Glob", "Grep"],
      },
    },
  },
})) {
  console.log(message);
}
```

### MCP Server Integration

Connect to external systems via Model Context Protocol:

```python
async for message in query(
    prompt="Open example.com and describe what you see",
    options=ClaudeAgentOptions(
        mcp_servers={
            "playwright": {"command": "npx", "args": ["@playwright/mcp@latest"]}
        }
    )
):
    print(message)
```

### Session Management

Maintain context across multiple exchanges:

```python
session_id = None

# First query: capture session ID
async for message in query(
    prompt="Read the authentication module",
    options=ClaudeAgentOptions(allowed_tools=["Read", "Glob"])
):
    if hasattr(message, 'subtype') and message.subtype == 'init':
        session_id = message.session_id

# Resume with full context
async for message in query(
    prompt="Now find all places that call it",
    options=ClaudeAgentOptions(resume=session_id)
):
    print(message)
```

### Authentication Options

```bash
# Direct Anthropic API (default)
export ANTHROPIC_API_KEY=your-api-key

# Amazon Bedrock
export CLAUDE_CODE_USE_BEDROCK=1
# + AWS credentials

# Google Vertex AI
export CLAUDE_CODE_USE_VERTEX=1
# + Google Cloud credentials

# Microsoft Foundry
export CLAUDE_CODE_USE_FOUNDRY=1
# + Azure credentials
```

### Filesystem Configuration

Enable Claude Code's filesystem-based configuration:

```python
options = ClaudeAgentOptions(
    setting_sources=["project"]
)
```

```typescript
options: {
  settingSources: ["project"];
}
```

This enables:

| Feature        | Location                           |
| -------------- | ---------------------------------- |
| Skills         | `.claude/skills/SKILL.md`          |
| Slash commands | `.claude/commands/*.md`            |
| Memory         | `CLAUDE.md` or `.claude/CLAUDE.md` |
| Plugins        | Programmatic via `plugins` option  |

### SDK vs CLI Comparison

| Use Case              | Best Choice |
| --------------------- | ----------- |
| Interactive dev       | CLI         |
| CI/CD pipelines       | SDK         |
| Custom applications   | SDK         |
| One-off tasks         | CLI         |
| Production automation | SDK         |

### DSM Agent SDK Integration

```python
from claude_agent_sdk import query, ClaudeAgentOptions, AgentDefinition

# FCP debugger agent
async for message in query(
    prompt="Debug the FCP cache miss for BTCUSDT",
    options=ClaudeAgentOptions(
        allowed_tools=["Read", "Glob", "Grep", "Bash", "Task"],
        agents={
            "fcp-debugger": AgentDefinition(
                description="FCP protocol debugging expert.",
                prompt="""Analyze FCP issues:
                - Check cache state in ~/.cache/dsm
                - Verify API availability
                - Analyze decision logic
                - Report root cause""",
                tools=["Read", "Grep", "Bash"]
            ),
            "silent-failure-hunter": AgentDefinition(
                description="Find silent failure patterns.",
                prompt="Search for bare except, except Exception, subprocess without check=True",
                tools=["Read", "Grep", "Glob"]
            )
        }
    )
):
    if hasattr(message, "result"):
        print(message.result)
```

---

## Prompt Engineering Best Practices

Optimize Claude 4.x model interactions with proven prompt engineering techniques.

### Be Explicit with Instructions

```text
# Less effective
Create an analytics dashboard

# More effective
Create an analytics dashboard. Include as many relevant features
and interactions as possible. Go beyond the basics to create
a fully-featured implementation.
```

### Add Context for Better Performance

```text
# Less effective
NEVER use ellipses

# More effective
Your response will be read aloud by a text-to-speech engine,
so never use ellipses since the text-to-speech engine will
not know how to pronounce them.
```

### XML Tags for Structure

Claude pays special attention to XML tags:

```text
<instructions>
Analyze the code for performance issues.
</instructions>

<context>
This is a high-throughput trading application.
</context>

<output_format>
Return findings as JSON with severity levels.
</output_format>
```

### Context Management Prompt

For Claude Code and agent harnesses:

```text
Your context window will be automatically compacted as it approaches
its limit, allowing you to continue working indefinitely from where
you left off. Therefore, do not stop tasks early due to token budget
concerns. As you approach your token budget limit, save your current
progress and state to memory before the context window refreshes.
Always be as persistent and autonomous as possible and complete tasks
fully, even if the end of your budget is approaching.
```

### Long-Horizon Task Management

**Multi-context window workflows**:

1. First context: Set up framework (tests, setup scripts)
2. Subsequent contexts: Iterate on todo list
3. Use structured state files (e.g., `tests.json`)
4. Create setup scripts (e.g., `init.sh`) for graceful restarts

**State tracking example**:

```json
// tests.json - structured state
{
  "tests": [
    { "id": 1, "name": "authentication_flow", "status": "passing" },
    { "id": 2, "name": "user_management", "status": "failing" }
  ],
  "total": 200,
  "passing": 150,
  "failing": 25
}
```

```text
// progress.txt - unstructured notes
Session 3 progress:
- Fixed authentication token validation
- Next: investigate user_management test failures
- Note: Do not remove tests
```

### Tool Usage Patterns

**Proactive action prompt**:

```text
<default_to_action>
By default, implement changes rather than only suggesting them.
If the user's intent is unclear, infer the most useful likely
action and proceed, using tools to discover any missing details
instead of guessing.
</default_to_action>
```

**Conservative action prompt**:

```text
<do_not_act_before_instructions>
Do not jump into implementation unless clearly instructed.
When the user's intent is ambiguous, default to providing
information, doing research, and providing recommendations
rather than taking action.
</do_not_act_before_instructions>
```

### Parallel Tool Calling

```text
<use_parallel_tool_calls>
If you intend to call multiple tools and there are no dependencies
between the tool calls, make all of the independent tool calls in
parallel. Maximize use of parallel tool calls where possible to
increase speed and efficiency. However, if some tool calls depend
on previous calls, call them sequentially. Never use placeholders
or guess missing parameters.
</use_parallel_tool_calls>
```

### Minimize Overengineering

```text
Avoid over-engineering. Only make changes that are directly
requested or clearly necessary. Keep solutions simple and focused.

Don't add features, refactor code, or make "improvements" beyond
what was asked. Don't add error handling for scenarios that can't
happen. Don't create helpers or abstractions for one-time operations.
Don't design for hypothetical future requirements.

The right amount of complexity is the minimum needed for the
current task.
```

### Code Exploration Prompt

```text
ALWAYS read and understand relevant files before proposing code
edits. Do not speculate about code you have not inspected. If the
user references a specific file/path, you MUST open and inspect it
before explaining or proposing fixes. Be rigorous and persistent
in searching code for key facts.
```

### Minimize Hallucinations

```text
<investigate_before_answering>
Never speculate about code you have not opened. If the user
references a specific file, you MUST read the file before
answering. Make sure to investigate and read relevant files
BEFORE answering questions about the codebase. Never make any
claims about code before investigating unless you are certain
of the correct answer.
</investigate_before_answering>
```

### Research and Information Gathering

```text
Search for this information in a structured way. As you gather
data, develop several competing hypotheses. Track your confidence
levels in your progress notes. Regularly self-critique your
approach and plan. Update a hypothesis tree or research notes
file to persist information and provide transparency.
```

### Subagent Orchestration

```text
# Conservative subagent usage
Only delegate to subagents when the task clearly benefits from
a separate agent with a new context window.
```

### Reduce Verbose Output

```text
<avoid_excessive_markdown_and_bullet_points>
When writing reports or analyses, write in clear, flowing prose
using complete paragraphs and sentences. Reserve markdown primarily
for `inline code`, code blocks, and simple headings.

DO NOT use ordered lists or unordered lists unless presenting
truly discrete items or the user explicitly requests a list.

Instead of listing items with bullets, incorporate them naturally
into sentences. NEVER output a series of overly short bullet points.
</avoid_excessive_markdown_and_bullet_points>
```

### Thinking Mode Tips

When extended thinking is disabled, Claude Opus 4.5 is sensitive to "think":

```text
# Instead of "think", use:
- "consider"
- "believe"
- "evaluate"
- "analyze"
- "reflect"
```

For interleaved thinking:

```text
After receiving tool results, carefully reflect on their quality
and determine optimal next steps before proceeding. Use your
thinking to plan and iterate based on this new information.
```

### DSM Prompt Engineering Patterns

**FCP Analysis Prompt**:

```text
<dsm_fcp_analysis>
When analyzing FCP (Failover Control Protocol) decisions:
1. Check cache state in ~/.cache/dsm
2. Verify API availability (rate limits, errors)
3. Analyze data freshness requirements
4. Consider symbol format (Binance: BTCUSDT, etc.)
5. Report decision logic and recommendations

Use structured state tracking:
- fcp_state.json for decision history
- progress.txt for analysis notes
</dsm_fcp_analysis>
```

**Data Validation Prompt**:

```text
<dsm_data_validation>
When validating OHLCV DataFrames:
1. Check required columns: open_time, open, high, low, close, volume
2. Verify UTC timestamps (all datetimes must be timezone-aware)
3. Validate data continuity (no gaps > expected interval)
4. Check OHLC relationships: high >= max(open, close), low <= min(open, close)
5. Use Polars for DataFrame operations (not pandas)
</dsm_data_validation>
```

### Best Practices Summary

| Technique                 | Purpose                          |
| ------------------------- | -------------------------------- |
| Explicit instructions     | Better precision from Claude 4.x |
| Context/motivation        | Improved understanding           |
| XML tags                  | Clear section separation         |
| Context management prompt | Long-running autonomous tasks    |
| Structured state files    | Multi-context window workflows   |
| Parallel tool calls       | Speed and efficiency             |
| Investigation prompts     | Reduce hallucinations            |
| Minimize overengineering  | Focused, simple solutions        |

---

## Computer Use Tool Reference

Claude can interact with desktop environments through screenshots and mouse/keyboard control for autonomous desktop automation.

### Model Compatibility

| Model            | Tool Version      | Beta Flag               |
| ---------------- | ----------------- | ----------------------- |
| Claude Opus 4.5  | computer_20251124 | computer-use-2025-11-24 |
| All other models | computer_20250124 | computer-use-2025-01-24 |

### Basic Usage

```python
import anthropic

client = anthropic.Anthropic()

response = client.beta.messages.create(
    model="claude-sonnet-4-5",
    max_tokens=1024,
    tools=[
        {
            "type": "computer_20250124",
            "name": "computer",
            "display_width_px": 1024,
            "display_height_px": 768,
            "display_number": 1,
        }
    ],
    messages=[{"role": "user", "content": "Take a screenshot of the desktop."}],
    betas=["computer-use-2025-01-24"]
)
```

### Available Actions

**Basic actions (all versions)**:

| Action     | Description             | Parameters         |
| ---------- | ----------------------- | ------------------ |
| screenshot | Capture current display | None               |
| left_click | Click at coordinates    | coordinate: [x, y] |
| type       | Type text string        | text: string       |
| key        | Press key combination   | text: "ctrl+s"     |
| mouse_move | Move cursor to position | coordinate: [x, y] |

**Enhanced actions (computer_20250124)**:

| Action          | Description                | Parameters                    |
| --------------- | -------------------------- | ----------------------------- |
| scroll          | Scroll in any direction    | coordinate, direction, amount |
| left_click_drag | Click and drag             | start_coordinate, coordinate  |
| right_click     | Right mouse button         | coordinate: [x, y]            |
| double_click    | Double click               | coordinate: [x, y]            |
| triple_click    | Triple click (select line) | coordinate: [x, y]            |
| hold_key        | Hold key for duration      | text, duration_seconds        |
| wait            | Pause between actions      | duration_seconds              |

**Enhanced actions (computer_20251124 - Opus 4.5)**:

| Action | Description                    | Parameters               |
| ------ | ------------------------------ | ------------------------ |
| zoom   | View screen region at full res | region: [x1, y1, x2, y2] |

### Action Examples

```json
// Take a screenshot
{"action": "screenshot"}

// Click at position
{"action": "left_click", "coordinate": [500, 300]}

// Type text
{"action": "type", "text": "Hello, world!"}

// Keyboard shortcut
{"action": "key", "text": "ctrl+s"}

// Scroll down
{
    "action": "scroll",
    "coordinate": [500, 400],
    "scroll_direction": "down",
    "scroll_amount": 3
}

// Shift+click for selection
{
    "action": "left_click",
    "coordinate": [500, 300],
    "text": "shift"
}

// Zoom to region (Opus 4.5)
{"action": "zoom", "region": [100, 200, 400, 350]}
```

### Tool Parameters

| Parameter         | Required | Description                   |
| ----------------- | -------- | ----------------------------- |
| type              | Yes      | Tool version                  |
| name              | Yes      | Must be "computer"            |
| display_width_px  | Yes      | Display width in pixels       |
| display_height_px | Yes      | Display height in pixels      |
| display_number    | No       | X11 display number            |
| enable_zoom       | No       | Enable zoom action (Opus 4.5) |

### Agent Loop Implementation

```python
async def sampling_loop(model, messages, max_iterations=10):
    client = Anthropic()
    tools = [
        {"type": "computer_20250124", "name": "computer",
         "display_width_px": 1024, "display_height_px": 768}
    ]

    iterations = 0
    while iterations < max_iterations:
        iterations += 1

        response = client.beta.messages.create(
            model=model,
            max_tokens=4096,
            messages=messages,
            tools=tools,
            betas=["computer-use-2025-01-24"]
        )

        messages.append({"role": "assistant", "content": response.content})

        # Process tool calls
        tool_results = []
        for block in response.content:
            if block.type == "tool_use":
                result = handle_computer_action(block.input["action"], block.input)
                tool_results.append({
                    "type": "tool_result",
                    "tool_use_id": block.id,
                    "content": result
                })

        if not tool_results:
            return messages  # Task complete

        messages.append({"role": "user", "content": tool_results})
```

### Coordinate Scaling

Handle higher resolutions by scaling coordinates:

```python
import math

def get_scale_factor(width, height):
    """Calculate scale for API constraints (1568px max edge, 1.15MP total)."""
    long_edge_scale = 1568 / max(width, height)
    total_pixels_scale = math.sqrt(1_150_000 / (width * height))
    return min(1.0, long_edge_scale, total_pixels_scale)

# Scale screenshot before sending
scale = get_scale_factor(screen_width, screen_height)
scaled_width = int(screen_width * scale)
scaled_height = int(screen_height * scale)

# Scale coordinates back up when executing
def execute_click(x, y):
    screen_x = x / scale
    screen_y = y / scale
    perform_click(screen_x, screen_y)
```

### Security Considerations

1. **Use sandboxed environment**: Containerized or VM with minimal privileges
2. **Avoid sensitive data access**: No account credentials without oversight
3. **Limit internet access**: Allowlist approved domains
4. **Human confirmation**: For meaningful real-world consequences
5. **Prompt injection defense**: Auto-classifiers steer to user confirmation

### Prompting Tips

```text
After each step, take a screenshot and carefully evaluate if you
have achieved the right outcome. Explicitly show your thinking:
"I have evaluated step X..." If not correct, try again. Only when
you confirm a step was executed correctly should you move on.
```

**For login credentials**:

```text
<robot_credentials>
username: test_user
password: test_pass
</robot_credentials>
```

### Combining with Other Tools

```python
tools = [
    {"type": "computer_20250124", "name": "computer",
     "display_width_px": 1024, "display_height_px": 768},
    {"type": "text_editor_20250728", "name": "str_replace_based_edit_tool"},
    {"type": "bash_20250124", "name": "bash"},
    # Custom tools
    {"name": "get_weather", "description": "Get weather",
     "input_schema": {...}}
]
```

### Error Handling

```json
// Screenshot failure
{
    "type": "tool_result",
    "tool_use_id": "toolu_01A09q90qw90lq917835lq9",
    "content": "Error: Failed to capture screenshot. Display unavailable.",
    "is_error": true
}

// Invalid coordinates
{
    "type": "tool_result",
    "tool_use_id": "toolu_01A09q90qw90lq917835lq9",
    "content": "Error: Coordinates (1200, 900) outside bounds (1024x768).",
    "is_error": true
}
```

### Limitations

1. **Latency**: May be slow for real-time interactions
2. **Vision accuracy**: May hallucinate coordinates
3. **Scrolling**: Improved in Claude 4 models
4. **Spreadsheets**: Use fine-grained mouse control
5. **Account creation**: Limited on social platforms
6. **Prompt injection**: Content may override instructions

### Token Usage

| Component                | Tokens                 |
| ------------------------ | ---------------------- |
| System prompt overhead   | 466-499                |
| Computer tool definition | 735 per tool           |
| Screenshots              | Vision pricing applies |

---

## Cost Management Reference

Track usage, set spend limits, and optimize Claude Code costs through context management and model selection.

### Pricing Overview

**Average costs**:

- ~$6 per developer per day (median)
- <$12/day for 90% of users
- ~$100-200/developer per month with Sonnet 4.5

**Model pricing (per million tokens)**:

| Model             | Input | Output |
| ----------------- | ----- | ------ |
| Claude Haiku 4.5  | $1    | $5     |
| Claude Sonnet 4.5 | $3    | $15    |
| Claude Opus 4.5   | $5    | $25    |

### Track Usage

**Using /cost command**:

```
Total cost:            $0.55
Total duration (API):  6m 19.7s
Total duration (wall): 6h 33m 10.2s
Total code changes:    0 lines added, 0 lines removed
```

**Status line display**: Configure to show context usage continuously.

### Team Rate Limit Recommendations

| Team Size     | TPM per User | RPM per User |
| ------------- | ------------ | ------------ |
| 1-5 users     | 200k-300k    | 5-7          |
| 5-20 users    | 100k-150k    | 2.5-3.5      |
| 20-50 users   | 50k-75k      | 1.25-1.75    |
| 50-100 users  | 25k-35k      | 0.62-0.87    |
| 100-500 users | 15k-20k      | 0.37-0.47    |
| 500+ users    | 10k-15k      | 0.25-0.35    |

### Context Management

**Auto-optimization features**:

- **Prompt caching**: Reduces costs for repeated system prompts
- **Auto-compaction**: Summarizes history at context limits

**Clear between tasks**:

```
/rename "feature-auth"
/clear
# Work on new task
/resume    # Return to previous session
```

**Custom compaction**:

```
/compact Focus on code samples and API usage
```

**CLAUDE.md compaction instructions**:

```markdown
# Compact instructions

When you are using compact, please focus on test output and code changes
```

### Model Selection Strategy

| Task Type             | Recommended Model |
| --------------------- | ----------------- |
| Daily development     | Sonnet 4.5        |
| Complex architecture  | Opus 4.5          |
| Simple subagent tasks | Haiku 4.5         |

```
/model sonnet    # Switch to Sonnet
/model opus      # Switch to Opus
/config          # Set default model
```

Subagent model selection:

```yaml
# .claude/agents/simple-task.md
model: haiku
```

### Reduce MCP Overhead

**Prefer CLI tools**:

```bash
# More efficient than MCP servers
gh pr list          # GitHub CLI
aws s3 ls           # AWS CLI
gcloud compute list # GCloud CLI
```

**Disable unused servers**:

```
/mcp    # View and manage servers
```

**Lower tool search threshold**:

```bash
# Trigger at 5% instead of default 10%
ENABLE_TOOL_SEARCH=auto:5 claude
```

### Extended Thinking Optimization

**Default**: 31,999 tokens (improves complex reasoning)

**Reduce for simple tasks**:

```bash
# Lower thinking budget
MAX_THINKING_TOKENS=8000 claude
```

```
/config    # Disable thinking entirely
```

Thinking tokens are billed as output tokens.

### Preprocessing Hooks for Cost Reduction

**Filter test output** (settings.json):

```json
{
  "hooks": {
    "PreToolUse": [
      {
        "matcher": "Bash",
        "hooks": [
          {
            "type": "command",
            "command": "~/.claude/hooks/filter-test-output.sh"
          }
        ]
      }
    ]
  }
}
```

**Filter script** (filter-test-output.sh):

```bash
#!/bin/bash
input=$(cat)
cmd=$(echo "$input" | jq -r '.tool_input.command')

# Filter test output to failures only
if [[ "$cmd" =~ ^(npm test|pytest|go test) ]]; then
  filtered_cmd="$cmd 2>&1 | grep -A 5 -E '(FAIL|ERROR|error:)' | head -100"
  echo '{"hookSpecificOutput":{"hookEventName":"PreToolUse","permissionDecision":"allow","updatedInput":{"command":"'"$filtered_cmd"'"}}}'
else
  echo "{}"
fi
```

### Skills vs CLAUDE.md

| Content Type           | Location  | Loaded    |
| ---------------------- | --------- | --------- |
| Essential instructions | CLAUDE.md | Always    |
| Specialized workflows  | Skills    | On-demand |

**Goal**: Keep CLAUDE.md under ~500 lines.

### Subagent Delegation

Offload verbose operations:

```yaml
# .claude/agents/log-analyzer.md
description: Analyze log files and return summary
model: haiku
tools:
  - Read
  - Grep
```

Verbose output stays in subagent context; only summary returns.

### Efficient Work Habits

1. **Plan mode**: Shift+Tab before complex implementation
2. **Course-correct early**: Escape to stop wrong direction
3. **Rewind**: /rewind or double-Escape to checkpoint
4. **Verification targets**: Include test cases, screenshots
5. **Incremental testing**: Write one file, test, continue

### Background Token Usage

| Operation                  | Cost           |
| -------------------------- | -------------- |
| Conversation summarization | ~$0.04/session |
| Command processing         | Minimal        |

### DSM Cost Optimization

```yaml
# .claude/agents/fcp-validator.md
---
description: Validate FCP behavior with minimal context
model: haiku
tools:
  - Read
  - Grep
  - Bash
---
Check FCP cache state and return summary.
Only report anomalies, not full cache contents.
```

**CLAUDE.md optimization**:

```markdown
# DSM Quick Reference

## Essential Commands

- `uv run pytest tests/unit/` - Unit tests
- `uv run pytest tests/integration/` - Integration tests

## Key Patterns

- FCP: See @.claude/rules/fcp-protocol.md
- Timestamps: Always UTC with timezone.utc
- DataFrames: Use Polars, not pandas

<!-- Detailed docs in skills, not here -->
```

---

## Troubleshooting Reference

Diagnose and resolve common Claude Code issues.

### Diagnostic Commands

```bash
# Check installation health
claude doctor

# Report bug with context
/bug

# View current context usage
/context

# Check API key
echo $ANTHROPIC_API_KEY

# Check version
claude --version
```

**What /doctor checks**:

- Installation type and version
- Auto-update status
- Invalid settings files (malformed JSON)
- MCP server configuration errors
- Keybinding problems
- Context usage warnings (large CLAUDE.md, high MCP tokens)
- Plugin and agent loading errors

### Configuration File Locations

| File                        | Purpose                             |
| --------------------------- | ----------------------------------- |
| ~/.claude/settings.json     | User settings                       |
| .claude/settings.json       | Project settings (committed)        |
| .claude/settings.local.json | Local project settings (gitignored) |
| ~/.claude.json              | Global state (theme, OAuth, MCP)    |
| .mcp.json                   | Project MCP servers (committed)     |
| managed-settings.json       | Managed settings (admin)            |
| managed-mcp.json            | Managed MCP servers (admin)         |

**Managed file locations**:

- macOS: `/Library/Application Support/ClaudeCode/`
- Linux/WSL: `/etc/claude-code/`
- Windows: `C:\Program Files\ClaudeCode\`

### Installation Issues

**Native installation (recommended)**:

```bash
# macOS, Linux, WSL
curl -fsSL https://claude.ai/install.sh | bash

# Windows PowerShell
irm https://claude.ai/install.ps1 | iex

# Specific version
curl -fsSL https://claude.ai/install.sh | bash -s <version>
```

<!-- SSoT-OK: Version placeholder in Claude Code documentation -->

**PATH not found** (Windows):

1. Open Environment Variables (Win+R → sysdm.cpl → Advanced)
2. Edit User PATH, add: `%USERPROFILE%\.local\bin`
3. Restart terminal

### WSL-Specific Issues

**OS detection problems**:

```bash
npm config set os linux
npm install -g @anthropic-ai/claude-code --force --no-os-check
```

**Node not found**:

```bash
# Check if using Windows paths
which npm   # Should start with /usr/, not /mnt/c/
which node

# Install via nvm (see https://github.com/nvm-sh/nvm for latest version)
curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/<version>/install.sh | bash
source ~/.nvm/nvm.sh
nvm install --lts
```

<!-- SSoT-OK: Version placeholder for nvm in Claude Code documentation -->

**nvm version conflicts**:

```bash
# Add to ~/.bashrc or ~/.zshrc
export NVM_DIR="$HOME/.nvm"
[ -s "$NVM_DIR/nvm.sh" ] && \. "$NVM_DIR/nvm.sh"
```

**Sandbox requirements** (WSL2):

```bash
# Ubuntu/Debian
sudo apt-get install bubblewrap socat

# Fedora
sudo dnf install bubblewrap socat
```

### Authentication Issues

```bash
# Reset authentication
/logout
# Close and restart Claude Code

# Force clean login
rm -rf ~/.config/claude-code/auth.json
claude
```

**Browser doesn't open**: Press `c` to copy OAuth URL to clipboard.

### Performance Issues

**High CPU/memory**:

1. Use `/compact` regularly
2. Close and restart between major tasks
3. Add build directories to `.gitignore`

**Command hangs**:

1. Press Ctrl+C to cancel
2. If unresponsive, close terminal and restart

### Search Issues

**Search not working**:

```bash
# Install system ripgrep
brew install ripgrep          # macOS
sudo apt install ripgrep      # Ubuntu/Debian
winget install BurntSushi.ripgrep.MSVC  # Windows

# Set environment variable
export USE_BUILTIN_RIPGREP=0
```

**Slow search on WSL**:

1. Submit more specific searches
2. Move project to Linux filesystem (`/home/` not `/mnt/c/`)
3. Consider native Windows instead

### IDE Integration Issues

**JetBrains not detected (WSL2)**:

```bash
# Find WSL2 IP
wsl hostname -I

# Create firewall rule (PowerShell Admin)
New-NetFirewallRule -DisplayName "Allow WSL2 Internal Traffic" \
  -Direction Inbound -Protocol TCP -Action Allow \
  -RemoteAddress 172.21.0.0/16 -LocalAddress 172.21.0.0/16
```

**Escape key not working (JetBrains)**:

1. Settings → Tools → Terminal
2. Uncheck "Move focus to the editor with Escape"
3. Apply changes

### Reset Configuration

```bash
# Reset all user settings
rm ~/.claude.json
rm -rf ~/.claude/

# Reset project settings
rm -rf .claude/
rm .mcp.json
```

### Common Error Solutions

| Error                         | Solution                               |
| ----------------------------- | -------------------------------------- |
| "installMethod is native..."  | Add ~/.local/bin to PATH               |
| "requires git-bash" (Windows) | Install Git for Windows                |
| "Sandbox requires WSL2"       | Upgrade WSL or run without sandboxing  |
| Permission denied             | Check npm prefix or use native install |
| MCP server not connecting     | Run claude --mcp-debug                 |
| Context limit exceeded        | Use /compact or /clear                 |

### Getting Help

1. **Built-in docs**: Ask Claude about its capabilities
2. **Report bugs**: Use `/bug` command
3. **GitHub issues**: <https://github.com/anthropics/claude-code/issues>
4. **Run diagnostics**: `/doctor`

### DSM-Specific Troubleshooting

**FCP cache issues**:

```bash
# Check cache state
ls -la ~/.cache/dsm/

# Clear cache
mise run cache:clear

# Verify API connectivity
uv run python -c "from data_source_manager import DataSourceManager; print('OK')"
```

**Python version issues**:

```bash
# Verify Python 3.13
uv run -p 3.13 python --version

# Check mise configuration
mise current python
```

**Test failures**:

```bash
# Run with verbose output
uv run pytest tests/ -v --tb=long

# Run specific test
uv run pytest tests/unit/test_manager.py -v
```

---

## Structured Outputs Reference

Guarantee schema-compliant JSON responses from Claude.

### Overview

Structured outputs constrain Claude's responses to follow a specific schema, ensuring valid, parseable output. Two complementary features:

| Feature                               | Purpose                  | When to Use                       |
| ------------------------------------- | ------------------------ | --------------------------------- |
| JSON outputs (`output_config.format`) | Control response format  | Data extraction, API responses    |
| Strict tool use (`strict: true`)      | Validate tool parameters | Agentic workflows, function calls |

**Benefits**:

- **Always valid**: No more `JSON.parse()` errors
- **Type safe**: Guaranteed field types and required fields
- **Reliable**: No retries needed for schema violations

### JSON Outputs Quick Start

**Python with Pydantic**:

```python
from pydantic import BaseModel
from anthropic import Anthropic, transform_schema

class ContactInfo(BaseModel):
    name: str
    email: str
    plan_interest: str
    demo_requested: bool

client = Anthropic()

# Method 1: Using .parse() (recommended)
response = client.messages.parse(
    model="claude-sonnet-4-5",
    max_tokens=1024,
    messages=[{"role": "user", "content": "Extract from email: John (john@example.com) wants Enterprise demo"}],
    output_format=ContactInfo,
)
contact = response.parsed_output
print(contact.name, contact.email)

# Method 2: Using .create() with transform_schema()
response = client.messages.create(
    model="claude-sonnet-4-5",
    max_tokens=1024,
    messages=[{"role": "user", "content": "Extract from email..."}],
    output_config={
        "format": {
            "type": "json_schema",
            "schema": transform_schema(ContactInfo),
        }
    }
)
```

**TypeScript with Zod**:

```typescript
import Anthropic from "@anthropic-ai/sdk";
import { z } from "zod";
import { zodOutputFormat } from "@anthropic-ai/sdk/helpers/zod";

const ContactInfoSchema = z.object({
  name: z.string(),
  email: z.string(),
  plan_interest: z.string(),
  demo_requested: z.boolean(),
});

const client = new Anthropic();

const response = await client.messages.create({
  model: "claude-sonnet-4-5",
  max_tokens: 1024,
  messages: [{ role: "user", content: "Extract from email..." }],
  output_config: { format: zodOutputFormat(ContactInfoSchema) },
});
```

### Strict Tool Use

Guarantee tool parameters match schema exactly:

```python
response = client.messages.create(
    model="claude-sonnet-4-5",
    max_tokens=1024,
    messages=[{"role": "user", "content": "Search flights to Tokyo"}],
    tools=[{
        "name": "search_flights",
        "description": "Search for available flights",
        "strict": True,  # Enable strict mode
        "input_schema": {
            "type": "object",
            "properties": {
                "destination": {"type": "string"},
                "departure_date": {"type": "string", "format": "date"},
                "passengers": {"type": "integer", "enum": [1, 2, 3, 4, 5, 6]}
            },
            "required": ["destination", "departure_date"],
            "additionalProperties": False
        }
    }]
)
```

**Without strict mode**: Claude might return `passengers: "two"` or `passengers: "2"`.
**With strict mode**: Always returns `passengers: 2` (correct integer type).

### Combined Usage

Use both JSON outputs and strict tool use together:

```python
response = client.messages.create(
    model="claude-sonnet-4-5",
    max_tokens=1024,
    messages=[{"role": "user", "content": "Plan a trip to Paris"}],
    # JSON outputs: structured response format
    output_config={
        "format": {
            "type": "json_schema",
            "schema": {
                "type": "object",
                "properties": {
                    "summary": {"type": "string"},
                    "next_steps": {"type": "array", "items": {"type": "string"}}
                },
                "required": ["summary", "next_steps"],
                "additionalProperties": False
            }
        }
    },
    # Strict tool use: guaranteed tool parameters
    tools=[{
        "name": "search_flights",
        "strict": True,
        "input_schema": {
            "type": "object",
            "properties": {
                "destination": {"type": "string"},
                "date": {"type": "string", "format": "date"}
            },
            "required": ["destination", "date"],
            "additionalProperties": False
        }
    }]
)
```

### JSON Schema Support

**Supported features**:

| Feature        | Examples                                              |
| -------------- | ----------------------------------------------------- |
| Basic types    | object, array, string, integer, number, boolean, null |
| Enums          | `{"type": "string", "enum": ["a", "b"]}`              |
| References     | `$ref`, `$def`, `definitions` (local only)            |
| Combinators    | `anyOf`, `allOf` (with limitations)                   |
| String formats | date-time, date, time, email, uri, uuid, ipv4, ipv6   |
| Array minItems | 0 or 1 only                                           |

**Not supported**:

- Recursive schemas
- Numerical constraints (`minimum`, `maximum`, `multipleOf`)
- String constraints (`minLength`, `maxLength`)
- `additionalProperties` set to anything other than `false`
- External `$ref` URLs
- Complex regex patterns (backreferences, lookahead)

### SDK Schema Transformation

SDKs automatically transform unsupported features:

1. Remove unsupported constraints (`minimum`, `maxLength`)
2. Add constraint info to descriptions ("Must be at least 100")
3. Add `additionalProperties: false` to all objects
4. Validate responses against original schema

**Example**:

```python
from pydantic import BaseModel, Field

class Product(BaseModel):
    name: str
    price: float = Field(ge=0, le=10000)  # Constraints in Pydantic
    quantity: int = Field(ge=1)

# SDK transforms to simplified schema for Claude
# But validates response against full Pydantic constraints
```

### Performance Considerations

**Grammar compilation**:

- First request with new schema has compilation latency
- Compiled grammars cached 24 hours
- Cache invalidated by schema structure changes (not name/description)

**Token costs**:

- Additional system prompt injected for format explanation
- Changing `output_config.format` invalidates prompt cache

### Error Handling

**Refusals** (`stop_reason: "refusal"`):

- Claude maintains safety properties even with structured outputs
- Refusal message takes precedence over schema constraints
- Still returns 200 status, still billed

**Token limit** (`stop_reason: "max_tokens"`):

- Output may be incomplete and not match schema
- Retry with higher `max_tokens`

**Schema errors** (400 status):

- "Too many recursive definitions in schema"
- "Schema is too complex"
- Solution: Simplify schema, reduce nesting

### Feature Compatibility

| Feature            | Compatible | Notes                          |
| ------------------ | ---------- | ------------------------------ |
| Batch processing   | Yes        | 50% discount applies           |
| Token counting     | Yes        | Count without compilation      |
| Streaming          | Yes        | Stream like normal responses   |
| Extended Thinking  | Yes        | Grammar applies to output only |
| Citations          | No         | 400 error if combined          |
| Message prefilling | No         | Incompatible with JSON outputs |

### DSM Structured Output Patterns

**OHLCV response schema**:

```python
from pydantic import BaseModel
from typing import List
from datetime import datetime

class OHLCVBar(BaseModel):
    open_time: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float

class OHLCVResponse(BaseModel):
    symbol: str
    timeframe: str
    bars: List[OHLCVBar]
    source: str
    is_complete: bool

# Use for FCP validation results
response = client.messages.parse(
    model="claude-sonnet-4-5",
    output_format=OHLCVResponse,
    messages=[{"role": "user", "content": f"Validate this OHLCV data: {data}"}]
)
```

**FCP status schema**:

```python
from pydantic import BaseModel
from typing import Optional
from enum import Enum

class FCPDecision(str, Enum):
    USE_CACHE = "use_cache"
    FETCH_LIVE = "fetch_live"
    FAILOVER = "failover"

class FCPStatus(BaseModel):
    symbol: str
    decision: FCPDecision
    cache_hit: bool
    source_used: str
    staleness_seconds: Optional[int]
    error_message: Optional[str]
```

---

## MCP Server Ecosystem Reference

Extend Claude Code capabilities with Model Context Protocol servers.

### Essential MCP Servers

| Server              | Purpose                 | Install Command                                                        |
| ------------------- | ----------------------- | ---------------------------------------------------------------------- |
| Context7            | Real-time documentation | `claude mcp add --transport sse context7 https://mcp.context7.com/sse` |
| Sequential Thinking | Structured reasoning    | `npx @modelcontextprotocol/server-sequential-thinking`                 |
| Playwright          | Web automation          | `npx @anthropic-ai/mcp-server-playwright`                              |
| GitHub              | Repository management   | `npx @modelcontextprotocol/server-github`                              |
| Memory              | Persistent knowledge    | `npx @modelcontextprotocol/server-memory`                              |
| Filesystem          | File operations         | `npx @modelcontextprotocol/server-filesystem`                          |

### Context7 MCP

Fetch real-time documentation from source repositories:

```bash
# Installation
claude mcp add --transport sse context7 https://mcp.context7.com/sse

# Usage in prompts
"Create a React Server Component using Next.js 14 patterns - use context7"
"Show me the latest Polars DataFrame API - use context7"
```

**Benefits**:

- Always current documentation (not training data)
- Version-specific code examples
- 20+ client support (Cursor, VS Code, Claude Desktop, JetBrains)

### Sequential Thinking MCP

Structured multi-step problem solving:

```json
// .mcp.json
{
  "mcpServers": {
    "sequential-thinking": {
      "command": "npx",
      "args": ["@modelcontextprotocol/server-sequential-thinking"]
    }
  }
}
```

**Use cases**:

- Complex architecture decisions
- Multi-step debugging
- Research planning
- Iterative refinement

**Stacking pattern**:

```
1. Sequential Thinking → Plan research path
2. Context7 → Fetch relevant documentation
3. Playwright → Validate in browser
4. Memory → Store findings
```

### Playwright MCP

Web automation using accessibility trees (not screenshots):

```json
// .mcp.json
{
  "mcpServers": {
    "playwright": {
      "command": "npx",
      "args": ["@anthropic-ai/mcp-server-playwright"]
    }
  }
}
```

**Capabilities**:

- Navigate websites
- Fill forms, click buttons
- Extract data
- Run E2E tests
- Debug visual issues

**Example prompts**:

```
"Go to localhost:3000, log in as 'testuser', verify dashboard loads"
"Fill the registration form with test data and submit"
"Extract all product prices from the catalog page"
```

### MCP Configuration

**Project-level** (`.mcp.json`):

```json
{
  "mcpServers": {
    "context7": {
      "command": "npx",
      "args": ["-y", "@upstash/context7-mcp"]
    },
    "github": {
      "command": "npx",
      "args": ["@modelcontextprotocol/server-github"],
      "env": {
        "GITHUB_TOKEN": "${GITHUB_TOKEN}"
      }
    },
    "sequential-thinking": {
      "command": "npx",
      "args": ["@modelcontextprotocol/server-sequential-thinking"]
    }
  }
}
```

**User-level** (`~/.claude.json`):

```json
{
  "mcpServers": {
    "memory": {
      "command": "npx",
      "args": ["@modelcontextprotocol/server-memory"],
      "scope": "user"
    }
  }
}
```

### Context Usage Management

MCP servers consume significant context:

| Server     | Typical Tokens | Tools |
| ---------- | -------------- | ----- |
| Playwright | ~13,600        | 21    |
| Omnisearch | ~14,200        | 20    |
| GitHub     | ~8,000         | 15    |
| Context7   | ~3,000         | 5     |

**Optimization strategies**:

1. **Limit active servers**: 5 servers work well; more degrades performance
2. **Clear between tasks**: Use `/clear` between unrelated work
3. **Monitor context**: Compact at 70% capacity
4. **Selective loading**: Only enable servers needed for current task

### Cloud Provider MCP Servers

**AWS**:

```json
{
  "mcpServers": {
    "aws-s3": {
      "command": "npx",
      "args": ["@anthropic-ai/mcp-server-aws-s3"],
      "env": {
        "AWS_ACCESS_KEY_ID": "${AWS_ACCESS_KEY_ID}",
        "AWS_SECRET_ACCESS_KEY": "${AWS_SECRET_ACCESS_KEY}"
      }
    }
  }
}
```

**Cloudflare** (16 specialized servers):

```json
{
  "mcpServers": {
    "cloudflare-workers": {
      "command": "npx",
      "args": ["@cloudflare/mcp-server-workers"],
      "env": {
        "CLOUDFLARE_API_TOKEN": "${CLOUDFLARE_API_TOKEN}"
      }
    }
  }
}
```

### Database MCP Servers

**PostgreSQL**:

```json
{
  "mcpServers": {
    "postgres": {
      "command": "npx",
      "args": ["@modelcontextprotocol/server-postgres"],
      "env": {
        "DATABASE_URL": "${DATABASE_URL}"
      }
    }
  }
}
```

**SQLite**:

```json
{
  "mcpServers": {
    "sqlite": {
      "command": "npx",
      "args": ["@modelcontextprotocol/server-sqlite", "--db", "./data.db"]
    }
  }
}
```

### Observability MCP Servers

**Sentry**:

```json
{
  "mcpServers": {
    "sentry": {
      "command": "npx",
      "args": ["@sentry/mcp-server"],
      "env": {
        "SENTRY_AUTH_TOKEN": "${SENTRY_AUTH_TOKEN}",
        "SENTRY_ORG": "my-org"
      }
    }
  }
}
```

**PostHog**:

```json
{
  "mcpServers": {
    "posthog": {
      "command": "npx",
      "args": ["@posthog/mcp-server"],
      "env": {
        "POSTHOG_API_KEY": "${POSTHOG_API_KEY}"
      }
    }
  }
}
```

### MCP Server Best Practices

**Start with pain points**:

```
1. Documentation outdated? → Context7
2. Need structured reasoning? → Sequential Thinking
3. Browser testing? → Playwright
4. Repository management? → GitHub
```

**Modular expansion**:

```
Week 1: Context7 (documentation)
Week 2: + Sequential Thinking (planning)
Week 3: + GitHub (PR workflow)
Week 4: + Playwright (E2E tests)
```

**Role-based selection**:

| Role       | Recommended Servers             |
| ---------- | ------------------------------- |
| Frontend   | Playwright, Figma, Context7     |
| Backend    | GitHub, PostgreSQL, Sentry      |
| DevOps     | AWS/Cloudflare, PostHog, GitHub |
| Full-stack | Context7, GitHub, Playwright    |

### DSM MCP Configuration

```json
// .mcp.json for data-source-manager
{
  "mcpServers": {
    "context7": {
      "command": "npx",
      "args": ["-y", "@upstash/context7-mcp"]
    },
    "sequential-thinking": {
      "command": "npx",
      "args": ["@modelcontextprotocol/server-sequential-thinking"]
    }
  }
}
```

**Usage patterns**:

```
# FCP debugging with sequential thinking
"Debug this FCP cache miss using sequential thinking"

# Polars documentation lookup
"Show me Polars group_by with rolling windows - use context7"

# Combined workflow
"Plan the FCP refactoring approach, then fetch current Polars best practices"
```

---

## Claude Agent SDK Reference

Build custom agents programmatically with Python and TypeScript SDKs.

### Installation

**Python**:

```bash
pip install claude-agent-sdk
export ANTHROPIC_API_KEY=your-api-key
```

**TypeScript**:

```bash
npm install @anthropic-ai/claude-agent-sdk
export ANTHROPIC_API_KEY=your-api-key
```

### Basic Usage

**Python**:

```python
import asyncio
from claude_agent_sdk import query, ClaudeAgentOptions

async def main():
    async for message in query(
        prompt="Analyze this codebase and suggest improvements",
        options=ClaudeAgentOptions(
            max_turns=10,
            setting_sources=["project"]  # Enable MCP, hooks, permissions
        )
    ):
        print(message)

asyncio.run(main())
```

**TypeScript**:

```typescript
import { query } from "@anthropic-ai/claude-agent-sdk";

for await (const message of query({
  prompt: "Analyze this codebase and suggest improvements",
  options: {
    maxTurns: 10,
    settingSources: ["project"], // Enable MCP, hooks, permissions
  },
})) {
  console.log(message);
}
```

### Available Hooks

| Hook Event           | Python | TypeScript | Trigger               | Use Case                 |
| -------------------- | ------ | ---------- | --------------------- | ------------------------ |
| `PreToolUse`         | Yes    | Yes        | Before tool executes  | Block dangerous commands |
| `PostToolUse`        | Yes    | Yes        | After tool returns    | Audit logging            |
| `PostToolUseFailure` | No     | Yes        | Tool execution fails  | Error handling           |
| `UserPromptSubmit`   | Yes    | Yes        | User prompt submitted | Inject context           |
| `Stop`               | Yes    | Yes        | Agent execution ends  | Save state               |
| `SubagentStart`      | No     | Yes        | Subagent spawned      | Track parallel tasks     |
| `SubagentStop`       | Yes    | Yes        | Subagent completes    | Aggregate results        |
| `PreCompact`         | Yes    | Yes        | Compaction requested  | Archive transcript       |
| `PermissionRequest`  | No     | Yes        | Permission prompt     | Custom approval          |
| `SessionStart`       | No     | Yes        | Session begins        | Initialize telemetry     |
| `SessionEnd`         | No     | Yes        | Session ends          | Cleanup resources        |
| `Notification`       | No     | Yes        | Agent status message  | Slack/PagerDuty alerts   |

### Hook Configuration

```python
from claude_agent_sdk import query, ClaudeAgentOptions, HookMatcher

async def protect_env_files(input_data, tool_use_id, context):
    file_path = input_data['tool_input'].get('file_path', '')
    if file_path.endswith('.env'):
        return {
            'hookSpecificOutput': {
                'hookEventName': input_data['hook_event_name'],
                'permissionDecision': 'deny',
                'permissionDecisionReason': 'Cannot modify .env files'
            }
        }
    return {}

async def main():
    async for message in query(
        prompt="Update configuration",
        options=ClaudeAgentOptions(
            hooks={
                'PreToolUse': [
                    HookMatcher(matcher='Write|Edit', hooks=[protect_env_files])
                ]
            }
        )
    ):
        print(message)
```

### Hook Callback Inputs

**Common fields** (all hooks):

| Field             | Type   | Description                  |
| ----------------- | ------ | ---------------------------- |
| `hook_event_name` | string | Hook type (PreToolUse, etc.) |
| `session_id`      | string | Current session ID           |
| `transcript_path` | string | Path to transcript           |
| `cwd`             | string | Current working directory    |

**PreToolUse/PostToolUse fields**:

| Field           | Type   | Description                    |
| --------------- | ------ | ------------------------------ |
| `tool_name`     | string | Name of the tool               |
| `tool_input`    | object | Tool arguments                 |
| `tool_response` | any    | Tool result (PostToolUse only) |

### Hook Callback Outputs

**Top-level fields**:

| Field            | Type    | Description                        |
| ---------------- | ------- | ---------------------------------- |
| `continue`       | boolean | Continue execution (default: true) |
| `stopReason`     | string  | Message when continue=false        |
| `suppressOutput` | boolean | Hide stdout from transcript        |
| `systemMessage`  | string  | Message injected for Claude        |

**hookSpecificOutput fields**:

| Field                      | Type                       | Description                         |
| -------------------------- | -------------------------- | ----------------------------------- |
| `hookEventName`            | string                     | Required. Use input.hook_event_name |
| `permissionDecision`       | 'allow' \| 'deny' \| 'ask' | Tool execution control              |
| `permissionDecisionReason` | string                     | Explanation for decision            |
| `updatedInput`             | object                     | Modified tool input                 |
| `additionalContext`        | string                     | Context added to conversation       |

### Hook Patterns

**Block dangerous commands**:

```python
async def block_dangerous(input_data, tool_use_id, context):
    command = input_data['tool_input'].get('command', '')
    if 'rm -rf /' in command or 'drop database' in command.lower():
        return {
            'hookSpecificOutput': {
                'hookEventName': input_data['hook_event_name'],
                'permissionDecision': 'deny',
                'permissionDecisionReason': 'Dangerous command blocked'
            }
        }
    return {}
```

**Auto-approve read-only tools**:

```python
async def auto_approve_readonly(input_data, tool_use_id, context):
    read_only = ['Read', 'Glob', 'Grep', 'LS']
    if input_data['tool_name'] in read_only:
        return {
            'hookSpecificOutput': {
                'hookEventName': input_data['hook_event_name'],
                'permissionDecision': 'allow',
                'permissionDecisionReason': 'Read-only auto-approved'
            }
        }
    return {}
```

**Redirect to sandbox**:

```python
async def redirect_sandbox(input_data, tool_use_id, context):
    if input_data['tool_name'] == 'Write':
        original = input_data['tool_input'].get('file_path', '')
        return {
            'hookSpecificOutput': {
                'hookEventName': input_data['hook_event_name'],
                'permissionDecision': 'allow',
                'updatedInput': {
                    **input_data['tool_input'],
                    'file_path': f'/sandbox{original}'
                }
            }
        }
    return {}
```

**Webhook notifications** (TypeScript):

```typescript
const webhookNotifier: HookCallback = async (input, toolUseID, { signal }) => {
  if (input.hook_event_name !== "PostToolUse") return {};

  await fetch("https://api.example.com/webhook", {
    method: "POST",
    body: JSON.stringify({
      tool: (input as PostToolUseHookInput).tool_name,
      timestamp: new Date().toISOString(),
    }),
    signal, // Cancel on hook timeout
  });

  return {};
};
```

### Permission Decision Flow

Evaluation order:

1. **Deny** rules checked first (any match = blocked)
2. **Ask** rules checked second
3. **Allow** rules checked third
4. **Default to Ask** if nothing matches

Multiple hooks: If any hook returns `deny`, operation blocked.

### Subagents

Define specialized subagents with isolated context:

```python
from claude_agent_sdk import query, ClaudeAgentOptions, Subagent

# Read-only analysis agent
analysis_agent = Subagent(
    name="analyzer",
    instructions="Analyze code patterns without making changes",
    tools=['Read', 'Glob', 'Grep']  # Restricted tool access
)

async def main():
    async for message in query(
        prompt="Use the analyzer agent to review this codebase",
        options=ClaudeAgentOptions(
            subagents=[analysis_agent]
        )
    ):
        print(message)
```

**Subagent characteristics**:

- Isolated context window
- Can have restricted tools
- Transcripts persist (default 30 days)
- Can be resumed via session ID
- Do NOT inherit parent permissions automatically

### Sessions

**Resume a session**:

```python
# First query - capture session ID
session_id = None
async for message in query(prompt="Start analysis"):
    if hasattr(message, 'session_id'):
        session_id = message.session_id
    print(message)

# Second query - resume session
async for message in query(
    prompt="Continue the analysis",
    options=ClaudeAgentOptions(resume=session_id)
):
    print(message)
```

### MCP Integration

MCP tools follow naming pattern: `mcp__<server>__<action>`

```python
# Match all Playwright MCP tools
HookMatcher(matcher='^mcp__playwright__', hooks=[playwright_hook])

# Match specific MCP action
HookMatcher(matcher='mcp__github__create_pr', hooks=[pr_hook])
```

### Custom Tools

Implement in-process MCP servers:

```python
from claude_agent_sdk import query, ClaudeAgentOptions, CustomTool

def fetch_weather(location: str) -> dict:
    """Fetch current weather for a location."""
    return {"temp": 72, "conditions": "sunny", "location": location}

weather_tool = CustomTool(
    name="get_weather",
    description="Get current weather for a location",
    input_schema={
        "type": "object",
        "properties": {
            "location": {"type": "string", "description": "City name"}
        },
        "required": ["location"]
    },
    handler=fetch_weather
)

async for message in query(
    prompt="What's the weather in San Francisco?",
    options=ClaudeAgentOptions(custom_tools=[weather_tool])
):
    print(message)
```

### Permission Modes

```python
# Bypass all permissions (dangerous!)
options = ClaudeAgentOptions(bypass_permissions=True)

# Note: bypassPermissions inherited by all subagents
# Cannot be overridden by subagents
```

### Third-Party API Providers

**Amazon Bedrock**:

```bash
export CLAUDE_CODE_USE_BEDROCK=1
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
export AWS_REGION=us-east-1
```

**Google Vertex AI**:

```bash
export CLAUDE_CODE_USE_VERTEX=1
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/credentials.json
export VERTEX_PROJECT_ID=your-project
export VERTEX_REGION=us-central1
```

### Configuration Options

| Option               | Python               | TypeScript          | Default   | Description                |
| -------------------- | -------------------- | ------------------- | --------- | -------------------------- |
| `max_turns`          | `max_turns`          | `maxTurns`          | unlimited | Maximum API round-trips    |
| `setting_sources`    | `setting_sources`    | `settingSources`    | []        | Enable project settings    |
| `hooks`              | `hooks`              | `hooks`             | {}        | Hook configurations        |
| `subagents`          | `subagents`          | `subagents`         | []        | Subagent definitions       |
| `custom_tools`       | `custom_tools`       | `customTools`       | []        | Custom tool definitions    |
| `resume`             | `resume`             | `resume`            | None      | Session ID to resume       |
| `bypass_permissions` | `bypass_permissions` | `bypassPermissions` | false     | Skip all permission checks |

### DSM Agent SDK Integration

```python
from claude_agent_sdk import query, ClaudeAgentOptions, HookMatcher, Subagent

# FCP validation subagent
fcp_validator = Subagent(
    name="fcp-validator",
    instructions="""
    Validate FCP cache behavior:
    1. Check cache state in ~/.cache/dsm/
    2. Verify staleness thresholds
    3. Report any anomalies
    """,
    tools=['Read', 'Glob', 'Grep', 'Bash']
)

# Hook to prevent accidental cache deletion
async def protect_cache(input_data, tool_use_id, context):
    command = input_data['tool_input'].get('command', '')
    if 'rm' in command and '.cache/dsm' in command:
        return {
            'hookSpecificOutput': {
                'hookEventName': input_data['hook_event_name'],
                'permissionDecision': 'deny',
                'permissionDecisionReason': 'Use mise run cache:clear instead'
            }
        }
    return {}

async def main():
    async for message in query(
        prompt="Validate FCP cache state for BTCUSDT",
        options=ClaudeAgentOptions(
            subagents=[fcp_validator],
            hooks={
                'PreToolUse': [
                    HookMatcher(matcher='Bash', hooks=[protect_cache])
                ]
            },
            setting_sources=['project']
        )
    ):
        print(message)
```

---

## Custom Tool Implementation Reference

Build and integrate custom tools with Claude's tool system.

### Tool Definition Schema

```python
{
    "name": "get_weather",  # ^[a-zA-Z0-9_-]{1,64}$
    "description": "Get the current weather in a given location. "
                   "Returns temperature and conditions. "
                   "Use when user asks about weather.",
    "input_schema": {
        "type": "object",
        "properties": {
            "location": {
                "type": "string",
                "description": "City and state, e.g. San Francisco, CA"
            },
            "unit": {
                "type": "string",
                "enum": ["celsius", "fahrenheit"],
                "description": "Temperature unit"
            }
        },
        "required": ["location"]
    }
}
```

**Best practices for descriptions**:

- Explain what the tool does
- Describe when it should be used
- Document what each parameter means
- List limitations and caveats
- Aim for 3-4+ sentences per tool

### Tool Runner (Beta)

Automatic tool execution without manual loop handling:

**Python with @beta_tool decorator**:

```python
import anthropic
import json
from anthropic import beta_tool

client = anthropic.Anthropic()

@beta_tool
def get_weather(location: str, unit: str = "fahrenheit") -> str:
    """Get the current weather in a given location.

    Args:
        location: The city and state, e.g. San Francisco, CA
        unit: Temperature unit, either 'celsius' or 'fahrenheit'
    """
    # Implementation
    return json.dumps({"temperature": "20°C", "condition": "Sunny"})

@beta_tool
def calculate_sum(a: int, b: int) -> str:
    """Add two numbers together.

    Args:
        a: First number
        b: Second number
    """
    return str(a + b)

# Tool runner automatically handles the loop
runner = client.beta.messages.tool_runner(
    model="claude-sonnet-4-5",
    max_tokens=1024,
    tools=[get_weather, calculate_sum],
    messages=[
        {"role": "user", "content": "What's the weather in Paris? Also, what's 15 + 27?"}
    ]
)
for message in runner:
    print(message.content[0].text)

# Or get final message directly
final_message = runner.until_done()
```

**TypeScript with Zod**:

```typescript
import { Anthropic } from "@anthropic-ai/sdk";
import { betaZodTool } from "@anthropic-ai/sdk/helpers/beta/zod";
import { z } from "zod";

const anthropic = new Anthropic();

const getWeatherTool = betaZodTool({
  name: "get_weather",
  description: "Get the current weather in a given location",
  inputSchema: z.object({
    location: z.string().describe("The city and state, e.g. San Francisco, CA"),
    unit: z.enum(["celsius", "fahrenheit"]).default("fahrenheit"),
  }),
  run: async (input) => {
    return JSON.stringify({ temperature: "20°C", condition: "Sunny" });
  },
});

const runner = anthropic.beta.messages.toolRunner({
  model: "claude-sonnet-4-5",
  max_tokens: 1024,
  tools: [getWeatherTool],
  messages: [{ role: "user", content: "What's the weather like in Paris?" }],
});

for await (const message of runner) {
  console.log(message.content[0].text);
}
```

### Parallel Tool Calls

Claude can execute multiple tools simultaneously:

```python
# Claude's response with parallel tool calls
{
  "role": "assistant",
  "content": [
    {"type": "text", "text": "I'll check both cities."},
    {"type": "tool_use", "id": "toolu_01", "name": "get_weather", "input": {"location": "SF"}},
    {"type": "tool_use", "id": "toolu_02", "name": "get_weather", "input": {"location": "NYC"}},
    {"type": "tool_use", "id": "toolu_03", "name": "get_time", "input": {"timezone": "America/Los_Angeles"}},
    {"type": "tool_use", "id": "toolu_04", "name": "get_time", "input": {"timezone": "America/New_York"}}
  ]
}

# Provide ALL results in a SINGLE user message
{
  "role": "user",
  "content": [
    {"type": "tool_result", "tool_use_id": "toolu_01", "content": "68°F, sunny"},
    {"type": "tool_result", "tool_use_id": "toolu_02", "content": "45°F, cloudy"},
    {"type": "tool_result", "tool_use_id": "toolu_03", "content": "2:30 PM PST"},
    {"type": "tool_result", "tool_use_id": "toolu_04", "content": "5:30 PM EST"}
  ]
}
```

**Critical**: All tool results MUST be in a single user message. Separate messages reduce parallel tool use.

**Maximize parallel tool use** with prompting:

```text
<use_parallel_tool_calls>
For maximum efficiency, whenever you perform multiple independent operations,
invoke all relevant tools simultaneously rather than sequentially.
Prioritize calling tools in parallel whenever possible.
</use_parallel_tool_calls>
```

### Tool Choice Options

| Value  | Behavior                            |
| ------ | ----------------------------------- |
| `auto` | Claude decides (default with tools) |
| `any`  | Must use one of the provided tools  |
| `tool` | Must use specific tool              |
| `none` | Cannot use any tools                |

```python
# Force specific tool
response = client.messages.create(
    model="claude-sonnet-4-5",
    max_tokens=1024,
    tools=tools,
    tool_choice={"type": "tool", "name": "get_weather"},
    messages=[{"role": "user", "content": "Tell me about Paris"}]
)

# Disable parallel tool use
response = client.messages.create(
    model="claude-sonnet-4-5",
    max_tokens=1024,
    tools=tools,
    tool_choice={"type": "auto", "disable_parallel_tool_use": True},
    messages=[{"role": "user", "content": "Check weather in SF and NYC"}]
)
```

### Error Handling

**Tool execution errors**:

```python
# Return error with is_error flag
{
    "type": "tool_result",
    "tool_use_id": "toolu_01",
    "content": "ConnectionError: weather service unavailable",
    "is_error": True
}
```

**Missing parameters**:

```python
{
    "type": "tool_result",
    "tool_use_id": "toolu_01",
    "content": "Error: Missing required 'location' parameter",
    "is_error": True
}
```

Claude retries 2-3 times with corrections before apologizing.

**Tool runner error handling**:

```python
for message in runner:
    tool_response = runner.generate_tool_call_response()
    if tool_response:
        for block in tool_response.content:
            if block.is_error:
                raise RuntimeError(f"Tool failed: {block.content}")
```

**Debug with logging**:

```bash
export ANTHROPIC_LOG=debug  # Full stack traces
export ANTHROPIC_LOG=info   # Tool errors only
```

### Tool Result Formatting

**Text result**:

```python
{"type": "tool_result", "tool_use_id": "toolu_01", "content": "15 degrees"}
```

**Image result**:

```python
{
    "type": "tool_result",
    "tool_use_id": "toolu_01",
    "content": [
        {"type": "text", "text": "Weather chart:"},
        {
            "type": "image",
            "source": {
                "type": "base64",
                "media_type": "image/jpeg",
                "data": "/9j/4AAQSkZJRg..."
            }
        }
    ]
}
```

**Document result**:

```python
{
    "type": "tool_result",
    "tool_use_id": "toolu_01",
    "content": [
        {
            "type": "document",
            "source": {
                "type": "text",
                "media_type": "text/plain",
                "data": "15 degrees"
            }
        }
    ]
}
```

### Tool Use Examples (Beta)

Provide concrete examples for complex tools:

```python
response = client.messages.create(
    model="claude-sonnet-4-5",
    max_tokens=1024,
    betas=["advanced-tool-use-2025-11-20"],
    tools=[{
        "name": "query_database",
        "description": "Execute SQL query against the database",
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {"type": "string"},
                "timeout": {"type": "integer"}
            },
            "required": ["query"]
        },
        "input_examples": [
            {"query": "SELECT * FROM users WHERE active = true", "timeout": 30},
            {"query": "SELECT COUNT(*) FROM orders WHERE date > '2024-01-01'"},
            {"query": "SELECT u.name, o.total FROM users u JOIN orders o ON u.id = o.user_id"}
        ]
    }],
    messages=[{"role": "user", "content": "Find all premium users"}]
)
```

### Strict Tool Use

Guarantee schema validation with `strict: true`:

```python
tools = [{
    "name": "search_flights",
    "strict": True,  # Enable strict mode
    "input_schema": {
        "type": "object",
        "properties": {
            "destination": {"type": "string"},
            "passengers": {"type": "integer", "enum": [1, 2, 3, 4, 5, 6]}
        },
        "required": ["destination"],
        "additionalProperties": False
    }
}]
```

**Without strict**: Claude might return `passengers: "two"`.
**With strict**: Always returns `passengers: 2` (validated integer).

### Stop Reasons

| Stop Reason  | Meaning                     | Action                     |
| ------------ | --------------------------- | -------------------------- |
| `end_turn`   | Claude finished naturally   | Process final response     |
| `tool_use`   | Claude wants to use tool(s) | Execute tools, continue    |
| `max_tokens` | Hit token limit             | Retry with higher limit    |
| `pause_turn` | Long-running turn paused    | Continue with same content |

### DSM Custom Tool Example

```python
import anthropic
import json
from anthropic import beta_tool
from data_source_manager import DataSourceManager

client = anthropic.Anthropic()
dsm = DataSourceManager()

@beta_tool
def fetch_ohlcv(symbol: str, timeframe: str = "1h", limit: int = 100) -> str:
    """Fetch OHLCV candlestick data for a trading symbol.

    Args:
        symbol: Trading pair symbol, e.g. BTCUSDT
        timeframe: Candle timeframe (1m, 5m, 15m, 1h, 4h, 1d)
        limit: Number of candles to fetch (max 1000)
    """
    try:
        df = dsm.fetch_ohlcv(symbol=symbol, timeframe=timeframe, limit=limit)
        return json.dumps({
            "symbol": symbol,
            "timeframe": timeframe,
            "rows": len(df),
            "columns": df.columns.tolist(),
            "sample": df.head(3).to_dicts()
        })
    except Exception as e:
        return json.dumps({"error": str(e)})

@beta_tool
def check_fcp_status(symbol: str) -> str:
    """Check Failover Control Protocol status for a symbol.

    Args:
        symbol: Trading pair symbol to check FCP status for
    """
    status = dsm.get_fcp_status(symbol)
    return json.dumps({
        "symbol": symbol,
        "decision": status.decision.value,
        "cache_hit": status.cache_hit,
        "source": status.source_used,
        "staleness_seconds": status.staleness_seconds
    })

# Use tools with DSM
runner = client.beta.messages.tool_runner(
    model="claude-sonnet-4-5",
    max_tokens=2048,
    tools=[fetch_ohlcv, check_fcp_status],
    messages=[
        {"role": "user", "content": "Get the latest 50 1h candles for BTCUSDT and check FCP status"}
    ]
)

for message in runner:
    print(message.content[0].text)
```
---

## Enterprise Deployment Reference

Deploy Claude Code across teams with cloud providers and managed settings.

### Deployment Options Comparison

| Feature    | Teams/Enterprise        | Anthropic Console | AWS Bedrock     | Google Vertex   | Microsoft Foundry   |
| ---------- | ----------------------- | ----------------- | --------------- | --------------- | ------------------- |
| Best for   | Most orgs (recommended) | Individual devs   | AWS-native      | GCP-native      | Azure-native        |
| Billing    | $150/seat (Premium)     | PAYG              | PAYG via AWS    | PAYG via GCP    | PAYG via Azure      |
| Auth       | SSO or email            | API key           | AWS creds       | GCP creds       | Entra ID or API key |
| Web Claude | Yes                     | No                | No              | No              | No                  |
| Enterprise | SSO, usage monitoring   | None              | IAM, CloudTrail | IAM, Audit Logs | RBAC, Azure Monitor |

### Claude for Teams

Self-service with collaboration features:

- Team management and billing
- Usage analytics dashboard
- Admin seat allocation
- Shared CLAUDE.md configuration

**Best for**: Smaller teams needing quick setup.

### Claude for Enterprise

Advanced controls for larger organizations:

- SSO and domain capture
- Role-based permissions
- Compliance API access
- Managed policy settings
- Granular spend controls

**Best for**: Organizations with security/compliance requirements.

### Amazon Bedrock Configuration

<!-- SSoT-OK: Environment variable examples for Claude Code documentation -->

```bash
# Enable Bedrock
export CLAUDE_CODE_USE_BEDROCK=1
export AWS_REGION=us-east-1

# AWS credentials (choose one method)
# Option 1: Environment variables
export AWS_ACCESS_KEY_ID=your-key-id
export AWS_SECRET_ACCESS_KEY=your-secret-key

# Option 2: AWS SSO
aws sso login --profile your-profile
export AWS_PROFILE=your-profile

# Option 3: IAM role (EC2/ECS)
# Automatic via instance metadata

# Corporate proxy (optional)
export HTTPS_PROXY='https://proxy.example.com:8080'

# LLM Gateway (optional)
export ANTHROPIC_BEDROCK_BASE_URL='https://your-llm-gateway.com/bedrock'
export CLAUDE_CODE_SKIP_BEDROCK_AUTH=1  # If gateway handles auth
```

**Benefits**: VPC integration, IAM policies, CloudTrail logging, regional deployment.

### Google Vertex AI Configuration

<!-- SSoT-OK: Environment variable examples for Claude Code documentation -->

```bash
# Enable Vertex
export CLAUDE_CODE_USE_VERTEX=1
export CLOUD_ML_REGION=us-east5
export ANTHROPIC_VERTEX_PROJECT_ID=your-project-id

# Authentication
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/credentials.json

# Corporate proxy (optional)
export HTTPS_PROXY='https://proxy.example.com:8080'

# LLM Gateway (optional)
export ANTHROPIC_VERTEX_BASE_URL='https://your-llm-gateway.com/vertex'
export CLAUDE_CODE_SKIP_VERTEX_AUTH=1  # If gateway handles auth
```

**Benefits**: GCP IAM, Cloud Audit Logs, regional compliance.

### Microsoft Foundry Configuration

<!-- SSoT-OK: Environment variable examples for Claude Code documentation -->

```bash
# Enable Microsoft Foundry
export CLAUDE_CODE_USE_FOUNDRY=1
export ANTHROPIC_FOUNDRY_RESOURCE=your-resource

# Authentication (choose one)
export ANTHROPIC_FOUNDRY_API_KEY=your-api-key  # API key
# Or omit for Entra ID auth (automatic with Azure CLI login)

# Corporate proxy (optional)
export HTTPS_PROXY='https://proxy.example.com:8080'

# LLM Gateway (optional)
export ANTHROPIC_FOUNDRY_BASE_URL='https://your-llm-gateway.com'
export CLAUDE_CODE_SKIP_FOUNDRY_AUTH=1  # If gateway handles auth
```

**Benefits**: Azure RBAC, Azure Monitor, Entra ID integration.

### LLM Gateway Configuration

Centralized routing for usage tracking and authentication:

<!-- SSoT-OK: Environment variable examples for Claude Code documentation -->

```bash
# Direct Anthropic API with gateway
export ANTHROPIC_BASE_URL='https://your-llm-gateway.com/anthropic'

# Bedrock with gateway
export ANTHROPIC_BEDROCK_BASE_URL='https://your-llm-gateway.com/bedrock'
export CLAUDE_CODE_SKIP_BEDROCK_AUTH=1

# Vertex with gateway
export ANTHROPIC_VERTEX_BASE_URL='https://your-llm-gateway.com/vertex'
export CLAUDE_CODE_SKIP_VERTEX_AUTH=1

# Foundry with gateway
export ANTHROPIC_FOUNDRY_BASE_URL='https://your-llm-gateway.com/foundry'
export CLAUDE_CODE_SKIP_FOUNDRY_AUTH=1
```

**Use cases**:

- Centralized usage tracking across teams
- Custom rate limiting and budgets
- Centralized authentication management
- Cost allocation and chargeback

### Managed Permissions

Deploy organization-wide settings that cannot be overridden:

**macOS**: `/Library/Application Support/ClaudeCode/`
**Linux/WSL**: `/etc/claude-code/`
**Windows**: `C:\Program Files\ClaudeCode\`

<!-- SSoT-OK: JSON config example for Claude Code documentation -->

**managed-settings.json**:

```json
{
  "permissions": {
    "deny": [
      "Bash(rm -rf *)",
      "Bash(git push --force *)",
      "Read(.env*)",
      "Write(.env*)"
    ],
    "allow": ["Bash(git *)", "Bash(npm *)", "Bash(uv *)"]
  },
  "env": {
    "CLAUDE_CODE_USE_BEDROCK": "true",
    "AWS_REGION": "us-east-1"
  }
}
```

**managed-mcp.json** (organization MCP servers):

```json
{
  "mcpServers": {
    "company-docs": {
      "command": "npx",
      "args": ["@company/mcp-docs-server"],
      "env": {
        "DOCS_API_KEY": "${COMPANY_DOCS_API_KEY}"
      }
    }
  }
}
```

### Admin Controls

| Control         | Capability                            |
| --------------- | ------------------------------------- |
| Seat management | Purchase, allocate, provision users   |
| Spend controls  | Organization and user-level limits    |
| Usage analytics | Lines accepted, accept rate, patterns |
| Policy settings | Tool permissions, file access, MCP    |

### SSO and Domain Capture

**Enterprise SSO flow**:

1. Admin configures SSO in enterprise console
2. User runs `claude` and is redirected to IdP
3. IdP authenticates against corporate directory
4. Claude Code receives token tied to org account
5. Managed settings automatically applied

**Domain capture**: Users with company email automatically join organization.

### Onboarding Best Practices

1. **One-click install**: Create internal installation script
2. **Shared CLAUDE.md**: Deploy organization standards to repos
3. **MCP configuration**: Central team maintains `.mcp.json`
4. **Guided usage**: Start with Q&A and smaller tasks

**Onboarding checklist**:

```markdown
- [ ] Admin provisions Claude Code premium seat
- [ ] User installs Claude Code CLI
- [ ] User authenticates via SSO
- [ ] CLAUDE.md in repo provides project context
- [ ] MCP servers configured for integrations
- [ ] User completes intro training
```

### Compliance Features

| Feature        | Description                             |
| -------------- | --------------------------------------- |
| Audit logs     | All tool calls logged with timestamps   |
| Data residency | Regional deployment via cloud providers |
| Access control | RBAC with IAM policies                  |
| Encryption     | TLS in transit, at-rest encryption      |
| Compliance API | Programmatic access to usage data       |

### Verify Configuration

```bash
# Check current configuration
/status

# Verify Bedrock connection
claude --version
echo "BEDROCK: $CLAUDE_CODE_USE_BEDROCK"
echo "REGION: $AWS_REGION"

# Verify Vertex connection
echo "VERTEX: $CLAUDE_CODE_USE_VERTEX"
echo "PROJECT: $ANTHROPIC_VERTEX_PROJECT_ID"

# Test proxy connectivity
curl -x $HTTPS_PROXY https://api.anthropic.com/health
```

### DSM Enterprise Configuration

<!-- SSoT-OK: mise.toml config example for Claude Code documentation -->

```toml
# .mise.toml for enterprise DSM deployment
[env]
CLAUDE_CODE_USE_BEDROCK = "true"
AWS_REGION = "us-east-1"
AWS_PROFILE = "dsm-production"

# Or for Vertex
# CLAUDE_CODE_USE_VERTEX = "true"
# CLOUD_ML_REGION = "us-central1"
# ANTHROPIC_VERTEX_PROJECT_ID = "dsm-prod-project"
```

**DSM managed permissions** (managed-settings.json):

```json
{
  "permissions": {
    "deny": [
      "Bash(rm -rf ~/.cache/dsm/*)",
      "Write(.env*)",
      "Bash(pip install *)"
    ],
    "allow": ["Bash(uv run *)", "Bash(mise run *)", "Read(~/.cache/dsm/*)"]
  }
}
```
---

## Plugin Development Reference

Build and distribute Claude Code plugins with skills, commands, hooks, and MCP servers.

### Plugin Directory Structure

**Basic plugin**:

```
my-plugin/
├── .claude-plugin/
│   └── plugin.json       # Plugin manifest
├── skills/
│   └── my-skill/
│       └── SKILL.md      # Skill definition
└── README.md
```

**Full-featured plugin**:

```
database-migration-plugin/
├── .claude-plugin/
│   └── plugin.json       # Plugin manifest
├── .mcp.json             # MCP server configuration
├── agents/
│   └── migration-planner.md
├── commands/
│   └── migrate.md
├── skills/
│   └── migration-best-practices/
│       └── SKILL.md
├── hooks/
│   └── hooks.json
└── scripts/
    └── migration-check.sh
```

### Plugin Manifest (plugin.json)

<!-- SSoT-OK: Example plugin manifest with version placeholders -->
```json
{
  "name": "my-plugin",
  "version": "<version>",
  "description": "Description of what the plugin does",
  "author": {
    "name": "Your Name",
    "email": "you@example.com"
  },
  "homepage": "https://github.com/you/my-plugin",
  "repository": "https://github.com/you/my-plugin",
  "license": "MIT",
  "keywords": ["automation", "testing"]
}
```

**Required fields**:

| Field | Description |
|-------|-------------|
| `name` | Plugin identifier (kebab-case, no spaces) |

**Optional fields**:

| Field | Description |
|-------|-------------|
| `version` | Semantic version |
| `description` | Brief description |
| `author` | Author info (`name`, `email`) |
| `homepage` | Documentation URL |
| `repository` | Source code URL |
| `license` | SPDX identifier (MIT, Apache-2.0) |
| `keywords` | Discovery tags |

### Skill Definition (SKILL.md)

```markdown
---

description: Review code for bugs, security, and performance
disable-model-invocation: true

---

Review the code I've selected or the recent changes for:

- Potential bugs or edge cases
- Security concerns
- Performance issues
- Readability improvements

Be concise and actionable.

````

**Frontmatter options**:

| Field | Type | Description |
|-------|------|-------------|
| `name` | string | Skill name (defaults to directory name) |
| `description` | string | When to use this skill |
| `disable-model-invocation` | boolean | Run without API call |
| `user-invocable` | boolean | Show in /help |
| `context` | string | `fork` for isolated context |
| `agent` | string | Subagent type (Explore, Plan) |

### Command Definition

**commands/migrate.md**:

```markdown
---
name: migrate
description: Run database migration
allowed-tools:
  - Bash
  - Read
  - Write
---

# Database Migration

Execute the migration for the specified version.

## Steps

1. Check current schema version
2. Validate migration files
3. Apply migrations in order
4. Verify schema state
````

### Hook Configuration

**hooks/hooks.json**:

```json
{
  "hooks": {
    "PostToolUse": [
      {
        "matcher": "Write|Edit",
        "hooks": [
          {
            "type": "command",
            "command": "${CLAUDE_PLUGIN_ROOT}/scripts/validate.sh"
          }
        ]
      }
    ],
    "PreToolUse": [
      {
        "matcher": "Bash",
        "hooks": [
          {
            "type": "command",
            "command": "${CLAUDE_PLUGIN_ROOT}/scripts/check-command.sh"
          }
        ]
      }
    ]
  }
}
```

**Note**: Use `${CLAUDE_PLUGIN_ROOT}` to reference files within the plugin directory.

### MCP Server Configuration

**.mcp.json**:

```json
{
  "mcpServers": {
    "plugin-db": {
      "command": "${CLAUDE_PLUGIN_ROOT}/servers/db-server",
      "args": ["--config", "${CLAUDE_PLUGIN_ROOT}/config.json"],
      "env": {
        "DB_URL": "${DATABASE_URL}"
      }
    }
  }
}
```

### Marketplace Structure

```
my-marketplace/
├── .claude-plugin/
│   └── marketplace.json    # Marketplace manifest
├── plugins/
│   ├── plugin-a/
│   │   ├── .claude-plugin/
│   │   │   └── plugin.json
│   │   └── skills/
│   └── plugin-b/
│       └── ...
├── CHANGELOG.md
└── README.md
```

### Marketplace Manifest (marketplace.json)

<!-- SSoT-OK: Example marketplace manifest with version placeholders -->

```json
{
  "name": "company-tools",
  "owner": {
    "name": "DevTools Team",
    "email": "devtools@example.com"
  },
  "metadata": {
    "description": "Internal development tools",
    "version": "<version>",
    "pluginRoot": "./plugins"
  },
  "plugins": [
    {
      "name": "code-formatter",
      "source": "./plugins/formatter",
      "description": "Automatic code formatting",
      "version": "<version>"
    },
    {
      "name": "deployment-tools",
      "source": {
        "source": "github",
        "repo": "company/deploy-plugin"
      },
      "description": "Deployment automation"
    }
  ]
}
```

**Required fields**:

| Field     | Description                         |
| --------- | ----------------------------------- |
| `name`    | Marketplace identifier (kebab-case) |
| `owner`   | Maintainer info (name required)     |
| `plugins` | Array of plugin entries             |

**Reserved names** (cannot use):

- claude-code-marketplace
- claude-code-plugins
- claude-plugins-official
- anthropic-marketplace
- anthropic-plugins
- agent-skills

### Plugin Source Types

**Relative path** (same repository):

```json
{
  "name": "my-plugin",
  "source": "./plugins/my-plugin"
}
```

**GitHub repository**:

<!-- SSoT-OK: Example GitHub source with version tag placeholder -->

```json
{
  "name": "github-plugin",
  "source": {
    "source": "github",
    "repo": "owner/plugin-repo",
    "ref": "v<version>",
    "sha": "a1b2c3d4e5f6..."
  }
}
```

**Git URL**:

```json
{
  "name": "git-plugin",
  "source": {
    "source": "url",
    "url": "https://gitlab.com/team/plugin.git",
    "ref": "main"
  }
}
```

### Installation Commands

```bash
# Add marketplace
/plugin marketplace add owner/repo
/plugin marketplace add https://gitlab.com/company/plugins.git
/plugin marketplace add ./local-marketplace

# Install plugin
/plugin install my-plugin@marketplace-name

# Update marketplace
/plugin marketplace update

# Validate plugin
/plugin validate .
claude plugin validate .
```

### Project Integration

**.claude/settings.json** (auto-prompt for marketplace):

```json
{
  "extraKnownMarketplaces": {
    "company-tools": {
      "source": {
        "source": "github",
        "repo": "your-org/claude-plugins"
      }
    }
  },
  "enabledPlugins": {
    "code-formatter@company-tools": true,
    "deployment-tools@company-tools": true
  }
}
```

### Managed Marketplace Restrictions

**Disable all marketplace additions**:

```json
{
  "strictKnownMarketplaces": []
}
```

**Allow specific marketplaces only**:

```json
{
  "strictKnownMarketplaces": [
    {
      "source": "github",
      "repo": "acme-corp/approved-plugins"
    },
    {
      "source": "url",
      "url": "https://plugins.example.com/marketplace.json"
    }
  ]
}
```

**Allow by host pattern**:

```json
{
  "strictKnownMarketplaces": [
    {
      "source": "hostPattern",
      "hostPattern": "^github\\.example\\.com$"
    }
  ]
}
```

### Private Repository Authentication

**Environment variables**:

| Provider  | Variables                    |
| --------- | ---------------------------- |
| GitHub    | `GITHUB_TOKEN` or `GH_TOKEN` |
| GitLab    | `GITLAB_TOKEN` or `GL_TOKEN` |
| Bitbucket | `BITBUCKET_TOKEN`            |

```bash
export GITHUB_TOKEN=ghp_xxxxxxxxxxxxxxxxxxxx
```

### Plugin Caching

Plugins are copied to a cache directory when installed:

- Files outside plugin directory won't be copied
- Use `${CLAUDE_PLUGIN_ROOT}` in configs
- Symlinks are followed during copying

**Workarounds for shared files**:

- Use symlinks inside plugin directory
- Restructure to include shared files in plugin

### Validation and Testing

```bash
# Validate marketplace syntax
claude plugin validate .
/plugin validate .

# Add for testing
/plugin marketplace add ./path/to/marketplace

# Install test plugin
/plugin install test-plugin@marketplace-name

# Check for issues
/plugin list
```

### Common Issues

| Issue               | Cause                 | Solution                          |
| ------------------- | --------------------- | --------------------------------- |
| Plugin not found    | Missing plugin.json   | Create .claude-plugin/plugin.json |
| Relative paths fail | URL-based marketplace | Use GitHub/git sources            |
| Files not found     | Plugin caching        | Use ${CLAUDE_PLUGIN_ROOT}         |
| Auth fails          | Missing token         | Set GITHUB_TOKEN env var          |

### DSM Plugin Example

**dsm-tools plugin structure**:

```
dsm-tools/
├── .claude-plugin/
│   └── plugin.json
├── skills/
│   ├── fcp-debugging/
│   │   └── SKILL.md
│   └── ohlcv-validation/
│       └── SKILL.md
├── commands/
│   ├── debug-fcp.md
│   └── validate-ohlcv.md
├── hooks/
│   └── hooks.json
└── scripts/
    └── check-cache.sh
```

<!-- SSoT-OK: Example DSM plugin manifest with version placeholder -->

**plugin.json**:

```json
{
  "name": "dsm-tools",
  "version": "<version>",
  "description": "DataSourceManager development tools",
  "author": {
    "name": "DSM Team"
  },
  "keywords": ["dsm", "fcp", "ohlcv", "trading"]
}
```

**skills/fcp-debugging/SKILL.md**:

```markdown
---
description: Debug FCP cache behavior for trading symbols
user-invocable: true
---

# FCP Debugging Skill

Debug the Failover Control Protocol for the specified symbol.

## Steps

1. Check cache state in ~/.cache/dsm/
2. Verify staleness thresholds
3. Test failover behavior
4. Report anomalies

## Usage

Provide the symbol (e.g., BTCUSDT) and I'll analyze the FCP state.
```

**hooks/hooks.json**:

```json
{
  "hooks": {
    "PreToolUse": [
      {
        "matcher": "Bash",
        "hooks": [
          {
            "type": "command",
            "command": "${CLAUDE_PLUGIN_ROOT}/scripts/check-cache.sh"
          }
        ]
      }
    ]
  }
}
```
---

## Context Engineering Reference

Optimize context usage for efficient Claude Code sessions.

### Context Hierarchy

Three-tier memory hierarchy with intelligent retrieval:

| Level | Location | Scope | Persistence |
|-------|----------|-------|-------------|
| User | `~/.claude/CLAUDE.md` | All projects | Permanent |
| Project | `./CLAUDE.md` | Current repo | Permanent |
| Dynamic | `@imports`, Skills | On-demand | Session |

**Loading priority**:

1. User CLAUDE.md (global settings)
2. Project CLAUDE.md (repo-specific)
3. Subdirectory CLAUDE.md files (auto-discovered)
4. Dynamic imports via `@file` syntax
5. Skills loaded on invocation

### Just-In-Time (JIT) Loading

Load context only when needed, not upfront:

**Skills as JIT context**:

```markdown
---

name: fcp-debugging
description: Debug FCP cache behavior

---

# Loaded only when skill is invoked

````

**@ imports for on-demand loading**:

```markdown
## References
- @references/api-docs.md  # Loaded when referenced
- @examples/usage.md       # Not loaded until needed
````

**Benefits**:

- Reduces initial context consumption
- Keeps main conversation lean
- Loads specialized knowledge when relevant

### Subagent Context Isolation

Subagents run in separate context windows:

```yaml
---
name: code-reviewer
description: Reviews code for quality
tools: Read, Grep, Glob
model: haiku
---
```

**Why use subagents**:

- Heavy operations stay isolated
- Verbose output doesn't pollute main context
- Only summary returns to parent
- Parallel research without context collision

**Built-in subagents**:

| Subagent        | Model   | Tools     | Purpose                         |
| --------------- | ------- | --------- | ------------------------------- |
| Explore         | Haiku   | Read-only | File discovery, codebase search |
| Plan            | Inherit | Read-only | Research for planning           |
| General-purpose | Inherit | All       | Complex multi-step tasks        |

### Compaction Strategies

**Auto-compaction** triggers at ~75-95% capacity:

```bash
# Override auto-compaction threshold
export CLAUDE_AUTOCOMPACT_PCT_OVERRIDE=50  # Trigger at 50%
```

**Manual compaction**:

```bash
/compact              # Compact current conversation
/compact --aggressive # More aggressive summarization
```

**Compaction best practices**:

- Compact before starting new major tasks
- Use subagents for verbose operations
- Clear between unrelated work sessions
- Monitor with `/stats` command

### Context Window Management

**80% rule**: Keep context below 80% for optimal quality.

**Monitor context**:

```bash
/stats    # View token usage statistics
/context  # Check current context state
```

**Reduce context consumption**:

1. Use subagents for exploration
2. Delegate tests to subagents
3. Clear with `/clear` between tasks
4. Use skills instead of inline instructions
5. Keep CLAUDE.md concise (<300 lines)

### Subagent Configuration

**Full configuration options**:

```yaml
---
name: db-reader
description: Execute read-only database queries
tools: Bash, Read
disallowedTools: Write, Edit
model: sonnet
permissionMode: default
skills:
  - sql-best-practices
hooks:
  PreToolUse:
    - matcher: "Bash"
      hooks:
        - type: command
          command: "./scripts/validate-readonly.sh"
---
You are a database analyst with read-only access.
Execute SELECT queries to answer questions about data.
```

**Frontmatter fields**:

| Field             | Required | Description                                            |
| ----------------- | -------- | ------------------------------------------------------ |
| `name`            | Yes      | Unique identifier (kebab-case)                         |
| `description`     | Yes      | When to delegate to this agent                         |
| `tools`           | No       | Allowed tools (inherits all if omitted)                |
| `disallowedTools` | No       | Tools to deny                                          |
| `model`           | No       | sonnet, opus, haiku, or inherit                        |
| `permissionMode`  | No       | default, acceptEdits, dontAsk, bypassPermissions, plan |
| `skills`          | No       | Skills to preload                                      |
| `hooks`           | No       | Lifecycle hooks                                        |

### Model Selection for Subagents

| Model   | Best For                        | Token Cost |
| ------- | ------------------------------- | ---------- |
| Haiku   | Fast searches, simple analysis  | Lowest     |
| Sonnet  | Balanced capability/speed       | Medium     |
| Opus    | Complex reasoning, architecture | Highest    |
| Inherit | Match parent conversation       | Varies     |

**Cost optimization pattern**:

```yaml
# Use Haiku for exploration (fast, cheap)
---
name: file-finder
model: haiku
tools: Read, Glob, Grep
---
# Use Sonnet for analysis (balanced)
---
name: code-analyzer
model: sonnet
tools: Read, Grep
---
# Use Opus for architecture (capable)
---
name: architect
model: opus
tools: Read, Grep, Glob, Task
---
```

### Subagent Patterns

**Isolate high-volume operations**:

```
Use a subagent to run the test suite and report only failing tests
```

**Parallel research**:

```
Research authentication, database, and API modules in parallel using separate subagents
```

**Chained subagents**:

```
Use code-reviewer to find issues, then optimizer to fix them
```

### Background vs Foreground

**Foreground** (default):

- Blocks main conversation
- Permission prompts pass through
- Use for interactive tasks

**Background**:

- Runs concurrently
- Pre-approves permissions upfront
- No interactive prompts
- Resume if fails

```bash
# Disable background tasks
export CLAUDE_CODE_DISABLE_BACKGROUND_TASKS=1
```

**Run in background**: Press Ctrl+B or ask Claude.

### Subagent Resumption

Subagents persist independently of main conversation:

```
Use code-reviewer to review auth module
[Agent completes]

Continue that code review for authorization logic
[Claude resumes with full context]
```

**Transcript location**: `~/.claude/projects/{project}/{sessionId}/subagents/agent-{id}.jsonl`

### Permission Modes

| Mode                | Behavior                    |
| ------------------- | --------------------------- |
| `default`           | Standard prompts            |
| `acceptEdits`       | Auto-accept file edits      |
| `dontAsk`           | Auto-deny prompts           |
| `bypassPermissions` | Skip all checks (dangerous) |
| `plan`              | Read-only exploration       |

### Preload Skills

Inject skill content into subagent context:

```yaml
---
name: api-developer
description: Implement API endpoints
skills:
  - api-conventions
  - error-handling-patterns
---
```

Skills are fully loaded at startup, not just available for invocation.

### Disable Subagents

In settings.json:

```json
{
  "permissions": {
    "deny": ["Task(Explore)", "Task(my-custom-agent)"]
  }
}
```

Or via CLI:

```bash
claude --disallowedTools "Task(Explore)"
```

### DSM Context Engineering

**CLAUDE.md optimization** (keep <300 lines):

```markdown
# DSM Quick Reference

## Commands

- `uv run pytest tests/unit/`
- `mise run cache:clear`

## Key Patterns

- FCP: See @.claude/rules/fcp-protocol.md
- Timestamps: UTC with timezone.utc
- DataFrames: Polars, not pandas
```

**DSM subagent configuration**:

```yaml
---
name: fcp-validator
description: Validate FCP cache behavior
tools: Read, Grep, Glob, Bash
model: haiku
skills:
  - fcp-protocol
---
Check FCP cache state and return summary.
Only report anomalies, not full cache contents.
```

**DSM context patterns**:

1. Use `@.claude/rules/fcp-protocol.md` for FCP details
2. Keep OHLCV schema in skills, not CLAUDE.md
3. Delegate cache inspection to subagents
4. Use Explore for codebase questions
---

## Keyboard Shortcuts Reference

Complete keyboard shortcuts for Claude Code interactive mode.

### General Controls

| Shortcut    | Description              | Context                  |
| ----------- | ------------------------ | ------------------------ |
| `Ctrl+C`    | Cancel input/generation  | Standard interrupt       |
| `Ctrl+D`    | Exit session             | EOF signal               |
| `Ctrl+G`    | Open in text editor      | Edit prompt externally   |
| `Ctrl+L`    | Clear terminal           | Keeps conversation       |
| `Ctrl+O`    | Toggle verbose output    | Show tool details        |
| `Ctrl+R`    | Reverse history search   | Search previous commands |
| `Ctrl+B`    | Background task          | Tmux: press twice        |
| `Esc+Esc`   | Rewind conversation      | Restore previous state   |
| `Shift+Tab` | Toggle permission mode   | Auto-Accept/Plan/Normal  |
| `Alt+P`     | Switch model             | Keep current prompt      |
| `Alt+T`     | Toggle extended thinking | After /terminal-setup    |

### Image Paste

| Platform | Shortcut                     |
| -------- | ---------------------------- |
| macOS    | `Ctrl+V` or `Cmd+V` (iTerm2) |
| Windows  | `Alt+V`                      |
| Linux    | `Ctrl+V`                     |

### Text Editing

| Shortcut | Description                        |
| -------- | ---------------------------------- |
| `Ctrl+K` | Delete to end of line              |
| `Ctrl+U` | Delete entire line                 |
| `Ctrl+Y` | Paste deleted text                 |
| `Alt+Y`  | Cycle paste history (after Ctrl+Y) |
| `Alt+B`  | Move back one word                 |
| `Alt+F`  | Move forward one word              |

### Multiline Input

| Method           | Shortcut       | Terminal                        |
| ---------------- | -------------- | ------------------------------- |
| Quick escape     | `\` + Enter    | All                             |
| macOS default    | `Option+Enter` | macOS                           |
| Shift+Enter      | `Shift+Enter`  | iTerm2, WezTerm, Ghostty, Kitty |
| Control sequence | `Ctrl+J`       | All                             |

### Quick Commands

| Prefix | Action                       |
| ------ | ---------------------------- |
| `/`    | Command or skill             |
| `!`    | Bash mode (direct execution) |
| `@`    | File path autocomplete       |

### macOS Terminal Configuration

**Option as Meta required for Alt shortcuts**:

**iTerm2**: Settings → Profiles → Keys → Set Left/Right Option to "Esc+"

**Terminal.app**: Settings → Profiles → Keyboard → Check "Use Option as Meta Key"

**VS Code**: Settings → Profiles → Keys → Set Left/Right Option to "Esc+"

### Vim Mode

Enable with `/vim` or permanently via `/config`.

**Mode Switching**:

| Command | Action               | From Mode |
| ------- | -------------------- | --------- |
| `Esc`   | Enter NORMAL         | INSERT    |
| `i`     | Insert before cursor | NORMAL    |
| `I`     | Insert at line start | NORMAL    |
| `a`     | Insert after cursor  | NORMAL    |
| `A`     | Insert at line end   | NORMAL    |
| `o`     | Open line below      | NORMAL    |
| `O`     | Open line above      | NORMAL    |

**Navigation (NORMAL mode)**:

| Command   | Action                 |
| --------- | ---------------------- |
| `h/j/k/l` | Left/down/up/right     |
| `w`       | Next word              |
| `e`       | End of word            |
| `b`       | Previous word          |
| `0`       | Beginning of line      |
| `$`       | End of line            |
| `^`       | First non-blank        |
| `gg`      | Beginning of input     |
| `G`       | End of input           |
| `f{char}` | Jump to character      |
| `F{char}` | Jump back to character |
| `t{char}` | Jump before character  |
| `T{char}` | Jump after character   |
| `;`       | Repeat f/F/t/T         |
| `,`       | Repeat f/F/t/T reverse |

**Editing (NORMAL mode)**:

| Command    | Action                |
| ---------- | --------------------- |
| `x`        | Delete character      |
| `dd`       | Delete line           |
| `D`        | Delete to end of line |
| `dw/de/db` | Delete word/end/back  |
| `cc`       | Change line           |
| `C`        | Change to end         |
| `cw/ce/cb` | Change word/end/back  |
| `yy/Y`     | Yank (copy) line      |
| `yw/ye/yb` | Yank word/end/back    |
| `p`        | Paste after cursor    |
| `P`        | Paste before cursor   |
| `>>`       | Indent line           |
| `<<`       | Dedent line           |
| `J`        | Join lines            |
| `.`        | Repeat last change    |

**Text Objects**:

| Object  | Action                     |
| ------- | -------------------------- |
| `iw/aw` | Inner/around word          |
| `iW/aW` | Inner/around WORD          |
| `i"/a"` | Inner/around double quotes |
| `i'/a'` | Inner/around single quotes |
| `i(/a(` | Inner/around parentheses   |
| `i[/a[` | Inner/around brackets      |
| `i{/a{` | Inner/around braces        |

### Built-in Commands

| Command                   | Purpose                      |
| ------------------------- | ---------------------------- |
| `/clear`                  | Clear conversation history   |
| `/compact [instructions]` | Compact with optional focus  |
| `/config`                 | Open settings                |
| `/context`                | Visualize context usage      |
| `/cost`                   | Show token usage             |
| `/doctor`                 | Check installation health    |
| `/exit`                   | Exit REPL                    |
| `/export [filename]`      | Export conversation          |
| `/help`                   | Get usage help               |
| `/init`                   | Initialize CLAUDE.md         |
| `/mcp`                    | Manage MCP servers           |
| `/memory`                 | Edit CLAUDE.md files         |
| `/model`                  | Select AI model              |
| `/permissions`            | View/update permissions      |
| `/plan`                   | Enter plan mode              |
| `/rename <name>`          | Rename session               |
| `/resume [session]`       | Resume conversation          |
| `/rewind`                 | Rewind conversation          |
| `/stats`                  | Visualize daily usage        |
| `/status`                 | Show version, model, account |
| `/statusline`             | Set up status line           |
| `/copy`                   | Copy last response           |
| `/tasks`                  | Manage background tasks      |
| `/theme`                  | Change color theme           |
| `/todos`                  | List TODO items              |
| `/usage`                  | Show plan limits             |
| `/vim`                    | Enable vim mode              |

### Reverse History Search

1. Press `Ctrl+R` to start
2. Type to search previous commands
3. `Ctrl+R` again for older matches
4. `Tab` or `Esc` to accept and edit
5. `Enter` to accept and execute
6. `Ctrl+C` to cancel

### Background Tasks

**Start background task**:

- Ask Claude to run in background
- Press `Ctrl+B` during execution
- Tmux users: press `Ctrl+B` twice

**Disable background tasks**:

```bash
export CLAUDE_CODE_DISABLE_BACKGROUND_TASKS=1
```

### Bash Mode

Run commands directly with `!` prefix:

```bash
! npm test
! git status
! ls -la
```

- Adds output to conversation context
- Shows real-time progress
- Supports `Ctrl+B` backgrounding
- Tab completion from history

### Task List

- `Ctrl+T` toggles task list view
- Shows up to 10 tasks
- Persists across compactions
- Named task lists:

```bash
CLAUDE_CODE_TASK_LIST_ID=my-project claude
```

### DSM Keyboard Workflow

**Quick test cycle**:

```bash
! uv run pytest tests/unit/ -v
```

**Background test run**:

```
Run the full test suite in the background
```

Then press `Ctrl+B` if needed.

**FCP debugging flow**:

```bash
! ls ~/.cache/dsm/          # Check cache state
! mise run cache:clear      # Clear if needed
```

**Vim mode for prompts**:

```
/vim                        # Enable vim mode
i                           # Insert mode
[type prompt]
Esc                         # Normal mode
dd                          # Delete line if wrong
i                           # Back to insert
```

---

<!-- SSoT-OK: Section added by autonomous Claude Code infrastructure improvement loop -->

## File Exclusion & Security Patterns Reference

Comprehensive guide to protecting sensitive files and enforcing security boundaries in Claude Code.

### Permission Deny Rules

The primary mechanism for file protection uses deny rules in `settings.json`:

```json
{
  "permissions": {
    "deny": [
      "Read(./.env)",
      "Read(./.env.*)",
      "Read(./secrets/**)",
      "Read(~/.aws/**)",
      "Read(~/.ssh/**)"
    ]
  }
}
```

#### Rule Evaluation Order

| Priority | Rule Type | Behavior |
|----------|-----------|----------|
| 1 (Highest) | deny | Always blocks, cannot be overridden |
| 2 | ask | Prompts for user approval |
| 3 (Lowest) | allow | Permits without prompting |

**Critical**: Deny rules take absolute precedence—even if an allow rule matches, deny blocks the operation.

### Pattern Syntax Reference

| Pattern | Effect |
|---------|--------|
| `Read(./.env)` | Blocks reading `.env` in current directory |
| `Read(./.env.*)` | Blocks `.env.local`, `.env.production`, etc. |
| `Read(./secrets/**)` | Recursive block on `secrets/` directory |
| `Read(~/path)` | Blocks home directory paths |
| `Read(**/config.json)` | Blocks `config.json` in any nested directory |
| `Read(.*)` | Blocks all hidden files |
| `Read` | Blocks all file reads (extreme) |

#### Bash Pattern Matching

Space placement matters in Bash patterns:

| Pattern | Matches | Does NOT Match |
|---------|---------|----------------|
| `Bash(ls *)` | `ls -la`, `ls foo` | `lsof` |
| `Bash(ls*)` | `ls -la`, `ls foo`, `lsof` | - |
| `Bash(npm run *)` | `npm run test`, `npm run build` | `npm install` |

### Configuration Scopes

Settings have hierarchical precedence:

| Scope | Location | Shared? | Precedence |
|-------|----------|---------|------------|
| Managed | System paths | IT-deployed | Highest |
| Local | `.claude/settings.local.json` | No (gitignored) | High |
| Project | `.claude/settings.json` | Yes (committed) | Medium |
| User | `~/.claude/settings.json` | No | Lowest |

**Managed settings locations**:

| Platform | Path |
|----------|------|
| macOS | `/Library/Application Support/ClaudeCode/managed-settings.json` |
| Linux/WSL | `/etc/claude-code/managed-settings.json` |
| Windows | `C:\Program Files\ClaudeCode\managed-settings.json` |

### DSM Security Configuration

The data-source-manager project uses this security configuration:

```json
{
  "permissions": {
    "deny": [
      "Read(.env*)",
      "Read(.mise.local.toml)",
      "Edit(.env*)",
      "Edit(.mise.local.toml)",
      "Bash(pip install *)",
      "Bash(git push --force *)",
      "Bash(python3.14 *)",
      "Bash(python3.12 *)"
    ],
    "ask": [
      "Bash(git push *)",
      "Bash(uv pip install *)"
    ],
    "allow": [
      "Bash(uv run *)",
      "Bash(mise run *)",
      "Bash(git *)",
      "Read",
      "Edit",
      "Write"
    ]
  }
}
```

### Security Best Practices

#### 1. Secrets Protection

```json
{
  "permissions": {
    "deny": [
      "Read(./.env)",
      "Read(./.env.*)",
      "Read(./secrets/**)",
      "Read(./.secrets/**)",
      "Read(./config/credentials.json)",
      "Read(~/.aws/**)",
      "Read(~/.ssh/**)",
      "Read(**/*token*)",
      "Read(**/*secret*)",
      "Read(**/*password*)"
    ]
  }
}
```

#### 2. Network Restriction

```json
{
  "permissions": {
    "deny": [
      "Bash(curl *)",
      "Bash(wget *)",
      "WebFetch(domain:internal.corp.com)"
    ]
  }
}
```

#### 3. Destructive Operations

```json
{
  "permissions": {
    "deny": [
      "Bash(rm -rf *)",
      "Bash(git push --force *)",
      "Bash(git reset --hard *)",
      "Bash(git clean -fd *)"
    ],
    "ask": [
      "Bash(rm *)",
      "Bash(git push *)"
    ]
  }
}
```

#### 4. Build Artifact Protection

```json
{
  "permissions": {
    "deny": [
      "Read(./build/**)",
      "Read(./dist/**)",
      "Read(./node_modules/**)",
      "Read(./.venv/**)",
      "Read(./__pycache__/**)"
    ]
  }
}
```

### Tool-Specific Deny Rules

| Tool | Syntax | Example |
|------|--------|---------|
| Read | `Read(path)` | `Read(./.env*)` |
| Edit | `Edit(path)` | `Edit(./secrets/**)` |
| Write | `Write(path)` | `Write(./.env)` |
| WebFetch | `WebFetch(domain:host)` | `WebFetch(domain:internal.com)` |
| Bash | `Bash(pattern)` | `Bash(curl *)` |
| MCP | `MCP(server:name)` | `MCP(filesystem)` |
| Task | `Task(agent:name)` | `Task(deploy)` |

### Git Integration

#### respectGitignore Setting

```json
{
  "respectGitignore": true
}
```

When enabled (default):
- Files matching `.gitignore` excluded from `@` autocomplete
- Does NOT block direct Read operations
- Useful for reducing noise, not security enforcement

#### Combining with Deny Rules

For security, combine gitignore with explicit deny rules:

```json
{
  "respectGitignore": true,
  "permissions": {
    "deny": [
      "Read(.gitignore)",
      "Read(./.env*)"
    ]
  }
}
```

### No .claudeignore File

**Important**: Claude Code does NOT support a `.claudeignore` file.

**Alternatives**:
1. Use `permissions.deny` in `settings.json` (primary)
2. Use `.claude/settings.local.json` for personal exclusions
3. Enable `respectGitignore` for autocomplete filtering
4. Use PreToolUse hooks for custom logic

### PreToolUse Hook for Custom Security

For complex security requirements, use PreToolUse hooks:

```bash
#!/bin/bash
# .claude/hooks/security-guard.sh

TOOL_NAME="$1"
INPUT_JSON="$2"

# Custom pattern matching
if [[ "$TOOL_NAME" == "Read" ]]; then
  FILE_PATH=$(echo "$INPUT_JSON" | jq -r '.file_path')
  
  # Block files containing "secret" in name
  if [[ "$FILE_PATH" == *secret* ]]; then
    echo '{"action": "block", "message": "Blocked: File contains sensitive keyword"}'
    exit 0
  fi
fi

# Allow all other operations
echo '{"action": "allow"}'
```

Configure in `.claude/hooks/hooks.json`:

```json
{
  "hooks": {
    "PreToolUse": [
      {
        "matcher": "*",
        "command": ".claude/hooks/security-guard.sh"
      }
    ]
  }
}
```

### Enterprise Managed Settings

For organization-wide security enforcement:

```json
// /Library/Application Support/ClaudeCode/managed-settings.json
{
  "permissions": {
    "deny": [
      "Read(~/.ssh/**)",
      "Read(~/.aws/**)",
      "Read(~/.config/gcloud/**)",
      "Bash(curl *)",
      "Bash(wget *)",
      "WebFetch"
    ],
    "disableBypassPermissionsMode": "disable"
  }
}
```

**Key enterprise options**:

| Setting | Effect |
|---------|--------|
| `disableBypassPermissionsMode` | Prevents `--dangerously-skip-permissions` |
| Managed deny rules | Cannot be overridden by user or project settings |

### Security Audit Checklist

| Check | Command/Action |
|-------|----------------|
| Review deny rules | `cat .claude/settings.json \| jq '.permissions.deny'` |
| Check managed settings | `cat /Library/Application\ Support/ClaudeCode/managed-settings.json` |
| Verify gitignore | `git ls-files --ignored --exclude-standard` |
| Test Read denial | Ask Claude to read `.env` file |
| Review hook logs | Check `.claude/hooks/` output |

### Common Security Patterns

#### Pattern: API Key Protection

```json
{
  "permissions": {
    "deny": [
      "Read(.env*)",
      "Read(**/*api*key*)",
      "Read(**/*apikey*)",
      "Read(./.secrets/**)",
      "Bash(*API_KEY*)",
      "Bash(*SECRET*)"
    ]
  }
}
```

#### Pattern: Database Credential Protection

```json
{
  "permissions": {
    "deny": [
      "Read(**/database.yml)",
      "Read(**/db.json)",
      "Read(**/*credentials*)",
      "Bash(*DB_PASSWORD*)",
      "Bash(*DATABASE_URL*)"
    ]
  }
}
```

#### Pattern: Infrastructure Secrets

```json
{
  "permissions": {
    "deny": [
      "Read(~/.kube/config)",
      "Read(~/.docker/config.json)",
      "Read(terraform.tfvars)",
      "Read(**/*.tfvars)",
      "Read(~/.config/gcloud/**)"
    ]
  }
}
```

### Troubleshooting Security

| Issue | Cause | Solution |
|-------|-------|----------|
| Deny rule not working | Pattern mismatch | Use exact path or correct glob syntax |
| Can still read file | Rule in wrong scope | Check precedence hierarchy |
| Bash command bypassed | Space in pattern | Use `Bash(cmd *)` not `Bash(cmd*)` |
| MCP accessing secrets | MCP not in deny | Add `MCP(server:name)` to deny |

### Security Limitations

**Deny rules are not security boundaries**:
- Claude can potentially infer file contents from error messages
- MCP servers may bypass Read restrictions
- Bash commands can access files indirectly
- System reminders may expose file paths

**Defense in depth**:
1. Use deny rules as first line
2. Don't store secrets in project directory
3. Use external secret managers (Doppler, 1Password)
4. Enable managed settings for enterprise
5. Monitor with PostToolUse hooks
---

<!-- SSoT-OK: Section added by autonomous Claude Code infrastructure improvement loop -->

## Checkpointing & Rewind Reference

Comprehensive guide to checkpoint management and session recovery in Claude Code.

### How Checkpoints Work

Claude Code automatically tracks file edits as you work, creating recovery points:

| Feature            | Behavior                                       |
| ------------------ | ---------------------------------------------- |
| Automatic tracking | Every user prompt creates a new checkpoint     |
| Persistence        | Checkpoints persist across sessions            |
| Cleanup            | Auto-cleaned after 30 days (configurable)      |
| Scope              | Only file edits via Claude's tools are tracked |

### Accessing Rewind

**Keyboard shortcut**: Press `Esc` twice (`Esc` + `Esc`)

**Command**: `/rewind`

**List checkpoints**: `/checkpoints`

**Jump to specific**: `/rewind <checkpoint-id>`

### Three Restore Options

| Option                | Effect                            | Use Case                                              |
| --------------------- | --------------------------------- | ----------------------------------------------------- |
| **Conversation only** | Rewind to user message, keep code | Claude confused but code is correct                   |
| **Code only**         | Revert files, keep conversation   | Execution failed but Claude's mental model is correct |
| **Both**              | Restore conversation and code     | Complete reset to prior state                         |

### Common Use Cases

**Exploring alternatives**:

```
User: "Try implementing this with recursion"
[Claude implements recursion]
User: [Esc Esc] → rewind to before recursion
User: "Now try with iteration"
```

**Recovering from mistakes**:

```
User: "Update the auth module"
[Claude makes breaking changes]
User: [Esc Esc] → Code only restore
User: "Let's take a more incremental approach"
```

**Iterating on features**:

```
User: "Add form validation"
[Claude adds validation v1]
User: [Esc Esc] → Conversation only restore
User: "I want stricter validation with regex"
```

### Limitations

#### Bash Commands NOT Tracked

Checkpointing does NOT track files modified by bash commands:

```bash
# These changes CANNOT be undone via rewind:
rm file.txt
mv old.txt new.txt
cp source.txt dest.txt
git checkout -- file.txt
```

**Workaround**: Use Claude's Edit tool instead of bash for file modifications:

- Edit tool changes ARE tracked
- Bash file operations are permanent

#### External Changes NOT Tracked

| Change Type                   | Tracked? |
| ----------------------------- | -------- |
| Claude's Edit tool            | Yes      |
| Claude's Write tool           | Yes      |
| Manual edits (outside Claude) | No       |
| Other concurrent sessions     | No       |
| Bash file operations          | No       |

#### Network/External Operations

Cannot be undone:

- API calls
- Database queries
- File uploads
- External service interactions

### Checkpointing vs Git

| Aspect        | Checkpoints   | Git              |
| ------------- | ------------- | ---------------- |
| Scope         | Session-level | Repository-level |
| Granularity   | Per-prompt    | Per-commit       |
| Persistence   | 30 days       | Permanent        |
| Collaboration | Single user   | Multi-user       |
| Use case      | Quick undo    | Version history  |

**Best practice**: Use checkpoints for exploration, Git for commits.

### CLI Integration

**Resume with rewind**:

```bash
claude --resume <session-id> --rewind-files <checkpoint-uuid>
```

**List session checkpoints**:

```bash
claude --resume <session-id> --list-checkpoints
```

### SDK/API Integration

**TypeScript SDK**:

```typescript
import { ClaudeCode } from "@anthropic-ai/claude-code-sdk";

const client = new ClaudeCode({
  fileCheckpointing: true, // Enable checkpointing
});

// Capture checkpoint UUID from response stream
const checkpointId = response.checkpoint_uuid;

// Rewind to checkpoint
await client.rewindFiles(checkpointId);
```

**Python SDK**:

```python
from claude_code_sdk import ClaudeCode

client = ClaudeCode(file_checkpointing=True)

# Capture checkpoint UUID
checkpoint_id = response.checkpoint_uuid

# Rewind to checkpoint
client.rewind_files(checkpoint_id)
```

### DSM Checkpoint Patterns

#### Pattern: Safe Module Updates

```
User: "I want to update the FCP module"
[Checkpoint created]

User: "Start by extracting the cache logic"
[Checkpoint created]

User: "Now update the failover handling"
[Checkpoint created]

# If something breaks:
User: [Esc Esc] → Code only restore to before failover changes
```

#### Pattern: Exploratory Implementation

```
User: "Let's try three different approaches for the rate limiter"

# Approach 1
User: "Implement with token bucket"
[Checkpoint created]
User: [Esc Esc] → Full restore

# Approach 2
User: "Implement with leaky bucket"
[Checkpoint created]
User: [Esc Esc] → Full restore

# Approach 3
User: "Implement with sliding window"
[Checkpoint created]

# Compare and decide
User: "Go back to the token bucket implementation"
User: /rewind <checkpoint-id-from-approach-1>
```

#### Pattern: Test-Driven Recovery

```
User: "Add tests for the new feature"
[Checkpoint: tests added]

User: "Now implement the feature"
[Checkpoint: feature implemented but tests fail]

User: [Esc Esc] → Code only restore
User: "The tests are failing, let me explain the expected behavior better..."
```

### Checkpoint Configuration

**Retention period** (in `settings.json`):

```json
{
  "checkpoints": {
    "retentionDays": 30,
    "maxCheckpointsPerSession": 100
  }
}
```

**Disable checkpointing** (not recommended):

```json
{
  "checkpoints": {
    "enabled": false
  }
}
```

### Troubleshooting Checkpoints

| Issue                 | Cause                     | Solution                               |
| --------------------- | ------------------------- | -------------------------------------- |
| Rewind not working    | Bash command made changes | Changes via bash are permanent         |
| Checkpoint missing    | Exceeded retention period | Checkpoints auto-delete after 30 days  |
| Can't find checkpoint | Wrong session             | Use `--resume` with correct session ID |
| Partial restore       | Mixed Edit/Bash changes   | Only Edit tool changes are restored    |

### Best Practices

1. **Prefer Edit tool over Bash** for file modifications you might want to undo
2. **Checkpoint before risky operations**: Ask Claude to make a git commit first
3. **Use conversation-only restore** when Claude's plan is wrong but code is fine
4. **Use code-only restore** when implementation failed but Claude understands the goal
5. **Combine with git** for permanent version history
6. **Don't rely on checkpoints** for destructive bash operations

### Checkpoint Workflow Integration

```
┌─────────────────────────────────────────────────────────┐
│                    Claude Code Session                   │
├─────────────────────────────────────────────────────────┤
│                                                          │
│  User Prompt ──► Checkpoint Created ──► Claude Response  │
│       │                  │                    │          │
│       │                  │                    ▼          │
│       │                  │              Edit Tool        │
│       │                  │                    │          │
│       │                  │                    ▼          │
│       │                  └──────────── File Changed      │
│       │                                       │          │
│       ▼                                       │          │
│  [Esc Esc] ──► Rewind Menu                   │          │
│       │                                       │          │
│       ├── Conversation Only ─────────────────┘          │
│       ├── Code Only ──► Restore Files                   │
│       └── Both ──► Full Restore                         │
│                                                          │
└─────────────────────────────────────────────────────────┘
```

### Session Recovery After Crash

If Claude Code crashes mid-session:

1. **Find session ID**:

   ```bash
   ls -lt ~/.claude/sessions/ | head -5
   ```

2. **Resume session**:

   ```bash
   claude --resume <session-id>
   ```

3. **List checkpoints**:

   ```
   /checkpoints
   ```

4. **Restore to last good state**:

   ```
   /rewind <checkpoint-id>
   ```

### Checkpoint Storage

Checkpoints are stored in:

- macOS/Linux: `~/.claude/checkpoints/`
- Windows: `%USERPROFILE%\.claude\checkpoints\`

**Structure**:

```
~/.claude/checkpoints/
├── <session-id>/
│   ├── <checkpoint-uuid>.json    # Metadata
│   └── <checkpoint-uuid>/        # File snapshots
│       ├── file1.py
│       └── dir/file2.py
```

**Manual cleanup** (if needed):

```bash
# Remove checkpoints older than 7 days
find ~/.claude/checkpoints -type d -mtime +7 -exec rm -rf {} +
```
---

<!-- SSoT-OK: Section added by autonomous Claude Code infrastructure improvement loop -->

## Extended Thinking Reference

Comprehensive guide to extended thinking for enhanced reasoning in Claude Code.

### Overview

Extended thinking gives Claude enhanced reasoning capabilities for complex tasks by allowing internal step-by-step reasoning before delivering a final answer.

**Supported models**:

- Claude Opus 4.5
- Claude Opus 4.1
- Claude Opus 4
- Claude Sonnet 4.5
- Claude Sonnet 4
- Claude Haiku 4.5

### Budget Tokens

The `budget_tokens` parameter determines the maximum tokens Claude can use for internal reasoning:

| Budget Range                 | Use Case           | Notes                                  |
| ---------------------------- | ------------------ | -------------------------------------- |
| 1,024 (minimum)              | Simple tasks       | Start here and increase as needed      |
| 4,000-8,000                  | Standard reasoning | Good for most coding tasks             |
| 10,000-16,000                | Complex analysis   | Architecture decisions, deep debugging |
| 31,999 (Claude Code default) | Maximum depth      | Triggered by "ultrathink" keyword      |
| 32,000+                      | Batch processing   | Use batch API to avoid timeouts        |

**Key points**:

- `budget_tokens` must be less than `max_tokens`
- Claude may not use the entire budget
- Higher budgets improve quality with diminishing returns
- Increased budget = increased latency

### UltraThink Keyword

Add "ultrathink" to any prompt to trigger maximum reasoning depth (31,999 tokens):

```
User: "ultrathink: Design an optimal caching strategy for our FCP module"
```

**Best for**:

- Complex architecture decisions
- Debugging difficult issues
- Deep code analysis
- Multi-step algorithmic problems

### API Usage

**Basic request**:

```json
{
  "model": "claude-sonnet-4-5",
  "max_tokens": 16000,
  "thinking": {
    "type": "enabled",
    "budget_tokens": 10000
  },
  "messages": [{ "role": "user", "content": "Analyze this algorithm..." }]
}
```

**Response format**:

```json
{
  "content": [
    {
      "type": "thinking",
      "thinking": "Let me analyze this step by step...",
      "signature": "WaUjzkypQ2mUEVM36O2..."
    },
    {
      "type": "text",
      "text": "Based on my analysis..."
    }
  ]
}
```

### Summarized Thinking (Claude 4)

Claude 4 models return **summarized** thinking, not full output:

| Aspect        | Behavior                                      |
| ------------- | --------------------------------------------- |
| Billing       | Charged for full thinking tokens, not summary |
| Output tokens | Billed count won't match visible count        |
| First lines   | More verbose for prompt engineering           |
| Migration     | Easy migration from Claude 3.7                |

### Streaming

When streaming with thinking enabled:

```python
with client.messages.stream(
    model="claude-sonnet-4-5",
    thinking={"type": "enabled", "budget_tokens": 10000},
    messages=[{"role": "user", "content": "Solve this..."}]
) as stream:
    for event in stream:
        if event.type == "content_block_delta":
            if event.delta.type == "thinking_delta":
                print(f"Thinking: {event.delta.thinking}")
            elif event.delta.type == "text_delta":
                print(f"Response: {event.delta.text}")
```

### Tool Use with Thinking

Extended thinking works with tool use but has constraints:

**Limitations**:

- Only `tool_choice: {"type": "auto"}` or `{"type": "none"}` supported
- `tool_choice: {"type": "any"}` or `{"type": "tool"}` will error
- Cannot toggle thinking mid-turn (during tool use loops)

**Preserving thinking blocks**:

```python
# First request
response = client.messages.create(
    model="claude-sonnet-4-5",
    thinking={"type": "enabled", "budget_tokens": 10000},
    tools=[weather_tool],
    messages=[{"role": "user", "content": "What's the weather?"}]
)

# Extract blocks
thinking_block = next(b for b in response.content if b.type == 'thinking')
tool_use_block = next(b for b in response.content if b.type == 'tool_use')

# Continue with tool result - MUST include thinking block
continuation = client.messages.create(
    model="claude-sonnet-4-5",
    thinking={"type": "enabled", "budget_tokens": 10000},
    tools=[weather_tool],
    messages=[
        {"role": "user", "content": "What's the weather?"},
        {"role": "assistant", "content": [thinking_block, tool_use_block]},
        {"role": "user", "content": [{"type": "tool_result", "tool_use_id": tool_use_block.id, "content": "72°F"}]}
    ]
)
```

### Interleaved Thinking

Claude 4 supports thinking between tool calls with beta header:

```python
response = client.messages.create(
    model="claude-sonnet-4-5",
    max_tokens=16000,
    thinking={"type": "enabled", "budget_tokens": 10000},
    extra_headers={"anthropic-beta": "interleaved-thinking-2025-05-14"},
    tools=[calculator, database],
    messages=[{"role": "user", "content": "Calculate revenue and compare..."}]
)
```

**Flow with interleaved thinking**:

```
Turn 1: [thinking] → [tool_use: calculator]
  ↓ tool result
Turn 2: [thinking about result] → [tool_use: database]
  ↓ tool result
Turn 3: [thinking before answer] → [text: final response]
```

### Prompt Caching Considerations

| Scenario                         | Cache Behavior                          |
| -------------------------------- | --------------------------------------- |
| Same thinking parameters         | Cache hit expected                      |
| Changed thinking budget          | Message cache invalidated               |
| System prompt                    | Remains cached despite thinking changes |
| Thinking blocks from prior turns | Stripped, don't count toward context    |

**Recommendation**: Use 1-hour cache duration for extended thinking tasks.

### Thinking Encryption

Thinking content is encrypted and returned in `signature` field:

```json
{
  "type": "thinking",
  "thinking": "Step by step analysis...",
  "signature": "EqQBCgIYAhIM1gbcDa9GJwZA2b3h..."
}
```

**Important**:

- `signature` is for verification only
- Pass complete, unmodified blocks back to API
- Signatures are compatible across platforms (API, Bedrock, Vertex)

### Redacted Thinking

Occasionally thinking is flagged by safety systems:

```json
{
  "type": "redacted_thinking",
  "data": "EmwKAhgBEgy3va3pzix/LafPsn4..."
}
```

**Handling redacted blocks**:

- Content is encrypted, not human-readable
- Pass back to API unmodified
- Claude can still use redacted reasoning
- Consider explaining to users: "Some reasoning was encrypted for safety"

### Feature Compatibility

| Feature           | Compatible? | Notes                                                       |
| ----------------- | ----------- | ----------------------------------------------------------- |
| temperature       | No          | Cannot modify                                               |
| top_k             | No          | Cannot modify                                               |
| top_p             | Yes         | Values 0.95-1.0 only                                        |
| Response pre-fill | No          | Cannot pre-fill                                             |
| Forced tool use   | No          | Auto or none only                                           |
| Prompt caching    | Partial     | System prompt cached, messages invalidated on budget change |

### DSM Extended Thinking Patterns

#### Pattern: Complex Debugging

```
User: "ultrathink: Debug why FCP is returning stale data despite cache invalidation"
```

#### Pattern: Architecture Review

```
User: "ultrathink: Review the DataSourceManager architecture and identify potential race conditions"
```

#### Pattern: Algorithm Design

```
User: "ultrathink: Design an optimal retry strategy with exponential backoff for rate-limited APIs"
```

### Best Practices

1. **Start small**: Begin with 1,024-4,000 tokens and increase as needed
2. **Use ultrathink sparingly**: Reserve for genuinely complex problems
3. **Monitor token usage**: Track thinking tokens for cost optimization
4. **Handle latency**: Extended thinking increases response time
5. **Preserve blocks**: Always pass thinking blocks back unmodified
6. **Use batch for large budgets**: Above 32k tokens, use batch API
7. **Test different budgets**: Find optimal balance for your use case

### Troubleshooting

| Issue                        | Cause                      | Solution                                |
| ---------------------------- | -------------------------- | --------------------------------------- |
| Slow responses               | High thinking budget       | Reduce budget or use batch              |
| Error with tool_choice       | Incompatible setting       | Use `auto` or `none` only               |
| Cache misses                 | Changed thinking budget    | Keep budget consistent                  |
| Missing thinking in response | Thinking disabled mid-turn | Plan thinking at turn start             |
| Redacted thinking            | Safety system triggered    | Expected behavior, pass back unmodified |
---

<!-- SSoT-OK: Section added by autonomous Claude Code infrastructure improvement loop -->

## MCP Configuration Reference

Comprehensive guide to Model Context Protocol (MCP) server configuration in Claude Code.

### Overview

MCP (Model Context Protocol) allows Claude Code to connect to external tools, databases, and APIs through a standardized protocol.

**Key capabilities**:

- Connect to databases (PostgreSQL, MySQL, etc.)
- Integrate with issue trackers (JIRA, GitHub Issues)
- Access monitoring tools (Sentry, Datadog)
- Query APIs and services
- Automate workflows

### Configuration Locations

| Location                        | Scope      | Shared?                  | Use Case                       |
| ------------------------------- | ---------- | ------------------------ | ------------------------------ |
| `.mcp.json`                     | Project    | Yes (version controlled) | Team-shared servers            |
| `.claude/settings.local.json`   | Local      | No                       | Personal project servers       |
| `~/.claude/settings.local.json` | User       | No                       | Personal cross-project servers |
| `~/.claude.json`                | User/Local | No                       | Legacy location                |

**Precedence order** (highest to lowest):

1. Local scope (project-specific, personal)
2. Project scope (`.mcp.json`, team-shared)
3. User scope (cross-project, personal)

### Transport Types

| Transport | Use Case                          | Command                                                |
| --------- | --------------------------------- | ------------------------------------------------------ |
| `http`    | Remote HTTP servers (recommended) | `claude mcp add --transport http <name> <url>`         |
| `sse`     | Server-Sent Events (deprecated)   | `claude mcp add --transport sse <name> <url>`          |
| `stdio`   | Local process servers             | `claude mcp add --transport stdio <name> -- <command>` |

### Adding MCP Servers

**HTTP server (remote)**:

```bash
claude mcp add --transport http notion https://mcp.notion.com/mcp

# With authentication header
claude mcp add --transport http secure-api https://api.example.com/mcp \
  --header "Authorization: Bearer your-token"
```

**Stdio server (local)**:

```bash
claude mcp add --transport stdio --env API_KEY=YOUR_KEY airtable \
  -- npx -y airtable-mcp-server
```

**From JSON configuration**:

```bash
claude mcp add-json weather-api '{"type":"http","url":"https://api.weather.com/mcp"}'
```

**Import from Claude Desktop**:

```bash
claude mcp add-from-claude-desktop
```

### Managing Servers

```bash
# List all configured servers
claude mcp list

# Get details for specific server
claude mcp get github

# Remove a server
claude mcp remove github

# Check status (within Claude Code)
/mcp
```

### Configuration Scopes

**Local scope** (default):

```bash
claude mcp add --transport http stripe https://mcp.stripe.com
# or explicitly:
claude mcp add --transport http stripe --scope local https://mcp.stripe.com
```

**Project scope** (team-shared):

```bash
claude mcp add --transport http paypal --scope project https://mcp.paypal.com/mcp
```

Creates `.mcp.json`:

```json
{
  "mcpServers": {
    "paypal": {
      "type": "http",
      "url": "https://mcp.paypal.com/mcp"
    }
  }
}
```

**User scope** (cross-project):

```bash
claude mcp add --transport http hubspot --scope user https://mcp.hubspot.com/anthropic
```

### .mcp.json Configuration

**Basic structure**:

```json
{
  "mcpServers": {
    "github": {
      "type": "http",
      "url": "https://api.githubcopilot.com/mcp/"
    },
    "local-db": {
      "type": "stdio",
      "command": "npx",
      "args": ["-y", "@bytebase/dbhub", "--dsn", "postgresql://..."],
      "env": {
        "DB_PASSWORD": "${DB_PASSWORD}"
      }
    }
  }
}
```

**Environment variable expansion**:

```json
{
  "mcpServers": {
    "api-server": {
      "type": "http",
      "url": "${API_BASE_URL:-https://api.example.com}/mcp",
      "headers": {
        "Authorization": "Bearer ${API_KEY}"
      }
    }
  }
}
```

Supported syntax:

- `${VAR}` - Required variable
- `${VAR:-default}` - With default value

### Tool Permissions

Control which MCP tools Claude can use in `settings.json`:

```json
{
  "permissions": {
    "allow": ["MCP(github)", "MCP(database:read_*)"],
    "deny": ["MCP(database:delete_*)", "MCP(filesystem)"]
  }
}
```

### MCP Tool Search

When many MCP servers are configured, Tool Search dynamically loads tools on-demand:

**Auto-activation**: Triggers when MCP tools exceed 10% of context window

**Configuration**:

```bash
# Custom threshold (5%)
ENABLE_TOOL_SEARCH=auto:5 claude

# Always enabled
ENABLE_TOOL_SEARCH=true claude

# Disabled
ENABLE_TOOL_SEARCH=false claude
```

**Requirements**: Sonnet 4+, Opus 4+ (Haiku not supported)

### MCP Resources

Reference MCP resources with @ mentions:

```
> Analyze @github:issue://123 and suggest a fix
> Compare @postgres:schema://users with @docs:file://database/user-model
```

### MCP Prompts as Commands

MCP servers can expose prompts as commands:

```
> /mcp__github__list_prs
> /mcp__github__pr_review 456
> /mcp__jira__create_issue "Bug in login" high
```

### Output Limits

| Setting           | Default       | Purpose          |
| ----------------- | ------------- | ---------------- |
| Warning threshold | 10,000 tokens | Displays warning |
| Maximum output    | 25,000 tokens | Hard limit       |

**Increase limit**:

```bash
MAX_MCP_OUTPUT_TOKENS=50000 claude
```

### Authentication

**OAuth 2.0 authentication**:

1. Add server: `claude mcp add --transport http sentry https://mcp.sentry.dev/mcp`
2. Authenticate: `/mcp` in Claude Code
3. Follow browser flow

**Clear authentication**: Use `/mcp` menu → "Clear authentication"

### Enterprise Managed Configuration

**Exclusive control** with `managed-mcp.json`:

Locations:

- macOS: `/Library/Application Support/ClaudeCode/managed-mcp.json`
- Linux/WSL: `/etc/claude-code/managed-mcp.json`
- Windows: `C:\Program Files\ClaudeCode\managed-mcp.json`

```json
{
  "mcpServers": {
    "company-internal": {
      "type": "stdio",
      "command": "/usr/local/bin/company-mcp-server",
      "args": ["--config", "/etc/company/mcp-config.json"]
    }
  }
}
```

**Policy-based control** with allowlists/denylists:

```json
{
  "allowedMcpServers": [
    { "serverName": "github" },
    { "serverCommand": ["npx", "-y", "approved-package"] },
    { "serverUrl": "https://mcp.company.com/*" }
  ],
  "deniedMcpServers": [
    { "serverName": "dangerous-server" },
    { "serverUrl": "https://*.untrusted.com/*" }
  ]
}
```

### DSM MCP Configuration

Example configuration for data-source-manager:

```json
{
  "mcpServers": {
    "dsm-database": {
      "type": "stdio",
      "command": "npx",
      "args": ["-y", "@bytebase/dbhub", "--dsn", "${DSM_DATABASE_URL}"],
      "env": {}
    },
    "dsm-sentry": {
      "type": "http",
      "url": "https://mcp.sentry.dev/mcp"
    }
  }
}
```

### Plugin MCP Servers

Plugins can bundle MCP servers:

**In `.mcp.json` at plugin root**:

```json
{
  "database-tools": {
    "command": "${CLAUDE_PLUGIN_ROOT}/servers/db-server",
    "args": ["--config", "${CLAUDE_PLUGIN_ROOT}/config.json"]
  }
}
```

**Or inline in `plugin.json`**:

```json
{
  "name": "my-plugin",
  "mcpServers": {
    "plugin-api": {
      "command": "${CLAUDE_PLUGIN_ROOT}/servers/api-server"
    }
  }
}
```

### Windows Considerations

On native Windows (not WSL), use `cmd /c` wrapper:

```bash
claude mcp add --transport stdio my-server -- cmd /c npx -y @some/package
```

### Troubleshooting

| Issue                  | Cause                             | Solution                               |
| ---------------------- | --------------------------------- | -------------------------------------- |
| "Connection closed"    | npx without cmd wrapper (Windows) | Use `cmd /c npx ...`                   |
| Server not found       | Wrong scope                       | Check with `claude mcp list`           |
| Authentication failed  | Token expired                     | Use `/mcp` to re-authenticate          |
| Tool not appearing     | Tool Search active                | Tools load on-demand                   |
| Large output warning   | Output exceeds 10k tokens         | Increase `MAX_MCP_OUTPUT_TOKENS`       |
| Project server blocked | Not approved                      | Run `claude mcp reset-project-choices` |

### Best Practices

1. **Use project scope** for team-shared servers (`.mcp.json`)
2. **Use local scope** for personal/sensitive configurations
3. **Use environment variables** for secrets in `.mcp.json`
4. **Prefer HTTP transport** over SSE for remote servers
5. **Enable Tool Search** for many MCP servers
6. **Review servers** before approving from `.mcp.json`
7. **Set appropriate output limits** for data-heavy servers

### Security Considerations

- MCP servers can run arbitrary code
- Only add servers from trusted sources
- Review server configurations before starting
- All actions require explicit approval
- Use enterprise managed settings for organizational control
- Store secrets in environment variables, not config files
---

<!-- SSoT-OK: Section added by autonomous Claude Code infrastructure improvement loop -->

## Batch Processing Reference

Comprehensive guide to the Message Batches API for large-scale asynchronous processing.

### Overview

The Message Batches API enables cost-effective asynchronous processing of large volumes of requests with 50% cost savings.

**Ideal use cases**:

- Large-scale evaluations (thousands of test cases)
- Content moderation (bulk user content analysis)
- Data analysis (insights for large datasets)
- Bulk content generation

### Batch Limitations

| Limit                | Value                       |
| -------------------- | --------------------------- |
| Maximum requests     | 100,000 per batch           |
| Maximum size         | 256 MB per batch            |
| Processing time      | Most complete within 1 hour |
| Expiration           | 24 hours maximum            |
| Results availability | 29 days after creation      |

### Pricing (50% Discount)

| Model               | Batch Input | Batch Output |
| ------------------- | ----------- | ------------ |
| Claude Opus 4.5     | $2.50/MTok  | $12.50/MTok  |
| Claude Opus 4.1/4   | $7.50/MTok  | $37.50/MTok  |
| Claude Sonnet 4.5/4 | $1.50/MTok  | $7.50/MTok   |
| Claude Haiku 4.5    | $0.50/MTok  | $2.50/MTok   |

### Creating a Batch

**Python**:

```python
import anthropic
from anthropic.types.message_create_params import MessageCreateParamsNonStreaming
from anthropic.types.messages.batch_create_params import Request

client = anthropic.Anthropic()

message_batch = client.messages.batches.create(
    requests=[
        Request(
            custom_id="request-1",
            params=MessageCreateParamsNonStreaming(
                model="claude-sonnet-4-5",
                max_tokens=1024,
                messages=[{"role": "user", "content": "Analyze this code..."}]
            )
        ),
        Request(
            custom_id="request-2",
            params=MessageCreateParamsNonStreaming(
                model="claude-sonnet-4-5",
                max_tokens=1024,
                messages=[{"role": "user", "content": "Review this function..."}]
            )
        )
    ]
)
```

### Tracking Batch Status

**Polling for completion**:

```python
import time

while True:
    batch = client.messages.batches.retrieve(batch_id)
    if batch.processing_status == "ended":
        break
    print(f"Batch {batch_id} still processing...")
    time.sleep(60)
```

**Processing status values**:

- `in_progress`: Currently processing
- `canceling`: Cancellation in progress
- `ended`: All requests completed

### Retrieving Results

**Stream results**:

```python
for result in client.messages.batches.results(batch_id):
    match result.result.type:
        case "succeeded":
            print(f"Success: {result.custom_id}")
        case "errored":
            if result.result.error.type == "invalid_request":
                print(f"Validation error: {result.custom_id}")
            else:
                print(f"Server error: {result.custom_id}")
        case "expired":
            print(f"Expired: {result.custom_id}")
```

**Result types**:

| Type        | Description                     | Billed? |
| ----------- | ------------------------------- | ------- |
| `succeeded` | Request completed successfully  | Yes     |
| `errored`   | Request encountered an error    | No      |
| `canceled`  | User canceled before processing | No      |
| `expired`   | Reached 24-hour limit           | No      |

### Prompt Caching with Batches

Combine prompt caching with batches for maximum savings (discounts stack):

```python
requests=[
    Request(
        custom_id="request-1",
        params=MessageCreateParamsNonStreaming(
            model="claude-sonnet-4-5",
            max_tokens=1024,
            system=[
                {"type": "text", "text": "You are a code reviewer."},
                {
                    "type": "text",
                    "text": "<large shared context>",
                    "cache_control": {"type": "ephemeral"}
                }
            ],
            messages=[{"role": "user", "content": "Review code A"}]
        )
    ),
    # More requests with same cached system prompt
]
```

**Cache hit rates**: 30-98% depending on traffic patterns

**Tips for better cache hits**:

1. Include identical `cache_control` blocks in every request
2. Maintain steady request stream (cache expires after 5 minutes)
3. Use 1-hour cache duration for batch workloads

### What Can Be Batched

All Messages API features work in batches:

- Vision (images)
- Tool use
- System messages
- Multi-turn conversations
- Extended thinking
- Beta features

### DSM Batch Patterns

#### Pattern: Bulk Symbol Analysis

```python
symbols = ["BTCUSDT", "ETHUSDT", "BNBUSDT", ...]

requests = [
    Request(
        custom_id=f"analyze-{symbol}",
        params=MessageCreateParamsNonStreaming(
            model="claude-sonnet-4-5",
            max_tokens=2048,
            messages=[{
                "role": "user",
                "content": f"Analyze trading patterns for {symbol}"
            }]
        )
    )
    for symbol in symbols
]

batch = client.messages.batches.create(requests=requests)
```

#### Pattern: Batch Code Review

````python
files = glob.glob("src/**/*.py", recursive=True)

requests = [
    Request(
        custom_id=f"review-{path}",
        params=MessageCreateParamsNonStreaming(
            model="claude-sonnet-4-5",
            max_tokens=4096,
            system="You are a Python code reviewer focusing on FCP patterns.",
            messages=[{
                "role": "user",
                "content": f"Review this file:\n```python\n{open(path).read()}\n```"
            }]
        )
    )
    for path in files[:100]  # Batch limit
]
````

### Managing Batches

**List all batches**:

```python
for batch in client.messages.batches.list(limit=20):
    print(f"{batch.id}: {batch.processing_status}")
```

**Cancel a batch**:

```python
batch = client.messages.batches.cancel(batch_id)
# Status becomes "canceling", then "ended"
```

### Best Practices

1. **Use meaningful custom_ids**: Results may return in any order
2. **Test with Messages API first**: Validate request shape before batching
3. **Break large datasets**: Use multiple batches for manageability
4. **Implement retry logic**: Handle `errored` results appropriately
5. **Monitor processing status**: Poll regularly for completion

### Troubleshooting

| Issue                | Cause                    | Solution                     |
| -------------------- | ------------------------ | ---------------------------- |
| 413 error            | Batch exceeds 256 MB     | Split into smaller batches   |
| Results not in order | Normal behavior          | Use `custom_id` to match     |
| Results unavailable  | Over 29 days old         | Retrieve sooner              |
| High expired count   | Processing took too long | Use smaller batches          |
| Validation errors    | Invalid request params   | Test with Messages API first |
---

<!-- SSoT-OK: Section added by autonomous Claude Code infrastructure improvement loop -->

## Vision & Multimodal Reference

Comprehensive guide to Claude's vision capabilities for image analysis.

### Overview

Claude's vision capabilities enable understanding and analyzing images for multimodal workflows.

**Key use cases**:

- Screenshot analysis and debugging
- OCR (text extraction from images)
- Chart and diagram interpretation
- UI design analysis
- Architecture diagram understanding
- Document and form processing

### Image Limits

| Limit                        | Value                |
| ---------------------------- | -------------------- |
| Maximum per request (API)    | 100 images           |
| Maximum per turn (claude.ai) | 20 images            |
| Maximum dimensions           | 8000x8000 px         |
| With 20+ images              | 2000x2000 px max     |
| File size (API)              | 5 MB per image       |
| File size (claude.ai)        | 10 MB per image      |
| Supported formats            | JPEG, PNG, GIF, WebP |

### Optimal Image Sizes

For best performance (no resizing), keep within these dimensions:

| Aspect Ratio | Image Size   |
| ------------ | ------------ |
| 1:1          | 1092x1092 px |
| 3:4          | 951x1268 px  |
| 2:3          | 896x1344 px  |
| 9:16         | 819x1456 px  |
| 1:2          | 784x1568 px  |

**Token calculation**: `tokens = (width × height) / 750`

### Image Input Methods

**1. Base64 encoding**:

```python
import anthropic
import base64
import httpx

client = anthropic.Anthropic()

image_data = base64.standard_b64encode(
    httpx.get("https://example.com/image.jpg").content
).decode("utf-8")

message = client.messages.create(
    model="claude-sonnet-4-5",
    max_tokens=1024,
    messages=[{
        "role": "user",
        "content": [
            {
                "type": "image",
                "source": {
                    "type": "base64",
                    "media_type": "image/jpeg",
                    "data": image_data
                }
            },
            {"type": "text", "text": "Describe this image."}
        ]
    }]
)
```

**2. URL reference**:

```python
message = client.messages.create(
    model="claude-sonnet-4-5",
    max_tokens=1024,
    messages=[{
        "role": "user",
        "content": [
            {
                "type": "image",
                "source": {
                    "type": "url",
                    "url": "https://example.com/image.jpg"
                }
            },
            {"type": "text", "text": "Describe this image."}
        ]
    }]
)
```

**3. Files API** (upload once, use multiple times):

```python
# Upload file
with open("image.jpg", "rb") as f:
    file_upload = client.beta.files.upload(
        file=("image.jpg", f, "image/jpeg")
    )

# Use in message
message = client.beta.messages.create(
    model="claude-sonnet-4-5",
    max_tokens=1024,
    betas=["files-api-2025-04-14"],
    messages=[{
        "role": "user",
        "content": [
            {
                "type": "image",
                "source": {"type": "file", "file_id": file_upload.id}
            },
            {"type": "text", "text": "Describe this image."}
        ]
    }]
)
```

### Multiple Images

Label images when comparing:

```python
message = client.messages.create(
    model="claude-sonnet-4-5",
    max_tokens=1024,
    messages=[{
        "role": "user",
        "content": [
            {"type": "text", "text": "Image 1:"},
            {"type": "image", "source": {"type": "url", "url": url1}},
            {"type": "text", "text": "Image 2:"},
            {"type": "image", "source": {"type": "url", "url": url2}},
            {"type": "text", "text": "How are these images different?"}
        ]
    }]
)
```

### Claude Code Vision Integration

**Reading screenshots in Claude Code**:

```
User: [Paste screenshot or drag image]
User: "What error is shown in this screenshot?"
```

**Screenshot debugging workflow**:

1. Take screenshot of error/UI issue
2. Paste into Claude Code
3. Ask Claude to analyze and suggest fixes
4. Claude proposes code changes

**Architecture diagram analysis**:

```
User: [Architecture diagram image]
User: "Generate deployment manifests for this architecture"
```

### DSM Vision Patterns

#### Pattern: Error Screenshot Analysis

```
User: [Screenshot of stack trace]
User: "Analyze this FCP error and suggest a fix"
```

#### Pattern: Chart Data Extraction

```
User: [Trading chart image]
User: "Extract the OHLCV data from this chart"
```

#### Pattern: UI-to-Code

```
User: [Dashboard mockup image]
User: "Generate a Streamlit dashboard matching this design"
```

### Best Practices

1. **Place images before text**: Better results with image-then-question structure
2. **Ensure clarity**: Use clear, high-quality, correctly-oriented images
3. **Resize large images**: Pre-resize to reduce latency
4. **Label multiple images**: Use "Image 1:", "Image 2:" prefixes
5. **Avoid tiny images**: Under 200px may degrade performance

### Limitations

| Limitation             | Description                                 |
| ---------------------- | ------------------------------------------- |
| People identification  | Cannot identify/name people in images       |
| Spatial reasoning      | Limited precision for layouts, positions    |
| Counting               | Approximate counts only, not precise        |
| AI-generated detection | Cannot reliably detect AI images            |
| Low-quality images     | May hallucinate on blurry/rotated images    |
| Medical diagnosis      | Not designed for complex diagnostic imaging |

### Image Cost Calculation

| Image Size   | Tokens | Cost (Sonnet 4.5) |
| ------------ | ------ | ----------------- |
| 200x200 px   | ~54    | ~$0.00016         |
| 1000x1000 px | ~1,334 | ~$0.004           |
| 1092x1092 px | ~1,590 | ~$0.0048          |

### Troubleshooting

| Issue                     | Cause                         | Solution                  |
| ------------------------- | ----------------------------- | ------------------------- |
| Image rejected            | Exceeds 8000x8000 px          | Resize before upload      |
| Slow time-to-first-token  | Image too large               | Pre-resize to ≤1.15 MP    |
| Inaccurate interpretation | Low quality/rotated           | Use clear, upright images |
| Metadata not read         | Claude doesn't parse metadata | Include info in prompt    |
| Request too large         | Over 32 MB with images        | Reduce image count/size   |
---

<!-- SSoT-OK: Section added by autonomous Claude Code infrastructure improvement loop -->

## CLI & Headless Mode Reference

Comprehensive guide to Claude Code CLI usage, headless mode, and CI/CD integration.

### Basic CLI Commands

| Command                 | Description                       | Example                             |
| ----------------------- | --------------------------------- | ----------------------------------- |
| `claude`                | Start interactive REPL            | `claude`                            |
| `claude "query"`        | Start REPL with initial prompt    | `claude "explain this project"`     |
| `claude -p "query"`     | Print mode (non-interactive)      | `claude -p "explain this function"` |
| `claude -c`             | Continue most recent conversation | `claude -c`                         |
| `claude -r "<session>"` | Resume session by ID or name      | `claude -r "auth-refactor"`         |
| `claude update`         | Update to latest version          | `claude update`                     |

### Print Mode (-p)

Print mode enables non-interactive usage for scripting and CI/CD:

```bash
# Simple query
claude -p "Analyze this function for bugs"

# Process piped content
cat logs.txt | claude -p "Explain these errors"

# Continue conversation in print mode
claude -c -p "Check for type errors"
```

### Output Formats

| Format        | Flag                          | Use Case                        |
| ------------- | ----------------------------- | ------------------------------- |
| `text`        | `--output-format text`        | Human-readable output (default) |
| `json`        | `--output-format json`        | Parseable JSON for automation   |
| `stream-json` | `--output-format stream-json` | NDJSON streaming for pipelines  |

**JSON output example**:

```bash
claude -p "List all files in src/" --output-format json | jq '.result'
```

**Stream-JSON (NDJSON) for chaining**:

```bash
claude -p --output-format stream-json "First task" \
  | claude -p --input-format stream-json --output-format stream-json "Process" \
  | claude -p --input-format stream-json "Final report"
```

### CI/CD Integration

**GitHub Actions example**:

```yaml
- name: Run Claude Code Analysis
  run: |
    claude -p "If there are linting errors, fix them" \
      --output-format json \
      --max-turns 5 \
      --max-budget-usd 2.00
```

**Key CI/CD flags**:

| Flag                             | Description                            |
| -------------------------------- | -------------------------------------- |
| `--max-turns N`                  | Limit agentic turns (prevents runaway) |
| `--max-budget-usd N`             | Spending limit in dollars              |
| `--dangerously-skip-permissions` | Skip permission prompts                |
| `--no-session-persistence`       | Don't save session to disk             |
| `--fallback-model sonnet`        | Use fallback when overloaded           |

### System Prompt Customization

| Flag                          | Behavior                   | Mode                |
| ----------------------------- | -------------------------- | ------------------- |
| `--system-prompt`             | Replace entire prompt      | Interactive + Print |
| `--system-prompt-file`        | Replace with file contents | Print only          |
| `--append-system-prompt`      | Append to default prompt   | Interactive + Print |
| `--append-system-prompt-file` | Append file contents       | Print only          |

**Examples**:

```bash
# Replace system prompt
claude --system-prompt "You are a Python expert"

# Append to default (recommended)
claude --append-system-prompt "Always use TypeScript"

# Load from file
claude -p --system-prompt-file ./prompts/review.txt "Review this PR"
```

### Tool Control

**Allow specific tools**:

```bash
claude --allowedTools "Bash(git log *)" "Read" "Glob"
```

**Restrict available tools**:

```bash
claude --tools "Bash,Edit,Read"

# Disable all tools
claude --tools ""
```

**Disallow specific tools**:

```bash
claude --disallowedTools "Bash(rm *)" "Edit"
```

### Custom Agents via CLI

Define subagents dynamically:

```bash
claude --agents '{
  "code-reviewer": {
    "description": "Expert code reviewer",
    "prompt": "You are a senior code reviewer",
    "tools": ["Read", "Grep", "Glob"],
    "model": "sonnet"
  }
}'
```

**Agent definition fields**:

| Field         | Required | Description                             |
| ------------- | -------- | --------------------------------------- |
| `description` | Yes      | When to invoke the subagent             |
| `prompt`      | Yes      | System prompt for subagent              |
| `tools`       | No       | Array of allowed tools                  |
| `model`       | No       | `sonnet`, `opus`, `haiku`, or `inherit` |

### Permission Modes

```bash
# Start in plan mode
claude --permission-mode plan

# Allow bypass as option (composable)
claude --permission-mode plan --allow-dangerously-skip-permissions

# Skip all permissions (CI/CD)
claude --dangerously-skip-permissions
```

### Session Management

**Resume by ID or name**:

```bash
claude --resume auth-refactor
claude -r abc123def456
```

**Fork session (new ID)**:

```bash
claude --resume abc123 --fork-session
```

**Specify session ID**:

```bash
claude --session-id "550e8400-e29b-41d4-a716-446655440000"
```

### Additional Directories

Grant Claude access to directories outside working directory:

```bash
claude --add-dir ../apps ../lib ../shared
```

### MCP Configuration via CLI

```bash
# Load MCP from file
claude --mcp-config ./mcp.json

# Use only specified MCP config
claude --strict-mcp-config --mcp-config ./mcp.json
```

### Structured Output (JSON Schema)

Get validated JSON output:

```bash
claude -p --json-schema '{
  "type": "object",
  "properties": {
    "functions": {"type": "array"},
    "complexity": {"type": "number"}
  }
}' "Analyze src/main.py"
```

### DSM CLI Patterns

#### Pattern: Automated Code Review

```bash
#!/bin/bash
claude -p \
  --output-format json \
  --max-turns 3 \
  --append-system-prompt "Focus on FCP patterns and silent failures" \
  "Review changes in src/fcp/" \
  | jq '.result.text'
```

#### Pattern: Batch File Processing

```bash
for file in src/**/*.py; do
  claude -p \
    --output-format json \
    --no-session-persistence \
    "Check $file for FCP compliance" \
    | jq -r '.result.text' >> review.log
done
```

#### Pattern: CI Pipeline Integration

```bash
# Run analysis with budget limit
result=$(claude -p \
  --output-format json \
  --max-budget-usd 1.00 \
  --max-turns 5 \
  "Fix linting errors in staged files")

# Check for errors
if echo "$result" | jq -e '.error' > /dev/null; then
  echo "Analysis failed"
  exit 1
fi
```

### Environment Variables

| Variable                  | Description                |
| ------------------------- | -------------------------- |
| `ANTHROPIC_API_KEY`       | API key for authentication |
| `CLAUDE_CODE_USE_BEDROCK` | Use AWS Bedrock (`true`)   |
| `CLAUDE_CODE_USE_VERTEX`  | Use Google Vertex (`true`) |
| `ENABLE_TOOL_SEARCH`      | Tool search behavior       |
| `MAX_MCP_OUTPUT_TOKENS`   | MCP output limit           |
| `MCP_TIMEOUT`             | MCP server startup timeout |

### Verbose and Debug Modes

```bash
# Verbose output (turn-by-turn)
claude --verbose -p "query"

# Debug with category filter
claude --debug "api,mcp" -p "query"

# Debug excluding categories
claude --debug "!statsig,!file" -p "query"
```

### Best Practices for CI/CD

1. **Always set limits**: Use `--max-turns` and `--max-budget-usd`
2. **Use JSON output**: Parse results programmatically
3. **Disable persistence**: Use `--no-session-persistence` for CI
4. **Set fallback model**: Use `--fallback-model sonnet` for reliability
5. **Skip permissions carefully**: Only use `--dangerously-skip-permissions` in trusted CI
6. **Log verbose output**: Use `--verbose` for debugging CI failures

### Troubleshooting CLI

| Issue               | Cause                    | Solution                               |
| ------------------- | ------------------------ | -------------------------------------- |
| Hangs on permission | Interactive prompt in CI | Use `--dangerously-skip-permissions`   |
| Output truncated    | Default text format      | Use `--output-format json`             |
| Budget exceeded     | Long-running task        | Set `--max-budget-usd` limit           |
| Session not found   | Invalid ID               | Use `--resume` without ID for picker   |
| MCP timeout         | Slow server startup      | Set `MCP_TIMEOUT` environment variable |
<!-- SSoT-OK: Hooks Reference - comprehensive hooks documentation from official docs -->

## Hooks Reference

### Overview

Hooks allow you to run custom shell commands at specific points during Claude Code's execution. They enable automation, custom validations, security enforcement, and integration with external tools.

### Hook Events

| Event              | Trigger                                      | Common Uses                                   |
| ------------------ | -------------------------------------------- | --------------------------------------------- |
| `PreToolUse`       | Before tool execution                        | Block dangerous commands, validate parameters |
| `PostToolUse`      | After tool execution                         | Detect patterns in output, log tool usage     |
| `SessionStart`     | When session begins                          | Load context, verify environment, show status |
| `Stop`             | When Claude produces final response          | Final validation, cleanup, notifications      |
| `Notification`     | When notification would be shown             | Custom alerts, phone notifications            |
| `UserPromptSubmit` | When user submits prompt (before processing) | Keyword detection, skill suggestions          |

### Configuration Format

Hooks are configured in `settings.json` under the `hooks` key:

```json
{
  "hooks": {
    "PreToolUse": [
      {
        "matcher": "Bash",
        "hooks": [
          {
            "type": "command",
            "command": "/path/to/validator.sh \"$TOOL_INPUT\""
          }
        ]
      }
    ],
    "PostToolUse": [
      {
        "matcher": "Write|Edit",
        "hooks": [
          {
            "type": "command",
            "command": "lint-check.sh \"$TOOL_INPUT\""
          }
        ]
      }
    ],
    "Stop": [
      {
        "matcher": "",
        "hooks": [
          {
            "type": "command",
            "command": "notify-complete.sh"
          }
        ]
      }
    ]
  }
}
```

### Matcher Patterns

Matchers use regex to filter which tool calls trigger the hook:

| Pattern               | Matches                            |
| --------------------- | ---------------------------------- |
| `"Bash"`              | All Bash tool calls                |
| `"Write\|Edit"`       | Write or Edit tool calls           |
| `""`                  | All tool calls (empty = match all) |
| `"Bash\\(git.*\\)"`   | Bash commands starting with git    |
| `"Read\\(.*\\.md\\)"` | Read calls for markdown files      |

### Exit Codes

| Exit Code | Meaning                                          |
| --------- | ------------------------------------------------ |
| 0         | Success - continue execution                     |
| 1         | Error - show stderr as error                     |
| 2         | Block - prevent tool execution (PreToolUse only) |

For `PreToolUse` hooks:

- Exit 0: Allow tool to proceed
- Exit 2: Block tool execution, show reason from stdout/stderr
- Exit 1: Show error but continue

### JSON Output Format

Hooks can return structured JSON for rich feedback:

```json
{
  "continue": true,
  "message": "Warning: Pattern detected",
  "severity": "warning"
}
```

Fields:

- `continue`: boolean - whether to proceed (true) or block (false)
- `message`: string - message to display
- `severity`: "info" | "warning" | "error"
- `stopReason`: string - reason for blocking (PreToolUse)

### Environment Variables

Available in all hooks:

| Variable          | Description                                       |
| ----------------- | ------------------------------------------------- |
| `$TOOL_NAME`      | Name of the tool (Bash, Write, Edit, etc.)        |
| `$TOOL_INPUT`     | JSON string of tool input parameters              |
| `$TOOL_OUTPUT`    | Tool output (PostToolUse only)                    |
| `$SESSION_ID`     | Current session identifier                        |
| `$CLAUDE_PROJECT` | Current project directory                         |
| `$CLAUDE_MODEL`   | Model being used (claude-sonnet-4-20250514, etc.) |

For Bash hooks specifically:

- `$BASH_COMMAND`: The actual command being executed

### PreToolUse Hook Examples

**Block Dangerous Commands**:

```bash
#!/bin/bash
# dsm-bash-guard.sh - Block dangerous bash commands

TOOL_INPUT="$1"
COMMAND=$(echo "$TOOL_INPUT" | jq -r '.command // empty')

# Block force push to main
if echo "$COMMAND" | grep -qE 'git push.*--force.*(main|master)'; then
  echo '{"continue": false, "message": "Force push to main/master blocked", "severity": "error"}'
  exit 2
fi

# Block pip install (use uv)
if echo "$COMMAND" | grep -qE '^pip install'; then
  echo '{"continue": false, "message": "Use uv instead of pip", "severity": "error"}'
  exit 2
fi

exit 0
```

**Validate File Paths**:

```bash
#!/bin/bash
# Block writes to sensitive files

TOOL_INPUT="$1"
FILE_PATH=$(echo "$TOOL_INPUT" | jq -r '.file_path // empty')

if [[ "$FILE_PATH" == *".env"* ]] || [[ "$FILE_PATH" == *"secrets"* ]]; then
  echo '{"continue": false, "message": "Cannot write to sensitive files"}'
  exit 2
fi

exit 0
```

### PostToolUse Hook Examples

**Detect Anti-Patterns**:

```bash
#!/bin/bash
# dsm-code-guard.sh - Detect silent failure patterns after Write/Edit

TOOL_OUTPUT="$1"

# Check for bare except
if echo "$TOOL_OUTPUT" | grep -qE 'except:\s*$|except:\s*pass'; then
  echo '{"continue": true, "message": "Warning: Bare except detected", "severity": "warning"}'
  exit 0
fi

# Check for subprocess without check=True
if echo "$TOOL_OUTPUT" | grep -qE 'subprocess\.(run|call|Popen)' && \
   ! echo "$TOOL_OUTPUT" | grep -q 'check=True'; then
  echo '{"continue": true, "message": "Warning: subprocess without check=True", "severity": "warning"}'
  exit 0
fi

exit 0
```

**Log Tool Usage**:

```bash
#!/bin/bash
# Log all tool usage for analytics

echo "$(date -Iseconds) | $TOOL_NAME | $SESSION_ID" >> ~/.claude/tool-usage.log
exit 0
```

### SessionStart Hook Examples

**Load Context**:

```bash
#!/bin/bash
# dsm-session-start.sh - Load FCP context at session start

# Display project status
echo "DSM Session Started"
echo "Python: $(python --version)"
echo "FCP Cache: $(ls -la .cache/fcp/ 2>/dev/null | wc -l) entries"

# Check for uncommitted changes
if ! git diff --quiet; then
  echo '{"continue": true, "message": "Warning: Uncommitted changes detected", "severity": "warning"}'
fi

exit 0
```

### Stop Hook Examples

**Final Validation**:

```bash
#!/bin/bash
# dsm-final-check.sh - Run at session end

# Check for uncommitted test files
if git status --porcelain | grep -q 'tests/'; then
  echo '{"continue": true, "message": "Note: Uncommitted test files", "severity": "info"}'
fi

# Run quick lint check
if command -v ruff &>/dev/null; then
  ERRORS=$(ruff check src/ 2>&1 | grep -c 'error' || true)
  if [ "$ERRORS" -gt 0 ]; then
    echo '{"continue": true, "message": "Lint errors detected", "severity": "warning"}'
  fi
fi

exit 0
```

**Send Notification**:

```bash
#!/bin/bash
# Notify when long task completes

# macOS notification
osascript -e 'display notification "Claude Code task complete" with title "Claude Code"'

# Or send to phone via ntfy
# curl -s -d "Task complete" ntfy.sh/my-topic

exit 0
```

### Notification Hook Examples

**Custom Alert System**:

```bash
#!/bin/bash
# Route notifications to custom system

MESSAGE="$1"

# Send to ntfy for phone notifications
curl -s -d "$MESSAGE" ntfy.sh/claude-alerts

# Also log locally
echo "$(date -Iseconds) | $MESSAGE" >> ~/.claude/notifications.log

exit 0
```

### UserPromptSubmit Hook Examples

**Keyword Detection and Skill Suggestion**:

```bash
#!/bin/bash
# dsm-skill-suggest.sh - Suggest relevant skills based on keywords

PROMPT="$1"

# FCP-related keywords
if echo "$PROMPT" | grep -qiE 'fcp|failover|cache|retry'; then
  echo '{"continue": true, "message": "Tip: Use /debug-fcp for FCP issues", "severity": "info"}'
  exit 0
fi

# Testing keywords
if echo "$PROMPT" | grep -qiE 'test|spec|coverage'; then
  echo '{"continue": true, "message": "Tip: Use /quick-test for testing", "severity": "info"}'
  exit 0
fi

exit 0
```

### Hook Configuration Scopes

Hooks can be configured at multiple levels:

| Scope          | Location                      | Priority |
| -------------- | ----------------------------- | -------- |
| User global    | `~/.claude/settings.json`     | Lowest   |
| Project shared | `.claude/settings.json`       | Medium   |
| Project local  | `.claude/settings.local.json` | Highest  |

Hooks from all scopes are merged, with higher priority hooks running first.

### Plugin Hooks

Plugins can register hooks via their `plugin.json` manifest:

```json
{
  "name": "my-plugin",
  "hooks": {
    "PreToolUse": [
      {
        "matcher": "Bash",
        "hooks": [
          {
            "type": "command",
            "command": "{{PLUGIN_DIR}}/hooks/bash-guard.sh"
          }
        ]
      }
    ]
  }
}
```

The `{{PLUGIN_DIR}}` placeholder is replaced with the plugin's installation directory.

### Skill and Agent Hooks

Skills and agents can define hooks in their YAML frontmatter:

```yaml
---
name: dsm-testing
hooks:
  PostToolUse:
    - matcher: "Write|Edit"
      command: "./hooks/lint-check.sh"
---
```

### Best Practices

**Performance**:

- Keep hooks fast (< 100ms) to avoid slowing interaction
- Use exit 0 early for non-matching cases
- Cache expensive computations

**Reliability**:

- Always handle missing environment variables gracefully
- Use `set -e` with caution (may cause unexpected exits)
- Test hooks independently before deployment

**Security**:

- Never echo sensitive data in hook output
- Validate all inputs before using in shell commands
- Use absolute paths for executables

**Debugging**:

- Add `--verbose` flag for development debugging
- Log to file for post-mortem analysis
- Use JSON output for structured error reporting

### Debugging Hooks

**View Hook Execution**:

```bash
# Enable hook debugging
export CLAUDE_HOOK_DEBUG=1
claude
```

**Test Hook Independently**:

```bash
# Test PreToolUse hook
echo '{"command": "git push --force origin main"}' | ./hooks/bash-guard.sh
echo "Exit code: $?"
```

**Check Hook Configuration**:

```bash
# View merged hook configuration
claude config hooks --show
```

### DSM Hook Implementation

The data-source-manager uses 5 hooks covering the complete lifecycle:

| Hook                 | Event            | Purpose                                    |
| -------------------- | ---------------- | ------------------------------------------ |
| dsm-session-start.sh | SessionStart     | Load FCP context at session start          |
| dsm-skill-suggest.sh | UserPromptSubmit | Suggest relevant skills based on keywords  |
| dsm-bash-guard.sh    | PreToolUse       | Block dangerous commands before execution  |
| dsm-code-guard.sh    | PostToolUse      | Detect silent failure patterns (11 checks) |
| dsm-final-check.sh   | Stop             | Final validation at session end            |

**Configuration** (`.claude/hooks/hooks.json`):

```json
{
  "hooks": {
    "SessionStart": [
      {
        "matcher": "",
        "hooks": [
          {
            "type": "command",
            "command": ".claude/hooks/dsm-session-start.sh"
          }
        ]
      }
    ],
    "UserPromptSubmit": [
      {
        "matcher": "",
        "hooks": [
          {
            "type": "command",
            "command": ".claude/hooks/dsm-skill-suggest.sh \"$PROMPT\""
          }
        ]
      }
    ],
    "PreToolUse": [
      {
        "matcher": "Bash",
        "hooks": [
          {
            "type": "command",
            "command": ".claude/hooks/dsm-bash-guard.sh \"$TOOL_INPUT\""
          }
        ]
      }
    ],
    "PostToolUse": [
      {
        "matcher": "Write|Edit",
        "hooks": [
          {
            "type": "command",
            "command": ".claude/hooks/dsm-code-guard.sh \"$TOOL_OUTPUT\""
          }
        ]
      }
    ],
    "Stop": [
      {
        "matcher": "",
        "hooks": [
          {
            "type": "command",
            "command": ".claude/hooks/dsm-final-check.sh"
          }
        ]
      }
    ]
  }
}
```

### Hook Antipatterns to Avoid

| Antipattern                    | Problem                             | Solution                             |
| ------------------------------ | ----------------------------------- | ------------------------------------ |
| Slow hooks (>1s)               | Blocks interaction                  | Optimize or run async                |
| Exit 1 for validation failures | Doesn't block, just shows error     | Use exit 2 for PreToolUse blocking   |
| Unbounded loops                | Hook never completes                | Add timeout or iteration limit       |
| External API calls             | Network dependency, slow            | Cache results, use background jobs   |
| Echo sensitive data            | Exposes secrets in output           | Redact or suppress output            |
| Missing error handling         | Unexpected failures                 | Wrap in try-catch equivalent         |
| gh CLI in hooks                | Process storms (recursive spawning) | Use direct API calls or cache tokens |

### Integration with CI/CD

Hooks can be used to integrate Claude Code with CI/CD pipelines:

```bash
#!/bin/bash
# ci-integration-hook.sh - Report to CI system

if [ -n "$CI" ]; then
  # Running in CI environment
  curl -X POST "$CI_WEBHOOK_URL" \
    -H "Content-Type: application/json" \
    -d "{\"event\": \"$TOOL_NAME\", \"session\": \"$SESSION_ID\"}"
fi

exit 0
```
<!-- SSoT-OK: Subagent Orchestration Reference - comprehensive subagent patterns from official docs -->

## Subagent Orchestration Reference

### Overview

Subagents are specialized AI assistants that handle specific types of tasks. Each subagent runs in its own context window with a custom system prompt, specific tool access, and independent permissions. When Claude encounters a task that matches a subagent's description, it delegates to that subagent, which works independently and returns results.

### Benefits of Subagents

| Benefit                 | Description                                           |
| ----------------------- | ----------------------------------------------------- |
| Context preservation    | Keep exploration out of main conversation             |
| Constraint enforcement  | Limit which tools a subagent can use                  |
| Configuration reuse     | Share subagents across projects with user-level scope |
| Behavior specialization | Focused system prompts for specific domains           |
| Cost control            | Route tasks to faster, cheaper models like Haiku      |

### Built-in Subagents

Claude Code includes several built-in subagents:

| Subagent          | Model   | Tools     | Purpose                                  |
| ----------------- | ------- | --------- | ---------------------------------------- |
| Explore           | Haiku   | Read-only | File discovery, code search, exploration |
| Plan              | Inherit | Read-only | Codebase research for planning           |
| general-purpose   | Inherit | All tools | Complex research, multi-step operations  |
| Bash              | Inherit | Bash      | Running terminal commands separately     |
| Claude Code Guide | Haiku   | Read-only | Questions about Claude Code features     |

**Explore Agent Thoroughness Levels**:

- `quick` - Targeted lookups, specific file searches
- `medium` - Balanced exploration, moderate depth
- `very thorough` - Comprehensive analysis, multiple locations

### Subagent vs Task Tool

| Aspect           | Task Tool             | Custom Subagents             |
| ---------------- | --------------------- | ---------------------------- |
| Persistence      | Ephemeral workers     | Persistent specialists       |
| Context overhead | ~20k tokens startup   | ~20k tokens startup          |
| Configuration    | Ad-hoc per invocation | Saved in markdown files      |
| Parallelism      | Up to 10 concurrent   | Up to 10 concurrent          |
| Best for         | One-off parallel work | Repeatable specialized tasks |

### Subagent Scopes

Subagents are loaded from different locations with priority:

| Location                     | Scope                   | Priority    |
| ---------------------------- | ----------------------- | ----------- |
| `--agents` CLI flag          | Current session         | 1 (highest) |
| `.claude/agents/`            | Current project         | 2           |
| `~/.claude/agents/`          | All your projects       | 3           |
| Plugin's `agents/` directory | Where plugin is enabled | 4 (lowest)  |

### Subagent File Format

Subagent files use YAML frontmatter followed by the system prompt:

```markdown
---
name: code-reviewer
description: Reviews code for quality and best practices
tools: Read, Glob, Grep
model: sonnet
---

You are a code reviewer. When invoked, analyze the code and provide
specific, actionable feedback on quality, security, and best practices.
```

### Frontmatter Fields

| Field             | Required | Description                                                      |
| ----------------- | -------- | ---------------------------------------------------------------- |
| `name`            | Yes      | Unique identifier using lowercase letters and hyphens            |
| `description`     | Yes      | When Claude should delegate to this subagent                     |
| `tools`           | No       | Tools the subagent can use (inherits all if omitted)             |
| `disallowedTools` | No       | Tools to deny, removed from inherited list                       |
| `model`           | No       | Model: `sonnet`, `opus`, `haiku`, or `inherit`                   |
| `permissionMode`  | No       | `default`, `acceptEdits`, `dontAsk`, `bypassPermissions`, `plan` |
| `skills`          | No       | Skills to load into subagent's context at startup                |
| `hooks`           | No       | Lifecycle hooks scoped to this subagent                          |

### Tool Categories

Configure subagent tools based on their role:

| Role Type     | Recommended Tools                       | Example Use Case      |
| ------------- | --------------------------------------- | --------------------- |
| Read-only     | Read, Grep, Glob                        | Reviewers, auditors   |
| Research      | Read, Grep, Glob, WebFetch, WebSearch   | Analysts, researchers |
| Code writers  | Read, Write, Edit, Bash, Glob, Grep     | Developers, engineers |
| Documentation | Read, Write, Edit, Glob, Grep, WebFetch | Writers, documenters  |

### Permission Modes

| Mode                | Behavior                                      |
| ------------------- | --------------------------------------------- |
| `default`           | Standard permission checking with prompts     |
| `acceptEdits`       | Auto-accept file edits                        |
| `dontAsk`           | Auto-deny permission prompts                  |
| `bypassPermissions` | Skip all permission checks (use with caution) |
| `plan`              | Plan mode (read-only exploration)             |

### Preloading Skills

Inject skill content at startup for domain knowledge:

```yaml
---
name: api-developer
description: Implement API endpoints following team conventions
skills:
  - api-conventions
  - error-handling-patterns
---
Implement API endpoints. Follow the conventions from the preloaded skills.
```

### Subagent Hooks

Hooks can run during subagent lifecycle:

**In Subagent Frontmatter**:

| Event         | Matcher Input | When It Fires                   |
| ------------- | ------------- | ------------------------------- |
| `PreToolUse`  | Tool name     | Before the subagent uses a tool |
| `PostToolUse` | Tool name     | After the subagent uses a tool  |
| `Stop`        | (none)        | When the subagent finishes      |

**In Project settings.json**:

| Event           | Matcher Input   | When It Fires                    |
| --------------- | --------------- | -------------------------------- |
| `SubagentStart` | Agent type name | When a subagent begins execution |
| `SubagentStop`  | Agent type name | When a subagent completes        |

### Orchestration Patterns

**Sequential Refinement Pattern**:

```
Agent 1: "Find authentication code" → Returns 50 files
Agent 2: "Focus on JWT implementation in these 10 files" → JWT analysis
Agent 3: "Check these 3 security concerns in JWT code" → Security audit
```

**Parallel Background Pattern**:

```
User: "Prepare for deployment"

Background Task 1: Run test suite
Background Task 2: Build production assets
Background Task 3: Run security audit
Background Task 4: Update dependencies

All complete → Review results → Deploy
```

**Chain Subagents Pattern**:

```
Use the code-reviewer subagent to find performance issues,
then use the optimizer subagent to fix them
```

**Isolate High-Volume Operations**:

```
Use a subagent to run the test suite and report only
the failing tests with their error messages
```

### Foreground vs Background

| Mode       | Behavior                                       |
| ---------- | ---------------------------------------------- |
| Foreground | Blocks main conversation, prompts pass through |
| Background | Runs concurrently, permissions pre-approved    |

**Background Mode Notes**:

- Claude prompts for permissions upfront before launching
- MCP tools are not available in background subagents
- If clarifying questions fail, subagent continues
- Use Ctrl+B to background a running task
- Set `CLAUDE_CODE_DISABLE_BACKGROUND_TASKS=1` to disable

### Resuming Subagents

Subagents can be resumed to continue previous work:

```
Use the code-reviewer subagent to review the authentication module
[Agent completes]

Continue that code review and now analyze the authorization logic
[Claude resumes the subagent with full context]
```

Transcripts persist in `~/.claude/projects/{project}/{sessionId}/subagents/agent-{agentId}.jsonl`.

### When to Use Subagents vs Main Conversation

**Use Main Conversation When**:

- Task needs frequent back-and-forth or iterative refinement
- Multiple phases share significant context
- Making a quick, targeted change
- Latency matters (subagents start fresh)

**Use Subagents When**:

- Task produces verbose output you don't need
- You want to enforce specific tool restrictions
- Work is self-contained and can return a summary

### Example Subagents

**Code Reviewer** (Read-only):

```markdown
---
name: code-reviewer
description: Expert code review specialist. Use proactively after writing or modifying code.
tools: Read, Grep, Glob, Bash
model: inherit
---

You are a senior code reviewer ensuring high standards.

When invoked:

1. Run git diff to see recent changes
2. Focus on modified files
3. Begin review immediately

Review checklist:

- Code is clear and readable
- Functions are well-named
- No duplicated code
- Proper error handling
- No exposed secrets
- Input validation implemented

Provide feedback organized by priority:

- Critical issues (must fix)
- Warnings (should fix)
- Suggestions (consider improving)
```

**Debugger** (With Edit access):

```markdown
---
name: debugger
description: Debugging specialist for errors, test failures, and unexpected behavior.
tools: Read, Edit, Bash, Grep, Glob
---

You are an expert debugger specializing in root cause analysis.

When invoked:

1. Capture error message and stack trace
2. Identify reproduction steps
3. Isolate the failure location
4. Implement minimal fix
5. Verify solution works

For each issue, provide:

- Root cause explanation
- Evidence supporting diagnosis
- Specific code fix
- Testing approach
- Prevention recommendations
```

**Database Query Validator** (With Hook validation):

```markdown
---
name: db-reader
description: Execute read-only database queries.
tools: Bash
hooks:
  PreToolUse:
    - matcher: "Bash"
      hooks:
        - type: command
          command: "./scripts/validate-readonly-query.sh"
---

You are a database analyst with read-only access. Execute SELECT queries only.

You cannot modify data. If asked to INSERT, UPDATE, DELETE, explain you only have read access.
```

### CLI-Defined Subagents

Pass subagents as JSON when launching Claude Code:

```bash
claude --agents '{
  "code-reviewer": {
    "description": "Expert code reviewer. Use proactively after code changes.",
    "prompt": "You are a senior code reviewer. Focus on code quality, security, and best practices.",
    "tools": ["Read", "Grep", "Glob", "Bash"],
    "model": "sonnet"
  }
}'
```

### Disabling Subagents

Add to `deny` array in settings:

```json
{
  "permissions": {
    "deny": ["Task(Explore)", "Task(my-custom-agent)"]
  }
}
```

Or use CLI flag:

```bash
claude --disallowedTools "Task(Explore)"
```

### Context Management

**Auto-Compaction**:

- Triggers at ~95% capacity by default
- Set `CLAUDE_AUTOCOMPACT_PCT_OVERRIDE` for earlier compaction
- Compaction events logged in transcript files

**Transcript Persistence**:

- Stored in separate files from main conversation
- Persist within session for resumption
- Cleaned up based on `cleanupPeriodDays` setting (default: 30 days)

### DSM Subagent Implementation

The data-source-manager uses 5 specialized subagents:

| Agent                 | Model   | Tools                        | Purpose                    |
| --------------------- | ------- | ---------------------------- | -------------------------- |
| api-reviewer          | Inherit | Read, Grep, Glob             | API consistency review     |
| data-fetcher          | Inherit | Read, Grep, Glob, Bash       | FCP-aware data fetching    |
| test-writer           | Inherit | Read, Write, Edit, Bash, ... | DSM test pattern following |
| silent-failure-hunter | Inherit | Read, Grep, Glob             | Silent failure detection   |
| fcp-debugger          | Inherit | Read, Grep, Glob, Bash       | FCP issue diagnosis        |

**Example DSM Agent** (`.claude/agents/fcp-debugger.md`):

```markdown
---
name: fcp-debugger
description: Diagnoses FCP issues including cache misses, retry storms, and provider failures.
tools: Read, Grep, Glob, Bash
model: inherit
---

You are an FCP (Failover Control Protocol) debugging specialist for data-source-manager.

When invoked:

1. Check FCP cache state in .cache/fcp/
2. Analyze retry logs for patterns
3. Verify provider status and fallback chain
4. Identify root cause of cache misses

FCP Decision Matrix:

- Cache hit → Return cached data
- Cache miss + primary up → Fetch and cache
- Cache miss + primary down → Fallback to secondary
- All providers down → Return stale cache with warning

Debug checklist:

- [ ] Cache directory exists and is writable
- [ ] Cache entries not expired
- [ ] Provider health check passing
- [ ] Retry count within limits
- [ ] Fallback chain configured correctly

Output format:

- Issue summary
- Root cause analysis
- Recommended fix
- Prevention steps
```

### Best Practices

**Design Principles**:

1. **Design focused subagents** - Each should excel at one specific task
2. **Write detailed descriptions** - Claude uses description for delegation
3. **Limit tool access** - Grant only necessary permissions
4. **Check into version control** - Share project subagents with team

**Performance Tips**:

- Use Haiku for fast, read-only operations
- Use Sonnet for balanced capability and speed
- Use Opus for complex reasoning tasks
- Keep subagent context focused to minimize overhead

**Orchestration Tips**:

- Spawn multiple subagents for independent investigations
- Chain subagents for multi-step workflows
- Use background mode for long-running tasks
- Resume subagents to continue previous work
<!-- SSoT-OK: Prompt Engineering Reference - Claude 4.x best practices from official docs -->

## Prompt Engineering Reference

### Overview

This section covers prompt engineering techniques for Claude 4.x models (Sonnet 4.5, Haiku 4.5, Opus 4.5), which have been trained for more precise instruction following than previous generations.

### General Principles

#### Be Explicit with Instructions

Claude 4.x models respond well to clear, explicit instructions:

| Less Effective                | More Effective                                                               |
| ----------------------------- | ---------------------------------------------------------------------------- |
| Create an analytics dashboard | Create an analytics dashboard. Include as many relevant features as possible |
| Fix the bug                   | Fix the bug by implementing proper input validation                          |
| Can you suggest some changes? | Change this function to improve its performance                              |

#### Add Context for Motivation

Provide context explaining why behavior is important:

| Less Effective      | More Effective                                                                                  |
| ------------------- | ----------------------------------------------------------------------------------------------- |
| NEVER use ellipses  | Your response will be read aloud by TTS, so never use ellipses since TTS won't pronounce them   |
| Use short responses | Keep responses under 100 words because this is for a mobile interface with limited screen space |

### XML Tags for Structure

XML tags help Claude parse prompts more accurately:

```xml
<instructions>
Analyze the code and provide feedback.
</instructions>

<example>
Input: function add(a, b) { return a + b; }
Output: Well-structured function with clear naming.
</example>

<formatting>
Use bullet points for issues.
Provide code examples for fixes.
</formatting>
```

**When to Use XML Tags**:

- Incorporating large amounts of data
- Separating different prompt components
- When clarity between sections matters
- Complex multi-part instructions

**Modern Note**: While XML tags are helpful, modern models understand structure without them. Start with explicit, clear instructions.

### System Prompt Structure

A good system prompt reads like a short contract:

```text
You are: [role - one line]
Goal: [what success looks like]
Constraints: [list]
If unsure: Say so explicitly and ask 1 clarifying question
Output format: [JSON schema OR heading structure OR bullet format]
```

**Example System Prompt**:

```text
You are: A senior code reviewer for a Python trading system
Goal: Identify potential bugs, security issues, and performance problems
Constraints:
- Focus on code correctness over style
- Flag any hardcoded values as potential issues
- Check for proper error handling
If unsure: Ask for clarification about business requirements
Output format:
### Critical Issues
- [issue]: [explanation]
### Warnings
- [warning]: [explanation]
### Suggestions
- [suggestion]: [explanation]
```

### Tool Usage Guidance

Claude 4.x models follow precise instructions about tool usage:

**Proactive Action**:

```xml
<default_to_action>
By default, implement changes rather than only suggesting them.
If the user's intent is unclear, infer the most useful likely action and proceed.
Try to infer the user's intent about whether a tool call is intended or not.
</default_to_action>
```

**Conservative Action**:

```xml
<do_not_act_before_instructions>
Do not jump into implementation unless clearly instructed to make changes.
When the user's intent is ambiguous, default to providing information and recommendations.
Only proceed with edits when the user explicitly requests them.
</do_not_act_before_instructions>
```

### Parallel Tool Calling

Claude 4.x models excel at parallel tool execution:

```xml
<use_parallel_tool_calls>
If you intend to call multiple tools and there are no dependencies between them,
make all independent calls in parallel. Prioritize calling tools simultaneously
whenever actions can be done in parallel. For example, when reading 3 files,
run 3 tool calls in parallel to read all files into context at the same time.

However, if some tool calls depend on previous calls to inform dependent values,
do NOT call these tools in parallel. Never use placeholders or guess missing parameters.
</use_parallel_tool_calls>
```

**To Reduce Parallel Execution**:

```text
Execute operations sequentially with brief pauses between each step to ensure stability.
```

### Long-Horizon Reasoning

Claude 4.5 models excel at long-horizon reasoning with exceptional state tracking:

**Multi-Context Window Workflows**:

1. **First context window**: Set up framework (write tests, create setup scripts)
2. **Future context windows**: Iterate on todo-list
3. **Write tests in structured format**: Track in `tests.json`
4. **Create setup scripts**: `init.sh` for graceful server starts, test suites
5. **Starting fresh vs compacting**: Consider fresh context over compaction

**State Management**:

```json
// Structured state file (tests.json)
{
  "tests": [
    { "id": 1, "name": "authentication_flow", "status": "passing" },
    { "id": 2, "name": "user_management", "status": "failing" }
  ],
  "total": 200,
  "passing": 150,
  "failing": 25
}
```

```text
// Progress notes (progress.txt)
Session 3 progress:
- Fixed authentication token validation
- Updated user model to handle edge cases
- Next: investigate user_management test failures
```

**Context Awareness Prompt**:

```text
Your context window will be automatically compacted as it approaches its limit,
allowing you to continue working indefinitely from where you left off.
Therefore, do not stop tasks early due to token budget concerns.

As you approach your token budget limit, save your current progress and state
to memory before the context window refreshes. Always be as persistent and
autonomous as possible and complete tasks fully.
```

### Extended Thinking

Claude 4.x models benefit from thinking capabilities:

```text
After receiving tool results, carefully reflect on their quality and determine
optimal next steps before proceeding. Use your thinking to plan and iterate
based on this new information, and then take the best next action.
```

**Note**: When extended thinking is disabled, Claude Opus 4.5 is sensitive to the word "think". Replace with "consider", "believe", or "evaluate".

### Format Control

**Tell Claude What to Do (Not What Not to Do)**:

| Less Effective                       | More Effective                                             |
| ------------------------------------ | ---------------------------------------------------------- |
| Do not use markdown in your response | Your response should be composed of smoothly flowing prose |
| Don't use bullet points              | Write in clear, flowing paragraphs                         |

**Use XML Format Indicators**:

```text
Write the prose sections of your response in <smoothly_flowing_prose_paragraphs> tags.
```

**Minimize Markdown and Bullets**:

````xml
<avoid_excessive_markdown_and_bullet_points>
When writing reports, documents, or long-form content, write in clear, flowing
prose using complete paragraphs. Use standard paragraph breaks for organization.

Reserve markdown primarily for:
- `inline code`
- code blocks (```...```)
- simple headings (##, ###)

Avoid using **bold** and *italics*.

DO NOT use ordered lists (1. ...) or unordered lists (*) unless:
a) you're presenting truly discrete items where a list format is the best option
b) the user explicitly requests a list or ranking

Instead of listing items with bullets, incorporate them naturally into sentences.
NEVER output a series of overly short bullet points.
</avoid_excessive_markdown_and_bullet_points>
````

### Subagent Orchestration

Claude 4.5 models recognize when tasks benefit from delegating to subagents:

**Let Claude Orchestrate Naturally**:

- Have well-defined subagent tools available
- Claude will delegate appropriately without explicit instruction

**Conservative Subagent Usage**:

```text
Only delegate to subagents when the task clearly benefits from a separate agent
with a new context window.
```

### Avoiding Overengineering

Claude 4.x models can overengineer. Add explicit prompting:

```xml
Avoid over-engineering. Only make changes that are directly requested or clearly
necessary. Keep solutions simple and focused.

Don't add features, refactor code, or make "improvements" beyond what was asked.
A bug fix doesn't need surrounding code cleaned up. A simple feature doesn't
need extra configurability.

Don't add error handling, fallbacks, or validation for scenarios that can't happen.
Trust internal code and framework guarantees. Only validate at system boundaries.

Don't create helpers, utilities, or abstractions for one-time operations.
Don't design for hypothetical future requirements. The right amount of complexity
is the minimum needed for the current task. Reuse existing abstractions where
possible and follow the DRY principle.
```

### Code Exploration

Encourage thorough code exploration:

```xml
<investigate_before_answering>
ALWAYS read and understand relevant files before proposing code edits.
Do not speculate about code you have not inspected.

If the user references a specific file/path, you MUST open and inspect it
before explaining or proposing fixes.

Be rigorous and persistent in searching code for key facts.
Thoroughly review the style, conventions, and abstractions of the codebase
before implementing new features or abstractions.
</investigate_before_answering>
```

### Minimizing Hallucinations

```xml
<investigate_before_answering>
Never speculate about code you have not opened. If the user references a specific
file, you MUST read the file before answering.

Make sure to investigate and read relevant files BEFORE answering questions about
the codebase. Never make any claims about code before investigating unless you are
certain of the correct answer - give grounded and hallucination-free answers.
</investigate_before_answering>
```

### Frontend Design

Avoid generic "AI slop" aesthetics:

```xml
<frontend_aesthetics>
You tend to converge toward generic outputs. In frontend design, this creates
what users call the "AI slop" aesthetic. Avoid this: make creative, distinctive
frontends that surprise and delight.

Focus on:
- Typography: Choose beautiful, unique fonts. Avoid Arial, Inter; opt for distinctive choices.
- Color & Theme: Commit to a cohesive aesthetic. Dominant colors with sharp accents.
- Motion: Use animations for effects. Focus on high-impact moments like page load.
- Backgrounds: Create atmosphere and depth rather than solid colors.

Avoid generic AI-generated aesthetics:
- Overused font families (Inter, Roboto, Arial)
- Clichéd color schemes (purple gradients on white)
- Predictable layouts and component patterns

Vary between light and dark themes, different fonts, different aesthetics.
Think outside the box!
</frontend_aesthetics>
```

### Test-Driven Development

Avoid hard-coding solutions to pass tests:

```text
Write a high-quality, general-purpose solution using standard tools available.
Do not create helper scripts or workarounds.

Implement a solution that works correctly for all valid inputs, not just the test cases.
Do not hard-code values or create solutions that only work for specific test inputs.
Instead, implement the actual logic that solves the problem generally.

Focus on understanding the problem requirements and implementing the correct algorithm.
Tests are there to verify correctness, not to define the solution.

If the task is unreasonable or if any tests are incorrect, inform me rather than
working around them.
```

### Model Self-Knowledge

Help Claude identify itself correctly:

```text
The assistant is Claude, created by Anthropic. The current model is Claude Sonnet 4.5.
```

For LLM-powered apps:

```text
When an LLM is needed, default to Claude Sonnet 4.5 unless the user requests otherwise.
The exact model string is claude-sonnet-4-5-20250929.
```

### DSM-Specific Prompting Patterns

**FCP Context**:

```text
This codebase uses the Failover Control Protocol (FCP) for data fetching.
When analyzing data fetching code:
1. Check for proper FCP cache handling
2. Verify fallback chain configuration
3. Ensure retry limits are respected
4. Look for silent failure patterns
```

**DataFrame Operations**:

```text
Use Polars for DataFrame operations (not pandas).
Standard column schema: open_time, open, high, low, close, volume
All timestamps must be UTC with timezone awareness.
```

**Symbol Formats**:

```text
Exchange symbol formats:
- Binance: BTCUSDT (uppercase, no separator)
- OKX: BTC-USDT (uppercase, hyphen separator)
- Standardized: BTC/USDT (uppercase, slash separator)

Always use the appropriate format for the target exchange.
```

### Communication Style Notes

Claude 4.5 models have a more concise, natural style:

- **More direct**: Fact-based progress reports, not self-celebratory
- **More conversational**: Fluent, less machine-like
- **Less verbose**: May skip detailed summaries unless prompted

**Request Visibility**:

```text
After completing a task that involves tool use, provide a quick summary
of the work you've done.
```

### Migration Considerations

When migrating to Claude 4.5:

1. **Be specific about desired behavior** - Describe exactly what you'd like in output
2. **Frame instructions with modifiers** - "Include as many relevant features as possible"
3. **Request features explicitly** - Animations, interactive elements need explicit requests
4. **Dial back aggressive language** - "CRITICAL: You MUST" → "Use this tool when"
<!-- SSoT-OK: Skills Architecture Reference - comprehensive skills documentation from official docs -->

## Skills Architecture Reference

### Overview

Skills extend what Claude can do. Create a `SKILL.md` file with instructions, and Claude adds it to its toolkit. Claude uses skills when relevant, or you can invoke one directly with `/skill-name`.

Skills follow the [Agent Skills](https://agentskills.io) open standard, which works across multiple AI tools.

### Skill Locations

Where you store a skill determines who can use it:

| Location   | Path                                     | Applies to                | Priority    |
| ---------- | ---------------------------------------- | ------------------------- | ----------- |
| Enterprise | Managed settings                         | All users in organization | 1 (highest) |
| Personal   | `~/.claude/skills/<skill-name>/SKILL.md` | All your projects         | 2           |
| Project    | `.claude/skills/<skill-name>/SKILL.md`   | This project only         | 3           |
| Plugin     | `<plugin>/skills/<skill-name>/SKILL.md`  | Where plugin is enabled   | 4 (lowest)  |

When skills share the same name, higher-priority locations win. Plugin skills use `plugin-name:skill-name` namespace.

### SKILL.md Structure

Every skill needs a `SKILL.md` file with two parts:

1. **YAML frontmatter** (between `---` markers) - Tells Claude when to use the skill
2. **Markdown content** - Instructions Claude follows when skill is invoked

```yaml
---
name: explain-code
description: Explains code with visual diagrams and analogies. Use when explaining how code works or when user asks "how does this work?"
---
When explaining code, always include:

1. **Start with an analogy**: Compare the code to something from everyday life
2. **Draw a diagram**: Use ASCII art to show the flow or relationships
3. **Walk through the code**: Explain step-by-step what happens
4. **Highlight a gotcha**: What's a common mistake?
```

### Frontmatter Fields

| Field                      | Required    | Description                                                                   |
| -------------------------- | ----------- | ----------------------------------------------------------------------------- |
| `name`                     | No          | Display name. If omitted, uses directory name. Lowercase, numbers, hyphens.   |
| `description`              | Recommended | What skill does and when to use it. Claude uses this to decide when to apply. |
| `argument-hint`            | No          | Hint for autocomplete. Example: `[issue-number]` or `[filename] [format]`     |
| `disable-model-invocation` | No          | `true` prevents Claude from auto-loading. Default: `false`                    |
| `user-invocable`           | No          | `false` hides from `/` menu. Default: `true`                                  |
| `allowed-tools`            | No          | Tools Claude can use without asking permission when skill is active           |
| `model`                    | No          | Model to use when skill is active                                             |
| `context`                  | No          | `fork` to run in forked subagent context                                      |
| `agent`                    | No          | Subagent type when `context: fork` is set (`Explore`, `Plan`, etc.)           |
| `hooks`                    | No          | Hooks scoped to this skill's lifecycle                                        |

### Types of Skill Content

**Reference Content** (knowledge applied to current work):

```yaml
---
name: api-conventions
description: API design patterns for this codebase
---
When writing API endpoints:
  - Use RESTful naming conventions
  - Return consistent error formats
  - Include request validation
```

**Task Content** (step-by-step instructions):

```yaml
---
name: deploy
description: Deploy the application to production
context: fork
disable-model-invocation: true
---

Deploy the application:
1. Run the test suite
2. Build the application
3. Push to the deployment target
```

### Invocation Control

| Frontmatter                      | You can invoke | Claude can invoke | When loaded                                    |
| -------------------------------- | -------------- | ----------------- | ---------------------------------------------- |
| (default)                        | Yes            | Yes               | Description always in context, loads on invoke |
| `disable-model-invocation: true` | Yes            | No                | Description not in context, loads on invoke    |
| `user-invocable: false`          | No             | Yes               | Description always in context, loads on invoke |

### String Substitutions

Skills support dynamic values:

| Variable               | Description                                      |
| ---------------------- | ------------------------------------------------ |
| `$ARGUMENTS`           | All arguments passed when invoking skill         |
| `$ARGUMENTS[N]`        | Specific argument by 0-based index               |
| `$N`                   | Shorthand for `$ARGUMENTS[N]` (`$0`, `$1`, etc.) |
| `${CLAUDE_SESSION_ID}` | Current session ID for logging/correlation       |

**Example**:

```yaml
---
name: fix-issue
description: Fix a GitHub issue
disable-model-invocation: true
---
Fix GitHub issue $ARGUMENTS following our coding standards.

1. Read the issue description
2. Understand the requirements
3. Implement the fix
4. Write tests
5. Create a commit
```

Invocation: `/fix-issue 123` → "Fix GitHub issue 123 following our coding standards..."

### Directory Structure

```
my-skill/
├── SKILL.md           # Main instructions (required)
├── template.md        # Template for Claude to fill in
├── examples/
│   └── sample.md      # Example output showing expected format
├── references/
│   └── api-spec.md    # Detailed reference documentation
└── scripts/
    └── validate.sh    # Script Claude can execute
```

Reference supporting files from SKILL.md:

```markdown
## Additional resources

- For complete API details, see [reference.md](reference.md)
- For usage examples, see [examples.md](examples.md)
```

**Tip**: Keep SKILL.md under 500 lines. Move detailed reference material to separate files.

### Running in Subagent

Add `context: fork` to run skill in isolation:

```yaml
---
name: deep-research
description: Research a topic thoroughly
context: fork
agent: Explore
---

Research $ARGUMENTS thoroughly:

1. Find relevant files using Glob and Grep
2. Read and analyze the code
3. Summarize findings with specific file references
```

**How it works**:

1. New isolated context is created
2. Subagent receives skill content as prompt
3. `agent` field determines execution environment
4. Results summarized and returned to main conversation

### Dynamic Context Injection

The `!`command\`\` syntax runs shell commands before skill content is sent:

```yaml
---
name: pr-summary
description: Summarize changes in a pull request
context: fork
agent: Explore
allowed-tools: Bash(gh *)
---

## Pull request context
- PR diff: !`gh pr diff`
- PR comments: !`gh pr view --comments`
- Changed files: !`gh pr diff --name-only`

## Your task
Summarize this pull request...
```

Commands execute immediately and output replaces placeholders. Claude only sees final result.

### Restricting Tool Access

```yaml
---
name: safe-reader
description: Read files without making changes
allowed-tools: Read, Grep, Glob
---
```

### Skill Hooks

Define hooks in skill frontmatter:

```yaml
---
name: test-runner
hooks:
  PreToolUse:
    - matcher: "Bash"
      hooks:
        - type: command
          command: "./scripts/validate-command.sh"
  PostToolUse:
    - matcher: "Edit|Write"
      hooks:
        - type: command
          command: "./scripts/lint-check.sh"
---
```

### Permission Rules

Control skill access:

```
# Allow specific skills
Skill(commit)
Skill(review-pr *)

# Deny specific skills
Skill(deploy *)

# Disable all skills
Skill
```

### Generating Visual Output

Skills can generate interactive HTML:

````yaml
---
name: codebase-visualizer
description: Generate interactive tree visualization of codebase
allowed-tools: Bash(python *)
---

# Codebase Visualizer

Run the visualization script from your project root:

```bash
python ~/.claude/skills/codebase-visualizer/scripts/visualize.py .
````

This creates `codebase-map.html` and opens it in your browser.

````

### Progressive Disclosure Pattern

At startup:
- Only skill descriptions are loaded into context
- Full skill content loads only when invoked

This minimizes context usage while making skills discoverable.

**Context budget**: Default 15,000 characters for skill descriptions. Increase with `SLASH_COMMAND_TOOL_CHAR_BUDGET` environment variable.

### Best Practices

**Description Writing**:
- Include both what the skill does AND when to use it
- Include all "when to use" information in description, not body
- Body is only loaded after triggering

**Content Guidelines**:
- Keep SKILL.md under 500 lines
- Split large content into separate files
- Claude is smart - only add context Claude doesn't already have

**Naming Conventions**:
- Use gerund form (verb + -ing) for skill names
- Makes activity clear: `explaining-code`, `reviewing-pr`, `deploying-app`

### Memory and Rules Reference

#### CLAUDE.md Hierarchy

| Memory Type        | Location                                     | Purpose                               |
| ------------------ | -------------------------------------------- | ------------------------------------- |
| Managed policy     | `/Library/Application Support/ClaudeCode/`   | Organization-wide instructions        |
| Project memory     | `./CLAUDE.md` or `./.claude/CLAUDE.md`       | Team-shared project instructions      |
| Project rules      | `./.claude/rules/*.md`                       | Modular topic-specific instructions   |
| User memory        | `~/.claude/CLAUDE.md`                        | Personal preferences (all projects)   |
| Project local      | `./CLAUDE.local.md`                          | Personal project-specific (gitignored)|

#### CLAUDE.md Imports

Use `@path/to/import` syntax:

```markdown
See @README for project overview and @package.json for npm commands.

# Additional Instructions
- git workflow @docs/git-instructions.md
- @~/.claude/my-project-instructions.md
````

Features:

- Relative and absolute paths allowed
- Recursive imports (max depth 5)
- Not evaluated inside code spans/blocks

#### Path-Specific Rules

Rules can be scoped to specific files:

```yaml
---
paths:
  - "src/api/**/*.ts"
---
# API Development Rules

- All API endpoints must include input validation
- Use the standard error response format
```

Rules without `paths:` apply globally.

**Glob patterns**:

| Pattern             | Matches                               |
| ------------------- | ------------------------------------- |
| `**/*.ts`           | All TypeScript files in any directory |
| `src/**/*`          | All files under src/ directory        |
| `*.md`              | Markdown files in project root        |
| `src/**/*.{ts,tsx}` | Both .ts and .tsx files               |
| `{src,lib}/**/*.ts` | TypeScript in src or lib              |

#### Rules Organization

```
.claude/rules/
├── frontend/
│   ├── react.md
│   └── styles.md
├── backend/
│   ├── api.md
│   └── database.md
└── general.md
```

All `.md` files discovered recursively. Subdirectories and symlinks supported.

### DSM Skill Implementation

The data-source-manager uses 4 skills:

| Skill           | Purpose                     | Context |
| --------------- | --------------------------- | ------- |
| dsm-usage       | DataSourceManager API usage | inline  |
| dsm-testing     | Testing patterns            | inline  |
| dsm-research    | Codebase research           | fork    |
| dsm-fcp-monitor | FCP monitoring              | fork    |

**Example DSM Skill** (`docs/skills/dsm-usage/SKILL.md`):

````yaml
---
name: dsm-usage
description: DataSourceManager API usage patterns. Use when fetching market data, handling OHLCV, or working with FCP.
---

## DataSourceManager API

### Basic Usage

```python
from data_source_manager import DataSourceManager

dsm = DataSourceManager()
df = dsm.get_ohlcv(
    symbol="BTC/USDT",
    timeframe="1h",
    start_time=datetime(2025, 1, 1, tzinfo=timezone.utc),
    end_time=datetime(2025, 1, 2, tzinfo=timezone.utc)
)
````

### FCP Integration

The Failover Control Protocol handles:

- Primary/fallback provider selection
- Cache management
- Retry logic with exponential backoff

See @references/fcp-protocol.md for detailed FCP documentation.

```

### Troubleshooting

**Skill not triggering**:
1. Check description includes natural keywords
2. Verify skill appears in "What skills are available?"
3. Rephrase request to match description
4. Invoke directly with `/skill-name`

**Skill triggers too often**:
1. Make description more specific
2. Add `disable-model-invocation: true`

**Claude doesn't see all skills**:
- Skills may exceed character budget (15,000 default)
- Run `/context` to check for excluded skills warning
- Increase `SLASH_COMMAND_TOOL_CHAR_BUDGET`

```
<!-- SSoT-OK: Settings & Permissions Reference - comprehensive settings documentation from official docs -->

## Settings & Permissions Reference

### Settings File Hierarchy

Claude Code uses a hierarchical scope system:

| Scope     | Location                                   | Precedence  | Shared            |
| --------- | ------------------------------------------ | ----------- | ----------------- |
| Managed   | `/Library/Application Support/ClaudeCode/` | 1 (highest) | Yes (IT deployed) |
| CLI flags | Command-line arguments                     | 2           | No                |
| Local     | `.claude/settings.local.json`              | 3           | No (gitignored)   |
| Project   | `.claude/settings.json`                    | 4           | Yes (committed)   |
| User      | `~/.claude/settings.json`                  | 5 (lowest)  | No                |

Settings are merged hierarchically - more specific scopes override broader ones.

### Core settings.json Structure

```json
{
  "permissions": {
    "allow": ["Bash(npm run *)", "Read(~/.zshrc)"],
    "ask": ["Bash(git push *)"],
    "deny": ["Bash(curl *)", "Read(./.env)"]
  },
  "env": {
    "CLAUDE_CODE_ENABLE_TELEMETRY": "1"
  },
  "model": "claude-sonnet-4-5-20250929",
  "outputStyle": "Explanatory",
  "sandbox": {},
  "hooks": {},
  "fileSuggestion": {}
}
```

### Permission Rules System

#### Rule Syntax Format

Rules follow: `Tool` or `Tool(specifier)`

```
Bash                           # Matches all bash commands
Bash(npm run *)               # Pattern with wildcards
Bash(npm run build)           # Exact command
Read(./.env)                  # File path
Read(./secrets/**)            # Directory glob (recursive)
WebFetch(domain:example.com)  # Domain restriction
Edit(./src/**)                # Write access to directory
Task(subagent-name)           # Subagent restriction
```

#### Evaluation Order

Rules evaluated in order (first match wins):

1. **Deny** rules (highest priority - always checked first)
2. **Ask** rules (require confirmation)
3. **Allow** rules (auto-approve)

**Important**: Deny rules always take precedence over allow rules, even if both match.

### Tool-Specific Patterns

| Tool     | Pattern Examples                                            |
| -------- | ----------------------------------------------------------- |
| Bash     | `Bash(npm run *)`, `Bash(git push *)`, `Bash(curl *)`       |
| Read     | `Read(./.env)`, `Read(./secrets/**)`, `Read(~/.ssh/id_rsa)` |
| Edit     | `Edit(./src/**)`, `Edit(.env)`                              |
| Write    | `Write(./src/**)`, `Write(./output/*)`                      |
| WebFetch | `WebFetch`, `WebFetch(domain:example.com)`                  |
| Task     | `Task`, `Task(subagent-name)`                               |
| MCP      | `MCP`, `MCP(memory)`, `MCP(github)`                         |
| Skill    | `Skill(commit)`, `Skill(deploy *)`                          |

### Wildcard Pattern Syntax

```json
{
  "permissions": {
    "allow": [
      "Bash(npm run *)", // Matches: npm run build, npm run test
      "Bash(git commit *)",
      "Bash(git * main)", // Matches: git push main, git pull main
      "Bash(* --version)", // Matches: node --version
      "Bash(* --help *)",
      "Read(./src/**)" // Recursive directory matching
    ],
    "deny": [
      "Bash(curl *)", // Block all curl commands
      "Read(./.env*)", // Block .env files
      "Read(./secrets/**)", // Block entire secrets directory
      "WebFetch" // Block all web requests
    ]
  }
}
```

**Glob Pattern Reference**:

| Pattern    | Matches                          |
| ---------- | -------------------------------- |
| `*`        | Single-level wildcard            |
| `**`       | Recursive wildcard (any depth)   |
| `./.env`   | Specific file                    |
| `./.env*`  | Files starting with .env         |
| `./src/**` | All files under src/ recursively |
| `~/.ssh/*` | Files directly in .ssh directory |

**Note**: Spaces in patterns matter. `Bash(ls *)` ≠ `Bash(ls*)`

### Permission Modes

```json
{
  "permissions": {
    "defaultMode": "acceptEdits",
    "disableBypassPermissionsMode": "disable"
  }
}
```

| Mode                           | Description                                    |
| ------------------------------ | ---------------------------------------------- |
| `acceptEdits`                  | Auto-accept file edits                         |
| `askEverytime`                 | Prompt for every operation                     |
| `disableBypassPermissionsMode` | Prevents `--dangerously-skip-permissions` flag |

### Secure Project Configuration Example

```json
{
  "permissions": {
    "allow": [
      "Bash(npm run lint)",
      "Bash(npm run test *)",
      "Bash(git diff)",
      "Bash(git status)",
      "Read(./src/**)",
      "Read(./package.json)",
      "Edit(./src/**)"
    ],
    "deny": [
      "Bash(rm -rf)",
      "Bash(sudo *)",
      "Bash(curl *)",
      "Bash(wget *)",
      "Read(./.env)",
      "Read(./.env.*)",
      "Read(./secrets/**)",
      "Read(~/.aws/**)",
      "Read(**/.env)",
      "Read(**/*.key)",
      "WebFetch(domain:internal-api.local)"
    ],
    "ask": ["Bash(git push *)", "Bash(npm publish *)"]
  }
}
```

### Additional Working Directories

```json
{
  "permissions": {
    "additionalDirectories": [
      "../docs/",
      "../shared-libs/",
      "/opt/company-tools/"
    ]
  }
}
```

### Sandbox Configuration

```json
{
  "sandbox": {
    "enabled": true,
    "autoAllowBashIfSandboxed": true,
    "excludedCommands": ["git", "docker"],
    "allowUnsandboxedCommands": true,
    "network": {
      "allowUnixSockets": ["~/.ssh/agent-socket", "/var/run/docker.sock"],
      "allowLocalBinding": true,
      "httpProxyPort": 8080,
      "socksProxyPort": 8081
    },
    "enableWeakerNestedSandbox": false
  }
}
```

### Managed Settings (Enterprise)

Enterprise administrators deploy `managed-settings.json` to enforce organization-wide policies:

**Locations**:

- macOS: `/Library/Application Support/ClaudeCode/managed-settings.json`
- Linux/WSL: `/etc/claude-code/managed-settings.json`
- Windows: `C:\Program Files\ClaudeCode\managed-settings.json`

**Example managed-settings.json**:

```json
{
  "model": "claude-opus-4-1-20250805",
  "permissions": {
    "deny": ["WebFetch", "Bash(curl *)", "Bash(sudo *)"]
  },
  "disableBypassPermissionsMode": "disable",
  "allowManagedHooksOnly": true,
  "allowedMcpServers": [{ "serverName": "github" }, { "serverName": "memory" }],
  "deniedMcpServers": [{ "serverName": "filesystem" }],
  "strictKnownMarketplaces": [
    { "source": "github", "repo": "acme-corp/approved-plugins" }
  ]
}
```

### Marketplace Allowlist

```json
{
  "strictKnownMarketplaces": [
    { "source": "github", "repo": "acme-corp/plugins" },
    { "source": "github", "repo": "acme-corp/security-tools", "ref": "v2.0" },
    { "source": "git", "url": "https://gitlab.example.com/tools/plugins.git" },
    { "source": "npm", "package": "@acme-corp/claude-plugins" },
    { "source": "url", "url": "https://plugins.example.com/marketplace.json" },
    { "source": "file", "path": "/usr/local/share/claude/marketplace.json" },
    { "source": "hostPattern", "hostPattern": "^github\\.example\\.com$" }
  ]
}
```

### Plugin Configuration

```json
{
  "enabledPlugins": {
    "formatter@acme-tools": true,
    "deployer@acme-tools": true,
    "analyzer@security-plugins": false
  },
  "extraKnownMarketplaces": {
    "acme-tools": {
      "source": {
        "source": "github",
        "repo": "acme-corp/claude-plugins"
      }
    }
  }
}
```

### File Suggestion Configuration

```json
{
  "fileSuggestion": {
    "type": "command",
    "command": "~/.claude/file-suggestion.sh"
  }
}
```

Script receives JSON input: `{"query": "src/comp"}`
Script outputs: Newline-separated file paths (max 15)

### Key Settings Reference

| Key                          | Type    | Description                            |
| ---------------------------- | ------- | -------------------------------------- |
| `model`                      | string  | Override default model                 |
| `outputStyle`                | string  | Response style (Explanatory, Concise)  |
| `language`                   | string  | Claude's response language             |
| `cleanupPeriodDays`          | number  | Inactive session cleanup (default: 30) |
| `respectGitignore`           | boolean | Exclude gitignore patterns from picker |
| `alwaysThinkingEnabled`      | boolean | Enable extended thinking by default    |
| `plansDirectory`             | string  | Custom plan storage location           |
| `showTurnDuration`           | boolean | Show "Cooked for X" messages           |
| `spinnerTipsEnabled`         | boolean | Show tips during processing            |
| `terminalProgressBarEnabled` | boolean | Terminal progress bar display          |
| `autoUpdatesChannel`         | string  | `stable` or `latest`                   |
| `apiKeyHelper`               | string  | Script for dynamic API key generation  |
| `statusLine`                 | object  | Custom status display configuration    |

### Environment Variables

**Authentication**:

| Variable               | Description                |
| ---------------------- | -------------------------- |
| `ANTHROPIC_API_KEY`    | API key for authentication |
| `ANTHROPIC_AUTH_TOKEN` | Custom auth token          |

**Model Configuration**:

| Variable                     | Description              |
| ---------------------------- | ------------------------ |
| `ANTHROPIC_MODEL`            | Override model selection |
| `CLAUDE_CODE_SUBAGENT_MODEL` | Model for subagents      |

**Cloud Providers**:

| Variable                  | Description             |
| ------------------------- | ----------------------- |
| `CLAUDE_CODE_USE_BEDROCK` | Enable AWS Bedrock      |
| `CLAUDE_CODE_USE_VERTEX`  | Enable Google Vertex    |
| `CLAUDE_CODE_USE_FOUNDRY` | Enable Palantir Foundry |

**Feature Flags**:

| Variable                               | Description              |
| -------------------------------------- | ------------------------ |
| `CLAUDE_CODE_ENABLE_TELEMETRY`         | Enable telemetry         |
| `CLAUDE_CODE_DISABLE_BACKGROUND_TASKS` | Disable background tasks |
| `DISABLE_AUTOUPDATER`                  | Disable auto-updates     |
| `DISABLE_TELEMETRY`                    | Disable all telemetry    |
| `DISABLE_ERROR_REPORTING`              | Disable error reporting  |

**Performance**:

| Variable                        | Description              | Default |
| ------------------------------- | ------------------------ | ------- |
| `CLAUDE_CODE_MAX_OUTPUT_TOKENS` | Max output tokens        | 32000   |
| `MAX_THINKING_TOKENS`           | Extended thinking tokens | 31999   |
| `BASH_MAX_TIMEOUT_MS`           | Bash command timeout     | 120000  |
| `BASH_MAX_OUTPUT_LENGTH`        | Max bash output length   | 100000  |

**Proxy & Network**:

| Variable                           | Description       |
| ---------------------------------- | ----------------- |
| `HTTP_PROXY`                       | HTTP proxy URL    |
| `HTTPS_PROXY`                      | HTTPS proxy URL   |
| `NO_PROXY`                         | Proxy bypass list |
| `CLAUDE_CODE_PROXY_RESOLVES_HOSTS` | Proxy handles DNS |

**Miscellaneous**:

| Variable                          | Description                |
| --------------------------------- | -------------------------- |
| `CLAUDE_CODE_SHELL`               | Override shell (bash, zsh) |
| `CLAUDE_CODE_TMPDIR`              | Custom temp directory      |
| `CLAUDE_CODE_HIDE_ACCOUNT_INFO`   | Hide account info in UI    |
| `CLAUDE_AUTOCOMPACT_PCT_OVERRIDE` | Compaction trigger %       |
| `SLASH_COMMAND_TOOL_CHAR_BUDGET`  | Skill description budget   |

### DSM Settings Configuration

The data-source-manager uses comprehensive settings:

**`.claude/settings.json`**:

```json
{
  "permissions": {
    "allow": [
      "Bash(uv run *)",
      "Bash(mise run *)",
      "Bash(pytest *)",
      "Bash(git diff)",
      "Bash(git status)",
      "Bash(git log *)",
      "Read(./src/**)",
      "Read(./tests/**)",
      "Edit(./src/**)",
      "Edit(./tests/**)"
    ],
    "deny": [
      "Bash(pip install *)",
      "Bash(python3.14 *)",
      "Bash(python3.12 *)",
      "Bash(git push --force *)",
      "Read(.env*)",
      "Read(.mise.local.toml)",
      "Read(**/*.key)",
      "Read(**/credentials*)"
    ],
    "ask": ["Bash(git push *)", "Bash(git commit *)"]
  },
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

**Personal Overrides** (`.claude/settings.local.json`):

```json
{
  "permissions": {
    "allow": ["Bash(git commit *)"]
  },
  "model": "claude-opus-4-5-20251101"
}
```

### Best Practices

**Security**:

1. Always deny access to secrets files (`.env`, credentials)
2. Block dangerous bash commands (`sudo`, `rm -rf`, `curl`)
3. Use ask rules for destructive operations (force push, publish)
4. Enable sandbox for untrusted operations

**Team Collaboration**:

1. Commit `.claude/settings.json` for team-shared rules
2. Use `.claude/settings.local.json` for personal preferences (gitignored)
3. Document deny rules with comments in team docs
4. Use managed settings for organization-wide policies

**Performance**:

1. Set appropriate timeouts for long-running commands
2. Configure output limits for verbose commands
3. Use sandbox to auto-approve safe commands
<!-- SSoT-OK: Agentic Coding Best Practices Reference - comprehensive agentic patterns from official docs -->

## Agentic Coding Best Practices Reference

### Core Principle

**Context window fills up fast, and performance degrades as it fills.**

Claude's context window holds your entire conversation, file contents, and command outputs. A single debugging session can consume tens of thousands of tokens. Managing context is the most critical skill.

### The Explore-Plan-Code-Commit Workflow

The recommended workflow has four phases:

| Phase     | Mode        | Description                                    |
| --------- | ----------- | ---------------------------------------------- |
| Explore   | Plan Mode   | Read files, understand codebase, ask questions |
| Plan      | Plan Mode   | Create detailed implementation plan            |
| Implement | Normal Mode | Code against plan, verify with tests           |
| Commit    | Normal Mode | Commit with descriptive message, create PR     |

**Example Workflow**:

```
# Phase 1: Explore (Plan Mode)
read /src/auth and understand how we handle sessions and login.
also look at how we manage environment variables for secrets.

# Phase 2: Plan (Plan Mode)
I want to add Google OAuth. What files need to change?
What's the session flow? Create a plan.

# Phase 3: Implement (Normal Mode)
implement the OAuth flow from your plan. write tests for the
callback handler, run the test suite and fix any failures.

# Phase 4: Commit (Normal Mode)
commit with a descriptive message and open a PR
```

**When to Skip Planning**:

- Small scope, clear fix (typo, log line, variable rename)
- You could describe the diff in one sentence
- Task is exploratory and you'll course-correct

### Verification Strategies

**Give Claude a way to verify its work - this is the single highest-leverage practice.**

| Strategy                      | Before                       | After                                                                                 |
| ----------------------------- | ---------------------------- | ------------------------------------------------------------------------------------- |
| Provide verification criteria | "implement email validation" | "write validateEmail. Tests: <user@example.com>→true, invalid→false. Run tests after" |
| Verify UI changes visually    | "make dashboard look better" | "[paste screenshot] implement this. Take screenshot and compare. List differences"    |
| Address root causes           | "the build is failing"       | "build fails with [error]. Fix and verify build succeeds. Address root cause"         |

**Verification can be**:

- Test suite
- Linter
- Bash command that checks output
- Screenshot comparison
- Expected output comparison

### Prompting Strategies

**Provide specific context**:

| Strategy                    | Before                                      | After                                                                                                        |
| --------------------------- | ------------------------------------------- | ------------------------------------------------------------------------------------------------------------ |
| Scope the task              | "add tests for foo.py"                      | "write test for foo.py covering edge case where user is logged out. avoid mocks"                             |
| Point to sources            | "why does ExecutionFactory have weird api?" | "look through ExecutionFactory's git history and summarize how its api came to be"                           |
| Reference existing patterns | "add a calendar widget"                     | "look at HotDogWidget.php for patterns. follow the pattern for new calendar widget"                          |
| Describe the symptom        | "fix the login bug"                         | "users report login fails after session timeout. check auth flow in src/auth/. write failing test, then fix" |

**Provide rich content**:

- Reference files with `@` instead of describing
- Paste images directly (copy/paste or drag-drop)
- Give URLs for documentation and API references
- Pipe data with `cat error.log | claude`
- Let Claude fetch what it needs using Bash, MCP, or file reads

### Context Management

**Use /clear between unrelated tasks**:

```
# Bad: Kitchen sink session
> Implement feature X
> [unrelated] How does logging work?
> [back to X] Fix that bug
# Context full of irrelevant information

# Good: Clean separation
> Implement feature X
> /clear
> How does logging work?
```

**Auto-compaction behavior**:

- Triggers when approaching context limits
- Preserves important code and decisions
- Summarizes conversation while freeing space

**Manual compaction**:

```
/compact Focus on the API changes
```

**In CLAUDE.md**:

```markdown
When compacting, always preserve the full list of modified files
and any test commands.
```

### Course-Correction Techniques

| Technique     | Usage                                        |
| ------------- | -------------------------------------------- |
| `Esc`         | Stop Claude mid-action, context preserved    |
| `Esc + Esc`   | Open rewind menu                             |
| `/rewind`     | Restore previous conversation and code state |
| `"Undo that"` | Have Claude revert its changes               |
| `/clear`      | Reset context for fresh start                |

**Two-correction rule**: If you've corrected Claude more than twice on the same issue, run `/clear` and start fresh with a better prompt.

### Subagent Usage

**Delegate research to preserve context**:

```
Use subagents to investigate how our authentication system handles
token refresh, and whether we have any existing OAuth utilities I
should reuse.
```

**Verification after implementation**:

```
use a subagent to review this code for edge cases
```

Subagents run in separate context windows and report back summaries, keeping your main conversation clean.

### Extended Thinking Triggers

Specific phrases map to thinking budget levels:

| Phrase         | Thinking Level |
| -------------- | -------------- |
| "think"        | Low            |
| "think hard"   | Medium         |
| "think harder" | High           |
| "ultrathink"   | Maximum        |

Use extended thinking for:

- Complex architectural decisions
- Challenging bugs
- Multi-step implementation planning
- Evaluating tradeoffs

### Session Management

**Checkpoints**:

- Every action creates a checkpoint
- Double-tap `Escape` or `/rewind` to access
- Options: restore conversation only, code only, or both
- Persist across sessions

**Resume conversations**:

```bash
claude --continue    # Resume most recent
claude --resume      # Select from recent sessions
```

**Naming sessions**:

```
/rename oauth-migration
/rename debugging-memory-leak
```

### Parallel Development Patterns

**Writer/Reviewer pattern**:

| Session A (Writer)                              | Session B (Reviewer)                                    |
| ----------------------------------------------- | ------------------------------------------------------- |
| `Implement a rate limiter for API endpoints`    |                                                         |
|                                                 | `Review rate limiter in @src/middleware/rateLimiter.ts` |
| `Here's review feedback: [B output]. Fix these` |                                                         |

**Git worktrees for parallel sessions**:

```bash
# Create worktree for feature
git worktree add ../feature-oauth oauth-branch

# Run Claude in each worktree
cd ../feature-oauth && claude
```

### Fan-Out Pattern

For large migrations or analyses:

```bash
# 1. Generate task list
claude -p "list all Python files needing migration" > files.txt

# 2. Loop through list
for file in $(cat files.txt); do
  claude -p "Migrate $file from React to Vue. Return OK or FAIL." \
    --allowedTools "Edit,Bash(git commit *)"
done
```

### Headless Mode

```bash
# One-off queries
claude -p "Explain what this project does"

# Structured output for scripts
claude -p "List all API endpoints" --output-format json

# Streaming for real-time processing
claude -p "Analyze this log file" --output-format stream-json
```

### CLAUDE.md Best Practices

**Include**:

- Bash commands Claude can't guess
- Code style rules that differ from defaults
- Testing instructions and preferred test runners
- Repository etiquette (branch naming, PR conventions)
- Architectural decisions specific to your project
- Developer environment quirks (required env vars)
- Common gotchas or non-obvious behaviors

**Exclude**:

- Anything Claude can figure out by reading code
- Standard language conventions Claude already knows
- Detailed API documentation (link instead)
- Information that changes frequently
- File-by-file descriptions of the codebase
- Self-evident practices like "write clean code"

**Maintenance**:

- Review when things go wrong
- Prune regularly
- Test changes by observing behavior shifts
- If Claude ignores rules, file is probably too long
- Add emphasis ("IMPORTANT", "YOU MUST") for critical rules

### Common Failure Patterns

| Pattern                  | Problem                               | Fix                                              |
| ------------------------ | ------------------------------------- | ------------------------------------------------ |
| Kitchen sink session     | Unrelated tasks pollute context       | `/clear` between unrelated tasks                 |
| Correcting over and over | Context polluted with failed attempts | After 2 corrections, `/clear` with better prompt |
| Over-specified CLAUDE.md | Important rules get lost in noise     | Ruthlessly prune, convert to hooks               |
| Trust-then-verify gap    | Plausible code without edge cases     | Always provide verification                      |
| Infinite exploration     | Claude reads hundreds of files        | Scope narrowly or use subagents                  |

### Interview Pattern

For larger features, have Claude interview you:

```
I want to build [brief description]. Interview me in detail using
the AskUserQuestion tool.

Ask about technical implementation, UI/UX, edge cases, concerns,
and tradeoffs. Don't ask obvious questions, dig into the hard parts
I might not have considered.

Keep interviewing until we've covered everything, then write a
complete spec to SPEC.md.
```

Then start a fresh session to execute the spec with clean context.

### Safe Autonomous Mode

```bash
# Full autonomy (use in sandbox only)
claude --dangerously-skip-permissions

# Better: sandbox with boundaries
/sandbox
```

Warning: Letting Claude run arbitrary commands can result in data loss, system corruption, or data exfiltration via prompt injection.

### DSM-Specific Agentic Patterns

**FCP Debugging Workflow**:

```
# Plan Mode
Explore the FCP implementation in src/fcp/. Understand:
- Cache lookup logic
- Fallback chain configuration
- Retry behavior with exponential backoff

Create a plan to fix cache miss rate for BTCUSDT symbol.

# Normal Mode
Implement the fix. Run the FCP test suite:
uv run pytest tests/fcp/ -v

Verify cache hit rate improves in test output.
```

**Data Validation Pattern**:

```
Use a subagent to validate the DataFrame schema after fetching:
- Check column names match OHLCV schema
- Verify timestamps are UTC
- Confirm no null values in critical columns
```

**Symbol Format Verification**:

```
When working with exchange symbols, always verify:
- Binance: BTCUSDT (uppercase, no separator)
- OKX: BTC-USDT (uppercase, hyphen separator)
- Standardized: BTC/USDT (slash separator)

Test symbol conversion before committing.
```
<!-- SSoT-OK: MCP Server Configuration Reference - comprehensive MCP documentation from official docs -->

## MCP Server Configuration Reference (Detailed)

### Overview

MCP (Model Context Protocol) is an open source standard for AI-tool integrations. MCP servers give Claude Code access to external tools, databases, and APIs.

### Configuration Locations

| Scope   | Location                                   | Purpose                         |
| ------- | ------------------------------------------ | ------------------------------- |
| Local   | `~/.claude.json` (under project path)      | Personal, project-specific      |
| Project | `.mcp.json` (project root)                 | Team-shared, version-controlled |
| User    | `~/.claude.json`                           | Personal, cross-project         |
| Managed | `/Library/Application Support/ClaudeCode/` | Enterprise, IT-deployed         |

### Transport Types

| Transport | Usage                             | Command Example                                |
| --------- | --------------------------------- | ---------------------------------------------- |
| `http`    | Remote HTTP servers (recommended) | `claude mcp add --transport http name url`     |
| `sse`     | Server-Sent Events (deprecated)   | `claude mcp add --transport sse name url`      |
| `stdio`   | Local processes                   | `claude mcp add --transport stdio name -- cmd` |

### Adding MCP Servers

**HTTP Server (Remote)**:

```bash
# Basic
claude mcp add --transport http notion https://mcp.notion.com/mcp

# With authentication
claude mcp add --transport http secure-api https://api.example.com/mcp \
  --header "Authorization: Bearer your-token"
```

**SSE Server (Deprecated)**:

```bash
claude mcp add --transport sse asana https://mcp.asana.com/sse
```

**Stdio Server (Local)**:

```bash
# Basic
claude mcp add --transport stdio airtable \
  -- npx -y airtable-mcp-server

# With environment variables
claude mcp add --transport stdio --env AIRTABLE_API_KEY=YOUR_KEY airtable \
  -- npx -y airtable-mcp-server
```

**Important**: All options (`--transport`, `--env`, `--scope`, `--header`) must come before server name. `--` separates server name from command arguments.

### Scope Options

```bash
# Local scope (default) - personal, current project
claude mcp add --transport http stripe --scope local https://mcp.stripe.com

# Project scope - team-shared via .mcp.json
claude mcp add --transport http paypal --scope project https://mcp.paypal.com/mcp

# User scope - personal, cross-project
claude mcp add --transport http hubspot --scope user https://mcp.hubspot.com/mcp
```

### .mcp.json Format

```json
{
  "mcpServers": {
    "github": {
      "type": "http",
      "url": "https://api.githubcopilot.com/mcp/"
    },
    "db-server": {
      "type": "stdio",
      "command": "/path/to/server",
      "args": ["--config", "config.json"],
      "env": {
        "DB_URL": "${DB_URL}"
      }
    }
  }
}
```

### Environment Variable Expansion

Supported in `.mcp.json`:

| Syntax            | Description                    |
| ----------------- | ------------------------------ |
| `${VAR}`          | Expands to value of VAR        |
| `${VAR:-default}` | Uses default if VAR is not set |

**Expansion locations**: `command`, `args`, `env`, `url`, `headers`

```json
{
  "mcpServers": {
    "api-server": {
      "type": "http",
      "url": "${API_BASE_URL:-https://api.example.com}/mcp",
      "headers": {
        "Authorization": "Bearer ${API_KEY}"
      }
    }
  }
}
```

### Managing Servers

```bash
# List all servers
claude mcp list

# Get server details
claude mcp get github

# Remove server
claude mcp remove github

# Check status (within Claude Code)
/mcp
```

### Authentication

For OAuth 2.0 servers:

1. Add the server: `claude mcp add --transport http sentry https://mcp.sentry.dev/mcp`
2. Within Claude Code: `/mcp`
3. Follow browser prompts to login

Tokens are stored securely and refreshed automatically.

### Tool Search Optimization

When MCP tool descriptions exceed 10% of context window, Tool Search activates automatically:

| `ENABLE_TOOL_SEARCH` Value | Behavior                                 |
| -------------------------- | ---------------------------------------- |
| `auto` (default)           | Activates at 10% threshold               |
| `auto:<N>`                 | Custom threshold (e.g., `auto:5` for 5%) |
| `true`                     | Always enabled                           |
| `false`                    | Disabled, all tools loaded upfront       |

```bash
# Custom 5% threshold
ENABLE_TOOL_SEARCH=auto:5 claude

# Disable tool search
ENABLE_TOOL_SEARCH=false claude
```

**Model Requirements**: Tool search requires Sonnet 4+ or Opus 4+. Haiku does not support tool search.

### Token Management

```bash
# Warning displayed at 10,000 tokens output
# Default maximum: 25,000 tokens

# Increase limit
export MAX_MCP_OUTPUT_TOKENS=50000
claude
```

### Server Timeout

```bash
# Set 10-second startup timeout
MCP_TIMEOUT=10000 claude
```

### Plugin-Provided MCP Servers

Plugins can bundle MCP servers in `.mcp.json` at plugin root or inline in `plugin.json`:

**Plugin .mcp.json**:

```json
{
  "database-tools": {
    "command": "${CLAUDE_PLUGIN_ROOT}/servers/db-server",
    "args": ["--config", "${CLAUDE_PLUGIN_ROOT}/config.json"],
    "env": {
      "DB_URL": "${DB_URL}"
    }
  }
}
```

**Inline in plugin.json**:

```json
{
  "name": "my-plugin",
  "mcpServers": {
    "plugin-api": {
      "command": "${CLAUDE_PLUGIN_ROOT}/servers/api-server",
      "args": ["--port", "8080"]
    }
  }
}
```

Features:

- Automatic lifecycle (start when plugin enables)
- `${CLAUDE_PLUGIN_ROOT}` for plugin-relative paths
- Support for all transport types

### MCP Resources

Reference resources using `@` mentions:

```
> Can you analyze @github:issue://123 and suggest a fix?
> Compare @postgres:schema://users with @docs:file://database/user-model
```

Resources are automatically fetched and included as attachments.

### MCP Prompts as Commands

MCP servers can expose prompts as commands:

```
> /mcp__github__list_prs
> /mcp__github__pr_review 456
> /mcp__jira__create_issue "Bug in login" high
```

### Managed MCP Configuration (Enterprise)

**Option 1: Exclusive Control** (`managed-mcp.json`):

Location:

- macOS: `/Library/Application Support/ClaudeCode/managed-mcp.json`
- Linux/WSL: `/etc/claude-code/managed-mcp.json`
- Windows: `C:\Program Files\ClaudeCode\managed-mcp.json`

```json
{
  "mcpServers": {
    "github": {
      "type": "http",
      "url": "https://api.githubcopilot.com/mcp/"
    },
    "company-internal": {
      "type": "stdio",
      "command": "/usr/local/bin/company-mcp-server"
    }
  }
}
```

**Option 2: Policy-Based Control** (in managed-settings.json):

```json
{
  "allowedMcpServers": [
    { "serverName": "github" },
    {
      "serverCommand": ["npx", "-y", "@modelcontextprotocol/server-filesystem"]
    },
    { "serverUrl": "https://mcp.company.com/*" }
  ],
  "deniedMcpServers": [
    { "serverName": "dangerous-server" },
    { "serverUrl": "https://*.untrusted.com/*" }
  ]
}
```

**URL Wildcards**:

- `https://mcp.company.com/*` - All paths on domain
- `https://*.example.com/*` - Any subdomain
- `http://localhost:*/*` - Any port on localhost

### Popular MCP Servers

| Server     | Purpose                   | Command                                                                     |
| ---------- | ------------------------- | --------------------------------------------------------------------------- |
| GitHub     | PRs, issues, code reviews | `claude mcp add --transport http github https://api.githubcopilot.com/mcp/` |
| Sentry     | Error monitoring          | `claude mcp add --transport http sentry https://mcp.sentry.dev/mcp`         |
| PostgreSQL | Database queries          | `claude mcp add --transport stdio db -- npx -y @bytebase/dbhub --dsn "..."` |
| Notion     | Documentation, wikis      | `claude mcp add --transport http notion https://mcp.notion.com/mcp`         |
| Figma      | Design integration        | `claude mcp add --transport http figma https://mcp.figma.com/mcp`           |

### Claude Code as MCP Server

Use Claude Code as a server for other applications:

```bash
claude mcp serve
```

Claude Desktop configuration:

```json
{
  "mcpServers": {
    "claude-code": {
      "type": "stdio",
      "command": "claude",
      "args": ["mcp", "serve"],
      "env": {}
    }
  }
}
```

### Import from Claude Desktop

```bash
# Import servers from Claude Desktop
claude mcp add-from-claude-desktop

# Verify import
claude mcp list
```

Works on macOS and WSL. Use `--scope user` for user configuration.

### Add from JSON

```bash
# HTTP server
claude mcp add-json weather-api '{"type":"http","url":"https://api.weather.com/mcp"}'

# Stdio server
claude mcp add-json local-server '{"type":"stdio","command":"/path/to/server","args":["--port","8080"]}'
```

### Windows Notes

On native Windows (not WSL), use `cmd /c` wrapper:

```bash
claude mcp add --transport stdio my-server -- cmd /c npx -y @some/package
```

### DSM MCP Configuration

Example `.mcp.json` for data-source-manager:

```json
{
  "mcpServers": {
    "postgres-analytics": {
      "type": "stdio",
      "command": "npx",
      "args": ["-y", "@bytebase/dbhub", "--dsn", "${ANALYTICS_DB_URL}"],
      "env": {}
    },
    "binance-status": {
      "type": "http",
      "url": "${BINANCE_MCP_URL:-https://api.binance.com/mcp}",
      "headers": {
        "X-MBX-APIKEY": "${BINANCE_API_KEY}"
      }
    }
  }
}
```

### Best Practices

**Security**:

- Treat third-party servers like code dependencies
- Pin versions, review scopes
- Run sensitive servers in sandboxes
- Use environment variables for credentials

**Performance**:

- Enable Tool Search for many tools
- Configure appropriate token limits
- Use project scope for team consistency

**Organization**:

- Use user scope for personal utilities
- Use project scope for team-shared tools
- Document server purposes in README
<!-- SSoT-OK: This section is authoritative for common workflows patterns -->

## Common Workflows Reference

Practical workflows for everyday Claude Code development tasks.

### Codebase Exploration Workflows

**Quick Overview Pattern**:

```
# Step 1: Get high-level overview
> give me an overview of this codebase

# Step 2: Dive into architecture
> explain the main architecture patterns used here

# Step 3: Understand specifics
> what are the key data models?
> how is authentication handled?
```

**Finding Relevant Code**:

```
# Find files
> find the files that handle user authentication

# Understand interactions
> how do these authentication files work together?

# Trace execution
> trace the login process from front-end to database
```

**DSM-Specific Exploration**:

```
# FCP exploration
> find files that implement FCP decision logic

# Symbol handling
> trace symbol format conversion from API to cache

# Cache structure
> explain the cache directory hierarchy
```

| Tip                 | Description                                  |
| ------------------- | -------------------------------------------- |
| Start broad         | Begin with overview, narrow to specifics     |
| Use domain language | Reference FCP, symbols, OHLCV in DSM context |
| Install LSP plugin  | Enables "go to definition" navigation        |

### Bug Fixing Workflows

**Error Diagnosis Pattern**:

```
# Step 1: Share error
> I'm seeing an error when I run uv run pytest

# Step 2: Get recommendations
> suggest a few ways to fix the TypeError in fetcher.py

# Step 3: Apply fix
> update fetcher.py with the null check you suggested
```

**DSM Bug Patterns**:

```
# FCP debugging
> why is FCP returning SKIP for this symbol?

# Cache issues
> the cache shows stale data, trace the invalidation logic

# Symbol format bugs
> trace symbol format conversion for BTC-USDT
```

| Approach           | When to Use                        |
| ------------------ | ---------------------------------- |
| Stack trace        | Share complete error output        |
| Reproduction steps | Include exact command to reproduce |
| Intermittent bugs  | Note frequency and conditions      |

### Refactoring Workflows

**Legacy Code Update Pattern**:

```
# Step 1: Find deprecated usage
> find deprecated API usage in our codebase

# Step 2: Get recommendations
> suggest how to refactor utils.py to use modern patterns

# Step 3: Apply safely
> refactor utils.py while maintaining same behavior

# Step 4: Verify
> run tests for the refactored code
```

**DSM Refactoring Patterns**:

```
# Fetcher modernization
> refactor BinanceFetcher to use async/await

# Error handling improvement
> add proper exception handling to the FCP module

# Cache optimization
> refactor cache layer to use connection pooling
```

| Principle           | Description                              |
| ------------------- | ---------------------------------------- |
| Small increments    | Test each change before next             |
| Backward compatible | Maintain existing behavior when needed   |
| Explain benefits    | Understand why modern approach is better |

### Test Writing Workflows

**Test Coverage Pattern**:

```
# Step 1: Identify gaps
> find functions in fetcher.py that are not covered by tests

# Step 2: Generate scaffolding
> add tests for the fetcher module

# Step 3: Add edge cases
> add test cases for edge conditions in the fetcher

# Step 4: Run and fix
> run the new tests and fix any failures
```

**DSM Test Patterns**:

```
# Integration tests
> add integration tests for BinanceFetcher with mocked responses

# FCP tests
> add tests for FCP decision logic edge cases

# Cache tests
> add tests for cache invalidation scenarios
```

**Test Generation Best Practices**:

| Practice                   | Description                        |
| -------------------------- | ---------------------------------- |
| Follow existing patterns   | Claude matches project conventions |
| Be specific about behavior | State what to verify               |
| Request edge cases         | Ask for boundary, error conditions |

### Plan Mode Workflow

Plan Mode restricts Claude to read-only operations for safe exploration.

**When to Use Plan Mode**:

| Scenario                  | Description                        |
| ------------------------- | ---------------------------------- |
| Multi-step implementation | Features requiring many file edits |
| Code exploration          | Research before any changes        |
| Interactive development   | Iterate on direction with Claude   |

**Starting Plan Mode**:

```bash
# Start session in Plan Mode
claude --permission-mode plan

# Run headless query in Plan Mode
claude --permission-mode plan -p "Analyze the authentication system"
```

**During Session**:

- Press `Shift+Tab` to cycle modes (Normal → Auto-Accept → Plan)
- `⏸ plan mode on` indicator shows active Plan Mode
- Press `Ctrl+G` to open plan in text editor

**Example: Planning Complex Refactor**:

```bash
claude --permission-mode plan
```

```
> I need to refactor our caching system to use Redis. Create a detailed migration plan.
```

Follow-up refinements:

```
> What about backward compatibility?
> How should we handle the migration?
```

**Configure as Default**:

```json
// .claude/settings.json
{
  "permissions": {
    "defaultMode": "plan"
  }
}
```

### Pull Request Workflow

**Quick PR Creation**:

```
> /commit-push-pr
```

Creates commit, pushes branch, and opens PR in one step.

**Step-by-Step PR**:

```
# Step 1: Summarize changes
> summarize the changes I've made to the caching module

# Step 2: Create PR
> create a pr

# Step 3: Enhance description
> enhance the PR description with more context
```

**DSM PR Patterns**:

```
# Feature PR
> create a pr for the new OKX fetcher implementation

# Bug fix PR
> create a pr for the FCP cache invalidation fix

# Refactor PR
> create a pr for the async fetcher migration
```

| Tip                      | Description                         |
| ------------------------ | ----------------------------------- |
| Review before submitting | Always review Claude's PR           |
| Highlight risks          | Ask Claude to note potential issues |
| Use Slack integration    | Configure MCP for auto-posting      |

### Documentation Workflow

**Documentation Pattern**:

```
# Step 1: Find gaps
> find functions without proper docstrings in the fetcher module

# Step 2: Generate docs
> add docstrings to the undocumented functions in fetcher.py

# Step 3: Enhance
> improve the documentation with more context and examples

# Step 4: Verify standards
> check if the documentation follows our project standards
```

**DSM Documentation Patterns**:

```
# API documentation
> document the public API for DataSourceManager

# ADR updates
> update the FCP ADR with the new decision logic

# README updates
> update README with new installation instructions
```

| Tip                  | Description                             |
| -------------------- | --------------------------------------- |
| Specify style        | Request JSDoc, docstrings, etc.         |
| Request examples     | Include usage examples in docs          |
| Focus on public APIs | Prioritize interfaces and complex logic |

### Image and Visual Workflow

**Adding Images to Conversation**:

| Method         | How                                                    |
| -------------- | ------------------------------------------------------ |
| Drag and drop  | Drag image into Claude Code window                     |
| Paste          | Copy image, paste with `Ctrl+V` (not `Cmd+V`)          |
| Path reference | Provide path: `Analyze this image: /path/to/image.png` |

**Image Analysis Prompts**:

```
# General analysis
> What does this image show?

# UI analysis
> Describe the UI elements in this screenshot

# Error context
> Here's a screenshot of the error. What's causing it?

# Code from visuals
> Generate CSS to match this design mockup
```

**DSM Visual Patterns**:

```
# Architecture diagram
> Here's our current architecture. How should we modify it?

# Error screenshot
> This is the FCP debug output. What's wrong?

# Chart analysis
> Analyze this OHLCV chart for anomalies
```

### File Reference Workflow

Use `@` to quickly include files without waiting for reads.

**Reference Types**:

```
# Single file
> Explain the logic in @src/fetchers/binance.py

# Directory
> What's the structure of @src/fetchers?

# MCP resources
> Show me the data from @github:repos/owner/repo/issues
```

**DSM File References**:

```
# Fetcher analysis
> Review @src/fetchers/binance.py for error handling

# Cache structure
> Explain @src/cache/ directory organization

# Test review
> Check @tests/fetchers/test_binance.py coverage
```

| Tip                  | Description                      |
| -------------------- | -------------------------------- |
| Relative or absolute | Both path formats work           |
| Context loading      | @ loads parent CLAUDE.md files   |
| Multiple files       | Reference several in one message |

### Extended Thinking Workflow

Extended thinking reserves tokens for step-by-step reasoning.

**Configuration**:

| Scope          | Method                                       |
| -------------- | -------------------------------------------- |
| Toggle         | `Option+T` (macOS) / `Alt+T` (Windows/Linux) |
| Global default | `/config` to toggle                          |
| Token limit    | `MAX_THINKING_TOKENS` environment variable   |

**View Thinking Process**:

Press `Ctrl+O` to toggle verbose mode, showing reasoning as gray italic text.

**When to Use Extended Thinking**:

| Scenario             | Benefit                       |
| -------------------- | ----------------------------- |
| Complex architecture | Explore multiple approaches   |
| Challenging bugs     | Analyze edge cases thoroughly |
| Multi-step planning  | Evaluate tradeoffs            |

**Token Budget**:

- **Enabled**: Up to 31,999 tokens for reasoning
- **Disabled**: 0 tokens for thinking
- **Charged**: All thinking tokens count toward cost

### Session Management Workflow

**Resume Previous Sessions**:

```bash
# Continue most recent
claude --continue

# Open session picker
claude --resume

# Resume by name
claude --resume auth-refactor
```

**During Session**:

```
# Switch to different session
> /resume

# Rename current session
> /rename auth-refactor
```

**Session Picker Shortcuts**:

| Shortcut | Action                   |
| -------- | ------------------------ |
| `↑`/`↓`  | Navigate sessions        |
| `→`/`←`  | Expand/collapse groups   |
| `Enter`  | Resume selected          |
| `P`      | Preview session          |
| `R`      | Rename session           |
| `/`      | Search filter            |
| `A`      | Toggle all projects      |
| `B`      | Filter by current branch |
| `Esc`    | Exit picker              |

**Best Practices**:

| Practice              | Description                                |
| --------------------- | ------------------------------------------ |
| Name early            | Use `/rename` when starting distinct tasks |
| Use `--continue`      | Quick access to most recent                |
| Preview before resume | Press `P` to check content                 |

### Git Worktree Workflow

Run parallel Claude sessions with isolated code.

**Create Worktree**:

```bash
# New worktree with new branch
git worktree add ../project-feature-a -b feature-a

# Worktree with existing branch
git worktree add ../project-bugfix bugfix-123
```

**Run Parallel Sessions**:

```bash
# Terminal 1: Feature work
cd ../project-feature-a
claude

# Terminal 2: Bug fix
cd ../project-bugfix
claude
```

**Manage Worktrees**:

```bash
# List all
git worktree list

# Remove when done
git worktree remove ../project-feature-a
```

**DSM Worktree Patterns**:

```bash
# Feature development
git worktree add ../dsm-okx-fetcher -b feature/okx-fetcher

# FCP debugging
git worktree add ../dsm-fcp-debug -b debug/fcp-cache-issue
```

| Tip                    | Description                          |
| ---------------------- | ------------------------------------ |
| Independent file state | Changes don't affect other worktrees |
| Shared Git history     | All worktrees share commits/remotes  |
| Initialize environment | Run `uv sync` in each new worktree   |

### Unix Utility Workflow

Use Claude as a command-line utility.

**Build Script Integration**:

```json
// package.json
{
  "scripts": {
    "lint:claude": "claude -p 'you are a linter. look at changes vs. main and report issues.'"
  }
}
```

**Pipe Data Through Claude**:

```bash
# Analyze build error
cat build-error.txt | claude -p 'explain the root cause' > output.txt

# Code review
git diff | claude -p 'review these changes for bugs'
```

**Output Formats**:

| Format        | Usage                         | Description              |
| ------------- | ----------------------------- | ------------------------ |
| `text`        | Default                       | Plain text response      |
| `json`        | `--output-format json`        | JSON array with metadata |
| `stream-json` | `--output-format stream-json` | Real-time JSON objects   |

**DSM Unix Patterns**:

```bash
# Analyze test failures
uv run pytest 2>&1 | claude -p 'explain test failures'

# Review FCP logs
cat logs/fcp-debug.log | claude -p 'identify FCP decision anomalies'
```

### Subagent Workflow

Use specialized subagents for specific tasks.

**View Available Subagents**:

```
> /agents
```

**Automatic Delegation**:

Claude automatically delegates to appropriate subagents:

```
> review my recent code changes for security issues
> run all tests and fix any failures
```

**Explicit Subagent Request**:

```
> use the code-reviewer subagent to check the auth module
> have the debugger subagent investigate the login issue
```

**Create Custom Subagents**:

```
> /agents
```

Select "Create New subagent" and define:

- Unique identifier (e.g., `fcp-debugger`)
- When Claude should use it
- Tool access permissions
- System prompt for behavior

**DSM Subagent Usage**:

```
# FCP debugging
> use the fcp-debugger agent to investigate cache miss

# API review
> use the api-reviewer agent to check the new fetcher

# Test writing
> use the test-writer agent to add cache tests
```

| Tip                     | Description                                  |
| ----------------------- | -------------------------------------------- |
| Project-specific        | Create in `.claude/agents/` for team sharing |
| Descriptive description | Enables automatic delegation                 |
| Minimal tools           | Limit to what subagent needs                 |

### Self-Documentation Workflow

Claude can answer questions about its own capabilities.

**Example Queries**:

```
> can Claude Code create pull requests?
> how does Claude Code handle permissions?
> what skills are available?
> how do I use MCP with Claude Code?
> how do I configure Claude Code for Amazon Bedrock?
```

Claude has built-in access to current documentation regardless of version.

| Tip                | Description                                  |
| ------------------ | -------------------------------------------- |
| Specific questions | Get detailed, accurate answers               |
| Latest docs        | Always current documentation                 |
| Complex features   | Explains MCP, enterprise, advanced workflows |

### DSM-Specific Workflow Patterns

**FCP Debugging Workflow**:

```
# Step 1: Check FCP state
> /debug-fcp BTCUSDT

# Step 2: Trace decision logic
> trace FCP decision for BTCUSDT from cache to API

# Step 3: Validate data
> validate the OHLCV data returned for BTCUSDT
```

**Data Fetching Workflow**:

```
# Step 1: Fetch with validation
> /fetch-data BTCUSDT 1h

# Step 2: Validate structure
> /validate-data

# Step 3: Check cache state
> verify cache was updated correctly
```

**Feature Development Workflow**:

```
# Step 1: Start guided development
> /feature-dev

# Step 2: Follow prompts for requirements
# Step 3: Implement with DSM patterns
# Step 4: Run quick tests
> /quick-test
```

**Code Review Workflow**:

```
# Step 1: Review against DSM patterns
> /review-dsm

# Step 2: Check specific concerns
> review error handling in the new fetcher

# Step 3: Validate integration
> check FCP integration for the new data source
```

### Workflow Composition Patterns

**TDD Workflow**:

```
# Step 1: Write failing test
> add a test for the new fetch_historical method

# Step 2: Implement feature
> implement fetch_historical to make the test pass

# Step 3: Refactor
> refactor fetch_historical for better readability

# Step 4: Verify
> run all tests to ensure no regressions
```

**Code Review + Fix Workflow**:

```
# Step 1: Get review
> review the changes in src/fetchers/

# Step 2: Address issues
> fix the issues you identified

# Step 3: Verify fixes
> run tests to confirm fixes work
```

**Research + Implement Workflow**:

```
# Step 1: Plan Mode research
claude --permission-mode plan
> analyze how we should implement rate limiting

# Step 2: Exit Plan Mode (Shift+Tab)
# Step 3: Implement based on plan
> implement rate limiting following the plan
```

### Workflow Best Practices

| Practice                      | Description                                |
| ----------------------------- | ------------------------------------------ |
| Start with exploration        | Understand before modifying                |
| Use Plan Mode for research    | Safe read-only analysis                    |
| Name sessions                 | Easy resume with `/rename`                 |
| Leverage subagents            | Delegate specialized tasks                 |
| Small increments              | Test each change before next               |
| Verify with tests             | Always run tests after changes             |
| Use DSM commands              | `/debug-fcp`, `/fetch-data`, `/quick-test` |
| Reference files with @        | Fast context loading                       |
| Extended thinking for complex | Enable for architecture decisions          |
<!-- SSoT-OK: This section is authoritative for interactive mode and terminal configuration -->

## Interactive Mode Reference

Complete reference for keyboard shortcuts, input modes, vim mode, and terminal configuration.

### General Control Shortcuts

| Shortcut     | Description                        | Context                                  |
| ------------ | ---------------------------------- | ---------------------------------------- |
| `Ctrl+C`     | Cancel current input or generation | Standard interrupt                       |
| `Ctrl+D`     | Exit Claude Code session           | EOF signal                               |
| `Ctrl+G`     | Open in default text editor        | Edit prompt in external editor           |
| `Ctrl+L`     | Clear terminal screen              | Keeps conversation history               |
| `Ctrl+O`     | Toggle verbose output              | Shows detailed tool execution            |
| `Ctrl+R`     | Reverse search command history     | Search previous commands                 |
| `Ctrl+V`     | Paste image from clipboard         | Also `Cmd+V` (iTerm2), `Alt+V` (Windows) |
| `Ctrl+B`     | Background running tasks           | Tmux users press twice                   |
| `Left/Right` | Cycle through dialog tabs          | Navigate permission dialogs              |
| `Up/Down`    | Navigate command history           | Recall previous inputs                   |
| `Esc Esc`    | Rewind code/conversation           | Restore to previous point                |
| `Shift+Tab`  | Toggle permission modes            | Normal ↔ Auto-Accept ↔ Plan              |
| `Alt+M`      | Toggle permission modes            | Alternative to Shift+Tab                 |
| `Alt+P`      | Switch model                       | Switch without clearing prompt           |
| `Alt+T`      | Toggle extended thinking           | Enable/disable thinking mode             |

### Text Editing Shortcuts

| Shortcut | Description                  | Context                        |
| -------- | ---------------------------- | ------------------------------ |
| `Ctrl+K` | Delete to end of line        | Stores for pasting             |
| `Ctrl+U` | Delete entire line           | Stores for pasting             |
| `Ctrl+Y` | Paste deleted text           | After `Ctrl+K` or `Ctrl+U`     |
| `Alt+Y`  | Cycle paste history          | After `Ctrl+Y`, cycles history |
| `Alt+B`  | Move cursor back one word    | Word navigation                |
| `Alt+F`  | Move cursor forward one word | Word navigation                |

**macOS Note**: `Alt+Y`, `Alt+B`, `Alt+F` require Option as Meta configuration.

### Theme and Display Shortcuts

| Shortcut  | Description                | Context                     |
| --------- | -------------------------- | --------------------------- |
| `Ctrl+T`  | Toggle syntax highlighting | Inside `/theme` picker only |
| `/theme`  | Change color theme         | Match terminal theme        |
| `/config` | Open settings interface    | Configure all options       |

### Multiline Input Methods

| Method           | Shortcut       | Terminal Support                |
| ---------------- | -------------- | ------------------------------- |
| Quick escape     | `\` + `Enter`  | All terminals                   |
| macOS default    | `Option+Enter` | macOS terminals                 |
| Shift+Enter      | `Shift+Enter`  | iTerm2, WezTerm, Ghostty, Kitty |
| Control sequence | `Ctrl+J`       | All terminals                   |
| Paste mode       | Direct paste   | Code blocks, logs               |

**Other Terminals** (VS Code, Alacritty, Zed, Warp): Run `/terminal-setup` to install Shift+Enter binding.

### Quick Command Prefixes

| Prefix | Description       | Example                      |
| ------ | ----------------- | ---------------------------- |
| `/`    | Command or skill  | `/help`, `/commit`           |
| `!`    | Bash mode         | `! npm test`, `! git status` |
| `@`    | File path mention | `@src/main.py`               |

### Built-in Commands Reference

**Session Commands**:

| Command                   | Purpose                         |
| ------------------------- | ------------------------------- |
| `/clear`                  | Clear conversation history      |
| `/compact [instructions]` | Compact conversation with focus |
| `/exit`                   | Exit the REPL                   |
| `/resume [session]`       | Resume conversation by ID/name  |
| `/rename <name>`          | Rename current session          |
| `/export [filename]`      | Export conversation to file     |

**Configuration Commands**:

| Command        | Purpose                    |
| -------------- | -------------------------- |
| `/config`      | Open settings (Config tab) |
| `/permissions` | View/update permissions    |
| `/model`       | Select/change AI model     |
| `/memory`      | Edit CLAUDE.md files       |
| `/mcp`         | Manage MCP connections     |
| `/theme`       | Change color theme         |
| `/statusline`  | Configure status line      |

**Utility Commands**:

| Command    | Purpose                           |
| ---------- | --------------------------------- |
| `/context` | Visualize context usage           |
| `/cost`    | Show token usage statistics       |
| `/doctor`  | Check installation health         |
| `/help`    | Get usage help                    |
| `/init`    | Initialize project with CLAUDE.md |
| `/stats`   | Visualize daily usage and history |
| `/status`  | Show version, model, account      |
| `/copy`    | Copy last response to clipboard   |
| `/tasks`   | List background tasks             |
| `/todos`   | List current TODO items           |
| `/usage`   | Show plan limits (subscribers)    |

**Mode Commands**:

| Command     | Purpose                             |
| ----------- | ----------------------------------- |
| `/vim`      | Enable vim mode                     |
| `/plan`     | Enter plan mode from prompt         |
| `/rewind`   | Rewind conversation/code            |
| `/teleport` | Resume remote session (subscribers) |

### Vim Editor Mode

Enable with `/vim` command or configure permanently via `/config`.

**Mode Switching**:

| Command | Action                   | From Mode |
| ------- | ------------------------ | --------- |
| `Esc`   | Enter NORMAL mode        | INSERT    |
| `i`     | Insert before cursor     | NORMAL    |
| `I`     | Insert at line beginning | NORMAL    |
| `a`     | Insert after cursor      | NORMAL    |
| `A`     | Insert at line end       | NORMAL    |
| `o`     | Open line below          | NORMAL    |
| `O`     | Open line above          | NORMAL    |

**Navigation (NORMAL mode)**:

| Command         | Action                         |
| --------------- | ------------------------------ |
| `h`/`j`/`k`/`l` | Move left/down/up/right        |
| `w`             | Next word                      |
| `e`             | End of word                    |
| `b`             | Previous word                  |
| `0`             | Beginning of line              |
| `$`             | End of line                    |
| `^`             | First non-blank character      |
| `gg`            | Beginning of input             |
| `G`             | End of input                   |
| `f{char}`       | Jump to next occurrence        |
| `F{char}`       | Jump to previous occurrence    |
| `t{char}`       | Jump before next occurrence    |
| `T{char}`       | Jump after previous occurrence |
| `;`             | Repeat last f/F/t/T            |
| `,`             | Reverse repeat f/F/t/T         |

**History Navigation**: When cursor at beginning/end of input, arrow keys navigate command history.

**Editing (NORMAL mode)**:

| Command        | Action                  |
| -------------- | ----------------------- |
| `x`            | Delete character        |
| `dd`           | Delete line             |
| `D`            | Delete to end of line   |
| `dw`/`de`/`db` | Delete word/to end/back |
| `cc`           | Change line             |
| `C`            | Change to end of line   |
| `cw`/`ce`/`cb` | Change word/to end/back |
| `yy`/`Y`       | Yank (copy) line        |
| `yw`/`ye`/`yb` | Yank word/to end/back   |
| `p`            | Paste after cursor      |
| `P`            | Paste before cursor     |
| `>>`           | Indent line             |
| `<<`           | Dedent line             |
| `J`            | Join lines              |
| `.`            | Repeat last change      |

**Text Objects (with d, c, y operators)**:

| Command   | Action                     |
| --------- | -------------------------- |
| `iw`/`aw` | Inner/around word          |
| `iW`/`aW` | Inner/around WORD          |
| `i"`/`a"` | Inner/around double quotes |
| `i'`/`a'` | Inner/around single quotes |
| `i(`/`a(` | Inner/around parentheses   |
| `i[`/`a[` | Inner/around brackets      |
| `i{`/`a{` | Inner/around braces        |

### Reverse History Search

Search through command history with `Ctrl+R`:

| Action                 | Key            |
| ---------------------- | -------------- |
| Start search           | `Ctrl+R`       |
| Navigate older matches | `Ctrl+R` again |
| Accept and edit        | `Tab` or `Esc` |
| Accept and execute     | `Enter`        |
| Cancel search          | `Ctrl+C`       |
| Cancel on empty        | `Backspace`    |

### Background Tasks

Run commands in background while continuing to work.

**Starting Background Tasks**:

| Method   | Description                             |
| -------- | --------------------------------------- |
| Prompt   | Ask Claude to run command in background |
| `Ctrl+B` | Move running command to background      |
| Tmux     | Press `Ctrl+B` twice (tmux prefix key)  |

**Background Task Features**:

- Output buffered for later retrieval via TaskOutput
- Unique IDs for tracking
- Automatic cleanup on exit
- Disable with `CLAUDE_CODE_DISABLE_BACKGROUND_TASKS=1`

**Common Backgrounded Commands**:

| Type                | Examples               |
| ------------------- | ---------------------- |
| Build tools         | webpack, vite, make    |
| Package managers    | npm, yarn, pnpm        |
| Test runners        | jest, pytest           |
| Development servers | Flask, Django, Next.js |
| Long processes      | docker, terraform      |

### Bash Mode (`!` prefix)

Run bash commands directly without Claude interpretation:

```bash
! npm test
! git status
! ls -la
```

**Features**:

- Adds command and output to conversation context
- Shows real-time progress and output
- Supports `Ctrl+B` backgrounding
- History-based autocomplete with `Tab`

### Task List Feature

Claude creates task lists for complex multi-step work.

**Task List Controls**:

| Action         | Method                  |
| -------------- | ----------------------- |
| Toggle view    | `Ctrl+T`                |
| Show all tasks | Ask "show me all tasks" |
| Clear tasks    | Ask "clear all tasks"   |

**Task List Behavior**:

- Shows up to 10 tasks at a time
- Persists across context compactions
- Share across sessions: `CLAUDE_CODE_TASK_LIST_ID=my-project`
- Revert to TODO list: `CLAUDE_CODE_ENABLE_TASKS=false`

### PR Review Status

Display PR status in footer when on branch with open PR.

**Status Colors**:

| Color  | Meaning           |
| ------ | ----------------- |
| Green  | Approved          |
| Yellow | Pending review    |
| Red    | Changes requested |
| Gray   | Draft             |

**Features**:

- Clickable link (Cmd+click / Ctrl+click)
- Auto-updates every 60 seconds
- Requires `gh` CLI installed and authenticated

### Terminal Configuration

**Theme Matching**:

Match Claude Code theme to terminal via `/config`. Terminal theme controlled by terminal application.

**Custom Status Line**:

Configure via `/statusline` for contextual info (model, directory, git branch).

### Option/Alt Key Setup

Required for shortcuts like `Alt+B`, `Alt+F`, `Alt+Y`, `Alt+M`, `Alt+P`.

**iTerm2**:

1. Settings → Profiles → Keys
2. Set Left/Right Option key to "Esc+"

**Terminal.app**:

1. Settings → Profiles → Keyboard
2. Check "Use Option as Meta Key"

**VS Code Terminal**:

1. Settings → Profiles → Keys
2. Set Left/Right Option key to "Esc+"

### Line Break Setup by Terminal

**Native Support** (no setup needed):

- iTerm2
- WezTerm
- Ghostty
- Kitty

**Requires `/terminal-setup`**:

- VS Code integrated terminal
- Alacritty
- Zed
- Warp

**Universal Methods**:

- `\` + `Enter` - Quick escape
- `Ctrl+J` - Control sequence
- Direct paste for code blocks

### Notification Setup

**iTerm2 System Notifications**:

1. Preferences → Profiles → Terminal
2. Enable "Silence bell"
3. Filter Alerts → "Send escape sequence-generated alerts"
4. Set notification delay

**Custom Notification Hooks**:

Create notification hooks for advanced handling. See hooks documentation.

### Handling Large Inputs

| Issue                  | Solution                          |
| ---------------------- | --------------------------------- |
| Long pasted content    | Write to file, ask Claude to read |
| VS Code truncation     | Use file-based workflow           |
| Very long instructions | Break into smaller prompts        |

### DSM Interactive Patterns

**Quick Status Checks**:

```bash
! uv run pytest tests/unit/ -v --tb=short
! mise run cache:status
! git log --oneline -5
```

**Background Development Server**:

```
# Background the FCP test server
> run the FCP mock server in background
```

Then use `Ctrl+B` to background, continue with other tasks.

**Vim Mode for Long Prompts**:

```
/vim
# Now use vim bindings for complex prompt editing
# i to insert, Esc to normal, dd to delete line
```

**File Reference Workflow**:

```
# Quick file reference with @
> Review @src/fetchers/binance.py

# Multiple files
> Compare @src/fetchers/binance.py and @src/fetchers/okx.py
```

### Keyboard Shortcut Summary

**Essential Shortcuts**:

| Shortcut    | Action             |
| ----------- | ------------------ |
| `Ctrl+C`    | Cancel/interrupt   |
| `Ctrl+D`    | Exit               |
| `Ctrl+L`    | Clear screen       |
| `Up/Down`   | History navigation |
| `Shift+Tab` | Toggle modes       |
| `Esc Esc`   | Rewind             |

**Power User Shortcuts**:

| Shortcut | Action          |
| -------- | --------------- |
| `Ctrl+R` | Reverse search  |
| `Ctrl+G` | External editor |
| `Ctrl+B` | Background task |
| `Ctrl+O` | Verbose mode    |
| `Alt+P`  | Switch model    |
| `Alt+T`  | Toggle thinking |

**Vim Mode Essentials**:

| Shortcut | Action      |
| -------- | ----------- |
| `i`/`a`  | Insert mode |
| `Esc`    | Normal mode |
| `dd`     | Delete line |
| `yy`     | Yank line   |
| `p`      | Paste       |
| `.`      | Repeat      |
<!-- SSoT-OK: This section is authoritative for CLI reference and checkpointing -->

## CLI Reference and Checkpointing

Complete reference for command-line interface, flags, and checkpoint/rewind functionality.

### CLI Commands

| Command                         | Description                       | Example                                 |
| ------------------------------- | --------------------------------- | --------------------------------------- |
| `claude`                        | Start interactive REPL            | `claude`                                |
| `claude "query"`                | Start REPL with initial prompt    | `claude "explain this project"`         |
| `claude -p "query"`             | Query via SDK, then exit          | `claude -p "explain this function"`     |
| `cat file \| claude -p "query"` | Process piped content             | `cat logs.txt \| claude -p "explain"`   |
| `claude -c`                     | Continue most recent conversation | `claude -c`                             |
| `claude -c -p "query"`          | Continue via SDK                  | `claude -c -p "Check for type errors"`  |
| `claude -r "<session>" "query"` | Resume session by ID/name         | `claude -r "auth-refactor" "Finish PR"` |
| `claude update`                 | Update to latest version          | `claude update`                         |
| `claude mcp`                    | Configure MCP servers             | See MCP documentation                   |

### Session Management Flags

| Flag                       | Description                         | Example                                      |
| -------------------------- | ----------------------------------- | -------------------------------------------- |
| `--continue`, `-c`         | Load most recent conversation       | `claude --continue`                          |
| `--resume`, `-r`           | Resume session by ID/name           | `claude --resume auth-refactor`              |
| `--fork-session`           | Create new session ID when resuming | `claude --resume abc123 --fork-session`      |
| `--session-id`             | Use specific session ID (UUID)      | `claude --session-id "550e8400-..."`         |
| `--no-session-persistence` | Don't save session (print mode)     | `claude -p --no-session-persistence "query"` |

### Model and Permission Flags

| Flag                                   | Description                           | Example                                            |
| -------------------------------------- | ------------------------------------- | -------------------------------------------------- |
| `--model`                              | Set model (alias or full name)        | `claude --model sonnet`                            |
| `--fallback-model`                     | Fallback when overloaded (print mode) | `claude -p --fallback-model sonnet "query"`        |
| `--permission-mode`                    | Start in permission mode              | `claude --permission-mode plan`                    |
| `--dangerously-skip-permissions`       | Skip all permission prompts           | Use with caution                                   |
| `--allow-dangerously-skip-permissions` | Enable bypassing as option            | Compose with `--permission-mode`                   |
| `--permission-prompt-tool`             | MCP tool for permission prompts       | `claude -p --permission-prompt-tool mcp_auth_tool` |

### Tool Control Flags

| Flag                       | Description                       | Example                           |
| -------------------------- | --------------------------------- | --------------------------------- |
| `--tools`                  | Restrict available tools          | `claude --tools "Bash,Edit,Read"` |
| `--allowedTools`           | Tools without permission prompts  | `"Bash(git log *)" "Read"`        |
| `--disallowedTools`        | Remove tools from context         | `"Bash(git log *)" "Edit"`        |
| `--disable-slash-commands` | Disable skills and slash commands | `claude --disable-slash-commands` |

**Tools Flag Values**:

| Value              | Effect              |
| ------------------ | ------------------- |
| `""`               | Disable all tools   |
| `"default"`        | All tools           |
| `"Bash,Edit,Read"` | Specific tools only |

### Output and Format Flags

| Flag                         | Description                           | Example                                          |
| ---------------------------- | ------------------------------------- | ------------------------------------------------ |
| `--print`, `-p`              | Print mode (non-interactive)          | `claude -p "query"`                              |
| `--output-format`            | Output format (text/json/stream-json) | `claude -p --output-format json "query"`         |
| `--input-format`             | Input format (text/stream-json)       | `claude -p --input-format stream-json`           |
| `--include-partial-messages` | Include streaming events              | Requires `--output-format=stream-json`           |
| `--json-schema`              | Get validated JSON output             | `claude -p --json-schema '{"type":"object"...}'` |
| `--verbose`                  | Enable verbose logging                | `claude --verbose`                               |
| `--debug`                    | Debug mode with categories            | `claude --debug "api,mcp"`                       |

### System Prompt Flags

| Flag                          | Behavior                      | Modes               |
| ----------------------------- | ----------------------------- | ------------------- |
| `--system-prompt`             | Replace entire default prompt | Interactive + Print |
| `--system-prompt-file`        | Replace with file contents    | Print only          |
| `--append-system-prompt`      | Append to default prompt      | Interactive + Print |
| `--append-system-prompt-file` | Append file contents          | Print only          |

**Usage Guidelines**:

| Flag                          | Use Case                                 |
| ----------------------------- | ---------------------------------------- |
| `--system-prompt`             | Complete control over behavior           |
| `--system-prompt-file`        | Team consistency, version control        |
| `--append-system-prompt`      | Add instructions, keep defaults (safest) |
| `--append-system-prompt-file` | Version-controlled additions             |

**Note**: `--system-prompt` and `--system-prompt-file` are mutually exclusive. Append flags can combine with replacement flags.

### Directory and Context Flags

| Flag           | Description                        | Example                            |
| -------------- | ---------------------------------- | ---------------------------------- |
| `--add-dir`    | Add additional working directories | `claude --add-dir ../apps ../lib`  |
| `--plugin-dir` | Load plugins from directories      | `claude --plugin-dir ./my-plugins` |

### MCP and Integration Flags

| Flag                  | Description                       | Example                                              |
| --------------------- | --------------------------------- | ---------------------------------------------------- |
| `--mcp-config`        | Load MCP servers from JSON        | `claude --mcp-config ./mcp.json`                     |
| `--strict-mcp-config` | Only use specified MCP config     | `claude --strict-mcp-config --mcp-config ./mcp.json` |
| `--chrome`            | Enable Chrome browser integration | `claude --chrome`                                    |
| `--no-chrome`         | Disable Chrome integration        | `claude --no-chrome`                                 |
| `--ide`               | Auto-connect to IDE on startup    | `claude --ide`                                       |

### Agent Flags

| Flag       | Description               | Example                          |
| ---------- | ------------------------- | -------------------------------- |
| `--agent`  | Specify agent for session | `claude --agent my-custom-agent` |
| `--agents` | Define subagents via JSON | See format below                 |

**Agents JSON Format**:

```json
{
  "code-reviewer": {
    "description": "Expert code reviewer. Use proactively after code changes.",
    "prompt": "You are a senior code reviewer. Focus on quality, security, best practices.",
    "tools": ["Read", "Grep", "Glob", "Bash"],
    "model": "sonnet"
  },
  "debugger": {
    "description": "Debugging specialist for errors and test failures.",
    "prompt": "You are an expert debugger. Analyze errors, identify root causes."
  }
}
```

**Agent Definition Fields**:

| Field         | Required | Description                            |
| ------------- | -------- | -------------------------------------- |
| `description` | Yes      | When to invoke the subagent            |
| `prompt`      | Yes      | System prompt for behavior             |
| `tools`       | No       | Array of allowed tools                 |
| `model`       | No       | Model alias: sonnet/opus/haiku/inherit |

### Budget and Limit Flags

| Flag               | Description                      | Example                                   |
| ------------------ | -------------------------------- | ----------------------------------------- |
| `--max-budget-usd` | Maximum API spend (print mode)   | `claude -p --max-budget-usd 5.00 "query"` |
| `--max-turns`      | Limit agentic turns (print mode) | `claude -p --max-turns 3 "query"`         |

### Settings Flags

| Flag                | Description                          | Example                                 |
| ------------------- | ------------------------------------ | --------------------------------------- |
| `--settings`        | Load settings from JSON file         | `claude --settings ./settings.json`     |
| `--setting-sources` | Setting sources to load              | `claude --setting-sources user,project` |
| `--betas`           | Beta headers for API (API key users) | `claude --betas interleaved-thinking`   |

### Remote Session Flags

| Flag         | Description                     | Example                               |
| ------------ | ------------------------------- | ------------------------------------- |
| `--remote`   | Create web session on claude.ai | `claude --remote "Fix the login bug"` |
| `--teleport` | Resume web session in terminal  | `claude --teleport`                   |

### Initialization Flags

| Flag            | Description                              | Example                |
| --------------- | ---------------------------------------- | ---------------------- |
| `--init`        | Run Setup hooks, start interactive       | `claude --init`        |
| `--init-only`   | Run Setup hooks, exit                    | `claude --init-only`   |
| `--maintenance` | Run Setup hooks with maintenance trigger | `claude --maintenance` |

### Version and Help

| Flag              | Description           |
| ----------------- | --------------------- |
| `--version`, `-v` | Output version number |
| `--help`          | Show help             |

### Checkpointing Overview

Claude Code automatically tracks file edits, allowing quick undo and rewind to previous states.

**Automatic Tracking**:

- Every user prompt creates a new checkpoint
- Checkpoints persist across sessions (accessible in resumed conversations)
- Auto-cleanup after 30 days (configurable)

**Accessing Checkpoints**:

| Method    | Description                            |
| --------- | -------------------------------------- |
| `Esc Esc` | Press Escape twice to open rewind menu |
| `/rewind` | Command to open rewind menu            |

### Rewind Restore Options

| Option                | Description                            | Use Case                              |
| --------------------- | -------------------------------------- | ------------------------------------- |
| **Conversation only** | Rewind to user message, keep code      | Try different approach with same code |
| **Code only**         | Revert file changes, keep conversation | Keep context, revert implementation   |
| **Both**              | Restore both to prior point            | Complete rollback                     |

### Checkpoint Use Cases

| Scenario                 | Description                                          |
| ------------------------ | ---------------------------------------------------- |
| Exploring alternatives   | Try different implementations without losing start   |
| Recovering from mistakes | Quick undo for bugs or broken functionality          |
| Iterating on features    | Experiment with variations, revert to working states |

### Checkpoint Limitations

**Bash Commands NOT Tracked**:

```bash
# These changes are PERMANENT, cannot undo:
rm file.txt
mv old.txt new.txt
cp source.txt dest.txt
```

Only direct file edits through Claude's editing tools are tracked.

**External Changes NOT Tracked**:

- Manual edits outside Claude Code
- Edits from concurrent sessions
- Exception: Changes to same files as current session

**Not a Git Replacement**:

| Checkpoints                 | Git                     |
| --------------------------- | ----------------------- |
| Session-level recovery      | Permanent history       |
| Quick "local undo"          | Commits, branches       |
| Complements version control | Long-term collaboration |

### DSM CLI Patterns

**Quick Test Run**:

```bash
claude -p "run the unit tests for fetchers and report any failures"
```

**FCP Debugging Session**:

```bash
claude --resume fcp-debug "continue investigating the cache miss issue"
```

**Code Review with Custom Agent**:

```bash
claude --agent api-reviewer "review the new OKX fetcher"
```

**Headless Data Validation**:

```bash
claude -p --output-format json "validate OHLCV data for BTCUSDT" > validation-report.json
```

**Budget-Limited Analysis**:

```bash
claude -p --max-budget-usd 2.00 "analyze the FCP decision logic"
```

**Plan Mode Research**:

```bash
claude --permission-mode plan "analyze how to add WebSocket support"
```

**Restricted Tools for Safety**:

```bash
claude --tools "Read,Grep,Glob" "explore the cache module"
```

**Custom System Prompt**:

```bash
claude --append-system-prompt "Focus on FCP patterns and cache invalidation"
```

### Print Mode Output Formats

**Text Format** (default):

```bash
claude -p "summarize this code" --output-format text
```

Returns plain text response.

**JSON Format**:

```bash
claude -p "analyze this" --output-format json > analysis.json
```

Returns JSON array with metadata (cost, duration, messages).

**Stream JSON Format**:

```bash
claude -p "process this" --output-format stream-json
```

Real-time JSON objects as processing occurs. Each line is valid JSON.

### Scripting and Automation Patterns

**CI/CD Integration**:

```bash
# Run code review in CI
claude -p --max-turns 5 --output-format json "review changes for bugs" > review.json
```

**Batch Processing**:

```bash
# Process multiple files
for file in src/fetchers/*.py; do
  claude -p "check $file for FCP compliance" >> compliance.txt
done
```

**Piping Data**:

```bash
# Analyze logs
cat fcp-debug.log | claude -p "identify FCP decision anomalies"

# Review git diff
git diff main | claude -p "review these changes"
```

**Structured Output**:

```bash
# Get JSON schema-validated output
claude -p --json-schema '{"type":"object","properties":{"issues":{"type":"array"}}}' \
  "analyze code and list issues"
```

### Session Management Patterns

**Continue Last Session**:

```bash
claude -c
```

**Resume Named Session**:

```bash
claude -r "fcp-refactor"
```

**Fork Session for Experimentation**:

```bash
claude --resume main-feature --fork-session
```

Creates new session ID, preserving original.

**Headless Continuation**:

```bash
claude -c -p "what was our last decision?"
```

### Environment Variable Alternatives

Many CLI flags have environment variable equivalents:

| Flag                | Environment Variable     |
| ------------------- | ------------------------ |
| `--model`           | `CLAUDE_MODEL`           |
| `--max-budget-usd`  | `MAX_BUDGET_USD`         |
| `--max-turns`       | `MAX_TURNS`              |
| `--permission-mode` | `CLAUDE_PERMISSION_MODE` |

See Settings documentation for full list.
<!-- SSoT-OK: This section is authoritative for context window and cost management -->

## Context Window and Cost Management Reference

Comprehensive guide to tracking costs, managing context, and optimizing token usage.

### Cost Overview

**Typical Costs**:

| Metric                        | Value               |
| ----------------------------- | ------------------- |
| Average per developer per day | $6                  |
| 90th percentile daily         | < $12               |
| Monthly average (Sonnet 4.5)  | ~$100-200/developer |

**Cost Variables**:

- Codebase size
- Query complexity
- Conversation length
- Number of concurrent instances
- Automation usage

### Tracking Costs

**`/cost` Command** (API users):

```
Total cost:            $0.55
Total duration (API):  6m 19.7s
Total duration (wall): 6h 33m 10.2s
Total code changes:    0 lines added, 0 lines removed
```

**Note**: Claude Max/Pro subscribers have usage in subscription. Use `/stats` for usage patterns.

**Status Line Display**:

Configure `/statusline` to show continuous token usage.

### Context Window Basics

**Total Capacity**: 200,000 tokens

**Token Costs Scale**: More context = more tokens used per message

**Automatic Optimizations**:

| Feature         | Description                                         |
| --------------- | --------------------------------------------------- |
| Prompt caching  | Reduces costs for repeated content (system prompts) |
| Auto-compaction | Summarizes history when approaching limits          |

### Auto-Compaction

When conversation approaches context limit:

1. Analyzes conversation for key information
2. Creates concise summary of interactions, decisions, code changes
3. Replaces old messages with summary
4. Continues seamlessly with preserved context

**Compaction vs Clear**:

| `/compact`                         | `/clear`                    |
| ---------------------------------- | --------------------------- |
| Summarizes and preserves key info  | Completely wipes history    |
| Preloads summary as new context    | Fresh start from scratch    |
| Use when retaining context matters | Use for completely new work |

**Custom Compaction Instructions**:

```
/compact Focus on code samples and API usage
```

**CLAUDE.md Compaction Config**:

```markdown
# Compact instructions

When you are using compact, please focus on test output and code changes
```

### Auto-Compact Threshold

Claude Code stops earlier to preserve working memory:

- Old behavior: Run until failure
- New behavior: Stop with ~25% unused capacity

**Why**: Higher reasoning quality per turn enables longer effective sessions.

### Context Management Commands

| Command    | Purpose                            |
| ---------- | ---------------------------------- |
| `/cost`    | Check current token usage          |
| `/context` | Visualize context usage as grid    |
| `/clear`   | Start fresh (wipe history)         |
| `/compact` | Summarize while preserving context |
| `/rename`  | Name session before clearing       |
| `/resume`  | Return to named session            |

### Reducing Token Usage

**Manage Context Proactively**:

| Strategy            | Description                            |
| ------------------- | -------------------------------------- |
| Clear between tasks | `/clear` when switching unrelated work |
| Custom compaction   | Specify what to preserve               |
| Use `/context`      | Identify what's consuming space        |

**Choose Right Model**:

| Model  | Use Case                                   | Cost   |
| ------ | ------------------------------------------ | ------ |
| Sonnet | Most coding tasks                          | Lower  |
| Opus   | Complex architecture, multi-step reasoning | Higher |
| Haiku  | Simple subagent tasks                      | Lowest |

Switch with `/model` or set default in `/config`.

### MCP Server Overhead

Each MCP server adds tool definitions to context, even when idle.

**Reduce Overhead**:

| Strategy               | Benefit                              |
| ---------------------- | ------------------------------------ |
| Prefer CLI tools       | `gh`, `aws`, `gcloud` more efficient |
| Disable unused servers | `/mcp` to see and manage             |
| Tool search            | Automatic when tools > 10% context   |

**Tool Search Configuration**:

```bash
# Lower threshold for earlier tool search activation
ENABLE_TOOL_SEARCH=auto:5  # Triggers at 5% of context
```

**Tool Search Benefits**:

- 46.9% reduction in total agent tokens
- 85% reduction in tool definition overhead
- Accuracy improvement: Opus 4.5 79.5% → 88.1%

### Code Intelligence Plugins

Install language server plugins for:

- Precise symbol navigation vs text search
- Fewer unnecessary file reads
- Automatic type error reporting

**Savings**: Single "go to definition" replaces grep + reading multiple files.

### Hooks for Preprocessing

Custom hooks preprocess data before Claude sees it.

**Example**: Filter test output to failures only:

```bash
#!/bin/bash
input=$(cat)
cmd=$(echo "$input" | jq -r '.tool_input.command')

if [[ "$cmd" =~ ^(npm test|pytest|go test) ]]; then
  filtered_cmd="$cmd 2>&1 | grep -A 5 -E '(FAIL|ERROR|error:)' | head -100"
  echo '{"hookSpecificOutput":{"hookEventName":"PreToolUse","permissionDecision":"allow","updatedInput":{"command":"'"$filtered_cmd"'"}}}'
else
  echo "{}"
fi
```

**Savings**: 10,000-line log → hundreds of tokens

### Skills for Domain Knowledge

Move specialized instructions from CLAUDE.md to skills:

| In CLAUDE.md          | In Skills                 |
| --------------------- | ------------------------- |
| Always loaded         | On-demand only            |
| Every session         | When invoked              |
| Consumes base context | Zero overhead when unused |

**Guideline**: Keep CLAUDE.md under ~500 lines with essentials only.

### Extended Thinking Settings

**Default**: 31,999 token budget (enabled for complex reasoning)

**Thinking Tokens**: Billed as output tokens (expensive)

**Cost Reduction**:

| Method        | How                        |
| ------------- | -------------------------- |
| Disable       | `/config` toggle           |
| Reduce budget | `MAX_THINKING_TOKENS=8000` |

**When to Reduce**: Simple tasks not needing deep reasoning.

### Subagent Delegation

Delegate verbose operations to subagents:

| Operation       | Benefit                                  |
| --------------- | ---------------------------------------- |
| Running tests   | Verbose output stays in subagent context |
| Fetching docs   | Only summary returns to main             |
| Processing logs | Isolate high-volume content              |

Configure in subagent definition:

```yaml
model: haiku # Cheaper for simple tasks
```

### Writing Efficient Prompts

| Bad (Vague)             | Good (Specific)                            |
| ----------------------- | ------------------------------------------ |
| "improve this codebase" | "add input validation to login in auth.ts" |
| Triggers broad scanning | Minimal file reads                         |

### Complex Task Strategies

| Strategy             | Description                                      |
| -------------------- | ------------------------------------------------ |
| Use Plan Mode        | `Shift+Tab` before implementation                |
| Course-correct early | `Escape` to stop, `/rewind` to restore           |
| Verification targets | Include test cases, screenshots, expected output |
| Test incrementally   | Write one file, test, continue                   |

### Background Token Usage

Small usage even when idle:

| Process                    | Purpose                |
| -------------------------- | ---------------------- |
| Conversation summarization | For `--resume` feature |
| Command processing         | `/cost` status checks  |

**Typical**: Under $0.04 per session

### Team Cost Management

**Workspace Spend Limits**:

Set via Console for total Claude Code workspace spend.

**Rate Limit Recommendations (TPM/RPM per user)**:

| Team Size     | TPM       | RPM       |
| ------------- | --------- | --------- |
| 1-5 users     | 200k-300k | 5-7       |
| 5-20 users    | 100k-150k | 2.5-3.5   |
| 20-50 users   | 50k-75k   | 1.25-1.75 |
| 50-100 users  | 25k-35k   | 0.62-0.87 |
| 100-500 users | 15k-20k   | 0.37-0.47 |
| 500+ users    | 10k-15k   | 0.25-0.35 |

**Example**: 200 users × 20k TPM = 4 million total TPM

**Note**: Rate limits apply at organization level, not per individual.

### Enterprise Cost Tracking

**Bedrock/Vertex/Foundry**: No metrics from cloud.

**Solution**: LiteLLM for tracking spend by key.

### DSM Cost Optimization Patterns

**Session Management**:

```
# Name session before clearing
> /rename fcp-debug

# Clear for new task
> /clear

# Resume later
> /resume fcp-debug
```

**Model Selection by Task**:

| Task                   | Model            |
| ---------------------- | ---------------- |
| FCP debugging          | sonnet           |
| Architecture decisions | opus             |
| Simple data validation | haiku (subagent) |

**Custom Compaction for DSM**:

```
/compact Focus on FCP decision logic, cache invalidation patterns, and symbol format handling
```

**CLAUDE.md Optimization**:

Keep DSM-specific guidance in skills:

```
docs/skills/
├── dsm-usage/     # API usage (invoke when needed)
├── dsm-testing/   # Test patterns (invoke when testing)
└── dsm-research/  # Codebase exploration (invoke when researching)
```

**Context Check Before Heavy Operations**:

```
> /context
# Check if MCP servers consuming space
> /mcp
# Disable unused servers
```

**Budget-Limited Headless**:

```bash
# Limit spend on automated runs
claude -p --max-budget-usd 2.00 "validate data pipeline"
```

### Cost Monitoring Best Practices

| Practice            | Frequency               |
| ------------------- | ----------------------- |
| Check `/cost`       | After complex tasks     |
| Review `/context`   | Before heavy operations |
| Clear stale context | Between unrelated tasks |
| Use `/stats`        | Weekly usage review     |

### Token Reduction Summary

| Strategy                  | Impact                     |
| ------------------------- | -------------------------- |
| Clear between tasks       | High                       |
| Tool search (MCP)         | 46.9% reduction            |
| Code intelligence plugins | Moderate                   |
| Model selection           | Variable                   |
| Extended thinking budget  | High for simple tasks      |
| Skills over CLAUDE.md     | Moderate                   |
| Subagent delegation       | High for verbose ops       |
| Specific prompts          | Moderate                   |
| Plan Mode                 | Prevents expensive re-work |
<!-- SSoT-OK: This section is authoritative for status line configuration -->

## Status Line Configuration Reference

Create custom status lines that display contextual information at the bottom of Claude Code.

### Quick Setup

**Option 1: Interactive Setup**:

```
/statusline
```

Or with specific requirements:

```
/statusline show the model name in orange
```

**Option 2: Direct Configuration**:

Add to `~/.claude/settings.json`:

```json
{
  "statusLine": {
    "type": "command",
    "command": "~/.claude/statusline.sh",
    "padding": 0
  }
}
```

### How Status Line Works

| Behavior       | Description                              |
| -------------- | ---------------------------------------- |
| Update trigger | Conversation messages update             |
| Update rate    | At most every 300ms                      |
| Output         | First line of stdout becomes status line |
| Styling        | ANSI color codes supported               |
| Input          | JSON context data via stdin              |

### JSON Input Structure

<!-- SSoT-OK: Example JSON showing structure, version is illustrative -->

Your script receives this JSON via stdin:

```json
{
  "hook_event_name": "Status",
  "session_id": "abc123...",
  "transcript_path": "/path/to/transcript.json",
  "cwd": "/current/working/directory",
  "model": {
    "id": "claude-opus-4-1",
    "display_name": "Opus"
  },
  "workspace": {
    "current_dir": "/current/working/directory",
    "project_dir": "/original/project/directory"
  },
  "version": "<version>",
  "output_style": {
    "name": "default"
  },
  "cost": {
    "total_cost_usd": 0.01234,
    "total_duration_ms": 45000,
    "total_api_duration_ms": 2300,
    "total_lines_added": 156,
    "total_lines_removed": 23
  },
  "context_window": {
    "total_input_tokens": 15234,
    "total_output_tokens": 4521,
    "context_window_size": 200000,
    "used_percentage": 42.5,
    "remaining_percentage": 57.5,
    "current_usage": {
      "input_tokens": 8500,
      "output_tokens": 1200,
      "cache_creation_input_tokens": 5000,
      "cache_read_input_tokens": 2000
    }
  }
}
```

### Available Data Fields

**Model Information**:

| Field        | Path                  | Example           |
| ------------ | --------------------- | ----------------- |
| Model ID     | `.model.id`           | `claude-opus-4-1` |
| Display name | `.model.display_name` | `Opus`            |

**Workspace Information**:

| Field             | Path                     | Description               |
| ----------------- | ------------------------ | ------------------------- |
| Current directory | `.workspace.current_dir` | Working directory         |
| Project directory | `.workspace.project_dir` | Original project root     |
| CWD               | `.cwd`                   | Current working directory |

**Session Information**:

| Field           | Path               | Description               |
| --------------- | ------------------ | ------------------------- |
| Session ID      | `.session_id`      | Unique session identifier |
| Transcript path | `.transcript_path` | Path to conversation file |
| Version         | `.version`         | Claude Code version       |

**Cost Information**:

| Field          | Path                          | Description        |
| -------------- | ----------------------------- | ------------------ |
| Total cost     | `.cost.total_cost_usd`        | USD spent          |
| Total duration | `.cost.total_duration_ms`     | Wall time (ms)     |
| API duration   | `.cost.total_api_duration_ms` | API call time (ms) |
| Lines added    | `.cost.total_lines_added`     | Code lines added   |
| Lines removed  | `.cost.total_lines_removed`   | Code lines removed |

**Context Window Information**:

| Field         | Path                                   | Description              |
| ------------- | -------------------------------------- | ------------------------ |
| Input tokens  | `.context_window.total_input_tokens`   | Total input tokens       |
| Output tokens | `.context_window.total_output_tokens`  | Total output tokens      |
| Window size   | `.context_window.context_window_size`  | Max context (200000)     |
| Used %        | `.context_window.used_percentage`      | Pre-calculated usage     |
| Remaining %   | `.context_window.remaining_percentage` | Pre-calculated remaining |

**Current Usage** (from last API call, may be null):

| Field          | Path                                                        |
| -------------- | ----------------------------------------------------------- |
| Input tokens   | `.context_window.current_usage.input_tokens`                |
| Output tokens  | `.context_window.current_usage.output_tokens`               |
| Cache creation | `.context_window.current_usage.cache_creation_input_tokens` |
| Cache read     | `.context_window.current_usage.cache_read_input_tokens`     |

### Simple Status Line Script

```bash
#!/bin/bash
input=$(cat)

MODEL_DISPLAY=$(echo "$input" | jq -r '.model.display_name')
CURRENT_DIR=$(echo "$input" | jq -r '.workspace.current_dir')

echo "[$MODEL_DISPLAY] 📁 ${CURRENT_DIR##*/}"
```

### Git-Aware Status Line

```bash
#!/bin/bash
input=$(cat)

MODEL_DISPLAY=$(echo "$input" | jq -r '.model.display_name')
CURRENT_DIR=$(echo "$input" | jq -r '.workspace.current_dir')

GIT_BRANCH=""
if git rev-parse --git-dir > /dev/null 2>&1; then
    BRANCH=$(git branch --show-current 2>/dev/null)
    if [ -n "$BRANCH" ]; then
        GIT_BRANCH=" | 🌿 $BRANCH"
    fi
fi

echo "[$MODEL_DISPLAY] 📁 ${CURRENT_DIR##*/}$GIT_BRANCH"
```

### Context Window Display

**Simple (pre-calculated)**:

```bash
#!/bin/bash
input=$(cat)

MODEL=$(echo "$input" | jq -r '.model.display_name')
PERCENT_USED=$(echo "$input" | jq -r '.context_window.used_percentage // 0')

echo "[$MODEL] Context: ${PERCENT_USED}%"
```

**Advanced (manual calculation)**:

```bash
#!/bin/bash
input=$(cat)

MODEL=$(echo "$input" | jq -r '.model.display_name')
CONTEXT_SIZE=$(echo "$input" | jq -r '.context_window.context_window_size')
USAGE=$(echo "$input" | jq '.context_window.current_usage')

if [ "$USAGE" != "null" ]; then
    CURRENT_TOKENS=$(echo "$USAGE" | jq '.input_tokens + .cache_creation_input_tokens + .cache_read_input_tokens')
    PERCENT_USED=$((CURRENT_TOKENS * 100 / CONTEXT_SIZE))
    echo "[$MODEL] Context: ${PERCENT_USED}%"
else
    echo "[$MODEL] Context: 0%"
fi
```

### Helper Functions Pattern

```bash
#!/bin/bash
input=$(cat)

# Helper functions
get_model_name() { echo "$input" | jq -r '.model.display_name'; }
get_current_dir() { echo "$input" | jq -r '.workspace.current_dir'; }
get_project_dir() { echo "$input" | jq -r '.workspace.project_dir'; }
get_version() { echo "$input" | jq -r '.version'; }
get_cost() { echo "$input" | jq -r '.cost.total_cost_usd'; }
get_duration() { echo "$input" | jq -r '.cost.total_duration_ms'; }
get_lines_added() { echo "$input" | jq -r '.cost.total_lines_added'; }
get_lines_removed() { echo "$input" | jq -r '.cost.total_lines_removed'; }
get_input_tokens() { echo "$input" | jq -r '.context_window.total_input_tokens'; }
get_output_tokens() { echo "$input" | jq -r '.context_window.total_output_tokens'; }
get_context_window_size() { echo "$input" | jq -r '.context_window.context_window_size'; }

# Use helpers
MODEL=$(get_model_name)
DIR=$(get_current_dir)
echo "[$MODEL] 📁 ${DIR##*/}"
```

### Python Status Line

```python
#!/usr/bin/env python3
import json
import sys
import os

data = json.load(sys.stdin)

model = data['model']['display_name']
current_dir = os.path.basename(data['workspace']['current_dir'])

git_branch = ""
if os.path.exists('.git'):
    try:
        with open('.git/HEAD', 'r') as f:
            ref = f.read().strip()
            if ref.startswith('ref: refs/heads/'):
                git_branch = f" | 🌿 {ref.replace('ref: refs/heads/', '')}"
    except:
        pass

print(f"[{model}] 📁 {current_dir}{git_branch}")
```

### Node.js Status Line

```javascript
#!/usr/bin/env node
const fs = require("fs");
const path = require("path");

let input = "";
process.stdin.on("data", (chunk) => (input += chunk));
process.stdin.on("end", () => {
  const data = JSON.parse(input);

  const model = data.model.display_name;
  const currentDir = path.basename(data.workspace.current_dir);

  let gitBranch = "";
  try {
    const head = fs.readFileSync(".git/HEAD", "utf8").trim();
    if (head.startsWith("ref: refs/heads/")) {
      gitBranch = ` | 🌿 ${head.replace("ref: refs/heads/", "")}`;
    }
  } catch (e) {}

  console.log(`[${model}] 📁 ${currentDir}${gitBranch}`);
});
```

### ANSI Color Codes

| Color   | Code       | Reset     |
| ------- | ---------- | --------- |
| Red     | `\033[31m` | `\033[0m` |
| Green   | `\033[32m` | `\033[0m` |
| Yellow  | `\033[33m` | `\033[0m` |
| Blue    | `\033[34m` | `\033[0m` |
| Magenta | `\033[35m` | `\033[0m` |
| Cyan    | `\033[36m` | `\033[0m` |
| Bold    | `\033[1m`  | `\033[0m` |

**Example with colors**:

```bash
#!/bin/bash
input=$(cat)
MODEL=$(echo "$input" | jq -r '.model.display_name')
COST=$(echo "$input" | jq -r '.cost.total_cost_usd')

# Color based on cost
if (( $(echo "$COST > 1" | bc -l) )); then
    COLOR="\033[31m"  # Red if > $1
elif (( $(echo "$COST > 0.5" | bc -l) )); then
    COLOR="\033[33m"  # Yellow if > $0.50
else
    COLOR="\033[32m"  # Green otherwise
fi

echo -e "[$MODEL] ${COLOR}\$${COST}\033[0m"
```

### Third-Party Tools

**ccstatusline** (sirmalloc):

- Powerline support
- Themes
- YAML configuration

**ccstatusline** (syou6162):

- YAML configuration
- Template syntax
- Command execution

**ccusage**:

- Usage analysis
- Status line integration

### Testing Status Line

```bash
# Test with mock JSON input
echo '{"model":{"display_name":"Test"},"workspace":{"current_dir":"/test"}}' | ./statusline.sh
```

### Troubleshooting

| Issue                      | Solution                                    |
| -------------------------- | ------------------------------------------- |
| Status line doesn't appear | Check script is executable (`chmod +x`)     |
| No output                  | Ensure script outputs to stdout, not stderr |
| JSON parsing fails         | Verify `jq` is installed                    |
| Colors not working         | Check terminal ANSI support                 |

### DSM Status Line Example

```bash
#!/bin/bash
input=$(cat)

# Extract data
MODEL=$(echo "$input" | jq -r '.model.display_name')
COST=$(echo "$input" | jq -r '.cost.total_cost_usd')
PERCENT=$(echo "$input" | jq -r '.context_window.used_percentage // 0')
DIR=$(echo "$input" | jq -r '.workspace.current_dir')

# Git branch
BRANCH=""
if git rev-parse --git-dir > /dev/null 2>&1; then
    B=$(git branch --show-current 2>/dev/null)
    [ -n "$B" ] && BRANCH=" 🌿 $B"
fi

# Format cost
COST_FMT=$(printf "%.2f" "$COST")

# Color context based on usage
if (( $(echo "$PERCENT > 80" | bc -l) )); then
    CTX_COLOR="\033[31m"  # Red
elif (( $(echo "$PERCENT > 60" | bc -l) )); then
    CTX_COLOR="\033[33m"  # Yellow
else
    CTX_COLOR="\033[32m"  # Green
fi

echo -e "[$MODEL] 📁 ${DIR##*/}$BRANCH | \$${COST_FMT} | ${CTX_COLOR}${PERCENT}%\033[0m"
```

**Output**: `[Sonnet] 📁 data-source-manager 🌿 main | $0.55 | 42%`

### Best Practices

| Practice            | Description                |
| ------------------- | -------------------------- |
| Keep concise        | Fit on one line            |
| Use emojis          | Make info scannable        |
| Use colors          | Highlight important data   |
| Cache expensive ops | Git status, etc.           |
| Test manually       | Mock JSON before deploying |
<!-- SSoT-OK: This section is authoritative for plugin marketplace and discovery -->

## Plugin Marketplace Reference

Discover, install, and manage plugins to extend Claude Code capabilities.

### What Plugins Provide

| Component   | Description                          |
| ----------- | ------------------------------------ |
| Skills      | Slash commands for workflows         |
| Agents      | Specialized subagents for tasks      |
| Hooks       | Behavior customization at key points |
| MCP Servers | External service connections         |

### How Marketplaces Work

**Two-Step Process**:

1. **Add the marketplace** - Register catalog for browsing
2. **Install individual plugins** - Choose what to install

Think of it like adding an app store, then downloading apps individually.

### Plugin Commands

| Command                      | Purpose                      |
| ---------------------------- | ---------------------------- |
| `/plugin`                    | Open plugin manager UI       |
| `/plugin marketplace add`    | Add a marketplace            |
| `/plugin marketplace list`   | List configured marketplaces |
| `/plugin marketplace update` | Refresh marketplace listings |
| `/plugin marketplace remove` | Remove a marketplace         |
| `/plugin install`            | Install a plugin             |
| `/plugin uninstall`          | Remove a plugin              |
| `/plugin enable`             | Enable a disabled plugin     |
| `/plugin disable`            | Disable without uninstalling |

**Shortcuts**: `/plugin market` = `/plugin marketplace`, `rm` = `remove`

### Plugin Manager Interface

Run `/plugin` to open tabbed interface:

| Tab              | Purpose                        |
| ---------------- | ------------------------------ |
| **Discover**     | Browse available plugins       |
| **Installed**    | View/manage installed plugins  |
| **Marketplaces** | Add/remove/update marketplaces |
| **Errors**       | View plugin loading errors     |

Navigate: `Tab` (forward), `Shift+Tab` (backward)

### Official Anthropic Marketplace

Automatically available at startup. Browse with:

```
/plugin
# Go to Discover tab
```

Install directly:

```
/plugin install plugin-name@claude-plugins-official
```

### Code Intelligence Plugins

Enable Claude's built-in LSP tool for code navigation.

| Language   | Plugin              | Binary Required              |
| ---------- | ------------------- | ---------------------------- |
| C/C++      | `clangd-lsp`        | `clangd`                     |
| C#         | `csharp-lsp`        | `csharp-ls`                  |
| Go         | `gopls-lsp`         | `gopls`                      |
| Java       | `jdtls-lsp`         | `jdtls`                      |
| Kotlin     | `kotlin-lsp`        | `kotlin-language-server`     |
| Lua        | `lua-lsp`           | `lua-language-server`        |
| PHP        | `php-lsp`           | `intelephense`               |
| Python     | `pyright-lsp`       | `pyright-langserver`         |
| Rust       | `rust-analyzer-lsp` | `rust-analyzer`              |
| Swift      | `swift-lsp`         | `sourcekit-lsp`              |
| TypeScript | `typescript-lsp`    | `typescript-language-server` |

**What Claude Gains**:

| Capability            | Description                                     |
| --------------------- | ----------------------------------------------- |
| Automatic diagnostics | Type errors, missing imports after edits        |
| Code navigation       | Jump to definition, find references, hover info |

**View diagnostics**: Press `Ctrl+O` when "diagnostics found" appears.

### External Integration Plugins

Pre-configured MCP servers for external services:

| Category           | Plugins                                  |
| ------------------ | ---------------------------------------- |
| Source control     | `github`, `gitlab`                       |
| Project management | `atlassian`, `asana`, `linear`, `notion` |
| Design             | `figma`                                  |
| Infrastructure     | `vercel`, `firebase`, `supabase`         |
| Communication      | `slack`                                  |
| Monitoring         | `sentry`                                 |

### Development Workflow Plugins

| Plugin              | Purpose                        |
| ------------------- | ------------------------------ |
| `commit-commands`   | Git commit, push, PR workflows |
| `pr-review-toolkit` | PR review agents               |
| `agent-sdk-dev`     | Claude Agent SDK tools         |
| `plugin-dev`        | Plugin creation toolkit        |

### Output Style Plugins

| Plugin                     | Purpose                             |
| -------------------------- | ----------------------------------- |
| `explanatory-output-style` | Educational implementation insights |
| `learning-output-style`    | Interactive learning mode           |

### Adding Marketplaces

**From GitHub** (owner/repo format):

```
/plugin marketplace add anthropics/claude-code
```

**From Other Git Hosts**:

<!-- SSoT-OK: Git ref examples showing syntax, not actual versions -->

```
# HTTPS
/plugin marketplace add https://gitlab.com/company/plugins.git

# SSH
/plugin marketplace add git@gitlab.com:company/plugins.git

# Specific branch/tag (append #ref)
/plugin marketplace add https://gitlab.com/company/plugins.git#main
```

**From Local Paths**:

```
/plugin marketplace add ./my-marketplace
/plugin marketplace add ./path/to/marketplace.json
```

**From Remote URLs**:

```
/plugin marketplace add https://example.com/marketplace.json
```

### Installation Scopes

| Scope       | Description            | Location                  |
| ----------- | ---------------------- | ------------------------- |
| **User**    | All projects, just you | `~/.claude/settings.json` |
| **Project** | All collaborators      | `.claude/settings.json`   |
| **Local**   | This repo, just you    | Not shared                |
| **Managed** | Admin-installed        | Cannot modify             |

**Install to scope**:

```
/plugin install plugin@marketplace --scope project
```

**Interactive install**: `/plugin` → Discover → Select plugin → Choose scope

### Install Plugins

**Direct install** (user scope default):

```
/plugin install plugin-name@marketplace-name
```

**Example**:

```
/plugin install commit-commands@anthropics-claude-code
```

**Using installed plugin**:

```
/commit-commands:commit
```

Plugin commands are namespaced: `plugin-name:command`

### Managing Plugins

**Disable without uninstalling**:

```
/plugin disable plugin-name@marketplace-name
```

**Re-enable**:

```
/plugin enable plugin-name@marketplace-name
```

**Uninstall**:

```
/plugin uninstall plugin-name@marketplace-name
```

### Auto-Updates

**Toggle per marketplace**:

1. `/plugin` → Marketplaces tab
2. Select marketplace
3. Enable/Disable auto-update

**Defaults**:

| Marketplace Type   | Auto-Update |
| ------------------ | ----------- |
| Official Anthropic | Enabled     |
| Third-party        | Disabled    |
| Local development  | Disabled    |

**Disable all auto-updates**:

```bash
export DISABLE_AUTOUPDATER=true
```

**Keep plugin auto-updates only**:

```bash
export DISABLE_AUTOUPDATER=true
export FORCE_AUTOUPDATE_PLUGINS=true
```

### Team Marketplace Configuration

Add to `.claude/settings.json` for team-wide availability:

```json
{
  "extraKnownMarketplaces": [
    {
      "type": "git",
      "source": "https://github.com/your-org/plugins.git"
    }
  ],
  "enabledPlugins": ["code-standards@your-org"]
}
```

Team members see prompts to install when trusting the folder.

### Troubleshooting

**/plugin command not recognized**:

1. Check version: `claude --version` (requires recent version with plugin support)
2. Update: `brew upgrade claude-code` or `npm update -g @anthropic-ai/claude-code`
3. Restart Claude Code

**Common Issues**:

| Issue                   | Solution                                                |
| ----------------------- | ------------------------------------------------------- |
| Marketplace not loading | Verify URL and `.claude-plugin/marketplace.json` exists |
| Installation failures   | Check source URLs accessible, repos public              |
| Files not found         | Plugins copied to cache; external paths don't work      |
| Skills not appearing    | `rm -rf ~/.claude/plugins/cache`, restart, reinstall    |

**Code Intelligence Issues**:

| Issue                        | Solution                                    |
| ---------------------------- | ------------------------------------------- |
| Language server not starting | Verify binary in `$PATH`, check Errors tab  |
| High memory usage            | Disable plugin, use built-in search instead |
| False positives in monorepos | Configure workspace correctly               |

### DSM Plugin Patterns

**Add cc-skills marketplace**:

```
/plugin marketplace add terrylica/cc-skills
```

**Install DSM-relevant plugins**:

```
# Code intelligence for Python
/plugin install pyright-lsp@claude-plugins-official

# Git workflows
/plugin install commit-commands@claude-plugins-official

# GitHub integration
/plugin install github@claude-plugins-official
```

**Project-scope for team**:

Add to `.claude/settings.json`:

```json
{
  "extraKnownMarketplaces": [
    {
      "type": "git",
      "source": "https://github.com/terrylica/cc-skills.git"
    }
  ],
  "enabledPlugins": ["pyright-lsp@claude-plugins-official"]
}
```

### Plugin Best Practices

| Practice              | Description                                 |
| --------------------- | ------------------------------------------- |
| Trust before install  | Review plugin source and purpose            |
| Use project scope     | Share with team via `.claude/settings.json` |
| Disable unused        | Reduce memory and context overhead          |
| Check Errors tab      | Debug loading issues                        |
| Clear cache on issues | `rm -rf ~/.claude/plugins/cache`            |

### Plugin Types Summary

| Type                 | Purpose             | Example                    |
| -------------------- | ------------------- | -------------------------- |
| Code Intelligence    | LSP integration     | `pyright-lsp`              |
| External Integration | Service connections | `github`, `slack`          |
| Development Workflow | Common tasks        | `commit-commands`          |
| Output Style         | Response format     | `explanatory-output-style` |
| Custom               | Your organization   | Team-specific plugins      |
<!-- SSoT-OK: This section is authoritative for Chrome browser integration -->

## Chrome Browser Integration Reference

Connect Claude Code to Chrome for browser automation, testing, and debugging.

### What Chrome Integration Enables

| Capability          | Description                                    |
| ------------------- | ---------------------------------------------- |
| Live debugging      | Read console errors, DOM state, fix code       |
| Design verification | Build UI, verify matches mockup in browser     |
| Web app testing     | Form validation, visual regression, user flows |
| Authenticated apps  | Access Google Docs, Gmail, Notion (logged in)  |
| Data extraction     | Pull structured info from web pages            |
| Task automation     | Data entry, form filling, multi-site workflows |
| Session recording   | Record interactions as GIFs                    |

### Prerequisites

| Requirement                | Details                                    |
| -------------------------- | ------------------------------------------ |
| Google Chrome              | Not Brave, Arc, or other Chromium browsers |
| Claude in Chrome extension | Latest version from Chrome Web Store       |
| Claude Code CLI            | Recent version with Chrome support         |
| Paid Claude plan           | Pro, Team, or Enterprise                   |

**Note**: WSL (Windows Subsystem for Linux) not supported.

### How It Works

1. Claude Code communicates via Chrome Native Messaging API
2. Commands sent to Claude in Chrome extension
3. Extension executes actions in browser
4. Results returned to Claude Code

**Key Behaviors**:

- Opens new tabs (doesn't take over existing)
- Shares browser login state
- Visible browser window required (no headless mode)
- Pauses for login, CAPTCHA (you handle, then continue)

### Setup

**Step 1: Update Claude Code**:

```bash
claude update
```

**Step 2: Start with Chrome enabled**:

```bash
claude --chrome
```

**Step 3: Verify connection**:

```
/chrome
```

Check status, manage settings. Install extension if not detected.

### Enable During Session

Run `/chrome` command within existing session.

### Enable by Default

Run `/chrome` → Select "Enabled by default"

**Note**: Increases context usage (browser tools always loaded). Use `--chrome` flag when needed instead if context is concern.

### Chrome Command

| Action    | Description             |
| --------- | ----------------------- |
| `/chrome` | Check connection status |
| `/chrome` | Manage settings         |
| `/chrome` | Reconnect extension     |
| `/chrome` | View permissions        |

### Available Tools

Run `/mcp` → Click `claude-in-chrome` to see full tool list.

**Capabilities**:

- Navigate pages
- Click and type
- Fill forms
- Scroll
- Read console logs
- Read network requests
- Manage tabs
- Resize windows
- Record GIFs

### Example: Test Local Web App

```
I just updated the login form validation. Can you open localhost:3000,
try submitting the form with invalid data, and check if the error
messages appear correctly?
```

Claude navigates, interacts with form, reports observations.

### Example: Debug with Console

```
Open the dashboard page and check the console for any errors when
the page loads.
```

Claude reads console, filters for specific patterns or error types.

### Example: Form Automation

```
I have a spreadsheet of customer contacts in contacts.csv. For each row,
go to our CRM at crm.example.com, click "Add Contact", and fill in the
name, email, and phone fields.
```

Claude reads local file, navigates web interface, enters data.

### Example: Google Docs

```
Draft a project update based on our recent commits and add it to my
Google Doc at docs.google.com/document/d/abc123
```

Claude opens document, clicks editor, types content. Works with any logged-in app.

### Example: Data Extraction

```
Go to the product listings page and extract the name, price, and
availability for each item. Save the results as a CSV file.
```

Claude navigates, reads content, compiles structured data.

### Example: Multi-Site Workflow

```
Check my calendar for meetings tomorrow, then for each meeting with
an external attendee, look up their company on LinkedIn and add a
note about what they do.
```

Claude works across tabs to gather info and complete workflow.

### Example: Record GIF Demo

```
Record a GIF showing how to complete the checkout flow, from adding
an item to the cart through to the confirmation page.
```

Claude records interaction sequence, saves as GIF file.

### Best Practices

| Practice                | Description                                            |
| ----------------------- | ------------------------------------------------------ |
| Handle modal dialogs    | JS alerts block commands; dismiss manually             |
| Use fresh tabs          | If tab unresponsive, ask for new tab                   |
| Filter console output   | Specify patterns rather than all output                |
| Explicit tool reference | Say "playwright mcp" first time to avoid bash fallback |

### Troubleshooting

**Extension not detected**:

1. Verify Chrome extension installed (latest version)
2. Verify Claude Code version: `claude --version`
3. Check Chrome is running
4. `/chrome` → "Reconnect extension"
5. Restart both Claude Code and Chrome

**Browser not responding**:

1. Check for modal dialog blocking page
2. Ask Claude to create new tab
3. Restart Chrome extension (disable/re-enable)

**First-time setup**:

Native messaging host installed automatically. Restart Chrome if permission errors.

### Permissions

Site-level permissions inherited from Chrome extension.

**Manage**: Chrome extension settings → Control which sites Claude can browse, click, type.

**View current**: `/chrome` shows permission settings.

### Playwright MCP Alternative

For comprehensive test automation, Playwright MCP provides:

| Feature              | Description                                        |
| -------------------- | -------------------------------------------------- |
| Cross-browser        | Chrome, Firefox, Safari                            |
| Device emulation     | 143 devices (iPhone, iPad, Pixel, Galaxy, Desktop) |
| Headless mode        | Background execution                               |
| Test code generation | Automated test creation                            |

**Configuration**:

Add to MCP servers config:

```json
{
  "mcpServers": {
    "playwright": {
      "command": "npx",
      "args": ["@anthropic-ai/mcp-server-playwright"]
    }
  }
}
```

**Usage**:

```
Use playwright mcp to open a browser to example.com
```

### Chrome vs Playwright MCP

| Feature          | Chrome Integration              | Playwright MCP           |
| ---------------- | ------------------------------- | ------------------------ |
| Login state      | Uses your sessions              | Separate sessions        |
| Visibility       | Visible browser                 | Headless option          |
| Browser support  | Chrome only                     | Chrome, Firefox, Safari  |
| Device emulation | None                            | 143 devices              |
| Best for         | Authenticated apps, quick debug | Automated testing, CI/CD |

### DSM Chrome Patterns

**Test Data Fetching UI**:

```
Open the DSM dashboard at localhost:8080, select BTCUSDT from the
symbol dropdown, click "Fetch Data", and verify the OHLCV table loads.
```

**Debug API Responses**:

```
Open the network tab, trigger a data fetch for ETHUSDT, and show me
the API response body and any console errors.
```

**Record Demo GIF**:

```
Record a GIF showing the complete flow of fetching historical data
for a symbol, including cache hit vs miss behavior.
```

**Extract Documentation**:

```
Go to the Binance API documentation page and extract the rate limits
for the klines endpoint. Save as rate-limits.md.
```

### Integration Summary

| Flag          | Description                |
| ------------- | -------------------------- |
| `--chrome`    | Enable Chrome integration  |
| `--no-chrome` | Disable Chrome integration |
| `/chrome`     | Manage in-session          |

**When to Use Chrome**:

- Testing authenticated web apps
- Quick browser debugging
- Working with sites you're logged into
- Recording demos

**When to Use Playwright MCP**:

- Automated test suites
- CI/CD pipelines
- Cross-browser testing
- Headless execution
<!-- SSoT-OK: This section is authoritative for IDE integration -->

## IDE Integration Reference

Integrate Claude Code with VS Code and JetBrains IDEs for enhanced development workflows.

### IDE Integration Overview

| IDE       | Integration Type | Key Features                               |
| --------- | ---------------- | ------------------------------------------ |
| VS Code   | Native extension | Graphical panel, inline diffs, @-mentions  |
| JetBrains | Plugin + CLI     | Diff viewer, context sharing, quick launch |

### VS Code Extension

**Installation**:

- Press `Cmd+Shift+X` / `Ctrl+Shift+X` → Search "Claude Code" → Install
- Or use Command Palette → "Extensions: Install Extension"

**Open Claude Code**:

| Method          | Description                                    |
| --------------- | ---------------------------------------------- |
| Spark icon      | Editor toolbar (top-right, requires file open) |
| Status bar      | Click "✱ Claude Code" (bottom-right)           |
| Command Palette | `Cmd+Shift+P` → "Claude Code"                  |
| Keyboard        | `Cmd+Esc` / `Ctrl+Esc`                         |

### VS Code Features

| Feature                | Description                      |
| ---------------------- | -------------------------------- |
| Graphical panel        | Native chat interface            |
| Inline diffs           | Side-by-side change review       |
| @-mentions             | Reference files with line ranges |
| Plan review            | Approve plans before changes     |
| Auto-accept            | Optional automatic edit approval |
| Multiple conversations | Tabs or windows                  |
| Terminal mode          | CLI-style interface option       |

### VS Code Keyboard Shortcuts

| Shortcut                           | Action                            |
| ---------------------------------- | --------------------------------- |
| `Cmd+Esc` / `Ctrl+Esc`             | Toggle focus editor/Claude        |
| `Cmd+Shift+Esc` / `Ctrl+Shift+Esc` | Open in new tab                   |
| `Cmd+N` / `Ctrl+N`                 | New conversation (Claude focused) |
| `Option+K` / `Alt+K`               | Insert @-mention reference        |
| `Shift+Enter`                      | Multi-line input                  |

### VS Code @-Mentions

```
> Explain @auth.js
> What's in @src/components/
> Review @file.ts#5-10
```

- Type `@` for fuzzy file matching
- Trailing `/` for folders
- `#L1-99` for line ranges
- `Option+K` / `Alt+K` inserts current selection

### VS Code Selection Context

- Claude automatically sees selected text
- Footer shows lines selected
- Click eye icon to toggle visibility
- `Shift+drag` files to attach

### VS Code Terminal Reference

```
@terminal:name
```

Reference terminal output by terminal title.

### VS Code Prompt Box Features

| Feature           | Description                                         |
| ----------------- | --------------------------------------------------- |
| Permission modes  | Click indicator to switch (normal/plan/auto-accept) |
| Command menu      | Type `/` for commands                               |
| Context indicator | Shows context window usage                          |
| Extended thinking | Toggle via `/` menu                                 |

### VS Code Permission Modes

| Mode        | Behavior                         | Setting             |
| ----------- | -------------------------------- | ------------------- |
| Default     | Ask permission each action       | `default`           |
| Plan        | Describe plan, wait for approval | `plan`              |
| Auto-accept | Make edits without asking        | `acceptEdits`       |
| Bypass      | Skip all prompts (dangerous)     | `bypassPermissions` |

Set default: `claudeCode.initialPermissionMode`

### VS Code Conversation History

- Click dropdown at top of panel
- Search by keyword
- Browse by time (Today, Yesterday, etc.)
- Resume remote sessions from claude.ai (Remote tab)

### VS Code Multiple Conversations

| Action     | Method                                 |
| ---------- | -------------------------------------- |
| New tab    | Command Palette → "Open in New Tab"    |
| New window | Command Palette → "Open in New Window" |

Tab indicators:

- Blue dot: Permission request pending
- Orange dot: Claude finished while hidden

### VS Code Extension Settings

| Setting                 | Default   | Description                    |
| ----------------------- | --------- | ------------------------------ |
| `selectedModel`         | `default` | Model for new conversations    |
| `useTerminal`           | `false`   | Terminal mode instead of panel |
| `initialPermissionMode` | `default` | Permission behavior            |
| `preferredLocation`     | `panel`   | Where Claude opens             |
| `autosave`              | `true`    | Auto-save before read/write    |
| `useCtrlEnterToSend`    | `false`   | Ctrl/Cmd+Enter to send         |
| `respectGitIgnore`      | `true`    | Exclude .gitignore patterns    |

### VS Code Checkpoints (Rewind)

Hover over message to reveal rewind button:

| Option            | Description                     |
| ----------------- | ------------------------------- |
| Fork conversation | New branch, keep code           |
| Rewind code       | Revert files, keep conversation |
| Fork and rewind   | Both                            |

### VS Code vs CLI

| Feature           | CLI | VS Code Extension |
| ----------------- | --- | ----------------- |
| Commands/skills   | All | Subset (type `/`) |
| MCP server config | Yes | Configure via CLI |
| Checkpoints       | Yes | Yes               |
| `!` bash shortcut | Yes | No                |
| Tab completion    | Yes | No                |

### VS Code Plugin Management

Type `/plugins` to open plugin interface:

- **Plugins tab**: Install, enable/disable
- **Marketplaces tab**: Add/remove sources
- Installation scopes: user, project, local

### JetBrains Plugin

**Supported IDEs**:

- IntelliJ IDEA
- PyCharm
- Android Studio
- WebStorm
- PhpStorm
- GoLand

**Installation**:

Install from JetBrains Marketplace: "Claude Code [Beta]"

### JetBrains Features

| Feature            | Description                    |
| ------------------ | ------------------------------ |
| Quick launch       | `Cmd+Esc` / `Ctrl+Esc`         |
| Diff viewing       | IDE diff viewer for changes    |
| Selection context  | Auto-shared with Claude        |
| File reference     | `Cmd+Option+K` / `Alt+Ctrl+K`  |
| Diagnostic sharing | Lint/syntax errors auto-shared |

### JetBrains Quick Launch

| Platform      | Shortcut   |
| ------------- | ---------- |
| macOS         | `Cmd+Esc`  |
| Windows/Linux | `Ctrl+Esc` |

Or click Claude Code button in UI.

### JetBrains Usage

**From IDE Terminal**:

```bash
claude
```

All integration features active.

**From External Terminal**:

```bash
claude
> /ide
```

Connect to JetBrains IDE.

### JetBrains Plugin Settings

**Settings → Tools → Claude Code [Beta]**:

| Setting                | Description                    |
| ---------------------- | ------------------------------ |
| Claude command         | Custom command path            |
| Suppress notification  | Skip "not found" notifications |
| Option+Enter multiline | Enable for macOS               |
| Automatic updates      | Check for plugin updates       |

**WSL Command**:

```
wsl -d Ubuntu -- bash -lic "claude"
```

### JetBrains ESC Key Fix

If ESC doesn't interrupt Claude:

1. Settings → Tools → Terminal
2. Uncheck "Move focus to the editor with Escape"
3. Or delete "Switch focus to Editor" shortcut

### JetBrains Remote Development

Install plugin in **remote host** via Settings → Plugin (Host).

### JetBrains Security Considerations

With auto-edit enabled, Claude can modify IDE config files that auto-execute.

**Recommendations**:

- Use manual approval mode
- Only use with trusted prompts
- Be aware of file access

### Context Sharing Benefits

| What's Shared   | Benefit                   |
| --------------- | ------------------------- |
| Open files      | Claude sees current work  |
| Selected code   | No manual copying         |
| Diagnostics     | Lint/type errors visible  |
| Terminal output | Reference with @terminal: |

### Diff Viewer Features

**VS Code**:

- Side-by-side comparison
- Permission prompt for each change
- Accept/reject/modify

**JetBrains**:

- IDE native diff viewer
- File list on left
- Changes on right
- Comment on specific lines

### Diff Stats Indicator

Shows lines added/removed (e.g., +12 -1).

Click to open diff viewer.

### Commenting on Diffs

1. Click any line in diff
2. Type feedback in comment box
3. Press Enter to send
4. `Cmd+Enter` to send all comments

Claude reads comments and updates.

### DSM IDE Patterns

**VS Code Setup**:

```json
{
  "claudeCode.selectedModel": "sonnet",
  "claudeCode.initialPermissionMode": "default",
  "claudeCode.autosave": true
}
```

**JetBrains PyCharm Setup**:

1. Install Claude Code [Beta] plugin
2. Restart PyCharm
3. Configure: Settings → Tools → Claude Code
4. Run `claude` in terminal

**Context-Aware DSM Work**:

```
> Review @src/fetchers/binance.py#50-100 for FCP compliance
> Check @tests/unit/test_fetcher.py for coverage gaps
> The selected code shows a cache miss - explain why
```

**Diagnostic Integration**:

When pyright reports type errors, Claude sees them automatically:

```
> Fix the type errors in the current file
```

### IDE Integration Best Practices

| Practice                    | Description                    |
| --------------------------- | ------------------------------ |
| Use context sharing         | Let Claude see diagnostics     |
| Review diffs carefully      | Check changes before accepting |
| Use plan mode               | For complex changes            |
| Multiple conversations      | Separate tasks in tabs         |
| Checkpoint before risky ops | Enable rewind capability       |

### Troubleshooting

<!-- SSoT-OK: Version requirements are illustrative -->

**VS Code spark icon missing**:

1. Open a file (not just folder)
2. Check VS Code version (recent version required)
3. Reload window
4. Disable conflicting extensions

**JetBrains plugin not working**:

1. Run from project root
2. Check plugin enabled
3. Restart IDE completely
4. For Remote Dev: install in remote host

**Claude not responding**:

1. Check internet connection
2. Start new conversation
3. Try CLI for detailed errors
## Headless and Programmatic Usage Reference

Reference for running Claude Code non-interactively via CLI, Python SDK, and TypeScript SDK.

### Overview

The Agent SDK provides the same tools, agent loop, and context management that power Claude Code. Available as:

- CLI (`claude -p`) for scripts and CI/CD
- Python package for full programmatic control
- TypeScript package for full programmatic control

### Basic CLI Usage

Add `-p` (or `--print`) flag for non-interactive execution:

```bash
# Simple question
claude -p "What does the auth module do?"

# With tool permissions
claude -p "Find and fix the bug in auth.py" --allowedTools "Read,Edit,Bash"
```

### Output Formats

Control response format with `--output-format`:

| Format        | Description                           | Use Case                |
| ------------- | ------------------------------------- | ----------------------- |
| `text`        | Plain text output (default)           | Simple scripts          |
| `json`        | Structured JSON with session metadata | Parsing, automation     |
| `stream-json` | Newline-delimited JSON for real-time  | Live streaming, CI logs |

**JSON output example**:

```bash
# Get project summary as JSON
claude -p "Summarize this project" --output-format json

# Extract just the result with jq
claude -p "Summarize this project" --output-format json | jq -r '.result'
```

### Structured Output with JSON Schema

Use `--json-schema` to enforce specific output structure:

```bash
claude -p "Extract the main function names from auth.py" \
  --output-format json \
  --json-schema '{"type":"object","properties":{"functions":{"type":"array","items":{"type":"string"}}},"required":["functions"]}'
```

Response includes metadata with structured output in `structured_output` field:

```bash
# Extract structured output
claude -p "Extract function names" \
  --output-format json \
  --json-schema '...' \
  | jq '.structured_output'
```

### Streaming Responses

Stream tokens as they're generated:

```bash
# Full streaming with verbose output
claude -p "Explain recursion" --output-format stream-json --verbose --include-partial-messages

# Filter for text deltas only
claude -p "Write a poem" --output-format stream-json --verbose --include-partial-messages | \
  jq -rj 'select(.type == "stream_event" and .event.delta.type? == "text_delta") | .event.delta.text'
```

### Auto-Approve Tools

Use `--allowedTools` with permission rule syntax:

```bash
# Run tests and fix failures
claude -p "Run the test suite and fix any failures" \
  --allowedTools "Bash,Read,Edit"

# Git operations with prefix matching
claude -p "Look at my staged changes and create an appropriate commit" \
  --allowedTools "Bash(git diff *),Bash(git log *),Bash(git status *),Bash(git commit *)"
```

**Important**: The trailing `*` enables prefix matching. Space before `*` is important - without it, `Bash(git diff*)` would also match `git diff-index`.

### Customizing System Prompt

```bash
# Append to default system prompt
gh pr diff "$1" | claude -p \
  --append-system-prompt "You are a security engineer. Review for vulnerabilities." \
  --output-format json

# Fully replace system prompt
claude -p "Analyze this code" --system-prompt "You are a code reviewer..."
```

### Continue Conversations

```bash
# Continue most recent conversation
claude -p "Review this codebase for performance issues"
claude -p "Now focus on the database queries" --continue
claude -p "Generate a summary of all issues found" --continue

# Resume specific session by ID
session_id=$(claude -p "Start a review" --output-format json | jq -r '.session_id')
claude -p "Continue that review" --resume "$session_id"
```

### CI/CD Integration Patterns

**GitHub Actions example**:

```yaml
jobs:
  code-review:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: Review PR
        run: |
          gh pr diff ${{ github.event.pull_request.number }} | \
          claude -p --append-system-prompt "Review for security issues" \
            --output-format json \
            --allowedTools "Read,Grep,Glob"
```

**GitLab CI example**:

```yaml
code-review:
  script:
    - git diff origin/main...HEAD | claude -p "Review these changes" --output-format json
```

### Permission Flags for CI/CD

| Flag                             | Purpose                     | Use Case              |
| -------------------------------- | --------------------------- | --------------------- |
| `--allowedTools "Read,Grep"`     | Limit to read-only tools    | Code review           |
| `--allowedTools "Bash,Edit"`     | Allow modifications         | Auto-fix              |
| `--max-turns N`                  | Limit execution turns       | Cost control          |
| `--dangerously-skip-permissions` | Skip all permission prompts | Isolated CI container |

**Warning**: Only use `--dangerously-skip-permissions` in isolated environments (CI containers).

### Python SDK Usage

```python
import anthropic

client = anthropic.Anthropic()

# Basic message
response = client.messages.create(
    model="claude-sonnet-4-5",
    max_tokens=16000,
    messages=[{"role": "user", "content": "Analyze this code"}]
)

# With extended thinking
response = client.messages.create(
    model="claude-sonnet-4-5",
    max_tokens=16000,
    thinking={"type": "enabled", "budget_tokens": 10000},
    messages=[{"role": "user", "content": "Complex analysis task"}]
)

# Process response blocks
for block in response.content:
    if block.type == "thinking":
        print(f"Thinking: {block.thinking}")
    elif block.type == "text":
        print(f"Response: {block.text}")
```

### TypeScript SDK Usage

```typescript
import Anthropic from "@anthropic-ai/sdk";

const client = new Anthropic();

// Basic message
const response = await client.messages.create({
  model: "claude-sonnet-4-5",
  max_tokens: 16000,
  messages: [{ role: "user", content: "Analyze this code" }],
});

// With extended thinking
const response = await client.messages.create({
  model: "claude-sonnet-4-5",
  max_tokens: 16000,
  thinking: { type: "enabled", budget_tokens: 10000 },
  messages: [{ role: "user", content: "Complex analysis task" }],
});

// Process response blocks
for (const block of response.content) {
  if (block.type === "thinking") {
    console.log(`Thinking: ${block.thinking}`);
  } else if (block.type === "text") {
    console.log(`Response: ${block.text}`);
  }
}
```

### Streaming with SDKs

**Python streaming**:

```python
with client.messages.stream(
    model="claude-sonnet-4-5",
    max_tokens=16000,
    thinking={"type": "enabled", "budget_tokens": 10000},
    messages=[{"role": "user", "content": "Explain recursion"}],
) as stream:
    for event in stream:
        if event.type == "content_block_delta":
            if event.delta.type == "thinking_delta":
                print(event.delta.thinking, end="", flush=True)
            elif event.delta.type == "text_delta":
                print(event.delta.text, end="", flush=True)
```

**TypeScript streaming**:

```typescript
const stream = await client.messages.stream({
  model: "claude-sonnet-4-5",
  max_tokens: 16000,
  thinking: { type: "enabled", budget_tokens: 10000 },
  messages: [{ role: "user", content: "Explain recursion" }],
});

for await (const event of stream) {
  if (event.type === "content_block_delta") {
    if (event.delta.type === "thinking_delta") {
      process.stdout.write(event.delta.thinking);
    } else if (event.delta.type === "text_delta") {
      process.stdout.write(event.delta.text);
    }
  }
}
```

### DSM-Specific Headless Patterns

**Batch FCP validation**:

```bash
# Validate FCP behavior across symbols
claude -p "Test FCP failover for BTCUSDT, ETHUSDT, SOLUSDT" \
  --allowedTools "Bash(uv run *),Read,Grep" \
  --output-format json \
  | jq '.result'
```

**Data integrity check**:

```bash
# Run DataFrame validation
claude -p "Validate OHLCV data integrity for the past 24 hours" \
  --allowedTools "Bash(uv run pytest *),Read" \
  --max-turns 10
```

**Automated test generation**:

```bash
# Generate tests for new module
claude -p "Generate pytest tests for src/datasourcemanager/new_module.py following DSM patterns" \
  --allowedTools "Read,Write,Edit,Bash(uv run pytest *)" \
  --output-format json
```

### Environment Variables for Headless Mode

| Variable                    | Purpose                          |
| --------------------------- | -------------------------------- |
| `ANTHROPIC_API_KEY`         | API authentication               |
| `CLAUDE_CODE_USE_BEDROCK=1` | Use AWS Bedrock instead of API   |
| `CLAUDE_CODE_USE_VERTEX=1`  | Use Google Vertex AI instead     |
| `MAX_THINKING_TOKENS`       | Override default thinking budget |
| `USE_BUILTIN_RIPGREP=0`     | Use system ripgrep instead       |

### Best Practices for CI/CD

1. **Use specific tool permissions** - Don't use `--dangerously-skip-permissions` unless in isolated container
2. **Set `--max-turns`** - Prevent runaway execution in automation
3. **Use JSON output** - Parse results programmatically
4. **Capture session IDs** - Enable continuation and debugging
5. **Handle streaming** - For long operations, use `stream-json` format
6. **Check exit codes** - Claude returns non-zero on errors
## Troubleshooting Reference

Comprehensive troubleshooting guide for Claude Code installation, configuration, and runtime issues.

### Quick Diagnostic Commands

```bash
# Check installation health
claude doctor

# Check version
claude --version

# Verify authentication
claude config

# Debug MCP servers
claude --mcp-debug
```

### The `/doctor` Command

Running `/doctor` within Claude Code checks:

- Installation type, version, and search functionality
- Auto-update status and available versions
- Invalid settings files (malformed JSON, incorrect types)
- MCP server configuration errors
- Keybinding configuration problems
- Context usage warnings (large CLAUDE.md files, high MCP token usage)
- Plugin and agent loading errors

### Configuration File Locations

| File                          | Purpose                                  |
| ----------------------------- | ---------------------------------------- |
| `~/.claude/settings.json`     | User settings (permissions, hooks)       |
| `.claude/settings.json`       | Project settings (committed)             |
| `.claude/settings.local.json` | Local project settings (not committed)   |
| `~/.claude.json`              | Global state (theme, OAuth, MCP servers) |
| `.mcp.json`                   | Project MCP servers (committed)          |
| `managed-settings.json`       | Managed settings (enterprise)            |
| `managed-mcp.json`            | Managed MCP servers (enterprise)         |

**Managed file locations**:

| Platform  | Location                                   |
| --------- | ------------------------------------------ |
| macOS     | `/Library/Application Support/ClaudeCode/` |
| Linux/WSL | `/etc/claude-code/`                        |
| Windows   | `C:\Program Files\ClaudeCode\`             |

### Resetting Configuration

```bash
# Reset all user settings and state
rm ~/.claude.json
rm -rf ~/.claude/

# Reset project-specific settings
rm -rf .claude/
rm .mcp.json
```

**Warning**: This removes all settings, MCP server configurations, and session history.

### Common Installation Issues

#### Windows Installation Issues (WSL)

**OS/platform detection issues**:

```bash
# Fix: Set npm config before installation
npm config set os linux

# Install with force flags (do NOT use sudo)
npm install -g @anthropic-ai/claude-code --force --no-os-check
```

**Node not found errors** (`exec: node: not found`):

```bash
# Check if using Windows npm
which npm
which node
# Should show Linux paths (/usr/) not Windows paths (/mnt/c/)

# Fix: Install Node via nvm in WSL
# SSoT-OK: nvm install script URL is canonical source
curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/master/install.sh | bash
source ~/.nvm/nvm.sh
nvm install --lts
```

**nvm version conflicts** (WSL imports Windows PATH):

```bash
# Add to ~/.bashrc or ~/.zshrc
export NVM_DIR="$HOME/.nvm"
[ -s "$NVM_DIR/nvm.sh" ] && \. "$NVM_DIR/nvm.sh"
[ -s "$NVM_DIR/bash_completion" ] && \. "$NVM_DIR/bash_completion"

# Or explicitly prepend Linux paths
export PATH="$HOME/.nvm/versions/node/$(node -v)/bin:$PATH"
```

**Warning**: Avoid disabling Windows PATH importing (`appendWindowsPath = false`) - breaks calling Windows executables from WSL.

#### WSL2 Sandbox Setup

```bash
# Install sandbox dependencies
# Ubuntu/Debian
sudo apt-get install bubblewrap socat

# Fedora
sudo dnf install bubblewrap socat

# Alpine
apk add ripgrep
```

**Note**: WSL1 does not support sandboxing.

#### Native Installation (Recommended)

```bash
# macOS, Linux, WSL - stable version
curl -fsSL https://claude.ai/install.sh | bash

# Specific version (replace with desired version)
curl -fsSL https://claude.ai/install.sh | bash -s <version>

# Windows PowerShell - stable version
irm https://claude.ai/install.ps1 | iex
```

Installation creates symlink at `~/.local/bin/claude`.

#### Windows: Git Bash Not Found

```powershell
# Set Git Bash path in PowerShell
$env:CLAUDE_CODE_GIT_BASH_PATH="C:\Program Files\Git\bin\bash.exe"

# Or add to system environment variables permanently
```

#### Windows: Command Not Found After Installation

1. Press `Win + R`, type `sysdm.cpl`, press Enter
2. Click **Advanced** → **Environment Variables**
3. Under "User variables", select **Path** → **Edit**
4. Click **New** and add: `%USERPROFILE%\.local\bin`
5. Restart terminal

### Authentication Issues

**Repeated permission prompts**:

```bash
# Use /permissions command to allow specific tools
/permissions
```

**Authentication problems**:

```bash
# 1. Sign out completely
/logout

# 2. Close Claude Code

# 3. Restart and re-authenticate
claude

# If browser doesn't open, press 'c' to copy OAuth URL
```

**Force clean login**:

```bash
rm -rf ~/.config/claude-code/auth.json
claude
```

### Performance and Stability

#### High CPU or Memory Usage

1. Use `/compact` regularly to reduce context size
2. Close and restart between major tasks
3. Add large build directories to `.gitignore`

#### Command Hangs or Freezes

1. Press `Ctrl+C` to cancel current operation
2. If unresponsive, close terminal and restart

#### Search and Discovery Issues

If Search tool, `@file` mentions, agents, and skills aren't working:

```bash
# Install system ripgrep
# macOS
brew install ripgrep

# Windows
winget install BurntSushi.ripgrep.MSVC

# Ubuntu/Debian
sudo apt install ripgrep

# Then set environment variable
export USE_BUILTIN_RIPGREP=0
```

#### Slow Search Results on WSL

Disk read performance penalties when working across file systems on WSL.

**Solutions**:

1. Submit more specific searches - specify directories or file types
2. Move project to Linux filesystem (`/home/`) instead of Windows (`/mnt/c/`)
3. Run Claude Code natively on Windows instead of WSL

### IDE Integration Issues

#### JetBrains IDE Not Detected on WSL2

Likely due to WSL2 networking or Windows Firewall.

**Option 1: Configure Windows Firewall** (recommended):

```bash
# 1. Find WSL2 IP
wsl hostname -I
# Example: 172.21.123.456

# 2. Create firewall rule (PowerShell as Admin)
New-NetFirewallRule -DisplayName "Allow WSL2 Internal Traffic" -Direction Inbound -Protocol TCP -Action Allow -RemoteAddress 172.21.0.0/16 -LocalAddress 172.21.0.0/16

# 3. Restart IDE and Claude Code
```

**Option 2: Switch to mirrored networking**:

Add to `.wslconfig` in Windows user directory:

```ini
[wsl2]
networkingMode=mirrored
```

Then restart WSL: `wsl --shutdown`

#### Escape Key Not Working in JetBrains

1. Go to Settings → Tools → Terminal
2. Either:
   - Uncheck "Move focus to the editor with Escape", or
   - Click "Configure terminal keybindings" → delete "Switch focus to Editor"
3. Apply changes

### Markdown Formatting Issues

#### Missing Language Tags on Code Blocks

**Solutions**:

1. Ask Claude to add language tags: "Add appropriate language tags to all code blocks"
2. Use post-processing hooks for automatic formatting
3. Manual verification after generation

#### Inconsistent Spacing

1. Request formatting corrections
2. Set up hooks to run `prettier` on generated markdown
3. Specify formatting preferences in CLAUDE.md

### DSM-Specific Troubleshooting

#### FCP-Related Issues

**Cache miss debugging**:

```bash
# Use fcp-debugger agent
/debug-fcp BTCUSDT

# Check cache integrity
mise run cache:clear
uv run python -c "from datasourcemanager import DataSourceManager; dsm = DataSourceManager(); print(dsm.cache_status())"
```

**Failover not triggering**:

1. Check rate limit status on primary source
2. Verify backup sources are configured
3. Review FCP decision log

#### DataFrame Validation Failures

```bash
# Use validate-data command
/validate-data

# Manual validation
uv run pytest tests/test_dataframe_validation.py -v
```

#### Symbol Format Errors

Check symbol format matches exchange requirements:

| Exchange | Format           | Example    |
| -------- | ---------------- | ---------- |
| Binance  | `{base}{quote}`  | `BTCUSDT`  |
| OKX      | `{base}-{quote}` | `BTC-USDT` |
| Bybit    | `{base}{quote}`  | `BTCUSDT`  |

#### Timestamp Issues

DSM requires UTC timestamps. Common issues:

1. **Naive datetime** - Always use timezone-aware datetimes
2. **Wrong timezone** - Convert to UTC before passing to DSM
3. **Milliseconds vs seconds** - DSM uses milliseconds

```python
# Correct timestamp handling
from datetime import datetime, timezone

# Get current UTC time
now = datetime.now(timezone.utc)

# Convert from naive to UTC
naive_dt = datetime(2024, 1, 1)
utc_dt = naive_dt.replace(tzinfo=timezone.utc)
```

### Getting More Help

1. **Use `/bug` command** - Report problems directly to Anthropic with full context
2. **Check GitHub issues** - [github.com/anthropics/claude-code/issues](https://github.com/anthropics/claude-code/issues)
3. **Run `/doctor`** - Diagnose common issues
4. **Ask Claude** - Claude has built-in access to its documentation

### Environment Variable Reference

| Variable                    | Purpose                            |
| --------------------------- | ---------------------------------- |
| `ANTHROPIC_API_KEY`         | API authentication                 |
| `CLAUDE_CODE_USE_BEDROCK`   | Use AWS Bedrock (set to 1)         |
| `CLAUDE_CODE_USE_VERTEX`    | Use Google Vertex AI (set to 1)    |
| `CLAUDE_CODE_GIT_BASH_PATH` | Git Bash path on Windows           |
| `USE_BUILTIN_RIPGREP`       | Use system ripgrep (set to 0)      |
| `MAX_THINKING_TOKENS`       | Override thinking budget           |
| `ENABLE_LSP_TOOL`           | Enable/disable LSP (set to 0/1)    |
| `MCP_TIMEOUT`               | MCP server timeout in milliseconds |
## Extended Thinking Reference

Reference for Claude's extended thinking capabilities with enhanced reasoning for complex tasks.

### Overview

Extended thinking gives Claude enhanced reasoning capabilities by allowing internal step-by-step reasoning before delivering final answers. Supported models include:

- Claude Opus 4.5, 4.1, 4
- Claude Sonnet 4.5, 4, 3.7
- Claude Haiku 4.5

### How Extended Thinking Works

When enabled, Claude creates `thinking` content blocks with internal reasoning, then incorporates insights before crafting a final response.

**Response format**:

```json
{
  "content": [
    {
      "type": "thinking",
      "thinking": "Let me analyze this step by step...",
      "signature": "WaUjzkypQ2mUEVM36O2TxuC06KN8xyfbJwyem2dw3URve/op91XWHOEBLLqIOMfFG..."
    },
    {
      "type": "text",
      "text": "Based on my analysis..."
    }
  ]
}
```

### Enabling Extended Thinking

**API usage**:

```python
import anthropic

client = anthropic.Anthropic()

response = client.messages.create(
    model="claude-sonnet-4-5",
    max_tokens=16000,
    thinking={
        "type": "enabled",
        "budget_tokens": 10000
    },
    messages=[{
        "role": "user",
        "content": "Are there an infinite number of prime numbers such that n mod 4 == 3?"
    }]
)

# Process thinking and text blocks
for block in response.content:
    if block.type == "thinking":
        print(f"Thinking summary: {block.thinking}")
    elif block.type == "text":
        print(f"Response: {block.text}")
```

### Budget Tokens Parameter

The `budget_tokens` parameter determines maximum tokens for internal reasoning:

| Budget Range | Recommended For                      |
| ------------ | ------------------------------------ |
| 1,024        | Minimum budget (simple tasks)        |
| 10,000       | Standard reasoning tasks             |
| 16,000+      | Complex analysis tasks               |
| 32,000+      | Deep reasoning (use batch API)       |
| 63,999       | Maximum (with `MAX_THINKING_TOKENS`) |

**Key considerations**:

- `budget_tokens` must be less than `max_tokens`
- Minimum budget is 1,024 tokens
- Claude may not use entire budget (especially above 32k)
- Higher budgets can improve quality but increase latency

### Summarized Thinking (Claude 4 Models)

Claude 4 models return **summarized** thinking instead of full output:

- Charged for full thinking tokens generated
- Billed output count **won't match** visible token count
- First few lines more verbose for prompt engineering
- Summarization preserves key ideas with minimal latency

**Note**: Claude Sonnet 3.7 continues to return full thinking output.

### Streaming Extended Thinking

```python
with client.messages.stream(
    model="claude-sonnet-4-5",
    max_tokens=16000,
    thinking={"type": "enabled", "budget_tokens": 10000},
    messages=[{"role": "user", "content": "Explain recursion"}],
) as stream:
    for event in stream:
        if event.type == "content_block_delta":
            if event.delta.type == "thinking_delta":
                print(event.delta.thinking, end="", flush=True)
            elif event.delta.type == "text_delta":
                print(event.delta.text, end="", flush=True)
```

**Streaming events**:

- `content_block_start` - New thinking or text block starting
- `thinking_delta` - Thinking content arriving
- `signature_delta` - Signature for verification
- `text_delta` - Response text arriving
- `content_block_stop` - Block complete

### Extended Thinking with Tool Use

Thinking can be used alongside tools for reasoning through selection and results:

```python
weather_tool = {
    "name": "get_weather",
    "description": "Get current weather for a location",
    "input_schema": {
        "type": "object",
        "properties": {"location": {"type": "string"}},
        "required": ["location"]
    }
}

response = client.messages.create(
    model="claude-sonnet-4-5",
    max_tokens=16000,
    thinking={"type": "enabled", "budget_tokens": 10000},
    tools=[weather_tool],
    messages=[{"role": "user", "content": "What's the weather in Paris?"}]
)
```

**Important limitations**:

- Only supports `tool_choice: {"type": "auto"}` or `tool_choice: {"type": "none"}`
- Cannot use `tool_choice: {"type": "any"}` or `tool_choice: {"type": "tool", "name": "..."}`

### Preserving Thinking Blocks

When continuing conversations with tools, **must pass thinking blocks back**:

```python
# Extract blocks from first response
thinking_block = next((b for b in response.content if b.type == 'thinking'), None)
tool_use_block = next((b for b in response.content if b.type == 'tool_use'), None)

# Include in continuation - CRITICAL
continuation = client.messages.create(
    model="claude-sonnet-4-5",
    max_tokens=16000,
    thinking={"type": "enabled", "budget_tokens": 10000},
    tools=[weather_tool],
    messages=[
        {"role": "user", "content": "What's the weather in Paris?"},
        {"role": "assistant", "content": [thinking_block, tool_use_block]},  # Must include
        {"role": "user", "content": [{
            "type": "tool_result",
            "tool_use_id": tool_use_block.id,
            "content": "20°C, sunny"
        }]}
    ]
)
```

### Interleaved Thinking (Beta)

Enables Claude to think between tool calls with beta header `interleaved-thinking-2025-05-14`:

**Without interleaved thinking**:

```
Turn 1: [thinking] → [tool_use]
Turn 2: [tool_use] (no thinking)
Turn 3: [text] (no thinking)
```

**With interleaved thinking**:

```
Turn 1: [thinking] → [tool_use]
Turn 2: [thinking] → [tool_use] (thinks after tool result)
Turn 3: [thinking] → [text] (thinks before final answer)
```

**Key considerations**:

- `budget_tokens` can exceed `max_tokens` (represents total across all thinking blocks)
- Supported for Claude 4 models only
- Enables more sophisticated multi-step reasoning

### Thinking and Prompt Caching

**Cache invalidation**:

- Changes to thinking parameters invalidate message cache breakpoints
- System prompts and tools remain cached despite thinking changes
- Interleaved thinking amplifies cache invalidation

**Thinking block caching behavior**:

- Thinking blocks from previous turns are stripped (not counted in context)
- Cached thinking blocks count as input tokens when read from cache
- Claude Opus 4.5 preserves thinking blocks by default (enables cache optimization)

### Thinking Encryption and Redaction

**Encryption**: Full thinking encrypted in `signature` field for verification.

**Redacted thinking**: Safety systems may flag content, returning `redacted_thinking` blocks:

```json
{
  "content": [
    {
      "type": "thinking",
      "thinking": "Let me analyze...",
      "signature": "..."
    },
    {
      "type": "redacted_thinking",
      "data": "EmwKAhgBEgy3va3pzix/LafPsn4..."
    },
    {
      "type": "text",
      "text": "Based on my analysis..."
    }
  ]
}
```

**Handling redacted blocks**:

- Filter from user display while preserving in API calls
- Pass back unmodified for reasoning continuity
- Still billable as output tokens

### Context Window Considerations

**With extended thinking**:

```
context window =
  (current input - previous thinking) +
  (thinking + encrypted thinking + text output)
```

**With thinking and tool use**:

```
context window =
  (current input + previous thinking + tool use) +
  (thinking + encrypted thinking + text output)
```

### Best Practices

**Budget optimization**:

- Start at minimum (1,024) and increase incrementally
- Higher budgets improve quality with diminishing returns
- Above 32k, use batch processing to avoid timeouts

**Performance**:

- Expect longer response times
- Streaming required when `max_tokens` > 21,333
- Factor in thinking block generation time

**Feature compatibility**:

- Not compatible with `temperature` or `top_k` modifications
- `top_p` limited to 1-0.95 range
- Cannot pre-fill responses

**Task selection**:

- Best for complex reasoning (math, coding, analysis)
- Don't use for simple queries (wastes tokens)
- Review extended thinking prompting tips for optimization

### DSM-Specific Extended Thinking Patterns

**Complex FCP analysis**:

```python
response = client.messages.create(
    model="claude-sonnet-4-5",
    max_tokens=16000,
    thinking={"type": "enabled", "budget_tokens": 16000},
    messages=[{
        "role": "user",
        "content": "Analyze the FCP failover behavior for BTCUSDT across all data sources, identify patterns in rate limiting, and suggest optimization strategies."
    }]
)
```

**DataFrame integrity investigation**:

```python
response = client.messages.create(
    model="claude-sonnet-4-5",
    max_tokens=16000,
    thinking={"type": "enabled", "budget_tokens": 10000},
    messages=[{
        "role": "user",
        "content": "Review the OHLCV data integrity for the past week, identify any gaps or anomalies, and determine root causes."
    }]
)
```

**Architecture decisions**:

```python
response = client.messages.create(
    model="claude-opus-4-5",
    max_tokens=32000,
    thinking={"type": "enabled", "budget_tokens": 32000},
    messages=[{
        "role": "user",
        "content": "Design a new caching strategy for DSM that handles rate limiting gracefully while maintaining data freshness. Consider trade-offs between memory usage, latency, and API costs."
    }]
)
```

### Environment Variables

| Variable              | Purpose                          |
| --------------------- | -------------------------------- |
| `MAX_THINKING_TOKENS` | Override default thinking budget |

**Unlocking higher budgets**:

```bash
# Set to 63,999 for 2x default budget
export MAX_THINKING_TOKENS=63999
```
## Prompt Engineering Reference for Claude 4.x

Reference for prompt engineering best practices with Claude 4.x models (Opus 4.5, Sonnet 4.5, Haiku 4.5).

### Key Changes in Claude 4.x

Claude 4.x models have been trained for **precise instruction following**:

- Takes instructions literally (does exactly what you ask)
- Less inferring of intent from vague requests
- More explicit direction needed for "above and beyond" behavior
- More concise and direct communication style

### General Principles

#### Be Explicit with Instructions

```
# Less effective
Create an analytics dashboard

# More effective
Create an analytics dashboard. Include as many relevant features
and interactions as possible. Go beyond the basics to create a
fully-featured implementation.
```

#### Add Context for Motivation

```
# Less effective
NEVER use ellipses

# More effective
Your response will be read aloud by a text-to-speech engine,
so never use ellipses since the text-to-speech engine will not
know how to pronounce them.
```

Claude generalizes from the explanation.

#### Match Prompt Style to Desired Output

The formatting style in your prompt influences Claude's response style:

- Remove markdown from prompt → reduces markdown in output
- Use prose → get prose back
- Use lists → get lists back

### Tool Usage Patterns

Claude 4.x follows precise instructions about tools. Be explicit:

```
# Claude will only suggest:
Can you suggest some changes to improve this function?

# Claude will make changes:
Change this function to improve its performance.

# Claude will implement:
Make these edits to the authentication flow.
```

**Proactive action prompt**:

```xml
<default_to_action>
By default, implement changes rather than only suggesting them.
If the user's intent is unclear, infer the most useful likely action
and proceed, using tools to discover any missing details instead of guessing.
</default_to_action>
```

**Conservative action prompt**:

```xml
<do_not_act_before_instructions>
Do not jump into implementation or change files unless clearly instructed.
When the user's intent is ambiguous, default to providing information,
doing research, and providing recommendations rather than taking action.
</do_not_act_before_instructions>
```

### Parallel Tool Calling

Claude 4.x excels at parallel tool execution:

```xml
<use_parallel_tool_calls>
If you intend to call multiple tools and there are no dependencies
between the tool calls, make all of the independent tool calls in parallel.
Prioritize calling tools simultaneously whenever the actions can be done
in parallel rather than sequentially. Maximize use of parallel tool calls
where possible to increase speed and efficiency. However, if some tool calls
depend on previous calls to inform dependent values, do NOT call these tools
in parallel and instead call them sequentially. Never use placeholders or
guess missing parameters in tool calls.
</use_parallel_tool_calls>
```

### Context Awareness

Claude 4.5 tracks its remaining context window. For agent harnesses with compaction:

```
Your context window will be automatically compacted as it approaches its limit,
allowing you to continue working indefinitely from where you left off. Therefore,
do not stop tasks early due to token budget concerns. As you approach your token
budget limit, save your current progress and state to memory before the context
window refreshes. Always be as persistent and autonomous as possible and complete
tasks fully, even if the end of your budget is approaching.
```

### Long-Horizon Reasoning

#### Multi-Context Window Workflows

1. **First window**: Set up framework (write tests, create setup scripts)
2. **Future windows**: Iterate on todo-list

**State management patterns**:

```json
// Structured state (tests.json)
{
  "tests": [
    { "id": 1, "name": "authentication_flow", "status": "passing" },
    { "id": 2, "name": "user_management", "status": "failing" },
    { "id": 3, "name": "api_endpoints", "status": "not_started" }
  ]
}
```

```
// Progress notes (progress.txt)
Session 3 progress:
- Fixed authentication token validation
- Updated user model to handle edge cases
- Next: investigate user_management test failures
```

**Starting fresh prompt**:

```
Call pwd; you can only read and write files in this directory.
Review progress.txt, tests.json, and the git logs.
Manually run through a fundamental integration test before moving
on to implementing new features.
```

**Encourage complete context usage**:

```
This is a very long task, so it may be beneficial to plan out your work clearly.
It's encouraged to spend your entire output context working on the task -
just make sure you don't run out of context with significant uncommitted work.
Continue working systematically until you have completed this task.
```

### Output Formatting Control

#### Minimize Markdown

```xml
<avoid_excessive_markdown_and_bullet_points>
When writing reports, documents, technical explanations, analyses, or any
long-form content, write in clear, flowing prose using complete paragraphs
and sentences. Use standard paragraph breaks for organization and reserve
markdown primarily for `inline code`, code blocks, and simple headings.

DO NOT use ordered lists (1. ...) or unordered lists (*) unless:
a) you're presenting truly discrete items where a list format is best, or
b) the user explicitly requests a list or ranking

Instead of listing items with bullets or numbers, incorporate them naturally
into sentences. NEVER output a series of overly short bullet points.
</avoid_excessive_markdown_and_bullet_points>
```

#### Use XML Format Indicators

```
Write the prose sections of your response in <smoothly_flowing_prose_paragraphs> tags.
```

### Avoiding Over-Engineering (Opus 4.5)

Claude Opus 4.5 tends to overengineer. Use explicit prompting:

```xml
<avoid_overengineering>
Avoid over-engineering. Only make changes that are directly requested
or clearly necessary. Keep solutions simple and focused.

Don't add features, refactor code, or make "improvements" beyond what was asked.
A bug fix doesn't need surrounding code cleaned up. A simple feature doesn't
need extra configurability.

Don't add error handling, fallbacks, or validation for scenarios that can't happen.
Trust internal code and framework guarantees. Only validate at system boundaries
(user input, external APIs). Don't use backwards-compatibility shims when you can
just change the code.

Don't create helpers, utilities, or abstractions for one-time operations.
Don't design for hypothetical future requirements. The right amount of complexity
is the minimum needed for the current task. Reuse existing abstractions where
possible and follow the DRY principle.
</avoid_overengineering>
```

### Code Exploration

Encourage thorough code reading:

```xml
<investigate_before_answering>
ALWAYS read and understand relevant files before proposing code edits.
Do not speculate about code you have not inspected. If the user references
a specific file/path, you MUST open and inspect it before explaining or
proposing fixes. Be rigorous and persistent in searching code for key facts.
Thoroughly review the style, conventions, and abstractions of the codebase
before implementing new features or abstractions.
</investigate_before_answering>
```

### Minimizing Hallucinations

```xml
<grounded_responses>
Never speculate about code you have not opened. If the user references a
specific file, you MUST read the file before answering. Make sure to
investigate and read relevant files BEFORE answering questions about the
codebase. Never make any claims about code before investigating unless you
are certain of the correct answer - give grounded and hallucination-free answers.
</grounded_responses>
```

### Avoid Hard-Coding

```
Please write a high-quality, general-purpose solution using the standard tools
available. Do not create helper scripts or workarounds to accomplish the task
more efficiently. Implement a solution that works correctly for all valid inputs,
not just the test cases. Do not hard-code values or create solutions that only
work for specific test inputs. Instead, implement the actual logic that solves
the problem generally.

Focus on understanding the problem requirements and implementing the correct
algorithm. Tests are there to verify correctness, not to define the solution.

If the task is unreasonable or infeasible, or if any of the tests are incorrect,
please inform me rather than working around them.
```

### Thinking Sensitivity

When extended thinking is **disabled**, Claude Opus 4.5 is sensitive to "think" and variants.

Replace with alternatives:

- "think" → "consider", "evaluate", "assess"
- "thinking" → "reasoning", "analyzing"

### Leveraging Extended Thinking

Guide Claude's thinking for better results:

```
After receiving tool results, carefully reflect on their quality and determine
optimal next steps before proceeding. Use your thinking to plan and iterate
based on this new information, and then take the best next action.
```

### Research Tasks

For complex research:

```
Search for this information in a structured way. As you gather data, develop
several competing hypotheses. Track your confidence levels in your progress
notes to improve calibration. Regularly self-critique your approach and plan.
Update a hypothesis tree or research notes file to persist information and
provide transparency. Break down this complex research task systematically.
```

### Subagent Orchestration

Claude 4.5 recognizes when to delegate to subagents proactively.

**Conservative subagent usage**:

```
Only delegate to subagents when the task clearly benefits from a
separate agent with a new context window.
```

### Model Identity

```
The assistant is Claude, created by Anthropic. The current model is Claude Sonnet 4.5.
```

For model strings:

```
When an LLM is needed, please default to Claude Sonnet 4.5 unless the user
requests otherwise. The exact model string for Claude Sonnet 4.5 is
claude-sonnet-4-5-20250929.
```

### DSM-Specific Prompt Patterns

**FCP analysis prompt**:

```xml
<dsm_fcp_analysis>
When analyzing FCP (Failover Control Protocol) behavior:
1. ALWAYS check rate limit status before assuming failover triggered
2. Review the decision log in .cache/fcp/ for actual failover events
3. Verify symbol format matches exchange requirements (BTCUSDT vs BTC-USDT)
4. Check timestamp handling (DSM uses UTC milliseconds exclusively)
5. Report confidence levels in diagnostic conclusions
</dsm_fcp_analysis>
```

**DataFrame validation prompt**:

```xml
<dsm_dataframe_validation>
When validating DataFrames from DSM:
1. Check for required columns: open_time, open, high, low, close, volume
2. Verify open_time is UTC timezone-aware (not naive)
3. Ensure no gaps in time series data
4. Validate OHLCV relationships (high >= max(open, close), low <= min(open, close))
5. Use Polars for DataFrame operations (not pandas)
</dsm_dataframe_validation>
```

**Symbol format prompt**:

```xml
<dsm_symbol_formats>
DSM uses exchange-specific symbol formats:
- Binance: {base}{quote} (e.g., BTCUSDT)
- OKX: {base}-{quote} (e.g., BTC-USDT)
- Bybit: {base}{quote} (e.g., BTCUSDT)

ALWAYS verify the target exchange before using a symbol format.
Never assume cross-exchange compatibility without validation.
</dsm_symbol_formats>
```

**Error handling prompt**:

```xml
<dsm_error_handling>
DSM error handling requirements:
- NEVER use bare except: clauses
- NEVER use except Exception: without logging
- NEVER suppress errors with pass
- ALWAYS use check=True for subprocess calls
- ALWAYS set timeout for HTTP requests
- ALWAYS handle rate limit errors with exponential backoff
</dsm_error_handling>
```

### XML Tag Best Practices

Claude 4.x responds well to XML-formatted instructions:

```xml
<!-- Group related instructions -->
<instructions>
  <context>Background information here</context>
  <task>What to accomplish</task>
  <constraints>Limitations and requirements</constraints>
  <output_format>Expected response structure</output_format>
</instructions>
```

Use descriptive tag names that convey purpose.

### Migration from Earlier Models

1. **Be specific** - Describe exactly what you want in output
2. **Use modifiers** - "Include as many relevant features as possible"
3. **Request features explicitly** - Animations, interactivity won't be inferred
4. **Dial back aggressive language** - "CRITICAL: You MUST" → "Use this when..."
## Batch Processing and Rate Limits Reference

Reference for Claude API batch processing, rate limits, and cost optimization strategies.

### Rate Limit Types

| Limit Type | Measurement       | Purpose            |
| ---------- | ----------------- | ------------------ |
| RPM        | Requests/minute   | API call frequency |
| ITPM       | Input tokens/min  | Input throughput   |
| OTPM       | Output tokens/min | Output throughput  |
| Spend      | $/month           | Monthly cost cap   |

### Usage Tiers

| Tier | Credit Purchase | Max Credit | RPM (Sonnet) | ITPM (Sonnet) | OTPM (Sonnet) |
| ---- | --------------- | ---------- | ------------ | ------------- | ------------- |
| 1    | $5              | $100       | 50           | 30,000        | 8,000         |
| 2    | $40             | $500       | 1,000        | 450,000       | 90,000        |
| 3    | $200            | $1,000     | 2,000        | 800,000       | 160,000       |
| 4    | $400            | $5,000     | 4,000        | 2,000,000     | 400,000       |

**Notes**:

- Sonnet 4.x limits apply to combined Sonnet 4 and Sonnet 4.5 traffic
- Opus 4.x limits apply to combined Opus 4, 4.1, and 4.5 traffic
- Advance tiers immediately upon reaching credit threshold

### Token Bucket Algorithm

Rate limits use the token bucket algorithm:

- Capacity continuously replenishes up to maximum
- No fixed reset intervals
- Short bursts can exceed rate limit

**Example**: 60 RPM may enforce as 1 request/second. Burst of 10 requests at once will hit limit.

### Cache-Aware ITPM

**Only uncached input tokens count toward ITPM** for most models:

| Token Type                    | Counts Toward ITPM |
| ----------------------------- | ------------------ |
| `input_tokens`                | Yes                |
| `cache_creation_input_tokens` | Yes                |
| `cache_read_input_tokens`     | No (most models)   |

**Effective throughput calculation**:

```
With 2,000,000 ITPM limit and 80% cache hit rate:
= 2M uncached + 8M cached = 10M effective tokens/minute
```

### 429 Error Handling

Rate limit exceeded returns 429 with:

- Error description identifying which limit exceeded
- `retry-after` header with seconds to wait

**Retry strategy**:

```python
import time
import anthropic
from anthropic import RateLimitError

client = anthropic.Anthropic()

def call_with_retry(prompt, max_retries=5):
    for attempt in range(max_retries):
        try:
            return client.messages.create(
                model="claude-sonnet-4-5",
                max_tokens=1024,
                messages=[{"role": "user", "content": prompt}]
            )
        except RateLimitError as e:
            if attempt == max_retries - 1:
                raise
            # Exponential backoff with jitter
            wait = (2 ** attempt) + random.uniform(0, 1)
            time.sleep(wait)
```

### Response Headers

| Header                                   | Description                        |
| ---------------------------------------- | ---------------------------------- |
| `retry-after`                            | Seconds to wait before retry       |
| `anthropic-ratelimit-requests-limit`     | Max requests per period            |
| `anthropic-ratelimit-requests-remaining` | Requests remaining                 |
| `anthropic-ratelimit-tokens-limit`       | Max tokens per period              |
| `anthropic-ratelimit-tokens-remaining`   | Tokens remaining (rounded to 1000) |
| `anthropic-ratelimit-input-tokens-*`     | Input-specific limits              |
| `anthropic-ratelimit-output-tokens-*`    | Output-specific limits             |

### Message Batches API

Process large volumes asynchronously with 50% discount on all tokens.

**Batch limits by tier**:

| Tier | RPM   | Max Requests in Queue | Max per Batch |
| ---- | ----- | --------------------- | ------------- |
| 1    | 50    | 100,000               | 100,000       |
| 2    | 1,000 | 200,000               | 100,000       |
| 3    | 2,000 | 300,000               | 100,000       |
| 4    | 4,000 | 500,000               | 100,000       |

**Usage**:

```python
import anthropic

client = anthropic.Anthropic()

# Create batch
batch = client.messages.batches.create(
    requests=[
        {
            "custom_id": f"request-{i}",
            "params": {
                "model": "claude-sonnet-4-5",
                "max_tokens": 1024,
                "messages": [{"role": "user", "content": prompt}]
            }
        }
        for i, prompt in enumerate(prompts)
    ]
)

# Check status
status = client.messages.batches.retrieve(batch.id)

# Get results when complete
if status.processing_status == "ended":
    results = client.messages.batches.results(batch.id)
```

**Key characteristics**:

- Results within 24 hours
- 50% discount on input and output tokens
- Can combine with prompt caching for additional savings
- Maximum 100,000 requests per batch

### Long Context Rate Limits

For 1M token context window (Sonnet 4.x only, Tier 4+):

| Threshold | ITPM      | OTPM    |
| --------- | --------- | ------- |
| >200K     | 1,000,000 | 200,000 |

**Note**: Entire request charged at long context rate if >200K tokens.

### Pricing Summary

| Model      | Input ($/M) | Output ($/M) | Batch Input | Batch Output |
| ---------- | ----------- | ------------ | ----------- | ------------ |
| Haiku 4.5  | $1          | $5           | $0.50       | $2.50        |
| Sonnet 4.5 | $3          | $15          | $1.50       | $7.50        |
| Opus 4.5   | $5          | $25          | $2.50       | $12.50       |

**Prompt caching pricing**:

- Cache write: 1.25x base input price
- Cache read: 0.1x base input price
- TTL: 5 minutes (1 hour available)

### Cost Optimization Strategies

#### 1. Prompt Caching

```python
response = client.messages.create(
    model="claude-sonnet-4-5",
    max_tokens=1024,
    system=[
        {
            "type": "text",
            "text": large_system_prompt,
            "cache_control": {"type": "ephemeral"}
        }
    ],
    messages=[{"role": "user", "content": user_query}]
)
```

**Savings**: 90% on repeated content.

#### 2. Batch Processing

- Use for non-urgent workloads
- 50% savings on all tokens
- Combine with caching for maximum savings

#### 3. Model Selection

| Use Case               | Recommended Model | Rationale           |
| ---------------------- | ----------------- | ------------------- |
| Simple tasks           | Haiku 4.5         | Lowest cost         |
| Balanced workloads     | Sonnet 4.5        | Cost/capability mix |
| Complex reasoning      | Opus 4.5          | Best capability     |
| High-volume processing | Haiku 4.5 + Batch | Cost optimization   |

#### 4. Token Optimization

- Set appropriate `max_tokens` (reduces OTPM estimation)
- Use prompt caching for repeated system prompts
- Minimize input by summarizing context

### DSM-Specific Rate Limit Patterns

**FCP with rate limit awareness**:

```python
from datasourcemanager import DataSourceManager

# DSM handles rate limiting internally via FCP
dsm = DataSourceManager(
    failover_enabled=True,
    rate_limit_cooldown=60,  # seconds
    max_retries=3
)

# FCP automatically switches sources on rate limit
data = dsm.get_ohlcv(
    symbol="BTCUSDT",
    interval="1h",
    limit=1000
)
```

**Batch data fetching**:

```python
# For large historical data requests, use batch mode
symbols = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]

# Process in batches to avoid rate limits
for batch in chunked(symbols, 10):
    data = dsm.get_multi_symbol_ohlcv(
        symbols=batch,
        interval="1h",
        limit=1000
    )
    time.sleep(1)  # Rate limit buffer
```

**Claude API integration with DSM**:

```python
import anthropic
from datasourcemanager import DataSourceManager

client = anthropic.Anthropic()
dsm = DataSourceManager()

# Cache DSM documentation as system prompt
dsm_docs = dsm.get_documentation()

response = client.messages.create(
    model="claude-sonnet-4-5",
    max_tokens=4096,
    system=[
        {
            "type": "text",
            "text": dsm_docs,
            "cache_control": {"type": "ephemeral"}
        }
    ],
    messages=[{"role": "user", "content": "Analyze BTCUSDT patterns"}]
)
```

### Monitoring Rate Limits

**Console monitoring**:

- View usage on Claude Console Usage page
- Rate limit charts show:
  - Hourly max uncached input tokens/minute
  - Current rate limit
  - Cache rate percentage

**Programmatic monitoring**:

```python
# Check headers after each request
response = client.messages.create(...)

remaining = response.headers.get("anthropic-ratelimit-tokens-remaining")
reset = response.headers.get("anthropic-ratelimit-tokens-reset")

if int(remaining) < 10000:
    print(f"Low tokens remaining, reset at {reset}")
```

### Workspace Rate Limits

Organizations can set per-workspace limits:

- Protects workspaces from overuse by others
- Cannot exceed organization limit
- Default workspace cannot have custom limits

**Example**: Organization has 40,000 ITPM, limit workspace A to 30,000 ITPM, leaving 10,000+ for other workspaces.
## MCP Server Development and Configuration Reference

Reference for Model Context Protocol (MCP) server development, configuration, and integration with Claude Code.

### Overview

MCP (Model Context Protocol) is an open source standard for AI-tool integrations. MCP servers give Claude Code access to external tools, databases, and APIs.

**Capabilities**:

- Query databases with natural language
- Integrate with issue trackers (GitHub, Jira)
- Monitor errors (Sentry)
- Automate browser testing (Playwright)
- Access cloud services (AWS, GCP)

### Transport Types

| Transport | Description                     | Use Case                    |
| --------- | ------------------------------- | --------------------------- |
| HTTP      | Recommended for remote servers  | Cloud services, APIs        |
| SSE       | Server-Sent Events (deprecated) | Legacy real-time servers    |
| stdio     | Local process communication     | Local tools, custom scripts |

### Installing MCP Servers

#### HTTP Server (Recommended)

```bash
# Basic syntax
claude mcp add --transport http <name> <url>

# Example: Notion
claude mcp add --transport http notion https://mcp.notion.com/mcp

# With authentication header
claude mcp add --transport http secure-api https://api.example.com/mcp \
  --header "Authorization: Bearer your-token"
```

#### SSE Server (Deprecated)

```bash
claude mcp add --transport sse asana https://mcp.asana.com/sse
```

#### stdio Server (Local)

```bash
# Basic syntax
claude mcp add [options] <name> -- <command> [args...]

# Example: Airtable
claude mcp add --transport stdio --env AIRTABLE_API_KEY=YOUR_KEY airtable \
  -- npx -y airtable-mcp-server
```

**Important**: All options must come before the server name. `--` separates Claude flags from server command.

### Managing Servers

```bash
# List all servers
claude mcp list

# Get server details
claude mcp get github

# Remove server
claude mcp remove github

# Check status (within Claude Code)
/mcp

# Import from Claude Desktop (macOS/WSL only)
claude mcp add-from-claude-desktop
```

### MCP Server Scopes

| Scope   | Storage Location | Accessibility              | Use Case                        |
| ------- | ---------------- | -------------------------- | ------------------------------- |
| local   | `~/.claude.json` | Current project only       | Personal, sensitive credentials |
| project | `.mcp.json`      | Shared via version control | Team collaboration              |
| user    | `~/.claude.json` | All projects               | Personal utilities              |

```bash
# Add with specific scope
claude mcp add --transport http stripe --scope local https://mcp.stripe.com
claude mcp add --transport http paypal --scope project https://mcp.paypal.com/mcp
claude mcp add --transport http hubspot --scope user https://mcp.hubspot.com/anthropic
```

### Project Scope Configuration

Project-scoped servers stored in `.mcp.json`:

```json
{
  "mcpServers": {
    "shared-server": {
      "command": "/path/to/server",
      "args": [],
      "env": {}
    }
  }
}
```

**Environment variable expansion**:

```json
{
  "mcpServers": {
    "api-server": {
      "type": "http",
      "url": "${API_BASE_URL:-https://api.example.com}/mcp",
      "headers": {
        "Authorization": "Bearer ${API_KEY}"
      }
    }
  }
}
```

Supported syntax:

- `${VAR}` - Variable value
- `${VAR:-default}` - Value with default fallback

### Authentication

OAuth 2.0 authentication for cloud services:

```bash
# Add server requiring auth
claude mcp add --transport http sentry https://mcp.sentry.dev/mcp

# Authenticate within Claude Code
/mcp
# Select "Authenticate" and follow browser flow
```

### MCP Tool Search

When many MCP servers consume >10% of context window, Tool Search activates automatically:

| ENABLE_TOOL_SEARCH | Behavior                                    |
| ------------------ | ------------------------------------------- |
| `auto` (default)   | Activates when MCP tools exceed 10% context |
| `auto:<N>`         | Custom threshold (e.g., `auto:5` for 5%)    |
| `true`             | Always enabled                              |
| `false`            | Disabled, all tools loaded upfront          |

```bash
# Custom threshold
ENABLE_TOOL_SEARCH=auto:5 claude

# Disable entirely
ENABLE_TOOL_SEARCH=false claude
```

### MCP Output Limits

| Setting               | Default | Description                     |
| --------------------- | ------- | ------------------------------- |
| Warning threshold     | 10,000  | Tokens before warning displayed |
| MAX_MCP_OUTPUT_TOKENS | 25,000  | Maximum allowed tokens          |

```bash
# Increase limit for large outputs
export MAX_MCP_OUTPUT_TOKENS=50000
claude
```

### Using Claude Code as MCP Server

Claude Code can expose its tools via MCP:

```bash
# Start as MCP server
claude mcp serve
```

**Claude Desktop configuration** (`claude_desktop_config.json`):

```json
{
  "mcpServers": {
    "claude-code": {
      "type": "stdio",
      "command": "claude",
      "args": ["mcp", "serve"],
      "env": {}
    }
  }
}
```

**Exposed tools**: View, Edit, LS, Bash, Read, Write, GrepTool, GlobTool, Replace.

### Plugin-Provided MCP Servers

Plugins can bundle MCP servers via `.mcp.json` at plugin root:

```json
{
  "database-tools": {
    "command": "${CLAUDE_PLUGIN_ROOT}/servers/db-server",
    "args": ["--config", "${CLAUDE_PLUGIN_ROOT}/config.json"],
    "env": {
      "DB_URL": "${DB_URL}"
    }
  }
}
```

Or inline in `plugin.json`:

```json
{
  "name": "my-plugin",
  "mcpServers": {
    "plugin-api": {
      "command": "${CLAUDE_PLUGIN_ROOT}/servers/api-server",
      "args": ["--port", "8080"]
    }
  }
}
```

### MCP Resources as @ Mentions

Reference MCP resources in prompts:

```
> Can you analyze @github:issue://123 and suggest a fix?
> Compare @postgres:schema://users with @docs:file://database/user-model
```

### MCP Prompts as Commands

MCP servers expose prompts as slash commands:

```
> /mcp__github__list_prs
> /mcp__github__pr_review 456
> /mcp__jira__create_issue "Bug in login flow" high
```

### Managed MCP Configuration (Enterprise)

**Exclusive control** (`managed-mcp.json`):

Location:

- macOS: `/Library/Application Support/ClaudeCode/managed-mcp.json`
- Linux/WSL: `/etc/claude-code/managed-mcp.json`
- Windows: `C:\Program Files\ClaudeCode\managed-mcp.json`

```json
{
  "mcpServers": {
    "github": {
      "type": "http",
      "url": "https://api.githubcopilot.com/mcp/"
    },
    "company-internal": {
      "type": "stdio",
      "command": "/usr/local/bin/company-mcp-server",
      "args": ["--config", "/etc/company/mcp-config.json"]
    }
  }
}
```

**Policy-based control** (allowlists/denylists):

```json
{
  "allowedMcpServers": [
    { "serverName": "github" },
    {
      "serverCommand": ["npx", "-y", "@modelcontextprotocol/server-filesystem"]
    },
    { "serverUrl": "https://mcp.company.com/*" }
  ],
  "deniedMcpServers": [
    { "serverName": "dangerous-server" },
    { "serverUrl": "https://*.untrusted.com/*" }
  ]
}
```

### Building Custom MCP Servers

**Python with MCP SDK**:

```python
from mcp import Server, Tool
from mcp.types import TextContent

server = Server("my-server")

@server.tool()
async def get_weather(location: str) -> list[TextContent]:
    """Get weather for a location."""
    # Fetch weather data
    data = await fetch_weather(location)
    return [TextContent(type="text", text=f"Weather in {location}: {data}")]

@server.tool()
async def search_database(query: str) -> list[TextContent]:
    """Search the database."""
    results = await db.search(query)
    return [TextContent(type="text", text=json.dumps(results))]

if __name__ == "__main__":
    server.run()
```

**TypeScript with MCP SDK**:

```typescript
import { Server } from "@modelcontextprotocol/sdk/server";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio";

const server = new Server(
  {
    name: "my-server",
    version: "1.0.0",
  },
  {
    capabilities: {
      tools: {},
    },
  },
);

server.setRequestHandler("tools/list", async () => ({
  tools: [
    {
      name: "get_weather",
      description: "Get weather for a location",
      inputSchema: {
        type: "object",
        properties: {
          location: { type: "string", description: "City name" },
        },
        required: ["location"],
      },
    },
  ],
}));

server.setRequestHandler("tools/call", async (request) => {
  if (request.params.name === "get_weather") {
    const { location } = request.params.arguments;
    const weather = await fetchWeather(location);
    return { content: [{ type: "text", text: JSON.stringify(weather) }] };
  }
});

const transport = new StdioServerTransport();
server.connect(transport);
```

### Server Instructions for Tool Search

When building MCP servers, add clear server instructions:

```python
server = Server(
    "dsm-tools",
    instructions="""
    DSM Tools Server - Use for data source management tasks:
    - Fetch OHLCV data from exchanges
    - Validate DataFrame integrity
    - Debug FCP failover behavior
    - Check symbol format compatibility
    """
)
```

### DSM-Specific MCP Patterns

**DSM MCP Server Example**:

```python
from mcp import Server, Tool
from datasourcemanager import DataSourceManager

server = Server("dsm-mcp")
dsm = DataSourceManager()

@server.tool()
async def get_ohlcv(
    symbol: str,
    interval: str = "1h",
    limit: int = 100
) -> list[TextContent]:
    """Fetch OHLCV data for a symbol."""
    try:
        df = dsm.get_ohlcv(symbol=symbol, interval=interval, limit=limit)
        return [TextContent(
            type="text",
            text=df.to_json(orient="records")
        )]
    except Exception as e:
        return [TextContent(type="text", text=f"Error: {e}")]

@server.tool()
async def check_fcp_status(symbol: str) -> list[TextContent]:
    """Check FCP failover status for a symbol."""
    status = dsm.get_fcp_status(symbol)
    return [TextContent(type="text", text=json.dumps(status))]

@server.tool()
async def validate_dataframe(data: str) -> list[TextContent]:
    """Validate OHLCV DataFrame structure."""
    import polars as pl
    df = pl.read_json(data)

    issues = []
    required_cols = ["open_time", "open", "high", "low", "close", "volume"]

    for col in required_cols:
        if col not in df.columns:
            issues.append(f"Missing column: {col}")

    if "open_time" in df.columns and df["open_time"].dtype != pl.Datetime:
        issues.append("open_time should be datetime")

    return [TextContent(
        type="text",
        text=json.dumps({"valid": len(issues) == 0, "issues": issues})
    )]
```

**Project MCP configuration** (`.mcp.json`):

```json
{
  "mcpServers": {
    "dsm": {
      "command": "uv",
      "args": ["run", "python", "-m", "datasourcemanager.mcp_server"],
      "env": {
        "DSM_CACHE_DIR": "${HOME}/.cache/dsm"
      }
    }
  }
}
```

### Popular MCP Servers

| Server     | URL/Command                                       | Purpose              |
| ---------- | ------------------------------------------------- | -------------------- |
| GitHub     | `https://api.githubcopilot.com/mcp/`              | PRs, issues, CI/CD   |
| Sentry     | `https://mcp.sentry.dev/mcp`                      | Error monitoring     |
| Notion     | `https://mcp.notion.com/mcp`                      | Documentation, notes |
| PostgreSQL | `npx -y @bytebase/dbhub --dsn "postgresql://..."` | Database queries     |
| Playwright | `npx -y @playwright/mcp@latest`                   | Browser automation   |
| Filesystem | `npx -y @modelcontextprotocol/server-filesystem`  | File operations      |

### Windows-Specific Notes

On native Windows (not WSL), use `cmd /c` wrapper for npx:

```bash
claude mcp add --transport stdio my-server -- cmd /c npx -y @some/package
```

### Dynamic Tool Updates

Claude Code supports MCP `list_changed` notifications for dynamic capability updates without reconnection.

### Troubleshooting

**Common issues**:

| Issue                | Solution                                     |
| -------------------- | -------------------------------------------- |
| Connection closed    | Check command path, use `cmd /c` on Windows  |
| spawn ENOENT         | Use full path to executable (`which claude`) |
| Timeout              | Set `MCP_TIMEOUT=10000` environment variable |
| Large output warning | Increase `MAX_MCP_OUTPUT_TOKENS`             |
| OAuth not working    | Use `/mcp` to authenticate, check browser    |

**Debugging**:

```bash
# Enable MCP debug mode
claude --mcp-debug

# Check server status
/mcp
```
## Claude Agent SDK Reference

Reference for building AI agents with the Claude Agent SDK in Python and TypeScript.

### Overview

The Claude Agent SDK provides the same tools, agent loop, and context management that power Claude Code, programmable in Python and TypeScript. Agents can autonomously read files, run commands, search the web, edit code, and more.

### Installation

**Install Claude Code runtime**:

```bash
# macOS/Linux/WSL
curl -fsSL https://claude.ai/install.sh | bash

# Homebrew
brew install --cask claude-code

# Windows
winget install Anthropic.ClaudeCode
```

**Install SDK**:

```bash
# TypeScript
npm install @anthropic-ai/claude-agent-sdk

# Python
pip install claude-agent-sdk
```

**Set API key**:

```bash
export ANTHROPIC_API_KEY=your-api-key
```

### Basic Usage

**Python**:

```python
import asyncio
from claude_agent_sdk import query, ClaudeAgentOptions

async def main():
    async for message in query(
        prompt="Find and fix the bug in auth.py",
        options=ClaudeAgentOptions(allowed_tools=["Read", "Edit", "Bash"])
    ):
        print(message)

asyncio.run(main())
```

**TypeScript**:

```typescript
import { query } from "@anthropic-ai/claude-agent-sdk";

for await (const message of query({
  prompt: "Find and fix the bug in auth.py",
  options: { allowedTools: ["Read", "Edit", "Bash"] },
})) {
  console.log(message);
}
```

### Built-in Tools

| Tool              | Description                                      |
| ----------------- | ------------------------------------------------ |
| `Read`            | Read any file in working directory               |
| `Write`           | Create new files                                 |
| `Edit`            | Make precise edits to existing files             |
| `Bash`            | Run terminal commands, scripts, git operations   |
| `Glob`            | Find files by pattern (`**/*.ts`, `src/**/*.py`) |
| `Grep`            | Search file contents with regex                  |
| `WebSearch`       | Search the web for current information           |
| `WebFetch`        | Fetch and parse web page content                 |
| `AskUserQuestion` | Ask user clarifying questions                    |
| `Task`            | Spawn subagents                                  |

### Subagents

Spawn specialized agents for focused subtasks with isolated context windows:

**Python**:

```python
from claude_agent_sdk import query, ClaudeAgentOptions, AgentDefinition

async def main():
    async for message in query(
        prompt="Use the code-reviewer agent to review this codebase",
        options=ClaudeAgentOptions(
            allowed_tools=["Read", "Glob", "Grep", "Task"],
            agents={
                "code-reviewer": AgentDefinition(
                    description="Expert code reviewer for quality and security reviews.",
                    prompt="Analyze code quality and suggest improvements.",
                    tools=["Read", "Glob", "Grep"]
                )
            }
        )
    ):
        if hasattr(message, "result"):
            print(message.result)

asyncio.run(main())
```

**TypeScript**:

```typescript
import { query } from "@anthropic-ai/claude-agent-sdk";

for await (const message of query({
  prompt: "Use the code-reviewer agent to review this codebase",
  options: {
    allowedTools: ["Read", "Glob", "Grep", "Task"],
    agents: {
      "code-reviewer": {
        description: "Expert code reviewer for quality and security reviews.",
        prompt: "Analyze code quality and suggest improvements.",
        tools: ["Read", "Glob", "Grep"],
      },
    },
  },
})) {
  if ("result" in message) console.log(message.result);
}
```

**Subagent benefits**:

- **Parallelization**: Multiple subagents work on different tasks simultaneously
- **Context isolation**: Subagents use own context windows, return only relevant info
- **Specialization**: Define focused agents with specific capabilities

### Hooks

Run custom code at key points in agent lifecycle:

| Hook               | Trigger                 |
| ------------------ | ----------------------- |
| `PreToolUse`       | Before tool execution   |
| `PostToolUse`      | After tool execution    |
| `Stop`             | Agent session ends      |
| `SessionStart`     | Agent session begins    |
| `SessionEnd`       | Agent session completes |
| `UserPromptSubmit` | User submits prompt     |

**Python example** (audit logging):

```python
from datetime import datetime
from claude_agent_sdk import query, ClaudeAgentOptions, HookMatcher

async def log_file_change(input_data, tool_use_id, context):
    file_path = input_data.get('tool_input', {}).get('file_path', 'unknown')
    with open('./audit.log', 'a') as f:
        f.write(f"{datetime.now()}: modified {file_path}\n")
    return {}

async def main():
    async for message in query(
        prompt="Refactor utils.py to improve readability",
        options=ClaudeAgentOptions(
            permission_mode="acceptEdits",
            hooks={
                "PostToolUse": [HookMatcher(matcher="Edit|Write", hooks=[log_file_change])]
            }
        )
    ):
        if hasattr(message, "result"):
            print(message.result)

asyncio.run(main())
```

**TypeScript example**:

```typescript
import { query, HookCallback } from "@anthropic-ai/claude-agent-sdk";
import { appendFileSync } from "fs";

const logFileChange: HookCallback = async (input) => {
  const filePath = (input as any).tool_input?.file_path ?? "unknown";
  appendFileSync(
    "./audit.log",
    `${new Date().toISOString()}: modified ${filePath}\n`,
  );
  return {};
};

for await (const message of query({
  prompt: "Refactor utils.py to improve readability",
  options: {
    permissionMode: "acceptEdits",
    hooks: {
      PostToolUse: [{ matcher: "Edit|Write", hooks: [logFileChange] }],
    },
  },
})) {
  if ("result" in message) console.log(message.result);
}
```

### MCP Integration

Connect to external systems via Model Context Protocol:

**Python**:

```python
from claude_agent_sdk import query, ClaudeAgentOptions

async def main():
    async for message in query(
        prompt="Open example.com and describe what you see",
        options=ClaudeAgentOptions(
            mcp_servers={
                "playwright": {"command": "npx", "args": ["@playwright/mcp@latest"]}
            }
        )
    ):
        if hasattr(message, "result"):
            print(message.result)

asyncio.run(main())
```

**TypeScript**:

```typescript
import { query } from "@anthropic-ai/claude-agent-sdk";

for await (const message of query({
  prompt: "Open example.com and describe what you see",
  options: {
    mcpServers: {
      playwright: { command: "npx", args: ["@playwright/mcp@latest"] },
    },
  },
})) {
  if ("result" in message) console.log(message.result);
}
```

### Permission Modes

| Mode                | Description                                   |
| ------------------- | --------------------------------------------- |
| `bypassPermissions` | Auto-approve all tools (use in isolated envs) |
| `acceptEdits`       | Auto-approve file edits                       |
| `default`           | Require approval for sensitive operations     |

**Read-only agent example**:

```python
async for message in query(
    prompt="Review this code for best practices",
    options=ClaudeAgentOptions(
        allowed_tools=["Read", "Glob", "Grep"],
        permission_mode="bypassPermissions"
    )
):
    if hasattr(message, "result"):
        print(message.result)
```

### Sessions

Maintain context across multiple exchanges:

**Python**:

```python
from claude_agent_sdk import query, ClaudeAgentOptions

async def main():
    session_id = None

    # First query: capture session ID
    async for message in query(
        prompt="Read the authentication module",
        options=ClaudeAgentOptions(allowed_tools=["Read", "Glob"])
    ):
        if hasattr(message, 'subtype') and message.subtype == 'init':
            session_id = message.session_id

    # Resume with full context
    async for message in query(
        prompt="Now find all places that call it",
        options=ClaudeAgentOptions(resume=session_id)
    ):
        if hasattr(message, "result"):
            print(message.result)

asyncio.run(main())
```

**TypeScript**:

```typescript
import { query } from "@anthropic-ai/claude-agent-sdk";

let sessionId: string | undefined;

// First query: capture session ID
for await (const message of query({
  prompt: "Read the authentication module",
  options: { allowedTools: ["Read", "Glob"] },
})) {
  if (message.type === "system" && message.subtype === "init") {
    sessionId = message.session_id;
  }
}

// Resume with full context
for await (const message of query({
  prompt: "Now find all places that call it",
  options: { resume: sessionId },
})) {
  if ("result" in message) console.log(message.result);
}
```

### Authentication Providers

| Provider          | Environment Variable        |
| ----------------- | --------------------------- |
| Anthropic API     | `ANTHROPIC_API_KEY`         |
| Amazon Bedrock    | `CLAUDE_CODE_USE_BEDROCK=1` |
| Google Vertex AI  | `CLAUDE_CODE_USE_VERTEX=1`  |
| Microsoft Foundry | `CLAUDE_CODE_USE_FOUNDRY=1` |

### Claude Code Features

Enable filesystem-based configuration:

```python
options=ClaudeAgentOptions(
    setting_sources=["project"]  # Load .claude/ configuration
)
```

```typescript
options: {
  settingSources: ["project"]; // Load .claude/ configuration
}
```

| Feature        | Location                  | Description                      |
| -------------- | ------------------------- | -------------------------------- |
| Skills         | `.claude/skills/SKILL.md` | Specialized capabilities         |
| Slash commands | `.claude/commands/*.md`   | Custom commands                  |
| Memory         | `CLAUDE.md`               | Project context and instructions |
| Plugins        | Via `plugins` option      | Custom extensions                |

### Agent SDK vs Client SDK

| Aspect         | Client SDK              | Agent SDK                   |
| -------------- | ----------------------- | --------------------------- |
| Tool execution | You implement tool loop | Claude handles autonomously |
| Setup          | More control, more code | Built-in tools, less code   |
| Use case       | Custom integrations     | Production automation       |

```python
# Client SDK: You implement tool loop
response = client.messages.create(...)
while response.stop_reason == "tool_use":
    result = your_tool_executor(response.tool_use)
    response = client.messages.create(tool_result=result, ...)

# Agent SDK: Claude handles tools autonomously
async for message in query(prompt="Fix the bug in auth.py"):
    print(message)
```

### Use Case Comparison

| Use Case              | Best Choice |
| --------------------- | ----------- |
| Interactive dev       | CLI         |
| CI/CD pipelines       | SDK         |
| Custom applications   | SDK         |
| One-off tasks         | CLI         |
| Production automation | SDK         |

### DSM-Specific Agent SDK Patterns

**DSM Code Reviewer Agent**:

```python
from claude_agent_sdk import query, ClaudeAgentOptions, AgentDefinition

async def review_dsm_code():
    async for message in query(
        prompt="Review the DataSourceManager implementation for best practices",
        options=ClaudeAgentOptions(
            allowed_tools=["Read", "Glob", "Grep", "Task"],
            agents={
                "dsm-reviewer": AgentDefinition(
                    description="DSM code reviewer checking FCP, caching, and error handling.",
                    prompt="""Review DSM code for:
                    1. FCP failover handling
                    2. Proper timestamp handling (UTC)
                    3. DataFrame validation patterns
                    4. Rate limit handling
                    5. No silent failures (bare except:)""",
                    tools=["Read", "Glob", "Grep"]
                ),
                "silent-failure-hunter": AgentDefinition(
                    description="Finds silent failure patterns in Python code.",
                    prompt="Find all instances of bare except:, except Exception:, and except: pass.",
                    tools=["Read", "Grep"]
                )
            }
        )
    ):
        if hasattr(message, "result"):
            print(message.result)

asyncio.run(review_dsm_code())
```

**DSM Test Generator Agent**:

```python
from claude_agent_sdk import query, ClaudeAgentOptions, AgentDefinition

async def generate_dsm_tests():
    async for message in query(
        prompt="Generate pytest tests for src/datasourcemanager/fetchers/binance.py",
        options=ClaudeAgentOptions(
            allowed_tools=["Read", "Write", "Edit", "Bash", "Task"],
            agents={
                "test-writer": AgentDefinition(
                    description="DSM test writer following project patterns.",
                    prompt="""Write pytest tests following DSM patterns:
                    1. Use pytest fixtures for DataSourceManager
                    2. Mock external API calls
                    3. Test FCP failover scenarios
                    4. Validate DataFrame structure
                    5. Test error handling paths""",
                    tools=["Read", "Write", "Edit"]
                )
            }
        )
    ):
        if hasattr(message, "result"):
            print(message.result)

asyncio.run(generate_dsm_tests())
```

**DSM FCP Debugger Agent**:

```python
from claude_agent_sdk import query, ClaudeAgentOptions, AgentDefinition

async def debug_fcp():
    async for message in query(
        prompt="Debug FCP failover behavior for BTCUSDT",
        options=ClaudeAgentOptions(
            allowed_tools=["Read", "Bash", "Grep", "Task"],
            agents={
                "fcp-debugger": AgentDefinition(
                    description="Debugs FCP failover and caching issues.",
                    prompt="""Debug FCP by:
                    1. Check cache status in .cache/fcp/
                    2. Review rate limit logs
                    3. Trace failover decision path
                    4. Validate source priority order
                    5. Test manual failover trigger""",
                    tools=["Read", "Bash", "Grep"]
                )
            }
        )
    ):
        if hasattr(message, "result"):
            print(message.result)

asyncio.run(debug_fcp())
```

### Multi-Agent Patterns

**Fan-Out Pattern** (parallel processing):

```python
agents={
    "search-agent-1": AgentDefinition(
        description="Search agent for user queries",
        prompt="Search email history for login issues",
        tools=["Read", "Grep"]
    ),
    "search-agent-2": AgentDefinition(
        description="Search agent for error logs",
        prompt="Search logs for authentication errors",
        tools=["Read", "Grep"]
    ),
    "search-agent-3": AgentDefinition(
        description="Search agent for config files",
        prompt="Search config for auth settings",
        tools=["Read", "Glob"]
    )
}
```

**Pipeline Pattern** (sequential processing):

```python
# Stage 1: Analyze
async for message in query(
    prompt="Analyze the codebase structure",
    options=ClaudeAgentOptions(allowed_tools=["Read", "Glob"])
):
    session_id = message.session_id if hasattr(message, 'session_id') else session_id

# Stage 2: Plan (with context)
async for message in query(
    prompt="Create a refactoring plan based on analysis",
    options=ClaudeAgentOptions(resume=session_id)
):
    pass

# Stage 3: Execute
async for message in query(
    prompt="Execute the refactoring plan",
    options=ClaudeAgentOptions(resume=session_id, allowed_tools=["Edit", "Write"])
):
    pass
```

### Error Handling

```python
from claude_agent_sdk import query, ClaudeAgentOptions
from claude_agent_sdk.exceptions import AgentError, ToolError

async def safe_query():
    try:
        async for message in query(
            prompt="Fix the bug",
            options=ClaudeAgentOptions(allowed_tools=["Read", "Edit"])
        ):
            if hasattr(message, "error"):
                print(f"Agent error: {message.error}")
            elif hasattr(message, "result"):
                print(message.result)
    except ToolError as e:
        print(f"Tool execution failed: {e}")
    except AgentError as e:
        print(f"Agent error: {e}")
```

### Resources

- **TypeScript SDK**: [github.com/anthropics/claude-agent-sdk-typescript](https://github.com/anthropics/claude-agent-sdk-typescript)
- **Python SDK**: [github.com/anthropics/claude-agent-sdk-python](https://github.com/anthropics/claude-agent-sdk-python)
- **Example agents**: [github.com/anthropics/claude-agent-sdk-demos](https://github.com/anthropics/claude-agent-sdk-demos)
## Skills Architecture Reference

### Overview

Skills are structured prompt templates that Claude Code can invoke to perform specialized tasks. They provide progressive disclosure of domain knowledge, enabling Claude to load context on-demand rather than cluttering the main CLAUDE.md file.

### SKILL.md Structure

Every skill is defined by a `SKILL.md` file with YAML frontmatter:

```markdown
---
name: dsm-usage
description: DataSourceManager API usage patterns
user-invocable: true
---

# DSM Usage Skill

Instructions for using DataSourceManager...

## References

- @references/fcp-protocol.md
- @examples/basic-fetch.md
```

### Skill Locations and Scopes

Skills can be defined at multiple levels with different scopes:

| Location   | Scope             | Discovery Path                      |
| ---------- | ----------------- | ----------------------------------- |
| Enterprise | Organization-wide | Managed settings                    |
| Personal   | User-specific     | `~/.claude/skills/`                 |
| Project    | Repository        | `docs/skills/` or `.claude/skills/` |
| Plugin     | Marketplace       | `plugins/{name}/skills/`            |

**Discovery priority** (highest to lowest):

1. Enterprise managed skills
2. Project skills (in working directory)
3. Personal skills
4. Plugin skills

### Frontmatter Reference

Complete YAML frontmatter options:

```yaml
---
# Required
name: skill-name # Unique identifier (kebab-case)
description: Brief purpose # Shown in skill listings

# Invocation Control
user-invocable: true # Can user invoke with /skill-name
disable-model-invocation: false # Prevent Claude from auto-invoking

# Execution Context
context: append # append (default) | fork
agent: Explore # Subagent type for fork context

# Tool Restrictions
allowed-tools: # Limit available tools
  - Read
  - Grep
  - Glob

# Model Selection
model: sonnet # opus | sonnet | haiku

# Hooks Integration
hooks: # Skill-specific hooks
  pre-invoke: validate.sh
  post-invoke: cleanup.sh
---
```

### Context Modes

**`context: append`** (default):

- Skill content added to current conversation
- Full tool access retained
- State persists in main context

**`context: fork`**:

- Skill runs in isolated subagent
- Dedicated context window
- Results returned to main conversation
- Ideal for research/exploration tasks

```yaml
---
name: dsm-research
context: fork
agent: Explore
allowed-tools:
  - Read
  - Grep
  - Glob
  - WebSearch
---
```

### Auto-Discovery for Monorepos

Claude Code auto-discovers skills from nested directories, enabling polyglot monorepo patterns:

```
monorepo/
├── CLAUDE.md              # Root hub
├── docs/skills/           # Shared skills
│   └── shared-skill/
│       └── SKILL.md
├── packages/
│   ├── api/
│   │   └── docs/skills/   # Package-specific
│   │       └── api-skill/
│   │           └── SKILL.md
│   └── web/
│       └── docs/skills/
│           └── web-skill/
│               └── SKILL.md
```

**Discovery behavior**:

- Skills discovered relative to working directory
- Nested skills accessible when in subdirectory
- Name conflicts resolved by proximity (closer = priority)

### Dynamic Context Injection

Use backtick-bang syntax to inject dynamic content:

```markdown
## Current State

`!git status --short`

## Recent Changes

`!git log --oneline -5`
```

**Supported injections**:

| Syntax           | Purpose              |
| ---------------- | -------------------- |
| `` `!command` `` | Shell command output |
| `@file.md`       | File content import  |
| `@directory/`    | Directory listing    |

### @ File Imports

Import external content into skills:

```markdown
## References

- @references/api-guide.md # Relative import
- @/docs/GLOSSARY.md # Repo-root import
- @examples/basic-fetch.md # Example code
```

**Import resolution**:

1. Relative to SKILL.md location
2. Repo-root paths start with `@/`
3. Supports glob patterns: `@examples/*.md`

### Supporting Files Structure

Organize skill assets in subdirectories:

```
skills/
└── dsm-usage/
    ├── SKILL.md           # Main skill definition
    ├── references/        # Domain documentation
    │   ├── fcp-protocol.md
    │   └── timestamp-rules.md
    ├── examples/          # Code examples
    │   ├── basic-fetch.md
    │   ├── multi-source.md
    │   └── error-handling.md
    ├── scripts/           # Runnable utilities
    │   ├── validate-data.py
    │   └── debug-fcp.sh
    └── templates/         # Code templates
        ├── new-source.py.tmpl
        └── test-fixture.py.tmpl
```

### Skill Invocation Patterns

**User invocation** (interactive):

```
/dsm-usage
/dsm-testing --verbose
```

**Claude invocation** (automatic):

- Claude detects relevant context from user query
- Invokes skill matching domain need
- Disabled with `disable-model-invocation: true`

**Programmatic invocation** (SDK):

```python
from claude_code import Client

client = Client()
result = await client.invoke_skill("dsm-usage", args={"symbol": "BTC/USDT"})
```

### Tool Restrictions

Limit skill capabilities for safety and focus:

```yaml
---
name: code-reviewer
allowed-tools:
  - Read
  - Grep
  - Glob
  # No Edit, Write, Bash - read-only review
---
```

**Common restriction patterns**:

| Pattern   | Tools                       | Use Case            |
| --------- | --------------------------- | ------------------- |
| Read-only | Read, Grep, Glob            | Code review         |
| Research  | Read, Grep, Glob, WebSearch | Investigation       |
| Full dev  | All tools                   | Feature development |
| Safe exec | Read, Write, Edit           | No bash access      |

### Model Selection in Skills

Override default model for specific skills:

```yaml
---
name: quick-lookup
model: haiku # Fast, cheap for simple tasks
---
```

```yaml
---
name: complex-refactor
model: opus # Best reasoning for complex tasks
---
```

**Model selection guidelines**:

| Task Type            | Recommended Model |
| -------------------- | ----------------- |
| Simple lookup        | haiku             |
| Code generation      | sonnet            |
| Complex reasoning    | opus              |
| Research/exploration | sonnet            |
| Critical decisions   | opus              |

### Hooks in Skills

Attach lifecycle hooks to skills:

```yaml
---
name: deploy-skill
hooks:
  pre-invoke: scripts/validate-env.sh
  post-invoke: scripts/notify-team.sh
---
```

**Hook events**:

- `pre-invoke`: Before skill execution
- `post-invoke`: After skill completes
- `on-error`: When skill fails

### Visual Output Generation

Skills can generate visual assets:

```markdown
## Generate Diagram

Create architecture diagram and save to `output/diagram.png`.

Use mermaid syntax:
\`\`\`mermaid
graph TD
A[Client] --> B[DSM]
B --> C[Source 1]
B --> D[Source 2]
\`\`\`
```

### DSM Skill Patterns

#### dsm-usage Skill

```yaml
---
name: dsm-usage
description: DataSourceManager API usage patterns
user-invocable: true
context: append
---

# DataSourceManager Usage

## Quick Start

@examples/basic-fetch.md

## FCP Protocol

@references/fcp-protocol.md

## Symbol Formats

@references/symbol-formats.md
```

#### dsm-testing Skill

```yaml
---
name: dsm-testing
description: DSM testing patterns and fixtures
user-invocable: true
context: append
allowed-tools:
  - Read
  - Write
  - Edit
  - Bash
  - Grep
  - Glob
---

# DSM Testing Skill

## Test Patterns

@references/test-patterns.md

## Fixture Generation

`!python scripts/generate-fixtures.py --list`

## Run Tests

@scripts/run-tests.md
```

#### dsm-research Skill

```yaml
---
name: dsm-research
description: Codebase exploration and research
user-invocable: true
context: fork
agent: Explore
allowed-tools:
  - Read
  - Grep
  - Glob
---

# DSM Research Skill

Research the data-source-manager codebase to answer questions.

## Codebase Structure

@references/architecture.md

## Key Components

- `src/data_source_manager/` - Core DSM implementation
- `src/data_source_manager/sources/` - Data source implementations
- `tests/` - Test suite
```

#### dsm-fcp-monitor Skill

```yaml
---
name: dsm-fcp-monitor
description: Monitor and debug FCP behavior
user-invocable: true
context: fork
agent: Explore
---

# FCP Monitor Skill

## Current FCP State

`!python -c "from data_source_manager import get_fcp_status; print(get_fcp_status())"`

## Recent Failures

`!grep -r "FCP" logs/ | tail -20`

## FCP Protocol Reference

@references/fcp-protocol.md
```

### Progressive Disclosure Pattern

Skills enable progressive context loading:

```
CLAUDE.md (< 300 lines)
    ↓ mentions skill
/dsm-usage (invoked on demand)
    ↓ imports reference
@references/fcp-protocol.md (loaded when needed)
    ↓ imports examples
@examples/error-recovery.md (loaded when needed)
```

**Benefits**:

- Main CLAUDE.md stays concise
- Context loaded only when relevant
- Reduces token usage
- Improves response quality

### Skill Discovery Commands

```bash
# List available skills
claude skills list

# Show skill details
claude skills show dsm-usage

# Invoke skill directly
claude skill dsm-usage

# Search skills
claude skills search "testing"
```

### Enterprise Skills Management

For team/enterprise deployments:

```json
{
  "managedSkills": {
    "enterprise-coding-standards": {
      "source": "https://internal.company.com/skills/coding-standards",
      "required": true,
      "autoInvoke": ["*.py", "*.ts"]
    }
  }
}
```

### Skill Versioning

Track skill versions for compatibility:

```yaml
---
name: dsm-usage
version: "<version>" # SSoT: SKILL.md frontmatter
min-claude-version: "<version>" # SSoT: Claude Code release notes
deprecated: false
deprecation-message: ""
---
```

### Debugging Skills

Enable skill debugging:

```bash
# Verbose skill execution
CLAUDE_DEBUG_SKILLS=1 claude

# Trace skill imports
CLAUDE_TRACE_IMPORTS=1 claude
```

**Debug output includes**:

- Skill discovery paths
- Import resolution
- Frontmatter parsing
- Tool restriction application

### Best Practices

1. **Keep SKILL.md focused**: One skill per domain concern
2. **Use @ imports**: Factor out reusable content
3. **Set appropriate context**: Fork for research, append for tasks
4. **Restrict tools**: Only grant necessary capabilities
5. **Document examples**: Include runnable examples
6. **Version skills**: Track breaking changes
7. **Test skills**: Validate skill behavior before shipping

### Common Mistakes

| Mistake              | Problem           | Solution                  |
| -------------------- | ----------------- | ------------------------- |
| Giant SKILL.md       | Context pollution | Split into @ imports      |
| No tool restrictions | Security risk     | Use allowed-tools         |
| Always fork context  | Lost state        | Use append for most tasks |
| Missing examples     | Poor adoption     | Add examples/ directory   |
| Hardcoded paths      | Portability       | Use relative imports      |

### Integration with Commands

Skills can be invoked from commands:

```yaml
# .claude/commands/quick-test.md
---
name: quick-test
description: Run quick verification tests
---

First load the testing skill:
/dsm-testing

Then run the quick test suite...
```

### Integration with Agents

Skills inform agent behavior:

```yaml
# .claude/agents/test-writer.md
---
name: test-writer
description: Writes tests following DSM patterns
skills:
  - dsm-testing
  - dsm-usage
---
```
## Settings and Permissions Reference

### Overview

Claude Code uses a hierarchical settings system that enables organizations, teams, and individuals to configure permissions, MCP servers, and behavior. Understanding this hierarchy is essential for security and collaboration.

### Settings File Hierarchy

Four-tier scope system with precedence (highest to lowest):

| Scope   | Location                             | Who it affects       | Shared?   |
| ------- | ------------------------------------ | -------------------- | --------- |
| Managed | System-level `managed-settings.json` | All users on machine | Yes (IT)  |
| Project | `.claude/settings.json`              | All collaborators    | Yes (git) |
| Local   | `.claude/settings.local.json`        | You, in repo only    | No        |
| User    | `~/.claude/settings.json`            | You, all projects    | No        |

### Enterprise Managed Settings Locations

| Platform  | Path                                                            |
| --------- | --------------------------------------------------------------- |
| macOS     | `/Library/Application Support/ClaudeCode/managed-settings.json` |
| Linux/WSL | `/etc/claude-code/managed-settings.json`                        |
| Windows   | `C:\Program Files\ClaudeCode\managed-settings.json`             |

Managed settings **cannot be overridden** and require administrator privileges.

### Permission Rules Structure

```json
{
  "permissions": {
    "allow": ["Rule1", "Rule2"],
    "ask": ["Rule3"],
    "deny": ["Rule4", "Rule5"]
  }
}
```

### Rule Evaluation Order

First match wins, in this order:

1. **Deny** rules (highest priority - always blocks)
2. **Ask** rules (requires user confirmation)
3. **Allow** rules (lowest priority - permits silently)

Deny rules always take precedence, even if allow rules match the same command.

### Pattern Syntax by Tool

#### Bash Rules

**Match all uses:**

```json
"Bash"           // matches all bash commands
"Bash(*)"        // equivalent to above
```

**Exact command:**

```json
"Bash(npm run build)"
"Bash(git commit -m 'message')"
```

**Wildcard patterns** (spaces matter):

```json
"Bash(npm run *)"      // matches npm run lint, npm run test
"Bash(git * main)"     // matches git push main, git pull main
"Bash(* --version)"    // matches any command with --version
"Bash(ls *)"           // matches ls -la but NOT lsof
"Bash(ls*)"            // matches both ls -la AND lsof
```

#### Read Rules

**Match all reads:**

```json
"Read"
```

**Specific file:**

```json
"Read(./.env)"
"Read(~/.zshrc)"
```

**Directory patterns:**

```json
"Read(./.env.*)"       // matches .env.local, .env.production
"Read(./secrets/**)"   // matches all files in secrets recursively
"Read(~/.aws/**)"      // blocks AWS config access
```

#### Edit Rules

**Allow writing to directories:**

```json
"Edit(./src/)"
"Edit(../docs/)"
```

**Deny specific files:**

```json
"Edit(./.env)"
"Edit(./package-lock.json)"
```

#### WebFetch Rules

**Match all web requests:**

```json
"WebFetch"
```

**Domain-specific:**

```json
"WebFetch(domain:example.com)"
"WebFetch(domain:github.com)"
```

### Complete Settings Example

```json
{
  "permissions": {
    "allow": [
      "Bash(npm run lint)",
      "Bash(npm run test *)",
      "Bash(git commit *)",
      "Read(./src/**)",
      "Read(./package.json)",
      "Edit(./src/)"
    ],
    "ask": ["Bash(git push *)", "Edit(./package.json)"],
    "deny": [
      "Bash(curl *)",
      "Bash(rm -rf *)",
      "Read(./.env)",
      "Read(./.env.*)",
      "Read(./secrets/**)",
      "WebFetch(domain:internal-api.company.com)",
      "Edit(./config/)"
    ]
  },
  "additionalDirectories": ["../docs/"],
  "defaultMode": "acceptEdits",
  "disableBypassPermissionsMode": "disable"
}
```

### Bash Permission Security Warning

Bash patterns that constrain arguments are fragile and unreliable:

```json
// ❌ UNRELIABLE - Don't use for security:
"Bash(curl http://github.com/ *)"

// Won't match:
// - curl -X GET http://github.com/...  (flags before URL)
// - curl https://github.com/...        (different protocol)
// - Commands with shell variables
```

**Better approach** - use deny-list:

```json
{
  "deny": ["Bash(curl *)", "Bash(wget *)", "Bash(rm -rf *)"]
}
```

### Settings Precedence Order

Complete evaluation order:

1. **Managed settings** (highest - cannot be overridden)
2. **Command-line arguments**
3. **Local project settings** (`.claude/settings.local.json`)
4. **Shared project settings** (`.claude/settings.json`)
5. **User settings** (lowest - `~/.claude/settings.json`)

Example: If user settings allow `Bash(npm run *)` but project settings deny it, the **project setting wins**.

### Enterprise Managed Settings Features

#### MCP Server Control

```json
{
  "allowedMcpServers": [{ "serverName": "github" }, { "serverName": "memory" }],
  "deniedMcpServers": [{ "serverName": "filesystem" }]
}
```

#### Plugin Marketplace Restrictions

```json
{
  "strictKnownMarketplaces": [
    { "source": "github", "repo": "acme-corp/approved-plugins" },
    { "source": "npm", "package": "@acme-corp/plugins" },
    { "source": "url", "url": "https://plugins.example.com/marketplace.json" }
  ]
}
```

#### Hook Lockdown

```json
{
  "allowManagedHooksOnly": true
}
```

Only managed and SDK hooks allowed when enabled.

### Common Configuration Patterns

#### Frontend Development

```json
{
  "permissions": {
    "allow": [
      "Bash(npm run *)",
      "Bash(git *)",
      "Read(./)",
      "Edit(./src/)",
      "Edit(./public/)"
    ],
    "deny": ["Read(./.env)", "Read(./node_modules/**)"]
  }
}
```

#### CI/CD Safety

```json
{
  "permissions": {
    "allow": ["Bash(npm run build)", "Bash(npm run test)", "Read(./)"],
    "deny": [
      "Bash(git push *)",
      "Bash(rm -rf *)",
      "Read(./.env.*)",
      "Read(./secrets/**)"
    ]
  }
}
```

#### Team Standardization

```json
{
  "permissions": {
    "allow": ["Bash(npm run *)", "Bash(docker *)", "Bash(git commit *)"],
    "ask": ["Bash(git push *)"]
  },
  "env": {
    "NODE_ENV": "development"
  },
  "companyAnnouncements": [
    "Code reviews required for all PRs",
    "New security policy in effect"
  ]
}
```

### DSM Settings Configuration

#### Project Settings (.claude/settings.json)

```json
{
  "permissions": {
    "allow": [
      "Bash(uv run *)",
      "Bash(mise run *)",
      "Bash(git commit *)",
      "Bash(git log *)",
      "Bash(git status *)",
      "Bash(git diff *)",
      "Read(./src/**)",
      "Read(./tests/**)",
      "Edit(./src/)",
      "Edit(./tests/)"
    ],
    "ask": ["Bash(git push *)", "Bash(git rebase *)"],
    "deny": [
      "Bash(pip install *)",
      "Bash(python3.14 *)",
      "Bash(python3.12 *)",
      "Bash(git push --force *)",
      "Bash(rm -rf *)",
      "Read(.env*)",
      "Read(.mise.local.toml)",
      "Edit(.env*)",
      "Edit(.mise.local.toml)"
    ]
  },
  "additionalDirectories": ["../cc-skills/"],
  "extraKnownMarketplaces": [
    { "source": "github", "repo": "terrylica/cc-skills" }
  ]
}
```

#### Local Overrides (.claude/settings.local.json)

```json
{
  "permissions": {
    "allow": ["Bash(python -c *)"]
  },
  "env": {
    "DEBUG": "1",
    "LOG_LEVEL": "DEBUG"
  }
}
```

### Environment Variables in Settings

```json
{
  "env": {
    "NODE_ENV": "development",
    "PYTHONPATH": "./src",
    "LOG_LEVEL": "INFO"
  }
}
```

Environment variables are applied to all Bash commands executed by Claude Code.

### Company Announcements

Display messages to all team members:

```json
{
  "companyAnnouncements": [
    "Sprint planning Monday 10am",
    "New FCP debugging guide: /fcp-debugger",
    "Remember: Use /dsm-testing before PRs"
  ]
}
```

### Additional Directories

Grant access to directories outside the project:

```json
{
  "additionalDirectories": ["../shared-lib/", "../docs/", "~/reference/"]
}
```

### Default Mode Configuration

```json
{
  "defaultMode": "acceptEdits"
}
```

Options:

- `"normal"` - Standard interactive mode
- `"acceptEdits"` - Auto-accept file edits
- `"planMode"` - Start in plan mode

### Bypass Permissions Mode

Control whether users can bypass permissions:

```json
{
  "disableBypassPermissionsMode": "disable"
}
```

Options:

- `"disable"` - Block bypass entirely
- `"warn"` - Allow with warning
- (absent) - Allow freely

### Settings Validation

Claude Code validates settings on load. Common errors:

| Error           | Cause             | Fix                 |
| --------------- | ----------------- | ------------------- |
| Invalid JSON    | Syntax error      | Check JSON validity |
| Unknown key     | Typo in key name  | Check documentation |
| Invalid pattern | Bad glob syntax   | Fix pattern syntax  |
| Path not found  | Missing directory | Create or fix path  |

### Debugging Settings

View effective settings:

```bash
# Show resolved settings
claude settings show

# Show specific scope
claude settings show --scope project

# Validate settings file
claude settings validate .claude/settings.json
```

### Best Practices

1. **Use project settings for team standards** - Commit `.claude/settings.json`
2. **Use local settings for experiments** - `.claude/settings.local.json` is gitignored
3. **Prefer deny-lists for security** - Allow-lists with wildcards are fragile
4. **Test permission patterns** - Verify behavior before committing
5. **Document custom patterns** - Add comments in settings.md
6. **Review managed settings** - Understand enterprise restrictions

### Common Mistakes

| Mistake                                | Problem                  | Solution               |
| -------------------------------------- | ------------------------ | ---------------------- |
| Relying on allow patterns for security | Patterns can be bypassed | Use deny rules         |
| Overly broad wildcards                 | Unexpected permissions   | Be specific            |
| Not testing patterns                   | Unexpected blocks        | Test before commit     |
| Conflicting project/user settings      | Confusion                | Document precedence    |
| Missing deny for secrets               | Accidental exposure      | Always deny .env files |

### Migration from Legacy Settings

If upgrading from older Claude Code versions:

1. Back up existing settings
2. Update to new JSON format
3. Convert string patterns to object format if needed
4. Validate with `claude settings validate`
5. Test key workflows

### Integration with Hooks

Settings can trigger hooks:

```json
{
  "hooks": {
    "onSettingsChange": [
      {
        "command": "echo 'Settings updated' >> ~/.claude/audit.log"
      }
    ]
  }
}
```
## Agentic Coding Best Practices Reference

### Overview

Claude Code is an agentic coding environment. Unlike a chatbot that answers questions and waits, Claude Code can read files, run commands, make changes, and autonomously work through problems. This changes how you work: instead of writing code yourself and asking Claude to review it, you describe what you want and Claude figures out how to build it.

### The Core Constraint

Most best practices stem from one constraint: **Claude's context window fills up fast, and performance degrades as it fills**.

Claude's context window holds your entire conversation, including every message, every file Claude reads, and every command output. A single debugging session or codebase exploration might generate tens of thousands of tokens.

When context fills:

- Claude may "forget" earlier instructions
- Error rates increase
- Performance degrades

**Context is the most important resource to manage**.

### The Explore-Plan-Code-Commit Workflow

Letting Claude jump straight to coding can produce code that solves the wrong problem. Use Plan Mode to separate exploration from execution.

#### Phase 1: Explore (Plan Mode)

```
# Enter Plan Mode - Claude reads files without making changes
read /src/auth and understand how we handle sessions and login.
also look at how we manage environment variables for secrets.
```

#### Phase 2: Plan (Plan Mode)

```
# Create a detailed implementation plan
I want to add Google OAuth. What files need to change?
What's the session flow? Create a plan.
```

Press `Ctrl+G` to open the plan in your editor for direct editing.

#### Phase 3: Implement (Normal Mode)

```
# Switch to Normal Mode and execute the plan
implement the OAuth flow from your plan. write tests for the
callback handler, run the test suite and fix any failures.
```

#### Phase 4: Commit (Normal Mode)

```
# Commit with descriptive message and create PR
commit with a descriptive message and open a PR
```

**When to skip planning**: For small, clear tasks (typos, log lines, renames), ask Claude to do it directly. Planning is most useful when uncertain about approach, modifying multiple files, or unfamiliar with the code.

### Give Claude Verification Criteria

This is the **single highest-leverage thing you can do**. Claude performs dramatically better when it can verify its own work.

| Strategy             | Before                                                | After                                                                                                                                              |
| -------------------- | ----------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------- |
| Provide verification | "implement a function that validates email addresses" | "write a validateEmail function. test cases: <user@example.com> is true, invalid is false, <user@.com> is false. run the tests after implementing" |
| Verify UI visually   | "make the dashboard look better"                      | "[paste screenshot] implement this design. take a screenshot and compare to original. list differences and fix them"                               |
| Address root causes  | "the build is failing"                                | "the build fails with this error: [paste error]. fix it and verify the build succeeds. address the root cause, don't suppress the error"           |

### Provide Specific Context

The more precise your instructions, the fewer corrections you'll need.

| Strategy           | Before                                             | After                                                                                                                                            |
| ------------------ | -------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------ |
| Scope the task     | "add tests for foo.py"                             | "write a test for foo.py covering the edge case where the user is logged out. avoid mocks."                                                      |
| Point to sources   | "why does ExecutionFactory have such a weird api?" | "look through ExecutionFactory's git history and summarize how its api came to be"                                                               |
| Reference patterns | "add a calendar widget"                            | "look at how existing widgets are implemented. HotDogWidget.php is a good example. follow the pattern to implement a new calendar widget."       |
| Describe symptoms  | "fix the login bug"                                | "users report that login fails after session timeout. check auth flow in src/auth/. write a failing test that reproduces the issue, then fix it" |

### Rich Content Input

- **Reference files with `@`** instead of describing locations
- **Paste images directly** via copy/paste or drag and drop
- **Give URLs** for documentation and API references
- **Pipe in data** with `cat error.log | claude`
- **Let Claude fetch** context using Bash, MCP tools, or file reads

### CLAUDE.md Best Practices

CLAUDE.md is read at the start of every conversation. Include only information Claude can't infer from code alone.

**Include:**

- Bash commands Claude can't guess
- Code style rules that differ from defaults
- Testing instructions and preferred runners
- Repository etiquette (branch naming, PR conventions)
- Architectural decisions specific to your project
- Developer environment quirks

**Exclude:**

- Anything Claude can figure out by reading code
- Standard language conventions
- Detailed API documentation (link instead)
- Information that changes frequently
- File-by-file codebase descriptions
- Self-evident practices like "write clean code"

Keep CLAUDE.md concise. For each line ask: "Would removing this cause Claude to make mistakes?" If not, cut it.

### Extended Thinking Triggers

Use specific words to allocate thinking budget:

| Trigger        | Budget Level |
| -------------- | ------------ |
| "think"        | Low          |
| "think hard"   | Medium       |
| "think harder" | High         |
| "ultrathink"   | Maximum      |

Higher levels get more compute time for comprehensive solution evaluation.

### Managing Context Effectively

#### Course-Correct Early

- **`Esc`**: Stop Claude mid-action, context preserved
- **`Esc + Esc` or `/rewind`**: Open rewind menu to restore state
- **`"Undo that"`**: Have Claude revert changes
- **`/clear`**: Reset context between unrelated tasks

If you've corrected Claude more than twice on the same issue, context is cluttered. Run `/clear` and start fresh with a better prompt.

#### Aggressive Context Management

- Use `/clear` frequently between tasks
- When auto-compaction triggers, Claude summarizes key decisions
- Run `/compact <instructions>` for control: `/compact Focus on the API changes`
- Add compaction instructions to CLAUDE.md: "When compacting, always preserve the full list of modified files"

### Use Subagents for Investigation

Since context is the fundamental constraint, subagents are powerful tools:

```
Use subagents to investigate how our authentication system handles token
refresh, and whether we have any existing OAuth utilities I should reuse.
```

Subagents:

- Run in separate context windows
- Report back summaries
- Keep your main conversation clean
- Don't clutter your context with exploration

Use subagents for verification too:

```
use a subagent to review this code for edge cases
```

### Let Claude Interview You

For larger features, have Claude interview you first:

```
I want to build [brief description]. Interview me in detail using the AskUserQuestion tool.

Ask about technical implementation, UI/UX, edge cases, concerns, and tradeoffs. Don't ask obvious questions, dig into the hard parts I might not have considered.

Keep interviewing until we've covered everything, then write a complete spec to SPEC.md.
```

Then start a fresh session to execute the spec with clean context.

### Session Management

#### Checkpoints and Rewind

Every action creates a checkpoint. Double-tap `Escape` or run `/rewind` to:

- Restore conversation only (keep code)
- Restore code only (keep conversation)
- Restore both

Checkpoints persist across sessions.

#### Resume Conversations

```bash
claude --continue    # Resume most recent conversation
claude --resume      # Select from recent conversations
```

Use `/rename` to give sessions descriptive names for later finding.

### Parallel Sessions

Run multiple Claude sessions in parallel:

- **Claude Desktop**: Manage multiple local sessions visually
- **Claude Code on web**: Run on cloud infrastructure in isolated VMs

**Writer/Reviewer Pattern:**

| Session A (Writer)                                                      | Session B (Reviewer)                                                                                                |
| ----------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------- |
| "Implement a rate limiter for our API endpoints"                        |                                                                                                                     |
|                                                                         | "Review the rate limiter in @src/middleware/rateLimiter.ts. Look for edge cases, race conditions, and consistency." |
| "Here's the review feedback: [Session B output]. Address these issues." |                                                                                                                     |

### Headless Mode for Automation

```bash
# One-off queries
claude -p "Explain what this project does"

# Structured output for scripts
claude -p "List all API endpoints" --output-format json

# Streaming for real-time processing
claude -p "Analyze this log file" --output-format stream-json
```

### Fan-Out Pattern for Large Migrations

1. **Generate task list**: Have Claude list all files needing migration
2. **Loop through with script**:

   ```bash
   for file in $(cat files.txt); do
     claude -p "Migrate $file from React to Vue. Return OK or FAIL." \
       --allowedTools "Edit,Bash(git commit *)"
   done
   ```

3. **Test on few files, then run at scale**

### Common Failure Patterns

| Pattern                  | Problem                                            | Fix                                                       |
| ------------------------ | -------------------------------------------------- | --------------------------------------------------------- |
| Kitchen sink session     | Unrelated tasks pollute context                    | `/clear` between unrelated tasks                          |
| Correcting over and over | Failed approaches pollute context                  | After 2 failures, `/clear` and write better prompt        |
| Over-specified CLAUDE.md | Important rules get lost                           | Ruthlessly prune, use hooks for critical rules            |
| Trust-then-verify gap    | Plausible implementation doesn't handle edge cases | Always provide verification (tests, scripts, screenshots) |
| Infinite exploration     | Claude reads hundreds of files                     | Scope investigations narrowly or use subagents            |

### DSM-Specific Agentic Patterns

#### FCP Debugging Workflow

```
1. Enter Plan Mode
2. Read src/data_source_manager/fcp/ to understand FCP architecture
3. Look at fcp-protocol.md rule for decision logic
4. Exit Plan Mode
5. Write failing test reproducing the FCP issue
6. Fix the underlying source logic
7. Verify test passes and cache behavior is correct
```

#### Data Fetching Verification

```
Use the data-fetcher agent to:
1. Fetch BTC/USDT:perp OHLCV data from okx
2. Validate DataFrame structure (open_time, open, high, low, close, volume)
3. Check timestamp handling (UTC, milliseconds)
4. Verify FCP behavior with cache hit/miss scenarios
```

#### Test-Driven Development with DSM

```
1. Write test for expected DataFrame structure
2. Write test for FCP cache hit behavior
3. Write test for source failover logic
4. Run tests - confirm they fail
5. Implement the feature
6. Run tests - confirm they pass
7. Use silent-failure-hunter agent to review
```

### Best Practices Summary

1. **Verify everything** - Tests, screenshots, expected outputs
2. **Explore before coding** - Use Plan Mode for complex tasks
3. **Be specific** - Reference files, mention constraints, show patterns
4. **Manage context** - Clear often, use subagents for research
5. **Course-correct early** - Stop and redirect immediately when off track
6. **Use checkpoints** - Rewind when approaches fail
7. **Parallelize work** - Multiple sessions for complex projects
8. **Automate repetitive tasks** - Headless mode, fan-out pattern

### Developing Intuition

Pay attention to what works:

- When Claude produces great output, notice what you did
- When Claude struggles, ask why: noisy context? vague prompt? task too big?

Over time you'll know:

- When to be specific vs open-ended
- When to plan vs explore
- When to clear context vs let it accumulate
