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
