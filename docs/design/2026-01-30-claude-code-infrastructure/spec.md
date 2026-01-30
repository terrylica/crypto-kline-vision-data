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
