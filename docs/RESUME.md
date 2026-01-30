# Session Resume Context

Last updated: 2026-01-30

## Recent Work

### UserPromptSubmit Hook & Skill Context Isolation (2026-01-30)

**Status**: Complete

**What was done**:

- Added UserPromptSubmit hook (dsm-skill-suggest.sh) for proactive skill suggestions
- Added `context: fork` to dsm-fcp-monitor skill for diagnostic isolation
- Updated design spec with hook events summary table and skill context isolation table
- Total hooks: 3 (UserPromptSubmit + PreToolUse + PostToolUse)

**Sources**: [claude-code-showcase](https://github.com/ChrisWiles/claude-code-showcase), [HumanLayer CLAUDE.md guide](https://www.humanlayer.dev/blog/writing-a-good-claude-md)

### Additional Hook & Documentation Enhancements (2026-01-30)

**Status**: Complete

**What was done**:

- Added 2 new PostToolUse checks (DataFrame validation, Polars preference)
- Total DSM-specific hooks: 11 checks covering silent failures and data integrity
- Updated .claude/README.md with hooks summary table
- Support `# polars-exception` comment to suppress Polars reminder

**Sources**: [awesome-claude-code](https://github.com/hesreallyhim/awesome-claude-code), [DataCamp hooks tutorial](https://www.datacamp.com/tutorial/claude-code-hooks)

### CC-Skills Patterns Integration (2026-01-30)

**Status**: Complete

**What was done**:

- Added TRIGGERS keywords to all 4 skills for better Claude invocation matching
- Added YAML frontmatter to design specs (adr, source, status, phase, last-updated)
- Documented code correctness philosophy with explicit rules tables
- Expanded hooks README with checked vs NOT checked rules rationale

**Pattern applied**: [terrylica/cc-skills](https://github.com/terrylica/cc-skills) repository patterns

### Path-Specific Rules & Agent Enhancements (2026-01-30)

**Status**: Complete

**What was done**:

- Added YAML frontmatter with `paths:` field to all 7 rules for conditional loading
- Enhanced all 5 agents with `skills:` field for context injection
- Added `permissionMode: plan` to read-only agents (api-reviewer, silent-failure-hunter)
- Improved agent descriptions with "Use proactively" triggers for automatic delegation
- Updated design spec with path-specific rules and agent best practices

**Pattern applied**: Official Claude Code memory optimization patterns from [code.claude.com/docs](https://code.claude.com/docs/en/sub-agents)

### Claude Code Infrastructure (2026-01-30)

**Status**: Complete

**What was done**:

- Created comprehensive Claude Code infrastructure for AI-assisted development
- 5 custom agents (api-reviewer, data-fetcher, fcp-debugger, silent-failure-hunter, test-writer)
- 6 slash commands (debug-fcp, fetch-data, quick-test, review-dsm, validate-data, feature-dev)
- 7 context rules (binance-api, caching-patterns, dataframe-operations, error-handling, fcp-protocol, symbol-formats, timestamp-handling)
- 4 progressive disclosure skills (dsm-usage, dsm-testing, dsm-research, dsm-fcp-monitor)
- 2 hooks (PreToolUse bash-guard, PostToolUse code-guard)

**Key files**:

- `.claude/settings.md` - Human-readable configuration reference
- `.claude/hooks/dsm-bash-guard.sh` - PreToolUse command validation
- `.claude/hooks/dsm-code-guard.sh` - PostToolUse code quality checks
- `docs/design/2026-01-30-claude-code-infrastructure/spec.md` - Implementation spec
- `docs/adr/2026-01-30-claude-code-infrastructure.md` - ADR

**Validation**:

- 23/23 infrastructure checks passing (`mise run claude:validate`)
- All hooks functional
- CLAUDE.md at 267 lines (under 300 limit)

### Domain-Specific CLAUDE.md Files (2026-01-30)

**Status**: Complete

**What was done**:

- Created `examples/CLAUDE.md` for example-specific context
- Created `tests/CLAUDE.md` for test-specific context
- Created `docs/CLAUDE.md` for documentation guide
- Implemented monorepo-style hierarchical loading pattern

**Pattern**: Claude Code loads these lazily when working in respective directories.

### Hook Implementation (2026-01-30)

**Status**: Complete

**Hooks created**:

| Hook              | Event       | Purpose                                   |
| ----------------- | ----------- | ----------------------------------------- |
| dsm-bash-guard.sh | PreToolUse  | Block dangerous commands before execution |
| dsm-code-guard.sh | PostToolUse | Detect silent failure patterns in code    |

**Blocked operations** (PreToolUse):

- Cache deletion without mise task
- Python version changes
- Force push to main/master
- Direct pip install

## Quick Commands

```bash
# Validate Claude Code infrastructure
mise run claude:validate

# Run unit tests
mise run test

# Quick validation (lint + tests + import)
mise run quick

# Diagnose FCP behavior
mise run fcp:diagnose
```

## Architecture Overview

```
data-source-manager/
├── CLAUDE.md              # Root instructions (hub)
├── .claude/               # Claude Code extensions
│   ├── agents/            # 5 subagents
│   ├── commands/          # 6 slash commands
│   ├── hooks/             # Pre/Post tool hooks
│   └── rules/             # 7 context rules
├── docs/
│   ├── skills/            # 4 progressive disclosure skills
│   ├── adr/               # Architecture decisions
│   └── design/            # Implementation specs
├── examples/CLAUDE.md     # Example context (lazy loaded)
└── tests/CLAUDE.md        # Test context (lazy loaded)
```

## Next Steps

- Monitor hook effectiveness in real usage
- Consider additional skill development as patterns emerge
- Update Lessons Learned section as issues are discovered
