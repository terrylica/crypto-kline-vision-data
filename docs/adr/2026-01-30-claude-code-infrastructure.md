---
status: accepted
date: 2026-01-30
decision-maker: terrylica
consulted: [cc-skills patterns, Claude Code documentation]
research-method: Analysis of cc-skills best practices and Claude Code official docs
---

# Claude Code Infrastructure for AI-Assisted Development

## Context and Problem Statement

Data Source Manager is a complex package with domain-specific patterns (FCP, timestamp handling, symbol formats) that require context for effective AI-assisted development. How do we structure documentation and tooling to maximize Claude Code effectiveness?

## Decision Drivers

- Reduce context pollution in main CLAUDE.md
- Enable progressive disclosure for detailed topics
- Provide specialized agents for common tasks
- Ensure domain rules are loaded only when relevant
- Follow established patterns from cc-skills

## Considered Options

1. **Monolithic CLAUDE.md** - All instructions in one file
2. **Wiki-style docs** - Separate markdown files without Claude Code integration
3. **Progressive Disclosure** - Skills, agents, rules, and commands structure

## Decision Outcome

Chosen option: **Progressive Disclosure** with Claude Code infrastructure because it provides the best balance of discoverability and context efficiency.

### Structure

```
.claude/
├── agents/           # Specialized subagents
│   ├── api-reviewer.md
│   ├── data-fetcher.md
│   ├── fcp-debugger.md
│   ├── silent-failure-hunter.md
│   └── test-writer.md
├── commands/         # Slash commands
│   ├── debug-fcp.md
│   ├── fetch-data.md
│   ├── feature-dev.md
│   ├── quick-test.md
│   ├── review-dsm.md
│   └── validate-data.md
├── hooks/            # Project-specific hooks
│   └── dsm-code-guard.sh
└── rules/            # Context rules (loaded on demand)
    ├── binance-api.md
    ├── caching-patterns.md
    ├── dataframe-operations.md
    ├── error-handling.md
    ├── fcp-protocol.md
    ├── symbol-formats.md
    └── timestamp-handling.md

docs/skills/
├── dsm-usage/        # DataSourceManager usage skill
│   ├── SKILL.md
│   ├── examples/
│   ├── references/
│   └── scripts/
├── dsm-testing/      # Testing skill
│   ├── SKILL.md
│   ├── examples/
│   ├── references/
│   └── scripts/
└── dsm-research/     # Codebase research skill
    └── SKILL.md
```

### Progressive Disclosure Pattern

1. **CLAUDE.md** (<300 lines) - Quick reference, navigation, critical rules
2. **Skills** (SKILL.md) - Task-specific guidance with examples and references
3. **Context Rules** (.claude/rules/) - Domain-specific guidance loaded on demand
4. **Agents** (.claude/agents/) - Specialized subagents for delegation
5. **Commands** (.claude/commands/) - Slash commands for common workflows

### Key Patterns

**Skills with @ Imports**:

```markdown
## References

- @references/fcp-protocol.md - Detailed FCP documentation
- @examples/basic-fetch.md - Usage examples
```

**Context Rules with YAML Frontmatter**:

```yaml
---
name: dsm-research
context: fork
agent: Explore
user-invocable: true
---
```

### Consequences

**Good:**

- CLAUDE.md stays concise and scannable
- Domain rules only loaded when relevant
- Specialized agents handle complex tasks
- Scripts provide runnable examples
- Consistent with cc-skills patterns

**Bad:**

- More files to maintain
- Requires understanding of Claude Code features
- @ imports require specific syntax

## Implementation Status

[x] TASK_COMPLETE

**Completed**: 2026-01-30

**Summary**: Comprehensive Claude Code infrastructure implemented with 102 commits.

| Metric              | Value                                                                            |
| ------------------- | -------------------------------------------------------------------------------- |
| Design spec lines   | 1547                                                                             |
| Sections            | 26                                                                               |
| Agents              | 5 (api-reviewer, data-fetcher, fcp-debugger, silent-failure-hunter, test-writer) |
| Commands            | 6 (debug-fcp, fetch-data, quick-test, review-dsm, validate-data, feature-dev)    |
| Skills              | 4 (dsm-usage, dsm-testing, dsm-research, dsm-fcp-monitor)                        |
| Rules               | 7 (binance-api, caching, dataframe, error, fcp, symbols, timestamp)              |
| Hooks               | 5 (SessionStart, UserPromptSubmit, PreToolUse, PostToolUse, Stop)                |
| CLAUDE.md hierarchy | 5 files (root, src/, tests/, docs/, examples/)                                   |

**Key patterns implemented**:

- Progressive disclosure with on-demand rule loading
- Hub-spoke navigation across CLAUDE.md hierarchy
- Model selection & routing with cost optimization
- Context window management (80% rule, compaction)
- Error recovery & troubleshooting patterns
- Security best practices with deny rules
- Multi-agent orchestration (Fan-Out, Pipeline, Map-Reduce)

## More Information

- `.claude/README.md` - Infrastructure documentation
- `docs/INDEX.md` - Documentation navigation hub
- `docs/design/2026-01-30-claude-code-infrastructure/spec.md` - Full design spec (1547 lines)
- [cc-skills](https://github.com/terrylica/cc-skills) - Pattern source
