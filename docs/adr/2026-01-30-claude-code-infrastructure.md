---
status: accepted
date: 2026-01-30
decision-maker: terrylica
consulted: [cc-skills patterns, Claude Code documentation]
research-method: Analysis of cc-skills best practices and Claude Code official docs
---

# Claude Code Infrastructure for AI-Assisted Development

## Context and Problem Statement

Crypto Kline Vision Data is a complex package with domain-specific patterns (FCP, timestamp handling, symbol formats) that require context for effective AI-assisted development. How do we structure documentation and tooling to maximize Claude Code effectiveness?

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
│   ├── review-ckvd.md
│   └── validate-data.md
├── hooks/            # Project-specific hooks
│   └── ckvd-code-guard.sh
└── rules/            # Context rules (loaded on demand)
    ├── binance-api.md
    ├── caching-patterns.md
    ├── dataframe-operations.md
    ├── error-handling.md
    ├── fcp-protocol.md
    ├── symbol-formats.md
    └── timestamp-handling.md

docs/skills/
├── ckvd-usage/        # CryptoKlineVisionData usage skill
│   ├── SKILL.md
│   ├── examples/
│   ├── references/
│   └── scripts/
├── ckvd-testing/      # Testing skill
│   ├── SKILL.md
│   ├── examples/
│   ├── references/
│   └── scripts/
└── ckvd-research/     # Codebase research skill
    └── SKILL.md
```

### Progressive Disclosure Pattern

1. **CLAUDE.md** (<300 lines) - Quick reference, navigation, critical rules
2. **Skills** (SKILL.md) - Task-specific guidance with examples and references
3. **Nested CLAUDE.md** (src/, tests/, etc.) - Domain-specific guidance loaded on demand
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
name: ckvd-research
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

**Summary**: Claude Code infrastructure implemented for CKVD.

| Metric              | Value                                                                            |
| ------------------- | -------------------------------------------------------------------------------- |
| Agents              | 5 (api-reviewer, data-fetcher, fcp-debugger, silent-failure-hunter, test-writer) |
| Commands            | 6 (debug-fcp, fetch-data, quick-test, review-ckvd, validate-data, feature-dev)   |
| Skills              | 4 (ckvd-usage, ckvd-testing, ckvd-research, ckvd-fcp-monitor)                    |
| Rules               | 7 (binance-api, caching, dataframe, error, fcp, symbols, timestamp)              |
| Hooks               | 5 (SessionStart, UserPromptSubmit, PreToolUse, PostToolUse, Stop)                |
| CLAUDE.md hierarchy | 5 files (root, src/, tests/, docs/, examples/)                                   |

**CKVD-specific patterns implemented**:

- Progressive disclosure with on-demand FCP and API rule loading
- Hub-spoke navigation across CLAUDE.md hierarchy (root, src/, tests/, docs/, examples/)
- Domain-specific context rules for Binance API, timestamps, symbols, caching
- Custom agents for CKVD tasks (silent-failure-hunter, fcp-debugger, api-reviewer)
- Skill-based guidance for CKVD usage, testing, and FCP monitoring

## More Information

- `.claude/README.md` - Infrastructure documentation
- `docs/INDEX.md` - Documentation navigation hub
- [cc-skills](https://github.com/terrylica/cc-skills) - Pattern source

**Note**: The original design spec was removed as it contained copied Anthropic documentation rather than CKVD-specific content. The actual deliverables are the agents, commands, skills, rules, and hooks listed above.
