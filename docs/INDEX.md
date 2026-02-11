# Documentation Index

Navigation hub for Crypto Kline Vision Data documentation.

**Quick links**: [CLAUDE.md](/CLAUDE.md) | [README.md](/README.md) | [GLOSSARY.md](GLOSSARY.md) | [TROUBLESHOOTING.md](TROUBLESHOOTING.md) | [Examples](/examples/)

---

## Core Documentation

| Directory                                | Purpose                             |
| ---------------------------------------- | ----------------------------------- |
| [api/](api/)                             | Binance API reference               |
| [benchmarks/](benchmarks/)               | Performance benchmarks and results  |
| [TROUBLESHOOTING.md](TROUBLESHOOTING.md) | Cache, logging, FCP troubleshooting |

---

## Local-Only (Git-Ignored)

These directories contain local/private content and are not tracked in git:

- `journals/` - Development journals and notes
- `sr&ed/` - SR&ED tax credit documentation

---

## Architecture Decision Records

ADRs should be placed in `docs/adr/` following MADR 4.0 format.
Naming convention: `YYYY-MM-DD-slug.md`

| ADR                                                                                  | Design Spec                                                       |
| ------------------------------------------------------------------------------------ | ----------------------------------------------------------------- |
| [Failover Control Protocol](/docs/adr/2025-01-30-failover-control-protocol.md)       | [spec](/docs/design/2025-01-30-failover-control-protocol/spec.md) |
| [src-layout Package Structure](/docs/adr/2025-01-30-src-layout-package-structure.md) | _(self-contained)_                                                |
| [Claude Code Infrastructure](/docs/adr/2026-01-30-claude-code-infrastructure.md)     | _(self-contained)_                                                |

See [itp:adr-graph-easy-architect](https://github.com/terrylica/cc-skills) for diagram creation.

---

## Claude Code Infrastructure

Claude Code extensions for AI-assisted development. See [.claude/](/.claude/) directory.

**Reference**: [settings.md](/.claude/settings.md) - Human-readable settings documentation

### Skills

Progressive disclosure for detailed guidance. Each has a SKILL.md with YAML frontmatter.

| Skill                                        | Purpose                              |
| -------------------------------------------- | ------------------------------------ |
| [ckvd-usage](skills/ckvd-usage/)             | CryptoKlineVisionData usage guide    |
| [ckvd-testing](skills/ckvd-testing/)         | Testing patterns and pytest          |
| [ckvd-research](skills/ckvd-research/)       | Codebase research (runs in subagent) |
| [ckvd-fcp-monitor](skills/ckvd-fcp-monitor/) | FCP monitoring and diagnostics       |

### Agents

Specialized subagents that Claude can delegate to:

| Agent                 | Purpose                                |
| --------------------- | -------------------------------------- |
| api-reviewer          | Reviews code for API consistency       |
| data-fetcher          | Fetches data with proper FCP handling  |
| test-writer           | Writes tests following CKVD patterns   |
| silent-failure-hunter | Finds silent failures and bare excepts |
| fcp-debugger          | Diagnoses FCP issues                   |

### Commands

Slash commands for common workflows:

| Command      | Purpose                           |
| ------------ | --------------------------------- |
| /review-ckvd | Review code against CKVD patterns |
| /feature-dev | Guided feature development        |

### Domain Context (migrated to hub-and-spoke)

Domain-specific rules (Binance API, exceptions, symbols, timestamps, caching, FCP) are now in [src/CLAUDE.md](/src/CLAUDE.md) — loaded on demand when working with source code.
