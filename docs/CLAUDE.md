# Documentation Guide

Context for working with data-source-manager documentation.

**Hub**: [Root CLAUDE.md](../CLAUDE.md) | **Sibling**: [examples/CLAUDE.md](../examples/CLAUDE.md) | [tests/CLAUDE.md](../tests/CLAUDE.md)

## Directory Structure

```
docs/
├── adr/                    ← Architecture Decision Records (MADR 4.0)
├── design/                 ← Implementation specifications (1:1 with ADRs)
├── skills/                 ← Claude Code progressive disclosure skills
├── api/                    ← API reference documentation
├── core_architecture/      ← System architecture and design
├── data_source_manager/    ← DSM class documentation
├── howto/                  ← Step-by-step guides
├── debugging/              ← Debugging techniques
├── testing/                ← Test writing guides
├── INDEX.md                ← Navigation hub
├── GLOSSARY.md             ← Domain terminology
└── TROUBLESHOOTING.md      ← Common issues and solutions
```

## ADR Conventions

**Naming**: `YYYY-MM-DD-slug.md` (no sequential numbers)

**Format**: [MADR 4.0](https://github.com/adr/madr)

**Current ADRs**:

| ADR                                                                                       | Purpose           |
| ----------------------------------------------------------------------------------------- | ----------------- |
| [2025-01-30-failover-control-protocol](adr/2025-01-30-failover-control-protocol.md)       | FCP architecture  |
| [2025-01-30-src-layout-package-structure](adr/2025-01-30-src-layout-package-structure.md) | Package layout    |
| [2026-01-30-claude-code-infrastructure](adr/2026-01-30-claude-code-infrastructure.md)     | Claude Code setup |

## Design Specs

**Location**: `docs/design/YYYY-MM-DD-slug/spec.md`

**Relationship**: 1:1 with ADRs. Each ADR has a corresponding design spec.

| Design Spec                                                              | ADR             |
| ------------------------------------------------------------------------ | --------------- |
| [FCP spec](design/2025-01-30-failover-control-protocol/spec.md)          | FCP ADR         |
| [Claude Code spec](design/2026-01-30-claude-code-infrastructure/spec.md) | Claude Code ADR |

## Skills Directory

Progressive disclosure skills in `docs/skills/`:

| Skill                                              | Purpose                     |
| -------------------------------------------------- | --------------------------- |
| [dsm-usage](skills/dsm-usage/SKILL.md)             | DataSourceManager API usage |
| [dsm-testing](skills/dsm-testing/SKILL.md)         | Testing patterns            |
| [dsm-research](skills/dsm-research/SKILL.md)       | Codebase research           |
| [dsm-fcp-monitor](skills/dsm-fcp-monitor/SKILL.md) | FCP monitoring              |

## Key Documents

| Document                                 | Purpose                |
| ---------------------------------------- | ---------------------- |
| [INDEX.md](INDEX.md)                     | Navigation hub         |
| [RESUME.md](RESUME.md)                   | Session resume context |
| [GLOSSARY.md](GLOSSARY.md)               | Domain terminology     |
| [TROUBLESHOOTING.md](TROUBLESHOOTING.md) | Issue resolution       |

## Link Conventions

When linking from docs:

| Target     | Format                     |
| ---------- | -------------------------- |
| Other docs | Relative (`./adr/file.md`) |
| Root files | Repo-root (`/CLAUDE.md`)   |
| External   | Full URL                   |
