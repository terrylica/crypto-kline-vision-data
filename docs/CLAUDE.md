# Documentation Guide

Context for working with data-source-manager documentation.

**Hub**: [Root CLAUDE.md](../CLAUDE.md) | **Siblings**: [src/](../src/CLAUDE.md) | [tests/](../tests/CLAUDE.md) | [examples/](../examples/CLAUDE.md)

## Directory Structure

```
docs/
├── adr/                    ← Architecture Decision Records (MADR 4.0)
├── api/                    ← API reference documentation
├── cache_diagnostics/      ← Cache debugging and validation
├── core_architecture/      ← System architecture and design
├── data_client_interface/  ← Provider interface specifications
├── data_source_manager/    ← DSM class documentation
├── debugging/              ← Debugging techniques
├── design/                 ← Implementation specifications (1:1 with ADRs)
├── howto/                  ← Step-by-step guides
├── optimizations/          ← Performance optimization notes
├── roadmap/                ← Feature roadmap and planning
├── skills/                 ← Claude Code progressive disclosure skills
├── testing/                ← Test writing guides
├── utils/                  ← Utility module documentation
├── GLOSSARY.md             ← Domain terminology
├── INDEX.md                ← Navigation hub
├── RESUME.md               ← Session resume context
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

**Relationship**: 1:1 with ADRs for significant features requiring detailed implementation specs.

| Design Spec                                                     | ADR     |
| --------------------------------------------------------------- | ------- |
| [FCP spec](design/2025-01-30-failover-control-protocol/spec.md) | FCP ADR |

**Note**: Claude Code infrastructure ADR is self-contained (no separate spec needed).

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
