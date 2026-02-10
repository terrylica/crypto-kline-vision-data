# Documentation Guide

Context for working with crypto-kline-vision-data documentation.

**Hub**: [Root CLAUDE.md](../CLAUDE.md) | **Siblings**: [src/](../src/CLAUDE.md) | [tests/](../tests/CLAUDE.md) | [examples/](../examples/CLAUDE.md) | [scripts/](../scripts/CLAUDE.md) | [playground/](../playground/CLAUDE.md)

## Directory Structure

```
docs/
├── adr/                    ← Architecture Decision Records (MADR 4.0)
├── api/                    ← Binance API reference documentation
├── benchmarks/             ← Performance benchmarks (Polars vs Pandas, streaming)
├── ckvd/                   ← CKVD class documentation
├── data_client_interface/  ← Provider interface specifications
├── design/                 ← Implementation specifications (1:1 with ADRs)
├── howto/                  ← Step-by-step guides (cache control, logging)
├── skills/                 ← Claude Code progressive disclosure skills
├── GLOSSARY.md             ← Domain terminology
├── INDEX.md                ← Navigation hub
└── TROUBLESHOOTING.md      ← Common issues and solutions
```

**Local-only directories** (git-ignored): `complaints_to_binance/`, `journals/`, `sr&ed/`

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

| Skill                                                | Purpose                         |
| ---------------------------------------------------- | ------------------------------- |
| [ckvd-usage](skills/ckvd-usage/SKILL.md)             | CryptoKlineVisionData API usage |
| [ckvd-testing](skills/ckvd-testing/SKILL.md)         | Testing patterns                |
| [ckvd-research](skills/ckvd-research/SKILL.md)       | Codebase research               |
| [ckvd-fcp-monitor](skills/ckvd-fcp-monitor/SKILL.md) | FCP monitoring                  |

## Benchmarks

Performance benchmark documentation in `docs/benchmarks/`:

| Document                                                    | Purpose                        |
| ----------------------------------------------------------- | ------------------------------ |
| [README.md](benchmarks/README.md)                           | Benchmark overview and results |
| [PERFORMANCE_SUMMARY.md](benchmarks/PERFORMANCE_SUMMARY.md) | Executive summary              |
| `scripts/`                                                  | Runnable benchmark scripts     |
| `results/`                                                  | Raw benchmark output           |

## Key Documents

| Document                                 | Purpose            |
| ---------------------------------------- | ------------------ |
| [INDEX.md](INDEX.md)                     | Navigation hub     |
| [GLOSSARY.md](GLOSSARY.md)               | Domain terminology |
| [TROUBLESHOOTING.md](TROUBLESHOOTING.md) | Issue resolution   |

## Link Conventions

When linking from docs:

| Target     | Format                     |
| ---------- | -------------------------- |
| Other docs | Relative (`./adr/file.md`) |
| Root files | Repo-root (`/CLAUDE.md`)   |
| External   | Full URL                   |
