# Documentation Index

Navigation hub for Crypto Kline Vision Data documentation.

**Quick links**: [CLAUDE.md](/CLAUDE.md) | [README.md](/README.md) | [GLOSSARY.md](GLOSSARY.md) | [TROUBLESHOOTING.md](TROUBLESHOOTING.md) | [Examples](/examples/)

---

## Core Documentation

| Directory                                        | Purpose                                  |
| ------------------------------------------------ | ---------------------------------------- |
| [api/](api/)                                     | API reference and endpoint documentation |
| [core_architecture/](core_architecture/)         | System architecture and design           |
| [ckvd/](ckvd/)                                   | CKVD class documentation                 |
| [data_client_interface/](data_client_interface/) | Provider interface specifications        |

---

## How-To Guides

| Guide                                             | Description                              |
| ------------------------------------------------- | ---------------------------------------- |
| [howto/](howto/)                                  | Step-by-step guides for common tasks     |
| [debugging/](debugging/)                          | Debugging techniques and troubleshooting |
| [Telemetry](/examples/README.md#telemetry-output) | NDJSON event schema and parsing          |

---

## Technical References

| Document                                                                         | Description                                     |
| -------------------------------------------------------------------------------- | ----------------------------------------------- |
| [python_package_principles_guide.md](python_package_principles_guide.md)         | Python packaging standards used in this project |
| [python_package_principles_checklist.md](python_package_principles_checklist.md) | Checklist for package compliance                |
| [httpx_migration_guide.md](httpx_migration_guide.md)                             | Migration from requests to httpx                |
| [http_client_benchmark_summary.md](http_client_benchmark_summary.md)             | HTTP client performance comparison              |

---

## Utilities & Internals

| Directory                                | Purpose                        |
| ---------------------------------------- | ------------------------------ |
| [utils/](utils/)                         | Utility module documentation   |
| [cache_diagnostics/](cache_diagnostics/) | Cache debugging and validation |
| [optimizations/](optimizations/)         | Performance optimization notes |

---

## Project Planning

| Directory            | Purpose                      |
| -------------------- | ---------------------------- |
| [roadmap/](roadmap/) | Feature roadmap and planning |

---

## Local-Only (Git-Ignored)

These directories contain local/private content and are not tracked in git:

- `journals/` - Development journals and notes
- `sr&ed/` - SR&ED tax credit documentation
- `complaints_to_binance/` - Support tickets and issue tracking

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

| Command        | Purpose                           |
| -------------- | --------------------------------- |
| /debug-fcp     | Debug FCP behavior for a symbol   |
| /quick-test    | Run quick verification tests      |
| /review-ckvd   | Review code against CKVD patterns |
| /fetch-data    | Fetch market data with validation |
| /validate-data | Validate DataFrame structure      |
| /feature-dev   | Guided feature development        |

### Domain Context (migrated to hub-and-spoke)

Domain-specific rules (Binance API, exceptions, symbols, timestamps, caching, FCP) are now in [src/CLAUDE.md](/src/CLAUDE.md) — loaded on demand when working with source code.
