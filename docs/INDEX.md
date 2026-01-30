# Documentation Index

Navigation hub for Data Source Manager documentation.

**Quick links**: [CLAUDE.md](/CLAUDE.md) | [README.md](/README.md) | [Examples](/examples/)

---

## Core Documentation

| Directory                                        | Purpose                                  |
| ------------------------------------------------ | ---------------------------------------- |
| [api/](api/)                                     | API reference and endpoint documentation |
| [core_architecture/](core_architecture/)         | System architecture and design           |
| [data_source_manager/](data_source_manager/)     | DSM class documentation                  |
| [data_client_interface/](data_client_interface/) | Provider interface specifications        |

---

## How-To Guides

| Guide                    | Description                              |
| ------------------------ | ---------------------------------------- |
| [howto/](howto/)         | Step-by-step guides for common tasks     |
| [debugging/](debugging/) | Debugging techniques and troubleshooting |
| [testing/](testing/)     | Test writing and execution guides        |

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

| Directory                                        | Purpose                      |
| ------------------------------------------------ | ---------------------------- |
| [roadmap/](roadmap/)                             | Feature roadmap and planning |
| [documentation_audit.md](documentation_audit.md) | Documentation coverage audit |

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

See [itp:adr-graph-easy-architect](https://github.com/terrylica/cc-skills) for diagram creation.

---

## Claude Code Skills

Skills provide progressive disclosure for Claude Code agents. Each skill has a SKILL.md with YAML frontmatter.

| Skill                              | Purpose                       |
| ---------------------------------- | ----------------------------- |
| [dsm-usage](skills/dsm-usage/)     | DataSourceManager usage guide |
| [dsm-testing](skills/dsm-testing/) | Testing patterns and pytest   |
