# Session Resume Context

Last updated: 2026-02-09

## Recent Work

### CLAUDE.md Hub-and-Spoke Overhaul (2026-02-08)

**Status**: Complete

Root CLAUDE.md pruned from ~300 to ~160 lines. Spokes enriched with domain-specific context:

| Spoke                | Content Added                              |
| -------------------- | ------------------------------------------ |
| `src/CLAUDE.md`      | Package structure, code patterns, FCP impl |
| `tests/CLAUDE.md`    | Test commands, markers, fixtures, mocking  |
| `docs/CLAUDE.md`     | ADRs, skills, benchmarks, troubleshooting  |
| `examples/CLAUDE.md` | Example conventions, common patterns       |
| `scripts/CLAUDE.md`  | Dev scripts, mise tasks, cache tools       |

### Package Rename: data-source-manager to crypto-kline-vision-data (2026-02-07)

**Status**: Complete (4 commits on main)

- Package name: `crypto-kline-vision-data` (PyPI), import: `ckvd`
- All `dsm_` prefixed files renamed to `ckvd_` prefix
- Environment variables: `DSM_*` renamed to `CKVD_*`
- Class name: `DataSourceManager` renamed to `CryptoKlineVisionData`
- Rollback tag: `v3-pre-rename`

### Cache Toggle Feature (GitHub #20) (2026-02-07)

**Status**: Complete

- Added `CKVD_ENABLE_CACHE` env var to disable cache globally
- Fixed `enforce_source=CACHE` + `use_cache=False` contradiction (raises RuntimeError)
- 34 new tests added

### Dead Code Removal + Clone Consolidation (2026-02-06)

**Status**: Complete

~500 net lines removed (199 insertions, 699 deletions across 6 commits).

Tools: vulture (dead code), PMD CPD (code clones), Semgrep (patterns).

Post-cleanup: vulture 0 items at 80% confidence, PMD CPD 4 structural clones remaining (non-actionable).

### Dead Code Elimination (2026-02-05)

**Status**: Complete (GitHub #12)

~1,880 lines removed. Key items:

- Fixed unreachable else block in `dataframe_utils.py` (vulture 100% finding)
- Deleted dead logger infrastructure (`utils/for_logger/` - 9 files, 1,413 LOC)
- Deleted unused config class, deprecated modules, deprecated functions

### DRY Principle Audit (2026-02-01)

**Status**: Complete

- Deleted 3 redundant example files
- Fixed broken lazy initialization demo
- Performance optimizations (iterrows to itertuples, while loops to list comprehensions)

### Claude Code Infrastructure (2026-01-30)

**Status**: Complete

Comprehensive Claude Code infrastructure created:

- 5 agents, 6 commands, 4 skills (domain rules migrated to src/CLAUDE.md spoke)
- Hooks: PreToolUse (bash-guard), PostToolUse (code-guard), Stop (final-check)
- Settings: permission rules (allow/deny patterns)
- ADR: `docs/adr/2026-01-30-claude-code-infrastructure.md`

---

## Quick Commands

```bash
# Validate Claude Code infrastructure
mise run claude:validate

# Run unit tests
mise run test

# Quick validation (lint + tests + import)
mise run quick

# Preview semantic-release
mise run release:dry
```

## Architecture Overview

```
crypto-kline-vision-data/
├── CLAUDE.md              # Root instructions (hub)
├── src/CLAUDE.md          # Source code patterns (spoke)
├── tests/CLAUDE.md        # Test context (spoke)
├── docs/CLAUDE.md         # Documentation guide (spoke)
├── examples/CLAUDE.md     # Example context (spoke)
├── scripts/CLAUDE.md      # Dev scripts (spoke)
├── .claude/               # Claude Code extensions
│   ├── agents/            # 5 subagents
│   ├── commands/          # 6 slash commands
│   └── hooks/             # Pre/Post tool hooks
└── docs/
    ├── skills/            # 4 progressive disclosure skills
    ├── adr/               # Architecture decisions
    └── design/            # Implementation specs
```

## Codebase Health

- Unit tests: 318 passed
- vulture: 0 dead code items at 80% confidence
- PMD CPD: 4 structural clones remaining (all non-actionable)
- ruff: pre-existing warnings (PLR0912, PLR0915, PLR0911, PLW2901 - legacy code)
