# ckvd-fcp-monitor Evolution Log

Reverse-chronological log of skill improvements.

---

## 2026-02-24: Clarify streaming and FCP as separate systems

**Trigger**: Public streaming API release, avoid confusion about FCP scope

**Changes**:

- Added "Streaming and FCP" section with comparison table
- Documented that streaming is a parallel data path (WebSocket, no cache/Vision)
- FCP applies to historical `get_data()`, streaming to real-time methods
- Added note to TodoWrite templates: "All templates are for FCP-based fetching"
- Clarified that monitoring FCP does not affect streaming performance

---

## 2026-02-10: Add scaffolding, fix cache paths, remove missing scripts

**Trigger**: Alignment audit against skill-architecture standards

**Changes**:

- Added TodoWrite Task Templates section (3 templates)
- Added Post-Change Checklist section
- Fixed cache diagnostic paths (was `~/.cache/ckvd`, now platformdirs path)
- Fixed cache_health.py to use platformdirs instead of hardcoded path
- Removed fcp_stats.py and warm_cache.py references (scripts never existed)

---

## 2026-01-30: Added adr field and evolution-log

**Trigger**: Adopting cc-skills patterns for skill self-evolution

**Changes**:

- Added `adr: docs/adr/2025-01-30-failover-control-protocol.md` to frontmatter
- Created `references/evolution-log.md` for tracking improvements
- Added link to evolution-log in Related section

**Rationale**: Skills should link to architectural decisions and track their own improvement history for maintainability.

---

## 2026-01-30: Initial skill creation

**Source**: Claude Code infrastructure ADR

**Features**:

- FCP decision flow diagram
- Common issues and diagnostics
- Performance optimization patterns
- Cache warm-up examples
- Script references (cache_health.py, fcp_stats.py, warm_cache.py)

**Frontmatter**:

- `context: fork` for diagnostic isolation
- `allowed-tools: Read, Bash, Grep, Glob`
- `TRIGGERS` keywords for Claude invocation matching
