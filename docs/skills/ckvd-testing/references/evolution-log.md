# ckvd-testing Evolution Log

Reverse-chronological log of skill improvements.

---

## 2026-02-24: Add async streaming test patterns

**Trigger**: Public streaming API release

**Changes**:

- Updated test organization diagram to include `tests/unit/streaming/`
- Added `@pytest.mark.asyncio` marker to Test Markers table
- New "Async Streaming Tests" section with pytest-asyncio setup
- Example async test with StreamConfig, mock KlineStream, and async context manager
- Template D: Write Async Stream Test (TodoWrite)
- Documented async fixture patterns and AsyncMock usage

---

## 2026-02-10: Add scaffolding

**Trigger**: Alignment audit against skill-architecture standards

**Changes**:

- Added TodoWrite Task Templates section (3 templates)
- Added Post-Change Checklist section
- Created this evolution-log.md

---

## 2026-01-30: Initial skill creation

**Source**: Claude Code infrastructure ADR

**Features**:

- Test workflow checklist
- Test organization diagram
- Running tests (unit, integration, all)
- Writing new tests pattern
- Mocking HTTP calls
- Helper script (run_quick_tests.sh)
- Progressive disclosure references (fixtures, coverage, mocking-patterns, markers)
