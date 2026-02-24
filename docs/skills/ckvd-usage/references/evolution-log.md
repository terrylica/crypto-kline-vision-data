# ckvd-usage Evolution Log

Reverse-chronological log of skill improvements.

---

## 2026-02-24: Add streaming API documentation

**Trigger**: [Public streaming API release](https://github.com/terrylica/crypto-kline-vision-data/releases)

**Changes**:

- Added "Streaming" row to Key Concepts table
- New "Streaming Real-Time Klines" section with sync and async examples
- Documented KlineUpdate fields and StreamConfig options
- Added Template D: Stream Real-Time Data (TodoWrite)
- Noted optional [streaming] extras requirement

---

## 2026-02-10: Add scaffolding and fix cache paths

**Trigger**: Alignment audit against skill-architecture standards

**Changes**:

- Added TodoWrite Task Templates section (3 templates)
- Added Post-Change Checklist section
- Created this evolution-log.md
- Fixed cache path in debugging.md (was `~/.cache/ckvd`, now platformdirs path)
- Fixed cache structure diagram in fcp-protocol.md

---

## 2026-01-30: Initial skill creation

**Source**: Claude Code infrastructure ADR

**Features**:

- Quick Start code example
- High-level API (fetch_market_data)
- Helper scripts (validate_symbol.py, check_cache.py, diagnose_fcp.py)
- Progressive disclosure references (market-types, intervals, fcp-protocol, debugging)
