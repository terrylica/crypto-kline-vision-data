# Task for crew-planner

Create a task breakdown for implementing this PRD.

## PRD: PRD.md

# PRD: Documentation Alignment Investigation

**Project**: crypto-kline-vision-data  
**PROSTAT**: docs alignment  
**Date**: 2026-02-25  
**Investigative Focus**: Verify all documentation is consistent with actual implementation

---

## Problem Statement

The crypto-kline-vision-data project has multiple documentation sources (README.md, CLAUDE.md files in each directory, docs/, examples/, API reference). As the project has evolved, documentation may have become misaligned with implementation. This investigation aims to identify and document all gaps.

---

## Investigation Perspectives (9 Tasks)

### Task 1: README vs Source Code API Alignment

**Perspective**: Surface-level public API  
**Investigate**: Compare README.md examples with actual `ckvd.__init__.py` exports and `CryptoKlineVisionData` class methods  
**Validation**: Run README code snippets to verify they work  
**Broadcast**: Key API discrepancies found between README and actual implementation

---

### Task 2: CLAUDE.md Hub-and-Spoke Consistency

**Perspective**: Claude Code documentation architecture  
**Investigate**: Check all 6 CLAUDE.md files (root, src, tests, docs, examples, scripts, playground) for cross-references, consistency of shared information (Python version, FCP priority, etc.)  
**Validation**: Verify links between CLAUDE.md files work and content is non-contradictory  
**Broadcast**: Inconsistencies found in hub-and-spoke documentation

---

### Task 3: Example Scripts API Validation

**Perspective**: Runnable examples  
**Investigate**: Test all examples in `examples/` directory — verify they import correctly and execute without errors  
**Validation**: Run each example with `uv run -p 3.13 python examples/<file>`  
**Broadcast**: Which examples fail and why (API changes, import errors, etc.)

---

### Task 4: API Boundary Documentation (pandas/polars)

**Perspective**: Output format documentation  
**Investigate**: Verify documented behavior for `return_polars` parameter matches implementation in `crypto_kline_vision_data.py`  
**Validation**: Test both output modes and compare with docs  
**Broadcast**: Any discrepancies in API boundary documentation

---

### Task 5: Exception Hierarchy Documentation

**Perspective**: Error handling docs  
**Investigate**: Compare documented exceptions in src/CLAUDE.md and README.md with actual exception classes in `utils/for_core/`  
**Validation**: Import each documented exception and verify it exists with correct attributes  
**Broadcast**: Missing, renamed, or incorrectly documented exceptions

---

### Task 6: Internal Link Validation

**Perspective**: Cross-documentation links  
**Investigate**: Check all markdown links within docs/, CLAUDE.md files for validity (use lychee or manual check)  
**Validation**: Verify each link resolves to existing file/section  
**Broadcast**: Broken or incorrect links found

---

### Task 7: Type Hints and Parameter Documentation

**Perspective**: API signature accuracy  
**Investigate**: Compare documented function signatures (in README, CLAUDE.md) with actual type hints in source files  
**Validation**: Run type checker or inspect actual signatures  
**Broadcast**: Parameter type mismatches or missing parameters

---

### Task 8: Streaming API Documentation Accuracy

**Perspective**: WebSocket real-time docs  
**Investigate**: Compare README streaming examples and src/CLAUDE.md streaming section with actual `KlineStream`, `KlineUpdate`, `StreamConfig` implementations  
**Validation**: Test streaming code if environment permits  
**Broadcast**: Streaming API documentation vs implementation gaps

---

### Task 9: Environment Variables & Configuration Docs

**Perspective**: Runtime configuration  
**Investigate**: Verify all documented environment variables (`CKVD_LOG_LEVEL`, `CKVD_ENABLE_CACHE`, `CKVD_USE_POLARS_OUTPUT`) are actually used in code  
**Validation**: Grep source for env var usage, test with wrong values  
**Broadcast**: Undocumented or non-functional env vars

---

## Task Dependencies

**Parallel (No Dependencies)**:
- Task 1, 2, 3, 4, 5, 6, 7, 8, 9 can all run independently

**Sequential (None required)**:
- All 9 perspectives are independent investigations

---

## Dynamic Task Creation (DCTG) Instructions

Each task worker MUST:

1. **Start with one angle** of their assigned perspective
2. **Analyze findings** to determine next investigation step
3. **Spawn follow-up sub-tasks** using `pi_messenger({ action: "task.create", ... })` when new angles emerge
4. **Use empirical validation** with bash commands (`uv run -p 3.13`) to verify claims
5. **Create scaffolding directories** under `/tmp/crew-<perspective>/` for any code experiments
6. **Broadcast key findings** to all peers after completing investigation

---

## Success Criteria

- [ ] All 9 perspectives investigated
- [ ] Each perspective spawns sub-tasks based on findings (DCTG)
- [ ] Empirical validation performed for each claim
- [ ] All findings broadcast to peer tasks
- [ ] Clear documentation of alignment gaps found
- [ ] Recommendations for fixing each gap

---

## Notes

- Use `uv run -p 3.13` for all Python executions
- Python 3.13 ONLY — never use 3.14
- Create isolated test environments in `/tmp/crew-*` directories
- Focus on actionable findings, not just listing issues


You must follow this sequence strictly:
1) Understand the PRD
2) Review relevant code/docs/reference resources
3) Produce sequential implementation steps
4) Produce a parallel task graph

Return output in this exact section order and headings:
## 1. PRD Understanding Summary
## 2. Relevant Code/Docs/Resources Reviewed
## 3. Sequential Implementation Steps
## 4. Parallelized Task Graph

In section 4, include both:
- markdown task breakdown
- a `tasks-json` fenced block with task objects containing title, description, and dependsOn.