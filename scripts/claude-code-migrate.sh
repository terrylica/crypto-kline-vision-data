#!/usr/bin/env bash
# =============================================================================
# Claude Code Session Migration Script
# =============================================================================
# Migrates Claude Code project context when renaming a local directory.
#
# Problem: Claude Code keys all session data by the absolute directory path.
#          Renaming the directory breaks the association with all historical
#          sessions, auto-memory, and history entries.
#
# Solution: Move Claude Code project data to match the new directory name,
#           rewrite path references in index files, and create a symlink
#           for backward compatibility.
#
# What this script does:
#   Phase 1: Pre-flight validation (nothing modified)
#   Phase 2: Backup original data
#   Phase 3: Move project directory in ~/.claude/projects/
#   Phase 4: Rewrite sessions-index.json (projectPath, fullPath, originalPath)
#   Phase 5: Rewrite history.jsonl (project field)
#   Phase 6: Create backward-compatibility symlink
#   Phase 7: Rename the actual repo directory
#   Phase 8: Post-flight verification
#
# Usage:
#   # Dry run (no changes):
#   bash scripts/claude-code-migrate.sh --dry-run
#
#   # Production run:
#   bash scripts/claude-code-migrate.sh
#
# Rollback:
#   bash scripts/claude-code-migrate.sh --rollback
#
# Prerequisites:
#   - All Claude Code sessions for this project must be closed
#   - python3 available in PATH
# =============================================================================

set -euo pipefail

# ── Configuration ──────────────────────────────────────────────────────────────
OLD_DIR="/Users/terryli/eon/data-source-manager"
NEW_DIR="/Users/terryli/eon/crypto-kline-vision-data"

# Claude Code encodes paths by replacing / and . with -
OLD_ENCODED="-Users-terryli-eon-data-source-manager"
NEW_ENCODED="-Users-terryli-eon-crypto-kline-vision-data"

CLAUDE_DIR="$HOME/.claude"
PROJECTS_DIR="${CLAUDE_DIR}/projects"
HISTORY_FILE="${CLAUDE_DIR}/history.jsonl"

BACKUP_DIR="${CLAUDE_DIR}/migration-backup-$(date +%Y%m%d-%H%M%S)"

# ── Argument parsing ──────────────────────────────────────────────────────────
DRY_RUN=false
ROLLBACK=false
for arg in "$@"; do
    case "$arg" in
        --dry-run) DRY_RUN=true ;;
        --rollback) ROLLBACK=true ;;
        *) echo "Unknown argument: $arg"; exit 1 ;;
    esac
done

# ── Colors ─────────────────────────────────────────────────────────────────────
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

pass()  { echo -e "  ${GREEN}[PASS]${NC} $1"; }
fail()  { echo -e "  ${RED}[FAIL]${NC} $1"; }
info()  { echo -e "  ${BLUE}[INFO]${NC} $1"; }
warn()  { echo -e "  ${YELLOW}[WARN]${NC} $1"; }
phase() { echo -e "\n${YELLOW}═══ $1 ═══${NC}"; }

# ── Rollback mode ──────────────────────────────────────────────────────────────
if [[ "${ROLLBACK}" == "true" ]]; then
    phase "ROLLBACK MODE"

    # Find most recent backup
    LATEST_BACKUP=$(find "${CLAUDE_DIR}" -maxdepth 1 -name 'migration-backup-*' -type d | sort -r | head -1)

    if [[ -z "${LATEST_BACKUP}" ]]; then
        fail "No backup found in ${CLAUDE_DIR}/migration-backup-*"
        exit 1
    fi

    info "Found backup: ${LATEST_BACKUP}"

    # Restore project directory
    if [[ -d "${LATEST_BACKUP}/projects/${OLD_ENCODED}" ]]; then
        # Remove new directory and symlink if they exist
        rm -f "${PROJECTS_DIR}/${OLD_ENCODED}" 2>/dev/null || true
        rm -rf "${PROJECTS_DIR:?}/${NEW_ENCODED:?}" 2>/dev/null || true
        cp -a "${LATEST_BACKUP}/projects/${OLD_ENCODED}" "${PROJECTS_DIR}/${OLD_ENCODED}"
        pass "Restored project directory"
    fi

    # Restore history.jsonl
    if [[ -f "${LATEST_BACKUP}/history.jsonl" ]]; then
        cp "${LATEST_BACKUP}/history.jsonl" "${HISTORY_FILE}"
        pass "Restored history.jsonl"
    fi

    # Restore repo directory if it was moved
    if [[ -d "${NEW_DIR}" && ! -d "${OLD_DIR}" ]]; then
        mv "${NEW_DIR}" "${OLD_DIR}"
        pass "Restored repo directory: ${OLD_DIR}"
    fi

    echo ""
    pass "Rollback complete. Backup preserved at: ${LATEST_BACKUP}"
    exit 0
fi

# ── Header ─────────────────────────────────────────────────────────────────────
echo ""
echo "============================================================="
if [[ "${DRY_RUN}" == "true" ]]; then
    echo " Claude Code Session Migration — DRY RUN (no changes)"
else
    echo " Claude Code Session Migration — PRODUCTION"
fi
echo "============================================================="
echo ""
echo " From: ${OLD_DIR}"
echo " To:   ${NEW_DIR}"
echo ""

# ══════════════════════════════════════════════════════════════════════════════
# Phase 1: Pre-flight Validation
# ══════════════════════════════════════════════════════════════════════════════
phase "Phase 1: Pre-flight Validation"

PREFLIGHT_FAIL=0

# Check source directory exists
if [[ -d "${OLD_DIR}" ]]; then
    pass "Source directory exists: ${OLD_DIR}"
else
    fail "Source directory NOT found: ${OLD_DIR}"
    PREFLIGHT_FAIL=1
fi

# Check target directory does NOT exist
if [[ ! -d "${NEW_DIR}" ]]; then
    pass "Target directory does not exist yet: ${NEW_DIR}"
else
    fail "Target directory already exists: ${NEW_DIR}"
    PREFLIGHT_FAIL=1
fi

# Check Claude Code project directory exists
if [[ -d "${PROJECTS_DIR}/${OLD_ENCODED}" ]]; then
    SESSION_COUNT=$(find "${PROJECTS_DIR}/${OLD_ENCODED}" -maxdepth 1 -name '*.jsonl' -type f | wc -l | tr -d ' ')
    pass "Claude Code project directory found: ${SESSION_COUNT} sessions"
else
    fail "Claude Code project directory NOT found: ${PROJECTS_DIR}/${OLD_ENCODED}"
    PREFLIGHT_FAIL=1
fi

# Check target Claude Code project directory does NOT exist
if [[ ! -d "${PROJECTS_DIR}/${NEW_ENCODED}" ]]; then
    pass "Target Claude Code project directory does not exist yet"
else
    fail "Target Claude Code project directory already exists: ${PROJECTS_DIR}/${NEW_ENCODED}"
    PREFLIGHT_FAIL=1
fi

# Check history.jsonl exists
if [[ -f "${HISTORY_FILE}" ]]; then
    HISTORY_COUNT=$(grep -c "\"${OLD_DIR}\"" "${HISTORY_FILE}" || true)
    pass "history.jsonl found: ${HISTORY_COUNT} entries to migrate"
else
    fail "history.jsonl NOT found: ${HISTORY_FILE}"
    PREFLIGHT_FAIL=1
fi

# Check python3 available
if command -v python3 &> /dev/null; then
    pass "python3 available"
else
    fail "python3 not found in PATH"
    PREFLIGHT_FAIL=1
fi

# Check no Claude Code sessions are running for this project
RUNNING=$(pgrep -f "claude.*data-source-manager" 2>/dev/null || true)
if [[ -z "${RUNNING}" ]]; then
    pass "No Claude Code sessions running for this project"
else
    warn "Claude Code may be running for this project — close all sessions first"
    PREFLIGHT_FAIL=1
fi

if [[ "${PREFLIGHT_FAIL}" -ne 0 ]]; then
    echo ""
    fail "Pre-flight failed. Fix issues above before proceeding."
    exit 1
fi

if [[ "${DRY_RUN}" == "true" ]]; then
    echo ""
    info "DRY RUN — showing what would happen without making changes"
fi

# ══════════════════════════════════════════════════════════════════════════════
# Phase 2: Backup
# ══════════════════════════════════════════════════════════════════════════════
phase "Phase 2: Backup"

if [[ "${DRY_RUN}" == "true" ]]; then
    info "Would backup to: ${BACKUP_DIR}/"
    info "Would copy: ${PROJECTS_DIR}/${OLD_ENCODED}/ (${SESSION_COUNT} sessions)"
    info "Would copy: ${HISTORY_FILE} (${HISTORY_COUNT} entries)"
else
    mkdir -p "${BACKUP_DIR}/projects"

    # Backup project directory
    cp -a "${PROJECTS_DIR}/${OLD_ENCODED}" "${BACKUP_DIR}/projects/${OLD_ENCODED}"
    pass "Backed up project directory (${SESSION_COUNT} sessions)"

    # Backup history.jsonl
    cp "${HISTORY_FILE}" "${BACKUP_DIR}/history.jsonl"
    pass "Backed up history.jsonl"

    info "Backup location: ${BACKUP_DIR}"
fi

# ══════════════════════════════════════════════════════════════════════════════
# Phase 3: Move project directory
# ══════════════════════════════════════════════════════════════════════════════
phase "Phase 3: Move Claude Code project directory"

if [[ "${DRY_RUN}" == "true" ]]; then
    info "Would move: ${OLD_ENCODED} → ${NEW_ENCODED}"
else
    mv "${PROJECTS_DIR}/${OLD_ENCODED}" "${PROJECTS_DIR}/${NEW_ENCODED}"
    pass "Renamed project directory"
fi

# ══════════════════════════════════════════════════════════════════════════════
# Phase 4: Rewrite sessions-index.json
# ══════════════════════════════════════════════════════════════════════════════
phase "Phase 4: Rewrite sessions-index.json"

INDEX_FILE="${PROJECTS_DIR}/${NEW_ENCODED}/sessions-index.json"
if [[ "${DRY_RUN}" == "true" ]]; then
    INDEX_FILE="${PROJECTS_DIR}/${OLD_ENCODED}/sessions-index.json"
fi

if [[ -f "${INDEX_FILE}" ]]; then
    if [[ "${DRY_RUN}" == "true" ]]; then
        REFS=$(grep -c "data-source-manager" "${INDEX_FILE}" || true)
        info "Would rewrite ${REFS} path references in sessions-index.json"
    else
        python3 << 'PYEOF'
import json

old_encoded = "-Users-terryli-eon-data-source-manager"
new_encoded = "-Users-terryli-eon-crypto-kline-vision-data"
old_dir = "/Users/terryli/eon/data-source-manager"
new_dir = "/Users/terryli/eon/crypto-kline-vision-data"

projects_dir = "/Users/terryli/.claude/projects"
index_path = f"{projects_dir}/{new_encoded}/sessions-index.json"

with open(index_path, 'r') as f:
    data = json.load(f)

changes = 0

# Top-level fields
for key in ("originalPath", "projectPath"):
    if key in data and isinstance(data[key], str) and old_dir in data[key]:
        data[key] = data[key].replace(old_dir, new_dir)
        changes += 1

# Per-entry fields
for entry in data.get("entries", []):
    if "projectPath" in entry and old_dir in entry["projectPath"]:
        entry["projectPath"] = entry["projectPath"].replace(old_dir, new_dir)
        changes += 1
    if "fullPath" in entry and old_encoded in entry["fullPath"]:
        entry["fullPath"] = entry["fullPath"].replace(old_encoded, new_encoded)
        changes += 1

with open(index_path, 'w') as f:
    json.dump(data, f, indent=4)
    f.write('\n')

print(f"{changes}")
PYEOF

        # Verify zero old references remain
        REMAIN=$(grep -c "data-source-manager" "${PROJECTS_DIR}/${NEW_ENCODED}/sessions-index.json" || true)
        if [[ "${REMAIN}" -eq 0 ]]; then
            pass "sessions-index.json: all paths updated"
        else
            fail "sessions-index.json: ${REMAIN} old references remain"
        fi

        # Validate JSON integrity
        if python3 -m json.tool "${PROJECTS_DIR}/${NEW_ENCODED}/sessions-index.json" > /dev/null 2>&1; then
            pass "sessions-index.json: valid JSON"
        else
            fail "sessions-index.json: INVALID JSON — run --rollback"
            exit 1
        fi
    fi
else
    info "No sessions-index.json found (skipping)"
fi

# ══════════════════════════════════════════════════════════════════════════════
# Phase 5: Rewrite history.jsonl
# ══════════════════════════════════════════════════════════════════════════════
phase "Phase 5: Rewrite history.jsonl"

if [[ "${DRY_RUN}" == "true" ]]; then
    info "Would rewrite ${HISTORY_COUNT} entries in history.jsonl"
else
    python3 << 'PYEOF'
import json

old_dir = "/Users/terryli/eon/data-source-manager"
new_dir = "/Users/terryli/eon/crypto-kline-vision-data"
history_path = "/Users/terryli/.claude/history.jsonl"
output_path = "/Users/terryli/.claude/history_migration_tmp.jsonl"

changes = 0
total = 0
errors = 0

with open(history_path, 'r') as fin, open(output_path, 'w') as fout:
    for line in fin:
        total += 1
        line = line.rstrip('\n')
        if not line:
            fout.write('\n')
            continue
        try:
            entry = json.loads(line)
            if entry.get("project") == old_dir:
                entry["project"] = new_dir
                changes += 1
            fout.write(json.dumps(entry, ensure_ascii=False) + '\n')
        except json.JSONDecodeError:
            errors += 1
            fout.write(line + '\n')

print(f"{changes}")
if errors > 0:
    print(f"ERRORS:{errors}", file=__import__('sys').stderr)
PYEOF

    # Atomic replace
    mv "${CLAUDE_DIR}/history_migration_tmp.jsonl" "${HISTORY_FILE}"

    # Verify
    NEW_COUNT=$(grep -c "\"${NEW_DIR}\"" "${HISTORY_FILE}" || true)
    OLD_REMAIN=$(grep -c "\"${OLD_DIR}\"" "${HISTORY_FILE}" || true)

    if [[ "${NEW_COUNT}" -eq "${HISTORY_COUNT}" ]]; then
        pass "history.jsonl: ${NEW_COUNT} entries migrated"
    else
        warn "history.jsonl: expected ${HISTORY_COUNT}, got ${NEW_COUNT}"
    fi

    if [[ "${OLD_REMAIN}" -eq 0 ]]; then
        pass "history.jsonl: zero old references remain"
    else
        fail "history.jsonl: ${OLD_REMAIN} old references remain"
    fi
fi

# ══════════════════════════════════════════════════════════════════════════════
# Phase 6: Create backward-compatibility symlink
# ══════════════════════════════════════════════════════════════════════════════
phase "Phase 6: Backward-compatibility symlink"

if [[ "${DRY_RUN}" == "true" ]]; then
    info "Would create symlink: ${OLD_ENCODED} → ${NEW_ENCODED}"
else
    ln -s "${PROJECTS_DIR}/${NEW_ENCODED}" "${PROJECTS_DIR}/${OLD_ENCODED}"
    if [[ -L "${PROJECTS_DIR}/${OLD_ENCODED}" ]]; then
        pass "Symlink created: ${OLD_ENCODED} → ${NEW_ENCODED}"
    else
        warn "Symlink creation failed (non-critical)"
    fi
fi

# ══════════════════════════════════════════════════════════════════════════════
# Phase 7: Rename the actual repo directory
# ══════════════════════════════════════════════════════════════════════════════
phase "Phase 7: Rename repository directory"

if [[ "${DRY_RUN}" == "true" ]]; then
    info "Would move: ${OLD_DIR} → ${NEW_DIR}"
else
    mv "${OLD_DIR}" "${NEW_DIR}"
    if [[ -d "${NEW_DIR}" ]]; then
        pass "Repository renamed: ${NEW_DIR}"
    else
        fail "Repository rename failed — run --rollback"
        exit 1
    fi
fi

# ══════════════════════════════════════════════════════════════════════════════
# Phase 8: Post-flight Verification
# ══════════════════════════════════════════════════════════════════════════════
phase "Phase 8: Post-flight Verification"

if [[ "${DRY_RUN}" == "true" ]]; then
    info "Would verify: directory, symlink, sessions-index, history, memory"
    echo ""
    echo "============================================================="
    echo -e "${GREEN} DRY RUN COMPLETE — No changes made${NC}"
    echo "============================================================="
    echo ""
    echo " To execute: bash ${NEW_DIR}/scripts/claude-code-migrate.sh"
    echo " To rollback: bash ${NEW_DIR}/scripts/claude-code-migrate.sh --rollback"
    exit 0
fi

VERIFY_FAIL=0

# Repo directory
if [[ -d "${NEW_DIR}" && ! -d "${OLD_DIR}" ]]; then
    pass "Repo directory: renamed correctly"
else
    fail "Repo directory: issue detected"
    VERIFY_FAIL=1
fi

# Claude Code project directory
if [[ -d "${PROJECTS_DIR}/${NEW_ENCODED}" ]]; then
    pass "Claude Code project: exists at new path"
else
    fail "Claude Code project: missing at new path"
    VERIFY_FAIL=1
fi

# Symlink
if [[ -L "${PROJECTS_DIR}/${OLD_ENCODED}" ]]; then
    pass "Backward-compat symlink: active"
else
    warn "Backward-compat symlink: missing (non-critical)"
fi

# Memory
if [[ -f "${PROJECTS_DIR}/${NEW_ENCODED}/memory/MEMORY.md" ]]; then
    pass "Auto-memory: preserved"
else
    warn "Auto-memory: not found (may not have existed)"
fi

# Session count
FINAL_SESSION_COUNT=$(find "${PROJECTS_DIR}/${NEW_ENCODED}" -maxdepth 1 -name '*.jsonl' -type f | wc -l | tr -d ' ')
if [[ "${FINAL_SESSION_COUNT}" -eq "${SESSION_COUNT}" ]]; then
    pass "Sessions: ${FINAL_SESSION_COUNT} preserved"
else
    fail "Sessions: expected ${SESSION_COUNT}, got ${FINAL_SESSION_COUNT}"
    VERIFY_FAIL=1
fi

# ── Summary ────────────────────────────────────────────────────────────────────
echo ""
echo "============================================================="
if [[ "${VERIFY_FAIL}" -eq 0 ]]; then
    echo -e "${GREEN} MIGRATION COMPLETE${NC}"
else
    echo -e "${RED} MIGRATION COMPLETED WITH WARNINGS${NC}"
fi
echo "============================================================="
echo ""
echo " Repository:    ${NEW_DIR}"
echo " Sessions:      ${FINAL_SESSION_COUNT} migrated"
echo " History:       ${HISTORY_COUNT} entries updated"
echo " Backup:        ${BACKUP_DIR}"
echo ""
echo " Next steps:"
echo "   1. cd ${NEW_DIR}"
echo "   2. Open a new Claude Code session to verify context loads"
echo "   3. If issues arise: bash ${NEW_DIR}/scripts/claude-code-migrate.sh --rollback"
echo "   4. Update git remote if needed: git remote set-url origin <new-url>"
echo ""

exit "${VERIFY_FAIL}"
