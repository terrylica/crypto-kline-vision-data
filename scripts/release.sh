#!/usr/bin/env bash
# Release pipeline: preflight → semantic-release → verify → PyPI → postflight.
#   bash scripts/release.sh            full release
#   bash scripts/release.sh --dry-run  preflight + semantic-release --dry-run only
# GH_TOKEN is minted per run from the repo account's gh profile (gh-token-for-repo), never stored.
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"

dry=0
[[ "${1:-}" == "--dry-run" ]] && dry=1

GH_TOKEN="$("$HOME/.claude/tools/bin/gh-token-for-repo")"
export GH_TOKEN

bash scripts/release/preflight.sh
if [[ $dry -eq 1 ]]; then
    semantic-release --no-ci --dry-run
    exit 0
fi
semantic-release --no-ci
bash scripts/release/verify.sh
./scripts/publish-to-pypi.sh
bash scripts/release/postflight.sh
