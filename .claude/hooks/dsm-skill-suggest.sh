#!/usr/bin/env bash
# DSM Skill Suggestion Hook (UserPromptSubmit)
# Analyzes user prompts and suggests relevant DSM skills
#
# Exit codes:
#   0 = success (suggestions provided via feedback)
#
# Output: JSON to stderr with feedback field

set -euo pipefail

# Get the user's prompt from stdin (JSONL format)
INPUT=$(cat)
PROMPT=$(echo "$INPUT" | jq -r '.prompt // ""' 2>/dev/null || echo "")

# Convert to lowercase for matching
PROMPT_LOWER=$(echo "$PROMPT" | tr '[:upper:]' '[:lower:]')

SUGGESTIONS=()

# dsm-usage triggers
if echo "$PROMPT_LOWER" | grep -qE 'fetch|data|klines|ohlcv|market data|datasourcemanager|binance|vision api|rest api|get_data'; then
    SUGGESTIONS+=("💡 Consider: /dsm-usage [symbol] - Fetch market data with FCP")
fi

# dsm-testing triggers
if echo "$PROMPT_LOWER" | grep -qE 'test|pytest|mock|fixture|coverage|unit test|integration test|marker'; then
    SUGGESTIONS+=("💡 Consider: /dsm-testing [pattern] - Run tests or write new tests")
fi

# dsm-fcp-monitor triggers
if echo "$PROMPT_LOWER" | grep -qE 'fcp|failover|cache (miss|hit|problem)|slow|performance|vision error|rest fallback|diagnos'; then
    SUGGESTIONS+=("💡 Consider: /dsm-fcp-monitor [symbol] - Diagnose FCP behavior")
fi

# dsm-research triggers
if echo "$PROMPT_LOWER" | grep -qE 'how does|understand|find|explore|architecture|pattern|implementation|code flow'; then
    SUGGESTIONS+=("💡 Consider: /dsm-research [topic] - Research codebase architecture")
fi

# If no suggestions or prompt is empty, exit silently
if [[ ${#SUGGESTIONS[@]} -eq 0 ]] || [[ -z "$PROMPT" ]]; then
    exit 0
fi

# Build feedback message
FEEDBACK=$(printf "%s\n" "${SUGGESTIONS[@]}")

# Output JSON response to stderr
echo "{\"feedback\": \"$FEEDBACK\"}" >&2

exit 0
