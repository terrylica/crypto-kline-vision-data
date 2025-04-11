#!/bin/bash

echo "=== Finding dangling Python scripts (not imported/used anywhere) ==="
echo "--- Production Code ---"
fdfind -e py -t f -E "__pycache__" -E "tests/*" -E "examples/*" -E "playground/*" | xargs -I{} bash -c 'file="{}"; basename=$(basename "$file" .py); if [[ ! "$file" =~ (__init__|conftest).py$ ]] && ! grep -r -l "import.*$basename" --include="*.py" . >/dev/null 2>&1 && ! grep -r -l "from.*$basename" --include="*.py" . >/dev/null 2>&1 && ! grep -r -l "$file" --include="*.sh" . >/dev/null 2>&1; then echo "  $file"; fi'

echo ""
echo "=== Finding unused code within files (running vulture) ==="
echo "--- High confidence unused code (100%) ---"
vulture . --min-confidence 100

echo ""
echo "--- Medium confidence unused code (90-99%) ---"
vulture . --min-confidence 90 --exclude 100

echo ""
echo "To analyze specific files with vulture: vulture file1.py file2.py --min-confidence 80"
