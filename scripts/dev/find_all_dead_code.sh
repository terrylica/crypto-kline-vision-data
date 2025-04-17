#!/bin/bash

# Define colors using tput for better compatibility
RED=$(tput setaf 1)
GREEN=$(tput setaf 2)
YELLOW=$(tput setaf 3)
BLUE=$(tput setaf 4)
MAGENTA=$(tput setaf 5)
CYAN=$(tput setaf 6)
BOLD=$(tput bold)
RESET=$(tput sgr0)

echo "${BOLD}${BLUE}=== Finding dangling Python scripts (not imported/used anywhere) ===${RESET}"
echo "${BOLD}${CYAN}--- Production Code ---${RESET}"
fdfind -e py -t f -E "__pycache__" -E "tests/*" -E "examples/*" -E "playground/*" -E "scripts/*" | xargs -I{} bash -c 'file="{}"; 
  basename=$(basename "$file" .py); 
  module_path=$(echo "$file" | sed "s/\.py$//; s/\//./g"); 
  if [[ ! "$file" =~ (__init__|conftest).py$ ]] && 
     ! grep -r -l "import.*$basename" --include="*.py" . >/dev/null 2>&1 && 
     ! grep -r -l "from.*$basename" --include="*.py" . >/dev/null 2>&1 && 
     ! grep -r -l "import.*$module_path" --include="*.py" . >/dev/null 2>&1 && 
     ! grep -r -l "from.*$module_path" --include="*.py" . >/dev/null 2>&1 && 
     ! grep -r -l "$file" --include="*.sh" . >/dev/null 2>&1 && 
     ! grep -r -l "python[23]* -m $module_path" --include="*.sh" . >/dev/null 2>&1 && 
     ! grep -r -l "python[23]* $file" --include="*.sh" . >/dev/null 2>&1 && 
     ! grep -r -l "scripts\..*\.$basename" --include="*.sh" . >/dev/null 2>&1; 
  then 
     echo -n "  "; 
     tput setaf 3;  # Yellow
     echo "$file"; 
     tput sgr0;  # Reset
  fi'

echo ""
echo "${BOLD}${BLUE}=== Finding unused code within files (running vulture) ===${RESET}"
echo "${BOLD}${RED}--- High confidence unused code (100%) ---${RESET}"
# Run vulture and format its output with proper line breaks
vulture . --min-confidence 100 | while IFS= read -r line; do
  if [[ $line == *".py:"* ]]; then
    file_part=$(echo "$line" | cut -d':' -f1)
    rest_part=$(echo "$line" | cut -d':' -f2-)
    echo "${YELLOW}${file_part}${RESET}:${rest_part}"
  else
    echo "$line"
  fi
done

echo ""
echo "${BOLD}${MAGENTA}--- Medium confidence unused code (90-99%) ---${RESET}"
# Run vulture for medium confidence (90-99%), ensuring we exclude high confidence (100%)
# Store results in a temporary file
tmp_file=$(mktemp)
vulture . --min-confidence 90 | grep -v "(100% confidence)" > "$tmp_file" || true

# Process the filtered results
if [ -s "$tmp_file" ]; then
  while IFS= read -r line; do
    if [[ $line == *".py:"* ]]; then
      file_part=$(echo "$line" | cut -d':' -f1)
      rest_part=$(echo "$line" | cut -d':' -f2-)
      echo "${YELLOW}${file_part}${RESET}:${rest_part}"
    else
      echo "$line"
    fi
  done < "$tmp_file"
else
  echo "${CYAN}No medium confidence issues found.${RESET}"
fi

# Clean up
rm -f "$tmp_file"

echo ""
echo "${BOLD}${GREEN}To analyze specific files with vulture: ${RESET}${CYAN}vulture file1.py file2.py --min-confidence 80${RESET}"

# Add a summary count at the end
echo ""
echo "${BOLD}${BLUE}=== Summary ===${RESET}"
dangling_count=$(fdfind -e py -t f -E "__pycache__" -E "tests/*" -E "examples/*" -E "playground/*" -E "scripts/*" | xargs -I{} bash -c 'file="{}"; 
  basename=$(basename "$file" .py); 
  module_path=$(echo "$file" | sed "s/\.py$//; s/\//./g"); 
  if [[ ! "$file" =~ (__init__|conftest).py$ ]] && 
     ! grep -r -l "import.*$basename" --include="*.py" . >/dev/null 2>&1 && 
     ! grep -r -l "from.*$basename" --include="*.py" . >/dev/null 2>&1 && 
     ! grep -r -l "import.*$module_path" --include="*.py" . >/dev/null 2>&1 && 
     ! grep -r -l "from.*$module_path" --include="*.py" . >/dev/null 2>&1 && 
     ! grep -r -l "$file" --include="*.sh" . >/dev/null 2>&1 && 
     ! grep -r -l "python[23]* -m $module_path" --include="*.sh" . >/dev/null 2>&1 && 
     ! grep -r -l "python[23]* $file" --include="*.sh" . >/dev/null 2>&1 && 
     ! grep -r -l "scripts\..*\.$basename" --include="*.sh" . >/dev/null 2>&1; 
  then 
     echo "$file"; 
  fi' | wc -l)

high_count=$(vulture . --min-confidence 100 | wc -l)
med_count=$(vulture . --min-confidence 90 | grep -v "(100% confidence)" | wc -l)

echo "${BOLD}${YELLOW}Dangling Python scripts: ${dangling_count}${RESET}"
echo "${BOLD}${RED}High confidence unused code: ${high_count}${RESET}"
echo "${BOLD}${MAGENTA}Medium confidence unused code: ${med_count}${RESET}"
echo "${BOLD}${GREEN}Total issues: $((dangling_count + high_count + med_count))${RESET}"
