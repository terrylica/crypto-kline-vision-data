---
name: dsm-research
description: Research DSM codebase for implementation details, patterns, or architecture questions. TRIGGERS - how does FCP work, understand data flow, find code, explore codebase, architecture questions.
argument-hint: "[topic]"
context: fork
agent: Explore
user-invocable: true
---

# DSM Codebase Research

Research the data-source-manager codebase to answer questions about: $ARGUMENTS

## Research Focus

### Key Areas to Investigate

1. **FCP Implementation** (`src/data_source_manager/core/sync/data_source_manager.py`)
   - How failover decisions are made
   - Cache → Vision → REST priority

2. **Provider Implementations** (`src/data_source_manager/core/providers/binance/`)
   - Vision API client
   - REST API client
   - Cache manager

3. **Utilities** (`src/data_source_manager/utils/`)
   - Market constraints and validation
   - Timestamp handling
   - DataFrame utilities

## Research Instructions

1. Use Glob to find relevant files
2. Use Grep to search for specific patterns
3. Read key files to understand implementation
4. Summarize findings with specific file:line references

## Expected Output

Provide:

- **Summary**: Brief answer to the research question
- **Key Files**: Files most relevant to the topic
- **Code References**: Specific file:line references
- **Related Topics**: Other areas worth investigating
