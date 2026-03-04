---
name: ckvd-research
description: Research CKVD codebase for implementation details, patterns, or architecture questions. TRIGGERS - how does FCP work, understand data flow, find code, explore codebase, architecture questions.
argument-hint: "[topic]"
context: fork
agent: Explore
user-invocable: true
---

# CKVD Codebase Research

Research the crypto-kline-vision-data codebase to answer questions about: $ARGUMENTS

## Research Focus

### Key Areas to Investigate

1. **FCP Implementation** (`src/ckvd/core/sync/crypto_kline_vision_data.py`)
   - How failover decisions are made
   - Cache → Vision → REST priority

2. **Provider Implementations** (`src/ckvd/core/providers/binance/`)
   - Vision API client
   - REST API client
   - Cache manager

3. **Utilities** (`src/ckvd/utils/`)
   - Market constraints and validation
   - Timestamp handling
   - DataFrame utilities

4. **Streaming Implementation** (`src/ckvd/core/streaming/`)
   - KlineStream class and stream lifecycle
   - KlineUpdate dataclass structure
   - StreamConfig configuration options
   - WebSocket client connection handling
   - Async message loop patterns

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

---

## TodoWrite Task Templates

### Template A: Investigate FCP Flow

```
1. Read src/ckvd/core/sync/crypto_kline_vision_data.py get_data() method
2. Trace cache check logic (get_cache_lazyframes in ckvd_cache_utils)
3. Trace Vision fetch logic (_fetch_from_vision)
4. Trace REST fallback logic (_fetch_from_rest)
5. Document FCP decision points with file:line references
```

### Template B: Trace Data Source

```
1. Identify the data source module (Vision, REST, Cache)
2. Read the provider client implementation
3. Trace data flow from API call to DataFrame return
4. Document error handling and retry logic
5. Summarize with file:line references
```

### Template C: Map Exception Handling

```
1. Read rest_exceptions.py and vision_exceptions.py
2. Grep for exception catch/raise patterns in core/
3. Map which exceptions trigger FCP fallback
4. Identify any silent failures or bare excepts
5. Document exception flow with file:line references
```

### Template D: Investigate Streaming Architecture

```
1. Read src/ckvd/core/streaming/kline_stream.py (KlineStream class)
2. Understand stream lifecycle: create → subscribe → iterate → close
3. Research KlineUpdate dataclass (fields: open_time, close, open, high, low, volume, symbol, interval, is_closed)
4. Trace async context manager patterns (__aenter__, __aexit__)
5. Study WebSocket client initialization and message handling
6. Document stream state management and error handling
7. Summarize async patterns with file:line references
```

---

## Post-Change Checklist

After modifying this skill:

- [ ] Key areas to investigate still match actual file paths
- [ ] Research instructions reference available tools
- [ ] Append changes to [references/evolution-log.md](./references/evolution-log.md)

---

## References

- @references/evolution-log.md - Skill change history
