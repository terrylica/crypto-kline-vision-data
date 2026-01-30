---
name: feature-dev
description: Guided DSM feature development with FCP-aware architecture
---

# DSM Feature Development Workflow

Structured approach to building features for data-source-manager with FCP awareness.

## Usage

```
/feature-dev <feature description>
```

## Workflow Phases

### Phase 1: Discovery

Understand the feature request:

1. Clarify what needs to be built
2. Identify which data source layers are affected:
   - Cache layer (Arrow files)
   - Vision API layer (historical data)
   - REST API layer (real-time data)
   - FCP orchestration
3. Determine market type impact (SPOT, FUTURES_USDT, FUTURES_COIN)
4. Summarize understanding and confirm

### Phase 2: Codebase Exploration

Explore relevant existing code:

**Key areas to investigate:**

- `src/data_source_manager/core/sync/data_source_manager.py` - FCP logic
- `src/data_source_manager/core/providers/binance/` - Provider implementations
- `src/data_source_manager/utils/` - Utilities and helpers

**Use subagents to explore:**

- "Find similar features and trace implementation"
- "Map the FCP data flow for this feature area"
- "Analyze error handling patterns in related code"

### Phase 3: Clarifying Questions

Fill in gaps before designing:

**DSM-specific questions to consider:**

1. Does this affect FCP fallback order?
2. What happens if cache is corrupted?
3. How should Vision API errors be handled?
4. What about rate limiting for REST fallback?
5. Are there timezone/timestamp implications?
6. Which market types are affected?
7. What validation is needed?

### Phase 4: Architecture Design

Design the implementation:

**DSM architecture principles:**

- FCP priority: Cache → Vision → REST
- All timestamps must be UTC
- All HTTP calls need explicit timeout
- No bare except clauses
- Polars preferred over Pandas

**Create 2-3 approaches and recommend one:**

- Minimal changes approach
- Clean architecture approach
- Pragmatic balance

### Phase 5: Implementation

Build the feature:

1. Read all identified files first
2. Follow existing patterns strictly
3. Use UTC datetimes: `datetime.now(timezone.utc)`
4. Add explicit timeouts to HTTP calls
5. Handle specific exceptions (no bare except)
6. Track progress with todos

### Phase 6: Verification

Verify the implementation:

```bash
# Run these commands to verify:

# 1. Lint check
uv run -p 3.13 ruff check --fix .

# 2. Unit tests
uv run -p 3.13 pytest tests/unit/ -v

# 3. Import check
uv run -p 3.13 python -c "from data_source_manager import DataSourceManager; print('OK')"
```

**Use review agents:**

- `silent-failure-hunter` - Check error handling
- `api-reviewer` - Check API consistency
- `fcp-debugger` - Verify FCP behavior

### Phase 7: Summary

Document what was built:

1. What was implemented
2. Which FCP layers were affected
3. Key decisions made
4. Files modified
5. Next steps / follow-up work

## DSM-Specific Checks

Before marking complete, verify:

- [ ] All HTTP requests have `timeout=` parameter
- [ ] No bare `except:` clauses
- [ ] All datetimes are UTC timezone-aware
- [ ] Symbol format matches market type
- [ ] Cache invalidation handled correctly
- [ ] FCP fallback works as expected
- [ ] Unit tests added for new code
