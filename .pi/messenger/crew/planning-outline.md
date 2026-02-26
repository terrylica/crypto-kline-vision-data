# Planning Outline

## 1. PRD Understanding Summary
The PRD is about **Documentation Alignment Investigation** - verifying all documentation is consistent with actual implementation. It has 9 independent investigation perspectives covering:

1. README vs Source Code API Alignment
2. CLAUDE.md Hub-and-Spoke Consistency  
3. Example Scripts API Validation
4. API Boundary Documentation (pandas/polars)
5. Exception Hierarchy Documentation
6. Internal Link Validation
7. Type Hints and Parameter Documentation
8. Streaming API Documentation Accuracy
9. Environment Variables & Configuration Docs

All 9 tasks can run **independently in parallel** as they're independent investigations.

## 2. Relevant Code/Docs/Resources Reviewed
- **Root CLAUDE.md**: Hub navigation, critical policies (Python 3.13 only), quick reference, FCP priority, API boundary, environment variables
- **README.md**: Quick start examples, API reference, WebSocket streaming, error handling, environment variables
- **src/CLAUDE.md**: Package structure (src/ckvd/), key classes, exception hierarchy
- **tests/CLAUDE.md**: Test commands, directory structure
- **docs/CLAUDE.md**: Documentation structure, ADR conventions
- **examples/CLAUDE.md**: Example files and mise tasks
- **scripts/CLAUDE.md**: Utility scripts structure
- **src/ckvd/__init__.py**: Main exports (lazy loading) - CryptoKlineVisionData, DataProvider, MarketType, Interval, ChartType, KlineUpdate, StreamConfig, KlineStream, fetch_market_data
- **utils/for_core/**: Exception files - rest_exceptions.py, vision_exceptions.py, streaming_exceptions.py

## 3. Sequential Implementation Steps
Since all 9 tasks are independent, there's no sequential dependency. However, each task should follow this investigation pattern:

1. **Start with one angle** of their assigned perspective
2. **Analyze findings** to determine next investigation step  
3. **Spawn follow-up sub-tasks** using `pi_messenger({ action: "task.create", ... })` when new angles emerge
4. **Use empirical validation** with bash commands (`uv run -p 3.13`) to verify claims
5. **Create scaffolding directories** under `/tmp/crew-<perspective>/` for any code experiments
6. **Broadcast key findings** to all peers after completing investigation

## 4. Parallelized Task Graph
---

## Tasks

### Task 1: README vs Source Code API Alignment

**Investigate**: Compare README.md examples with actual `ckvd.__init__.py` exports and `CryptoKlineVisionData` class methods.

**Validation**: Run README code snippets to verify they work with `uv run -p 3.13`.

**Key areas to check**:
- `CryptoKlineVisionData.create()` signature matches docs
- `DataProvider`, `MarketType`, `Interval` exports exist
- `fetch_market_data` function signature matches docs
- All README imports work correctly

**Broadcast**: Key API discrepancies found between README and actual implementation

Dependencies: none

---

### Task 2: CLAUDE.md Hub-and-Spoke Consistency

**Investigate**: Check all 6 main CLAUDE.md files (root, src, tests, docs, examples, scripts, playground) for cross-references, consistency of shared information (Python version 3.13, FCP priority, etc.)

**Validation**: Verify links between CLAUDE.md files work and content is non-contradictory. Check for:
- Python version consistency (should be 3.13 everywhere)
- FCP priority description consistency
- Environment variables consistency
- Import conventions consistency

**Broadcast**: Inconsistencies found in hub-and-spoke documentation

Dependencies: none

---

### Task 3: Example Scripts API Validation

**Investigate**: Test all examples in `examples/` directory — verify they import correctly and execute without errors.

**Validation**: Run each example with `uv run -p 3.13 python examples/<file>`:
- `quick_start.py`
- `ckvd_cache_control_example.py`
- `ckvd_lazy_initialization_demo.py`
- `ckvd_logging_demo.py`
- `clean_feature_engineering_example.py`
- Any others in sync/ subdirectory

**Broadcast**: Which examples fail and why (API changes, import errors, etc.)

Dependencies: none

---

### Task 4: API Boundary Documentation (pandas/polars)

**Investigate**: Verify documented behavior for `return_polars` parameter matches implementation in `crypto_kline_vision_data.py`.

**Validation**: Test both output modes and compare with docs:
- Default: pandas DataFrame (backward compatible)
- Opt-in: Polars DataFrame with `return_polars=True`

Check `CryptoKlineVisionData.get_data()` method signature and implementation.

**Broadcast**: Any discrepancies in API boundary documentation

Dependencies: none

---

### Task 5: Exception Hierarchy Documentation

**Investigate**: Compare documented exceptions in src/CLAUDE.md and README.md with actual exception classes in `utils/for_core/`.

**Validation**: Import each documented exception and verify it exists with correct attributes:
- From `rest_exceptions.py`: RateLimitError, RestAPIError
- From `vision_exceptions.py`: VisionAPIError
- From `streaming_exceptions.py`: Streaming exceptions
- Check `.details` attribute exists on all exceptions

**Broadcast**: Missing, renamed, or incorrectly documented exceptions

Dependencies: none

---

### Task 6: Internal Link Validation

**Investigate**: Check all markdown links within docs/, CLAUDE.md files for validity.

**Validation**: Use lychee or manual check to verify each link resolves to existing file/section:
- Run `lychee --config .lychee.toml docs/ CLAUDE.md src/CLAUDE.md tests/CLAUDE.md`
- Check relative vs absolute link formats

**Broadcast**: Broken or incorrect links found

Dependencies: none

---

### Task 7: Type Hints and Parameter Documentation

**Investigate**: Compare documented function signatures (in README, CLAUDE.md) with actual type hints in source files.

**Validation**: Run type checker or inspect actual signatures:
- Check `CryptoKlineVisionData.get_data()` parameter types
- Check `fetch_market_data()` parameter types
- Compare with docs - look for missing parameters, wrong types, optional/required mismatches

**Broadcast**: Parameter type mismatches or missing parameters

Dependencies: none

---

### Task 8: Streaming API Documentation Accuracy

**Investigate**: Compare README streaming examples and src/CLAUDE.md streaming section with actual `KlineStream`, `KlineUpdate`, `StreamConfig` implementations.

**Validation**: Test streaming code if environment permits:
- Check `KlineUpdate` dataclass fields: symbol, open_price, high_price, low_price, close_price, volume, is_closed, timestamp
- Check `StreamConfig` attributes: symbols, interval, buffer_size
- Check `KlineStream` methods: stream_data(), stream_data_sync(), close()
- Compare with README code examples

**Broadcast**: Streaming API documentation vs implementation gaps

Dependencies: none

---

### Task 9: Environment Variables & Configuration Docs

**Investigate**: Verify all documented environment variables (`CKVD_LOG_LEVEL`, `CKVD_ENABLE_CACHE`, `CKVD_USE_POLARS_OUTPUT`) are actually used in code.

**Validation**: Grep source for env var usage, test with wrong values:
- `grep -r "CKVD_LOG_LEVEL" src/ckvd/`
- `grep -r "CKVD_ENABLE_CACHE" src/ckvd/`
- `grep -r "CKVD_USE_POLARS_OUTPUT" src/ckvd/`
- Check config.py for how they're read
- Test behavior with invalid values

**Broadcast**: Undocumented or non-functional env vars

Dependencies: none

---

```tasks-json
[
  {
    "title": "README vs Source Code API Alignment",
    "description": "Compare README.md examples with actual ckvd.__init__.py exports and CryptoKlineVisionData class methods. Run README code snippets to verify they work with uv run -p 3.13. Check CryptoKlineVisionData.create() signature, DataProvider/MarketType/Interval exports, fetch_market_data function signature. Broadcast any key API discrepancies found between README and actual implementation.",
    "dependsOn": []
  },
  {
    "title": "CLAUDE.md Hub-and-Spoke Consistency",
    "description": "Check all 6 main CLAUDE.md files (root, src, tests, docs, examples, scripts, playground) for cross-references, consistency of shared information (Python version 3.13, FCP priority, etc.). Verify links between CLAUDE.md files work and content is non-contradictory. Check Python version consistency, FCP priority description, environment variables, and import conventions. Broadcast any inconsistencies found.",
    "dependsOn": []
  },
  {
    "title": "Example Scripts API Validation",
    "description": "Test all examples in examples/ directory - verify they import correctly and execute without errors. Run each example with uv run -p 3.13 python examples/<file>: quick_start.py, ckvd_cache_control_example.py, ckvd_lazy_initialization_demo.py, ckvd_logging_demo.py, clean_feature_engineering_example.py, and any in sync/ subdirectory. Broadcast which examples fail and why (API changes, import errors, etc.).",
    "dependsOn": []
  },
  {
    "title": "API Boundary Documentation (pandas/polars)",
    "description": "Verify documented behavior for return_polars parameter matches implementation in crypto_kline_vision_data.py. Test both output modes: Default pandas DataFrame (backward compatible) and Opt-in Polars DataFrame with return_polars=True. Check CryptoKlineVisionData.get_data() method signature and implementation. Broadcast any discrepancies in API boundary documentation.",
    "dependsOn": []
  },
  {
    "title": "Exception Hierarchy Documentation",
    "description": "Compare documented exceptions in src/CLAUDE.md and README.md with actual exception classes in utils/for_core/. Import each documented exception and verify it exists with correct attributes: RateLimitError and RestAPIError from rest_exceptions.py, VisionAPIError from vision_exceptions.py, streaming exceptions from streaming_exceptions.py. Check .details attribute exists on all exceptions. Broadcast missing, renamed, or incorrectly documented exceptions.",
    "dependsOn": []
  },
  {
    "title": "Internal Link Validation",
    "description": "Check all markdown links within docs/, CLAUDE.md files for validity. Use lychee or manual check to verify each link resolves to existing file/section. Run lychee --config .lychee.toml docs/ CLAUDE.md src/CLAUDE.md tests/CLAUDE.md. Check relative vs absolute link formats. Broadcast broken or incorrect links found.",
    "dependsOn": []
  },
  {
    "title": "Type Hints and Parameter Documentation",
    "description": "Compare documented function signatures (in README, CLAUDE.md) with actual type hints in source files. Run type checker or inspect actual signatures: check CryptoKlineVisionData.get_data() parameter types, fetch_market_data() parameter types. Compare with docs - look for missing parameters, wrong types, optional/required mismatches. Broadcast parameter type mismatches or missing parameters.",
    "dependsOn": []
  },
  {
    "title": "Streaming API Documentation Accuracy",
    "description": "Compare README streaming examples and src/CLAUDE.md streaming section with actual KlineStream, KlineUpdate, StreamConfig implementations. Test streaming code if environment permits: check KlineUpdate dataclass fields (symbol, open_price, high_price, low_price, close_price, volume, is_closed, timestamp), StreamConfig attributes (symbols, interval, buffer_size), KlineStream methods (stream_data(), stream_data_sync(), close()). Compare with README code examples. Broadcast streaming API documentation vs implementation gaps.",
    "dependsOn": []
  },
  {
    "title": "Environment Variables & Configuration Docs",
    "description": "Verify all documented environment variables (CKVD_LOG_LEVEL, CKVD_ENABLE_CACHE, CKVD_USE_POLARS_OUTPUT) are actually used in code. Grep source for env var usage: grep -r CKVD_LOG_LEVEL src/ckvd/, grep -r CKVD_ENABLE_CACHE src/ckvd/, grep -r CKVD_USE_POLARS_OUTPUT src/ckvd/. Check config.py for how they're read. Test behavior with invalid values. Broadcast undocumented or non-functional env vars.",
    "dependsOn": []
  }
]
```
