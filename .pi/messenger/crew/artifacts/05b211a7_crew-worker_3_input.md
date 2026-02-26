# Task for crew-worker

# Task Assignment

**Task ID:** task-4
**Task Title:** API Boundary Documentation (pandas/polars)
**PRD:** PRD.md


## Your Mission

Implement this task following the crew-worker protocol:
1. Join the mesh
2. Read task spec to understand requirements
3. Start task and reserve files
4. Implement the feature
5. Commit your changes
6. Release reservations and mark complete

## Concurrent Tasks

These tasks are being worked on by other workers in this wave. Discover their agent names after joining the mesh via `pi_messenger({ action: "list" })`.

- task-1: README vs Source Code API Alignment
- task-2: CLAUDE.md Hub-and-Spoke Consistency
- task-3: Example Scripts API Validation
- task-5: Exception Hierarchy Documentation
- task-6: Internal Link Validation
- task-7: Type Hints and Parameter Documentation
- task-8: Streaming API Documentation Accuracy
- task-9: Environment Variables & Configuration Docs

## Task Specification

# API Boundary Documentation (pandas/polars)

Verify documented behavior for return_polars parameter matches implementation in crypto_kline_vision_data.py. Test both output modes: Default pandas DataFrame (backward compatible) and Opt-in Polars DataFrame with return_polars=True. Check CryptoKlineVisionData.get_data() method signature and implementation. Broadcast any discrepancies in API boundary documentation.


## Plan Context

Now I have enough information to create the task breakdown. Let me summarize my findings and create the task structure.

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
3. **Spawn fol

[Spec truncated - read full spec from .pi/messenger/crew/plan.md]
## Coordination

**Message budget: 10 messages this session.** The system enforces this — sends are rejected after the limit.

**Broadcasts go to the team feed — only the user sees them live.** Other workers see your broadcasts in their initial context only. Use DMs for time-sensitive peer coordination.

### Announce yourself
After joining the mesh and starting your task, announce what you're working on:

```typescript
pi_messenger({ action: "broadcast", message: "Starting <task-id> (<title>) — will create <files>" })
```

### Coordinate with peers
If a concurrent task involves files or interfaces related to yours, send a brief DM. Only message when there's a concrete coordination need — shared files, interfaces, or blocking questions.

```typescript
pi_messenger({ action: "send", to: "<peer-name>", message: "I'm exporting FormatOptions from types.ts — will you need it?" })
```

### Responding to messages
If a peer asks you a direct question, reply briefly. Ignore messages that don't require a response. Do NOT start casual conversations.

### On completion
Announce what you built:

```typescript
pi_messenger({ action: "broadcast", message: "Completed <task-id>: <file> exports <symbols>" })
```

### Reservations
Before editing files, check if another worker has reserved them via `pi_messenger({ action: "list" })`. If a file you need is reserved, message the owner to coordinate. Do NOT edit reserved files without coordinating first.

### Questions about dependencies
If your task depends on a completed task and something about its implementation is unclear, read the code and the task's progress log at `.pi/messenger/crew/tasks/<task-id>.progress.md`. Dependency authors are from previous waves and are no longer in the mesh.

### Claim next task
After completing your assigned task, check if there are ready tasks you can pick up:

```typescript
pi_messenger({ action: "task.ready" })
```

If a task is ready, claim and implement it. If `task.start` fails (another worker claimed it first), check for other ready tasks. Only claim if your current task completed cleanly and quickly.

