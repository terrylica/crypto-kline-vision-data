# Task Management Utilities

This module provides a set of utilities for handling asyncio tasks with a focus on proper cancellation and cleanup patterns. The utilities implement Model-Defined Controls (MDC) best practices to ensure robust asynchronous code.

## Key Features

- **Event-based cancellation**: Alternative to timeout-based mechanisms (MDC Tier 1)
- **Task tracking and cleanup**: Prevent task leakage and resource waste (MDC Tier 1)
- **Cancellation propagation**: Ensure proper cascading cancellation (MDC Tier 1)
- **Progress tracking**: Better monitoring of task execution (MDC Tier 2)

## MDC Tier Practices

The utilities implement practices categorized into importance tiers:

- **Tier 1 (Critical)**: Practices that prevent resource leaks and ensure proper cancellation
- **Tier 2 (Important)**: Practices for better observability and control
- **Tier 3 (Nice-to-have)**: Practices that improve overall robustness

## Key Components

### Event-Based Task Waiting

```python
from utils.task_management import wait_with_cancellation

# Create a task
task = asyncio.create_task(my_coroutine())

# Set up events for control
cancel_event = asyncio.Event()
completion_event = asyncio.Event()

# Wait for task with event-based control
success = await wait_with_cancellation(
    task,
    completion_event=completion_event,
    cancel_event=cancel_event,
    timeout=5.0  # Fallback timeout
)

if success:
    print("Task completed successfully")
else:
    print("Task was cancelled or timed out")
```

### Task Tracking

```python
from utils.task_management import TaskTracker

# Create a tracker
tracker = TaskTracker()

# Create and track tasks
task1 = tracker.add(asyncio.create_task(coro1()))
task2 = tracker.add(asyncio.create_task(coro2()))

# Later, cancel all tracked tasks
success_count, error_count = await tracker.cancel_all()
print(f"Cancelled {success_count} tasks, {error_count} failures")
```

### Cancellation Propagation

```python
from utils.task_management import propagate_cancellation

# Create parent and child cancellation events
parent_cancel = asyncio.Event()
child_cancels = [asyncio.Event(), asyncio.Event()]

# Set up propagation
propagation_task = asyncio.create_task(
    propagate_cancellation(parent_cancel, child_cancels)
)

# When parent is cancelled, all children will be too
parent_cancel.set()  # This will trigger all child cancellations
```

### Lingering Task Cleanup

```python
from utils.task_management import cleanup_lingering_tasks

# After your main operation, clean up any lingering tasks
await cleanup_lingering_tasks()
```

## Best Practices

1. **Always check for cancellation**: Check both event-based and task-based cancellation

   ```python
   if cancel_event.is_set() or asyncio.current_task().cancelled():
       # Handle cancellation
   ```

2. **Set events in finally blocks**: Ensure events are set even if exceptions occur

   ```python
   try:
       # Task work
   finally:
       completion_event.set()
   ```

3. **Re-raise CancelledError**: Properly propagate cancellation signals

   ```python
   except asyncio.CancelledError:
       # Cleanup if necessary
       raise  # Re-raise to propagate
   ```

4. **Use TaskTracker for automatic removal**: Tasks are automatically removed when done
   ```python
   task_tracker = TaskTracker()
   task_tracker.add(asyncio.create_task(coroutine()))
   ```

## Full Example

See `examples/task_management_demo.py` for a comprehensive demonstration of all features.

Run this example with:

```bash
python examples/task_management_demo.py
```

You can run individual demos with:

```bash
python examples/task_management_demo.py --only event
python examples/task_management_demo.py --only propagation
python examples/task_management_demo.py --only tracking
```
