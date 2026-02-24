#!/usr/bin/env python3
# ADR: docs/adr/2025-01-30-failover-control-protocol.md
"""ConnectionMachine: declarative 10-state FSM for WebSocket lifecycle.

Uses python-statemachine (v2.4+) for a fully declarative state machine that
replaces hand-rolled connection state management. Declarative FSMs eliminate
6 classes of bugs: impossible states, missing transitions, double-enter,
forgotten cleanup, untested paths, and diagram drift.

States (10):
    IDLE → CONNECTING → CONNECTED → SUBSCRIBING → ACTIVE
    ACTIVE → DRAINING → RECONNECTING → BACKOFF → CONNECTING (retry)
    RECONNECTING / BACKOFF → EXHAUSTED (max attempts)
    any → CLOSED (final)
"""

from __future__ import annotations

from statemachine import State, StateMachine
from statemachine.exceptions import TransitionNotAllowed

from ckvd.utils.for_core.streaming_exceptions import StreamReconnectExhaustedError

# Re-export for callers that need to catch invalid transition errors
__all__ = ["ConnectionMachine", "TransitionNotAllowed"]


class ConnectionMachine(StateMachine):
    """10-state WebSocket connection lifecycle FSM.

    Usage:
        machine = ConnectionMachine(max_attempts=5)
        machine.connect()       # idle → connecting
        machine.handshake_ok()  # connecting → connected
        machine.subscribe()     # connected → subscribing
        machine.subscribed()    # subscribing → active
        # ... stream messages ...
        machine.disconnect()    # active → draining
        machine.close()         # draining → closed

    On reconnect:
        machine.handshake_fail()        # connecting → reconnecting
        machine.wait()                  # reconnecting → backoff
        machine.retry()                 # backoff → connecting
        # (or) machine.give_up()        # → exhausted (raises StreamReconnectExhaustedError)

    Attributes:
        reconnect_count: Number of reconnection attempts made so far.
        max_attempts: Maximum reconnect attempts before raising exhausted.
    """

    # -------------------------------------------------------------------------
    # States
    # -------------------------------------------------------------------------
    idle = State(initial=True)
    connecting = State()
    connected = State()
    subscribing = State()
    active = State()
    draining = State()
    reconnecting = State()
    backoff = State()
    exhausted = State()
    closed = State(final=True)

    # -------------------------------------------------------------------------
    # Transitions
    # -------------------------------------------------------------------------
    connect = idle.to(connecting)
    handshake_ok = connecting.to(connected)
    handshake_fail = connecting.to(reconnecting)
    subscribe = connected.to(subscribing)
    subscribed = subscribing.to(active)
    subscribe_fail = subscribing.to(draining)
    disconnect = active.to(draining) | connected.to(draining)
    schedule_reconnect = draining.to(reconnecting)
    wait = reconnecting.to(backoff)
    retry = backoff.to(connecting)
    give_up = reconnecting.to(exhausted) | backoff.to(exhausted)
    close = (
        active.to(closed)
        | idle.to(closed)
        | exhausted.to(closed)
        | draining.to(closed)
        | connected.to(closed)
        | subscribing.to(closed)
        | reconnecting.to(closed)
        | backoff.to(closed)
    )

    # -------------------------------------------------------------------------
    # Guards / Actions
    # -------------------------------------------------------------------------

    def __init__(self, max_attempts: int = 5) -> None:
        """Initialize ConnectionMachine.

        Args:
            max_attempts: Maximum reconnect attempts before StreamReconnectExhaustedError.
        """
        self.max_attempts = max_attempts
        self.reconnect_count = 0
        super().__init__()

    def on_enter_reconnecting(self, source: str = "") -> None:
        """Increment reconnect counter when entering RECONNECTING state."""
        self.reconnect_count += 1

    def on_enter_exhausted(self) -> None:
        """Raise StreamReconnectExhaustedError when all attempts are used up."""
        raise StreamReconnectExhaustedError(
            f"WebSocket reconnect exhausted after {self.reconnect_count} attempts",
            details={
                "attempts": self.reconnect_count,
                "max_attempts": self.max_attempts,
            },
        )

    def reset_reconnect_count(self) -> None:
        """Reset the reconnect counter (call after successful reconnection to ACTIVE)."""
        self.reconnect_count = 0

    def should_give_up(self) -> bool:
        """True if the reconnect count has reached the max attempts limit."""
        return self.reconnect_count >= self.max_attempts
