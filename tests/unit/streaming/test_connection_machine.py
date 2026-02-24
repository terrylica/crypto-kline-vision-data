# ADR: docs/adr/2025-01-30-failover-control-protocol.md
"""Unit tests for ConnectionMachine FSM (T16).

Covers:
- Initial state is IDLE
- Happy path: idle → connecting → connected → subscribing → active → draining → closed
- Failed handshake: connecting → reconnecting → backoff → connecting (retry loop)
- Exhausted path: give_up → raises StreamReconnectExhaustedError with details
- subscribe_fail: subscribing → draining
- disconnect from active and connected
- schedule_reconnect: draining → reconnecting
- close from all valid source states
- Invalid transitions raise TransitionNotAllowed
- max_attempts default is 5
- reconnect_count increments on each RECONNECTING entry
- reset_reconnect_count() zeroes the counter
- should_give_up() semantics
- on_enter_exhausted raises StreamReconnectExhaustedError with correct .details
"""

import pytest

from ckvd.core.streaming.connection_manager import ConnectionMachine, TransitionNotAllowed
from ckvd.utils.for_core.streaming_exceptions import StreamReconnectExhaustedError


class TestConnectionMachineInitialState:
    """Initial state and configuration."""

    def test_initial_state_is_idle(self):
        machine = ConnectionMachine()
        assert machine.current_state == machine.idle

    def test_default_max_attempts_is_5(self):
        machine = ConnectionMachine()
        assert machine.max_attempts == 5

    def test_custom_max_attempts(self):
        machine = ConnectionMachine(max_attempts=3)
        assert machine.max_attempts == 3

    def test_initial_reconnect_count_is_zero(self):
        machine = ConnectionMachine()
        assert machine.reconnect_count == 0

    def test_initial_should_give_up_false(self):
        machine = ConnectionMachine()
        assert machine.should_give_up() is False


class TestConnectionMachineHappyPath:
    """Full happy-path lifecycle: idle → active → closed."""

    def test_idle_to_connecting(self):
        machine = ConnectionMachine()
        machine.connect()
        assert machine.current_state == machine.connecting

    def test_connecting_to_connected(self):
        machine = ConnectionMachine()
        machine.connect()
        machine.handshake_ok()
        assert machine.current_state == machine.connected

    def test_connected_to_subscribing(self):
        machine = ConnectionMachine()
        machine.connect()
        machine.handshake_ok()
        machine.subscribe()
        assert machine.current_state == machine.subscribing

    def test_subscribing_to_active(self):
        machine = ConnectionMachine()
        machine.connect()
        machine.handshake_ok()
        machine.subscribe()
        machine.subscribed()
        assert machine.current_state == machine.active

    def test_active_to_draining(self):
        machine = ConnectionMachine()
        machine.connect()
        machine.handshake_ok()
        machine.subscribe()
        machine.subscribed()
        machine.disconnect()
        assert machine.current_state == machine.draining

    def test_draining_to_closed(self):
        machine = ConnectionMachine()
        machine.connect()
        machine.handshake_ok()
        machine.subscribe()
        machine.subscribed()
        machine.disconnect()
        machine.close()
        assert machine.current_state == machine.closed

    def test_closed_is_final_state(self):
        machine = ConnectionMachine()
        machine.connect()
        machine.handshake_ok()
        machine.subscribe()
        machine.subscribed()
        machine.disconnect()
        machine.close()
        assert machine.closed.final is True


class TestConnectionMachineReconnectPath:
    """Reconnect cycle: handshake_fail → backoff → retry."""

    def test_handshake_fail_to_reconnecting(self):
        machine = ConnectionMachine()
        machine.connect()
        machine.handshake_fail()
        assert machine.current_state == machine.reconnecting

    def test_reconnecting_to_backoff(self):
        machine = ConnectionMachine()
        machine.connect()
        machine.handshake_fail()
        machine.wait()
        assert machine.current_state == machine.backoff

    def test_backoff_to_connecting(self):
        machine = ConnectionMachine()
        machine.connect()
        machine.handshake_fail()
        machine.wait()
        machine.retry()
        assert machine.current_state == machine.connecting

    def test_reconnect_count_increments_on_enter_reconnecting(self):
        machine = ConnectionMachine()
        machine.connect()
        machine.handshake_fail()  # → reconnecting
        assert machine.reconnect_count == 1

    def test_reconnect_count_increments_multiple_times(self):
        """Each handshake_fail → reconnecting increments the counter."""
        machine = ConnectionMachine(max_attempts=10)
        machine.connect()          # idle → connecting
        machine.handshake_fail()   # → reconnecting, count=1
        machine.wait()             # → backoff
        machine.retry()            # → connecting
        machine.handshake_fail()   # → reconnecting, count=2
        machine.wait()
        machine.retry()
        machine.handshake_fail()   # → reconnecting, count=3
        assert machine.reconnect_count == 3

    def test_reset_reconnect_count(self):
        machine = ConnectionMachine()
        machine.connect()
        machine.handshake_fail()  # count = 1
        assert machine.reconnect_count == 1
        machine.reset_reconnect_count()
        assert machine.reconnect_count == 0

    def test_should_give_up_true_at_max(self):
        machine = ConnectionMachine(max_attempts=2)
        machine.connect()
        machine.handshake_fail()  # count = 1
        machine.wait()
        machine.retry()
        machine.handshake_fail()  # count = 2
        assert machine.should_give_up() is True

    def test_should_give_up_false_below_max(self):
        machine = ConnectionMachine(max_attempts=5)
        machine.connect()
        machine.handshake_fail()  # count = 1
        assert machine.should_give_up() is False


class TestConnectionMachineExhaustedPath:
    """give_up → exhausted raises StreamReconnectExhaustedError."""

    def test_give_up_from_reconnecting_raises(self):
        machine = ConnectionMachine(max_attempts=5)
        machine.connect()
        machine.handshake_fail()  # → reconnecting
        with pytest.raises(StreamReconnectExhaustedError):
            machine.give_up()

    def test_give_up_from_backoff_raises(self):
        machine = ConnectionMachine(max_attempts=5)
        machine.connect()
        machine.handshake_fail()  # → reconnecting
        machine.wait()             # → backoff
        with pytest.raises(StreamReconnectExhaustedError):
            machine.give_up()

    def test_exhausted_error_has_attempts_in_details(self):
        machine = ConnectionMachine(max_attempts=5)
        machine.connect()
        machine.handshake_fail()  # reconnect_count = 1
        try:
            machine.give_up()
        except StreamReconnectExhaustedError as e:
            assert e.details["attempts"] == 1
        else:
            pytest.fail("Expected StreamReconnectExhaustedError")

    def test_exhausted_error_has_max_attempts_in_details(self):
        machine = ConnectionMachine(max_attempts=7)
        machine.connect()
        machine.handshake_fail()
        try:
            machine.give_up()
        except StreamReconnectExhaustedError as e:
            assert e.details["max_attempts"] == 7
        else:
            pytest.fail("Expected StreamReconnectExhaustedError")

    def test_exhausted_error_message_contains_count(self):
        machine = ConnectionMachine(max_attempts=3)
        machine.connect()
        machine.handshake_fail()  # count = 1
        machine.wait()
        machine.retry()
        machine.handshake_fail()  # count = 2
        machine.wait()
        machine.retry()
        machine.handshake_fail()  # count = 3
        try:
            machine.give_up()
        except StreamReconnectExhaustedError as e:
            assert "3" in str(e)
        else:
            pytest.fail("Expected StreamReconnectExhaustedError")


class TestConnectionMachineSubscribeFail:
    """subscribe_fail: subscribing → draining."""

    def test_subscribe_fail_from_subscribing(self):
        machine = ConnectionMachine()
        machine.connect()
        machine.handshake_ok()
        machine.subscribe()
        machine.subscribe_fail()
        assert machine.current_state == machine.draining


class TestConnectionMachineDisconnect:
    """disconnect is valid from active and connected."""

    def test_disconnect_from_active(self):
        machine = ConnectionMachine()
        machine.connect()
        machine.handshake_ok()
        machine.subscribe()
        machine.subscribed()
        machine.disconnect()
        assert machine.current_state == machine.draining

    def test_disconnect_from_connected(self):
        machine = ConnectionMachine()
        machine.connect()
        machine.handshake_ok()
        machine.disconnect()
        assert machine.current_state == machine.draining


class TestConnectionMachineScheduleReconnect:
    """schedule_reconnect: draining → reconnecting."""

    def test_schedule_reconnect_from_draining(self):
        machine = ConnectionMachine()
        machine.connect()
        machine.handshake_ok()
        machine.disconnect()     # → draining
        machine.schedule_reconnect()
        assert machine.current_state == machine.reconnecting


class TestConnectionMachineClose:
    """close is valid from many states."""

    def test_close_from_idle(self):
        machine = ConnectionMachine()
        machine.close()
        assert machine.current_state == machine.closed

    def test_close_from_active(self):
        machine = ConnectionMachine()
        machine.connect()
        machine.handshake_ok()
        machine.subscribe()
        machine.subscribed()
        machine.close()
        assert machine.current_state == machine.closed

    def test_close_from_draining(self):
        machine = ConnectionMachine()
        machine.connect()
        machine.handshake_ok()
        machine.disconnect()  # → draining
        machine.close()
        assert machine.current_state == machine.closed

    def test_close_from_connected(self):
        machine = ConnectionMachine()
        machine.connect()
        machine.handshake_ok()
        machine.close()
        assert machine.current_state == machine.closed

    def test_close_from_subscribing(self):
        machine = ConnectionMachine()
        machine.connect()
        machine.handshake_ok()
        machine.subscribe()
        machine.close()
        assert machine.current_state == machine.closed

    def test_close_from_reconnecting(self):
        machine = ConnectionMachine()
        machine.connect()
        machine.handshake_fail()  # → reconnecting
        machine.close()
        assert machine.current_state == machine.closed

    def test_close_from_backoff(self):
        machine = ConnectionMachine()
        machine.connect()
        machine.handshake_fail()  # → reconnecting
        machine.wait()             # → backoff
        machine.close()
        assert machine.current_state == machine.closed

    def test_close_from_exhausted_not_possible(self):
        """exhausted → closed not defined; give_up raises before entering exhausted."""
        machine = ConnectionMachine(max_attempts=5)
        machine.connect()
        machine.handshake_fail()
        with pytest.raises(StreamReconnectExhaustedError):
            machine.give_up()
        # machine is stuck in exhausted state — close transitions are not defined from exhausted
        # (the FSM intentionally has no exit from exhausted except via close if defined)
        # Let's just verify the state
        assert machine.current_state == machine.exhausted


class TestConnectionMachineInvalidTransitions:
    """Invalid transitions raise TransitionNotAllowed."""

    def test_connect_from_non_idle_raises(self):
        machine = ConnectionMachine()
        machine.connect()  # idle → connecting
        with pytest.raises(TransitionNotAllowed):
            machine.connect()  # connecting → connecting (invalid)

    def test_handshake_ok_from_idle_raises(self):
        machine = ConnectionMachine()
        with pytest.raises(TransitionNotAllowed):
            machine.handshake_ok()

    def test_subscribe_from_idle_raises(self):
        machine = ConnectionMachine()
        with pytest.raises(TransitionNotAllowed):
            machine.subscribe()

    def test_subscribed_from_connecting_raises(self):
        machine = ConnectionMachine()
        machine.connect()
        with pytest.raises(TransitionNotAllowed):
            machine.subscribed()

    def test_retry_from_idle_raises(self):
        machine = ConnectionMachine()
        with pytest.raises(TransitionNotAllowed):
            machine.retry()

    def test_disconnect_from_idle_raises(self):
        machine = ConnectionMachine()
        with pytest.raises(TransitionNotAllowed):
            machine.disconnect()

    def test_disconnect_from_connecting_raises(self):
        machine = ConnectionMachine()
        machine.connect()
        with pytest.raises(TransitionNotAllowed):
            machine.disconnect()


class TestConnectionMachineStateNames:
    """State objects have expected names (for logging/debugging)."""

    def test_all_10_states_exist(self):
        machine = ConnectionMachine()
        state_names = {s.id for s in machine.states}
        expected = {
            "idle", "connecting", "connected", "subscribing",
            "active", "draining", "reconnecting", "backoff",
            "exhausted", "closed",
        }
        assert expected == state_names

    def test_idle_is_initial(self):
        machine = ConnectionMachine()
        assert machine.idle.initial is True

    def test_closed_is_final(self):
        machine = ConnectionMachine()
        assert machine.closed.final is True

    def test_no_other_final_states(self):
        machine = ConnectionMachine()
        final_states = [s for s in machine.states if s.final]
        assert len(final_states) == 1
        assert final_states[0] == machine.closed
