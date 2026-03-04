# ADR: docs/adr/2026-02-24-websocket-streaming-subsystem.md
"""Toxiproxy fixtures for streaming integration tests.

Manages toxiproxy-server lifecycle and proxy creation for Binance WebSocket
chaos testing. Tests are automatically skipped if toxiproxy-server binary
is not installed.

Prerequisites:
    brew install toxiproxy   # macOS
    # or download from https://github.com/Shopify/toxiproxy/releases
"""

from __future__ import annotations

import shutil
import subprocess
import time

import pytest

# Skip entire module if toxiproxy-python not installed
toxiproxy_mod = pytest.importorskip("toxiproxy", reason="toxiproxy-python not installed")


@pytest.fixture(scope="session")
def toxiproxy_server():
    """Start toxiproxy-server if not running, skip if binary not found.

    Session-scoped: one server process for all integration tests.
    Terminated on session teardown.
    """
    if not shutil.which("toxiproxy-server"):
        pytest.skip("toxiproxy-server binary not installed (brew install toxiproxy)")

    # Check if already running (e.g. as a service)
    try:
        from toxiproxy import Toxiproxy

        Toxiproxy().populate([])  # ping the API
        yield None  # already running, don't manage lifecycle
        return
    except (OSError, ConnectionError):
        pass  # not running yet — we'll start it below

    # Start toxiproxy-server
    proc = subprocess.Popen(
        ["toxiproxy-server"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    time.sleep(1)  # wait for server to start

    # Verify it started
    if proc.poll() is not None:
        pytest.skip("toxiproxy-server failed to start")

    yield proc
    proc.terminate()
    proc.wait(timeout=5)


@pytest.fixture
def toxiproxy_api(toxiproxy_server):
    """Toxiproxy API client, ready to create proxies."""
    from toxiproxy import Toxiproxy

    return Toxiproxy()


@pytest.fixture
def binance_ws_proxy(toxiproxy_api):
    """Create a Toxiproxy proxy for Binance futures WebSocket.

    Upstream: fstream.binance.com:443
    Listen: random local port (127.0.0.1:0)
    """
    proxy = toxiproxy_api.create(
        upstream="fstream.binance.com:443",
        name="binance_ws_test",
        listen="127.0.0.1:0",
    )
    yield proxy
    proxy.destroy()


@pytest.fixture
def binance_spot_ws_proxy(toxiproxy_api):
    """Create a Toxiproxy proxy for Binance spot WebSocket."""
    proxy = toxiproxy_api.create(
        upstream="stream.binance.com:443",
        name="binance_spot_ws_test",
        listen="127.0.0.1:0",
    )
    yield proxy
    proxy.destroy()
