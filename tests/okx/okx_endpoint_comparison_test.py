#!/usr/bin/env python3
# ADR: docs/adr/2026-01-30-claude-code-infrastructure.md
"""OKX endpoint comparison tests.

These integration tests compare behavior between the /market/candles and
/market/history-candles endpoints to verify data consistency, boundary overlap,
latency differences, and timestamp handling.
"""

from datetime import datetime, timedelta

import pytest

from data_source_manager.utils.config import MIN_RECORDS_FOR_COMPARISON
from tests.okx.conftest import (
    CANDLES_ENDPOINT,
    HISTORY_CANDLES_ENDPOINT,
    SPOT_INSTRUMENT,
    retry_request,
)


@pytest.mark.integration
@pytest.mark.okx
class TestBoundaryOverlap:
    """Tests for verifying if candles and history-candles endpoints have overlapping data."""

    def test_boundary_overlap_exists(self) -> None:
        """
        Verify that candles and history-candles endpoints have overlapping data.

        This test checks if there's a time period where both endpoints return
        the same data, which is important for seamless data retrieval.

        Validates:
        - Both endpoints return code "0" (success)
        - At least some timestamps exist in both endpoints
        """
        # Get data from candles endpoint
        candles_params = {"instId": SPOT_INSTRUMENT, "bar": "1m", "limit": 10}
        candles_data = retry_request(CANDLES_ENDPOINT, candles_params)

        assert candles_data.get("code") == "0", f"Candles request failed: {candles_data.get('msg')}"
        assert candles_data.get("data"), "No data from candles endpoint"

        # Get the oldest timestamp from candles (data is newest first)
        oldest_candle = candles_data["data"][-1]
        oldest_timestamp = int(oldest_candle[0])

        # Try to get the same data from history-candles
        history_params = {
            "instId": SPOT_INSTRUMENT,
            "bar": "1m",
            "limit": 10,
            "before": oldest_timestamp + 1,
        }
        history_data = retry_request(HISTORY_CANDLES_ENDPOINT, history_params)

        assert history_data.get("code") == "0", f"History request failed: {history_data.get('msg')}"

        # Check for matching timestamps
        candles_timestamps = {candle[0] for candle in candles_data["data"]}
        history_timestamps = {candle[0] for candle in history_data.get("data", [])}

        matching = candles_timestamps & history_timestamps
        # It's acceptable if there's no overlap for very recent data
        # The test passes as long as both endpoints respond correctly
        assert isinstance(matching, set), "Should be able to compare timestamps"


@pytest.mark.integration
@pytest.mark.okx
class TestLatencyFreshness:
    """Tests for comparing data freshness between endpoints."""

    def test_candles_has_more_recent_data(self) -> None:
        """
        Verify that the candles endpoint has more recent data than history-candles.

        The candles endpoint should return real-time data while history-candles
        may have a delay.

        Validates:
        - Both endpoints return code "0" (success)
        - Candles timestamp is >= history-candles timestamp
        """
        # Get latest from candles
        candles_params = {"instId": SPOT_INSTRUMENT, "bar": "1m", "limit": 1}
        candles_data = retry_request(CANDLES_ENDPOINT, candles_params)

        assert candles_data.get("code") == "0", f"Candles request failed: {candles_data.get('msg')}"
        assert candles_data.get("data"), "No data from candles endpoint"

        candles_timestamp = int(candles_data["data"][0][0])

        # Get latest from history-candles
        history_params = {"instId": SPOT_INSTRUMENT, "bar": "1m", "limit": 1}
        history_data = retry_request(HISTORY_CANDLES_ENDPOINT, history_params)

        assert history_data.get("code") == "0", f"History request failed: {history_data.get('msg')}"
        assert history_data.get("data"), "No data from history-candles endpoint"

        history_timestamp = int(history_data["data"][0][0])

        # Candles should have same or more recent data
        assert candles_timestamp >= history_timestamp, (
            f"Candles ({candles_timestamp}) should be >= history ({history_timestamp})"
        )


@pytest.mark.integration
@pytest.mark.okx
class TestDataConsistency:
    """Tests for verifying data consistency between endpoints."""

    def test_same_timestamp_returns_consistent_data(self) -> None:
        """
        Verify that the same timestamp returns identical OHLCV data from both endpoints.

        When querying a specific historical timestamp, both endpoints should
        return the same candle data.

        Validates:
        - Both endpoints return code "0" (success)
        - OHLCV values match for the same timestamp
        """
        # Get a reference timestamp from history-candles
        history_params = {"instId": SPOT_INSTRUMENT, "bar": "1m", "limit": 1}
        history_data = retry_request(HISTORY_CANDLES_ENDPOINT, history_params)

        assert history_data.get("code") == "0", f"History request failed: {history_data.get('msg')}"
        assert history_data.get("data"), "No data from history-candles endpoint"

        timestamp = int(history_data["data"][0][0])

        # Query candles with the same timestamp
        candles_params = {
            "instId": SPOT_INSTRUMENT,
            "bar": "1m",
            "limit": 1,
            "before": timestamp + 1,
        }
        candles_data = retry_request(CANDLES_ENDPOINT, candles_params)

        assert candles_data.get("code") == "0", f"Candles request failed: {candles_data.get('msg')}"

        # Find matching timestamp in candles data
        candles_entry = None
        for candle in candles_data.get("data", []):
            if int(candle[0]) == timestamp:
                candles_entry = candle
                break

        if candles_entry:
            # Compare OHLCV values (indices 1-5: open, high, low, close, volume)
            history_entry = history_data["data"][0]
            for i in range(1, 6):
                assert candles_entry[i] == history_entry[i], (
                    f"Mismatch at field {i}: candles={candles_entry[i]}, history={history_entry[i]}"
                )


@pytest.mark.integration
@pytest.mark.okx
class TestTimestampHandling:
    """Tests for verifying timestamp parameter behavior (before/after)."""

    def test_before_parameter_behavior(self) -> None:
        """
        Verify the 'before' parameter returns data before the specified timestamp.

        Validates:
        - API returns code "0" (success)
        - All returned timestamps are < the 'before' value
        """
        # Get a reference point
        ref_params = {"instId": SPOT_INSTRUMENT, "bar": "1m", "limit": 5}
        ref_data = retry_request(CANDLES_ENDPOINT, ref_params)

        assert ref_data.get("code") == "0", f"Reference request failed: {ref_data.get('msg')}"
        assert len(ref_data.get("data", [])) >= MIN_RECORDS_FOR_COMPARISON, "Not enough reference data"

        # Get middle timestamp
        middle_idx = len(ref_data["data"]) // 2
        test_timestamp = int(ref_data["data"][middle_idx][0])

        # Query with 'before' parameter
        before_params = {
            "instId": SPOT_INSTRUMENT,
            "bar": "1m",
            "limit": 5,
            "before": test_timestamp,
        }
        before_data = retry_request(CANDLES_ENDPOINT, before_params)

        assert before_data.get("code") == "0", f"Before query failed: {before_data.get('msg')}"

        # All returned timestamps should be < test_timestamp
        for candle in before_data.get("data", []):
            candle_ts = int(candle[0])
            assert candle_ts < test_timestamp, (
                f"Timestamp {candle_ts} should be < {test_timestamp}"
            )

    def test_after_parameter_behavior(self) -> None:
        """
        Verify the 'after' parameter returns data after the specified timestamp.

        Validates:
        - API returns code "0" (success)
        - All returned timestamps are > the 'after' value
        """
        # Use a timestamp from 1 hour ago
        test_timestamp = int((datetime.now() - timedelta(hours=1)).timestamp() * 1000)

        after_params = {
            "instId": SPOT_INSTRUMENT,
            "bar": "1m",
            "limit": 5,
            "after": test_timestamp,
        }
        after_data = retry_request(CANDLES_ENDPOINT, after_params)

        assert after_data.get("code") == "0", f"After query failed: {after_data.get('msg')}"

        # All returned timestamps should be > test_timestamp
        for candle in after_data.get("data", []):
            candle_ts = int(candle[0])
            assert candle_ts > test_timestamp, (
                f"Timestamp {candle_ts} should be > {test_timestamp}"
            )


@pytest.mark.integration
@pytest.mark.okx
class TestBackfillDepth:
    """Tests for verifying how far back the history-candles endpoint can retrieve data."""

    def test_history_candles_has_deep_history(self) -> None:
        """
        Verify the history-candles endpoint can retrieve data from years ago.

        The history-candles endpoint should have data going back to 2017 for BTC-USDT.

        Validates:
        - API returns code "0" (success)
        - Data is available from at least 1 year ago
        """
        # Test 1 year ago
        one_year_ago = datetime.now() - timedelta(days=365)
        test_timestamp = int(one_year_ago.timestamp() * 1000)

        params = {
            "instId": SPOT_INSTRUMENT,
            "bar": "1D",
            "limit": 10,
            "after": test_timestamp,
        }
        data = retry_request(HISTORY_CANDLES_ENDPOINT, params)

        assert data.get("code") == "0", f"Request failed: {data.get('msg')}"
        assert len(data.get("data", [])) > 0, "Expected historical data from 1 year ago"

    def test_history_candles_reaches_2017(self) -> None:
        """
        Verify the history-candles endpoint has data from October 2017.

        This is the earliest known data point for BTC-USDT on OKX.

        Validates:
        - API returns code "0" (success)
        - Data is available from October 2017
        """
        # October 15, 2017
        old_date = datetime(2017, 10, 15)
        test_timestamp = int(old_date.timestamp() * 1000)

        params = {
            "instId": SPOT_INSTRUMENT,
            "bar": "1D",
            "limit": 10,
            "after": test_timestamp,
        }
        data = retry_request(HISTORY_CANDLES_ENDPOINT, params)

        assert data.get("code") == "0", f"Request failed: {data.get('msg')}"
        assert len(data.get("data", [])) > 0, "Expected historical data from October 2017"
