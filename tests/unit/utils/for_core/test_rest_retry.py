"""Tests for the unified retry module (rest_retry.py).

Verifies:
- create_retry_decorator() factory produces working decorators
- RateLimitError is excluded from retry (immediate propagation)
- RestAPIError/requests exceptions are retried
- retry_count parameter is wired correctly
- reraise=True propagates original exceptions (not RetryError)

Related: GitHub Issue #18 (Rate Limit Handling Overhaul), Phase 2
"""

from unittest.mock import MagicMock, patch

import pytest
import requests

from data_source_manager.utils.for_core.rest_exceptions import (
    HTTPError,
    NetworkError,
    RateLimitError,
    RestAPIError,
)
from data_source_manager.utils.for_core.rest_retry import (
    MAX_RETRY_WAIT_SECONDS,
    _RetryIfNotRateLimit,
    create_retry_decorator,
)


class TestCreateRetryDecorator:
    """Tests for the create_retry_decorator factory."""

    @patch("data_source_manager.utils.for_core.rest_retry.logger")
    def test_retries_on_rest_api_error(self, _mock_logger):
        """RestAPIError triggers retry up to retry_count."""
        call_count = 0

        @create_retry_decorator(retry_count=3)
        def flaky_func():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise HTTPError(500, "Internal Server Error")
            return "success"

        result = flaky_func()
        assert result == "success"
        assert call_count == 3

    @patch("data_source_manager.utils.for_core.rest_retry.logger")
    def test_retries_on_network_error(self, _mock_logger):
        """NetworkError (RestAPIError subclass) triggers retry."""
        call_count = 0

        @create_retry_decorator(retry_count=2)
        def flaky_func():
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise NetworkError("Connection reset")
            return "recovered"

        result = flaky_func()
        assert result == "recovered"
        assert call_count == 2

    def test_rate_limit_error_not_retried(self):
        """RateLimitError propagates immediately without retry."""
        call_count = 0

        @create_retry_decorator(retry_count=3)
        def rate_limited_func():
            nonlocal call_count
            call_count += 1
            raise RateLimitError(retry_after=60)

        with pytest.raises(RateLimitError) as exc_info:
            rate_limited_func()

        # Called exactly once — no retries
        assert call_count == 1
        assert exc_info.value.retry_after == 60

    @patch("data_source_manager.utils.for_core.rest_retry.logger")
    def test_retry_count_wired(self, _mock_logger):
        """retry_count parameter controls number of attempts."""
        call_count = 0

        @create_retry_decorator(retry_count=5)
        def always_fails():
            nonlocal call_count
            call_count += 1
            raise RestAPIError("persistent error")

        with pytest.raises(RestAPIError):
            always_fails()

        assert call_count == 5

    @patch("data_source_manager.utils.for_core.rest_retry.logger")
    def test_reraise_propagates_original_exception(self, _mock_logger):
        """reraise=True ensures original exception type propagates (not RetryError)."""

        @create_retry_decorator(retry_count=2)
        def fails_with_http_error():
            raise HTTPError(503, "Service Unavailable")

        # Should catch HTTPError directly, not tenacity.RetryError
        with pytest.raises(HTTPError) as exc_info:
            fails_with_http_error()

        assert exc_info.value.status_code == 503

    @patch("data_source_manager.utils.for_core.rest_retry.logger")
    def test_retries_on_requests_exception(self, _mock_logger):
        """requests.RequestException triggers retry."""
        call_count = 0

        @create_retry_decorator(retry_count=2)
        def connection_fails():
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise requests.ConnectionError("Connection refused")
            return "ok"

        result = connection_fails()
        assert result == "ok"
        assert call_count == 2

    def test_default_retry_count_is_3(self):
        """Default retry_count is 3 when not specified."""
        call_count = 0

        @create_retry_decorator()
        def always_fails():
            nonlocal call_count
            call_count += 1
            raise RestAPIError("error")

        with pytest.raises(RestAPIError):
            always_fails()

        assert call_count == 3


class TestRetryIfNotRateLimit:
    """Tests for the _RetryIfNotRateLimit predicate."""

    def test_returns_false_for_rate_limit_error(self):
        """RateLimitError should not be retried."""
        predicate = _RetryIfNotRateLimit()
        retry_state = MagicMock()
        retry_state.outcome.failed = True
        retry_state.outcome.exception.return_value = RateLimitError(retry_after=60)

        assert predicate(retry_state) is False

    def test_returns_true_for_http_error(self):
        """HTTPError should be retried."""
        predicate = _RetryIfNotRateLimit()
        retry_state = MagicMock()
        retry_state.outcome.failed = True
        retry_state.outcome.exception.return_value = HTTPError(500, "error")

        assert predicate(retry_state) is True

    def test_returns_true_for_rest_api_error(self):
        """RestAPIError (base class) should be retried."""
        predicate = _RetryIfNotRateLimit()
        retry_state = MagicMock()
        retry_state.outcome.failed = True
        retry_state.outcome.exception.return_value = RestAPIError("error")

        assert predicate(retry_state) is True


class TestMaxRetryWaitSeconds:
    """Tests for the MAX_RETRY_WAIT_SECONDS constant."""

    def test_max_retry_wait_is_120(self):
        """MAX_RETRY_WAIT_SECONDS should be 120."""
        assert MAX_RETRY_WAIT_SECONDS == 120


class TestMetricsRateLimitTracking:
    """Tests for P4.1: Metrics tracking of RateLimitError.

    Verifies that track_api_call correctly detects RateLimitError
    (which has retry_after, not status_code) and increments rate_limited_calls.
    """

    def test_rate_limit_error_tracked_in_metrics(self):
        """RateLimitError should increment rate_limited_calls counter."""
        from data_source_manager.utils.for_core.rest_metrics import RestMetricsTracker

        tracker = RestMetricsTracker()
        tracker.reset()

        # Simulate tracking a call that raised RateLimitError
        tracker.record_api_call(
            endpoint="https://api.binance.com/api/v3/klines",
            params={"symbol": "BTCUSDT"},
            start_time=0.0,
            end_time=0.1,
            success=False,
            error_type="RateLimitError",
            status_code=429,
        )

        metrics = tracker.get_metrics()
        assert metrics["rate_limited_calls"] == 1

    def test_track_api_call_decorator_detects_rate_limit_error(self):
        """track_api_call decorator should pass status_code=429 for RateLimitError."""
        from data_source_manager.utils.for_core.rest_metrics import (
            RestMetricsTracker,
            track_api_call,
        )

        tracker = RestMetricsTracker()
        tracker.reset()

        @track_api_call(endpoint="test", params={})
        def raise_rate_limit():
            raise RateLimitError(retry_after=60)

        with pytest.raises(RateLimitError):
            raise_rate_limit()

        metrics = tracker.get_metrics()
        assert metrics["rate_limited_calls"] == 1
        assert metrics["failed_calls"] == 1


class TestApiRateLimitFix:
    """Tests for P2.3: api.py rate limit handling (no double-wait).

    Verifies that make_api_request raises RateLimitError on 429
    instead of sleeping + raising TimeoutError.
    """

    def test_429_raises_rate_limit_error(self):
        """HTTP 429 should raise RateLimitError, not TimeoutError."""
        from data_source_manager.utils.network.api import make_api_request

        mock_client = MagicMock()
        mock_response = MagicMock()
        mock_response.status_code = 429
        mock_response.headers = {"retry-after": "30"}
        mock_client.get.return_value = mock_response

        with pytest.raises(RateLimitError) as exc_info:
            make_api_request(mock_client, "https://api.example.com/test")

        assert exc_info.value.retry_after == 30

    def test_418_raises_rate_limit_error(self):
        """HTTP 418 (IP ban) should raise RateLimitError."""
        from data_source_manager.utils.network.api import make_api_request

        mock_client = MagicMock()
        mock_response = MagicMock()
        mock_response.status_code = 418
        mock_response.headers = {}
        mock_client.get.return_value = mock_response

        with pytest.raises(RateLimitError) as exc_info:
            make_api_request(mock_client, "https://api.example.com/test")

        # Default retry_after when header missing
        assert exc_info.value.retry_after == 60

    def test_429_default_retry_after_is_60(self):
        """Missing retry-after header should default to 60s (not 1s)."""
        from data_source_manager.utils.network.api import make_api_request

        mock_client = MagicMock()
        mock_response = MagicMock()
        mock_response.status_code = 429
        mock_response.headers = {}  # No retry-after header
        mock_client.get.return_value = mock_response

        with pytest.raises(RateLimitError) as exc_info:
            make_api_request(mock_client, "https://api.example.com/test")

        assert exc_info.value.retry_after == 60
