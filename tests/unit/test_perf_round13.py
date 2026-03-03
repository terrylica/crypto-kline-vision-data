"""Performance benchmarks for Round 13: REST Client Migration.

# ADR: docs/adr/2025-01-30-failover-control-protocol.md

Validates:
1. orjson.loads() is faster than json.loads() for API response parsing
2. create_optimized_client() returns httpx.Client (not requests.Session)
3. fetch_chunk uses httpx.Client type (not requests.Session)
4. Retry decorator catches httpx.HTTPError (not requests.RequestException)
5. Exception handling maps httpx exceptions to CKVD exception hierarchy
"""

import inspect
import json
import timeit

import httpx
import orjson

from ckvd.utils.for_core.rest_client_utils import create_optimized_client, fetch_chunk
from ckvd.utils.for_core.rest_retry import _RetryIfNotRateLimit, create_retry_decorator


def _create_sample_json_payload(n_klines: int = 1000) -> bytes:
    """Create a realistic REST API response body (klines array as JSON bytes)."""
    klines = []
    for i in range(n_klines):
        klines.append([
            1704067200000 + i * 60000,  # open_time
            f"{100.0 + i % 50:.8f}",  # open
            f"{101.0 + i % 50:.8f}",  # high
            f"{99.0 + i % 50:.8f}",  # low
            f"{100.5 + i % 50:.8f}",  # close
            f"{1000.0:.8f}",  # volume
            1704067259999 + i * 60000,  # close_time
            f"{50000.0:.8f}",  # quote_volume
            100,  # count
            f"{500.0:.8f}",  # taker_buy_volume
            f"{25000.0:.8f}",  # taker_buy_quote_volume
            "0",
        ])
    return json.dumps(klines).encode()


class TestOrjsonParsing:
    """Benchmark: orjson.loads() vs json.loads() for REST API responses."""

    def test_orjson_faster_than_json(self):
        """orjson should be faster than stdlib json for API response parsing."""
        payload = _create_sample_json_payload(1000)
        iterations = 1000

        json_time = timeit.timeit(lambda: json.loads(payload), number=iterations)
        orjson_time = timeit.timeit(lambda: orjson.loads(payload), number=iterations)

        speedup = json_time / orjson_time
        # orjson is faster and also accepts bytes directly (no decode step).
        # Speedup varies by data shape; assert at least 1.2x.
        assert speedup >= 1.2, (
            f"Expected >=1.2x speedup, got {speedup:.2f}x "
            f"(json={json_time:.4f}s, orjson={orjson_time:.4f}s)"
        )

    def test_orjson_correctness(self):
        """orjson.loads() should produce identical results to json.loads()."""
        payload = _create_sample_json_payload(100)

        json_result = json.loads(payload)
        orjson_result = orjson.loads(payload)

        assert json_result == orjson_result, "orjson and json should produce identical results"

    def test_fetch_chunk_uses_orjson(self):
        """fetch_chunk should use orjson.loads, not json.loads or response.json()."""
        source = inspect.getsource(fetch_chunk)

        assert "orjson.loads" in source, "fetch_chunk should use orjson.loads()"
        assert "response.json()" not in source, "fetch_chunk should not use response.json()"


class TestHttpxClientType:
    """Verify REST client uses httpx.Client (not requests.Session)."""

    def test_create_optimized_client_returns_httpx(self):
        """create_optimized_client() should return httpx.Client."""
        client = create_optimized_client()
        try:
            assert isinstance(client, httpx.Client), (
                f"Expected httpx.Client, got {type(client).__name__}"
            )
        finally:
            client.close()

    def test_client_has_correct_headers(self):
        """httpx.Client should have standard REST API headers."""
        client = create_optimized_client()
        try:
            headers = dict(client.headers)
            assert "user-agent" in headers, "Should have User-Agent header"
            assert "accept" in headers, "Should have Accept header"
        finally:
            client.close()

    def test_client_follows_redirects(self):
        """httpx.Client should follow redirects."""
        client = create_optimized_client()
        try:
            assert client.follow_redirects is True, "Should follow redirects"
        finally:
            client.close()

    def test_fetch_chunk_type_hint_is_httpx(self):
        """fetch_chunk should accept httpx.Client, not requests.Session."""
        source = inspect.getsource(fetch_chunk)

        assert "httpx.Client" in source, "fetch_chunk should use httpx.Client type"
        assert "requests.Session" not in source, "fetch_chunk should not reference requests.Session"


class TestRetryHttpxCompatibility:
    """Verify retry decorator catches httpx exceptions."""

    def test_retry_filter_catches_httpx_http_error(self):
        """_RetryIfNotRateLimit should catch httpx.HTTPError."""
        source = inspect.getsource(_RetryIfNotRateLimit)

        assert "httpx.HTTPError" in source, "Retry filter should catch httpx.HTTPError"
        assert "requests.RequestException" not in source, (
            "Retry filter should not reference requests.RequestException"
        )

    def test_retry_actually_retries_httpx_connect_error(self):
        """httpx.ConnectError should trigger retry."""
        call_count = 0

        @create_retry_decorator(retry_count=2)
        def flaky():
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise httpx.ConnectError("refused")
            return "ok"

        result = flaky()
        assert result == "ok"
        assert call_count == 2

    def test_retry_actually_retries_httpx_timeout(self):
        """httpx.TimeoutException should trigger retry."""
        call_count = 0

        @create_retry_decorator(retry_count=2)
        def flaky():
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise httpx.ReadTimeout("timed out")
            return "ok"

        result = flaky()
        assert result == "ok"
        assert call_count == 2


class TestExceptionMapping:
    """Verify httpx exceptions map to CKVD exception hierarchy."""

    def test_connect_error_maps_to_network_error(self):
        """httpx.ConnectError handling should produce NetworkError."""
        source = inspect.getsource(fetch_chunk)

        # Verify the exception mapping pattern exists
        assert "httpx.ConnectError" in source, "Should catch httpx.ConnectError"
        assert "NetworkError" in source, "Should raise NetworkError"

    def test_timeout_maps_to_rest_timeout_error(self):
        """httpx.TimeoutException handling should produce RestTimeoutError."""
        source = inspect.getsource(fetch_chunk)

        assert "httpx.TimeoutException" in source, "Should catch httpx.TimeoutException"
        assert "RestTimeoutError" in source, "Should raise RestTimeoutError"

    def test_broad_httpx_error_maps_to_rest_api_error(self):
        """httpx.HTTPError catch-all should produce RestAPIError."""
        source = inspect.getsource(fetch_chunk)

        assert "httpx.HTTPError" in source, "Should catch httpx.HTTPError"
        assert "RestAPIError" in source, "Should raise RestAPIError"

    def test_no_requests_references_in_fetch_chunk(self):
        """fetch_chunk should not reference the requests library."""
        source = inspect.getsource(fetch_chunk)

        assert "requests." not in source, (
            "fetch_chunk should not reference requests library"
        )

    def test_no_requests_references_in_retry(self):
        """rest_retry module should not reference the requests library."""
        from ckvd.utils.for_core import rest_retry

        source = inspect.getsource(rest_retry)

        assert "import requests" not in source, (
            "rest_retry should not import requests"
        )
        assert "requests.RequestException" not in source, (
            "rest_retry should not reference requests.RequestException"
        )
