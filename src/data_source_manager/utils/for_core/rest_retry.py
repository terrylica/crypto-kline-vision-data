"""Unified retry module for REST API operations.

Provides a configurable retry decorator factory that:
- Accepts retry_count parameter (wires config to tenacity)
- Uses reraise=True (propagates original exceptions, not RetryError)
- Excludes RateLimitError from retry (rate limits are per-minute, short retries are harmful)
- Applies exponential backoff with jitter for non-rate-limit errors

Related: GitHub Issue #18 (Rate Limit Handling Overhaul), Phase 2
"""

import json
import random

import requests
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from data_source_manager.utils.for_core.rest_exceptions import RateLimitError, RestAPIError
from data_source_manager.utils.loguru_setup import logger

# Maximum wait time between retries (seconds)
MAX_RETRY_WAIT_SECONDS = 120


class _RetryIfNotRateLimit(retry_if_exception_type):
    """Retry on RestAPIError/requests exceptions, but NOT on RateLimitError.

    RateLimitError indicates a per-minute rate limit from the exchange.
    Retrying after a short delay is harmful — it triggers repeat 429s and
    can escalate to 418 IP bans. Let the caller handle rate limits explicitly.
    """

    def __init__(self):
        super().__init__((RestAPIError, requests.RequestException, json.JSONDecodeError))

    def __call__(self, retry_state):
        # Never retry RateLimitError
        if retry_state.outcome.failed:
            exc = retry_state.outcome.exception()
            if isinstance(exc, RateLimitError):
                return False
        return super().__call__(retry_state)


def create_retry_decorator(retry_count: int = 3):
    """Create a tenacity retry decorator with configurable retry count.

    Args:
        retry_count: Maximum number of attempts (default 3).

    Returns:
        A tenacity retry decorator.
    """
    return retry(
        stop=stop_after_attempt(retry_count),
        wait=wait_exponential(multiplier=1, min=1, max=MAX_RETRY_WAIT_SECONDS)
        + _jitter_wait(),
        retry=_RetryIfNotRateLimit(),
        reraise=True,
        before_sleep=lambda retry_state: logger.warning(
            f"Retrying after error (attempt {retry_state.attempt_number}/{retry_count}): "
            f"{retry_state.outcome.exception()}"
        ),
    )


class _jitter_wait:
    """Add random jitter (0-1s) to tenacity wait times.

    Implements the tenacity wait protocol (__call__ returns seconds to wait).
    Jitter prevents thundering herd when multiple clients retry simultaneously.
    """

    def __call__(self, retry_state):
        return random.uniform(0, 1)
