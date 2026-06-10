"""
Unit tests for RateLimiter.

No real HTTP calls. Only tests token-bucket semantics and that acquire()
doesn't crash. Timing tests use small rates to stay fast.
"""

from __future__ import annotations

import time
from unittest.mock import patch

import pytest

from ingestion.rate_limiter import RateLimiter


class TestRateLimiterInit:
    def test_invalid_rate_raises(self):
        with pytest.raises(ValueError, match="rate must be positive"):
            RateLimiter(rate=0)

    def test_invalid_burst_raises(self):
        with pytest.raises(ValueError, match="burst must be >= 1"):
            RateLimiter(burst=0)

    def test_default_rate_accessible(self):
        rl = RateLimiter(rate=2.5)
        assert rl.rate == 2.5


class TestTokenBucket:
    def test_first_acquire_does_not_block(self):
        """Bucket starts full — first acquire should be near-instant."""
        rl = RateLimiter(rate=1.0, burst=3, jitter=0.0)
        t0 = time.monotonic()
        rl.acquire()
        elapsed = time.monotonic() - t0
        assert elapsed < 0.1, f"First acquire took {elapsed:.3f}s — should be instant"

    def test_burst_allows_multiple_fast_acquires(self):
        """With burst=3 the first three acquires should all be instant."""
        rl = RateLimiter(rate=0.5, burst=3, jitter=0.0)
        t0 = time.monotonic()
        rl.acquire()
        rl.acquire()
        rl.acquire()
        elapsed = time.monotonic() - t0
        assert elapsed < 0.3, f"Burst of 3 took {elapsed:.3f}s"

    @patch("time.sleep")
    def test_acquire_sleeps_when_tokens_exhausted(self, mock_sleep):
        """After exhausting the burst, acquire() must call time.sleep."""
        rl = RateLimiter(rate=1.0, burst=1, jitter=0.0)
        rl.acquire()  # consumes the only token immediately
        rl.acquire()  # now tokens=0, should sleep
        assert mock_sleep.called, "Expected time.sleep to be called when tokens are exhausted"

    @patch("time.sleep")
    def test_jitter_calls_sleep(self, mock_sleep):
        """Non-zero jitter must produce at least one sleep call."""
        rl = RateLimiter(rate=100.0, burst=10, jitter=0.5)
        rl.acquire()
        assert mock_sleep.called

    @patch("time.sleep")
    def test_no_jitter_no_sleep_when_tokens_available(self, mock_sleep):
        """With jitter=0 and tokens available, acquire() must NOT sleep."""
        rl = RateLimiter(rate=100.0, burst=5, jitter=0.0)
        rl.acquire()
        mock_sleep.assert_not_called()
