"""
Token-bucket rate limiter for the Steam API ingester.

Steam sustains ~10 reviews/sec; each page returns up to 100 reviews,
so the safe page request rate is roughly 0.1 rps. We default to a more
practical 1.0 rps with burst=3 and add per-request jitter so concurrent
runs (or future parallel workers) don't all fire at the same instant.

Thread-safe: a single RateLimiter instance can be shared across threads.
"""

from __future__ import annotations

import random
import threading
import time


class RateLimiter:
    """Token-bucket rate limiter with configurable rate, burst capacity, and jitter.

    Args:
        rate:   Sustained request rate in requests-per-second. Default 1.0.
        burst:  Maximum tokens that can accumulate (handles short idle gaps).
        jitter: Max extra random delay expressed as a fraction of 1/rate.
                E.g. jitter=0.2 at rate=1.0 adds up to 0.2 s of random delay.
    """

    def __init__(
        self,
        rate: float = 1.0,
        burst: int = 3,
        jitter: float = 0.2,
    ) -> None:
        if rate <= 0:
            raise ValueError(f"rate must be positive, got {rate}")
        if burst < 1:
            raise ValueError(f"burst must be >= 1, got {burst}")

        self._rate = rate
        self._burst = float(burst)
        self._jitter = jitter
        self._tokens = float(burst)        # start full
        self._last_refill = time.monotonic()
        self._lock = threading.Lock()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def acquire(self) -> None:
        """Block until a token is available, then consume it.

        After acquiring, an optional random jitter sleep is applied to
        spread requests in time even when multiple callers fire together.
        """
        wait = self._consume_token()
        if wait > 0:
            time.sleep(wait)
        # Jitter is applied outside the lock so it doesn't hold up other threads.
        if self._jitter > 0:
            time.sleep(random.uniform(0.0, self._jitter / self._rate))

    @property
    def rate(self) -> float:
        return self._rate

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _consume_token(self) -> float:
        """Refill the bucket and consume one token. Returns seconds to sleep."""
        with self._lock:
            now = time.monotonic()
            elapsed = now - self._last_refill
            self._tokens = min(self._burst, self._tokens + elapsed * self._rate)
            self._last_refill = now

            if self._tokens >= 1.0:
                self._tokens -= 1.0
                return 0.0
            else:
                # How long until the next token arrives?
                wait = (1.0 - self._tokens) / self._rate
                self._tokens = 0.0
                return wait
