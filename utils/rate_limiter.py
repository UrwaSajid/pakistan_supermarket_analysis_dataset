"""
Token-bucket rate limiter with per-domain tracking.

Usage
-----
limiter = RateLimiter(calls=5, period=1.0)
with limiter:
    response = requests.get(url)
"""

import threading
import time


class RateLimiter:
    """
    Thread-safe token-bucket rate limiter.

    Parameters
    ----------
    calls  : Maximum number of calls allowed per ``period`` seconds.
    period : Rolling window in seconds.
    """

    def __init__(self, calls: int = 5, period: float = 1.0) -> None:
        self.calls  = calls
        self.period = period
        self._lock  = threading.Lock()
        self._timestamps: list[float] = []

    # ── Context-manager interface ─────────────────────────────────────────────
    def __enter__(self) -> "RateLimiter":
        self.acquire()
        return self

    def __exit__(self, *_) -> None:
        pass

    # ── Core logic ────────────────────────────────────────────────────────────
    def acquire(self) -> None:
        """Block until a call token is available."""
        while True:
            with self._lock:
                now = time.monotonic()
                # Drop timestamps outside the current window
                self._timestamps = [t for t in self._timestamps if now - t < self.period]
                if len(self._timestamps) < self.calls:
                    self._timestamps.append(now)
                    return
                # Calculate how long to sleep until the oldest token expires
                sleep_for = self.period - (now - self._timestamps[0]) + 0.001
            time.sleep(max(sleep_for, 0))

    def __call__(self, fn):
        """Decorator usage: @rate_limiter"""
        import functools

        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            self.acquire()
            return fn(*args, **kwargs)

        return wrapper
