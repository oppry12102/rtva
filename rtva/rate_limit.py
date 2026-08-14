"""Async-safe token-bucket primitive.

Used to throttle `POST /v1/streams` so a misbehaving client can't churn through
session creation. Each client token gets its own bucket sized by
`session_bucket_capacity` (burst) and `session_bucket_refill_per_sec`
(steady-state, e.g. 0.1 ≈ 1 session per 10 s).

Single-process only — when the server runs N workers behind a single uvicorn,
this still works because the limiter lives in `app.state` and is shared across
requests on the same event loop. Multi-process fan-out would need a shared
store; not implemented here.
"""

from __future__ import annotations

import asyncio
import time


class TokenBucket:
    """Async-safe token bucket.

    `capacity` is the burst budget; tokens refill at `refill_per_sec` tokens
    per second of wall clock, capped at `capacity`. `acquire(cost)` consumes
    one token by default and returns True if a token was spent, False if the
    bucket was empty.
    """

    __slots__ = ("capacity", "refill_per_sec", "_tokens", "_last", "_lock")

    def __init__(self, capacity: int, refill_per_sec: float) -> None:
        self.capacity = max(1, int(capacity))
        self.refill_per_sec = float(refill_per_sec)
        self._tokens = float(self.capacity)         # start full
        self._last = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self, cost: float = 1.0) -> bool:
        async with self._lock:
            now = time.monotonic()
            self._tokens = min(
                self.capacity,
                self._tokens + (now - self._last) * self.refill_per_sec,
            )
            self._last = now
            if self._tokens >= cost:
                self._tokens -= cost
                return True
            return False

    def tokens_available(self) -> float:
        """Snapshot of current tokens (not under lock; for observability only)."""
        now = time.monotonic()
        return min(
            self.capacity,
            self._tokens + (now - self._last) * self.refill_per_sec,
        )