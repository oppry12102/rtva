"""Worker pool + scheduler + backpressure.

Operating point (measured against the live M3 API):
    p50 latency   ~4.1s
    per-worker    ~0.14-0.23 req/s
    16-way conc   2.30 req/s aggregate, 15/16 success

Default config: 4 workers, 1.5s window @ 1.5s stride -> 0.67 fast calls/s,
leaves ~1.6 req/s for escalations.

Backpressure ladder degrades quality (NOT throughput of ingest) when the
queue backs up. Ingest MUST never block.
"""

from __future__ import annotations

import asyncio
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Awaitable, Callable, Optional

from .config import get_settings


@dataclass
class BackpressureLevel:
    level: int                    # 0..3
    label: str
    window_hz: float
    max_frames: int
    resolution: tuple[int, int]
    workers: int


LADDER: list[BackpressureLevel] = [
    BackpressureLevel(0, "normal",   0.67, 8,  (448, 252), 4),
    BackpressureLevel(1, "mild",     0.50, 5,  (448, 252), 4),
    BackpressureLevel(2, "heavy",    0.33, 3,  (336, 189), 3),
    BackpressureLevel(3, "circuit",  0.20, 2,  (256, 144), 2),
]


@dataclass
class WorkerPool:
    """Bounded pool of in-flight `analyze_window` coroutines."""

    max_workers: int = field(default_factory=lambda: get_settings().workers)

    sem: asyncio.Semaphore = field(init=False)
    in_flight: int = 0
    total_dispatched: int = 0
    total_completed: int = 0
    total_failed: int = 0
    total_parse_failures: int = 0
    max_in_flight_seen: int = 0

    def __post_init__(self) -> None:
        self.sem = asyncio.Semaphore(self.max_workers)

    async def submit(self, coro_factory: Callable[[], Awaitable]) -> any:
        await self.sem.acquire()
        self.in_flight += 1
        self.total_dispatched += 1
        self.max_in_flight_seen = max(self.max_in_flight_seen, self.in_flight)
        try:
            result = await coro_factory()
            self.total_completed += 1
            return result
        except Exception:
            self.total_failed += 1
            raise
        finally:
            self.in_flight -= 1
            self.sem.release()


@dataclass
class Backpressure:
    """Monitors queue depth + error rate and steps through LADDER."""

    current_level: int = 0
    last_level_change_t: float = 0.0
    depth_at_level_enter: int = 0
    error_window: deque = field(default_factory=lambda: deque(maxlen=60))  # last 60 events

    bp_l1: int = field(default_factory=lambda: get_settings().bp_l1)
    bp_l2: int = field(default_factory=lambda: get_settings().bp_l2)
    bp_l3: int = field(default_factory=lambda: get_settings().bp_l3)

    def update(self, queue_depth: int) -> Optional[int]:
        """Return new level if changed."""
        now = time.monotonic()
        target = self.current_level
        # immediate downgrade if depth high
        if queue_depth >= self.bp_l3:
            target = 3
        elif queue_depth >= self.bp_l2:
            target = max(target, 2)
        elif queue_depth >= self.bp_l1:
            target = max(target, 1)
        # errors -> downgrade
        recent_errors = sum(1 for t in self.error_window if t > now - 60)
        if recent_errors > 5 and self.current_level < 2:
            target = max(target, 2)

        if target > self.current_level:
            self.current_level = target
            self.last_level_change_t = now
            self.depth_at_level_enter = queue_depth
            return self.current_level

        if target < self.current_level:
            # upgrade only after depth has stayed below the level's threshold
            thresholds = [0, self.bp_l1, self.bp_l2, self.bp_l3]
            thresh = thresholds[self.current_level]
            if queue_depth < thresh and (now - self.last_level_change_t) > 5.0:
                self.current_level = target
                self.last_level_change_t = now
                return self.current_level
        return None

    def record_error(self) -> None:
        self.error_window.append(time.monotonic())


def current_level() -> BackpressureLevel:
    return LADDER[0]  # default; replaced at runtime by Backpressure.current_level


@dataclass
class WindowScheduler:
    """Cadence of window dispatch + window_id + t_start/t_end bookkeeping."""

    window_seconds: float = field(default_factory=lambda: get_settings().window_seconds)
    target_fps: int = field(default_factory=lambda: get_settings().target_fps)

    _next_dispatch_t: float = 0.0
    window_id: int = 0

    def next_window(self, now: float) -> Optional[tuple[int, float, float]]:
        """Return (window_id, t_start, t_end) when it's time to dispatch, else None."""
        if now < self._next_dispatch_t:
            return None
        self.window_id += 1
        t_end = now
        t_start = t_end - self.window_seconds
        self._next_dispatch_t = now + self.window_seconds  # default: stride == window
        return self.window_id, t_start, t_end
