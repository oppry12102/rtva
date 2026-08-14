"""Single-session pipeline: source -> gate -> windows -> M3 -> events -> fanout.

This is the orchestrator. It owns:
    - one PyAV source (file or URL)
    - one MotionGate
    - one WindowScheduler
    - one WorkerPool
    - one StreamingMemory
    - one Backpressure controller
    - a callback `emit` (set by the server) that pushes WS messages to clients
"""

from __future__ import annotations

import asyncio
import time
import uuid
from collections import deque
from dataclasses import dataclass, field
from typing import Awaitable, Callable, Optional

import numpy as np

from .config import get_settings
from .events import Event, merge_events
from .frame_source import FrameSource, PyAVSource
from .gate import MotionGate
from .llm import M3Client, encode_jpeg
from .memory import StreamingMemory
from .prompts import (
    SYSTEM_PROMPT,
    build_user_message,
    build_escalation_user_message,
)
from .scheduler import (
    LADDER, Backpressure, BackpressureLevel, WindowScheduler, WorkerPool,
)

EmitFn = Callable[[dict], Awaitable[None]]


# --- Stats -------------------------------------------------------------------


@dataclass
class Stats:
    session_id: str
    started_at: float
    frames_received: int = 0
    windows_dispatched: int = 0
    windows_completed: int = 0
    windows_failed: int = 0
    escalations_dispatched: int = 0
    events_emitted: int = 0
    queue_depth: int = 0
    backpressure_level: int = 0
    latency_p50_ms: float = 0.0
    latency_p95_ms: float = 0.0
    latency_max_ms: float = 0.0
    cache_hits: int = 0
    cache_misses: int = 0
    total_prompt_tokens: int = 0
    total_completion_tokens: int = 0
    total_parse_failures: int = 0
    last_window_t: float = 0.0

    _latencies_ms: deque = field(default_factory=lambda: deque(maxlen=100))

    def record_latency(self, dt_s: float) -> None:
        ms = dt_s * 1000.0
        self._latencies_ms.append(ms)
        if self._latencies_ms:
            s = sorted(self._latencies_ms)
            self.latency_p50_ms = s[len(s)//2]
            self.latency_p95_ms = s[min(len(s)-1, int(len(s)*0.95))]
            self.latency_max_ms = s[-1]

    def to_dict(self) -> dict:
        d = self.__dict__.copy()
        d.pop("_latencies_ms", None)
        d.pop("started_at", None)
        d["uptime_s"] = time.time() - self.started_at
        return d


# --- Frame buffer ------------------------------------------------------------


@dataclass
class FrameRing:
    """Bounded ring of (pts, rgb) tuples used both by the gate and by window assembly."""
    maxlen: int = 64
    buf: deque = field(default_factory=lambda: deque(maxlen=64))

    def push(self, pts: float, rgb: np.ndarray) -> None:
        self.buf.append((pts, rgb))

    def recent(self, t_from: float, t_to: float) -> list[tuple[float, np.ndarray]]:
        # buf holds newest at the right; iterate in order
        return [(p, r) for (p, r) in self.buf if t_from <= p <= t_to]

    def newest(self) -> Optional[tuple[float, np.ndarray]]:
        return self.buf[-1] if self.buf else None


# --- Pipeline ----------------------------------------------------------------


@dataclass
class Pipeline:
    source_url: str = ""
    emit: EmitFn = None  # type: ignore[assignment]
    options: dict = field(default_factory=dict)
    frame_source: Optional[FrameSource] = None  # if set, takes precedence over source_url

    session_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    started_at: float = field(default_factory=time.time)

    gate: MotionGate = field(default_factory=MotionGate)
    sched: WindowScheduler = field(default_factory=WindowScheduler)
    pool: WorkerPool = field(default_factory=WorkerPool)
    bp: Backpressure = field(default_factory=Backpressure)
    mem: StreamingMemory = field(default_factory=StreamingMemory)
    ring: FrameRing = field(default_factory=FrameRing)
    stats: Stats = field(init=False)
    client: M3Client = field(init=False)
    _stop: asyncio.Event = field(default_factory=asyncio.Event)
    _tasks: list[asyncio.Task] = field(default_factory=list)
    _last_escalation_at: float = 0.0
    _escalation_in_flight: bool = False

    def __post_init__(self) -> None:
        if self.frame_source is None:
            if not self.source_url:
                raise ValueError("Pipeline requires either source_url or frame_source")
            self.frame_source = PyAVSource(self.source_url,
                                           target_fps=self.sched.target_fps)
        self.stats = Stats(session_id=self.session_id, started_at=self.started_at)
        self.client = M3Client()

    async def run(self) -> None:
        await self._emit({"type": "session.started",
                          "session_id": self.session_id,
                          "source": self.source_url,
                          "options": self.options})
        await self.client.__aenter__()
        try:
            consumer = asyncio.create_task(self._consume())
            cadencer = asyncio.create_task(self._cadencer())
            stats_task = asyncio.create_task(self._stats_loop())
            self._tasks = [consumer, cadencer, stats_task]
            await consumer
            self._stop.set()
            for t in (cadencer, stats_task):
                t.cancel()
        finally:
            await self.client.__aexit__()
            await self._emit({"type": "session.ended", "session_id": self.session_id})

    async def stop(self) -> None:
        self._stop.set()
        try:
            await self.frame_source.aclose()
        except Exception:
            pass
        for t in self._tasks:
            t.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)

    # --- loops --------------------------------------------------------------

    async def _consume(self) -> None:
        try:
            async for vf in self.frame_source.frames():
                if self._stop.is_set():
                    break
                self.stats.frames_received += 1
                self.ring.push(vf.pts, vf.rgb)
                # gate evaluation
                sig = self.gate.update(vf.rgb, t=vf.pts)
                if sig.fired:
                    await self._on_gate_fire(sig)
        except asyncio.CancelledError:
            return
        except Exception as exc:
            await self._emit({"type": "error", "code": "source", "message": repr(exc)})

    async def _cadencer(self) -> None:
        try:
            while not self._stop.is_set():
                await asyncio.sleep(0.05)
                # stream-time now (latest PTS in ring); fall back to wall
                latest = self.ring.buf[-1][0] if self.ring.buf else 0.0
                now = latest
                nxt = self.sched.next_window(now)
                if nxt is None:
                    continue
                window_id, t_start, t_end = nxt
                lvl = LADDER[self.bp.current_level]
                if self._stop.is_set():
                    break
                self.stats.windows_dispatched += 1
                self.stats.queue_depth = self.pool.in_flight
                asyncio.create_task(self._safe_dispatch(window_id, t_start, t_end, lvl))
                self.sched._next_dispatch_t = now + 1.0 / lvl.window_hz
        except asyncio.CancelledError:
            return

    async def _stats_loop(self) -> None:
        try:
            while not self._stop.is_set():
                await asyncio.sleep(1.0)
                self.stats.queue_depth = self.pool.in_flight
                self.stats.backpressure_level = self.bp.current_level
                await self._emit({"type": "stats", **self.stats.to_dict()})
                # backpressure update
                new_lvl = self.bp.update(self.pool.in_flight)
                if new_lvl is not None:
                    await self._emit({"type": "backpressure", "level": new_lvl,
                                      "label": LADDER[new_lvl].label})
        except asyncio.CancelledError:
            return

    # --- dispatch ------------------------------------------------------------

    async def _escalate(self, t: float) -> None:
        """Run a thinking-enabled M3 call on the most recent window for richer output.

        Stub: rate-limited to 1 in-flight, 10s/session minimum gap. On success,
        the result is patched back as `event.updated` over WS. On failure,
        silently swallowed (the fast-pass answer already shipped).
        """
        now = time.time()
        if now - self._last_escalation_at < 10.0:
            return
        if self._escalation_in_flight:
            return
        if not self.ring.buf:
            return
        # take last window worth of frames
        frames = list(self.ring.buf)[-8:]
        if len(frames) < 2:
            return
        offsets = [p - frames[0][0] for p, _ in frames]
        from .llm import encode_jpeg
        try:
            self._escalation_in_flight = True
            self._last_escalation_at = now
            self.stats.escalations_dispatched += 1
            frames_b64 = [encode_jpeg(rgb, get_settings().escalate_resolution)
                          for _, rgb in frames]
            msg = build_escalation_user_message(
                window_id=-1, t_start=frames[0][0], t_end=frames[-1][0],
                fast_pass_summary="(see recent narrative lines)", offsets=offsets,
            )
            result = await self.client.analyze_window(
                SYSTEM_PROMPT, msg, frames_b64, escalate=True,
            )
            # patch: re-emit narrative only (events already shipped via fast pass)
            if result.narrative:
                await self._emit({"type": "narrative",
                                  "window_id": -1,
                                  "t_start": frames[0][0],
                                  "t_end": frames[-1][0],
                                  "text": result.narrative,
                                  "escalated": True})
        except Exception:
            pass  # never let escalation crash the pipeline
        finally:
            self._escalation_in_flight = False

    async def _on_gate_fire(self, sig) -> None:
        # provisional event: client sees something happened RIGHT NOW
        ev = Event.provisional_from_gate(t=sig.t, reason="significant change")
        self.mem.ingest_new_events([ev])
        await self._emit({"type": "event.provisional", "event": ev.to_dict()})
        # escalation: if the gate flagged HIGH salience, queue an async
        # thinking-enabled M3 re-analysis of the same window for richer output.
        if sig.high_salience:
            asyncio.create_task(self._escalate(sig.t))

    async def _safe_dispatch(self, window_id: int, t_start: float, t_end: float,
                              lvl: BackpressureLevel) -> None:
        try:
            await self._dispatch_window(window_id, t_start, t_end, lvl)
        except Exception as exc:
            import traceback
            print(f"[pipeline] dispatch w{window_id} crashed: {exc!r}\n{traceback.format_exc()}")

    async def _dispatch_window(self, window_id: int, t_start: float, t_end: float,
                                lvl: BackpressureLevel) -> None:
        # assemble frames from ring
        frames = self.ring.recent(t_start, t_end)
        if len(frames) < 2:
            return
        # pick top-K by gate motion (motion already in ring? not stored — fall back to uniform sample)
        # For v1, uniform sample within the window.
        step = max(1, len(frames) // lvl.max_frames)
        sampled = frames[::step][:lvl.max_frames]
        if not sampled:
            return
        offsets = [p - sampled[0][0] for p, _ in sampled]
        rgb64 = [encode_jpeg(rgb, lvl.resolution) for _, rgb in sampled]
        await self._run_one(window_id, t_start, t_end, rgb64, offsets, lvl, escalate=False)

    async def _run_one(self, window_id: int, t_start: float, t_end: float,
                       frames_b64: list[str], offsets: list[float],
                       lvl: BackpressureLevel, escalate: bool) -> None:
        async def coro():
            self.stats.queue_depth = self.pool.in_flight + 1
            try:
                ts, te = t_start, t_end
                msg = (build_escalation_user_message(window_id, ts, te, "", offsets)
                       if escalate else
                       build_user_message(window_id, ts, te,
                                          self.mem.narrative_lines(),
                                          self.mem.event_lines(),
                                          self.mem.scene_summary,
                                          offsets))
                t0 = time.monotonic()
                result = await self.client.analyze_window(SYSTEM_PROMPT, msg, frames_b64,
                                                          escalate=escalate)
                dt = time.monotonic() - t0
                self.stats.record_latency(dt)
                self.stats.last_window_t = t_end
                if result.usage.cached_tokens > 0:
                    self.stats.cache_hits += 1
                else:
                    self.stats.cache_misses += 1
                self.stats.total_prompt_tokens += result.usage.prompt_tokens
                self.stats.total_completion_tokens += result.usage.completion_tokens
                if result.parse_failures:
                    self.stats.total_parse_failures += result.parse_failures
                await self._apply_result(window_id, t_start, t_end, result, escalate)
                return result
            except Exception as exc:
                self.bp.record_error()
                await self._emit({"type": "error", "code": "llm",
                                  "window_id": window_id, "message": repr(exc)})
                raise
            finally:
                self.stats.queue_depth = self.pool.in_flight

        try:
            await self.pool.submit(coro)
        except Exception:
            self.stats.windows_failed += 1

    async def _apply_result(self, window_id: int, t_start: float, t_end: float,
                            result, escalate: bool) -> None:
        self.stats.windows_completed += 1
        if not result.narrative and not result.events:
            return  # parse failure already counted
        # update memory
        self.mem.add_narrative(result.narrative)
        # build Event objects
        new_events: list[Event] = []
        for raw in result.events:
            new_events.append(Event(
                event_id=str(uuid.uuid4()),
                type=raw.get("type", "action"),
                t_start=t_start + raw.get("t_start", 0.0),
                t_end=t_start + raw.get("t_end", 0.0),
                description=raw.get("description", ""),
                confidence=raw.get("confidence", 0.5),
                actors=raw.get("actors", []),
                objects=raw.get("objects", []),
                location=raw.get("location", ""),
                key_entities=raw.get("key_entities", []),
                is_continuation=raw.get("is_continuation", False),
                source_windows=[window_id],
                first_seen=t_start + raw.get("t_start", 0.0),
                last_updated=time.time(),
                provisional=False,
            ))
        results = self.mem.ingest_new_events(new_events)
        for action, old, new in results:
            self.stats.events_emitted += 1
            await self._emit({
                "type": "event.confirmed" if not escalate else "event.updated",
                "event": new.to_dict(),
                "previous_event_id": old.event_id if old else None,
                "action": action,
            })
        # also push narrative
        await self._emit({
            "type": "narrative",
            "window_id": window_id,
            "t_start": t_start,
            "t_end": t_end,
            "text": result.narrative,
        })

    async def _emit(self, msg: dict) -> None:
        try:
            await self.emit(msg)
        except Exception:
            pass
