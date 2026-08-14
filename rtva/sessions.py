"""Session registry shared between legacy /sessions routes and /v1/* routes.

Owns the `SessionRecord` dataclass, the `SessionManager` class, and the singleton
`manager` instance. Lives in its own module so `rtva.api_v1` and `rtva.server`
can both import it without circularity.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Optional

from .config import get_settings
from .frame_source import FrameSource
from .pipeline import Pipeline


log = logging.getLogger("rtva.sessions")


@dataclass
class SessionRecord:
    pipeline: Pipeline
    observers: set = field(default_factory=set)
    replay_buf: deque = field(default_factory=lambda: deque(maxlen=200))
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    channel: str = "file"            # "file" | "ws" | "http" | "kcp"
    ingestor: object = None          # WSIngestor | HttpPostIngestor | KcpIngestor | None
    owner_token_label: str = ""      # label of the bearer that created it


class SessionManager:
    def __init__(self) -> None:
        self._sessions: dict[str, SessionRecord] = {}
        self._lock = asyncio.Lock()
        self._reaper_task: Optional[asyncio.Task] = None

    # ----- create paths -----

    async def create(self, source: str, options: Optional[dict] = None) -> SessionRecord:
        return await self._create_with(source_url=source, options=options,
                                       channel="file", ingestor=None,
                                       owner_token_label="")

    async def create_with_ingestor(self, *, frame_source: FrameSource,
                                   options: Optional[dict], channel: str,
                                   ingestor, owner_token_label: str) -> SessionRecord:
        return await self._create_with(frame_source=frame_source, options=options,
                                       channel=channel, ingestor=ingestor,
                                       owner_token_label=owner_token_label)

    async def _create_with(self, *, source_url: str = "",
                           frame_source: FrameSource | None = None,
                           options: Optional[dict], channel: str,
                           ingestor, owner_token_label: str) -> SessionRecord:
        options = options or {}

        async def emit(msg: dict) -> None:
            # Capture the session record directly in the closure (was looked up
            # via msg['session_id'] — which several message types never set, so
            # event.* / narrative were silently dropped before reaching any WS).
            # Inject session_id so observers / KCP peers / replay buffer can
            # correlate. setdefault keeps any session_id the pipeline already set.
            msg.setdefault("session_id", record.pipeline.session_id)
            record.replay_buf.append(msg)
            text = json.dumps(msg, default=str)
            dead = []
            for ws in list(record.observers):
                try:
                    await ws.send_text(text)
                except Exception:
                    dead.append(ws)
            for ws in dead:
                record.observers.discard(ws)
            # KCP outbound: ingestor may be a KcpIngestor with a bound peer
            if hasattr(record.ingestor, "send_event"):
                try:
                    record.ingestor.send_event(msg)
                except Exception:
                    pass

        pipeline = Pipeline(source_url=source_url, frame_source=frame_source,
                            emit=emit, options=options)
        record = SessionRecord(pipeline=pipeline, channel=channel,
                               ingestor=ingestor, owner_token_label=owner_token_label)
        async with self._lock:
            self._sessions[pipeline.session_id] = record
        asyncio.create_task(self._run(record))
        return record

    # ----- lifecycle -----

    async def _run(self, record: SessionRecord) -> None:
        try:
            await record.pipeline.run()
        except Exception as exc:
            print(f"[server] pipeline crashed: {exc!r}")
        finally:
            async with self._lock:
                self._sessions.pop(record.pipeline.session_id, None)

    async def stop(self, session_id: str) -> bool:
        async with self._lock:
            rec = self._sessions.get(session_id)
            if not rec:
                return False
            await rec.pipeline.stop()
            for ws in list(rec.observers):
                try:
                    await ws.close()
                except Exception:
                    pass
            self._sessions.pop(session_id, None)
            return True

    def get(self, session_id: str) -> Optional[SessionRecord]:
        return self._sessions.get(session_id)

    def all(self) -> list[SessionRecord]:
        return list(self._sessions.values())

    # ----- reaper -----

    async def start_reaper(self) -> None:
        """Start the periodic zombie-session sweeper.

        Wakes every `reaper_interval_s` and reaps sessions that:
            A) never received a frame and have lived past
               `session_never_started_timeout_s`, or
            B) have no observers AND the ingestor has been idle past
               `session_idle_timeout_s`.
        """
        if self._reaper_task is not None and not self._reaper_task.done():
            return
        self._reaper_task = asyncio.create_task(self._reaper_loop())
        log.info("session reaper started")

    async def stop_reaper(self) -> None:
        if self._reaper_task is None:
            return
        self._reaper_task.cancel()
        try:
            await self._reaper_task
        except (asyncio.CancelledError, Exception):
            pass
        self._reaper_task = None
        log.info("session reaper stopped")

    async def _reaper_loop(self) -> None:
        interval = max(0.5, get_settings().reaper_interval_s)
        while True:
            try:
                await asyncio.sleep(interval)
                await self._sweep_once()
            except asyncio.CancelledError:
                return
            except Exception as exc:
                log.warning("reaper loop error: %r", exc)

    async def _sweep_once(self) -> None:
        s = get_settings()
        never_started_limit = s.session_never_started_timeout_s
        idle_limit = s.session_idle_timeout_s
        now_wall = time.time()
        victims: list[str] = []

        async with self._lock:
            snapshot = list(self._sessions.items())
            for sid, rec in snapshot:
                st = rec.pipeline.stats
                uptime_s = now_wall - rec.pipeline.started_at
                # Class A: never produced a frame.
                if st.frames_received == 0 and uptime_s > never_started_limit:
                    victims.append(sid)
                    continue
                # Class B: source went away (no observers + idle ingestor).
                if not rec.observers and self._idle_seconds(rec) > idle_limit:
                    victims.append(sid)

        for sid in victims:
            log.info("reaping session %s", sid[:8])
            try:
                await self.stop(sid)
            except Exception as exc:
                log.warning("reaper: stop(%s) failed: %r", sid[:8], exc)

    @staticmethod
    def _idle_seconds(rec: SessionRecord) -> float:
        """Best-effort idle time. 0 if ingestor has no `idle_seconds`."""
        ing = rec.ingestor
        if ing is None:
            return 0.0
        get = getattr(ing, "idle_seconds", None)
        if get is None:
            return 0.0
        try:
            return float(get)
        except Exception:
            return 0.0


# Module-level singleton, imported by both server and api_v1.
manager = SessionManager()